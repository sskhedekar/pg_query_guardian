-- =============================================================================
-- pg_query_guardian : CLEANUP / UNINSTALL
-- Run on PRIMARY (postgres database), auto-commit ON in DBeaver
--
-- Removes everything pg_query_guardian installed:
--   · pg_cron job         (unscheduled — extension kept unless only guardian uses it)
--   · guardian schema     (all tables, views, functions, indexes)
--   · guardian_monitor    (role + all grants)
--   · dblink extension    (only if no other objects depend on it)
--   · pg_cron extension   (only if no other jobs exist)
--
-- All role drops and schema drops replicate to the replica via WAL automatically.
-- No replica cleanup script needed.
-- =============================================================================

-- =============================================================================
-- SAFETY: confirm this is the primary
-- =============================================================================
DO $$
BEGIN
    IF pg_is_in_recovery() THEN
        RAISE EXCEPTION
            '[cleanup] ERROR: Run this on PRIMARY, not replica. '
            'pg_is_in_recovery() = true. Aborting.';
    END IF;
    RAISE NOTICE '[cleanup] Confirmed: running on PRIMARY.';
END;
$$;

-- =============================================================================
-- STEP 1: Unschedule the pg_cron job
-- Removes only guardian's job — does not affect other cron jobs on the instance.
-- =============================================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        RAISE NOTICE '[cleanup] pg_cron not installed — skipping job unschedule.';
        RETURN;
    END IF;

    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'pg_query_guardian') THEN
        PERFORM cron.unschedule('pg_query_guardian');
        RAISE NOTICE '[cleanup] pg_cron job "pg_query_guardian" unscheduled.';
    ELSE
        RAISE NOTICE '[cleanup] pg_cron job not found — already removed or never scheduled.';
    END IF;
END;
$$;

-- =============================================================================
-- STEP 2: Drop guardian schema (CASCADE)
-- Drops in one statement:
--   Tables    : config, killed_queries, exemptions
--   View      : v_recent_kills
--   Indexes   : kq_killed_at_idx, kq_usename_idx, kq_application_idx, exemptions_unique_idx
--   Functions : terminate_runaway_queries(), cron_job_wrapper(), kill_summary(),
--               add/remove_exempt_user/app/pattern(), show_exemptions(),
--               show_connection(), rotate_password(), update_replica_host()
-- =============================================================================
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'guardian') THEN
        RAISE NOTICE '[cleanup] Dropping guardian schema and all contained objects...';
    ELSE
        RAISE NOTICE '[cleanup] guardian schema not found — skipping.';
    END IF;
END;
$$;

DROP SCHEMA IF EXISTS guardian CASCADE;

-- =============================================================================
-- STEP 3: Revoke grants from guardian_monitor, then drop the role
-- Grants must be revoked before DROP ROLE — otherwise PostgreSQL errors
-- on "role has privileges on X".
-- =============================================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'guardian_monitor') THEN
        RAISE NOTICE '[cleanup] guardian_monitor role not found — skipping.';
        RETURN;
    END IF;

    REVOKE CONNECT ON DATABASE postgres FROM guardian_monitor;

    BEGIN
        REVOKE pg_monitor        FROM guardian_monitor;
    EXCEPTION WHEN OTHERS THEN NULL; END;

    BEGIN
        REVOKE pg_signal_backend FROM guardian_monitor;
    EXCEPTION WHEN OTHERS THEN NULL; END;

    BEGIN
        REVOKE SELECT ON guardian.killed_queries FROM guardian_monitor;
    EXCEPTION WHEN OTHERS THEN NULL; END;

    BEGIN
        REVOKE USAGE ON SCHEMA guardian FROM guardian_monitor;
    EXCEPTION WHEN OTHERS THEN NULL; END;

    DROP ROLE guardian_monitor;
    RAISE NOTICE '[cleanup] guardian_monitor grants revoked and role dropped.';
END;
$$;

-- Drop legacy role from older installs (pre-unified-table design)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'guardian_writer') THEN
        DROP ROLE guardian_writer;
        RAISE NOTICE '[cleanup] guardian_writer legacy role dropped.';
    END IF;
END;
$$;

-- =============================================================================
-- STEP 4: Drop dblink extension
-- Skipped if any objects outside guardian schema still depend on dblink.
-- If your database uses dblink for other purposes, this step is skipped safely.
-- =============================================================================
DO $$
DECLARE
    v_deps INTEGER;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'dblink') THEN
        RAISE NOTICE '[cleanup] dblink not installed — skipping.';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO v_deps
    FROM pg_depend d
    JOIN pg_extension e ON e.oid = d.refobjid
    JOIN pg_proc p      ON p.oid = d.objid
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE e.extname = 'dblink'
      AND n.nspname NOT IN ('guardian', 'pg_catalog');

    IF v_deps > 0 THEN
        RAISE NOTICE
            '[cleanup] dblink has % dependent object(s) outside the guardian schema. '
            'Keeping dblink — remove those dependencies first if you want to drop it.',
            v_deps;
    ELSE
        DROP EXTENSION IF EXISTS dblink CASCADE;
        RAISE NOTICE '[cleanup] dblink extension dropped.';
    END IF;
END;
$$;

-- =============================================================================
-- STEP 5: Drop pg_cron extension
-- Skipped if any other cron jobs still exist on this instance.
-- If you use pg_cron for other scheduled tasks, the extension is kept safely.
-- =============================================================================
DO $$
DECLARE
    v_remaining INTEGER;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        RAISE NOTICE '[cleanup] pg_cron not installed — skipping.';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO v_remaining FROM cron.job;

    IF v_remaining > 0 THEN
        RAISE NOTICE
            '[cleanup] % other cron job(s) found on this instance. '
            'Keeping pg_cron extension — only the guardian job was removed in Step 1.',
            v_remaining;
    ELSE
        DROP EXTENSION IF EXISTS pg_cron CASCADE;
        RAISE NOTICE '[cleanup] pg_cron extension dropped (no other jobs existed).';
    END IF;
END;
$$;

-- =============================================================================
-- VERIFY — all values should be 0
-- Any non-zero value means something was not removed.
-- The guardian_cron_job column is only checked when pg_cron is still installed.
-- =============================================================================
SELECT
    (SELECT COUNT(*) FROM pg_namespace
     WHERE nspname = 'guardian')                                         AS guardian_schema,
    (SELECT COUNT(*) FROM pg_roles
     WHERE rolname IN ('guardian_monitor', 'guardian_writer'))           AS guardian_roles,
    (SELECT COUNT(*) FROM pg_extension
     WHERE extname = 'pg_cron')                                          AS pg_cron_ext,
    (SELECT COUNT(*) FROM pg_extension
     WHERE extname = 'dblink')                                           AS dblink_ext;

-- If pg_cron is still present (shared with other jobs), verify guardian's job is gone:
-- SELECT * FROM cron.job WHERE jobname = 'pg_query_guardian';
-- Expected: 0 rows

-- =============================================================================
-- MANUAL STEP: AWS RDS parameter group (only if pg_cron was dropped above)
-- =============================================================================
-- pg_cron in shared_preload_libraries is set at the RDS parameter group level.
-- Dropping the extension removes it from PostgreSQL, but the parameter group
-- entry remains. On the next restart PostgreSQL will log a warning that it
-- tried to load pg_cron but the extension is not installed.
--
-- To fully clean this up:
--   AWS Console → RDS → Parameter Groups → your-parameter-group
--   → Edit → shared_preload_libraries → remove 'pg_cron' → Save → Reboot
--
-- If you plan to reinstall guardian later: leave the parameter group as-is.
-- The warning is harmless and no reboot is needed until you reinstall.
-- =============================================================================
