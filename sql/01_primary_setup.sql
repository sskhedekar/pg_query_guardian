-- =============================================================================
-- pg_query_guardian : PRIMARY SETUP (single script — no replica setup needed)
-- Run on PRIMARY, connected to postgres database, auto-commit ON
--
-- Architecture:
--   PRIMARY : guardian schema, config, killed_queries, all functions, pg_cron job
--   REPLICA : no setup needed — only needs guardian_monitor role (auto-replicated)
--
--   Every minute:
--     pg_cron (primary) → dblink → replica pg_stat_activity
--                        → dblink → pg_terminate_backend on replica
--                        → INSERT directly into guardian.killed_queries (local, no dblink)
--
-- Kill conditions (two only):
--   1. RUNTIME_EXCEEDED       — query running > max_runtime_minutes (default 10)
--   2. INSTANCE_RESOURCE_HIGH — instance pressure > instance_threshold_pct (default 80)
--                               kills the single longest-running SELECT
--
-- BEFORE RUNNING: fill in replica connection details in the config block below
-- =============================================================================

-- Safety: confirm this is the primary
DO $$
BEGIN
    IF pg_is_in_recovery() THEN
        RAISE EXCEPTION '[guardian] ERROR: This must run on PRIMARY. pg_is_in_recovery() = true. Aborting.';
    END IF;
    RAISE NOTICE '[guardian] Confirmed: this is the PRIMARY.';
END;
$$;

-- =============================================================================
-- REPLICA CONNECTION CONFIG — EDIT THESE TWO VALUES BEFORE RUNNING
-- =============================================================================
DO $$
DECLARE
    v_replica_host  TEXT := 'YOUR-REPLICA-ENDPOINT.ap-south-1.rds.amazonaws.com'; -- EDIT THIS
    v_monitor_pw    TEXT := 'YOUR-MONITOR-PASSWORD';                               -- EDIT THIS
BEGIN
    IF v_replica_host LIKE 'YOUR-REPLICA%' THEN
        RAISE EXCEPTION '[guardian] You must set v_replica_host to your actual replica endpoint.';
    END IF;
    IF v_monitor_pw = 'YOUR-MONITOR-PASSWORD' THEN
        RAISE EXCEPTION '[guardian] You must choose a password for guardian_monitor.';
    END IF;

    PERFORM set_config('guardian.replica_host', v_replica_host, false);
    PERFORM set_config('guardian.monitor_pw',   v_monitor_pw,   false);

    RAISE NOTICE '[guardian] Config validated. replica_host=%', v_replica_host;
END;
$$;

-- =============================================================================
-- 1. EXTENSIONS
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS dblink;
CREATE EXTENSION IF NOT EXISTS pg_cron;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'dblink')
       AND EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        RAISE NOTICE '[guardian] Extensions OK: dblink + pg_cron both installed.';
    ELSE
        RAISE WARNING '[guardian] One or more extensions failed to install. '
                      'Check that pg_cron is in shared_preload_libraries and the instance was restarted.';
    END IF;
END;
$$;

-- =============================================================================
-- 2. SCHEMA
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS guardian;

-- =============================================================================
-- 3. CONFIG TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS guardian.config (
    key         TEXT        PRIMARY KEY,
    value       TEXT        NOT NULL,
    description TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO guardian.config (key, value, description) VALUES
    ( 'max_runtime_minutes',
      '10',
      'Kill any SELECT on the replica running longer than this many minutes. '
      'Change: UPDATE guardian.config SET value=''15'', updated_at=now() WHERE key=''max_runtime_minutes'';' ),
    ( 'instance_threshold_pct',
      '80',
      'When replica instance pressure exceeds this %, kill the longest-running SELECT. '
      'CPU proxy  = (active client backends / max_connections) * 100. '
      'MEM proxy  = pg_stat_io: SUM(evictions) / (SUM(evictions) + SUM(writes) + SUM(extends)) * 100. '
      'PostgreSQL 17+ only — uses pg_stat_io, not pg_stat_bgwriter.' ),
    ( 'dry_run',
      'false',
      'true = evaluate and log what WOULD be killed, but do not terminate. '
      'Enable:  UPDATE guardian.config SET value=''true'',  updated_at=now() WHERE key=''dry_run''; '
      'Disable: UPDATE guardian.config SET value=''false'', updated_at=now() WHERE key=''dry_run''; ' ),
    ( 'guardian.replica_host',
      'PLACEHOLDER',
      'Replica endpoint used by cron_job_wrapper() via dblink. '
      'To update: SELECT guardian.update_replica_host(''new-endpoint.rds.amazonaws.com'');' ),
    ( 'guardian.monitor_pw',
      'PLACEHOLDER',
      'guardian_monitor role password used by cron_job_wrapper() via dblink. '
      'To rotate: SELECT guardian.rotate_password(''NewPassword'');' ),
    ( 'guardian_exempt_tag',
      '/* guardian_exempt */',
      'Comment tag that exempts a query from guardian kills. '
      'Prepend this exact string to any query that should never be killed: '
      '  /* guardian_exempt */ SELECT ... '
      'To change: UPDATE guardian.config SET value=''/* approved_long_query */'', updated_at=now() '
      'WHERE key=''guardian_exempt_tag''; ' )
ON CONFLICT (key) DO NOTHING;

UPDATE guardian.config SET value = current_setting('guardian.replica_host'), updated_at = now()
WHERE key = 'guardian.replica_host';

UPDATE guardian.config SET value = current_setting('guardian.monitor_pw'), updated_at = now()
WHERE key = 'guardian.monitor_pw';

-- =============================================================================
-- 4. AUDIT LOG TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS guardian.killed_queries (
    id                        BIGSERIAL     PRIMARY KEY,
    killed_at                 TIMESTAMPTZ   NOT NULL DEFAULT now(),
    replica_host              TEXT          NOT NULL,
    pid                       INTEGER       NOT NULL,
    usename                   TEXT,
    application_name          TEXT,
    client_addr               INET,
    client_hostname           TEXT,
    backend_start             TIMESTAMPTZ,
    query_start               TIMESTAMPTZ,
    query                     TEXT          NOT NULL,
    query_duration            INTERVAL      NOT NULL,
    runtime_minutes           NUMERIC(10,2) NOT NULL,
    kill_reason               TEXT          NOT NULL
                                  CHECK (kill_reason IN (
                                      'RUNTIME_EXCEEDED',
                                      'INSTANCE_RESOURCE_HIGH'
                                  )),
    threshold_runtime_minutes NUMERIC(10,2),
    threshold_instance_pct    NUMERIC(6,2),
    instance_cpu_pct          NUMERIC(6,2),
    instance_mem_pct          NUMERIC(6,2),
    terminated                BOOLEAN       NOT NULL DEFAULT false,
    terminate_error           TEXT,
    log_note                  TEXT,
    action                    TEXT          NOT NULL DEFAULT 'KILLED'
                                  CHECK (action IN ('KILLED', 'DRY_RUN'))
);

CREATE INDEX IF NOT EXISTS kq_killed_at_idx   ON guardian.killed_queries (killed_at DESC);
CREATE INDEX IF NOT EXISTS kq_usename_idx     ON guardian.killed_queries (usename);
CREATE INDEX IF NOT EXISTS kq_application_idx ON guardian.killed_queries (application_name);

-- =============================================================================
-- 5. EXEMPTIONS TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS guardian.exemptions (
    id          SERIAL      PRIMARY KEY,
    type        TEXT        NOT NULL
                    CHECK (type IN ('USER', 'APP', 'PATTERN')),
    value       TEXT        NOT NULL,
    reason      TEXT        NOT NULL,
    added_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    added_by    TEXT        NOT NULL DEFAULT current_user
);

CREATE UNIQUE INDEX IF NOT EXISTS exemptions_unique_idx
    ON guardian.exemptions (type, lower(value));

INSERT INTO guardian.exemptions (type, value, reason, added_by) VALUES
    ( 'APP', 'pg_dump',
      'pg_dump runs long SELECT queries during backup — never kill.',
      'guardian_setup' ),
    ( 'APP', 'pg_query_guardian',
      'Guardian own dblink session on replica — never kill.',
      'guardian_setup' ),
    ( 'APP', 'pganalyze-collector',
      'pganalyze collector runs analytical queries — never kill.',
      'guardian_setup' )
ON CONFLICT (type, lower(value)) DO NOTHING;

-- =============================================================================
-- 6. CONVENIENCE VIEW
-- =============================================================================

DROP VIEW IF EXISTS guardian.v_recent_kills;

CREATE VIEW guardian.v_recent_kills AS
SELECT
    kq.id,
    kq.killed_at,
    kq.replica_host,
    kq.usename,
    kq.application_name,
    kq.client_addr::TEXT         AS client_addr,
    kq.kill_reason,
    kq.runtime_minutes,
    kq.query_duration,
    kq.instance_cpu_pct,
    kq.instance_mem_pct,
    kq.threshold_runtime_minutes,
    kq.threshold_instance_pct,
    kq.terminated,
    kq.terminate_error,
    kq.log_note,
    kq.pid,
    kq.action,
    left(kq.query, 300)          AS query_preview
FROM guardian.killed_queries kq
ORDER BY kq.killed_at DESC;

-- =============================================================================
-- 7. SUMMARY FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION guardian.kill_summary(
    p_since INTERVAL DEFAULT '24 hours'
)
RETURNS TABLE (
    period                  TEXT,
    replica_host            TEXT,
    total_kills             BIGINT,
    runtime_kills           BIGINT,
    instance_kills          BIGINT,
    unique_users            BIGINT,
    unique_applications     BIGINT,
    avg_runtime_mins        NUMERIC,
    max_runtime_mins        NUMERIC,
    successful_terminations BIGINT,
    failed_terminations     BIGINT,
    dry_run_evaluations     BIGINT
)
LANGUAGE sql VOLATILE AS $$
    SELECT
        p_since::TEXT,
        replica_host,
        COUNT(*),
        COUNT(*) FILTER (WHERE kill_reason = 'RUNTIME_EXCEEDED'),
        COUNT(*) FILTER (WHERE kill_reason = 'INSTANCE_RESOURCE_HIGH'),
        COUNT(DISTINCT usename),
        COUNT(DISTINCT application_name),
        ROUND(AVG(runtime_minutes)::NUMERIC, 2),
        ROUND(MAX(runtime_minutes)::NUMERIC, 2),
        COUNT(*) FILTER (WHERE terminated = true  AND action = 'KILLED'),
        COUNT(*) FILTER (WHERE terminated = false AND action = 'KILLED'),
        COUNT(*) FILTER (WHERE action = 'DRY_RUN')
    FROM guardian.killed_queries
    WHERE killed_at >= now() - p_since
    GROUP BY replica_host
    ORDER BY COUNT(*) DESC;
$$;

-- =============================================================================
-- 8. guardian_monitor ROLE
-- =============================================================================

DO $$
DECLARE
    v_pw TEXT := current_setting('guardian.monitor_pw', true);
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'guardian_monitor') THEN
        EXECUTE format('CREATE ROLE guardian_monitor WITH LOGIN PASSWORD %L CONNECTION LIMIT 3', v_pw);
        RAISE NOTICE '[guardian] guardian_monitor role created.';
    ELSE
        EXECUTE format('ALTER ROLE guardian_monitor PASSWORD %L', v_pw);
        RAISE NOTICE '[guardian] guardian_monitor role already exists — password updated.';
    END IF;
END;
$$;

GRANT CONNECT ON DATABASE postgres TO guardian_monitor;
GRANT pg_monitor        TO guardian_monitor;
GRANT pg_signal_backend TO guardian_monitor;
GRANT USAGE  ON SCHEMA guardian TO guardian_monitor;
GRANT SELECT ON guardian.killed_queries TO guardian_monitor;

-- =============================================================================
-- 9. MAIN TERMINATION FUNCTION
-- =============================================================================
-- Runs on the primary every minute via pg_cron.
-- Connects to the replica via dblink to:
--   a) Read pg_stat_activity
--   b) Read instance metrics (pg_stat_io + pg_settings — PostgreSQL 17+)
--   c) Call pg_terminate_backend for runaway queries
-- Logs all kills directly to guardian.killed_queries (local INSERT, no dblink)
--
-- Exemption loading: ONE query scans guardian.exemptions once using FILTER
-- aggregation to extract users, apps, and patterns in a single pass.
-- This replaces the previous 6 separate queries across 3 tables.

DROP FUNCTION IF EXISTS guardian.terminate_runaway_queries(TEXT);

CREATE OR REPLACE FUNCTION guardian.terminate_runaway_queries(
    p_replica_connstr TEXT
)
RETURNS TABLE (
    action              TEXT,
    killed_pid          INTEGER,
    killed_user         TEXT,
    killed_app          TEXT,
    kill_reason         TEXT,
    runtime_minutes     NUMERIC,
    instance_cpu_pct    NUMERIC,
    instance_mem_pct    NUMERIC,
    query_preview       TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    -- Config
    v_max_runtime_mins  NUMERIC;
    v_inst_threshold    NUMERIC;
    v_dry_run           BOOLEAN;
    v_exempt_tag        TEXT;

    -- Instance metrics
    v_inst_cpu          NUMERIC;
    v_inst_mem          NUMERIC;
    v_replica_host      TEXT;

    -- Exemption clause fragments injected into dblink queries
    -- Each is either '' (no exemptions of that type) or a valid SQL AND fragment
    v_user_clause       TEXT;   -- AND usename != ALL(ARRAY['u1','u2']::TEXT[])
    v_app_clause        TEXT;   -- AND application_name != ALL(ARRAY['a1','a2']::TEXT[])
    v_pattern_clause    TEXT;   -- AND NOT (query ILIKE 'p1' OR query ILIKE 'p2')
    v_tag_clause        TEXT;   -- AND query NOT ILIKE '/* guardian_exempt */%'

    -- Intermediate exemption aggregates from single-query load
    v_user_quoted       TEXT;   -- quote_literal values for user clause
    v_app_quoted        TEXT;   -- quote_literal values for app clause
    v_pattern_ilike     TEXT;   -- 'query ILIKE p1 OR query ILIKE p2' fragment
    v_user_cnt          INTEGER;
    v_app_cnt           INTEGER;
    v_pattern_cnt       INTEGER;

    -- Loop variables
    r                   RECORD;
    v_terminated        BOOLEAN;
    v_term_error        TEXT;
    v_action            TEXT;
    v_log_note          TEXT;
    v_killed_pids       INTEGER[] := ARRAY[]::INTEGER[];
    v_top_row           RECORD;
BEGIN
    -- =========================================================================
    -- Load config (4 targeted SELECTs — avoids MAX(boolean) limitation)
    -- =========================================================================
    SELECT value::NUMERIC INTO v_max_runtime_mins FROM guardian.config WHERE key = 'max_runtime_minutes';
    SELECT value::NUMERIC INTO v_inst_threshold   FROM guardian.config WHERE key = 'instance_threshold_pct';
    SELECT value::BOOLEAN INTO v_dry_run           FROM guardian.config WHERE key = 'dry_run';
    SELECT value          INTO v_exempt_tag        FROM guardian.config WHERE key = 'guardian_exempt_tag';

    v_max_runtime_mins := COALESCE(v_max_runtime_mins, 10);
    v_inst_threshold   := COALESCE(v_inst_threshold,   80);
    v_dry_run          := COALESCE(v_dry_run,          false);
    v_exempt_tag       := COALESCE(v_exempt_tag,       '/* guardian_exempt */');

    RAISE NOTICE '[guardian] Thresholds: runtime=% min, instance=% pct, dry_run=%',
        v_max_runtime_mins, v_inst_threshold, v_dry_run;

    -- =========================================================================
    -- Load ALL exemptions in ONE query
    -- =========================================================================
    SELECT
        string_agg(quote_literal(value), ',')
            FILTER (WHERE type = 'USER')                          AS user_quoted,
        COUNT(*) FILTER (WHERE type = 'USER')                     AS user_cnt,
        string_agg(quote_literal(value), ',')
            FILTER (WHERE type = 'APP')                           AS app_quoted,
        COUNT(*) FILTER (WHERE type = 'APP')                      AS app_cnt,
        string_agg(format('query ILIKE %L', value), ' OR ')
            FILTER (WHERE type = 'PATTERN')                       AS pattern_ilike,
        COUNT(*) FILTER (WHERE type = 'PATTERN')                  AS pattern_cnt
    INTO
        v_user_quoted,    v_user_cnt,
        v_app_quoted,     v_app_cnt,
        v_pattern_ilike,  v_pattern_cnt
    FROM guardian.exemptions;

    -- Build SQL clauses from aggregated results
    v_user_clause    := CASE WHEN v_user_cnt > 0
                        THEN 'AND usename != ALL(ARRAY[' || v_user_quoted || ']::TEXT[])'
                        ELSE '' END;

    v_app_clause     := CASE WHEN v_app_cnt > 0
                        THEN 'AND application_name != ALL(ARRAY[' || v_app_quoted || ']::TEXT[])'
                        ELSE '' END;

    v_pattern_clause := CASE WHEN v_pattern_cnt > 0
                        THEN 'AND NOT (' || v_pattern_ilike || ')'
                        ELSE '' END;

    -- Comment tag clause — always active, always one condition
    v_tag_clause := format('AND query NOT ILIKE %L', v_exempt_tag || '%');

    -- Log exemption summary
    RAISE NOTICE '[guardian] Exemptions loaded: users=%, apps=%, patterns=%',
        v_user_cnt, v_app_cnt, v_pattern_cnt;

    -- =========================================================================
    -- Get replica hostname
    -- =========================================================================
    BEGIN
        SELECT host INTO v_replica_host
        FROM dblink(p_replica_connstr, 'SELECT inet_server_addr()::TEXT') AS t(host TEXT);
        v_replica_host := COALESCE(v_replica_host, 'unknown-replica');
    EXCEPTION WHEN OTHERS THEN
        v_replica_host := 'unknown-replica';
    END;

    -- =========================================================================
    -- Get instance metrics from replica
    -- =========================================================================
    BEGIN
        SELECT cpu_pct, mem_pct INTO v_inst_cpu, v_inst_mem
        FROM dblink(
            p_replica_connstr,
            $METRIC$
            SELECT
                ROUND(
                    (SELECT COUNT(*)::NUMERIC FROM pg_stat_activity
                     WHERE state NOT IN ('idle','idle in transaction','idle in transaction (aborted)')
                       AND backend_type = 'client backend'
                       AND usename != 'rdsadmin')
                    / GREATEST((SELECT setting::NUMERIC FROM pg_settings WHERE name = 'max_connections'), 1)
                    * 100, 2
                ) AS cpu_pct,
                ROUND(
                    COALESCE(
                        (SELECT SUM(evictions)::NUMERIC
                         / GREATEST(SUM(evictions) + SUM(writes) + SUM(extends), 1)
                         * 100
                         FROM pg_stat_io
                         WHERE backend_type = 'client backend'
                           AND object = 'relation'),
                        0
                    ), 2
                ) AS mem_pct
            $METRIC$
        ) AS t(cpu_pct NUMERIC, mem_pct NUMERIC);
    EXCEPTION WHEN OTHERS THEN
        v_inst_cpu := 0;
        v_inst_mem := 0;
        RAISE WARNING '[guardian] Could not get instance metrics: %', SQLERRM;
    END;

    RAISE NOTICE '[guardian] Replica metrics: cpu=% pct, mem=% pct', v_inst_cpu, v_inst_mem;

    -- =========================================================================
    -- PASS 1: RUNTIME_EXCEEDED
    -- Kill every active SELECT/WITH on replica running > max_runtime_minutes
    -- All four exemption clauses injected into the remote query
    -- =========================================================================
    FOR r IN
        SELECT t.pid, t.usename, t.application_name,
               t.client_addr, t.client_hostname,
               t.backend_start, t.query_start, t.query,
               t.query_duration, t.runtime_minutes
        FROM dblink(
            p_replica_connstr,
            format(
                $STAT$
                SELECT pid, usename, application_name,
                       client_addr::TEXT, client_hostname,
                       backend_start, query_start, query,
                       now() - query_start AS query_duration,
                       ROUND(EXTRACT(EPOCH FROM (now() - query_start)) / 60::NUMERIC, 2) AS runtime_minutes
                FROM pg_stat_activity
                WHERE state       = 'active'
                  AND (query ILIKE 'select%%' OR query ILIKE 'with%%')
                  AND usename     != 'rdsadmin'
                  AND query_start IS NOT NULL
                  AND EXTRACT(EPOCH FROM (now() - query_start)) / 60 >= %s
                  %s
                  %s
                  %s
                  %s
                ORDER BY query_start ASC
                $STAT$,
                v_max_runtime_mins,
                v_user_clause,
                v_app_clause,
                v_pattern_clause,
                v_tag_clause
            )
        ) AS t(pid INTEGER, usename TEXT, application_name TEXT,
               client_addr TEXT, client_hostname TEXT,
               backend_start TIMESTAMPTZ, query_start TIMESTAMPTZ, query TEXT,
               query_duration INTERVAL, runtime_minutes NUMERIC)
    LOOP
        v_terminated := false;
        v_term_error := NULL;
        v_log_note   := NULL;

        RAISE NOTICE '[guardian] RUNTIME_EXCEEDED: pid=% user=% app=% runtime=%min',
            r.pid, r.usename, r.application_name, r.runtime_minutes;

        IF v_dry_run THEN
            v_action     := 'DRY_RUN';
            v_terminated := true;
            v_log_note   := 'dry_run=true — evaluated but not terminated';
        ELSE
            v_action := 'KILLED';
            BEGIN
                SELECT t.terminated INTO v_terminated
                FROM dblink(p_replica_connstr,
                    format('SELECT pg_terminate_backend(%s)', r.pid)
                ) AS t(terminated BOOLEAN);
                IF NOT v_terminated THEN
                    v_term_error := 'pg_terminate_backend returned false — process may have already finished';
                END IF;
            EXCEPTION WHEN OTHERS THEN
                v_terminated := false;
                v_term_error := SQLERRM;
            END;
        END IF;

        INSERT INTO guardian.killed_queries (
            killed_at, replica_host,
            pid, usename, application_name,
            client_addr, client_hostname, backend_start, query_start,
            query, query_duration, runtime_minutes, kill_reason,
            threshold_runtime_minutes, threshold_instance_pct,
            instance_cpu_pct, instance_mem_pct,
            terminated, terminate_error, log_note, action
        ) VALUES (
            now(), v_replica_host,
            r.pid, r.usename, r.application_name,
            r.client_addr::INET, r.client_hostname, r.backend_start, r.query_start,
            r.query, r.query_duration, r.runtime_minutes, 'RUNTIME_EXCEEDED',
            v_max_runtime_mins, v_inst_threshold,
            v_inst_cpu, v_inst_mem,
            v_terminated, v_term_error, v_log_note, v_action
        );

        v_killed_pids := array_append(v_killed_pids, r.pid);

        RETURN QUERY SELECT
            v_action, r.pid, r.usename, r.application_name,
            'RUNTIME_EXCEEDED'::TEXT,
            r.runtime_minutes, v_inst_cpu, v_inst_mem,
            left(r.query, 200);
    END LOOP;

    -- =========================================================================
    -- PASS 2: INSTANCE_RESOURCE_HIGH
    -- If instance pressure > threshold, kill the single longest-running SELECT
    -- Same exemption clauses applied — exempt queries are never killed
    -- =========================================================================
    IF v_inst_cpu > v_inst_threshold OR v_inst_mem > v_inst_threshold THEN

        RAISE NOTICE '[guardian] INSTANCE_RESOURCE_HIGH: cpu=% pct, mem=% pct, threshold=% pct',
            v_inst_cpu, v_inst_mem, v_inst_threshold;

        BEGIN
            SELECT t.pid, t.usename, t.application_name,
                   t.client_addr, t.client_hostname,
                   t.backend_start, t.query_start, t.query,
                   t.query_duration, t.runtime_minutes
            INTO v_top_row
            FROM dblink(
                p_replica_connstr,
                format(
                    $STAT2$
                    SELECT pid, usename, application_name,
                           client_addr::TEXT, client_hostname,
                           backend_start, query_start, query,
                           now() - query_start AS query_duration,
                           ROUND(EXTRACT(EPOCH FROM (now() - query_start)) / 60::NUMERIC, 2) AS runtime_minutes
                    FROM pg_stat_activity
                    WHERE state       = 'active'
                      AND (query ILIKE 'select%%' OR query ILIKE 'with%%')
                      AND usename     != 'rdsadmin'
                      AND query_start IS NOT NULL
                      AND pid         != ALL(ARRAY[%s]::INTEGER[])
                      %s
                      %s
                      %s
                      %s
                    ORDER BY query_start ASC
                    LIMIT 1
                    $STAT2$,
                    CASE WHEN array_length(v_killed_pids, 1) IS NULL
                         THEN '0'
                         ELSE array_to_string(v_killed_pids, ',')
                    END,
                    v_user_clause,
                    v_app_clause,
                    v_pattern_clause,
                    v_tag_clause
                )
            ) AS t(pid INTEGER, usename TEXT, application_name TEXT,
                   client_addr TEXT, client_hostname TEXT,
                   backend_start TIMESTAMPTZ, query_start TIMESTAMPTZ, query TEXT,
                   query_duration INTERVAL, runtime_minutes NUMERIC);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '[guardian] Could not query replica for Pass 2: %', SQLERRM;
            v_top_row := NULL;
        END;

        IF v_top_row.pid IS NOT NULL THEN
            v_terminated := false;
            v_term_error := NULL;
            v_log_note   := format(
                'Instance pressure: cpu=%s pct mem=%s pct threshold=%s pct',
                v_inst_cpu, v_inst_mem, v_inst_threshold
            );

            RAISE NOTICE '[guardian] INSTANCE_RESOURCE_HIGH: killing pid=% user=% app=% runtime=%min',
                v_top_row.pid, v_top_row.usename, v_top_row.application_name, v_top_row.runtime_minutes;

            IF v_dry_run THEN
                v_action     := 'DRY_RUN';
                v_terminated := true;
                v_log_note   := v_log_note || ' | dry_run=true — evaluated but not terminated';
            ELSE
                v_action := 'KILLED';
                BEGIN
                    SELECT t.terminated INTO v_terminated
                    FROM dblink(p_replica_connstr,
                        format('SELECT pg_terminate_backend(%s)', v_top_row.pid)
                    ) AS t(terminated BOOLEAN);
                    IF NOT v_terminated THEN
                        v_term_error := 'pg_terminate_backend returned false — process may have already finished';
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    v_terminated := false;
                    v_term_error := SQLERRM;
                END;
            END IF;

            INSERT INTO guardian.killed_queries (
                killed_at, replica_host,
                pid, usename, application_name,
                client_addr, client_hostname, backend_start, query_start,
                query, query_duration, runtime_minutes, kill_reason,
                threshold_runtime_minutes, threshold_instance_pct,
                instance_cpu_pct, instance_mem_pct,
                terminated, terminate_error, log_note, action
            ) VALUES (
                now(), v_replica_host,
                v_top_row.pid, v_top_row.usename, v_top_row.application_name,
                v_top_row.client_addr::INET, v_top_row.client_hostname,
                v_top_row.backend_start, v_top_row.query_start,
                v_top_row.query, v_top_row.query_duration, v_top_row.runtime_minutes,
                'INSTANCE_RESOURCE_HIGH',
                v_max_runtime_mins, v_inst_threshold,
                v_inst_cpu, v_inst_mem,
                v_terminated, v_term_error, v_log_note, v_action
            );

            RETURN QUERY SELECT
                v_action,
                v_top_row.pid, v_top_row.usename, v_top_row.application_name,
                'INSTANCE_RESOURCE_HIGH'::TEXT,
                v_top_row.runtime_minutes, v_inst_cpu, v_inst_mem,
                left(v_top_row.query, 200);
        ELSE
            RAISE NOTICE '[guardian] INSTANCE_RESOURCE_HIGH triggered but no eligible queries found.';
        END IF;

    ELSE
        RAISE NOTICE '[guardian] Instance metrics within threshold — no instance-level kill.';
    END IF;

    RAISE NOTICE '[guardian] Run complete.';
END;
$$;

-- =============================================================================
-- 10. pg_cron WRAPPER FUNCTION
-- =============================================================================

DO $$
DECLARE
    v_host TEXT := current_setting('guardian.replica_host', true);
    v_pw   TEXT := current_setting('guardian.monitor_pw',  true);
BEGIN
    EXECUTE format(
        $F$
        CREATE OR REPLACE FUNCTION guardian.cron_job_wrapper()
        RETURNS VOID
        LANGUAGE plpgsql
        SECURITY DEFINER
        AS $BODY$
        DECLARE
            v_connstr TEXT;
        BEGIN
            v_connstr := format(
                'host=%%s dbname=postgres user=guardian_monitor password=%%s '
                'sslmode=require connect_timeout=5 application_name=pg_query_guardian',
                %L, %L
            );
            PERFORM guardian.terminate_runaway_queries(v_connstr);
        END;
        $BODY$
        $F$,
        v_host, v_pw
    );
    RAISE NOTICE '[guardian] cron_job_wrapper created. replica_host=%', v_host;
END;
$$;

-- =============================================================================
-- 11. SCHEDULE WITH pg_cron
-- =============================================================================

DO $$
DECLARE
    v_job_id BIGINT;
BEGIN
    BEGIN
        PERFORM cron.unschedule('pg_query_guardian');
        RAISE NOTICE '[guardian] Removed previous pg_cron job.';
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    SELECT cron.schedule(
        'pg_query_guardian',
        '* * * * *',
        'SELECT guardian.cron_job_wrapper()'
    ) INTO v_job_id;

    RAISE NOTICE '[guardian] pg_cron job scheduled every minute. job_id=%', v_job_id;
END;
$$;

-- =============================================================================
-- 12. HELPER: add_exempt_user()
-- =============================================================================
-- Exempt a PostgreSQL user from all guardian kills.
-- Usage: SELECT guardian.add_exempt_user('analytics_user', 'BI team — reports up to 2h');

CREATE OR REPLACE FUNCTION guardian.add_exempt_user(
    p_username  TEXT,
    p_reason    TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    IF p_username IS NULL OR length(trim(p_username)) = 0 THEN
        RAISE EXCEPTION '[guardian] Username cannot be empty.';
    END IF;
    IF p_reason IS NULL OR length(trim(p_reason)) = 0 THEN
        RAISE EXCEPTION '[guardian] Reason cannot be empty. Document why this user is exempt.';
    END IF;

    INSERT INTO guardian.exemptions (type, value, reason, added_by)
    VALUES ('USER', trim(p_username), trim(p_reason), current_user)
    ON CONFLICT (type, lower(value)) DO UPDATE
        SET reason   = EXCLUDED.reason,
            added_at = now(),
            added_by = EXCLUDED.added_by;

    RETURN format('[guardian] User "%s" exempted. Takes effect on next pg_cron tick.', p_username);
END;
$$;

COMMENT ON FUNCTION guardian.add_exempt_user IS
    'Exempts a PostgreSQL user from all guardian query termination. '
    'Usage: SELECT guardian.add_exempt_user(''username'', ''reason'');';

-- =============================================================================
-- 13. HELPER: remove_exempt_user()
-- =============================================================================
-- Usage: SELECT guardian.remove_exempt_user('analytics_user');

CREATE OR REPLACE FUNCTION guardian.remove_exempt_user(p_username TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_deleted INTEGER;
BEGIN
    DELETE FROM guardian.exemptions
    WHERE type = 'USER' AND lower(value) = lower(trim(p_username));
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    IF v_deleted = 0 THEN
        RETURN format('[guardian] User "%s" was not in the exemption list.', p_username);
    END IF;
    RETURN format('[guardian] User "%s" removed. Takes effect on next pg_cron tick.', p_username);
END;
$$;

COMMENT ON FUNCTION guardian.remove_exempt_user IS
    'Removes a user from the guardian exemption list. '
    'Usage: SELECT guardian.remove_exempt_user(''username'');';

-- =============================================================================
-- 14. HELPER: add_exempt_app()
-- =============================================================================
-- Exempt by application_name — set in the client connection string.
-- Recommended for application queries with runtime variables.
--
-- How the app sets it:
--   JDBC:      jdbc:postgresql://host/db?applicationName=nightly_report
--   psycopg2:  psycopg2.connect(..., application_name='nightly_report')
--   libpq:     application_name=nightly_report (in connection string)
--   DBeaver:   Connection → PostgreSQL tab → Application name field
--
-- Usage: SELECT guardian.add_exempt_app('nightly_report', 'ETL job — runs 90 min');

CREATE OR REPLACE FUNCTION guardian.add_exempt_app(
    p_app_name  TEXT,
    p_reason    TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    IF p_app_name IS NULL OR length(trim(p_app_name)) = 0 THEN
        RAISE EXCEPTION '[guardian] Application name cannot be empty.';
    END IF;
    IF p_reason IS NULL OR length(trim(p_reason)) = 0 THEN
        RAISE EXCEPTION '[guardian] Reason cannot be empty. Document why this app is exempt.';
    END IF;

    INSERT INTO guardian.exemptions (type, value, reason, added_by)
    VALUES ('APP', trim(p_app_name), trim(p_reason), current_user)
    ON CONFLICT (type, lower(value)) DO UPDATE
        SET reason   = EXCLUDED.reason,
            added_at = now(),
            added_by = EXCLUDED.added_by;

    RETURN format('[guardian] App "%s" exempted. Takes effect on next pg_cron tick.', p_app_name);
END;
$$;

COMMENT ON FUNCTION guardian.add_exempt_app IS
    'Exempts all queries from a given application_name from guardian kills. '
    'Application must set application_name in its connection string. '
    'Usage: SELECT guardian.add_exempt_app(''nightly_report'', ''reason'');';

-- =============================================================================
-- 15. HELPER: remove_exempt_app()
-- =============================================================================
-- Usage: SELECT guardian.remove_exempt_app('nightly_report');

CREATE OR REPLACE FUNCTION guardian.remove_exempt_app(p_app_name TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_deleted INTEGER;
BEGIN
    DELETE FROM guardian.exemptions
    WHERE type = 'APP' AND lower(value) = lower(trim(p_app_name));
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    IF v_deleted = 0 THEN
        RETURN format('[guardian] App "%s" was not in the exemption list.', p_app_name);
    END IF;
    RETURN format('[guardian] App "%s" removed. Takes effect on next pg_cron tick.', p_app_name);
END;
$$;

COMMENT ON FUNCTION guardian.remove_exempt_app IS
    'Removes an application from the guardian exemption list. '
    'Usage: SELECT guardian.remove_exempt_app(''nightly_report'');';

-- =============================================================================
-- 16. HELPER: add_exempt_pattern()
-- =============================================================================
-- Exempt queries matching a text pattern (ILIKE syntax, % as wildcard).
-- Best for queries with stable text prefixes.
-- For queries with runtime variables, use add_exempt_app() or comment tag instead.
--
-- Examples:
--   'select%from reporting.%'    — any query reading from reporting schema
--   'with monthly_summary%'      — CTEs starting with monthly_summary
--
-- Usage: SELECT guardian.add_exempt_pattern('select%from reporting.%', 'Nightly reports');

CREATE OR REPLACE FUNCTION guardian.add_exempt_pattern(
    p_pattern   TEXT,
    p_reason    TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_id INTEGER;
BEGIN
    IF p_pattern IS NULL OR length(trim(p_pattern)) = 0 THEN
        RAISE EXCEPTION '[guardian] Pattern cannot be empty.';
    END IF;
    IF p_reason IS NULL OR length(trim(p_reason)) = 0 THEN
        RAISE EXCEPTION '[guardian] Reason cannot be empty. Document why this pattern is exempt.';
    END IF;

    INSERT INTO guardian.exemptions (type, value, reason, added_by)
    VALUES ('PATTERN', trim(p_pattern), trim(p_reason), current_user)
    ON CONFLICT (type, lower(value)) DO UPDATE
        SET reason   = EXCLUDED.reason,
            added_at = now(),
            added_by = EXCLUDED.added_by
    RETURNING id INTO v_id;

    RETURN format('[guardian] Pattern "%s" added with id=%s. Takes effect on next pg_cron tick.',
        p_pattern, v_id);
END;
$$;

COMMENT ON FUNCTION guardian.add_exempt_pattern IS
    'Adds a query text pattern (ILIKE) to the guardian exemption list. '
    'Use %% as wildcard. Case-insensitive. '
    'Usage: SELECT guardian.add_exempt_pattern(''select%from reporting%'', ''reason'');';

-- =============================================================================
-- 17. HELPER: remove_exempt_pattern()
-- =============================================================================
-- Usage: SELECT guardian.remove_exempt_pattern(3);   -- id from show_exemptions()

CREATE OR REPLACE FUNCTION guardian.remove_exempt_pattern(p_id INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_pattern TEXT;
BEGIN
    SELECT value INTO v_pattern
    FROM guardian.exemptions
    WHERE id = p_id AND type = 'PATTERN';

    IF v_pattern IS NULL THEN
        RETURN format('[guardian] No PATTERN exemption found with id=%s.', p_id);
    END IF;

    DELETE FROM guardian.exemptions WHERE id = p_id AND type = 'PATTERN';

    RETURN format('[guardian] Pattern id=%s ("%s") removed. Takes effect on next pg_cron tick.',
        p_id, v_pattern);
END;
$$;

COMMENT ON FUNCTION guardian.remove_exempt_pattern IS
    'Removes a query pattern exemption by id. '
    'Get id from: SELECT * FROM guardian.show_exemptions(); '
    'Usage: SELECT guardian.remove_exempt_pattern(3);';

-- =============================================================================
-- 18. HELPER: show_exemptions()
-- =============================================================================
-- Shows all active exemptions (users, apps, patterns) plus comment tag config.
-- Usage: SELECT * FROM guardian.show_exemptions();

CREATE OR REPLACE FUNCTION guardian.show_exemptions()
RETURNS TABLE (
    exemption_type  TEXT,
    id              TEXT,
    value           TEXT,
    reason          TEXT,
    added_at        TIMESTAMPTZ,
    added_by        TEXT
)
LANGUAGE sql
VOLATILE
SECURITY DEFINER
AS $$
    -- All exemption rows from unified table
    SELECT
        CASE type
            WHEN 'USER'    THEN 'USER'
            WHEN 'APP'     THEN 'APPLICATION'
            WHEN 'PATTERN' THEN 'QUERY PATTERN'
        END         AS exemption_type,
        id::TEXT,
        value,
        reason,
        added_at,
        added_by
    FROM guardian.exemptions
    UNION ALL
    -- Comment tag from config (always present)
    SELECT
        'COMMENT TAG'::TEXT,
        'config'::TEXT,
        value,
        'Queries starting with this tag are never killed (set in guardian.config)',
        updated_at,
        'guardian.config'::TEXT
    FROM guardian.config
    WHERE key = 'guardian_exempt_tag'
    ORDER BY exemption_type, added_at;
$$;

COMMENT ON FUNCTION guardian.show_exemptions IS
    'Shows all active exemptions: users, apps, patterns, and comment tag. '
    'Usage: SELECT * FROM guardian.show_exemptions();';

-- =============================================================================
-- 19. HELPER: rotate_password()
-- =============================================================================
-- Usage: SELECT guardian.rotate_password('NewStrongPassword2026');

CREATE OR REPLACE FUNCTION guardian.rotate_password(p_new_password TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_host TEXT;
BEGIN
    IF p_new_password IS NULL OR length(trim(p_new_password)) = 0 THEN
        RAISE EXCEPTION '[guardian] Password cannot be empty.';
    END IF;

    SELECT value INTO v_host FROM guardian.config WHERE key = 'guardian.replica_host';

    IF v_host IS NULL THEN
        RAISE EXCEPTION '[guardian] Replica host not found in guardian.config. Has setup been run?';
    END IF;

    EXECUTE format('ALTER ROLE guardian_monitor PASSWORD %L', p_new_password);

    UPDATE guardian.config
    SET value = p_new_password, updated_at = now()
    WHERE key = 'guardian.monitor_pw';

    EXECUTE format(
        $F$
        CREATE OR REPLACE FUNCTION guardian.cron_job_wrapper()
        RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS $BODY$
        DECLARE v_connstr TEXT;
        BEGIN
            v_connstr := format(
                'host=%%s dbname=postgres user=guardian_monitor password=%%s sslmode=require connect_timeout=5 application_name=pg_query_guardian',
                %L, %L
            );
            PERFORM guardian.terminate_runaway_queries(v_connstr);
        END;
        $BODY$
        $F$,
        v_host, p_new_password
    );

    RETURN format('[guardian] Password rotated. cron_job_wrapper updated. replica_host=%s', v_host);
END;
$$;

COMMENT ON FUNCTION guardian.rotate_password IS
    'Rotates the guardian_monitor password. '
    'Updates the role, guardian.config, and recreates cron_job_wrapper(). '
    'Usage: SELECT guardian.rotate_password(''NewPassword2026'');';

-- =============================================================================
-- 20. HELPER: update_replica_host()
-- =============================================================================
-- Usage: SELECT guardian.update_replica_host('new-replica.xxxx.ap-south-1.rds.amazonaws.com');

CREATE OR REPLACE FUNCTION guardian.update_replica_host(p_new_host TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_pw       TEXT;
    v_old_host TEXT;
BEGIN
    IF p_new_host IS NULL OR length(trim(p_new_host)) = 0 THEN
        RAISE EXCEPTION '[guardian] Replica host cannot be empty.';
    END IF;

    SELECT value INTO v_old_host FROM guardian.config WHERE key = 'guardian.replica_host';
    SELECT value INTO v_pw       FROM guardian.config WHERE key = 'guardian.monitor_pw';

    IF v_pw IS NULL THEN
        RAISE EXCEPTION '[guardian] guardian_monitor password not found in guardian.config. Has setup been run?';
    END IF;

    UPDATE guardian.config
    SET value = p_new_host, updated_at = now()
    WHERE key = 'guardian.replica_host';

    EXECUTE format(
        $F$
        CREATE OR REPLACE FUNCTION guardian.cron_job_wrapper()
        RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS $BODY$
        DECLARE v_connstr TEXT;
        BEGIN
            v_connstr := format(
                'host=%%s dbname=postgres user=guardian_monitor password=%%s sslmode=require connect_timeout=5 application_name=pg_query_guardian',
                %L, %L
            );
            PERFORM guardian.terminate_runaway_queries(v_connstr);
        END;
        $BODY$
        $F$,
        p_new_host, v_pw
    );

    RETURN format('[guardian] Replica host updated. old=%s new=%s. cron_job_wrapper recreated.',
        v_old_host, p_new_host);
END;
$$;

COMMENT ON FUNCTION guardian.update_replica_host IS
    'Updates the replica endpoint in guardian.config and recreates cron_job_wrapper(). '
    'Usage: SELECT guardian.update_replica_host(''new-replica.xxxx.rds.amazonaws.com'');';

-- =============================================================================
-- 21. HELPER: show_connection()
-- =============================================================================
-- Usage: SELECT * FROM guardian.show_connection();

CREATE OR REPLACE FUNCTION guardian.show_connection()
RETURNS TABLE (
    setting     TEXT,
    value       TEXT,
    updated_at  TIMESTAMPTZ
)
LANGUAGE sql
VOLATILE
SECURITY DEFINER
AS $$
    SELECT
        CASE key
            WHEN 'guardian.replica_host' THEN 'replica_host'
            WHEN 'guardian.monitor_pw'   THEN 'monitor_password'
            ELSE key
        END                                                    AS setting,
        CASE key
            WHEN 'guardian.monitor_pw' THEN '****** (hidden)'
            ELSE value
        END                                                    AS value,
        updated_at
    FROM guardian.config
    WHERE key IN ('guardian.replica_host', 'guardian.monitor_pw')
    ORDER BY key;
$$;

COMMENT ON FUNCTION guardian.show_connection IS
    'Shows current replica connection config. Password is masked. '
    'Usage: SELECT * FROM guardian.show_connection();';

-- =============================================================================
-- VERIFY
-- =============================================================================

-- Config (expect 6 rows)
SELECT key, value FROM guardian.config ORDER BY key;

-- pg_cron job (active = true)
SELECT jobid, jobname, schedule, active
FROM cron.job
WHERE jobname = 'pg_query_guardian';

-- Extensions (expect 2 rows: dblink + pg_cron)
SELECT extname, extversion
FROM pg_extension
WHERE extname IN ('dblink', 'pg_cron')
ORDER BY extname;

-- Exemptions table (expect 4 rows — built-in maintenance tools)
-- To see them: SELECT * FROM guardian.show_exemptions();
SELECT type, COUNT(*) AS rows
FROM guardian.exemptions
GROUP BY type
ORDER BY type;

-- All guardian functions (expect 13)
SELECT routine_name
FROM information_schema.routines
WHERE routine_schema = 'guardian'
ORDER BY routine_name;
