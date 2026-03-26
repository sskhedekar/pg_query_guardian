# pg_query_guardian

PostgreSQL automated query termination for AWS RDS read replicas.

## Architecture

- `pg_cron` on PRIMARY fires every minute → `cron_job_wrapper()` → `terminate_runaway_queries(connstr)`
- `terminate_runaway_queries()` connects to REPLICA via dblink:
  - Reads `pg_stat_activity` (SELECT/WITH queries only)
  - Calls `pg_terminate_backend()` on runaway queries
  - Writes audit log via local INSERT on primary (no dblink needed)
- `guardian_monitor` role created on primary, WAL-replicated to replica automatically
- PostgreSQL 17 on AWS RDS ap-south-1

## Key files

- `sql/01_primary_setup.sql` — full install, run on primary only
- `sql/cleanup_primary.sql` — full uninstall

## Schema

```
guardian.config          — 6 rows: thresholds, dry_run, replica_host, monitor_pw, exempt_tag
guardian.killed_queries  — audit log, 3 indexes: killed_at DESC, usename, application_name
guardian.exemptions      — unified table: type = USER | APP | PATTERN
                           unique index on (type, lower(value))
```

## Kill conditions

1. `RUNTIME_EXCEEDED` — query running > `max_runtime_minutes` (default 10)
2. `INSTANCE_RESOURCE_HIGH` — instance pressure > `instance_threshold_pct` (default 80%)
   - CPU proxy: active client backends / max_connections * 100
   - MEM proxy: pg_stat_io eviction ratio (PostgreSQL 17+ only)

## Exemption loading (performance critical)

`terminate_runaway_queries()` loads all exemptions in **one query** using FILTER aggregation:

```sql
SELECT
    string_agg(quote_literal(value), ',') FILTER (WHERE type = 'USER')    AS user_quoted,
    string_agg(quote_literal(value), ',') FILTER (WHERE type = 'APP')     AS app_quoted,
    string_agg(format('query ILIKE %L', value), ' OR ') FILTER (WHERE type = 'PATTERN') AS pattern_ilike,
    COUNT(*) FILTER (WHERE type = 'USER'),
    COUNT(*) FILTER (WHERE type = 'APP'),
    COUNT(*) FILTER (WHERE type = 'PATTERN')
FROM guardian.exemptions;
```

Single scan, 1 page read, 1 execution plan. Replaces the old 6-query approach across 3 tables.

## Built-in exemptions (pre-populated at install)

| value | reason |
|---|---|
| `pg_dump` | Runs long SELECTs during backup |
| `pg_repack` | Runs SELECT + WITH CTEs for table reorganisation |
| `pg_query_guardian` | Guardian's own dblink session on replica |
| `pganalyze-collector` | Runs analytical queries against pg_stat_statements |

Note: `pg_restore` and `pg_basebackup` were intentionally excluded:
- `pg_restore` targets a writable instance — never runs on a read replica
- `pg_basebackup` uses WAL streaming (walsender process) — guardian never sees it

## Helper functions (13 total)

```sql
SELECT guardian.add_exempt_user('username', 'reason');
SELECT guardian.remove_exempt_user('username');
SELECT guardian.add_exempt_app('app_name', 'reason');
SELECT guardian.remove_exempt_app('app_name');
SELECT guardian.add_exempt_pattern('select%from reporting%', 'reason');
SELECT guardian.remove_exempt_pattern(3);            -- id from show_exemptions()
SELECT guardian.show_exemptions();
SELECT guardian.kill_summary('24 hours');
SELECT guardian.rotate_password('NewPassword');
SELECT guardian.update_replica_host('new-endpoint.rds.amazonaws.com');
SELECT * FROM guardian.show_connection();
-- terminate_runaway_queries() and cron_job_wrapper() called internally by pg_cron
```

## Monitoring queries

```sql
-- Recent kills
SELECT * FROM guardian.v_recent_kills LIMIT 20;

-- Is guardian running?
SELECT start_time, status, return_message
FROM cron.job_run_details
WHERE jobid = (SELECT jobid FROM cron.job WHERE jobname = 'pg_query_guardian')
ORDER BY start_time DESC LIMIT 5;

-- Kill summary
SELECT * FROM guardian.kill_summary('24 hours');
```

## Key design decisions

- `pg_cron` cannot run on RDS read replica — `CREATE EXTENSION` writes to system catalogs which are WAL-replicated (read-only on replica). This forced pg_cron to the primary.
- Audit log on primary — same reason, replica cannot INSERT to local tables.
- `guardian_monitor` created on primary with `pg_monitor` + `pg_signal_backend`, WAL-replicates to replica.
- `sslmode=require` in all dblink connection strings — required for RDS with `rds.force_ssl=1`.
- `quote_literal()` for all dynamic SQL values — prevents SQL injection into dblink queries.
- `action` column in `killed_queries` — distinguishes `DRY_RUN` from `KILLED` in reporting.
- Dollar-quoting levels in `cron_job_wrapper`: `$F$` outer, `$BODY$` inner, `%%s` for embedded format placeholders.

## Disabling / pausing guardian

```sql
-- Option 1: dry run (keeps running, logs what would be killed, terminates nothing)
UPDATE guardian.config SET value = 'true', updated_at = now() WHERE key = 'dry_run';
-- Re-enable
UPDATE guardian.config SET value = 'false', updated_at = now() WHERE key = 'dry_run';

-- Option 2: stop completely
SELECT cron.unschedule('pg_query_guardian');
-- Restart
SELECT cron.schedule('pg_query_guardian', '* * * * *', 'SELECT guardian.cron_job_wrapper()');
```

## Git conventions

- Commit style: conventional commits (`feat:`, `fix:`, `docs:`, `refactor:`)
- Each SQL change = separate commit with clear message
