# pg_query_guardian

A self-contained PostgreSQL watchdog that automatically terminates runaway SELECT queries on AWS RDS read replicas. Built entirely in PL/pgSQL — no Lambda, no EC2, no external dependencies.

---

## Background

### The Problem

The business had a requirement to terminate any SELECT query running longer than 10 minutes on the PostgreSQL read replica. The solution at the time was a Python script scheduled on an EC2 instance that connected to the replica and called `pg_terminate_backend()` on long-running queries.

This worked — until it did not:

- **Replica renamed or endpoint changed** after a restore or maintenance event. The EC2 script had the old hostname hardcoded. It would fail silently, and most application teams had no visibility into that EC2 instance or its logs to notice.
- **PostgreSQL version upgrade** to a new major version changed internal views and system catalog behaviour. The script broke without anyone realising, leaving the replica unprotected.
- **EC2 dependency** meant the enforcement mechanism lived completely outside the database. Any EC2 restart, IAM permission change, or OS patch could silently disable query protection. There was no audit trail, no alerting, and no way to query "was this query killed and why?" from inside the database.

### The Solution

Move the entire mechanism inside PostgreSQL itself — on the primary where extensions can actually be installed. Use `pg_cron` to schedule the job, `dblink` to reach the replica, and a guardian schema to store configuration, exemptions, and an audit log — all queryable from SQL with no external tooling required.

When the replica endpoint changes, a single function call updates the config and regenerates the internal connection string. When PostgreSQL upgrades, the code lives in version-controlled SQL that can be reviewed and updated as part of the upgrade process. When a query is killed, the full context — who ran it, how long it ran, what the instance metrics were at the time — is logged permanently in a table on the primary.

---

## What It Does

Every minute, pg_query_guardian:

1. Reads thresholds from a config table on the primary
2. Connects to the read replica via dblink
3. Scans `pg_stat_activity` for runaway SELECT queries
4. Terminates offenders using `pg_terminate_backend()`
5. Logs every kill to an audit table on the primary

Two independent kill conditions:

| Condition | Trigger | Action |
|---|---|---|
| `RUNTIME_EXCEEDED` | Any SELECT running longer than `max_runtime_minutes` | Kill all such queries |
| `INSTANCE_RESOURCE_HIGH` | Instance pressure > `instance_threshold_pct` | Kill the single longest-running SELECT |

---

## Architecture

```
┌─────────────────────────────────── AWS VPC · ap-south-1 ───────────────────────────────────┐
│                                                                                              │
│  ┌──────────────── Primary RDS ────────────────┐    ┌──────────── Replica RDS (read-only) ──┐ │
│  │                                             │    │                                       │ │
│  │  pg_cron  ──► cron_job_wrapper()            │    │  pg_stat_activity                     │ │
│  │                    │                        │    │  pg_terminate_backend()               │ │
│  │                    ▼                        │    │  guardian_monitor role                │ │
│  │     terminate_runaway_queries()             │    │    (WAL-replicated from primary)      │ │
│  │          ├─ dblink SELECT ─────────────────►│───►│  pg_stat_activity                     │ │
│  │          └─ dblink KILL ───────────────────►│───►│  pg_terminate_backend()               │ │
│  │                    │                        │    │                                       │ │
│  │  ┌── guardian schema ──────────────────┐   │    │  Kill conditions:                     │ │
│  │  │  config  │  killed_queries          │   │    │  · RUNTIME_EXCEEDED                   │ │
│  │  │  exemptions (USER · APP · PATTERN)  │   │    │  · INSTANCE_RESOURCE_HIGH             │ │
│  │  └─────────────────────────────────────┘   │    │                                       │ │
│  │                    ▲                        │    └───────────────────────────────────────┘ │
│  │              audit INSERT (local)           │                                              │
│  └─────────────────────────────────────────────┘                                              │
│                          │                                                                   │
│                          └──────────── WAL replication ──────────────────────────────────►  │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

**How it works each minute:**

1. `pg_cron` on the primary fires `cron_job_wrapper()` — a zero-argument function with the replica endpoint and password baked into its body at install time
2. `cron_job_wrapper()` calls `terminate_runaway_queries(connstr)` with the dblink connection string
3. The function loads all exemptions in **one query** (single scan of `guardian.exemptions` with FILTER aggregation)
4. **Pass 1 — RUNTIME_EXCEEDED:** reads `pg_stat_activity` on the replica via dblink, kills every SELECT/WITH running longer than `max_runtime_minutes`
5. **Pass 2 — INSTANCE_RESOURCE_HIGH:** if instance pressure exceeds `instance_threshold_pct`, kills the single longest-running query not already killed in Pass 1
6. Every kill is logged to `guardian.killed_queries` via a direct local INSERT on the primary

**Why the primary runs everything:** RDS read replicas cannot install extensions because `CREATE EXTENSION` writes to system catalogs which are physically read-only (WAL-replicated). The audit log lives on the primary for the same reason.

**guardian_monitor role exists on both instances.** It is created on the primary with `CREATE ROLE`. WAL replication copies it to the replica automatically — no replica setup script needed. The role is used by dblink which connects *from* the primary *to* the replica, so the replica must have it as a valid login role with `pg_monitor` and `pg_signal_backend` grants.

**Key design decisions:**

- **pg_cron runs on the primary**, not the replica. RDS read replicas cannot install extensions because `CREATE EXTENSION` writes to system catalogs which are physically read-only (WAL-replicated from primary). This was a major architectural discovery during development.
- **Audit log lives on the primary** for the same reason — the replica cannot INSERT to local tables.
- **guardian_monitor role replicates automatically** — created on the primary, WAL replication carries it to the replica. No replica setup needed.
- **Single script installation** — everything deploys from one SQL file run on the primary.

---

## Tech Stack

| Component | Choice | Why |
|---|---|---|
| Database | AWS RDS PostgreSQL 17 | Target environment |
| Scheduler | pg_cron 1.6 | Native PostgreSQL scheduler, zero external infra |
| Cross-instance comms | dblink | Built into PostgreSQL, no extra setup |
| Instance type | db.t3.micro | Free tier / POC — burstable, 1GB RAM |
| Dataset | postgres_air (~30M rows) | Realistic workload for testing |
| Region | ap-south-1 (Mumbai) | Cost optimisation |
| Client tools | DBeaver + pgbench | GUI for SQL, CLI for load testing |

---

## Installation

### Prerequisites

- AWS RDS PostgreSQL 17 primary + read replica
- Parameter group with `pg_cron` in `shared_preload_libraries`
- DBeaver or psql access to primary
- Both instances must be in the same VPC (for dblink connectivity)

### Steps

**1. Download the setup script**

`01_primary_setup.sql`

**2. Edit the two connection values at the top**

```sql
v_replica_host  TEXT := 'your-replica.xxxx.ap-south-1.rds.amazonaws.com'; -- EDIT
v_monitor_pw    TEXT := 'choose-a-strong-password';                        -- EDIT
```

**3. Run on PRIMARY → postgres database**

Use DBeaver or psql with auto-commit enabled. The script is fully idempotent — safe to re-run.


**4. Verify**

```sql
-- Config (should show 5 rows)
SELECT key, value FROM guardian.config ORDER BY key;

-- pg_cron job (should show active=true)
SELECT jobid, jobname, schedule, active
FROM cron.job WHERE jobname = 'pg_query_guardian';

-- First run results (wait 2 minutes)
SELECT start_time, status, return_message
FROM cron.job_run_details
WHERE jobid = 1
ORDER BY start_time DESC
LIMIT 5;
```

---

## Configuration

All thresholds live in `guardian.config` on the primary. Changes take effect on the next pg_cron tick (within 60 seconds). No restarts, no code changes.

| Key | Default | Description |
|---|---|---|
| `max_runtime_minutes` | `10` | Kill any SELECT running longer than this |
| `instance_threshold_pct` | `80` | Kill longest SELECT when instance pressure exceeds this % |
| `dry_run` | `false` | Log kills without actually terminating |
| `guardian.replica_host` | set at install | Replica endpoint (used by helper functions) |
| `guardian.monitor_pw` | set at install | guardian_monitor password (used by helper functions) |

### Temporarily Disabling Guardian

**Option 1 — Dry run mode** (recommended)

Guardian continues running every minute — evaluating queries and logging what it would kill — but does not terminate anything. Use this during maintenance windows, after a PostgreSQL upgrade, or when onboarding a new replica and you want to observe before enforcing.

```sql
-- Disable kills, keep logging
UPDATE guardian.config SET value = 'true', updated_at = now()
WHERE key = 'dry_run';

-- Re-enable
UPDATE guardian.config SET value = 'false', updated_at = now()
WHERE key = 'dry_run';
```

**Option 2 — Unschedule the pg_cron job**

Guardian stops running entirely. Use when you need a complete pause.

```sql
-- Stop
SELECT cron.unschedule('pg_query_guardian');

-- Restart
SELECT cron.schedule('pg_query_guardian', '* * * * *', 'SELECT guardian.cron_job_wrapper()');
```

Both changes take effect immediately — no restart required.



```sql
-- Lower runtime threshold to 5 minutes
UPDATE guardian.config SET value = '5', updated_at = now()
WHERE key = 'max_runtime_minutes';

-- Enable dry-run mode (safe observation)
UPDATE guardian.config SET value = 'true', updated_at = now()
WHERE key = 'dry_run';

-- Tighten instance threshold to 70%
UPDATE guardian.config SET value = '70', updated_at = now()
WHERE key = 'instance_threshold_pct';
```

---

---

## Exemptions

Exemptions prevent guardian from killing specific users, applications, or queries — regardless of runtime or instance pressure. All exemptions take effect on the next pg_cron tick (within 60 seconds).

### Three Exemption Types

| Type | Best For | Requires App Change? |
|---|---|---|
| User | Service accounts, ETL users, DBA users | No |
| Application name | App queries with runtime variables | Yes — set `application_name` in connection string |
| Comment tag | Any query — prepend tag to query text | Yes — add comment to query |
| Query pattern | Queries with stable text prefix | No |

### Built-in Maintenance Tool Exemptions

The following tools are **pre-exempted automatically** at install time. They connect as regular client backends and issue SELECT queries — without these exemptions guardian would kill them at the runtime threshold.

| Application name | Tool | Why protected |
|---|---|---|
| `pg_dump` | PostgreSQL backup | Issues long SELECT on every table during backup |
| `pg_query_guardian` | Guardian own dblink session | Guardian's own connection to the replica |
| `pganalyze-collector` | pganalyze monitoring | Runs analytical queries against pg_stat_statements |

These are inserted with `ON CONFLICT DO NOTHING` — re-running the script never overwrites them. To remove one: `SELECT guardian.remove_exempt_app('pg_dump');`

> **Note:** `autovacuum`, WAL senders, and logical replication workers are automatically safe — they are not `client backend` processes and are filtered at the query level, not via the exemption tables.

After installing, add any monitoring agents specific to your environment:
```sql
SELECT guardian.add_exempt_app('datadog-agent',   'Datadog monitoring agent');
SELECT guardian.add_exempt_app('your-etl-tool',   'Internal ETL — check with data team');
```

### User Exemptions

```sql
SELECT guardian.add_exempt_user('analytics_user', 'BI team — reports run up to 2h');
SELECT guardian.remove_exempt_user('analytics_user');
```

### Application Name Exemptions (recommended for app queries)

The application sets `application_name` in its connection string. Guardian checks `pg_stat_activity.application_name` and skips matching queries regardless of query length or runtime variable values.

```sql
SELECT guardian.add_exempt_app('nightly_etl', 'ETL service — runs 90 min on weekends');
SELECT guardian.remove_exempt_app('nightly_etl');
```

How the application sets it:
```
JDBC:      jdbc:postgresql://host/db?applicationName=nightly_etl
psycopg2:  psycopg2.connect(..., application_name='nightly_etl')
libpq:     application_name=nightly_etl  (in connection string)
DBeaver:   Connection → PostgreSQL tab → Application name field
```

### Comment Tag Exemption

Prepend the configured tag to any query that should never be killed. Works for 100-line queries with runtime variables — only the first few characters are checked.

```sql
/* guardian_exempt */ SELECT order_id, customer_id, ...
FROM orders
WHERE created_at BETWEEN $1 AND $2   -- runtime variables, any length — doesn't matter
  AND region_id = $3
```

Default tag is `/* guardian_exempt */`. To change it:
```sql
UPDATE guardian.config SET value = '/* long_running_approved */', updated_at = now()
WHERE key = 'guardian_exempt_tag';
```

### Query Pattern Exemptions

Use ILIKE syntax with `%` as wildcard. Best for queries with stable text prefixes.

```sql
SELECT guardian.add_exempt_pattern('with monthly_summary%', 'Monthly CTE — 45 min');
SELECT guardian.remove_exempt_pattern(3);  -- id from show_exemptions()
```

> **Limitation:** Pattern matching breaks when runtime variables appear in the matched portion. Use application name or comment tag for queries that change on every run.

### See All Active Exemptions

```sql
SELECT * FROM guardian.show_exemptions();
```

---

## Kill Conditions — How They Work

### RUNTIME_EXCEEDED

Evaluates every SELECT in `pg_stat_activity` on the replica:

```sql
WHERE state       = 'active'
  AND query       ILIKE 'select%'
  AND usename     != 'rdsadmin'
  AND EXTRACT(EPOCH FROM (now() - query_start)) / 60 >= max_runtime_minutes
```

**Kills all matching queries per tick.** If 5 queries have each been running for 12 minutes and the threshold is 10, all 5 are terminated.

### INSTANCE_RESOURCE_HIGH

Two independent proxy metrics are computed on the replica each tick:

**CPU proxy:**
```
active_client_backends / max_connections * 100
```
Measures connection saturation. On db.t3.micro with max_connections=87: reaching 10% means ~9 simultaneous active queries.

**Memory proxy (PostgreSQL 17+):**
```
SUM(evictions) / (SUM(evictions) + SUM(writes) + SUM(extends)) * 100
FROM pg_stat_io WHERE backend_type = 'client backend'
```
Measures shared_buffers pressure. High eviction ratio means pages are being thrown out faster than they are written — a sign of memory pressure forcing disk spill.

> **Note:** `pg_stat_io` is cumulative since last stats reset. The eviction ratio reflects a long-running average, not an instantaneous snapshot. This makes the memory proxy a trend indicator rather than a precise real-time alarm. Requires PostgreSQL 17+ — `pg_stat_bgwriter.buffers_backend` was removed in PG17.

If **either** metric exceeds `instance_threshold_pct`, guardian kills the **single longest-running SELECT** (oldest `query_start`) that was not already killed in Pass 1.

---

## Monitoring

### Is guardian running?

```sql
-- Check pg_cron job is active
SELECT jobname, schedule, active
FROM cron.job
WHERE jobname = 'pg_query_guardian';

-- Check last 5 runs
SELECT start_time, status, return_message
FROM cron.job_run_details
WHERE jobid = (SELECT jobid FROM cron.job WHERE jobname = 'pg_query_guardian')
ORDER BY start_time DESC
LIMIT 5;
```

`status = succeeded` — ran cleanly. `return_message = SELECT 1` means no runaway queries found that tick. `status = failed` — check `return_message` for the error.

```sql
SELECT killed_at, kill_reason, pid,
       usename, runtime_minutes, terminated,
       query_preview
FROM guardian.v_recent_kills
ORDER BY killed_at DESC
LIMIT 20;
```

### Kill Summary

```sql
-- Last 24 hours (default)
SELECT * FROM guardian.kill_summary();

-- Last 7 days
SELECT * FROM guardian.kill_summary('7 days');

-- Last hour
SELECT * FROM guardian.kill_summary('1 hour');
```

Returns per-replica aggregates:

| Column | Description |
|---|---|
| `total_kills` | All queries terminated |
| `runtime_kills` | RUNTIME_EXCEEDED kills |
| `instance_kills` | INSTANCE_RESOURCE_HIGH kills |
| `avg_runtime_mins` | Average runtime of killed queries |
| `max_runtime_mins` | Longest query terminated |
| `successful_terminations` | pg_terminate_backend returned true |
| `failed_terminations` | Termination failed (query already finished) |

### Instance Pressure (run on replica)

```sql
-- CPU proxy
SELECT
    COUNT(*) AS active_backends,
    (SELECT setting::INT FROM pg_settings WHERE name = 'max_connections') AS max_conn,
    ROUND(COUNT(*)::NUMERIC /
        (SELECT setting::NUMERIC FROM pg_settings WHERE name = 'max_connections')
        * 100, 2) AS cpu_proxy_pct
FROM pg_stat_activity
WHERE state NOT IN ('idle','idle in transaction','idle in transaction (aborted)')
  AND backend_type = 'client backend'
  AND usename != 'rdsadmin';

-- Memory proxy (PostgreSQL 17+)
SELECT
    SUM(evictions) AS total_evictions,
    SUM(writes)    AS total_writes,
    ROUND(SUM(evictions)::NUMERIC /
        GREATEST(SUM(evictions) + SUM(writes) + SUM(extends), 1)
        * 100, 2) AS mem_proxy_pct
FROM pg_stat_io
WHERE backend_type = 'client backend'
  AND object = 'relation';
```

---

## Helper Functions

### Rotate Password

Use every 6 months or per your security policy.

```sql
SELECT guardian.rotate_password('NewStrongPassword2027');
```

What it does:
1. `ALTER ROLE guardian_monitor PASSWORD '...'` — updates role on primary, WAL replicates to replica
2. Updates `guardian.config` with new password
3. Recreates `cron_job_wrapper()` with new password baked in

### Update Replica Host

Use when the replica endpoint changes (restore from snapshot, rename, failover).

```sql
SELECT guardian.update_replica_host('new-replica.xxxx.ap-south-1.rds.amazonaws.com');
```

What it does:
1. Updates `guardian.config` with new host
2. Recreates `cron_job_wrapper()` with new host baked in

### Show Current Connection

```sql
SELECT * FROM guardian.show_connection();
```

Returns the current replica host and confirms a password exists (masked as `******`). Safe to run anytime.

---

## Testing

Load testing was done with [pgbench](https://www.postgresql.org/docs/current/pgbench.html) from a local Mac using Postgres.app.

### Test 1 — RUNTIME_EXCEEDED

```bash
# Create slow query script (full sequential scan of 30M rows)
cat > ~/slow_select.sql << 'EOF'
SELECT count(*), avg(pass_id) FROM postgres_air.boarding_pass;
EOF

# Run 5 concurrent clients for 3 minutes
pgbench \
  --host=your-replica-endpoint \
  --port=5432 \
  --username=postgres \
  --dbname=postgres_air \
  --client=5 --jobs=5 --time=180 \
  --no-vacuum --file=~/slow_select.sql
```

**Threshold used:** `max_runtime_minutes = 1`

**Result:** Queries killed at ~1.18 minutes runtime. pgbench reports `terminating connection due to administrator command` — the expected success signal.

### Test 2 — INSTANCE_RESOURCE_HIGH

```bash
# Create memory-intensive query (window functions on 30M rows)
cat > ~/hammer.sql << 'EOF'
SELECT
    passenger_id, booking_leg_id, pass_id, boarding_time,
    ROW_NUMBER() OVER (PARTITION BY passenger_id ORDER BY boarding_time DESC) AS rn,
    RANK()       OVER (PARTITION BY passenger_id ORDER BY boarding_time DESC) AS rnk,
    SUM(pass_id) OVER (PARTITION BY passenger_id) AS total_passes,
    AVG(pass_id) OVER (PARTITION BY passenger_id) AS avg_pass,
    COUNT(*)     OVER (PARTITION BY passenger_id) AS pass_count,
    LAG(boarding_time)  OVER (PARTITION BY passenger_id ORDER BY boarding_time) AS prev_boarding,
    LEAD(boarding_time) OVER (PARTITION BY passenger_id ORDER BY boarding_time) AS next_boarding
FROM postgres_air.boarding_pass
ORDER BY passenger_id, boarding_time DESC;
EOF

# Run 20 concurrent clients
pgbench \
  --host=your-replica-endpoint \
  --port=5432 \
  --username=postgres \
  --dbname=postgres_air \
  --client=20 --jobs=20 --time=300 \
  --no-vacuum --file=~/hammer.sql
```

**Threshold used:** `instance_threshold_pct = 10`

**Why this query is brutal on t3.micro:**
- 30M row full scan
- Multiple window function sorts over `passenger_id` partitions
- `work_mem = 4MB` forces massive disk spill to temp files
- 20 concurrent clients doing this simultaneously saturates both CPU and shared_buffers

**Result:** Memory proxy spiked to 96.46%. Guardian killed queries at 1-2 minutes runtime with `kill_reason = INSTANCE_RESOURCE_HIGH`.

### Test Results

```
kill_reason             | count
------------------------+-------
RUNTIME_EXCEEDED        |   12
INSTANCE_RESOURCE_HIGH  |    8

terminated = true       |   17
terminated = false      |    3  ← query finished before kill arrived (race condition, not a bug)
```

---

## Schema Reference

### guardian.exemptions

| Column | Type | Description |
|---|---|---|
| `id` | SERIAL | Auto-increment primary key |
| `type` | TEXT | `USER`, `APP`, or `PATTERN` — enforced by CHECK constraint |
| `value` | TEXT | The username, app name, or pattern text |
| `reason` | TEXT | Why this exemption exists — required, cannot be empty |
| `added_at` | TIMESTAMPTZ | When added |
| `added_by` | TEXT | Who added it (`current_user` at INSERT time) |

Unique index on `(type, lower(value))` — prevents duplicates within each type, case-insensitively.

### guardian.config

| Column | Type | Description |
|---|---|---|
| `key` | TEXT PK | Setting name |
| `value` | TEXT | Setting value |
| `description` | TEXT | Human-readable explanation |
| `updated_at` | TIMESTAMPTZ | Last change timestamp |

### guardian.killed_queries

| Column | Type | Description |
|---|---|---|
| `id` | BIGSERIAL PK | Auto-increment |
| `killed_at` | TIMESTAMPTZ | When guardian fired |
| `replica_host` | TEXT | Which replica (inet_server_addr) |
| `pid` | INTEGER | Terminated backend PID |
| `usename` | TEXT | PostgreSQL user |
| `application_name` | TEXT | Client application |
| `client_addr` | INET | Client IP |
| `backend_start` | TIMESTAMPTZ | When session connected |
| `query_start` | TIMESTAMPTZ | When query started |
| `query` | TEXT | Full query text |
| `query_duration` | INTERVAL | How long it ran |
| `runtime_minutes` | NUMERIC(10,2) | runtime_minutes at kill time |
| `kill_reason` | TEXT | RUNTIME_EXCEEDED or INSTANCE_RESOURCE_HIGH |
| `threshold_runtime_minutes` | NUMERIC | Threshold active at kill time |
| `threshold_instance_pct` | NUMERIC | Threshold active at kill time |
| `instance_cpu_pct` | NUMERIC | CPU proxy at kill time |
| `instance_mem_pct` | NUMERIC | Memory proxy at kill time |
| `terminated` | BOOLEAN | pg_terminate_backend result |
| `terminate_error` | TEXT | Error if termination failed |
| `log_note` | TEXT | Additional context |

---

## Roles

### guardian_monitor

Created on primary, auto-replicated to replica via WAL.

| Grant | Purpose |
|---|---|
| `CONNECT ON DATABASE postgres` | Connect to postgres database on replica |
| `pg_monitor` | Read `pg_stat_activity` on replica |
| `pg_signal_backend` | Call `pg_terminate_backend()` on replica |
| `CONNECTION LIMIT 3` | Prevent connection exhaustion |

---

## Project Structure

```
pg_query_guardian/
│
├── CLAUDE.md                 ← project context for Claude Code (VSCode)
├── README.md                 ← this file
│
└── sql/
    ├── 01_primary_setup.sql  ← install (run on primary)
    └── cleanup_primary.sql   ← uninstall
```

**No replica script needed.** Everything runs on the primary. The `guardian_monitor` role replicates to the replica automatically via WAL.

---

## Known Limitations

**Only kills SELECT queries.** INSERT, UPDATE, DELETE, and CALL statements are excluded by design. On a read replica, only SELECT is possible anyway — but this is worth noting for any future adaptation to primary instances.

**guardian_monitor cannot kill superuser sessions.** `pg_terminate_backend()` on a superuser process requires superuser privileges. Affected kills will show `terminated = false` with an appropriate error message.

**Race condition on fast queries.** If a query finishes between when guardian reads `pg_stat_activity` and when it calls `pg_terminate_backend()`, the kill returns false. This appears as a failed termination in the audit log but is not a real failure — the query already completed.

**Memory proxy is cumulative.** `pg_stat_io` is cumulative since last stats reset. On a busy replica with lots of historical I/O, the eviction ratio may be low even during a current spike. This makes the memory proxy better as a trend indicator than an instant snapshot.

**Single replica.** The current design targets one replica. Supporting multiple replicas would require one `cron_job_wrapper()` per replica or a dynamic dispatch approach.

---

## Grafana Dashboard

A Grafana dashboard running on EC2 provides real-time visibility into guardian activity and replica health. It connects directly to the read replica using the `guardian_monitor` role — no Prometheus required.

**Guardian Kills** — total kills, kills over time, kill reasons, top offending users and applications, and a recent kills audit table.

**Replica Health** — active connections, longest running query, connections by state, and a live active queries list.

![Guardian Kills](assets/Dashboard%201.png)

![Replica Health](assets/Dashboard%202.png)

> See [docs/grafana-setup.md](docs/grafana-setup.md) for full setup instructions (gitignored, not pushed to GitHub).

---

## Future Improvements

- **AWS Secrets Manager integration** for password management instead of storing in `guardian.config`
- **Alerting** — SNS notification when a kill occurs
- **Multi-replica support** — dynamic target list from a replicas config table
- **Prometheus metrics** — expose kill counts and instance pressure as metrics

---

## Lessons Learned

**RDS read replicas are more read-only than you think.** `CREATE EXTENSION` writes to system catalogs. System catalogs are WAL-replicated. Therefore `CREATE EXTENSION` fails on RDS replicas with `cannot execute CREATE EXTENSION in a read-only transaction`. This forced the entire architecture to shift — pg_cron had to move to the primary.

**PostgreSQL 17 changed pg_stat_bgwriter.** Columns `buffers_backend` and `buffers_checkpoint` were removed and moved to `pg_stat_io`. Any code relying on these columns must be updated for PG17+.

**DBeaver wraps statements in transactions differently than psql.** `\echo` commands fail in DBeaver. With auto-commit off, a function error rolls back the entire script including previously successful DDL. Always enable auto-commit in DBeaver for setup scripts.

**`CREATE OR REPLACE FUNCTION` cannot change the return type.** Renaming output columns (to fix ambiguity with `RETURNS TABLE`) requires `DROP FUNCTION` first. PostgreSQL treats the return type as part of the function signature.

---

## References

- [postgres_air dataset](https://github.com/hettie-d/postgres_air)
- [pg_cron documentation](https://github.com/citusdata/pg_cron)
- [AWS RDS PostgreSQL parameter groups](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html)
- [pg_stat_io (PostgreSQL 17)](https://www.postgresql.org/docs/17/monitoring-stats.html#MONITORING-PG-STAT-IO-VIEW)
- [dblink documentation](https://www.postgresql.org/docs/current/dblink.html)

---

## Author

Built as a weekend project to solve a real production problem with AWS RDS PostgreSQL read replicas. Every architectural decision was driven by actual constraints encountered during development — particularly the RDS read-only catalog limitation that forced the entire design to run from the primary.

Developed with assistance from [Claude Code](https://claude.ai/code) (Anthropic) for iterative SQL development, design review, and documentation.
