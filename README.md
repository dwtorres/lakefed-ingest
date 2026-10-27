# lakefed_cursor

`lakefed_cursor` copies tables from external databases into Delta tables on Databricks. A control
row in Lakebase describes each source table. One job task ingests a whole `task_collection` with a
thread pool on a job cluster. No SQL warehouse is needed.

The engine supports two load types. `cursor` reads the rows above a high watermark and merges them
by primary key. `full` reloads the whole table on every run. SQL Server CT and CDC are not
supported. Sources: SQL Server, Oracle, PostgreSQL, MySQL. `cursor` runs on SQL Server, Oracle and
PostgreSQL. `cursor` on MySQL fails at plan time.

Names: bundle `lakefed_cursor`, jobs `lakefed_cursor` and `lakefed_cursor_setup`, Lakebase schema
`lakefed_cursor` (schema version 2), secret scope `lakefed-cursor`, UC catalog
`lakefed_cursor_lakebase`. No resource reuses a `lakefed_ingest` or `edp` name. The deploy gate,
`scripts/validate.sh`, fails on such a name.

## Why one task per collection

A workspace allows 2,000 concurrent task runs and 10,000 run submits per hour. A task per table
stalls at that limit. One task per `task_collection` runs its tables in-process with a thread pool
and Spark FAIR scheduler pools. Throughput is then bound by cluster compute. A table failure is
isolated and recorded in Lakebase.

To scale, shard tables into collections that one cluster ingests inside the cadence window. Run
collections concurrently, one run per collection on its own job cluster. A per-collection Lakebase
advisory lock makes a duplicate run of the same collection exit as `skipped`.

## Layout

```
databricks.yml                     # bundle lakefed_cursor, variables, targets
scripts/validate.sh                # deploy gate: name test, then bundle validate
resources/
  lakefed_cursor.yml               # ingestion job: one run ingests a task_collection
  lakefed_cursor_setup.yml         # Lakebase instance, UC catalog, secret scope, schema job
src/lakefed_ingest/
  ingest.ipynb                     # the ingestion notebook: plan, then fan out per table
  planner.py                       # route each control row; apply the batched capture (pure)
  worker.py                        # per-table worker: read, write, checkpoint, telemetry
  cursor_engine.py                 # cursor strategy: expression check, high watermark, window read
  source_adapters.py               # per-engine dialect: URL, driver, quoting, literals, cursor SQL
  load_strategies.py               # load types cursor and full; the support matrix
  source_io.py                     # source reads: Spark JDBC or remote_query; credentials
  sink_writer.py                   # sink writes: MERGE, seed, full reload, delete, write retry
  parallel.py                      # thread pool, FAIR pools, WorkSlots budget, SourceLimiter
  partitions.py                    # partitioned-seed planning
  table_retry.py                   # table retries after a transient source error
  state_store.py                   # Lakebase accessor: control, checkpoints, telemetry, locks
  lakebase_schema.sql              # Postgres DDL, idempotent, schema version 2
  apply_lakebase_schema.ipynb      # applies the DDL
notebooks/load_control_example.ipynb   # insert control rows
tools/source_profile.sql           # read-only SQL Server profiling script
docs/deploy.md, docs/monitoring.md, docs/lakebase_schema.md
specs/001-cursor-only-engine/      # spec, plan, contracts, quickstart
tests/                             # unit tests, no cluster; synthetic SQL Server database
```

## How a run works

One run of job `lakefed_cursor` ingests one `task_collection`.

1. The run takes the collection lock in Lakebase. A duplicate run exits as `skipped`.
2. The planner loads the control rows and checkpoints and routes each table. `full` reloads.
   `cursor` seeds, reseeds, or increments. A row with an unsupported (`src_type`, `load_type`) pair
   fails at stage `plan`.
3. One batched `MAX` query per source database captures the high watermark `hi` of every seeded
   cursor table. Tables with `hi` equal to the checkpoint are quiet. Their checkpoints advance in one
   Lakebase write. No source read and no Delta commit happens for them.
4. Seeds, reseeds, reloads and changed tables run in parallel. Each worker reads once, writes to
   Delta, and then advances the checkpoint.
5. The run writes per-table events and a run summary to Lakebase telemetry.

### Sources and load types

`control.src_type` selects a source adapter in `source_adapters.py`. `control.load_type` selects a
load strategy in `load_strategies.py`.

| `src_type` | `cursor` | `full` |
|---|---|---|
| `sqlserver` | yes | yes |
| `oracle` | yes | yes |
| `postgresql` | yes | yes |
| `mysql` | no, fails at plan | yes |

- `full` reads the table once into a data-only `INSERT OVERWRITE`. The table id, grants and
  properties stay. With `load_partitioned` the reload uses the staged swap. No primary key is
  needed. The checkpoint records `strategy = 'full'`, phase `loaded`, and no position.
- Drivers: Databricks Runtime ships the SQL Server and PostgreSQL JDBC drivers. Oracle and MySQL
  need a cluster library. See the `libraries` comments in `resources/lakefed_cursor.yml`.
- Oracle: `src_database` is the service name. `src_schema` is the owner. Identifiers are quoted, so
  use the stored upper-case names. The partitioned seed sizes the table from `ALL_TABLES`.
- MySQL: `src_schema` is the database.
- `jdbc_options` is an optional JSON object of extra Spark JDBC read options, for example
  `{"fetchsize": "50000"}`. Connection, credential, driver and query options cannot be overridden.
- A row with `use_remote_query = true` reads through the Unity Catalog connection in
  `src_connection` with `remote_query`. The connection holds the credentials. The row needs no
  `secret_key`, `src_host` or `src_port`. The job identity needs `USE CONNECTION` and `MANAGE` on
  the connection. `remote_query` needs Databricks Runtime 18.3 or later. Keep each sink on one read
  path, because Spark JDBC and `remote_query` map source types differently.

### Credentials

One secret per source database lives in scope `lakefed-cursor`. `control.secret_key` names it. The
value is JSON. `auth_mode` selects the shape. A missing `auth_mode` means `sql`.

| `auth_mode` | Fields | Engine |
|---|---|---|
| `sql` | `user`, `password` | all |
| `ntlm` | `user`, `password`, `domain` | `sqlserver` |
| `aad_sp` | `client_id`, `client_secret`, `tenant_id` | `sqlserver` |

A missing field fails the table before any connect attempt. The message names the field, never the
value. `ntlm` or `aad_sp` on a source other than SQL Server fails at plan time. Log lines and
telemetry never contain a password or a client secret. See
`specs/001-cursor-only-engine/contracts/secret_json.md`.

### Cursor tables

A cursor table has a primary key and one cursor expression. `cursor_expression` is one SQL
expression in the source dialect over one to three columns of one type. Fold several columns with
`GREATEST` over `COALESCE` inputs:

```sql
GREATEST(COALESCE(UPDATED_AT, DATE '1900-01-01'), COALESCE(CREATED_AT, DATE '1900-01-01'))
```

`cursor_kind` names the clock of the expression.

| `cursor_kind` | Meaning | Reads |
|---|---|---|
| `rowversion` | one SQL Server `rowversion` column | exact bounds. `hi` is capped below `MIN_ACTIVE_ROWVERSION()` |
| `db_timestamp` | a timestamp the database sets | lookback window |
| `app_timestamp` | a timestamp the application sets | lookback window |
| `identity` | an identity or sequence number | exact bounds. Inserts only |

The engine checks the expression before any source read. The check fails a table in four cases:

- The expression has zero or more than three columns.
- The expression uses a function other than `GREATEST` or `COALESCE`.
- The columns have different types.
- An input column is nullable and `null_cursor_policy` is `reject`.

If an input column is nullable, set `null_cursor_policy = 'seed_only'`. The seed then copies a row
with a NULL expression. No increment reads that row again.

**Seed.** The worker captures `hi` before the full copy. If `load_partitioned` is set, the copy is
partitioned. The checkpoint then records `hi` and phase `incremental`. The first
increment reads the rows written during the seed again.

**Increment.** The window is `expression > lo AND expression <= hi`. For a temporal kind, `lo` is
the checkpoint minus `cursor_lookback_s` seconds. The job parameter gives the default, 3600. The
control row column `cursor_lookback_s` overrides it. A late commit can land inside the lookback window. For this reason a temporal increment
always runs, also for an unchanged `hi`. For `identity` and `rowversion`,
`lo` is the checkpoint. The read is one source query. An upsert MERGE by primary key applies it. If the cursor
expression of a matched row did not change, the MERGE does not update the row. The checkpoint advances to `hi` after
the MERGE commits.

**Skew.** A temporal `hi` is capped at the source clock plus `cursor_skew_s` seconds, default 300.
One row stamped in the future cannot pin the window. The run logs `alert=max_above_clock` and
writes it to the checkpoint detail.

**Identity.** An identity value is taken at insert time, not at commit time. A late commit below a
read `hi` is lost. The kind is inserts-only. The run logs the note `inserts_only` once per table.

**Reseed.** A changed `cursor_expression` or `cursor_kind` reseeds the table. Added columns in
`select_list` reseed the table. The position holds the expression text, so the engine detects the
change on the next run.

**Failure.** A `hi` below the checkpoint fails the table. The position never moves backwards. This
happens after a source truncate or reload.

Supported column types by class:

- numeric: integer, `decimal`, `numeric`, Oracle `NUMBER` with scale 0
- temporal: `date`, `datetime`, `datetime2`, `smalldatetime`, `datetimeoffset`, Oracle `DATE` and `TIMESTAMP`, PostgreSQL `timestamp`
- rowversion: SQL Server `rowversion`

Float, real and string columns are rejected. A SQL Server
`datetime` literal carries three fractional digits. SQL Server below 2022 has no `GREATEST`, so the
adapter renders a `CASE` form.

### Deletes

A cursor never captures a delete. `delete_mode` adds one.

| `delete_mode` | Behavior |
|---|---|
| `none` | default. Deleted source rows stay in the sink |
| `pk_snapshot` | after an increment, read every source primary key and delete the sink rows absent from it |
| `soft_column`, `full_compare` | reserved. Fail at plan time |

If `last_delete_snapshot_at` is NULL or older than `delete_min_interval_s` seconds, the snapshot
runs. A NULL interval means 86400. The snapshot is client-scoped on a consolidated sink. It runs at stage
`snapshot` and records `last_delete_snapshot_at`.

### Quiet tables

Most tables do not change between runs. One batched `MAX` query per source database and cursor type
reads `hi` for `detection_chunk` tables at once, default 200. The query also reads the source clock
for temporal types. A failed chunk falls back to one `MAX` per table. The run records one
`ingest_event` with action `capture` per source database. Quiet tables advance in one bulk Lakebase
write. `freshness_interval_s` skips that write for tables stamped more recently than this many
seconds.

### Partitioned seed

Set `load_partitioned = true`, `partition_col` and `partition_size_mb`. `partition_col` is numeric,
date or datetime, often the integer primary key. The seed computes
`num_partitions = max(size_mb / partition_size_mb, 2)` and reads range predicates in parallel into a
staging table. NULL values land in the first partition. When every partition succeeds, one atomic
Delta commit replaces the target from staging. Grants, row filters and lineage stay. A failed seed
leaves the target unchanged.

### Consolidated sinks

By default each control row owns its sink table. To share one sink across many source databases,
set `control.client_id` on each row and point them at the same sink.

- Every row carries the `client_id` value in the sink column `__client_id__`. The sink is created
  with liquid clustering on it.
- The MERGE matches on `__client_id__` and the primary key. The delete snapshot deletes only that
  client's rows.
- Seeds and full reloads replace only that client's rows with `REPLACE WHERE __client_id__ = ...`.
  A partitioned seed stages in a per-client table.
- Schema evolution absorbs column drift between clients.
- Checkpoints stay per control row.
- `client_id` must match `^[A-Za-z0-9_.:-]{1,128}$`. `__client_id__` must not be in `select_list`.
- `client_attrs` is an optional JSON object `{column: value}`. The engine writes each value as a
  STRING column on every row.

A unique index allows one row per (sink, `client_id`). A sink with a dedicated row and consolidated
rows fails those rows at stage `plan`.

Concurrent writers can hit a Delta conflict. A conflict retries the whole unit with exponential
backoff and full jitter. Defaults: 6 attempts, 1 s base, 60 s cap. Environment variables
`LAKEFED_WRITE_RETRY_ATTEMPTS`, `LAKEFED_WRITE_RETRY_BASE_S` and `LAKEFED_WRITE_RETRY_MAX_S` change
them.

### Table retries

Set job parameter `table_retries` above 0 to retry a table in the same run after a transient source
error. Transient errors are a refused or dropped connection, a timeout, or Azure SQL throttling.
Permanent errors fail at once. An unknown error is permanent.

- The pause doubles with each attempt, with jitter. Defaults: 10 s base, 120 s cap. Environment
  variables `LAKEFED_TABLE_RETRY_BASE_S` and `LAKEFED_TABLE_RETRY_MAX_S` change them. A waiting
  table holds no slot.
- `table_retry_budget` caps the retries of one run, default 20. A source server stops retrying for
  the rest of the run after 5 failed retries. Environment variable `LAKEFED_TABLE_RETRY_PER_SOURCE`
  changes it.
- Each attempt is one `ingest_event` row. A retried attempt ends as `retried`. `retry_count`
  numbers the attempts. The run `detail` holds the totals under `table_retries`.
- The next attempt is planned from the fresh checkpoint. Every write is idempotent, so a retry
  never duplicates rows.

### Concurrency

`parallelism` is the one concurrency budget of a run. It counts units of query work: a cursor read
plus MERGE, a seed, one partition, one small source query, the staging swap. Default 32.
`max_queries_per_source` caps the concurrent queries per source server across every run. Each run
takes its share, the cap divided by the runs reading the server, at least 1. 0 means no cap.

### Job parameters

Each tuning parameter defaults to the bundle variable of the same name in `databricks.yml`. A
target can set the variable. A run can override the parameter.

| Parameter | Default | Meaning |
|---|---|---|
| `task_collection` | `example_cohort` | the collection this run ingests |
| `db_instance` | `lakefed-cursor` | Lakebase instance |
| `db_name` | `databricks_postgres` | Lakebase database |
| `secret_scope` | `lakefed-cursor` | scope of the source secrets |
| `parallelism` | `32` | concurrent query units |
| `trust_server_certificate` | `true` | trust the source TLS certificate without validation |
| `cursor_lookback_s` | `3600` | lookback for temporal cursors. The control row overrides it |
| `cursor_skew_s` | `300` | seconds `hi` can exceed the source clock |
| `detection_chunk` | `200` | tables per batched `MAX` query |
| `max_seeds_per_run` | `0` | seeds per run. 0 means no limit |
| `freshness_interval_s` | `0` | skip the bulk advance for tables stamped inside this many seconds |
| `max_queries_per_source` | `0` | concurrent queries per source server across runs. 0 means no cap |
| `table_retries` | `0` | extra attempts per table. 0 means off |
| `table_retry_budget` | `20` | retries per run |

## Metadata (Lakebase)

All state lives in Lakebase schema `lakefed_cursor`. File `src/lakefed_ingest/lakebase_schema.sql`
holds the DDL.

| Table | Holds |
|---|---|
| `control` | one row per (source database, table): source, secret key, sink, cursor, deletes |
| `checkpoint` | per-table state: strategy, position, phase, status, freshness, last delete snapshot |
| `ingest_run`, `ingest_event` | run and table telemetry |
| `schema_version` | the schema versions applied |

Column reference and the checkpoint lifecycle: [docs/lakebase_schema.md](docs/lakebase_schema.md).

The position is JSON `{"expr", "kind", "type", "value"}`. `expr` is the expression text at the
last seed. `kind` is the `cursor_kind`. `type` is the source type of the expression result. `value`
is digits for numeric, ISO 8601 for temporal, a decimal integer for rowversion. See
`specs/001-cursor-only-engine/contracts/position.md`.

The database records its schema version. The first run whose code needs a newer version applies the
DDL in one transaction before it starts. Job `lakefed_cursor_setup` applies it ahead of a run. The
migration needs the schema owner. No data migrates from a `lakefed_ingest` deployment.

The Lakebase database is registered in Unity Catalog as `lakefed_cursor_lakebase`. Query the
tables from DBSQL. See [docs/monitoring.md](docs/monitoring.md).

### Control rows

Key columns by group:

- grouping: `task_collection`, `task_enabled`, `tier`, `priority`
- source: `src_type`, `src_host`, `src_port`, `secret_key`, `src_connection`, `use_remote_query`, `src_database`, `src_schema`, `src_table`, `jdbc_options`
- read: `primary_key`, `select_list`, `load_type`, `load_partitioned`, `partition_col`, `partition_size_mb`
- sink: `sink_catalog`, `sink_schema`, `sink_table`, `client_id`, `client_attrs`
- cursor: `cursor_expression`, `cursor_kind`, `cursor_lookback_s`, `null_cursor_policy`
- deletes: `delete_mode`, `delete_min_interval_s`

`load_type` defaults to `cursor`. A `cursor` row needs `primary_key`, `cursor_expression` and
`cursor_kind`. A `full` row has no cursor columns. CHECK constraints reject other combinations. See
`specs/001-cursor-only-engine/contracts/control_row.md` and `notebooks/load_control_example.ipynb`.

Writes use schema evolution. A column added to `select_list` is added to the sink, and the table
reseeds. New sinks get `delta.enableTypeWidening`. For an older sink run
`ALTER TABLE <sink> SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')`.

## Prerequisites

| Prerequisite | How |
|---|---|
| Lakebase instance, database, UC catalog | bundle resources in `resources/lakefed_cursor_setup.yml`, or adopt existing ones with `bundle deployment bind` |
| Schema | job `lakefed_cursor_setup`. An upgrade migrates on the next run |
| Lakebase auth | SDK-minted OAuth token at run time. The run identity must be a Postgres role on the instance |
| Cluster to Lakebase network path, port 5432 | the workspace IP access list must allow cluster egress, or use Private Link |
| Source credentials | one JSON secret per source database in scope `lakefed-cursor`, or a UC connection for `use_remote_query` rows |
| Cluster to source network path | SQL Server 1433, Oracle 1521, PostgreSQL 5432, MySQL 3306 |
| Oracle or MySQL JDBC driver | uncomment the library in `resources/lakefed_cursor.yml` |

WARNING: a deploy of `lakefed_cursor_setup.yml` creates a billable Lakebase instance unless you bind
an existing one.

## Deploy and run

Run every command from the repo root. Choose the profile yourself. See [docs/deploy.md](docs/deploy.md).

1. Check the names and the bundle.

   ```bash
   scripts/validate.sh -t <target> -p <profile>
   ```

2. Deploy the bundle.

   ```bash
   databricks bundle deploy -t <target> -p <profile>
   ```

3. Create the Lakebase schema.

   ```bash
   databricks bundle run lakefed_cursor_setup -t <target> -p <profile>
   ```

4. Store one secret per source database. Use a hidden prompt. Do not paste a secret into a notebook.

   ```bash
   databricks secrets put-secret lakefed-cursor <secret_key> -p <profile>
   ```

5. Insert control rows with `notebooks/load_control_example.ipynb`.

6. Run the ingestion.

   ```bash
   databricks bundle run lakefed_cursor -t <target> -p <profile> --params task_collection=<collection>
   ```

The first run seeds. Later runs increment cursor tables and reload full tables. The job ships with a
paused 15-minute schedule. A schedule cannot pass parameters, so it ingests only the default
collection. Trigger other collections with `run-now` and `job_parameters`.

## Live proof

The SQL Server live proof is on hold, because no SQL Server source is available. The Oracle live proof
runs in Phase 10 of `specs/001-cursor-only-engine/tasks.md`. Results go to `tasks/todo.md`.
`docs/baseline.md` records the Lakeflow Connect comparison baseline.

## Tests

```bash
.venv/bin/pytest -rs tests
```

Unit tests need no cluster. A skipped test is a failure. Every cursor test runs against the SQL
Server, Oracle and PostgreSQL adapters. `tests/synthetic/` builds a synthetic SQL Server database
for a live test. See [tests/synthetic/README.md](tests/synthetic/README.md).

## Not supported

SQL Server CT, CDC, append-mode sinks, `cursor` on MySQL, `delete_mode` `soft_column` and
`full_compare`, production alerting.

## Help

Databricks support does not cover this project. Open a GitHub issue for questions or bugs.

## License

&copy; 2026 Databricks, Inc. All rights reserved. The source in this repository is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| library | description | license | source |
|---|---|---|---|
| certifi | Mozilla CA bundle (live SQL Server validation tests) | MPL-2.0 | https://pypi.org/project/certifi/ |
| databricks-sdk | Databricks SDK for Python | Apache-2.0 | https://pypi.org/project/databricks-sdk/ |
| Microsoft JDBC Driver for SQL Server | SQL Server JDBC driver (included in Databricks Runtime) | MIT | https://github.com/microsoft/mssql-jdbc |
| MySQL Connector/J | MySQL JDBC driver (optional cluster library) | GPL-2.0 with Universal FOSS Exception | https://github.com/mysql/mysql-connector-j |
| Oracle JDBC driver (ojdbc11) | Oracle JDBC driver (optional cluster library) | Oracle Free Use Terms and Conditions | https://www.oracle.com/downloads/licenses/oracle-free-license.html |
| pandas | Data manipulation and analysis (table profiler notebook) | BSD-3-Clause | https://pypi.org/project/pandas/ |
| PostgreSQL JDBC Driver | PostgreSQL JDBC driver (included in Databricks Runtime) | BSD-2-Clause | https://github.com/pgjdbc/pgjdbc |
| psycopg | PostgreSQL database adapter (v3), for Lakebase | LGPL-3.0 | https://pypi.org/project/psycopg/ |
| pyspark | Apache Spark Python API (provided by Databricks Runtime) | Apache-2.0 | https://pypi.org/project/pyspark/ |
| pytest | Testing framework | MIT | https://pypi.org/project/pytest/ |
| python-tds | SQL Server TDS client (live SQL Server validation tests) | MIT | https://pypi.org/project/python-tds/ |
