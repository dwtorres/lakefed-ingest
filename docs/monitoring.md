# Observability

The ingestion job ingests a whole `task_collection` inside one job task. The Jobs UI shows no
per-table task. The run writes per-table telemetry to Lakebase instead. The Lakebase database is
registered as the Unity Catalog catalog `lakefed_cursor_lakebase` by the `database_catalogs` resource
in `lakefed_cursor_setup.yml`. Query the telemetry from DBSQL. No sync job and no copy is needed.
Writes go to Lakebase as point writes from the run. Reads go through UC federation.

## Tables (Lakebase, UC path `lakefed_cursor_lakebase.lakefed_cursor`)

If your target overrides the `lakebase_catalog` bundle variable, use that catalog name in the
queries below.

| Table | Grain | Written by |
| --- | --- | --- |
| `ingest_run` | one row per run | `start_run` at launch, `finish_run` at the end |
| `ingest_event` | one row per (run, table) | the worker: `record_event_start`, then `record_event_finish` |
| `checkpoint.last_success_at` | per source table | stamped on each successful event, for freshness and lag |

`ingest_event` carries these columns:

- `action`: `seed`, `increment`, `reseed`, `full`, or `capture`.
- `stage`: how far the table got. `plan`, `capture`, `ensure_sink`, `read`, `seed`, `merge`, `load`,
  `snapshot`, `checkpoint`. On a failure, this is where it failed. Stage `plan` means the control row
  is invalid, for example an unsupported (`src_type`, `load_type`) pair.
- `status`: `running`, `ok`, `failed`, `skipped`, or `retried`.
- `duration_ms`, `rows_read`, `rows_merged`.
- `position_from`, `position_to`: the position JSON before and after. NULL for `full`.
- `pool`, `error`, `retry_count`.

One row is inserted at start with status `running` and updated at the finish. Concurrent workers
touch distinct rows, so there is no write contention.

**Capture events.** One event per source database records the batched `MAX` query, with action
`capture`, stage `capture`, and status `skipped`. `rows_read` is the number of tables the query
captured. The `error` column names the tables that fell back to a per-table `MAX`.

**Quiet tables.** A cursor table whose `hi` equals its checkpoint gets no event. Its checkpoint
advances in one bulk write. `ingest_run.skipped` counts it. Its `last_success_at` is stamped.

**Alerts and notes.** Two flags describe a cursor table:

- `alert=max_above_clock`: the temporal `hi` was above the source clock plus `cursor_skew_s`. The
  engine capped it. A source row carries a timestamp in the future. The flag goes to the run log
  and to the checkpoint `detail`.
- `note=inserts_only`: the table has an `identity` cursor. Updates and late commits below `hi` are
  not captured. The note goes to the run log as `[inserts_only]`, once per table per run.

**Retries.** With `table_retries` above 0, an attempt that fails with a transient source error ends
as `retried`, with its stage and error. The table gets another row for its next attempt.
`retry_count` numbers the attempts from 0. Only a final failure is `failed`.

**Abandoned runs.** A cancelled run cannot close its own rows. The next run of the same collection
holds the collection lock, so it closes them at launch. The dead run and its `running` events become
`failed`. The `detail` names the run that closed them under `closed_by_run`.

Table and column reference: [lakebase_schema.md](lakebase_schema.md).

## Canonical queries

Run these in DBSQL on a SQL warehouse or serverless. The Lakebase catalog is a database catalog. A
query from a classic all-purpose or job cluster fails with `PERMISSION_DENIED`. The ingestion job
connects to Lakebase directly and does not use the catalog.

The queries use the Spark SQL dialect against the catalog `lakefed_cursor_lakebase`. `:collection`
and `:run_id` are DBSQL query parameters.

In-flight tables of the latest run of a collection:

```sql
SELECT e.src_database, e.src_table, e.stage, e.pool, e.started_at
FROM lakefed_cursor_lakebase.lakefed_cursor.ingest_event e
WHERE e.run_id = (
        SELECT max(run_id) FROM lakefed_cursor_lakebase.lakefed_cursor.ingest_run
        WHERE task_collection = :collection)
  AND e.status = 'running'
ORDER BY e.started_at;
```

Failures of the latest run, with the stage and the error:

```sql
SELECT src_database, src_schema, src_table, stage, left(error, 200) AS error, finished_at
FROM lakefed_cursor_lakebase.lakefed_cursor.ingest_event
WHERE run_id = (
        SELECT max(run_id) FROM lakefed_cursor_lakebase.lakefed_cursor.ingest_run
        WHERE task_collection = :collection)
  AND status = 'failed'
ORDER BY finished_at DESC;
```

Per-table freshness and lag:

```sql
SELECT src_database, src_schema, src_table, strategy, last_success_at,
       timestampdiff(MINUTE, last_success_at, current_timestamp()) AS lag_minutes
FROM lakefed_cursor_lakebase.lakefed_cursor.checkpoint
ORDER BY last_success_at NULLS FIRST;
```

Per-run throughput and outcome. `skipped` counts the quiet tables:

```sql
SELECT run_id, task_collection, total, ok, failed, skipped, reseeded,
       timestampdiff(SECOND, started_at, finished_at) AS seconds,
       round(total / nullif(timestampdiff(SECOND, started_at, finished_at), 0), 2) AS tables_per_sec
FROM lakefed_cursor_lakebase.lakefed_cursor.ingest_run
ORDER BY started_at DESC
LIMIT 20;
```

Capture events of one run, one per source database:

```sql
SELECT src_database, rows_read AS tables_captured, duration_ms, error
FROM lakefed_cursor_lakebase.lakefed_cursor.ingest_event
WHERE run_id = :run_id AND action = 'capture'
ORDER BY src_database;
```

Tables with a capped watermark:

```sql
SELECT src_database, src_schema, src_table, detail, updated_at
FROM lakefed_cursor_lakebase.lakefed_cursor.checkpoint
WHERE detail LIKE '%alert=max_above_clock%'
ORDER BY updated_at DESC;
```

Delete snapshots, by last run time:

```sql
SELECT src_database, src_schema, src_table, last_delete_snapshot_at
FROM lakefed_cursor_lakebase.lakefed_cursor.checkpoint
WHERE last_delete_snapshot_at IS NOT NULL
ORDER BY last_delete_snapshot_at;
```

The retried attempts of a run:

```sql
SELECT src_database, src_schema, src_table, retry_count, stage, left(error, 200) AS error, finished_at
FROM lakefed_cursor_lakebase.lakefed_cursor.ingest_event
WHERE run_id = :run_id AND status = 'retried'
ORDER BY finished_at;
```

The run `detail` is JSON. With `table_retries` set it holds `table_retries` with `retries`,
`recovered`, `failed_after_retry`, `budget`, `budget_left`, `budget_exhausted`, `sources_stopped`
and `by_class`. With `max_queries_per_source` set it holds `source_caps` with each source share and
peak permits.

## Optional: durable history

Lakebase is the live store. To keep run history in Delta, run one scheduled statement. A periodic
snapshot is the simplest form:

```sql
CREATE OR REPLACE TABLE <catalog>.<schema>.ingest_event_history AS
SELECT * FROM lakefed_cursor_lakebase.lakefed_cursor.ingest_event;
```

For append-only growth, use an incremental MERGE on the primary key:

```sql
MERGE INTO <catalog>.<schema>.ingest_event_history t
USING lakefed_cursor_lakebase.lakefed_cursor.ingest_event s
ON t.id = s.id
WHEN NOT MATCHED THEN INSERT *;
```

Schedule either statement as a DBSQL query. The UC-federated reads above work without it.
