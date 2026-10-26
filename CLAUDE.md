# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

The repo holds a metadata-driven framework that ingests external databases into Databricks Delta
tables, as one Databricks Asset Bundle (`lakefed_cursor`, at the repo root: `databricks.yml`,
`resources/`, `src/lakefed_ingest/`, `tests/`). One job task per `task_collection`, in-process
threads + FAIR pools on a job cluster; Lakebase (Postgres) state; Spark JDBC; source adapters (SQL
Server, Oracle, PostgreSQL, MySQL) + load strategies (watermark cursor, full reload).

## Commands

### Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pytest            # unit tests need nothing else
```

### Run Tests
```bash
pytest                                   # the same as pytest tests (pytest.ini testpaths)
pytest tests/planner_test.py::test_name  # single test
```

### Deploy
```bash
scripts/validate.sh -t <target> -p <profile>      # name test (no lakefed_ingest / edp names), then bundle validate
databricks bundle deploy -t <target> -p <profile>
databricks bundle run lakefed_cursor_setup -t <target> -p <profile>   # create the Lakebase schema (upgrades migrate on the next ingest run)
databricks bundle run lakefed_cursor -t <target> -p <profile> --params task_collection=<name>
```

## The engine (repo root)

Warehouse-free: everything runs on cluster compute. Source reads use Spark JDBC with per-database
credentials (JSON `{user,password}`) from a secret scope named by `control.secret_key`; or, for a
row with `use_remote_query`, `remote_query` through the UC connection in `src_connection` (no
secret). See `README.md`, `docs/deploy.md`, `docs/monitoring.md`, `docs/lakebase_schema.md`.

### Lakebase state (`src/lakefed_ingest/lakebase_schema.sql`, schema `lakefed_cursor`)
- `control` — one row per (source database, table): `task_collection`, `src_type` (source adapter:
  `sqlserver` | `oracle` | `postgresql` | `mysql`), `src_host`/`src_port` and `secret_key` (JDBC) or
  `src_connection` + `use_remote_query` (UC connection), `src_database`, `src_schema`,
  `src_table`, `primary_key`, `select_list` (explicit columns), sink, `load_type` (load
  strategy: `cursor` (SQL Server, PostgreSQL; placeholder elsewhere) | `full`), partitioned-seed settings,
  `cursor_expression` (one SQL expression over 1 to 3 columns of one type, GREATEST / COALESCE only),
  `cursor_kind` (`rowversion` | `db_timestamp` | `app_timestamp` | `identity`), `cursor_lookback_s`
  (NULL = job parameter), `null_cursor_policy` (`reject` | `seed_only`), `delete_mode` (`none` |
  `pk_snapshot`; `soft_column`, `full_compare` fail at plan), `delete_min_interval_s`, `jdbc_options`
  (JSON), `client_id` (optional: consolidated sink shared by many source DBs; written to sink column
  `__client_id__`), `client_attrs` (JSON {column: value}: identity columns written as STRING on every
  row). CHECK constraints (`contracts/control_row.md`) enforce valid combos; a unique index allows one
  row per (sink, client_id).
- `checkpoint` — `strategy` + `position` (text; JSON {expr,kind,type,value} for `cursor`, see
  `contracts/position.md`; a changed `expr` or `kind` reseeds), phase (`seeding → incremental` for
  cursor, `loaded` for full), status, freshness, `last_delete_snapshot_at`.
- `ingest_run`, `ingest_event` — telemetry (`run_id`, `position_from/to`). The DB is registered in UC
  (default catalog `lakefed_cursor_lakebase`) for DBSQL monitoring.
- DDL is applied by `StateStore.migrate` (from `apply_lakebase_schema.ipynb`, and from `ingest.ipynb`
  when `schema_version` is behind `state_store.SCHEMA_VERSION`, so upgrades are deploy-only): one
  transaction, split on `;` — no `DO $$` blocks, no `CREATE INDEX CONCURRENTLY`. Migrations must be
  idempotent (`if not exists`, `drop constraint if exists`). Renames (no IF EXISTS form in Postgres) go
  in `state_store.RENAMES`; only the pending ones run, before the DDL. Every DDL or RENAMES change
  bumps `SCHEMA_VERSION` (`tests/state_store_test.py` pins its fingerprint). Every upgrade re-applies the whole
  file from any earlier version, so statements must be idempotent and additive; `ddl_problems` in the same test
  checks the forms, and statements that change existing columns or rows need a `REVIEWED_NON_ADDITIVE` entry
  (docs/lakebase_schema.md, "Changing the schema").

### Jobs (`resources/`)
- `lakefed_cursor` → task `ingest` → `ingest.ipynb`: one run ingests a `task_collection`. Parameters:
  `task_collection`, `parallelism` (single budget of concurrent query units), `secret_scope`,
  `detection_chunk` (unused since Change Tracking left), `trust_server_certificate`,
  `max_seeds_per_run` (seed budget, 0 = none),
  `freshness_interval_s` (unused until the quiet-table bulk advance returns), `max_queries_per_source`
  (cap per source across runs, 0 = none: each run takes cap / runs reading the source, counted from
  shared advisory locks; `parallel.SourceLimiter`), `table_retries` (extra attempts per table after a
  transient source error, 0 = off; `table_retry.py`), `table_retry_budget` (retries per run),
  `cursor_lookback_s` (temporal cursor: seconds `lo` is pulled back; row `cursor_lookback_s` overrides),
  `cursor_skew_s` (temporal cursor: `hi` is capped at the source clock plus this), `db_instance`, `db_name`.
  The tuning parameters' defaults are bundle variables of the same name (`databricks.yml`).
- `lakefed_cursor_setup` → `apply_lakebase_schema.ipynb`; plus bundle resources for the Lakebase
  instance, its UC catalog, and the secret scope (`database_instances.lakebase`,
  `database_catalogs.lakebase_catalog`, `secret_scopes.secret_scope`).

### Run flow (`ingest.ipynb`)
1. Per-collection Lakebase advisory lock (duplicate run exits `skipped`).
2. Plan (`planner.py`, pure): unsupported (`src_type`, `load_type`) pair → fail per table (stage
   `plan`); `full` → reload; `cursor` → `cursor_seed` / `schema_reseed` / `cursor` (per-table high
   watermark, `cursor_engine`). Seeds are capped at `max_seeds_per_run`.
3. Per-table worker: full reload (`load_strategies.FullStrategy` → `sink_writer.seed`), cursor seed
   (capture the high watermark before the seed; partitioned seed via staging + atomic swap), reseed
   enqueue, or cursor window read `(position, hi]` + idempotent PK MERGE (schema evolution) then
   checkpoint advance.
   With `table_retries` > 0, a transient source failure is retried in the run (`table_retry.with_retries`;
   the next attempt from `planner.retry_plan` on the fresh checkpoint).

### Modules
| File | Purpose |
|---|---|
| `src/lakefed_ingest/source_io.py` | Source reads (Spark JDBC, or `remote_query` through a UC connection): credentials, URL, the query unit (`_read`, `_unit`, `source_key`) |
| `src/lakefed_ingest/sink_writer.py` | Sink writes: MERGE / INSERT text, consolidated sinks (`client_id`, `client_attrs`), sink DDL, seed (plain or partitioned), write-conflict retry |
| `src/lakefed_ingest/worker.py` | Per-table worker (`ingest_one(plan, run_id, deps, attempt)`): routes `full` / `cursor_seed` / `cursor` / `schema_reseed`, telemetry, and the checkpoint advance after the sink commit (`tests/worker_test.py`); `_replan` for retries |
| `src/lakefed_ingest/table_retry.py` | Table retries: classify transient vs permanent errors (deny rules win, unknown = permanent), equal-jitter backoff, per-run budget and per-source stop (`RetryPolicy`), the worker wrapper (`with_retries`); the next attempt comes from `planner.retry_plan` |
| `src/lakefed_ingest/source_adapters.py` | Per-engine dialect (`src_type`): JDBC URL/driver/options, quoting, literals, select/size/bounds SQL, cursor methods (type lookup and class, source clock, text rendering, typed literals, lookback, batched MAX, GREATEST form; MySQL raises) |
| `src/lakefed_ingest/load_strategies.py` | Per-`load_type` strategy, position encoding, `supported()` matrix (+ placeholder pairs); cdc placeholder |
| `src/lakefed_ingest/cursor_engine.py` | Cursor (watermark) strategy for every adapter with cursor methods (no `src_type` branch): type lookup, high watermark capped at the clock + skew, typed literals, (lo, hi] read + upsert MERGE |
| `src/lakefed_ingest/planner.py` | Strategy routing + skip-no-change planning (no Spark/DB; unit-tested with fakes) |
| `src/lakefed_ingest/parallel.py` | Thread pool, FAIR pools, `WorkSlots` budget, `SourceLimiter` (per-source permits) |
| `src/lakefed_ingest/partitions.py` | Partitioned-seed planning |
| `src/lakefed_ingest/state_store.py` | Lakebase accessor (psycopg, SDK-minted OAuth token) + pure helpers |
| `tests/conftest.py` | Puts `src/lakefed_ingest` on `sys.path` |
| `tests/synthetic/` | Synthetic SQL Server DB generator, change generator, reconcile |
| `tools/source_profile.sql` | Read-only SQL Server profiling script for sizing |

### Engineering rules
- No `.cache()` / `.persist()`; read each source once per cycle (Delta MERGE materializes the JDBC
  source itself).
- No SQL warehouse dependency in the default version.
- Checkpoints advance only after the Delta commit (at-least-once + idempotent MERGE).
- Every sink write must be client-scoped when `client_id` is set: tag rows (`sink_writer._tag`),
  write `write_columns(cfg)`, MERGE on `merge_keys(cfg)`, overwrite via
  `insert_overwrite_sql(..., cfg)` (REPLACE WHERE client_id), stage via
  `staging_table_name(sink, cfg)`. Wrap writes in `with_write_retry` (Delta conflict retry,
  exponential backoff).
