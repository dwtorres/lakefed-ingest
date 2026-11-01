---
ontology: true
type: workflow-plan
domain: lakefed-cursor
summary: Implementation plan for the cursor-only ingestion engine, eight ordered work steps over the existing code
tags: [plan, cursor, ingestion, sqlserver, oracle, databricks, lakebase]
related: [spec, research, data-model, quickstart, ../../.specify/memory/constitution, ../../docs/cursor_orchestration_analysis]
status: draft
discovered: 2026-10-25
verified: false
workspace: lakefed-cursor
---

# Implementation Plan: Cursor-Only Ingestion Engine

**Branch**: `001-cursor-only-engine` | **Date**: 2026-10-25 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-cursor-only-engine/spec.md`

## Summary

Strip Change Tracking, CDC, and append mode from the root version of lakefed-ingest. Keep the
tested modules. Patch the cursor path. Add the Oracle adapter. Replace `cursor_column` with a cursor
expression and a cursor contract on the control row. Add `pk_snapshot` deletes and three auth
modes. Prove the result on one SQL Server table and one Oracle table in `devtest`.

The work runs in eight ordered steps. Each step leaves the test suite green. The owner fixed the order:

| Step | Work | Starts from |
|---|---|---|
| 1 | Strip CT, CDC, append. Delete red tests, keep green tests. | `ct_engine.py`, `planner.py`, `load_strategies.py`, `batch_append.py`, `state_store.py`, `table_retry.py`, `ingest.ipynb` |
| 2 | Move the per-table worker out of the notebook into `worker.py` with tests. | `ingest.ipynb` `ingest_one`, `_replan`, `assert_lock_held` |
| 3 | Patch the cursor bugs: gaps 2, 3, 4, 5, 8 of analysis section 3.4. | `cursor_engine.py` |
| 4 | Oracle adapter: cursor methods on `SourceAdapter`, Oracle implementation, PostgreSQL branch moved into the adapter. | `source_adapters.py`, `cursor_engine.py` |
| 5 | Cursor expression and control columns. Lakebase migration. Schema and bundle names per Principle IX. `SCHEMA_VERSION` bump. | `lakebase_schema.sql`, `state_store.py`, `cursor_engine.py`, `planner.py`, `databricks.yml`, `resources/` |
| 6 | Delete mode `pk_snapshot`. | `worker.py`, `sink_writer.py`, `state_store.py` |
| 7 | Auth modes `sql`, `ntlm`, `aad_sp`. | `source_io.py` (`_extract_creds`, `_jdbc_config`), `source_adapters.py` |
| 8 | Devtest run on one table per engine. | `quickstart.md`, `scripts/validate.sh` |

## Technical Context

**Language/Version**: Python 3.11 on the Databricks Runtime job cluster. Local tests run on the repo venv.

**Primary Dependencies**: Spark JDBC, psycopg for Lakebase, and databricks-sdk for the OAuth token, all on the cluster. Local tests need `pytest` only. No new dependencies.

**Storage**: Lakebase (Postgres) schema `lakefed_cursor` for control, checkpoint, telemetry. Delta tables in a `devtest` sink catalog.

**Testing**: `pytest tests` with fakes. No Spark, no database in unit tests. One live `devtest` run in step 8.

**Target Platform**: Databricks job cluster, classic single-user. Serverless is out of scope.

**Project Type**: Single project: a Python package under `src/lakefed_ingest`, one thin notebook, one asset bundle.

**Performance Goals**: One `MAX` query per source database per run for all cursor tables. Zero sink versions per run for a quiet table.

**Constraints**: No SQL warehouse. No `.cache()` or `.persist()`. Checkpoint advance after Delta commit only. No branch on source type outside `source_adapters.py`. No new dependencies. No `edp` or `lakefed_ingest` resource touched.

**Scale/Scope**: About 4,400 lines today. About 1,600 lines deleted. One new module (`worker.py`), one rename into two (`ct_engine.py` into `source_io.py` and `sink_writer.py`).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Evidence |
|---|---|---|
| I. Strip and patch, never rewrite | PASS | Every step names the file and function it starts from. The one new module, `worker.py`, replaces the notebook cells `ingest_one`, `_replan`, and `assert_lock_held`. Fixed decision call 1 sets the rename of `ct_engine.py` to `source_io.py` and `sink_writer.py`. |
| II. Cursor only | PASS | Step 1 deletes CT, CDC, and append code paths. `load_strategies.supported` allows `cursor` and `full` only. The control constraint `valid_load_type` allows the same two. |
| III. Everything inside Databricks | PASS | No warehouse. Lakebase state. Spark JDBC or `remote_query`. No new orchestrator. |
| IV. SQL Server and Oracle first-class | PASS | Step 4 adds cursor methods to `SourceAdapter` and removes `_is_pg` branching from `cursor_engine.py`. Tests parametrize over `SqlServerAdapter` and `OracleAdapter`. See [contracts/source_adapter.md](contracts/source_adapter.md). |
| V. One expression per table | PASS | Step 5 stores `{expr, kind, type, value}` in `checkpoint.position`. A changed `expr` reseeds. Nullable inputs fail under `reject`. See [data-model.md](data-model.md). |
| VI. Checkpoint after commit | PASS | Step 2 moves the ordering into `worker.py` with a test that asserts the sink write call precedes the checkpoint advance call. |
| VII. Keep what is tested | PASS with note | `parallel.py` and `partitions.py` stay untouched. `source_adapters.py` gains additive methods, each with a failing test first. `table_retry.py` loses `RETENTION_LAPSED_MARKER` and the CT permission marker in step 1. A test asserts the new classification first. `state_store.py` loses `record_pending`, the `pending_*` fields, and `ct_begin_version` in step 1. It gains `last_delete_snapshot_at` in step 6. Each change starts with a failing test. |
| VIII. Docs in STE | PASS | This plan, research, data model, contracts, and quickstart follow the STE rules. Step 1 and step 5 rewrite README and module headers. |
| IX. Own names | PASS | Step 5 renames the bundle to `lakefed_cursor`, the schema to `lakefed_cursor`, and the secret scope to `lakefed-cursor`. A test in `tests/job_parameters_test.py` fails on any resource name that starts with `lakefed_ingest` or `edp`. The Python package path `src/lakefed_ingest` stays. See research R-10. |

Fixed decisions 1, 11, 12, 13, and 14 are all honored. Call 13, the Lakeflow Connect baseline, runs once before step 1 for SQL Server and once before step 4 for Oracle. See quickstart. Adopted recommendations: 2, 3, 4, 5, 6, 7, 9. Rejected recommendations: 8, chunked increments, and 10, append mode. Both are out of scope in the spec.

Gate result: PASS. No entry in Complexity Tracking.

## Project Structure

### Documentation (this feature)

```text
specs/001-cursor-only-engine/
├── plan.md              # This file
├── research.md          # Phase 0: decisions R-01 to R-12
├── data-model.md        # Phase 1: control row, checkpoint, position, secret JSON
├── quickstart.md        # Phase 1: per-step checks and the devtest run
├── contracts/
│   ├── source_adapter.md    # adapter methods every dialect provides
│   ├── control_row.md       # control columns, constraints, defaults
│   ├── position.md          # checkpoint position JSON
│   ├── secret_json.md       # auth mode secret shapes
│   └── job_parameters.md    # job parameters and bundle variables
├── checklists/requirements.md
└── tasks.md             # Phase 2 (speckit-tasks), not written here
```

### Source Code (repository root)

```text
src/lakefed_ingest/
├── source_adapters.py     # keep. add cursor methods. OracleAdapter real. PostgreSQL branch moves here
├── source_io.py           # from ct_engine.py: creds, JDBC config, _read, remote_query, source_key, seed read
├── sink_writer.py         # from ct_engine.py: sink DDL, merge SQL, insert overwrite, with_write_retry, _tag
├── cursor_engine.py       # keep. patch bounds, literals, expression, batched MAX
├── worker.py              # new: per-table worker from ingest.ipynb ingest_one. ordering invariant
├── planner.py             # keep cursor and full routes. delete CT routes and detect_group
├── load_strategies.py     # CursorStrategy, FullStrategy only
├── parallel.py            # unchanged
├── partitions.py          # unchanged
├── table_retry.py         # keep. drop CT markers
├── state_store.py         # keep. drop pending and ct fields. add last_delete_snapshot_at. SCHEMA_VERSION 2
├── lakebase_schema.sql    # schema lakefed_cursor. new control columns. drop six dead columns
├── ingest.ipynb           # thin: parameters, lock, plan, pool, call worker
└── apply_lakebase_schema.ipynb

tests/
├── source_adapters_test.py   # parametrized over sqlserver and oracle
├── source_io_test.py         # from ct_engine_test.py green half
├── sink_writer_test.py       # from ct_engine_test.py green half
├── cursor_engine_test.py     # gap tests. parametrized over both dialects
├── worker_test.py            # new: ordering, routes, PK snapshot
├── planner_test.py           # cursor and full routes only
├── load_strategies_test.py
├── state_store_test.py       # SCHEMA_VERSION 2 fingerprint
├── table_retry_test.py
├── job_parameters_test.py    # plus name validation
└── (deleted) batch_append_test.py, ct_engine_test.py red half

resources/
├── lakefed_cursor.yml         # from lakefed_ingest.yml
└── lakefed_cursor_setup.yml   # from lakefed_ingest_setup.yml
databricks.yml                 # bundle lakefed_cursor
```

**Structure Decision**: Single project. The split of `ct_engine.py` follows fixed decision call 1. `worker.py` is the only new module. Its starting point is the notebook function `ingest_one` and its helpers.

## Step Detail

Each step ends with `pytest tests` green with no skips, a Conventional Commit, and a one-line checkpoint in `tasks/todo.md`.

### Step 1: Strip CT, CDC, append

Delete `batch_append.py` and `tests/batch_append_test.py`.

In `ct_engine.py`, delete these CT and append functions:

- `current_version`, `database_ct_state`, `ct_table_status`, `ct_begin_version`, `require_table_ct`, `min_valid_version`
- `ct_projection`, `ct_join`, `ct_read_query`, `ct_retention_guard_sql`, `ct_read_query_guarded`, `ct_read_session_init`, `ct_read`, `is_retention_lapsed_error`
- `detection_sql`, `classify`, `detect_changes`, `all_current`, `source_group_key`
- `read_and_merge_ct`, `read_and_append_ct`, `append_committed_sql`, `append_committed`, `append_batch`, `append_meta`, `is_append`
- `change_read`, `snapshot_read`, `committed_clients_sql`, `committed_clients`, `seed_prep_sql`, `seed_prep`, `prepare_seed`
- `_insert_into`, `_write_snapshot`, and the append branch of `write_mode`

Split the rest into `source_io.py` and `sink_writer.py`. In `planner.py`, delete `ct_position`, `group_by_source`, `detect_group`, `merge_group_results`, the CT branch of `route_tables`, and the `pending` and `check` branches of `retry_plan`. Keep `bulk_rows_to_write` for step 5. In `load_strategies.py`, delete `CtStrategy`, `CdcStrategy`, and the CT and CDC pairs in `PLACEHOLDER`. In `state_store.py`, delete `record_pending`, the `pending_*` fields, `ct_begin_version`, and `reseed_queue`. In `table_retry.py`, delete `RETENTION_LAPSED_MARKER` and the CT permission marker. Step 1 does not touch `lakebase_schema.sql`. Step 5 changes it. In `ingest.ipynb`, delete the CT group plan, `_apply_changes_or_reseed`, and the append routes.

Tests: delete every test that asserts CT, CDC, append, pending, landed, or check behavior. Keep every other test and move it with its function. `tests/ct_engine_test.py` splits into `source_io_test.py` and `sink_writer_test.py`.

Check: `pytest tests` green. `rg -n "change_tracking|CHANGETABLE|append_batch|pending_position|verify_run_id" src tests` returns nothing.

### Step 2: Worker module

Create `worker.py` from `ingest_one`, `_replan`, `_pools_label`. Signature: `ingest_one(plan, run_id, deps, attempt=0)`, where `deps` carries `spark`, `store`, `slots`, `executor`, `log`, `params`. The notebook keeps `widget`, the lock, `assert_lock_held`, the pool setup, the source share refresh, and the plan call.

Tests (`worker_test.py`): fake `deps`. Assert the call order sink write then checkpoint advance for `cursor_seed`, `cursor`, `full`. Assert a failed write leaves the checkpoint untouched. Assert `schema_reseed` and `plan` failure routes.

Check: `pytest tests` green. `tests/job_parameters_test.py` still passes.

### Step 3: Cursor bug patches

In `cursor_engine.py`, test first per gap:

- Gap 5: `_minus_seconds` and `literal` round to three fractional digits for `datetime` and `smalldatetime`.
- Gap 2: the `lookback_s` argument replaces the `CURSOR_OVERLAP_S` environment variable. The notebook passes the job parameter. The row override wins.
- Gap 4: `high_watermark` returns `min(MAX(expr), source_now + skew_s)` for temporal kinds. The alert flag returns with it.
- Gap 3: the `identity` kind reads without lookback. `plan_increment` returns an `inserts_only` note. The worker logs it once per run per table.
- Gap 8: if `hi` is below the position, `plan_increment` raises a clear error.

Check: each gap has a test named `test_gap_N_*` that fails on the step 2 commit and passes on the step 3 commit.

### Step 4: Oracle adapter

Add to `SourceAdapter`: `cursor_type_sql`, `source_now_sql`, `max_batch_sql`, `cursor_literal`, `minus_seconds_sql`, `greatest_expr`. Implement for `SqlServerAdapter`, `OracleAdapter`, `PostgresAdapter`. Move the `_is_pg` branches of `cursor_engine.py` into `PostgresAdapter`. `MySqlAdapter` raises `NotImplementedError` from the cursor methods, and `load_strategies.unsupported_reason` reports it at plan time.

Tests: `source_adapters_test.py` and `cursor_engine_test.py` parametrize over `sqlserver` and `oracle`. A test asserts `rg "_is_pg|src_type ==" src/lakefed_ingest/cursor_engine.py src/lakefed_ingest/worker.py` finds nothing.

### Step 5: Cursor expression, control columns, migration, names

In `lakebase_schema.sql`, the schema becomes `lakefed_cursor`. Add `cursor_expression`, `cursor_kind`, `cursor_lookback_s`, `delete_mode`, `delete_min_interval_s`, and `null_cursor_policy`. Drop `job_name`, `src_catalog`, `enable_iceberg_reads`, `sink_cluster_cols`, `source_instance_group`, `warehouse_id`, `ct_enabled`, `cdc_capture_instance`, `cursor_column`, and `write_mode`. `load_type` defaults to `cursor` with a check on `cursor` and `full`. The checkpoint drops `ct_begin_version`, `pending_position`, and `pending_run_id`. It adds `last_delete_snapshot_at`. `state_store.SCHEMA_VERSION` becomes 2 with its fingerprint pinned. The position JSON gains `expr` and `kind`. `cursor_engine.validate_expression` checks the column count, the shared type, and the nullable inputs. One `max_batch_sql` query per source database captures `hi`, with a per-table fallback. `planner.bulk_rows_to_write` advances the quiet tables. The merge predicate is `WHEN MATCHED AND s.expr <> t.expr`. Rename the bundle, the resources, the secret scope, and the catalog. Add the name validation test.

Check: `state_store_test.py` fingerprint test passes at version 2. `ddl_problems` reports nothing unreviewed. Gap 6, 10, 11 tests pass.

### Step 6: Delete mode

In `worker.py`, the snapshot runs after the increment. It runs only when `delete_mode` is `pk_snapshot`, the phase is `incremental`, and the interval has passed. The worker reads the PK columns through `source_io`, anti-joins them against the sink, and deletes the missing rows scoped to `client_id`. Then it calls `store.record_delete_snapshot`. `sink_writer.delete_missing_sql` builds the delete statement.

Check: gap 1 test. A test that the snapshot is skipped inside the interval and during a seed.

### Step 7: Auth modes

`source_io._extract_creds` parses `auth_mode`. `source_adapters.SqlServerAdapter.auth_options(creds)` returns JDBC options per mode. `OracleAdapter` accepts `sql` only and fails the others before connect. Error text lists the three modes and never prints a value.

Check: four unit tests, one per spec scenario of User Story 7.

### Step 8: Devtest run

Follow [quickstart.md](quickstart.md). The baseline ran in steps 1 and 4 (T012a, T034a). Deploy, run setup, load two control rows, seed, change the source, run the increment, and reconcile.

Check: SC-001 reconcile shows zero differences. Run log shows one `MAX` query per source database.

## Complexity Tracking

No constitution violations. Table left empty on purpose.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| none | | |
