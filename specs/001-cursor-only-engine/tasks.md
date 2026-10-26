---
ontology: true
type: workflow-tasks
domain: lakefed-cursor
summary: Dependency-ordered task list for the cursor-only ingestion engine, grouped by plan step and labeled by user story
tags: [tasks, cursor, ingestion, sqlserver, oracle, databricks, lakebase]
related: [plan, spec, research, data-model, quickstart, contracts/source_adapter, contracts/control_row, contracts/position, contracts/secret_json, contracts/job_parameters, ../../.specify/memory/constitution]
status: draft
discovered: 2026-10-25
verified: false
workspace: lakefed-cursor
---

# Tasks: Cursor-Only Ingestion Engine

**Input**: Design documents from `specs/001-cursor-only-engine/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: Included. Constitution Principle VII and spec FR-039 require a failing test before each behavior change. Every cursor feature test runs against the `sqlserver` and `oracle` adapters (FR-040).

**Organization**: The owner fixed the step order in plan.md. Phases follow that order. Each task carries the user story it serves. When `pytest tests` is green with zero skips and one Conventional Commit is made, the phase ends.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependency on an unfinished task)
- **[Story]**: User story from spec.md (US1 to US7)
- Every task names the file it changes. Every task starts from existing code (Principle I).

## Path Conventions

Single project. Package `src/lakefed_ingest/`, tests `tests/`, bundle `databricks.yml` and `resources/`, docs `README.md` and `docs/`.

## Story to step map

| Story | Priority | Plan step | Phase |
|---|---|---|---|
| US1 SQL Server cursor | P1 | 1, 2, 5, 8 | 3, 10 |
| US2 Oracle cursor | P1 | 4, 8 | 5, 10 |
| US3 Expression cursor | P2 | 5 | 6 |
| US4 Safe window bounds | P2 | 3 | 4 |
| US5 PK snapshot deletes | P3 | 6 | 8 |
| US6 Quiet tables | P3 | 5 | 7 |
| US7 Auth modes | P3 | 7 | 9 |

---

## Phase 1: Setup (plan step 1: strip CT, CDC, append)

**Purpose**: Delete the dead load paths so every later phase works on cursor and full code only (FR-001 to FR-003).

- [X] T001 Delete `src/lakefed_ingest/batch_append.py` and `tests/batch_append_test.py`
- [X] T002 Delete the CT, detection, append, and pending functions from `src/lakefed_ingest/ct_engine.py`: `current_version`, `database_ct_state`, `ct_table_status`, `ct_begin_version`, `require_table_ct`, `min_valid_version`, `ct_projection`, `ct_join`, `ct_read_query`, `ct_retention_guard_sql`, `ct_read_query_guarded`, `ct_read_session_init`, `ct_read`, `is_retention_lapsed_error`, `detection_sql`, `classify`, `detect_changes`, `all_current`, `source_group_key`, `read_and_merge_ct`, `read_and_append_ct`, `append_committed_sql`, `append_committed`, `append_batch`, `append_meta`, `is_append`, `change_read`, `snapshot_read`, `committed_clients_sql`, `committed_clients`, `seed_prep_sql`, `seed_prep`, `prepare_seed`, `_insert_into`, `_write_snapshot`, and the append branch of `write_mode`
- [X] T003 Split the rest of `src/lakefed_ingest/ct_engine.py` into `src/lakefed_ingest/source_io.py` (read side per research R-02: `secret_ref`, `_extract_creds`, `_creds`, `_uc_host`, `_jdbc_config`, `config_cache_key`, `jdbc_url`, `trust_server_certificate`, `_read`, `remote_query_sql`, `uses_remote_query`, `source_key`, `_unit`, `_units`, seed read) and `src/lakefed_ingest/sink_writer.py` (write side: `sink_fqn`, `staging_table_name`, `quote_delta_ident`, `_with_table_suffix`, `client_id`, `client_attrs`, `write_columns`, `merge_keys`, `client_predicate`, `_tag`, `_lit`, `merge_on_cfg`, `merge_sql`, `create_sink_sql`, `ensure_sink_table`, `check_sink_mode`, `insert_sql`, `insert_overwrite_sql`, `_insert_overwrite`, `missing_sink_columns`, `with_write_retry`, `is_write_conflict`, `backoff_delay`, seed write, `_partitioned_seed`), then delete `ct_engine.py`
- [X] T004 Split `tests/ct_engine_test.py` into `tests/source_io_test.py` and `tests/sink_writer_test.py`. Keep every test whose behavior survives. Delete tests that assert CT, CDC, append, pending, landed, check, or `verify_run_id` behavior
- [X] T005 Delete `ct_position`, `group_by_source`, `detect_group`, `merge_group_results`, the CT branch of `route_tables`, and the `pending` and `check` branches of `retry_plan` in `src/lakefed_ingest/planner.py`. Keep `config_error`, `route_tables` cursor and full branches, `_route_cursor`, `retry_plan`, `apply_seed_budget`, `bulk_rows_to_write`
- [X] T006 Delete the CT and append tests from `tests/planner_test.py`. Keep cursor, full, seed budget, retry, and bulk advance tests
- [X] T007 [P] Delete `CtStrategy`, `CdcStrategy`, and the CT and CDC pairs in `PLACEHOLDER` in `src/lakefed_ingest/load_strategies.py`. Keep `CursorStrategy`, `FullStrategy`, `get_strategy`, `supported`, `unsupported_reason`, `support_matrix`. Update `tests/load_strategies_test.py` to assert `supported` returns true for `cursor` and `full` only
- [X] T008 [P] Remove the `import batch_append as ba` and `import ct_engine as ce` lines and the `BatchRunner` fake from `tests/table_retry_test.py`. Write a failing test there that asserts a CT retention error and a CT permission error classify as permanent, then delete `RETENTION_LAPSED_MARKER` and the CT permission marker from `src/lakefed_ingest/table_retry.py`
- [X] T009 [P] Write failing tests in `tests/state_store_test.py` that assert `record_pending_sql`, `RESEED_TABLE`, and the `pending_*` and `ct_begin_version` columns are gone from the checkpoint SQL, then delete `record_pending`, `record_pending_sql`, `RESEED_TABLE`, the `pending_position` and `pending_run_id` fields, and `ct_begin_version` from `src/lakefed_ingest/state_store.py`. Leave `src/lakefed_ingest/lakebase_schema.sql` and `SCHEMA_VERSION` for Phase 6
- [X] T010 Delete the CT group plan, `_apply_changes_or_reseed`, the append routes, the `pending` and `check` routes, and the `snapshot_isolation`, `append_batch_size`, and `reseed_on_new_columns` widgets from `src/lakefed_ingest/ingest.ipynb`. Point every import from `ct_engine` to `source_io` or `sink_writer`
- [X] T011 Remove `snapshot_isolation`, `append_batch_size`, and `reseed_on_new_columns` from `resources/lakefed_ingest.yml` and `databricks.yml` so `tests/job_parameters_test.py` passes
- [X] T011a Delete `tests/validation_test.py` and `tests/validation_harness.py`. Both target Change Tracking (`create_ct_table`) and the live case skips without credentials, which the constitution counts as a failure. Move the two `reconcile` tests into `tests/synthetic_test.py` against `tests/synthetic/reconcile.py`. Delete the `validation_test.py` paragraph in `README.md` line 502
- [X] T011b Set `testpaths = tests` in `pytest.ini` and delete the two-version comment. Bare `pytest` and `pytest tests` then run the same suite
- [X] T011c Commit the staged deletion of `job_orchestration/`. Delete the two-version table, the `pytest job_orchestration/tests` line, the `cd job_orchestration` deploy block, and the whole "Job orchestration version" section from the root `CLAUDE.md`. Delete every `job_orchestration` mention in `README.md`. The repo holds one bundle
- [X] T012 Run `pytest -rs` and `rg -n "change_tracking|CHANGETABLE|append_batch|pending_position|verify_run_id|batch_append|ct_engine" src tests`. Expect zero failures, zero skips, no grep output. Commit `refactor(engine): strip change tracking, cdc, and append paths`
- [ ] T012a Run the Lakeflow Connect baseline for the SQL Server source per quickstart.md: one table through a query-based pipeline in `devtest`. Record cost and lag in `docs/baseline.md` with MOP frontmatter. Delete the pipeline (fixed decision call 13, before any cursor code change)

**Checkpoint**: Cursor and full are the only load paths. Line count drops by about 1,600 (SC-008).

---

## Phase 2: Foundational (plan step 2: worker module)

**Purpose**: Move the per-table worker into a tested module so the checkpoint-after-commit invariant has a unit test (FR-027, FR-037, Principle VI). Blocks every user story phase.

- [X] T013 Create `tests/worker_test.py` with a fake `deps` object (`spark`, `store`, `slots`, `executor`, `log`, `params`) and failing tests that assert: the sink write call precedes the checkpoint advance call for routes `cursor_seed`, `cursor`, and `full`. A failed sink write leaves the checkpoint untouched. The `schema_reseed` route enqueues a reseed. The `plan` failure route records a failed event and no position. The `cursor_seed` route captures `hi` through `cursor_engine.high_watermark` before the seed read and advances the position to that `hi`
- [X] T014 Create `src/lakefed_ingest/worker.py` from the notebook functions `ingest_one`, `_replan`, and `_pools_label` in `src/lakefed_ingest/ingest.ipynb`. Signature `ingest_one(plan, run_id, deps, attempt=0)` per research R-03. Make T013 pass
- [X] T015 Replace the worker cells in `src/lakefed_ingest/ingest.ipynb` with: build `deps`, bind `worker.ingest_one`, pass it to `parallel.run_parallel`. Keep `widget`, the advisory lock, `assert_lock_held`, the pool setup, the source share refresh, and the plan call
- [X] T016 Run `pytest tests` and `rg -n "def ingest_one" src/lakefed_ingest/ingest.ipynb`. Expect green, no grep output. Commit `refactor(worker): move per-table worker into worker.py`

**Checkpoint**: `worker_test.py` proves the ordering invariant. User story phases can start.

---

## Phase 3: User Story 1 - Ingest a SQL Server table by cursor (Priority: P1) 🎯 MVP

**Goal**: The surviving cursor path seeds and increments a SQL Server table, and any `load_type` other than `cursor` or `full` fails at plan time (FR-005). The `load_type` default and the live run land in Phase 6 and Phase 10.

**Independent Test**: `pytest tests/planner_test.py tests/cursor_engine_test.py tests/worker_test.py -k "cursor or full or load_type"` is green. Live proof is Phase 10.

- [X] T017 [P] [US1] Add tests in `tests/planner_test.py` that `config_error` rejects `load_type` values `ct`, `cdc`, and `bogus` with a reason that names the two valid values, and that a `cursor` row with no `primary_key` fails at plan time
- [X] T018 [P] [US1] Add a test in `tests/planner_test.py` that `route_tables` on a seeded SQL Server cursor row with a position returns route `cursor`, and on no checkpoint returns `cursor_seed`
- [X] T019 [US1] Make T017 and T018 pass in `src/lakefed_ingest/planner.py` `config_error` and `route_tables`. The reason text for a bad `load_type` lists `cursor` and `full`
- [X] T020 [US1] Add a test in `tests/worker_test.py` that the `cursor` route calls `cursor_engine.plan_increment`, then the sink merge, then `store.advance`, with the position equal to the captured `hi`. Make it pass in `src/lakefed_ingest/worker.py`
- [X] T021 [US1] Run `pytest tests`. Commit `test(planner): pin cursor routes and load_type validation`

**Checkpoint**: A SQL Server cursor row plans, seeds, and increments through the worker with fakes.

---

## Phase 4: User Story 4 - Safe window bounds (Priority: P2, plan step 3)

**Goal**: Patch gaps 2, 3, 4, 5, and 8 of analysis section 3.4 in `cursor_engine.py` (FR-017 to FR-022), and pin gap 7 as covered by lookback plus idempotent merge. Each gap has one test named `test_gap_N_*`. Gaps 2, 3, 4, 5, and 8 fail on the Phase 2 commit.

**Independent Test**: `pytest tests/cursor_engine_test.py -k gap` lists six passing tests. On the Phase 2 commit the same command fails five of them.

- [ ] T022 [P] [US4] Write `test_gap_5_datetime_literal_three_digits` in `tests/cursor_engine_test.py`: `_minus_seconds` and `literal` for `datetime` and `smalldatetime` round to at most three fractional digits
- [ ] T023 [P] [US4] Write `test_gap_2_lookback_argument_replaces_env` in `tests/cursor_engine_test.py`: `read_where` takes a `lookback_s` argument. The control row `cursor_lookback_s` wins over the job default. No read of `LAKEFED_CURSOR_OVERLAP_S`
- [ ] T024 [P] [US4] Write `test_gap_4_hi_capped_at_source_clock_plus_skew` in `tests/cursor_engine_test.py`: `high_watermark` with a fake `MAX` in 2099 and a fake source clock returns `min(MAX, now + skew_s)` and an alert flag `max_above_clock`
- [ ] T025 [P] [US4] Write `test_gap_3_identity_reads_without_lookback` in `tests/cursor_engine_test.py`: kind `identity` ignores lookback and `plan_increment` returns note `inserts_only`
- [ ] T026 [P] [US4] Write `test_gap_8_hi_below_position_raises` in `tests/cursor_engine_test.py`: `plan_increment` raises an error whose text names a possible source truncate or reload, and the position argument is not changed
- [ ] T026a [P] [US4] Write `test_gap_7_tie_rows_land_in_later_window` in `tests/cursor_engine_test.py`, parametrized over both dialects: two rows share the cursor value `hi`. The window `(lo, hi]` reads both. The next window with lookback reads both again. `upsert_merge_sql` is idempotent on the primary key, so a second merge of the same rows produces no change
- [ ] T027 [US4] Make T022 pass in `src/lakefed_ingest/cursor_engine.py` `_minus_seconds` and `literal` (FR-021)
- [ ] T028 [US4] Make T023 pass: replace `CURSOR_OVERLAP_S` and the `LAKEFED_CURSOR_OVERLAP_S` environment read in `src/lakefed_ingest/cursor_engine.py` `read_where` with a `lookback_s` argument. Pass `params.cursor_lookback_s` from `src/lakefed_ingest/worker.py` with the row `cursor_lookback_s` as override (FR-017)
- [ ] T029 [US4] Make T024 pass in `src/lakefed_ingest/cursor_engine.py` `high_watermark` and `high_watermark_sql`: the query also returns the source clock. The function returns `(hi, alert)`. The worker logs an `ingest_event` with detail `alert=max_above_clock` when set (FR-018)
- [ ] T030 [US4] Make T025 pass in `src/lakefed_ingest/cursor_engine.py` `plan_increment` and `read_where`. `src/lakefed_ingest/worker.py` logs the `inserts_only` note once per run per table (FR-020)
- [ ] T031 [US4] Make T026 pass in `src/lakefed_ingest/cursor_engine.py` `plan_increment` (FR-022)
- [ ] T032 [US4] Add `cursor_lookback_s` (default `3600`) and `cursor_skew_s` (default `300`) as widgets in `src/lakefed_ingest/ingest.ipynb`, job parameters in `resources/lakefed_ingest.yml`, and bundle variables in `databricks.yml` per contracts/job_parameters.md. Keep `tests/job_parameters_test.py` green
- [ ] T032a [P] [US4] Write `test_max_timeout_fails_table_and_keeps_position` in `tests/worker_test.py`: a `high_watermark` read that raises leaves the checkpoint untouched and records a failed event for that table only (spec edge case, FR-022)
- [ ] T032b [P] [US4] Write `test_rows_below_cap_read_after_capped_hi` in `tests/cursor_engine_test.py`, parametrized over both dialects: after `hi` is capped at clock plus skew, the next `plan_increment` with position equal to that `hi` reads rows between it and the new cap, and the 2099 row is read once the clock passes it (US4 scenario 2)
- [ ] T033 [US4] Add a test in `tests/cursor_engine_test.py` that a rowversion `hi` is below `MIN_ACTIVE_ROWVERSION` (existing behavior, FR-019) and runs under the `sqlserver` adapter only
- [ ] T034 [US4] Run `pytest tests` and `rg -n "LAKEFED_CURSOR_OVERLAP_S" src tests`. Expect green, no grep output. Commit `fix(cursor): safe window bounds for lookback, skew, identity, and reload`

**Checkpoint**: Six gap tests pass. Window bounds are safe on SQL Server and PostgreSQL.

---

## Phase 5: User Story 2 - Ingest an Oracle table by cursor (Priority: P1, plan step 4)

**Goal**: Every dialect difference becomes a `SourceAdapter` method. `OracleAdapter` is real. `cursor_engine.py` has no `_is_pg` branch (FR-031 to FR-033, Principle IV).

**Independent Test**: `pytest tests/source_adapters_test.py tests/cursor_engine_test.py` runs every cursor case under `sqlserver` and `oracle`. Live proof is Phase 10.

- [ ] T034a [US2] Run the Lakeflow Connect baseline for the Oracle source per quickstart.md. Append cost and lag to `docs/baseline.md`. Delete the pipeline (call 13, before the Oracle adapter)
- [ ] T035 [P] [US2] Write failing tests in `tests/source_adapters_test.py` parametrized over `sqlserver` and `oracle` for the new methods in contracts/source_adapter.md: `cursor_type_sql(schema, table, columns)`, `type_class_of(type_name)`, `source_now_sql(type_name)`, `max_batch_sql(tables, now_type)`, `cursor_literal(value, type_name)`, `minus_seconds_sql(expr, seconds, type_name)`, `greatest_expr(inputs, type_name, server_version)`, `server_version_sql()`, `pk_snapshot_sql(schema, table, pk_columns)`. Assert Oracle `max_batch_sql` ends with `FROM DUAL`, Oracle `cursor_literal` uses `TO_TIMESTAMP` or `TO_DATE`, Oracle `minus_seconds_sql` uses `NUMTODSINTERVAL`, SQL Server `greatest_expr` returns a `CASE` form for `server_version` below 16, SQL Server `server_version_sql` selects `SERVERPROPERTY('ProductMajorVersion')` and Oracle and PostgreSQL return `None`, and `MySqlAdapter` raises `NotImplementedError` with the dialect name from every new method
- [ ] T036 [P] [US2] Write a test in `tests/source_adapters_test.py` that `type_class_of` maps SQL Server `rowversion` and `timestamp` to `rowversion`, Oracle `NUMBER` with scale 0 to `numeric`, and Oracle `TIMESTAMP(6)` and `DATE` to `temporal`, and raises for an unknown type
- [ ] T037 [US2] Implement the new methods on `SourceAdapter`, `SqlServerAdapter`, `OracleAdapter`, and `PostgresAdapter` in `src/lakefed_ingest/source_adapters.py`. `MySqlAdapter` raises. Make T035 and T036 pass. SQL Server `source_now_sql` returns `SYSUTCDATETIME()` for `datetime2` and `datetimeoffset` and `GETDATE()` for `datetime` and `smalldatetime`. Oracle returns `SYSTIMESTAMP` or `SYSDATE`. PostgreSQL returns `now()` (research R-05)
- [ ] T038 [US2] Move the `_is_pg` branches of `column_type_sql`, `nullable_sql`, `high_watermark_sql`, `literal`, `_col`, and `_pg_column_where` in `src/lakefed_ingest/cursor_engine.py` into the adapter methods from T037. Delete `_is_pg`. `cursor_engine` resolves the adapter once per call with `source_adapters.for_cfg(cfg)` and calls methods only
- [ ] T039 [US2] Parametrize every window, literal, and gap test in `tests/cursor_engine_test.py` over `src_type` in (`sqlserver`, `oracle`) with a `pytest.fixture(params=...)`. Keep the existing `postgresql` cases
- [ ] T040 [US2] Add a test in `tests/load_strategies_test.py` that `unsupported_reason("mysql", "cursor")` returns a reason that names the adapter and `NotImplementedError`, and make it pass in `src/lakefed_ingest/load_strategies.py` `unsupported_reason` (FR-033)
- [ ] T041 [P] [US2] Add `tests/no_dialect_branch_test.py` that reads `src/lakefed_ingest/cursor_engine.py` and `src/lakefed_ingest/worker.py` and asserts neither contains `_is_pg` or `src_type ==` (SC-007)
- [ ] T042 [US2] Run `pytest tests` and `rg -n "_is_pg|src_type ==" src/lakefed_ingest/cursor_engine.py src/lakefed_ingest/worker.py`. Expect green, no grep output. Commit `feat(oracle): cursor adapter methods for sqlserver, oracle, postgresql`

**Checkpoint**: Oracle and SQL Server share one cursor engine. Every cursor test runs twice.

---

## Phase 6: User Story 3 - Fold up to three columns into one cursor (Priority: P2, plan step 5a)

**Goal**: Replace `cursor_column` with `cursor_expression` and `cursor_kind`. Store the expression in the position. Reseed on a changed expression. New control columns, constraints, Lakebase migration, `SCHEMA_VERSION` 2, and the Principle IX names (FR-004 to FR-015, FR-038). This phase also closes US1 scenario 1 (`load_type` default `cursor`) and gaps 6 and 11.

**Independent Test**: `pytest tests/state_store_test.py -k "fingerprint or ddl"` passes at `SCHEMA_VERSION == 2`. `pytest tests/cursor_engine_test.py -k "gap_6 or expression"` passes under both dialects. `pytest tests/job_parameters_test.py` rejects any `lakefed_ingest` or `edp` name.

### Position and expression

- [ ] T043 [P] [US3] Write failing tests in `tests/cursor_engine_test.py`: `encode_position(expr, kind, typ, value)` emits the four fields of contracts/position.md. `decode_position` returns `None` when `expr`, `kind`, or `type` is missing. The old `{col, type, value}` shape decodes to `None`. A `datetime` value carries at most three fractional digits
- [ ] T044 [P] [US3] Write `test_gap_6_expression_validation` cases in `tests/cursor_engine_test.py`, parametrized over both dialects: `validate_expression` accepts one to three columns, rejects zero or four columns with a message that states the count, rejects columns of two different types with a message that names both, rejects a nullable input under `null_cursor_policy` `reject` with a message that names the column, accepts the same nullable input under `seed_only`, rejects a `rowversion` kind over more than one column, and rejects any function other than `GREATEST` or `COALESCE`
- [ ] T045 [P] [US3] Write failing tests in `tests/planner_test.py`: `_route_cursor` returns `schema_reseed` when the position `expr` or `kind` differs byte for byte from the control row, and `cursor` when both match. `config_error` rejects `delete_mode` `soft_column` and `full_compare` as not implemented and rejects an unknown `cursor_kind`
- [ ] T046 [US3] Make T043 pass in `src/lakefed_ingest/cursor_engine.py` `encode_position` and `decode_position` (research R-04, FR-013)
- [ ] T047 [US3] Add `validate_expression(cfg, column_types)` to `src/lakefed_ingest/cursor_engine.py` and extend `column_type_sql` to use `SourceAdapter.cursor_type_sql` over the referenced columns. Make T044 pass (FR-011, FR-012, FR-015, FR-009)
- [ ] T047a [US3] Add `server_version(cfg, deps)` to `src/lakefed_ingest/source_io.py`: runs `adapter.server_version_sql()` through `_read` once per source, caches by `config_cache_key`, returns `None` when the adapter returns `None`. Test in `tests/source_io_test.py`: two calls for one source issue one query
- [ ] T047b [P] [US3] Write a failing test in `tests/cursor_engine_test.py`, parametrized over both dialects: under `null_cursor_policy` `seed_only`, `read_where` for an increment renders `(lo, hi]` over the expression so a row whose expression is NULL is not read, and the seed read has no cursor predicate (FR-009)
- [ ] T048 [US3] Replace every `cursor_column` read in `src/lakefed_ingest/cursor_engine.py` (`_col`, `read_where`, `high_watermark_sql`, `upsert_merge_sql`) with the `cursor_expression` text rendered through `SourceAdapter.greatest_expr` for the source side, with `server_version` from `source_io.server_version`. Make T047b pass
- [ ] T049 [US3] Make T045 pass in `src/lakefed_ingest/planner.py` `_route_cursor` and `config_error` (FR-014, FR-008, FR-007). Call `cursor_engine.validate_expression` from the plan stage in `src/lakefed_ingest/worker.py` before any source read
- [ ] T050 [US3] Add `sink_expr(expression)` to `src/lakefed_ingest/sink_writer.py` that maps `GREATEST` and `COALESCE` one to one for Spark SQL and raises for any other function. Add a test in `tests/sink_writer_test.py` (research R-07)

### Lakebase schema and state store

- [ ] T051 [US3] Write failing tests in `tests/state_store_test.py`: `SCHEMA_VERSION == 2` with the new fingerprint. `ddl_problems` reports nothing unreviewed. The DDL contains no `lakefed_ingest.` prefix. The DDL contains the columns `cursor_expression`, `cursor_kind`, `cursor_lookback_s`, `delete_mode`, `delete_min_interval_s`, `null_cursor_policy`, `last_delete_snapshot_at`. The DDL contains `drop column if exists` for `job_name`, `src_catalog`, `enable_iceberg_reads`, `sink_cluster_cols`, `source_instance_group`, `warehouse_id`, `ct_enabled`, `cdc_capture_instance`, `cursor_column`, `write_mode`, `ct_begin_version`, `pending_position`, `pending_run_id`. The DDL contains `drop table if exists lakefed_cursor.reseed_queue`. `RENAMES` is empty. Name the dropped-column assertion `test_gap_11_dead_columns_dropped` (SC-003)
- [ ] T052 [US3] Rewrite `src/lakefed_ingest/lakebase_schema.sql`: schema `lakefed_cursor`. Add the six control columns with the types and defaults of contracts/control_row.md (`cursor_expression text null`, `cursor_kind text null`, `cursor_lookback_s int null`, `delete_mode text not null default 'none'`, `delete_min_interval_s int null`, `null_cursor_policy text not null default 'reject'`). `load_type text not null default 'cursor'`. Add constraints `valid_load_type`, `valid_src_type`, `valid_cursor_kind`, `valid_delete_mode`, `valid_null_cursor_policy`, `valid_cursor_metadata`, `valid_full_metadata`, `valid_source`, `valid_remote_query`, `valid_intervals`, and unique index `control_sink_client_uq` on `(sink_catalog, sink_schema, sink_table, coalesce(client_id, ''))` with the exact rules of contracts/control_row.md. Drop constraints `valid_ct_metadata`, `valid_write_mode`, and any constraint that names `ct`, `cdc`, or `append`. Drop the listed columns with `drop column if exists`. Add `checkpoint.last_delete_snapshot_at timestamptz null`. Drop `reseed_queue`. Delete the old CT backfill statements. Every statement idempotent, no `DO $$`, no `CREATE INDEX CONCURRENTLY` (research R-11, FR-004, FR-005, FR-006, FR-010)
- [ ] T053 [US3] Update `src/lakefed_ingest/state_store.py`: `SCHEMA = "lakefed_cursor"`, `SCHEMA_VERSION = 2`, `RENAMES = []`, add each dropped column to `REVIEWED_NON_ADDITIVE`, read and write `cursor_expression`, `cursor_kind`, `cursor_lookback_s`, `delete_mode`, `delete_min_interval_s`, `null_cursor_policy` in the control select, and `last_delete_snapshot_at` in the checkpoint select. Make T051 pass
- [ ] T054 [US3] Update every fixture in `tests/planner_test.py`, `tests/cursor_engine_test.py`, `tests/worker_test.py`, and `tests/source_io_test.py` from `cursor_column` to `cursor_expression` plus `cursor_kind`

### Names (Principle IX)

- [ ] T055 [P] [US3] Add `test_no_resource_name_starts_with_lakefed_ingest_or_edp` to `tests/job_parameters_test.py`: read `databricks.yml` and every file in `resources/`. Collect the bundle name, every job name and key, every variable default, and the secret scope, catalog, schema, and Lakebase instance names. Fail on any name that starts with `lakefed_ingest` or `edp` (FR-038). The test fails before T056
- [ ] T056 [US3] Rename `resources/lakefed_ingest.yml` to `resources/lakefed_cursor.yml` and `resources/lakefed_ingest_setup.yml` to `resources/lakefed_cursor_setup.yml`. Set bundle name `lakefed_cursor`, job names and keys `lakefed_cursor` and `lakefed_cursor_setup`, variable defaults `db_instance` `lakefed-cursor`, catalog `lakefed_cursor_lakebase`, `secret_scope` `lakefed-cursor` in `databricks.yml` and both resource files. Update the resource path in `tests/job_parameters_test.py`. Make T055 pass (research R-10)
- [ ] T056a [US3] Add `scripts/validate.sh`: runs `pytest tests/job_parameters_test.py -q` then `databricks bundle validate "$@"`. Exit on the first failure. Replace every `databricks bundle validate` and `databricks bundle deploy` line in quickstart.md, docs/deploy.md, and README.md with `scripts/validate.sh -t <target> -p <profile>` followed by the deploy. The bundle has no hook that runs pytest, so this script is the deploy gate the constitution names (FR-038, Principle IX)
- [ ] T057 [US3] Run `pytest tests`, `rg -n "lakefed_ingest\." src/lakefed_ingest/lakebase_schema.sql`, and `rg -n "cursor_column" src tests`. Expect green, no grep output. Commit `feat(cursor): expression cursor, control columns, schema v2, own names`

**Checkpoint**: A three-column expression over nullable inputs plans, seeds, and reseeds on change. Schema is `lakefed_cursor` at version 2. No resource carries an old name.

---

## Phase 7: User Story 6 - Quiet tables cost little (Priority: P3, plan step 5b)

**Goal**: One batched `MAX` query per source database. Quiet tables advance in one bulk write without a source read. The merge updates only rows whose cursor value changed (FR-023, FR-024, FR-026. gap 10).

**Independent Test**: `pytest tests/cursor_engine_test.py tests/planner_test.py tests/sink_writer_test.py -k "gap_10 or batch or quiet or bulk"` passes under both dialects.

- [ ] T058 [P] [US6] Write `test_gap_10_merge_updates_only_changed_cursor` in `tests/sink_writer_test.py` and `tests/cursor_engine_test.py`: `upsert_merge_sql` contains `WHEN MATCHED AND <sink_expr over s> <> <sink_expr over t> THEN UPDATE`, with the expression rendered once per alias. A `client_id` row keeps the client predicate in the `ON` clause
- [ ] T059 [P] [US6] Write failing tests in `tests/cursor_engine_test.py`, parametrized over both dialects: `capture_batch(tables, adapter, read, chunk)` issues one query per `detection_chunk` tables using `SourceAdapter.max_batch_sql`, decodes one `hi` per alias and one clock from the single result row, and on a failed chunk falls back to one `high_watermark` query per table of that chunk
- [ ] T060 [P] [US6] Write failing tests in `tests/planner_test.py`: tables whose captured `hi` equals the position go to the `unchanged` list and `bulk_rows_to_write` advances their `last_success_at` in one write, skipping rows fresher than `freshness_interval_s`. Tables whose `hi` moved go to the `cursor` route with `hi` already set
- [ ] T061 [US6] Make T058 pass in `src/lakefed_ingest/cursor_engine.py` `upsert_merge_sql` using `sink_writer.sink_expr`
- [ ] T062 [US6] Add `capture_batch` to `src/lakefed_ingest/cursor_engine.py`. Make T059 pass (research R-06)
- [ ] T063 [US6] Make T060 pass in `src/lakefed_ingest/planner.py` `route_tables` and `bulk_rows_to_write`. The plan carries `hi` so the worker does not query `MAX` again
- [ ] T064 [US6] In `src/lakefed_ingest/ingest.ipynb`, group cursor candidates by source database, call `cursor_engine.capture_batch` once per group with `detection_chunk`, record one `ingest_event` with stage `capture` per source database, then call `store.bulk_advance` with `planner.bulk_rows_to_write` for the unchanged tables. Add a test in `tests/worker_test.py` that a plan with `hi` set performs no `MAX` read
- [ ] T065 [US6] Run `pytest tests`. Commit `perf(cursor): batched hi capture, bulk advance, conditional merge update`

**Checkpoint**: A source with N cursor tables sees one `MAX` query. A quiet table writes zero sink versions.

---

## Phase 8: User Story 5 - Capture deletes with a PK snapshot (Priority: P3, plan step 6)

**Goal**: `delete_mode` `pk_snapshot` reads all source primary keys at most once per interval and deletes sink rows absent from the source, scoped to `client_id` (FR-028 to FR-030. gap 1).

**Independent Test**: `pytest tests/worker_test.py -k snapshot` passes.

- [ ] T066 [P] [US5] Write `test_gap_1_pk_snapshot_deletes_missing_rows` and companion tests in `tests/worker_test.py`: with `delete_mode` `pk_snapshot`, phase `incremental`, and `last_delete_snapshot_at` older than `delete_min_interval_s`, the worker reads the PK snapshot after the increment, runs the delete, then calls `store.record_delete_snapshot`. Inside the interval it does no snapshot read. During phase `seeding` it skips the snapshot. With `delete_mode` `none` it never reads a snapshot. The delete runs after the increment merge and before the checkpoint advance of the snapshot time
- [ ] T067 [P] [US5] Write a failing test in `tests/sink_writer_test.py`: `delete_missing_sql(sink_fqn, view, cfg)` returns `DELETE FROM <sink> WHERE <client predicate> AND NOT EXISTS (SELECT 1 FROM <view> WHERE <pk match>)`, with the client predicate present only when `client_id` is set
- [ ] T068 [P] [US5] Write a failing test in `tests/source_io_test.py`: `read_pk_snapshot(cfg, deps)` selects only the primary key columns through `SourceAdapter.pk_snapshot_sql` and the `_read` path, and takes one slot from `deps.slots` and one permit from the per-source limiter
- [ ] T069 [P] [US5] Write a failing test in `tests/state_store_test.py`: `record_delete_snapshot_sql()` updates `last_delete_snapshot_at = now()` for one `control_id`
- [ ] T070 [US5] Add `delete_missing_sql` to `src/lakefed_ingest/sink_writer.py`. Make T067 pass (research R-08)
- [ ] T071 [US5] Add `read_pk_snapshot` to `src/lakefed_ingest/source_io.py`. Make T068 pass (FR-030)
- [ ] T072 [US5] Add `record_delete_snapshot` and `record_delete_snapshot_sql` to `src/lakefed_ingest/state_store.py`. Make T069 pass (FR-029)
- [ ] T073 [US5] Add the snapshot step to `src/lakefed_ingest/worker.py` after the increment: condition on `delete_mode == "pk_snapshot"`, phase `incremental`, and interval passed (default 86400 when `delete_min_interval_s` is NULL). Wrap the delete in `sink_writer.with_write_retry`. Record one `ingest_event` with stage `snapshot`. Make T066 pass (FR-028)
- [ ] T074 [US5] Run `pytest tests/worker_test.py -k snapshot` and `pytest tests`. Commit `feat(deletes): pk_snapshot delete mode`

**Checkpoint**: Source deletes reach the sink once per interval. Seeds and quiet intervals skip the snapshot.

---

## Phase 9: User Story 7 - Source credentials in three auth modes (Priority: P3, plan step 7)

**Goal**: The secret JSON carries `auth_mode` `sql`, `ntlm`, or `aad_sp`. The engine builds JDBC options per mode and never prints a secret (FR-034 to FR-036).

**Independent Test**: `pytest tests/source_io_test.py tests/source_adapters_test.py -k auth` passes with one case per spec scenario.

- [ ] T075 [P] [US7] Write failing tests in `tests/source_io_test.py` for `_extract_creds`: `{user, password}` with no mode yields `auth_mode` `sql`. `ntlm` without `domain` raises before any connect with a message that names `domain`. `aad_sp` without `tenant_id` raises and names `tenant_id`. An unknown mode raises with a message that lists `sql`, `ntlm`, `aad_sp`. No raised message contains the `password` or `client_secret` value
- [ ] T076 [P] [US7] Write failing tests in `tests/source_adapters_test.py` for `auth_options(creds)`: `SqlServerAdapter` returns `{user, password}` for `sql`. `{user, password, domain, authenticationScheme: NTLM, integratedSecurity: true}` for `ntlm`. `{authentication: ActiveDirectoryServicePrincipal, user: <client_id>, password: <client_secret>}` for `aad_sp`. `OracleAdapter` and `PostgresAdapter` return `{user, password}` for `sql` and raise `NotImplementedError` for `ntlm` and `aad_sp`
- [ ] T077 [P] [US7] Write a failing test in `tests/planner_test.py`: `config_error` rejects `auth_mode` `ntlm` or `aad_sp` on a row whose `src_type` is not `sqlserver`
- [ ] T077a [P] [US7] Write a failing test in `tests/worker_test.py`: a connect error whose text contains the `password` and `client_secret` values reaches the worker. The recorded `ingest_event.detail` and every `deps.log` line contain neither value (FR-036)
- [ ] T078 [US7] Make T075 pass in `src/lakefed_ingest/source_io.py` `_extract_creds` (research R-09)
- [ ] T079 [US7] Add `auth_options` to `SourceAdapter`, `SqlServerAdapter`, `OracleAdapter`, `PostgresAdapter`, and `MySqlAdapter` in `src/lakefed_ingest/source_adapters.py`. Make T076 pass
- [ ] T080 [US7] Call `adapter.auth_options(creds)` from `src/lakefed_ingest/source_io.py` `_jdbc_config` in place of the direct `user` and `password` options. Make T077 pass in `src/lakefed_ingest/planner.py` `config_error`. Make T077a pass: `src/lakefed_ingest/worker.py` passes error text through `source_io.redact(text, creds)` before any log or event write
- [ ] T081 [US7] Run `pytest tests` and `rg -n "password" src/lakefed_ingest/source_io.py | rg -v "creds\[|options\["`. Expect green, no grep output. Commit `feat(auth): sql, ntlm, and aad_sp secret modes`

**Checkpoint**: Each secret shape builds the right connection. Errors list the modes and hide the values.

---

## Phase 10: Polish, docs, and devtest run (plan step 8)

**Purpose**: Documentation in STE (FR-041, Principle VIII) and the live proof of US1 and US2 (SC-001, SC-004, SC-005, SC-006). The Lakeflow Connect baseline (call 13) ran in T012a and T034a.

- [ ] T082 [P] Rewrite `README.md` for the cursor-only engine: delete every mention of Change Tracking, CDC, append mode, `batch_append.py`, `ct_engine.py`, `write_mode`, and `cursor_column`. Describe `cursor_expression`, `cursor_kind`, lookback, skew, `delete_mode`, auth modes, bundle `lakefed_cursor`, schema `lakefed_cursor`, scope `lakefed-cursor`. STE sentences
- [ ] T083 [P] Rewrite the module header docstrings of `src/lakefed_ingest/cursor_engine.py`, `src/lakefed_ingest/source_io.py`, `src/lakefed_ingest/sink_writer.py`, `src/lakefed_ingest/worker.py`, `src/lakefed_ingest/planner.py`, `src/lakefed_ingest/load_strategies.py`, `src/lakefed_ingest/state_store.py`, and `src/lakefed_ingest/table_retry.py` to describe the cursor-only behavior in STE. No header cites a deleted file
- [ ] T084 [P] Update `docs/deploy.md`, `docs/monitoring.md`, and `docs/lakebase_schema.md` to the `lakefed_cursor` names, schema version 2, the new control columns, the `capture` and `snapshot` event stages, and the `alert=max_above_clock` and `note=inserts_only` details. Delete CT and append sections
- [ ] T085 [P] Update the root `CLAUDE.md` to the cursor-only modules, jobs, parameters, and run flow. Delete the CT, append, and `batch_append` lines. Confirm no `job_orchestration` mention remains after T011c
- [ ] T086 Add `tests/docs_test.py` that asserts `README.md`, `docs/deploy.md`, `docs/monitoring.md`, `docs/lakebase_schema.md`, `CLAUDE.md`, and every `src/lakefed_ingest/*.py` module docstring contain none of `ct_engine`, `batch_append`, `CHANGETABLE`, `change tracking`, `write_mode`, `cursor_column`. `docs/cursor_orchestration_analysis.md` and `docs/baseline.md` are history and are not scanned (gap 12, FR-041)
- [ ] T088 Run quickstart.md steps 1 to 4 with `--profile` chosen by the operator: `scripts/validate.sh -t devtest -p <profile>`, `databricks bundle deploy -t devtest`, `databricks bundle run lakefed_cursor_setup -t devtest`, store two secrets in scope `lakefed-cursor`, insert two control rows into `lakefed_cursor.control` with `delete_mode` `pk_snapshot` and `delete_min_interval_s` `0`
- [ ] T089 Run quickstart.md steps 5 to 9: seed, change the source (10 inserts, 5 updates, 5 deletes per table), increment, reconcile with a full outer join on the primary key, increment again with no change. Record the row counts, the `capture` event count per source database, the `inserts_only` note, and `DESCRIBE HISTORY` version counts in `tasks/todo.md` Review section (SC-001, SC-004, SC-005)
- [ ] T090 Run quickstart.md step 10 if a Windows login or an Entra ID service principal exists for the SQL Server source. Otherwise record the gap in `tasks/todo.md`
- [ ] T091 Run `databricks bundle summary -t devtest -p <profile>` and confirm no resource starts with `lakefed_ingest` or `edp` (SC-006). Run `git diff --stat 72255e0 -- src tests` and confirm about 1,600 or more deleted lines (SC-008). Run `pytest tests -rs` and confirm zero skips (SC-002). Commit `docs(cursor): cursor-only readme, headers, and devtest results`

**Checkpoint**: Both tables reconcile to zero differences. Docs match the code.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 Setup**: starts now. T001 to T006 touch the same files in sequence. T007, T008, T009 run in parallel after T003. Phase 1 ends with the SQL Server baseline (T012a). Phase 5 starts with the Oracle baseline (T034a).
- **Phase 2 Foundational**: after Phase 1. Blocks every story phase.
- **Phase 3 US1**: after Phase 2.
- **Phase 4 US4**: after Phase 3. Gap tests fail on the Phase 2 commit by design.
- **Phase 5 US2**: after Phase 4. Moves the dialect SQL that Phase 4 patched into adapters.
- **Phase 6 US3**: after Phase 5. The expression renders through `greatest_expr` from Phase 5.
- **Phase 7 US6**: after Phase 6. The batched `MAX` and the merge predicate use the expression.
- **Phase 8 US5**: after Phase 6. Needs `delete_mode` and `last_delete_snapshot_at` from the schema. Independent of Phase 7.
- **Phase 9 US7**: after Phase 2. Independent of Phases 4 to 8. The plan keeps it as step 7.
- **Phase 10 Polish**: after every other phase.

### User Story Dependencies

- **US1 (P1)**: plan-time proof in Phase 3. Live proof in Phase 10 needs US3 (schema) and US6 (capture).
- **US2 (P1)**: Phase 5 needs the patched `cursor_engine` from Phase 4. Live proof in Phase 10.
- **US3 (P2)**: needs US2 adapter methods.
- **US4 (P2)**: needs only Phase 2.
- **US5 (P3)**: needs US3 schema columns.
- **US6 (P3)**: needs US3 expression and position.
- **US7 (P3)**: needs only Phase 2.

### Within Each Phase

- Tests marked as failing are written first and fail before the implementation task.
- Adapter methods before engine calls. Schema before state store. State store before worker.
- Each phase ends with `pytest tests` green, zero skips, the grep check, and one commit.

### Parallel Opportunities

- Phase 1: T007, T008, T009 after T003.
- Phase 4: T022 to T026a, T032a, T032b together, then T027 to T031 in order.
- Phase 5: T035, T036, T041 together.
- Phase 6: T043, T044, T045, T047b, T055 together. T051 alone before T052.
- Phase 7: T058, T059, T060 together.
- Phase 8: T066 to T069 together, then T070 to T072 together, then T073.
- Phase 9: T075, T076, T077, T077a together. the whole phase can run beside Phases 4 to 8 on a branch.
- Phase 10: T082 to T085 together.

---

## Parallel Example: Phase 4 (US4)

```bash
# Write the five gap tests together:
Task: "test_gap_5_datetime_literal_three_digits in tests/cursor_engine_test.py"
Task: "test_gap_2_lookback_argument_replaces_env in tests/cursor_engine_test.py"
Task: "test_gap_4_hi_capped_at_source_clock_plus_skew in tests/cursor_engine_test.py"
Task: "test_gap_3_identity_reads_without_lookback in tests/cursor_engine_test.py"
Task: "test_gap_8_hi_below_position_raises in tests/cursor_engine_test.py"

# Then patch cursor_engine.py in task order T027 to T031.
```

## Parallel Example: Phase 8 (US5)

```bash
# Failing tests in four files at once:
Task: "snapshot route tests in tests/worker_test.py"
Task: "delete_missing_sql test in tests/sink_writer_test.py"
Task: "read_pk_snapshot test in tests/source_io_test.py"
Task: "record_delete_snapshot_sql test in tests/state_store_test.py"

# Then three implementations in three files at once:
Task: "delete_missing_sql in src/lakefed_ingest/sink_writer.py"
Task: "read_pk_snapshot in src/lakefed_ingest/source_io.py"
Task: "record_delete_snapshot in src/lakefed_ingest/state_store.py"
```

---

## Implementation Strategy

### MVP First (Phases 1 to 3)

1. Complete Phase 1. The engine is cursor and full only.
2. Complete Phase 2. The worker has a tested ordering invariant.
3. Complete Phase 3. A SQL Server cursor row plans, seeds, and increments with fakes.
4. Stop and validate: `pytest tests` green, grep checks clean.

### Incremental Delivery

1. Phase 4 makes the window safe. Commit.
2. Phase 5 makes Oracle first-class. Commit.
3. Phase 6 adds the expression cursor, schema v2, and own names. Commit.
4. Phase 7 cuts the cost of quiet tables. Commit.
5. Phase 8 adds deletes. Commit.
6. Phase 9 adds auth modes. Commit.
7. Phase 10 proves both engines live and fixes the docs. Commit.

### Parallel Team Strategy

Two people:

1. Both finish Phases 1 and 2.
2. Person A runs Phases 4, 5, 6, 7 in order. Person B runs Phase 9 on a branch, then Phase 10 docs tasks T082 to T085.
3. Person B rebases Phase 9 after Phase 6 lands. Both run Phase 8 and the devtest tasks together.

---

## Notes

- Every task names its file. Tasks with no starting point are rejected by the constitution.
- `pytest tests` with zero skips is the gate at the end of every phase.
- Never auto-select a Databricks profile. The operator passes `--profile`.
- No `edp` or `lakefed_ingest` resource changes at any point (SC-006).
- Write a one-line checkpoint in `tasks/todo.md` after each phase.
