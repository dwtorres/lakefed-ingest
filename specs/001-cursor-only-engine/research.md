---
ontology: true
type: decision
domain: lakefed-cursor
summary: Phase 0 research decisions for the cursor-only engine plan, R-01 to R-12
tags: [research, decisions, cursor, oracle, sqlserver, lakebase, auth]
related: [plan, spec, ../../.specify/memory/constitution, ../../docs/cursor_orchestration_analysis]
status: draft
discovered: 2026-10-25
verified: false
workspace: lakefed-cursor
---

# Research: Cursor-Only Ingestion Engine

The Technical Context has no NEEDS CLARIFICATION marker. The spec and the constitution fix the
stack. This file records the design calls the plan makes where the spec leaves a choice.

## R-01: Keep versus rewrite per module

**Decision**: follow the survival table in analysis section 3.5.

| Module | Survival | Action |
|---|---|---|
| `parallel.py` | 100% | Keep. No change. |
| `partitions.py` | 100% | Keep. No change. |
| `source_adapters.py` | 100% | Keep. Add cursor methods. Make `OracleAdapter` real. |
| `table_retry.py` | 90% | Keep. Drop two CT markers. |
| `cursor_engine.py` | 85% | Keep. Patch five gaps. Move dialect SQL to adapters. |
| `state_store.py` | 75% | Keep. Drop pending and CT fields. Add one field. |
| `ct_engine.py` | 55% | Split into `source_io.py` and `sink_writer.py`. Delete the CT and append half. |
| `load_strategies.py` | 30% | Keep `CursorStrategy`, `FullStrategy`, `supported`, `unsupported_reason`. Delete the rest. |
| `planner.py` | 25% | Keep `config_error`, `route_tables` cursor and full branches, `_route_cursor`, `retry_plan`, `apply_seed_budget`, `bulk_rows_to_write`. Delete the rest. |
| `batch_append.py` | 0% | Delete. |
| `ingest.ipynb` | 35% | Keep parameters, lock, pool, plan call. Move the worker out. |

**Rationale**: constitution Principle I and VII. The table is the owner's estimate.

**Alternatives considered**: a fresh `cursor_only` package. Rejected by Principle I.

## R-02: Split of `ct_engine.py`

**Decision**: `source_io.py` holds the read side. `sink_writer.py` holds the write side.

`source_io.py`:

- `secret_ref`, `_extract_creds`, `_creds`, `_uc_host`, `_jdbc_config`, `config_cache_key`
- `jdbc_url`, `trust_server_certificate`, `snapshot_isolation`
- `_read`, `remote_query_sql`, `uses_remote_query`, `source_key`, `_unit`, `_units`
- the read side of `seed`

`sink_writer.py`:

- `sink_fqn`, `staging_table_name`, `quote_delta_ident`, `_with_table_suffix`
- `client_id`, `client_attrs`, `write_columns`, `merge_keys`, `client_predicate`, `_tag`, `_lit`
- `merge_on_cfg`, `merge_sql`, `create_sink_sql`, `ensure_sink_table`, `check_sink_mode`
- `insert_sql`, `insert_overwrite_sql`, `_insert_overwrite`, `missing_sink_columns`
- `with_write_retry`, `is_write_conflict`, `backoff_delay`
- the write side of `seed` and `_partitioned_seed`

**Rationale**: fixed decision call 1 names the two files. The split follows the read and write
sides that exist in the module today.

**Alternatives considered**: one file `source_io.py`. Rejected by call 1.

## R-03: Worker module shape

**Decision**: `worker.py` exposes `ingest_one(plan, run_id, deps, attempt=0)`. `deps` is a plain
object with `spark`, `store`, `slots`, `executor`, `log`, `params`. The notebook builds `deps` and
passes a bound function to `parallel.run_parallel`.

**Rationale**: a `deps` object lets the unit test pass fakes with no monkeypatch. Recommendation 2
in analysis section 5.

**Alternatives considered**: module globals set by the notebook, as `ct_engine._spark` does today.
Rejected because a test then needs monkeypatch on every call.

## R-04: Position JSON

**Decision**: `{"expr": "...", "kind": "db_timestamp", "type": "datetime2", "value": "..."}`.
`decode_position` rejects a position with no `expr`. There is no upgrade path from the old
`{col, type, value}` shape.

**Rationale**: the spec assumes the schema `lakefed_cursor` is new. No checkpoint row migrates.

**Alternatives considered**: accept `col` as an alias of `expr`. Rejected because no row needs it.

## R-05: Source clock and skew

**Decision**: `SourceAdapter.source_now_sql()` returns the server clock in the cursor type.
SQL Server: `SYSUTCDATETIME()` for `datetime2` and `datetimeoffset`, `GETDATE()` for `datetime` and
`smalldatetime`, matched to the column type. Oracle: `SYSTIMESTAMP` or `SYSDATE` by type.
PostgreSQL: `now()`. The batched `MAX` query returns the clock in the same result set. `skew_s` is a
job parameter, default 300.

**Rationale**: spec FR-018 and the edge case on clock skew. One round trip returns `hi` and the
clock, so they come from one moment.

**Alternatives considered**: cluster clock. Rejected by the spec assumption.

## R-06: Batched `MAX` query

**Decision**: one `SELECT` per source database with one subquery per table:
`SELECT (SELECT MAX(expr) FROM t1) AS c1, (SELECT MAX(expr) FROM t2) AS c2, ..., <source_now> AS now`.
Chunk at `detection_chunk` tables per query. A failed chunk falls back to one query per table.

**Rationale**: FR-023. Reuses the `detection_chunk` parameter and the per-table fallback that the CT
detection path has today.

**Alternatives considered**: `UNION ALL` of one row per table. Rejected because a scalar subquery
per column is one result row and simpler to decode.

## R-07: Merge change predicate

**Decision**: `WHEN MATCHED AND <expr over s> <> <expr over t> THEN UPDATE`. The expression is
rendered twice, once with the source alias, once with the target alias. The adapter method
`greatest_expr` produces the dialect form. The sink side runs in Spark SQL. `sink_writer.sink_expr` rewrites the operator's
expression for the sink. It maps `GREATEST` and `COALESCE` one to one. Any other function in the
expression fails at plan time.

**Rationale**: FR-026 and spec assumption on the change check. Spark SQL has `GREATEST` and
`COALESCE`.

**Alternatives considered**: a row hash. Rejected by the spec assumption.

## R-08: Delete snapshot

**Decision**: `source_io.read_pk_snapshot` selects the PK columns through the same `_read` path and
slot. `sink_writer.delete_missing_sql` is
`DELETE FROM sink WHERE <client predicate> AND NOT EXISTS (SELECT 1 FROM view WHERE pk match)`.
The checkpoint stores `last_delete_snapshot_at`. If the phase is `seeding`, the worker skips the snapshot. If the interval has not passed,
the worker skips the snapshot.

**Rationale**: FR-028 to FR-030, recommendation 9.

**Alternatives considered**: `MERGE ... WHEN NOT MATCHED BY SOURCE THEN DELETE`. Rejected because the
PK view holds every source key, so the anti-join `DELETE` is one statement with no update branch.

## R-09: Auth modes

**Decision**: the secret JSON gains `auth_mode`. SQL Server JDBC options per mode:

| Mode | Options |
|---|---|
| `sql` | `user`, `password` |
| `ntlm` | `user`, `password`, `domain`, `authenticationScheme=NTLM`, `integratedSecurity=true` |
| `aad_sp` | `authentication=ActiveDirectoryServicePrincipal`, `user=<client_id>`, `password=<client_secret>`. The tenant goes in the user value as `<client_id>@<tenant_id>` if the driver version needs it. |

`source_io._extract_creds` makes sure that the required fields per mode are present before any connect. Oracle accepts
`sql` only and fails `ntlm` and `aad_sp` at plan time. The `remote_query` path does not use the
secret, so auth mode does not apply there.

**Rationale**: FR-034 to FR-036. The Microsoft JDBC driver on Databricks Runtime supports these
options. The exact `tenantId` handling is unverified for the Runtime driver version and is a
quickstart check.

**Alternatives considered**: a separate secret key per mode. Rejected because one JSON per source
database is the existing contract.

## R-10: Names

**Decision**: the names below.

| Resource | Name |
|---|---|
| Bundle | `lakefed_cursor` |
| Jobs | `lakefed_cursor`, `lakefed_cursor_setup` |
| Lakebase schema | `lakefed_cursor` |
| Lakebase instance variable default | `lakefed-cursor` |
| Catalog variable default | `lakefed_cursor_lakebase` |
| Secret scope | `lakefed-cursor` |
| Resource files | `resources/lakefed_cursor.yml`, `resources/lakefed_cursor_setup.yml` |
 The Python package path
`src/lakefed_ingest` does not change.

**Rationale**: Principle IX names deployable resources. The package path is not a deployed
resource. A package rename touches every import and every test for no runtime gain.

**Alternatives considered**: rename the package to `src/lakefed_cursor`. Rejected under Principle
I, because it is a replacement with no behavior change.

## R-11: Schema version

**Decision**: `SCHEMA_VERSION` goes from 1 to 2 in step 5. `lakebase_schema.sql` rewrites every
`lakefed_ingest.` prefix to `lakefed_cursor.`. Dropped columns use `drop column if exists` and
go in `REVIEWED_NON_ADDITIVE`. The old migration statements that backfill CT fields are deleted.
`RENAMES` is empty.

**Rationale**: the fingerprint test and `ddl_problems` in `tests/state_store_test.py` enforce the
form. A new schema has no rows, so the drops are safe.

**Alternatives considered**: keep `lakefed_ingest.` and set the schema by a variable. Rejected by
Principle IX.

## R-12: Job parameters

**Decision**: remove `snapshot_isolation`, `append_batch_size`, `reseed_on_new_columns`. Keep
`detection_chunk` for the batched `MAX`. Add `cursor_lookback_s` default 3600 and `cursor_skew_s`
default 300 as job parameters with bundle variables of the same name. `tests/job_parameters_test.py`
keeps every widget aligned.

**Rationale**: FR-017, spec assumptions on defaults. `reseed_on_new_columns` was a CT schema-evolution
switch. The cursor reseed rule is the expression text, FR-014.

**Alternatives considered**: keep `reseed_on_new_columns` for cursor tables. Rejected because
`_route_cursor` reseeds on a changed `select_list` today and the spec does not ask to change that.
