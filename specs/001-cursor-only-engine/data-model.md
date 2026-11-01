---
ontology: true
type: spec
domain: lakefed-cursor
summary: Data model for the cursor-only engine, control row, checkpoint, position, run, secret JSON
tags: [data-model, lakebase, control, checkpoint, cursor, schema-version]
related: [plan, spec, research, contracts/control_row, contracts/position, contracts/secret_json]
status: draft
discovered: 2026-10-25
verified: false
workspace: lakefed-cursor
---

# Data Model: Cursor-Only Ingestion Engine

Lakebase schema `lakefed_cursor`. `state_store.SCHEMA_VERSION` is 2. Column details live in
[contracts/control_row.md](contracts/control_row.md) and [contracts/position.md](contracts/position.md).

## Entities

### Control row (`lakefed_cursor.control`)

One row per source table and sink. Fields by group:

| Group | Fields |
|---|---|
| Identity | `id`, `task_collection`, `task_enabled`, `tier`, `priority` |
| Source | `src_type`, `src_host`, `src_port`, `secret_key`, `src_connection`, `use_remote_query`, `src_database`, `src_schema`, `src_table`, `jdbc_options` |
| Sink | `sink_catalog`, `sink_schema`, `sink_table`, `client_id`, `client_attrs` |
| Read | `primary_key`, `select_list`, `load_type`, `load_partitioned`, `partition_col`, `partition_size_mb` |
| Cursor | `cursor_expression`, `cursor_kind`, `cursor_lookback_s`, `null_cursor_policy` |
| Deletes | `delete_mode`, `delete_min_interval_s` |

Removed from the root version: `job_name`, `src_catalog`, `enable_iceberg_reads`,
`sink_cluster_cols`, `source_instance_group`, `warehouse_id`, `ct_enabled`, `cdc_capture_instance`,
`cursor_column`, `write_mode`.

Rules:

- `load_type` is `cursor` or `full`. Default `cursor`.
- A `cursor` row has `primary_key`, `cursor_expression`, and `cursor_kind`.
- A `full` row has no cursor fields.
- `cursor_kind` is `rowversion`, `db_timestamp`, `app_timestamp`, or `identity`.
- A `rowversion` expression names one column.
- `delete_mode` is `none`, `soft_column`, `pk_snapshot`, or `full_compare`. Default `none`.
- `null_cursor_policy` is `reject` or `seed_only`. Default `reject`.
- `cursor_lookback_s` and `delete_min_interval_s` are NULL or a non-negative integer. NULL means the job default.
- One row per `(sink_catalog, sink_schema, sink_table, client_id)`.

### Checkpoint (`lakefed_cursor.checkpoint`)

One row per control row, keyed by `control_id`.

| Field | Meaning |
|---|---|
| `strategy` | `cursor` or `full` |
| `position` | JSON per [contracts/position.md](contracts/position.md). NULL before the first seed |
| `phase` | `seeding`, `incremental`, or `loaded` |
| `status` | `ok`, `running`, `failed`, or `quarantined` |
| `detail` | last error or note |
| `select_list` | the list the sink was built with. A change reseeds |
| `last_success_at` | freshness |
| `last_delete_snapshot_at` | last PK snapshot time. NULL means never |
| `updated_at` | last write |

Removed: `ct_begin_version`, `pending_position`, `pending_run_id`. Table `reseed_queue` is dropped.

State transitions for `strategy = cursor`:

```text
(no row) --seed ok--> incremental, position = hi captured before the seed read (temporal: hi minus the seed duration)
incremental --increment ok--> incremental, position = hi
incremental --expression text changed--> seeding --seed ok--> incremental
incremental --select_list changed--> seeding --seed ok--> incremental
incremental --hi below position--> status failed, position unchanged
incremental --quiet (hi equals position, identity or rowversion)--> incremental, last_success_at = now, no source read
incremental --hi equals position, temporal--> reads (hi - lookback, hi], then as increment ok
incremental --snapshot ok--> incremental, last_delete_snapshot_at = now
```

The seed writes `phase = incremental` only after the sink commit. A failed seed leaves no position. A temporal seed records `hi` minus the seed duration (`cursor_engine.seed_position`), so the first increment re-reads rows committed during the seed. An identity seed has no lookback, so rows committed during the seed with a value at or below `hi` are missed. The run log states this with the `inserts_only` note.

State transitions for `strategy = full`: `(no row) --reload ok--> loaded`, then `loaded --reload ok--> loaded`.

### Position

JSON in `checkpoint.position`. Fields `expr`, `kind`, `type`, `value`. See
[contracts/position.md](contracts/position.md).

### Run (`lakefed_cursor.ingest_run`, `lakefed_cursor.ingest_event`)

Unchanged shape from the root version. `ingest_event.position_from` and `position_to` hold the
position JSON text. New event stages: `capture` for the batched `MAX`, `snapshot` for the PK
snapshot. `alert=max_above_clock` is written to the checkpoint `detail` and the run log. `inserts_only` is written to the run log once per run per table. Neither is an `ingest_event` column.

### Secret JSON

One secret per source database in scope `lakefed-cursor`. See
[contracts/secret_json.md](contracts/secret_json.md). Not stored in Lakebase.

## Relationships

- `checkpoint.control_id` references `control.id`. One to one.
- `ingest_event.control_id` references `control.id`. Many to one.
- `ingest_event.run_id` references `ingest_run.run_id`. Many to one.
- `control.secret_key` names a key in the secret scope. Many control rows can share one key.

## Validation at plan time

`planner.config_error` and `cursor_engine.validate_expression` reject a row before any source read:

1. `load_type` not in `cursor`, `full`.
2. `src_type` and `load_type` pair not supported. MySQL cursor is a placeholder.
3. `cursor` row with no `primary_key`.
4. Expression references zero or more than three columns.
5. Referenced columns differ in type.
6. A referenced column is nullable and `null_cursor_policy` is `reject`.
7. `cursor_kind` is `rowversion` and the expression is not one `rowversion` column.
8. `delete_mode` is `soft_column` or `full_compare`.
9. Secret JSON `auth_mode` unknown, or a required field for the mode is missing.
10. `auth_mode` is `ntlm` or `aad_sp` and `src_type` is not `sqlserver`.

## Migration

`lakebase_schema.sql` creates schema `lakefed_cursor`. Every statement is idempotent. Dropped columns
use `drop column if exists` and are listed in `REVIEWED_NON_ADDITIVE`. `RENAMES` is empty. The
fingerprint for version 2 is pinned in `tests/state_store_test.py`. No data migrates from
`lakefed_ingest`.
