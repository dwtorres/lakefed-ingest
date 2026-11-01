---
ontology: true
type: spec
domain: lakefed-cursor
summary: Control row contract, columns, defaults, and CHECK constraints of lakefed_cursor.control
tags: [contract, control-table, lakebase, cursor, constraints]
related: [../data-model, ../plan, position]
status: draft
discovered: 2026-10-25
verified: false
workspace: lakefed-cursor
---

# Contract: Control Row

Table `lakefed_cursor.control`. File `src/lakefed_ingest/lakebase_schema.sql`.

## New columns

| Column | Type | Default | Values |
|---|---|---|---|
| `cursor_expression` | text | NULL | one SQL expression over one to three source columns |
| `cursor_kind` | text | NULL | `rowversion`, `db_timestamp`, `app_timestamp`, `identity` |
| `cursor_lookback_s` | int | NULL | NULL or >= 0. NULL means the job parameter |
| `delete_mode` | text | `none` | `none`, `soft_column`, `pk_snapshot`, `full_compare` |
| `delete_min_interval_s` | int | NULL | NULL or >= 0. NULL means 86400 |
| `null_cursor_policy` | text | `reject` | `reject`, `seed_only` |

## Changed columns

| Column | Change |
|---|---|
| `load_type` | default `cursor`. Check `load_type in ('cursor', 'full')` |

## Dropped columns

`job_name`, `src_catalog`, `enable_iceberg_reads`, `sink_cluster_cols`, `source_instance_group`,
`warehouse_id`, `ct_enabled`, `cdc_capture_instance`, `cursor_column`, `write_mode`.

## Constraints

| Name | Rule |
|---|---|
| `valid_load_type` | `load_type in ('cursor', 'full')` |
| `valid_src_type` | `src_type in ('sqlserver', 'oracle', 'postgresql', 'mysql')` |
| `valid_cursor_kind` | `cursor_kind is null or cursor_kind in (...)` |
| `valid_delete_mode` | `delete_mode in (...)` |
| `valid_null_cursor_policy` | `null_cursor_policy in ('reject', 'seed_only')` |
| `valid_cursor_metadata` | `load_type <> 'cursor' or (primary_key is not null and cursor_expression is not null and cursor_kind is not null)` |
| `valid_full_metadata` | `load_type <> 'full' or (cursor_expression is null and cursor_kind is null)` |
| `valid_source` | `src_host is not null or src_connection is not null` |
| `valid_remote_query` | `not use_remote_query or src_connection is not null` |
| `valid_intervals` | `coalesce(cursor_lookback_s, 0) >= 0 and coalesce(delete_min_interval_s, 0) >= 0` |
| `control_sink_client_uq` | unique on `(sink_catalog, sink_schema, sink_table, coalesce(client_id, ''))` |

Dropped constraints: `valid_ct_metadata`, `valid_write_mode`, and any constraint that names `ct`,
`cdc`, or `append`.

## Example row

```sql
insert into lakefed_cursor.control
  (task_collection, src_type, src_host, secret_key, src_database, src_schema, src_table,
   sink_catalog, sink_schema, sink_table, primary_key, select_list,
   cursor_expression, cursor_kind, cursor_lookback_s)
values
  ('devtest', 'oracle', 'ora-host', 'ora_devtest', 'ORCLPDB1', 'HR', 'EMPLOYEES',
   'devtest', 'lakefed_cursor', 'hr_employees', 'EMPLOYEE_ID', 'EMPLOYEE_ID, FIRST_NAME, UPDATED_AT, CREATED_AT',
   'GREATEST(COALESCE(UPDATED_AT, DATE ''1900-01-01''), COALESCE(CREATED_AT, DATE ''1900-01-01''))',
   'db_timestamp', 900);
```
