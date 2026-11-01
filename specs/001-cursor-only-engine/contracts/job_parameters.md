---
ontology: true
type: spec
domain: lakefed-cursor
summary: Job parameters and bundle variables of the lakefed_cursor ingestion job
tags: [contract, job-parameters, bundle, databricks, lookback, skew]
related: [../plan, ../research, control_row]
status: draft
discovered: 2026-10-25
verified: false
workspace: lakefed-cursor
---

# Contract: Job Parameters

Job `lakefed_cursor`, file `resources/lakefed_cursor.yml`. Each tuning parameter defaults to the
bundle variable of the same name in `databricks.yml`. `tests/job_parameters_test.py` enforces the
alignment and the name rule.

## Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `task_collection` | none | the collection this run ingests |
| `db_instance` | `lakefed-cursor` | Lakebase instance |
| `db_name` | `databricks_postgres` | Lakebase database |
| `secret_scope` | `lakefed-cursor` | scope of the source secrets |
| `parallelism` | unchanged | concurrent query units |
| `trust_server_certificate` | unchanged | JDBC TLS setting |
| `cursor_lookback_s` | `3600` | default lookback for temporal cursors. The control row overrides it |
| `cursor_skew_s` | `300` | seconds `hi` can exceed the source clock |
| `detection_chunk` | unchanged | tables per batched `MAX` query |
| `max_seeds_per_run` | unchanged | seed budget |
| `freshness_interval_s` | unchanged | skip no-op bulk advance writes newer than this |
| `max_queries_per_source` | unchanged | per-source cap |
| `table_retries` | `0` | retries per table |
| `table_retry_budget` | unchanged | retries per run |

## Removed parameters

`snapshot_isolation`, `append_batch_size`, `reseed_on_new_columns`.

## Name rule

A test reads `databricks.yml` and every file in `resources/`. It collects these names:

- the bundle name
- every job name and job key
- every variable default
- the secret scope, catalog, schema, and Lakebase instance names

If any collected name starts with `lakefed_ingest` or `edp`, the test fails.
