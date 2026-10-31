---
ontology: true
type: mop
domain: lakefed-cursor
summary: Lakeflow Connect query-based baseline per source engine, cost and lag, fixed decision call 13
tags: [baseline, lakeflow-connect, oracle, sqlserver, devtest, cost, lag]
related: [../specs/001-cursor-only-engine/quickstart, ../specs/001-cursor-only-engine/plan, ../specs/001-cursor-only-engine/tasks]
status: complete
discovered: 2026-10-26
verified: true
workspace: lakefed-cursor
---

# Lakeflow Connect Baseline

This file records the Lakeflow Connect baseline for each source engine (T012a, T034a).
Lakeflow Connect is not a dependency of the cursor engine.

## SQL Server (T012a)

On hold, no source. No SQL Server database is reachable from the `devtest` workspace.

## Oracle (T034a)

Date: 2026-10-31. Workspace: profile `ding-dong-ditch` (Azure, `adb-7405613017070986`).
Operator: verbena1@gmail.com. Rows landed. The baseline is recorded.

| Item | Value |
|---|---|
| Source connection | UC connection `oracle_freesql`, type ORACLE, `db.freesql.com:2484`, TLS, service `23ai_34ui2`, user `DTORRES_SCHEMA_AI814` |
| Connection check | `remote_query('oracle_freesql', ...)` on warehouse `a7c0f6a74986874e` returned `n = 105` |
| Table | `DTORRES_SCHEMA_AI814.LAKEFED_DEVTEST` (ID NUMBER PK, NAME VARCHAR2, UPDATED_AT TIMESTAMP(6)), 105 rows |
| Pipeline | `lakefed_baseline_oracle_t034a`, id `34db0f00-f0b7-4d1f-b99a-01df60574010`, Lakeflow Connect query-based, `source_type = ORACLE`, serverless, triggered, channel PREVIEW |
| Sink | `my_catalog.lakefed_cursor_devtest.oracle_baseline_lakefed_devtest`, streaming table |
| Result | Attempt 1 FAILED (spec error, see below). Attempt 2 COMPLETED. 105 rows landed, one MERGE with `numSourceRows 105`, `numTargetRowsInserted 105` |
| Run duration | Attempt 2: 46 s from `start-update` (10:35:17 EDT) to COMPLETED (10:36:03 EDT). States: INITIALIZING, SETTING_UP_TABLES, RUNNING, COMPLETED |
| Lag | 39 s from `start-update` (14:35:17Z) to the data commit (Delta version 2, MERGE, 14:35:56Z). Table created at 14:35:38Z. MERGE `executionTimeMs 11513`, `materializeSourceTimeMs 8974` |
| DBU / cost | Not visible yet. `system.billing.usage` returned no row for `usage_metadata.dlt_pipeline_id` at 10:37 EDT (billing lags by hours) |

### Errors

Attempt 1 (update `4275e705`) failed after 34 s with:

```
[SAAS_CONNECTOR_INTERNAL_ERROR] An internal error occurred in the ORACLE connector.
Please file a ticket. Error code: UNHANDLED.
```

The nested exception (`java.lang.IllegalArgumentException`) names the cause:

```
service_name cannot be empty and needs to be configured as the source_catalog in the pipeline spec.
```

The connector auto-retried once (`cause = RETRY_ON_FAILURE`) with the same error before the stop.
Fix: set `objects[].table.source_catalog` to the Oracle service name (`23ai_34ui2`). The 2026-10-26
attempt in workspace `dbc-8e058692-373e` showed the same top-level error; its root cause was most
likely this missing field, not the connection protocol.

### Facts established

- The Oracle query-based connector needs `source_catalog = <service_name>` on each table object.
  The top-level `SAAS_CONNECTOR_INTERNAL_ERROR` hides this. Read `error.exceptions[1].message`.
- A TLS (TCPS, port 2484) UC connection works with the connector.
- One run for 105 rows: 46 s wall clock, 39 s to data. The cursor engine job takes about 7 minutes
  per run, almost all cluster start (see the devtest run below).

### Cleanup

Deleted: pipeline `lakefed_baseline_oracle_t034a`. `pipelines get` returns not found.
Also removed: the sink streaming table. Lakeflow Connect owns the table, so `pipelines delete`
dropped it. Kept: connection `oracle_freesql`, foreign catalog `oracle_freesql_catalog`, schema
`my_catalog.lakefed_cursor_devtest` (holds the engine sink `oracle_lakefed_devtest`).

## Devtest run 2026-10-31 (T089, the cursor engine itself)

Workspace: profile `ding-dong-ditch` (Azure, `adb-7405613017070986`). Job cluster: one
`Standard_D4ds_v4`, single node. Source: Oracle `db.freesql.com:2484` (TCPS), service
`23ai_34ui2`, schema `DTORRES_SCHEMA_AI814`, table `LAKEFED_DEVTEST` (ID NUMBER PK, NAME,
UPDATED_AT TIMESTAMP(6)), 100 rows created by the probe notebook. Control row: `load_type cursor`,
`cursor_expression UPDATED_AT`, `cursor_kind app_timestamp`, secret `lakefed-cursor/oracle_devtest`,
`jdbc_options {"protocol": "tcps"}`. Sink `my_catalog.lakefed_cursor_devtest.oracle_lakefed_devtest`.

| Step | Run id (engine / job) | Result |
|---|---|---|
| Seed | 6 / 1038635287192307 group of fixes, final seed 09:03 EDT | `seeded 1`, 100 rows, checkpoint `incremental` |
| Change source | probe run 183740887546180 | 10 inserts (101-110), 5 updates (1-5), 5 deletes (96-100), source 105 rows |
| Increment | 7 | `incremented 1`, `capture_queries_sources 1`, MERGE 10 inserted, 5 updated |
| Reconcile | probe run 926490377508270 | 110 sink, 105 source, 5 differences: exactly IDs 96-100 (`delete_mode none`) |
| `delete_mode pk_snapshot`, `delete_min_interval_s 0` | 8 | `incremented 1`, DELETE of the 5 rows, `last_delete_snapshot_at` set |
| Reconcile | probe run 91767964247780 | 105 sink, 105 source, 0 differences (SC-001) |
| Increment, no change | 9 | `incremented 1`, sink versions 5 and 6 added (empty MERGE, snapshot DELETE) |

Each job run takes about 7 minutes, almost all cluster start. One `MAX` query per source
database per run (SC-004).

### Bugs the live run found (all fixed, unit tests added)

1. Oracle upper-cases unquoted aliases, so `row["type_name"]` failed. Fix: `OracleAdapter.result_alias`
   quotes every alias the engine reads (`a27e838`).
2. `nullable_sql` referenced the alias unquoted (`ORA-00904`). Fix in the same change.
3. Oracle raises `ORA-00937` for a scalar subquery or a clock beside `MAX()` with no `GROUP BY`.
   Fix: `high_watermark_sql` is scalar subqueries only, every dialect (`65966ed`).
4. The runtime does not ship the Oracle JDBC driver. The job installs `ojdbc11` from Maven (`ca8b101`).
5. TCPS needs `jdbc:oracle:thin:@tcps://host:port/service`. `jdbc_options.protocol` selects it (`ca8b101`).

### Open findings

- A quiet temporal table still writes one empty MERGE version per run (SC quiet-table goal not met for
  temporal cursors; the lookback window is always read). Decide: skip the MERGE when the window
  read is empty, or accept the empty commit.
- `delete_min_interval_s 0` runs the PK snapshot every run. Use a real interval in production.
- The stored secret `oracle_devtest` carries text around the JSON object. `source_io._extract_creds`
  tolerates it (`947eb26`); store bare JSON and drop that fallback.
