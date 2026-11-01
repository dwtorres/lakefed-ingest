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

Date: 2026-10-31. Workspace: profile `ding-dong-ditch` (Azure, `adb-7405613017070986`).
Source: the feature 002 VM `lakefedsqltest` (`10.0.17.4`, private IP), reached from serverless
through the private network gateway (`ALL_TRAFFIC`). Rows landed. The baseline is recorded.

| Item | Value |
|---|---|
| Source connection | UC connection `lakefed_sqlserver_test`, type SQLSERVER, host `sqlserver.png-test.contoso.com:1433`, `trustServerCertificate true`, user `lakefed_sql` |
| Connection check | `remote_query('lakefed_sqlserver_test', ...)` on warehouse `a7c0f6a74986874e` returned `n = 10063` |
| Table | `lakefed_test.dbo.doc_attachment_079` (16 columns, `id` PK, `row_version rowversion`), 10,063 rows at seed |
| Pipeline | `lakefed_baseline_sqlserver_t012a`, id `e5303890-c006-4396-b01e-c3be376b3e67`, query-based, serverless, triggered, cursor `row_version`, `primary_keys [id]`, hard deletion scan 60 s. Spec: `specs/002-azure-sqlserver-test-source/contracts/lakeflow_pipeline.json` |
| Sink | `my_catalog.lakefed_cursor_devtest.sqlserver_baseline_doc_attachment_079`, streaming table |
| Seed | Update `f6a0ad5e`, COMPLETED. 10,063 rows, one MERGE `numTargetRowsInserted 10063` |
| Change | `change_generator.py` run_label `chg-us4` at 22:22:22 EDT: 10 inserts, 5 updates, 5 deletes, source 10,068 rows |
| Increment | COMPLETED. MERGE 10 inserted, MERGE 5 updated, DELETE. Target 10,068 rows. Reconcile by `id` and `row_version` through the connection: 0 differences |

| FR-011 value | Lakeflow Connect |
|---|---|
| Rows | source 10,068, sink 10,068 |
| Run duration | seed 68 s from `start-update` (22:13:24 EDT) to COMPLETED (22:14:32); increment 84 s (22:23:14 to 22:24:38) |
| Lag | seed: 51 s from `start-update` to the MERGE commit (22:14:15). Increment: 1 min 41 s from the last source change (22:22:22) to the MERGE commit (22:24:03), 49 s from `start-update` |
| Cost per run | pending billing (`system.billing.usage`, `usage_metadata.dlt_pipeline_id = 'e5303890-c006-4396-b01e-c3be376b3e67'`) |

### Facts established

- Serverless reaches a VNet target through the gateway by host name only. An IPv4 literal is refused
  (NAT64). A private DNS A record `sqlserver.png-test.contoso.com` in the gateway zone fixed it.
  A one-off socket probe notebook on serverless showed the IP refused and the name OPEN.
- The SQL Server query-based connector accepts a `rowversion` cursor column.
- The connector applied the 5 updates as updates. The cursor engine saw 0 updates on the same
  generator pattern in US2, because its `UPDATE TOP (5)` rows were deleted before the read. Both
  engines reconciled to zero differences.

### Cleanup

Deleted: pipeline `lakefed_baseline_sqlserver_t012a` (`pipelines get` returns not found) and
connection `lakefed_sqlserver_test`. Lakeflow Connect owns the sink, so `pipelines delete` dropped
it. The gateway stays on until the feature teardown (T056).

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

- Fixed (`cf303f7`): a quiet temporal table wrote one empty MERGE version per run, because the lookback
  window re-reads rows the sink already holds. `changed_rows_sql` probes the staged window with a
  LEFT ANTI JOIN; no differing row means no MERGE. Run 12 (no source change) added only the snapshot
  DELETE version (`delete_min_interval_s 0` in devtest). An empty snapshot DELETE still records a
  version; a production interval limits that to once per interval.
- `delete_min_interval_s 0` runs the PK snapshot every run. Use a real interval in production.
- The stored secret `oracle_devtest` carries text around the JSON object. `source_io._extract_creds`
  tolerates it (`947eb26`); store bare JSON and drop that fallback.

## Devtest run (T093)

Date: 2026-10-31. Workspace: profile `ding-dong-ditch` (Azure, `adb-7405613017070986`). Job
cluster: one `Standard_D4ds_v4`, single node. Source: SQL Server 2022 Developer on the VM
`lakefedsqltest` (`10.0.17.4`, private IP, feature 002), database `lakefed_test`, table
`dbo.doc_attachment_079` (16 columns, PK `id`, cursor `row_version rowversion`), 10,058 rows from
`build_synth_db.py` at `scale_factor 0.01`, Change Tracking off. Control row: `load_type cursor`,
`cursor_kind rowversion`, `delete_mode pk_snapshot`, `delete_min_interval_s 0`, secret
`lakefed-cursor/sqlserver_test_sql`. Sink `my_catalog.lakefed_cursor_devtest.sqlserver_doc_attachment_079`.

| Step | Run id (engine) | Result |
|---|---|---|
| Seed | 15 (13 and 14 failed, see below) | `seeded 1`, 10,058 rows, checkpoint `incremental`, position 12099 |
| Change source | `change_generator.py`, run_label `chg-us2`, 17:41:25 EDT | 10 inserts, 5 updates, 5 deletes, source 10,063 rows |
| Increment | 16 | `incremented 1`, one `capture` event, MERGE 10 inserted and 0 updated, snapshot DELETE 5, position 12109 |
| Reconcile | `reconcile.py` | `tables_checked 1`, `matched 1`, `mismatched []` (SC-002) |
| Increment, no change | 17 | `unchanged 1`, no MERGE version, one empty snapshot DELETE version |

MERGE updated 0 rows because `UPDATE TOP (5)` and `DELETE TOP (5)` have no ORDER BY. They hit the
same five rows, so the updated rows left the table before the read. The sink count is 10,063.

| FR-011 value | Cursor engine |
|---|---|
| Rows | source 10,063, sink 10,063 |
| Run duration | seed 4 min 2 s wall clock (24 s in the engine); increment 4 min 24 s (34 s in the engine) |
| Lag | 4 min 11 s from the last source change to the MERGE commit, almost all cluster start |
| Cost per run | pending billing (`system.billing.usage`, job `1081189664090958`) |

### Bugs the live run found (all fixed, unit tests added)

1. `select_list = '*'` fails at `ensure_sink` with `Invalid column name '*'`. The engine quotes
   each name. The contract example in `specs/001-cursor-only-engine/contracts/control_row.md` now
   lists columns.
2. `cursor_engine.high_watermark` read `row["src_now"]` on a Spark `Row` for a numeric kind. The
   query has no clock column for `rowversion`, so the seed failed with `KeyError: 'src_now'`. The
   unit tests used dicts with `.get`, which hid it. Fix: read `src_now` only for a temporal kind.
   Test: `test_numeric_high_watermark_reads_no_clock_column`.

### Build notes

- `terraform apply` failed twice before the VM existed: the subscription has 0 DSv5 cores in eastus
  (the VM is `Standard_D2ds_v4`), and `Microsoft.SqlVirtualMachine` was not registered.
- Source up in 19 minutes (US1/AC1), table loaded in 30 minutes (SC-001).

## Auth modes (T095)

Date: 2026-10-31. One control row, `secret_key` switched between three secrets in scope
`lakefed-cursor`. The source table was quiet, so each run reports `unchanged 1`: the capture query
and the PK snapshot ran over that login.

| Mode | Secret | Engine run | Result | Wall clock |
|---|---|---|---|---|
| `sql` | `sqlserver_test_sql` | 15, 16, 17 | seed, increment, no change | 4 min each |
| `ntlm` | `sqlserver_test_ntlm` (`domain lakefedsqltest`) | 18 | `ok 1`, `unchanged 1` | 4 min 3 s |
| `aad_sp` | `sqlserver_test_aad_sp` with `tenant_id` | 19 | `ok 1`, `unchanged 1` | 9 min 20 s |
| `aad_sp` | `sqlserver_test_aad_sp` without `tenant_id` | 20, 21 | 20 failed in the engine's secret check; 21 `ok 1` after the fix | 4 min |

`tenant_id` answer: **not needed** (FR-009). `SQLServerAdapter.auth_options` never passed it to the
driver, and run 19 connected. `AUTH_FIELDS` no longer requires it; `contracts/secret_json.md`,
`README.md`, and `CLAUDE.md` match.

US3/AC4: the ten job run logs and every `ingest_event.error` since run 13 hold no `password` or
`client_secret` word and no literal secret value.
