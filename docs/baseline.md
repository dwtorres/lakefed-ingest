---
ontology: true
type: mop
domain: lakefed-cursor
summary: Lakeflow Connect query-based baseline per source engine, cost and lag, fixed decision call 13
tags: [baseline, lakeflow-connect, oracle, sqlserver, devtest, cost, lag]
related: [../specs/001-cursor-only-engine/quickstart, ../specs/001-cursor-only-engine/plan, ../specs/001-cursor-only-engine/tasks]
status: partial
discovered: 2026-10-26
verified: false
workspace: lakefed-cursor
---

# Lakeflow Connect Baseline

This file records the Lakeflow Connect baseline for each source engine (T012a, T034a).
Lakeflow Connect is not a dependency of the cursor engine.

## SQL Server (T012a)

On hold, no source. No SQL Server database is reachable from the `devtest` workspace.

## Oracle (T034a)

Date: 2026-10-26. Workspace: profile `dbc-8e058692-373e`. Operator: verbena1@gmail.com.

| Item | Value |
|---|---|
| Source connection | UC connection `freedb`, type ORACLE, `db.freesql.com:2484`, service `23ai_34ui2`, credential USERNAME_PASSWORD, `encryption_protocol = native_network_encryption` |
| Table | `SYS.DUAL` (placeholder). Schema discovery failed, see below |
| Pipeline | `lakefed_baseline_oracle_t034a`, id `5bf48105-dc75-43bc-a91b-41de13189274`, Lakeflow Connect query-based, `source_type = ORACLE`, serverless, triggered, channel PREVIEW |
| Sink | `devtest_edp_bronze.lakefed_cursor_devtest.oracle_baseline_dual` (never created) |
| Result | FAILED. Zero rows landed |
| Run duration | 47 s from `start-update` (19:57:53 EDT) to FAILED (19:58:40 EDT). The connector then auto-retried 5 times (`cause = RETRY_ON_FAILURE`), each failed the same way, before the stop |
| Lag | Not measurable, no data landed |
| DBU / cost | Not visible. `system.billing.usage` returned no row for the pipeline id at 20:12 EDT (billing lags by hours) |

### Errors

1. Schema discovery through a temporary foreign catalog `lakefed_baseline_oracle` on `freedb`:
   `databricks schemas list` returned an empty list. SQL on the serverless warehouse returned:

   ```
   BAD_REQUEST [FAILED_JDBC.CONNECTION] Failed JDBC jdbc:oracle:*********(redacted)
   on the operation: Failed to connect to the database. SQLSTATE: HV000
   ```

   TCP to `db.freesql.com` ports 2484 and 1521 is open from the operator's machine. Probable cause:
   port 2484 is the Oracle TCPS port, but the connection sets `native_network_encryption`, which
   belongs with port 1521. Fix the connection (port 1521 with native encryption, or TCPS on 2484)
   before the next attempt. The connection was not changed in this task.

2. Pipeline update (every attempt):

   ```
   [SAAS_CONNECTOR_INTERNAL_ERROR] An internal error occurred in the ORACLE connector.
   Please file a ticket. Error code: UNHANDLED.
   ```

### Facts established

- The workspace offers the Oracle Lakeflow Connect query-based connector. `pipelines create` with
  `ingestion_definition.source_type = ORACLE` succeeded.
- The baseline run itself did not succeed. Cost and lag are not recorded.

### Cleanup

Deleted: pipeline `lakefed_baseline_oracle_t034a`, foreign catalog `lakefed_baseline_oracle`.
Kept: schema `devtest_edp_bronze.lakefed_cursor_devtest` (empty, no sink table was written).

### Rerun

1. Fix the `freedb` connection protocol and port.
2. Verify with `SHOW SCHEMAS IN <foreign catalog>` on the serverless warehouse.
3. Pick one small table with a timestamp or numeric id column.
4. Create the pipeline as above with that table. Run once. Record duration, rows and DBUs here.
5. Delete the pipeline and the foreign catalog.
