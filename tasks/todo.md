---
ontology: true
type: plan
domain: lakefed-ingest
summary: Two week viability test of lakefed as the orchestration engine for edp database sources, with caboodle_poc side by side in devtest and then rl_solutions
tags: [viability, caboodle, rl-solutions, cursor, overlap, oracle, side-by-side]
status: active
discovered: 2026-10-25
related: [cursor_engine, planner, source_adapters, edp-data-engineering]
---

# Two week viability test

Goal: decide with evidence whether lakefed replaces the edp source to bronze path for database sources. The decision rule is at the end. Nothing in edp changes during the test.

## Week 1: make lakefed able to run caboodle_poc

- [x] 1. Temporal cursor overlap. Add `LAKEFED_CURSOR_OVERLAP_S` (environment variable, default `600`) to `cursor_engine.py`. If the cursor kind is `temporal`, `read_where` subtracts the overlap from `lo` in Python before the literal is built. Rowversion and numeric kinds keep the exact bound. Verify: a new test in `tests/cursor_engine_test.py` asserts the shifted bound for temporal and the exact bound for the other two kinds.
- [x] 2. Seed position for a temporal cursor. `cursor_seed` records the high watermark minus the seed duration, so a row that committed during a long seed is inside the first increment window. Verify: a planner test with a fake seed that takes 2 hours asserts the first `cursor` route reads from `hi - 2h - overlap`.
- [ ] 3. Register caboodle_poc in a devtest Lakebase control table. 48 `incremental_watermark` datasets become `load_type cursor`, `cursor_column _LastUpdatedInstant`, `primary_key` from the edp metadata. The 2 `snapshot` datasets become `load_type full`. `DateDim` (generated) stays in edp. `src_type sqlserver`, `write_mode merge`. Use a new `task_collection caboodle_poc`. Verify: `planner.route_tables` on the 50 rows gives 48 `cursor_seed` and 2 `full`, no `unsupported`.
- [ ] 4. Secret. The Fabric SQL endpoint uses a service principal with `authentication=ActiveDirectoryServicePrincipal` in the JDBC URL. Make sure that `SqlServerAdapter.jdbc_url` accepts that through `jdbc_options`, or add it. Store the `{user,password}` JSON secret in the lakefed secret scope. Never echo the secret. Verify: `ct_engine._read` of `SELECT 1` succeeds from the devtest job cluster.
- [ ] 5. Deploy the bundle to devtest with `--profile <name>` chosen by the operator. Run `lakefed_ingest_setup`, then `lakefed_ingest` with `task_collection=caboodle_poc`, `parallelism=8`, `max_queries_per_source=16` (the measured Fabric ceiling in edp). Verify: `ingest_run` shows one `succeeded` run and 50 `ingest_event` rows with stage `seed` or `full`.

## Week 2: side by side

- [ ] 6. Schedule the lakefed job on the same cadence as the edp `caboodle_poc` chunk job. Both read the same Fabric endpoint for 5 days. Verify each morning with one DBSQL query per pair: row count of the edp bronze current slice versus the lakefed sink, and distinct primary key count equal to row count on the lakefed sink. Record the numbers in the Review section.
- [ ] 7. Failure injection. On day 3, cancel the lakefed run during the fan out. Verify: the next run shows no duplicate keys and `ingest_event` shows the interrupted tables rerun from the same position. Do the same to the edp chunk job and record what the state table shows.
- [ ] 8. Source pressure. Read `sys.dm_exec_sessions` on the Fabric endpoint during both runs. Verify: lakefed never exceeds 16 sessions and the run time is at or below the edp chain time.
- [ ] 9. rl_solutions. If step 6 passes 5 days, register `rl_solutions`. Verified in edp metadata: `src_type mssql`, database `RL6_Prod`, 55 datasets, 53 incremental on `LAST_MODIFIED`, 2 full, and `windows_auth` true. Windows auth means the JDBC read needs `integratedSecurity=true` and `authenticationScheme=NTLM`. Pass both through `jdbc_options` on each control row, the same way the Fabric `authentication` option is passed in step 4. Before you register, run one query on the source: the type and nullability of `LAST_MODIFIED` on every table. The cursor engine refuses a nullable column. Run 3 days side by side.
- [ ] 10. Postgres cursor on the Supabase instance. Add the Postgres branches to `cursor_engine.py`: type lookup from `information_schema.columns`, `MAX` cast to text for the high watermark, and a `'...'::timestamp` or `::numeric` literal. Move `(postgresql, cursor)` to `SUPPORT`. Fixtures in `tests/cursor_engine_test.py` gain `src_type`. Then use the full control of the instance to prove the overlap: open a transaction, insert a row with an old timestamp, run the collection, commit the row, run again. Verify: the row lands on the second run and nothing is duplicated.

## Silver input shape (verified)

`_promote_caboodle_scd2` in `ingest/fabric.py` reads the bronze run slice by `run_key` and drops `_run_id`. The lakefed sink is current state, not a slice. For the side by side, silver stays on the edp bronze. If lakefed is adopted, the SCD2 promotion must read the whole lakefed sink and compare on `_LastUpdatedInstant`. That change is not part of this test.

## Decision rule

If all four conditions hold, adopt lakefed for database sources. The four conditions are:

1. No duplicate keys on any day.
2. Row counts match the edp slice within the overlap window.
3. The failure injection recovered without manual repair.
4. Source sessions stayed under the cap.

If any condition fails, stop the migration. Record the cause in the Review section.

## Not in scope

Silver, gold, masking, classification, file and API sources. Change Tracking. `DateDim`. Oracle and MySQL cursors.

## Review

(fill after each day of step 6, and after steps 7 to 9)
