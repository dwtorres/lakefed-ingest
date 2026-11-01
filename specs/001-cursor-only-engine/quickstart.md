---
ontology: true
type: doc
domain: lakefed-cursor
summary: Quickstart, the checks that prove each plan step and the devtest run on one table per engine
tags: [quickstart, devtest, acceptance, cursor, sqlserver, oracle]
related: [plan, spec, data-model, contracts/control_row, contracts/secret_json]
status: draft
discovered: 2026-10-25
verified: false
workspace: lakefed-cursor
---

# Quickstart: Cursor-Only Ingestion Engine

## Prerequisites

- Python 3.11 with the repo venv. `pip install pytest`.
- Databricks CLI 1.22 or later. A profile for the `devtest` workspace. Never auto-select a profile.
- A Lakebase instance named `lakefed-cursor` in `devtest`, or permission to create one.
- One SQL Server table and one Oracle table reachable from the `devtest` job cluster.
- A secret per source database in scope `lakefed-cursor`. See [contracts/secret_json.md](contracts/secret_json.md).

## Per-step checks

Run these after each plan step. Every step must pass before the next starts.

1. Run `pytest tests`. Expect zero failures and zero skips.
2. Run the step's grep check from the table below. Expect no output.
3. Commit with a Conventional Commit message.

| Step | Grep check |
|---|---|
| 1 | `rg -n "change_tracking\|CHANGETABLE\|append_batch\|pending_position\|verify_run_id\|batch_append" src tests` |
| 2 | `rg -n "def ingest_one" src/lakefed_ingest/ingest.ipynb` |
| 3 | `rg -n "LAKEFED_CURSOR_OVERLAP_S" src tests` |
| 4 | `rg -n "_is_pg\|src_type ==" src/lakefed_ingest/cursor_engine.py src/lakefed_ingest/worker.py` |
| 5 | `rg -n "lakefed_ingest\." src/lakefed_ingest/lakebase_schema.sql` and `rg -n "cursor_column" src tests` |
| 6 | none. Run `pytest tests/worker_test.py -k snapshot` |
| 7 | `rg -n "password" src/lakefed_ingest/source_io.py \| rg -v "creds\[\|options\["` |

Step 3 gap tests: `pytest tests/cursor_engine_test.py -k gap` lists one test per gap 2, 3, 4, 5, 8.
Check out the step 2 commit and run the same command. Expect those tests to fail there.

Step 5 schema: `pytest tests/state_store_test.py -k "fingerprint or ddl"`. Expect
`SCHEMA_VERSION == 2`.

## Lakeflow Connect baseline (fixed decision call 13)

Run one source table through a Lakeflow Connect query-based pipeline in `devtest` before the
engine gains cursor support for that source: SQL Server at the end of step 1 (T012a), Oracle at the
start of step 4 (T034a). Record the cost and the lag in `docs/baseline.md`. Then delete the
pipeline. Lakeflow Connect is not a dependency.

## Devtest run (step 8)

Run from the repo root.

1. Deploy the bundle.

   ```bash
   scripts/validate.sh -t devtest -p <profile>
   databricks bundle deploy -t devtest -p <profile>
   ```

2. Create the Lakebase schema.

   ```bash
   databricks bundle run lakefed_cursor_setup -t devtest -p <profile>
   ```

3. Store one secret per source database in scope `lakefed-cursor`. Use the Databricks CLI secrets
   command with a hidden prompt. Do not paste a secret into a notebook or a chat.

4. Insert two control rows into `lakefed_cursor.control`, one `sqlserver` and one `oracle`, in
   `task_collection` `devtest`. Use the example in [contracts/control_row.md](contracts/control_row.md).
   Set `delete_mode` to `pk_snapshot` and `delete_min_interval_s` to `0` on both rows.

5. Run the seed.

   ```bash
   databricks bundle run lakefed_cursor -t devtest -p <profile> --params task_collection=devtest
   ```

   Expected: both checkpoints show `phase = incremental`. The sink row count equals the source row
   count. The position JSON holds the expression text.

6. Change the source. On each table, insert 10 rows, update 5 rows so the cursor moves, and delete
   5 rows.

7. Run the increment with the same command as step 5.

   Expected: the sink gains 10 rows, shows the 5 updates, and loses the 5 deleted rows. The run log
   shows one `capture` event per source database. For an `identity` cursor, the log shows one
   `inserts_only` note.

8. Reconcile. For each table, run a full outer join between the source and the sink on the primary
   key, through `remote_query` or a JDBC read in a notebook. Expected: zero rows differ.

9. Run the increment again with no source change.

   Expected: `DESCRIBE HISTORY` on each sink shows no new version. The checkpoint `last_success_at`
   moved.

10. Auth modes. If a Windows login or an Entra ID service principal exists for the SQL Server source,
    change the secret to that shape and run step 7 again. Expected: the run connects and ingests.
    If no such login exists, record this gap in `tasks/todo.md`.

## Acceptance

If steps 5 to 9 produce the expected results on both tables, SC-001 passes. Run
`databricks bundle summary -t devtest -p <profile>`. If the output lists no resource that starts
with `lakefed_ingest` or `edp`, SC-006 passes.
