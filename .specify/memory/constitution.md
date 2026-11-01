---
ontology: true
type: constitution
domain: lakefed-cursor
summary: Governing principles and fixed constraints for the lakefed_cursor sister engine
tags: [constitution, governance, cursor, ingestion, databricks]
related: [cursor_orchestration_analysis]
status: active
discovered: 2026-10-25
verified: true
workspace: lakefed-cursor
---

# lakefed_cursor Constitution

This repository is a copy of lakefed-ingest, root version, commit 72255e0. It is a sister engine.
It disrupts nothing that runs today. Every rule below is a MUST unless the text says otherwise.

## Core Principles

### I. Strip and Patch, Never Rewrite From Zero

Every task starts from existing code in `src/lakefed_ingest`. A task MUST name the file and the
function it changes. A task that proposes a new module MUST name the existing module it replaces
and MUST state why the existing module cannot carry the change. A rename counts as a replacement
and MUST follow the same rule.

Rationale: the analysis shows that about 2,750 of 4,381 lines survive a cursor-only engine. A
rewrite throws away tested reads, writes, seeds, locks, retries, and telemetry.

### II. Cursor Only

The engine supports one load strategy: the cursor. Change Tracking and CDC code paths MUST be
deleted, not hidden behind flags. A `load_type` other than `cursor` or `full` MUST fail at plan
time. No task adds a CT or CDC branch, placeholder, or control column.

Rationale: Change Tracking is not available on the target sources. Dead branches cost review time
and hide bugs.

### III. Everything Runs Inside Databricks

State lives in Lakebase. Compute is a job cluster. The engine MUST NOT depend on a SQL warehouse.
Source reads use Spark JDBC with per-database credentials from the secret scope, or `remote_query`
through a Unity Catalog connection. No external orchestrator, agent, or SaaS connector is a
dependency.

Rationale: the owner's constraint, and the reason the root version exists.

### IV. SQL Server and Oracle Are Both First-Class

A feature is not done until it passes on SQL Server and on Oracle. Every dialect difference lives
in `source_adapters.py` as an adapter method, never as an `if` on the engine name inside an engine
module. A test suite for a cursor feature MUST run the same case against both dialects.

Rationale: Oracle cursor support is a placeholder today. Building SQL Server first and porting
later produces a second engine.

### V. A Cursor Is One SQL Expression Per Table

The control row holds one cursor expression. The expression can fold up to three columns into one
value, for example with `GREATEST` over `COALESCE` inputs. All folded columns MUST share one type
and one clock. The checkpoint position stores the expression text. A changed expression MUST
trigger a reseed. Nullable inputs MUST be wrapped in `COALESCE` so the expression is never NULL.

Rationale: Oracle tables carry up to three candidate cursor columns. If any input is NULL, Oracle
`GREATEST` returns NULL. SQL Server has `GREATEST` only from version 2022, so older versions need a
`CASE` expression from the adapter.

### VI. Checkpoints Advance Only After the Delta Commit

The write to the sink commits first. The checkpoint advance follows. Delivery is at-least-once.
Every MERGE MUST be idempotent on the primary key. The ordering invariant MUST live in a module with
a unit test, not only in a notebook cell.

Rationale: a checkpoint written before the commit loses rows on a failed write.

### VII. Keep What Is Tested

These modules keep their current behavior: `parallel.py`, `partitions.py`, `source_adapters.py`,
`table_retry.py`, `state_store.py`. A behavior change in any of them MUST start with a failing test
that shows the new behavior. Additive changes, such as new adapter methods, follow the same rule.
Removal of CT markers from `table_retry.py` counts as a behavior change.

Rationale: these modules carry the thread pool, the slot budget, the per-source limiter, the
partition planner, and the Lakebase accessor. They are generic and tested.

### VIII. Docs Follow Simplified Technical English

Documentation follows ASD-STE100. Short sentences, active voice, one word one meaning. An
instruction has at most 20 words. A description has at most 25 words. Instructions use the command
form. No synonyms for a term already in use. This rule covers README files, module headers,
`docs/`, and the Spec Kit artifacts under `specs/`.

Rationale: the analysis found docs that cite deleted files and module headers that contradict the
code. Short, uniform text drifts less.

### IX. Own Names

The bundle is `lakefed_cursor`. The Lakebase schema is `lakefed_cursor`. The secret scope is
`lakefed-cursor`. The sink catalog is in `devtest`. The engine MUST NOT reuse an `edp` or
`lakefed_ingest` job, table, schema, secret scope, or Lakebase instance. If a resource name starts
with `lakefed_ingest` or `edp`, the deploy MUST fail validation.

Rationale: the sister engine disrupts nothing that runs today.

## Fixed Design Decisions

These decisions were made by the owner on 2026-10-25. They come from section 5 of
`docs/cursor_orchestration_analysis.md`. A spec, plan, or task MUST treat them as constraints.
Changing one requires a constitution amendment.

- Call 1, base: the lakefed root version at commit 72255e0. Rename `ct_engine.py` to
  `source_io.py` and `sink_writer.py`. Delete `batch_append.py` and the CT branch of the planner.
  Delete the `pending`, `landed`, and `verify_run_id` routes. Delete the six dead control columns:
  `job_name`, `src_catalog`, `enable_iceberg_reads`, `sink_cluster_cols`, `source_instance_group`,
  `warehouse_id`. Default `load_type` to `cursor`.
- Call 11, dialects: dialect SQL lives in `source_adapters.py`. Oracle is required from day one.
  `ORA_ROWSCN` is block-level unless the table has ROWDEPENDENCIES, so it is not a default cursor.
  Timestamps and sequence IDs are the realistic Oracle cursors. Sequence IDs share the late-commit
  hole of SQL Server identity columns.
- Call 12, shape: keep the in-process thread pool and one job task per task collection, for SQL
  Server and Oracle. Plan the serverless path as `remote_query` through a UC connection, because
  `set_pool` and raw JDBC do not run on serverless.
- Call 13, baseline: before building cursor support for a given source, run one table through a
  Lakeflow Connect query-based pipeline and record cost and lag. Lakeflow Connect is a comparison
  baseline only, never a dependency, because it allows exactly one cursor column.
- Call 14, expression cursor: one SQL expression per table over up to three columns. Example:
  `GREATEST(COALESCE(c1, DATE '1900-01-01'), COALESCE(c2, DATE '1900-01-01'), COALESCE(c3,
  DATE '1900-01-01'))`. Expect a scan on the source unless a function-based index exists. Ask the
  DBA for the index.

The other nine calls in section 5 are recommendations. A spec can adopt or reject them with a
written reason.

## Development Workflow

- Every feature follows the Spec Kit flow: specify, plan, tasks, implement. The plan's
  Constitution Check MUST list each principle and state pass or fail.
- Tests run with `pytest tests` and MUST pass before a task is marked done. A skipped test is a
  failure.
- If a cursor feature's test passes against the SQL Server adapter and the Oracle adapter, the
  feature is done.
- Each task names the existing code it starts from. A task with no starting point is rejected.
- Deploy with `databricks bundle deploy -t <target> -p <profile>` from the repo root only.
  Never auto-select a profile.
- Commits use the Conventional Commits format.

## Governance

This constitution overrides every other practice document in the repository, including
`CLAUDE.md`, where they conflict. Amendments follow this procedure:

1. Open a pull request that changes only `.specify/memory/constitution.md`.
2. State the version bump and the reason in the pull request body.
3. Get approval from the repository owner.
4. Update dependent specs and plans in a follow-up pull request.

Versioning: MAJOR for a removed or redefined principle or fixed decision. MINOR for a new principle,
a new fixed decision, or materially expanded guidance. PATCH for wording and clarifications.

Compliance review: every pull request review MUST check the changed code against Principles I, II,
IV, VI, VII, and IX. The `speckit-analyze` command treats a constitution conflict as CRITICAL.

**Version**: 1.0.0 | **Ratified**: 2026-10-25 | **Last Amended**: 2026-10-25
