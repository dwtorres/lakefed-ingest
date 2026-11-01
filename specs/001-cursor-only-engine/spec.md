---
ontology: true
type: workflow-spec
domain: lakefed-cursor
summary: Feature spec for the cursor-only ingestion engine for SQL Server and Oracle
tags: [spec, cursor, ingestion, sqlserver, oracle, databricks]
related: [../../.specify/memory/constitution, ../../docs/cursor_orchestration_analysis]
status: draft
discovered: 2026-10-25
verified: false
workspace: lakefed-cursor
---

# Feature Specification: Cursor-Only Ingestion Engine

**Feature Branch**: `001-cursor-only-engine`

**Created**: 2026-10-25

**Status**: Draft

**Input**: User description: "Feature: cursor-only ingestion engine for SQL Server and Oracle, built by stripping Change Tracking from this repo and patching the cursor path. Source of truth: docs/cursor_orchestration_analysis.md, sections 3.4, 3.5, 4.2, and 5."

## Terms

One name for one thing. Use these names in every artifact of this feature.

- **Control row**: one row of the control table. It describes one source table and its sink.
- **Cursor expression**: one SQL expression over one to three source columns. Its value orders the rows.
- **Cursor kind**: the class of the cursor expression: `rowversion`, `db_timestamp`, `app_timestamp`, `identity`.
- **Position**: the last cursor value the engine ingested for one table. The checkpoint stores it.
- **Window**: the cursor range `(lo, hi]` that one run reads for one table.
- **Lookback**: seconds that the engine pulls `lo` back for temporal cursors.
- **Skew**: seconds that the engine allows `hi` to exceed the source clock.
- **Seed**: the first full read of a source table into an empty sink.
- **Reseed**: a new seed of a table that has a sink already.
- **PK snapshot**: a read of all primary key values from the source, used to find deleted rows.
- **Operator**: the person who writes control rows and runs the job.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Ingest a SQL Server table by cursor (Priority: P1)

The operator writes one control row for a SQL Server table with a cursor expression and a cursor kind. The operator runs the job. The first run seeds the sink. Each later run reads only the rows whose cursor value moved past the position. The sink matches the source for inserted and updated rows.

**Why this priority**: This is the only load path the engine supports. Without it the engine has no value.

**Independent Test**: Seed one SQL Server table in `devtest` into a fresh sink. Insert and update rows at the source. Run again. Compare the sink to the source on the primary key.

**Acceptance Scenarios**:

1. **Given** a control row with `load_type` unset, **When** the operator saves it, **Then** `load_type` is `cursor`.
2. **Given** an empty sink, **When** the job runs, **Then** the sink holds every source row. For an identity or rowversion cursor the position equals the captured `hi`. For a temporal cursor the position is `hi` minus the seed duration, so the first increment re-reads rows that committed during the seed.
3. **Given** a seeded sink and 10 new source rows, **When** the job runs, **Then** the sink gains exactly those 10 rows.
4. **Given** a seeded sink and one updated source row with a moved cursor value, **When** the job runs, **Then** the sink row shows the new values.
5. **Given** a control row with `load_type` `ct`, `cdc`, or any value other than `cursor` or `full`, **When** the job plans, **Then** the table fails at the plan stage with a clear reason.

---

### User Story 2 - Ingest an Oracle table by cursor (Priority: P1)

The operator writes one control row for an Oracle table. The same job ingests it with the same cursor rules as SQL Server. Oracle timestamp and sequence cursors work. The operator does not need an Oracle-specific job or notebook.

**Why this priority**: The constitution makes Oracle first-class from day one. A SQL Server-only engine is not done.

**Independent Test**: Seed one Oracle table in `devtest` into a fresh sink. Insert and update rows. Run again. Compare the sink to the source on the primary key.

**Acceptance Scenarios**:

1. **Given** an Oracle table with a timestamp cursor, **When** the job runs twice, **Then** both runs behave as in User Story 1 scenarios 2 to 4.
2. **Given** an Oracle table with a sequence cursor of kind `identity`, **When** the job runs, **Then** the run succeeds and the run log states that the cursor is inserts-only and can miss late commits.
3. **Given** a cursor feature test, **When** the test suite runs, **Then** the same case runs against the SQL Server dialect and the Oracle dialect.

---

### User Story 3 - Fold up to three columns into one cursor (Priority: P2)

The operator has a table where the change time is in one of three nullable columns. The operator writes one cursor expression that picks the greatest non-null value. The engine stores the expression text with the position. If the operator changes the expression, the next run reseeds the table.

**Why this priority**: Oracle tables carry up to three candidate columns. Without this, those tables cannot use a cursor.

**Independent Test**: Write a control row with a three-column expression over nullable columns. Seed. Update a row through each column in turn. Confirm each update reaches the sink.

**Acceptance Scenarios**:

1. **Given** an expression over three columns where only the third is set on a row, **When** the job runs, **Then** that row is read in the window.
2. **Given** an expression that can return NULL for some row, **When** the job plans, **Then** the table fails at the plan stage with a message that names the nullable input.
3. **Given** a seeded table, **When** the operator changes the expression text, **Then** the next run reseeds the table.
4. **Given** an expression over four columns, **When** the job plans, **Then** the table fails at the plan stage.

---

### User Story 4 - Safe window bounds (Priority: P2)

The engine protects the window against late commits, clock skew, and bad source values. Temporal windows pull `lo` back by the lookback. The engine caps `hi` at the source clock plus skew. Rowversion `hi` never passes an open transaction. A SQL Server `datetime` cursor with milliseconds works on every run.

**Why this priority**: These are the silent data-loss gaps in section 3.4 of the analysis. A run that looks green but skips rows is worse than a failed run.

**Independent Test**: Unit tests with fake sources for each bound. One live SQL Server `datetime` table with nonzero milliseconds in `devtest`.

**Acceptance Scenarios**:

1. **Given** a temporal cursor and a lookback of 900 seconds, **When** the job reads, **Then** `lo` is the position minus 900 seconds and rows in that overlap merge without duplicates.
2. **Given** a source row stamped in the year 2099, **When** the job captures `hi`, **Then** `hi` is the source clock plus skew, the run logs an alert, and later rows below 2099 are still read.
3. **Given** a SQL Server rowversion cursor and an open transaction, **When** the job captures `hi`, **Then** `hi` is below the lowest active rowversion.
4. **Given** a SQL Server `datetime` cursor whose position has nonzero milliseconds, **When** the job reads, **Then** the read succeeds and the literal carries three fractional digits.
5. **Given** a source `MAX` that is lower than the stored position, **When** the job plans, **Then** the table fails with a message that names a possible source truncate or reload. It does not move the position backwards.
6. **Given** a job parameter for lookback and a control row override, **When** the job reads, **Then** the row override wins.

---

### User Story 5 - Capture deletes with a PK snapshot (Priority: P3)

The operator sets `delete_mode` to `pk_snapshot` and a minimum interval. On a run past the interval the engine reads all primary keys from the source, finds sink rows with no source key, and deletes them.

**Why this priority**: Deletes are gap 1 in section 3.4. Many tables can wait for this, so it follows the read path.

**Independent Test**: Seed a table. Delete rows at the source. Run past the interval. Confirm the sink lost exactly those rows.

**Acceptance Scenarios**:

1. **Given** `delete_mode` `pk_snapshot` and 5 source deletes, **When** a run passes the interval, **Then** the sink loses exactly those 5 rows.
2. **Given** `delete_mode` `pk_snapshot`, **When** a run is inside the interval, **Then** the engine does no snapshot read.
3. **Given** `delete_mode` `none`, **When** rows are deleted at the source, **Then** the sink keeps them.
4. **Given** `delete_mode` `soft_column` or `full_compare`, **When** the job plans, **Then** the table fails at the plan stage as not implemented.

---

### User Story 6 - Quiet tables cost little (Priority: P3)

A table with no change in a run causes no sink rewrite. The engine captures `hi` for every table of one source database in one query. Rows read in the overlap that did not change do not update the sink.

**Why this priority**: Gap 10 in section 3.4. This is cost, not correctness, so it follows the correctness stories.

**Independent Test**: Seed a table. Run twice with no source change. Confirm the second run wrote no sink version.

**Acceptance Scenarios**:

1. **Given** 20 tables on one source database, **When** the job captures `hi`, **Then** the source sees one `MAX` query for those 20 tables.
2. **Given** a quiet table, **When** the job runs, **Then** the sink has no new table version.
3. **Given** an overlap row whose cursor value did not change, **When** the merge runs, **Then** the sink row is not rewritten.

---

### User Story 7 - Source credentials in three auth modes (Priority: P3)

The operator stores a secret JSON per source database. The JSON names an auth mode: `sql`, `ntlm`, or `aad_sp`. The engine builds the connection from that mode. An old secret with no mode still works as `sql`.

**Why this priority**: The target sources use Windows and Entra ID logins. Without this, some sources cannot connect.

**Independent Test**: Unit tests that build connection options from each secret shape. One live connect per mode in `devtest` when such a source exists.

**Acceptance Scenarios**:

1. **Given** a secret `{user, password}` with no mode, **When** the engine connects, **Then** it uses SQL login.
2. **Given** a secret with mode `ntlm` and a domain, **When** the engine connects, **Then** it uses Windows login with that domain.
3. **Given** a secret with mode `aad_sp`, a client id, a client secret, and a tenant, **When** the engine connects, **Then** it uses an Entra ID service principal.
4. **Given** a secret with an unknown mode, **When** the engine connects, **Then** it fails with a message that lists the three valid modes and does not print the secret.

---

### Edge Cases

- A source `MAX` query times out. The table fails for this run. The position does not move.
- The sink write commits and the process stops before the checkpoint advance. The next run reads the same window again. The merge is idempotent, so the sink stays correct.
- Two rows share one cursor value at the window edge. The window is `(lo, hi]` with lookback, so both rows land in a later window.
- A table has an identity cursor and a lookback. The lookback does not apply. The run logs that the cursor is inserts-only.
- The source clock and the engine clock differ by more than the skew. The engine caps `hi` at the source clock, not the engine clock.
- DST moves a local-time `datetime` back one hour. The default lookback is 3600 seconds or more, so no row is lost.
- A control row has `client_id`. Every read, merge, and delete stays scoped to that client.
- A PK snapshot runs while a seed is in flight. The engine skips the snapshot for that table in that run.
- A source table has no primary key. The table fails at the plan stage.

## Requirements *(mandatory)*

### Functional Requirements

Removal:

- **FR-001**: The engine MUST have no Change Tracking or CDC code path, control column, or plan branch.
- **FR-002**: The engine MUST have no batched append module and no append-mode route. The control table MUST NOT have `write_mode`. Every sink write is a merge.
- **FR-003**: The engine MUST have no `pending`, `landed`, or `verify_run_id` route.
- **FR-004**: The control table MUST NOT have the columns `job_name`, `src_catalog`, `enable_iceberg_reads`, `sink_cluster_cols`, `source_instance_group`, `warehouse_id`.
- **FR-005**: `load_type` MUST default to `cursor`. The engine MUST accept `cursor` and `full`. Any other value MUST fail at plan time.

Control row:

- **FR-006**: The control row MUST hold `cursor_expression`, `cursor_kind`, `cursor_lookback_s`, `delete_mode`, `delete_min_interval_s`, `null_cursor_policy`.
- **FR-007**: `cursor_kind` MUST be one of `rowversion`, `db_timestamp`, `app_timestamp`, `identity`.
- **FR-008**: `delete_mode` MUST be one of `none`, `soft_column`, `pk_snapshot`, `full_compare`. Only `none` and `pk_snapshot` run. The other two MUST fail at plan time.
- **FR-009**: `null_cursor_policy` MUST be one of `reject`, `seed_only`. `reject` fails the table at plan time when the expression can be NULL. `seed_only` ingests NULL-cursor rows on seed and never on increments.
- **FR-010**: The control table MUST enforce the valid values of FR-007 to FR-009 with constraints.

Cursor expression:

- **FR-011**: A cursor expression MUST reference one to three source columns. More than three MUST fail at plan time.
- **FR-012**: All referenced columns MUST share one data type. A mismatch MUST fail at plan time.
- **FR-013**: The position MUST store the expression text, the cursor kind, and the value.
- **FR-014**: A changed expression text MUST trigger a reseed on the next run.
- **FR-015**: Every nullable input MUST be wrapped so that the expression is never NULL, or the table MUST fail under `null_cursor_policy` `reject`.

Window:

- **FR-016**: The engine MUST capture `hi` before the read and MUST read `(lo, hi]`.
- **FR-017**: For temporal cursors, `lo` MUST be the position minus `cursor_lookback_s`. The job parameter sets the default. The control row overrides it.
- **FR-018**: For temporal cursors, `hi` MUST be the smaller of the source `MAX` and the source clock plus skew. When `MAX` exceeds the source clock, the run MUST log an alert for that table.
- **FR-019**: For rowversion cursors, `hi` MUST stay below the lowest active rowversion at capture time.
- **FR-020**: For identity cursors, the engine MUST ingest without lookback and MUST log once per run per table that the cursor is inserts-only.
- **FR-021**: A SQL Server `datetime` or `smalldatetime` literal MUST carry at most three fractional digits.
- **FR-022**: When the source `MAX` is below the position, the table MUST fail with a message that names a possible source truncate or reload. The position MUST NOT move backwards.
- **FR-023**: The engine MUST capture `hi` for all cursor tables of one source database in one query. A failed batch MUST fall back to one query per table.
- **FR-024**: Identity and rowversion tables whose `hi` equals the position MUST advance their checkpoint freshness in one bulk write and MUST NOT read the source again in that run. A temporal table whose `hi` equals the position MUST still read `(hi - lookback, hi]`, because a row can commit late below `hi`.

Sink write:

- **FR-025**: Every sink write MUST be an idempotent merge on the primary key, or on client id plus primary key when `client_id` is set.
- **FR-026**: The merge MUST update a matched row only when the source cursor value differs from the sink cursor value.
- **FR-027**: The checkpoint MUST advance only after the sink commit. This ordering MUST live in a module with a unit test.

Deletes:

- **FR-028**: With `delete_mode` `pk_snapshot`, the engine MUST read all source primary keys, find sink rows absent from the source, and delete them, scoped to `client_id` when set.
- **FR-029**: The snapshot MUST run at most once per `delete_min_interval_s` per table. The checkpoint MUST record the last snapshot time.
- **FR-030**: The snapshot read MUST use the same slot budget and per-source limiter as other reads.

Dialects:

- **FR-031**: Every dialect difference MUST be an adapter method. No engine module MAY branch on the source type.
- **FR-032**: The Oracle adapter MUST provide cursor type lookup, source clock, bound queries, typed literals, and the batched `MAX` query.
- **FR-033**: Existing PostgreSQL cursor behavior MUST keep working. MySQL stays a placeholder that fails at plan time.

Credentials:

- **FR-034**: The secret JSON MUST accept `auth_mode` with values `sql`, `ntlm`, `aad_sp`. A missing mode MUST mean `sql`.
- **FR-035**: `ntlm` MUST require `domain`. `aad_sp` MUST require `client_id` and `client_secret` only. A missing field MUST fail before any connect attempt. The live proof showed `tenant_id` is not needed (`docs/baseline.md`, section "Auth modes (T095)").
- **FR-036**: No error message or log line MAY contain a secret value.

Structure:

- **FR-037**: The per-table worker MUST live in a module with unit tests. The notebook MUST only parse parameters and call the module.
- **FR-038**: The bundle, the Lakebase schema, the secret scope, and the sink catalog MUST use the names in constitution Principle IX. A resource name that starts with `lakefed_ingest` or `edp` MUST fail bundle validation. The gate is `scripts/validate.sh`, which runs the name test before `databricks bundle validate`.

Tests and docs:

- **FR-039**: Every gap in section 3.4 of the analysis, except gaps 9 and 12, MUST have at least one unit test that fails before the fix and passes after it.
- **FR-040**: Every cursor feature test MUST run against the SQL Server adapter and the Oracle adapter.
- **FR-041**: README, module headers, and `docs/` MUST describe the cursor-only engine and MUST NOT cite deleted files. `docs/cursor_orchestration_analysis.md` and `docs/baseline.md` are history and are exempt.

### Key Entities

- **Control row**: source location, credentials pointer, sink location, primary key, cursor expression, cursor kind, lookback, delete mode, delete interval, null cursor policy, optional client id.
- **Checkpoint**: one per control row. Position (expression text, kind, value), phase, status, freshness, last PK snapshot time.
- **Run**: one job execution for one task collection. Holds the per-table outcomes and the alerts.
- **Secret JSON**: credentials for one source database. Auth mode plus the fields that mode needs.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: One SQL Server table and one Oracle table ingest end to end in `devtest` into fresh sinks. After a seed and one increment with inserts, updates, and deletes, the sink equals the source on every primary key.
- **SC-002**: All tests pass with no skips, with the existing tests kept where the behavior survives.
- **SC-003**: Each of gaps 1 to 8, 10, and 11 in section 3.4 has a named test. Gap 9 is out of scope. Gap 12 is covered by FR-041.
- **SC-004**: A quiet table produces zero new sink versions per run.
- **SC-005**: A source database with N cursor tables sees one `MAX` query per run when the batch succeeds.
- **SC-006**: No job, table, schema, secret scope, or Lakebase instance named `edp` or `lakefed_ingest` changes during development or acceptance.
- **SC-007**: A reviewer finds no source type branch outside the adapter module.
- **SC-008**: Deleted code is about 1,600 lines or more, in line with the survival table in section 3.5.

## Out of Scope

- Lakeflow Connect as a dependency. It stays a comparison baseline only.
- Serverless compute.
- New PostgreSQL or MySQL cursor features beyond what exists today.
- Append mode and SCD2 silver tables (gap 9, design call 10).
- `soft_column` and `full_compare` delete modes beyond the enum value.
- Chunked increments with per-chunk checkpoints (design call 8).
- Migration of `edp` data, jobs, or metadata.
- Composite (cursor, pk) paging for ties (gap 7 is covered by lookback plus idempotent merge, not by paging).

## Assumptions

- The `job_orchestration/` version is removed. The repo holds one bundle, `lakefed_cursor`.
- The Lakebase schema `lakefed_cursor` is new. No existing checkpoint rows migrate. The position format can change freely.
- The default lookback is 3600 seconds. The default skew is 300 seconds. Both are job parameters.
- The default `delete_min_interval_s` is 86400 seconds.
- An identity or sequence cursor is accepted as inserts-only with a logged warning. It is not rejected.
- A multi-column expression is valid for temporal and identity kinds only. A rowversion cursor is one column.
- SQL Server versions before 2022 get a `CASE` form of the expression from the adapter when the operator writes `GREATEST`.
- The operator provides one SQL Server table and one Oracle table in `devtest`, with credentials in the `lakefed-cursor` secret scope, for SC-001.
- The source clock comes from the source database, not from the cluster.
- The merge change-check compares the cursor expression value. A row-hash predicate is not needed for this feature.
- The PK snapshot reads only primary key columns and runs through the same JDBC or `remote_query` path as other reads.
- Gap 7 (ties at a numeric boundary) is covered by the lookback for temporal cursors and by the inserts-only warning for identity cursors.

## Dependencies

- `.specify/memory/constitution.md` version 1.0.0. The fixed decisions in it are constraints.
- `docs/cursor_orchestration_analysis.md` sections 3.4, 3.5, 4.2, 5.
- A `devtest` workspace target, a Lakebase instance, and the `lakefed-cursor` secret scope.
- One reachable SQL Server source and one reachable Oracle source for acceptance.
