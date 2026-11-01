---
ontology: true
type: spec
domain: lakefed-cursor
summary: One SQL Server test source in the operator's Azure subscription for the live-proof tasks T012a, T093, T095
tags: [spec, azure, sqlserver, live-proof, ntlm, entra-id, lakeflow-connect, baseline]
related: [../001-cursor-only-engine/spec, ../001-cursor-only-engine/quickstart, ../001-cursor-only-engine/contracts/secret_json, ../../docs/baseline, ../../.specify/memory/constitution]
status: draft
discovered: 2026-10-31
verified: false
workspace: lakefed-cursor
---

# Feature Specification: Azure SQL Server Test Source

**Feature Branch**: `002-azure-sqlserver-test-source`

**Created**: 2026-10-31

**Status**: Draft

**Input**: User description: "Azure test source for the blocked live-proof tasks T012a, T093, T095 of feature 001-cursor-only-engine. An operator stands up one SQL Server source in their own Azure subscription so the cursor engine can be proven live: (1) a SQL Server that supports a SQL login, a Windows (NTLM) login, and an Entra service principal login, so one live connect per auth mode can run (US7 scenarios 2 and 3; confirm whether the JDBC driver needs tenant_id); (2) one synthetic table built from tests/synthetic with a rowversion or datetime2 cursor, seeded, changed (10 inserts, 5 updates, 5 deletes), incremented, and reconciled on the primary key from Databricks devtest (US1, SC-001); (3) the Lakeflow Connect query-based baseline for the same table, with cost and lag recorded in docs/baseline.md, then the pipeline deleted. Network path from the Databricks workspace to the source, secret JSON in scope lakefed-cursor per contracts/secret_json.md, teardown of every Azure resource when done, and a cost ceiling the operator sets. Out of scope: Oracle, production data, Change Tracking, CDC."

## Purpose

Feature 001 has three tasks that need a live SQL Server source: T012a, T093, and T095. No source
exists today. This feature gives the operator one disposable source in their own Azure subscription.
The source proves the cursor engine end to end and then goes away.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Stand up and tear down the source (Priority: P1)

The operator creates one SQL Server source in one Azure resource group. The source holds one
synthetic table with a primary key and a cursor column. The devtest Databricks workspace can reach
the source. When the proof is done, the operator deletes the resource group. Nothing is left behind.

**Why this priority**: Every other story needs the source. Teardown keeps the cost bounded.

**Independent Test**: Create the source. Connect from a devtest notebook with a SQL login. Read the
row count. Delete the resource group. List the subscription and find no resource of this feature.

**Acceptance Scenarios**:

1. **Given** an empty resource group, **When** the operator runs the setup steps, **Then** one SQL
   Server accepts a SQL login from a devtest job cluster. Elapsed time: under 30 minutes.
2. **Given** the source is up, **When** the operator builds the synthetic table, **Then** the table
   holds the expected row count. It has a `rowversion` or `datetime2` cursor column.
3. **Given** the source is up, **When** a host outside the devtest workspace tries to connect,
   **Then** the connection is refused.
4. **Given** the proof is done, **When** the operator deletes the resource group, **Then** no
   resource of this feature remains and no further cost accrues.
5. **Given** a cost ceiling the operator sets, **When** spend reaches 80 percent of the ceiling,
   **Then** the operator receives an alert.

---

### User Story 2 - Prove the cursor engine on a live table (Priority: P1)

The operator stores a SQL login secret and inserts one control row. The operator then runs feature
001 quickstart steps 5 to 9: seed, change set, increment, reconcile, and no-change run. The
result is recorded in `docs/baseline.md`. This closes T093.

**Why this priority**: This is SC-001 of feature 001. It is the proof that the engine works.

**Independent Test**: Run quickstart steps 5 to 9 against the source. Compare the sink to the
source on the primary key. Zero rows differ.

**Acceptance Scenarios**:

1. **Given** an empty sink and one control row, **When** the seed runs, **Then** the sink row count
   equals the source row count. The checkpoint phase is `incremental`.
2. **Given** a seeded sink and 10 inserts, 5 updates, and 5 deletes, **When** the increment runs,
   **Then** the sink shows exactly those 20 changes.
3. **Given** the increment ran, **When** the operator reconciles source and sink on the primary
   key, **Then** zero rows differ.
4. **Given** no source change, **When** the increment runs again, **Then** the sink gains no new
   version and the checkpoint freshness moves.
5. **Given** the runs are done, **When** the operator reads `docs/baseline.md`, **Then** it holds
   the date, row counts, `capture` event count, and run durations.

---

### User Story 3 - Prove the three auth modes (Priority: P2)

The operator connects to the same source three times, once per secret shape: `sql`, `ntlm`, and
`aad_sp`. Each connect runs one increment. The operator records whether the Entra service
principal login needs `tenant_id`. This closes T095.

**Why this priority**: Feature 001 US7 scenarios 2 and 3 are unproven. The `tenant_id` field is
unused in code today and the answer decides whether it stays.

**Independent Test**: Change the secret to each shape in turn. Run the increment. The run connects
and ingests, or fails with a message that names the auth mode.

**Acceptance Scenarios**:

1. **Given** a secret with `auth_mode` `ntlm`, a Windows login, and the login's domain, **When**
   the increment runs, **Then** the run connects and ingests.
2. **Given** a secret with `auth_mode` `aad_sp` and an Entra service principal, **When** the
   increment runs, **Then** the run connects and ingests.
3. **Given** the `aad_sp` connect, **When** the operator removes `tenant_id` from the secret and
   runs again, **Then** `docs/baseline.md` records "needed" or "not needed".
4. **Given** any auth mode, **When** a run fails, **Then** no log line or event holds a password
   or a client secret.

---

### User Story 4 - Record the Lakeflow Connect baseline (Priority: P3)

The operator runs the same synthetic table through a Lakeflow Connect query-based pipeline in
devtest. The operator records the cost and the lag in `docs/baseline.md`, then deletes the
pipeline. This closes T012a.

**Why this priority**: Fixed decision call 13 asks for a cost and lag reference. It is a
comparison, not a correctness proof.

**Independent Test**: Create the pipeline. Run it twice, once for the seed and once after a change.
Read the cost and the lag. Delete the pipeline. List the pipelines and find none for this table.

**Acceptance Scenarios**:

1. **Given** the synthetic table, **When** the pipeline runs its first load, **Then** the target
   table row count equals the source row count.
2. **Given** a change at the source, **When** the pipeline runs again, **Then** the target shows
   the change and the operator records the wall-clock lag.
3. **Given** two pipeline runs, **When** the operator reads the billing data, **Then** `docs/baseline.md`
   records each run's cost next to the cursor engine cost.
4. **Given** the baseline is recorded, **When** the operator deletes the pipeline, **Then** no
   pipeline for this table remains.

---

### Edge Cases

- The source is unreachable from the job cluster. The run fails at the first connect with a
  message that names the host and the port.
- The Windows login's domain is the machine name, not an Active Directory domain. The secret holds
  the machine name as `domain`.
- The Entra connect is not encrypted or the server certificate is not trusted. The login fails
  with a certificate chain error. Set `trust_server_certificate` on the job.
- The Entra service principal has no login on the database. The run fails with the database error
  text and no secret value.
- The source clock drifts from the job cluster clock. The `datetime2` high watermark is capped at
  the source clock plus `cursor_skew_s` and the alert is recorded.
- Spend reaches the cost ceiling before the proof is done. The operator tears down and records the
  partial result.
- The resource group delete fails on a locked resource. The operator removes the lock and retries.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The source MUST be one SQL Server that accepts a SQL login, a Windows login, and an
  Entra service principal login.
- **FR-002**: Every Azure resource of this feature MUST live in one resource group named for this
  feature. Deleting that group MUST remove every resource.
- **FR-003**: The source MUST accept connections only from the devtest Databricks workspace.
- **FR-004**: The source MUST hold one synthetic table from `tests/synthetic` with a primary key
  and a `rowversion` or `datetime2` cursor column. Row count: at least 10,000.
- **FR-005**: The setup MUST NOT enable Change Tracking or CDC on the database or the table.
- **FR-006**: Each credential MUST be stored as one secret in scope `lakefed-cursor` in the shape
  of `contracts/secret_json.md`. No secret value appears in a notebook, a doc, or a chat.
- **FR-007**: The proof MUST follow feature 001 quickstart steps 5 to 9 for the SQL login.
- **FR-008**: The proof MUST run one increment per auth mode `ntlm` and `aad_sp`.
- **FR-009**: The proof MUST state whether the `aad_sp` connect needs `tenant_id`.
- **FR-010**: The baseline MUST run the same table through a Lakeflow Connect query-based pipeline
  in devtest for one seed and one change.
- **FR-011**: `docs/baseline.md` MUST record four values for each engine: row counts, run
  duration, lag from source change to sink visibility, and cost per run.
- **FR-012**: The Lakeflow Connect pipeline MUST be deleted after the baseline is recorded.
- **FR-013**: The operator MUST set a cost ceiling before the first resource is created. A budget
  alert MUST fire at 80 percent of the ceiling.
- **FR-014**: The setup steps MUST be written as a numbered runbook that a second operator can
  follow without the first operator present.
- **FR-015**: The runbook MUST end with a teardown step and a check that lists zero remaining
  resources.

### Key Entities

- **Test source**: one SQL Server in one resource group. Attributes: host, port, database name,
  three logins, the allowed client address.
- **Synthetic table**: one table from `tests/synthetic`. Attributes: primary key, cursor column,
  row count.
- **Secret**: one per auth mode in scope `lakefed-cursor`. Shapes per `contracts/secret_json.md`.
- **Baseline record**: one section in `docs/baseline.md` per engine. Attributes: date, row counts,
  duration, lag, cost, tenant_id result.
- **Cost ceiling**: one amount the operator sets. Attributes: amount, currency, alert threshold.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A second operator stands up the source from the runbook in under 2 hours.
- **SC-002**: The seed, change, increment, and reconcile pass with zero rows differing on the
  primary key.
- **SC-003**: All three auth modes connect and ingest. The `tenant_id` answer is recorded.
- **SC-004**: `docs/baseline.md` holds a cost and a lag for the cursor engine and for Lakeflow
  Connect on the same table.
- **SC-005**: Feature 001 tasks T012a, T093, and T095 are marked done with a link to the record.
- **SC-006**: Total spend stays under the ceiling the operator set.
- **SC-007**: After teardown, the subscription holds zero resources of this feature.

## Assumptions

- The operator owns an Azure subscription. The operator can create a resource group, a virtual
  machine, a network rule, a budget, and an Entra service principal.
- One SQL Server 2022 on an Azure Windows virtual machine accepts all three login types. The
  Windows login is a local account over NTLM with the machine name as domain. The Entra login is
  enabled through the SQL IaaS Agent extension with a managed identity that holds the Directory
  Readers role. Entra authentication cannot be disabled once enabled. Every Entra connection must
  be encrypted, so the job sets `trust_server_certificate` or the server holds a trusted certificate.
- Sources: Microsoft Learn, "Enable Microsoft Entra authentication for SQL Server on Azure VMs"
  and "Connect using Microsoft Entra authentication" (JDBC), read on 2026-10-31. If the operator prefers two sources, the auth-mode proof may run on a second source.
- The devtest Databricks workspace is on Azure. Its job cluster reaches the source over a public
  address with a network rule limited to the workspace egress address. A private link is not
  required for a disposable test.
- The default cost ceiling is 50 USD if the operator sets none.
- The synthetic table generator in `tests/synthetic` builds the table. Its Change Tracking
  statements are not run.
- The Lakeflow Connect baseline uses the SQL login only.
- Oracle, production data, Change Tracking, and CDC are out of scope.
