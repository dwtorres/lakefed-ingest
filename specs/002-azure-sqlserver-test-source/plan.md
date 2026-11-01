---
ontology: true
type: plan
domain: lakefed-cursor
summary: Plan for the disposable Azure SQL Server test source, Terraform in the ding-dong-ditch project, proof runs, teardown
tags: [plan, azure, sqlserver, terraform, devtest, live-proof, teardown]
related: [spec, research, data-model, quickstart, contracts/terraform_toggle, ../001-cursor-only-engine/tasks, ../../.specify/memory/constitution]
status: draft
discovered: 2026-10-31
verified: false
workspace: lakefed-cursor
---

# Implementation Plan: Azure SQL Server Test Source

**Branch**: `002-azure-sqlserver-test-source` | **Date**: 2026-10-31 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/002-azure-sqlserver-test-source/spec.md`

## Summary

One disposable SQL Server 2022 VM in the devtest VNet, built by one toggle-gated Terraform file in
the ding-dong-ditch project. It accepts a SQL login, a Windows login over NTLM, and an Entra service
principal login. One synthetic table with a `rowversion` cursor proves the cursor engine (T093),
the three auth modes (T095), and the Lakeflow Connect baseline (T012a). Teardown is the toggle set
to `false` plus one `az` call. See [research.md](research.md) for each decision.

## Technical Context

**Language/Version**: Terraform 1.9 or later, azurerm `~> 4.50` (the project pins), PowerShell 5.1
on the VM, Python 3.11 notebooks on Databricks Runtime (job cluster).

**Primary Dependencies**:

- The Terraform project at `/Users/dwtorres/src/ding-dong-ditch-azure-terraform`, state backend `tfstatedingdongditch`.
- Azure CLI 2.48 or later. Azure PowerShell `Az.SqlVirtualMachine`.
- Databricks CLI 1.23, profile `ding-dong-ditch`.
- The `tests/synthetic` notebooks.

**Storage**: SQL Server database `lakefed_test` on the VM. Lakebase schema `lakefed_cursor` (state).
Sink `my_catalog.lakefed_cursor_devtest`.

**Testing**: `pytest tests` (zero skips) for the notebook helper changes. Live proof per
[quickstart.md](quickstart.md). `terraform validate` and `terraform test` in the Terraform project.

**Target Platform**: Azure eastus, subscription `9d3145a6-2bd1-4cf7-9de4-f08f8c8a91b3`, tenant
`bee4ed0d-cccd-4956-aab7-f1370bea8757`, workspace `adb-7405613017070986`.

**Project Type**: infrastructure toggle plus runbook plus live proof.

**Performance Goals**: source up in under 30 minutes (US1/AC1). Runbook from zero in under 2 hours
(SC-001).

**Constraints**: spend under 50 USD (SC-006). Private IP only (FR-003). No Change Tracking, no CDC
(FR-005). No secret value in a notebook, a doc, or a chat (FR-006).

**Scale/Scope**: one VM, one database, one table of 10,058 rows, three logins, three secrets, one
control row, one pipeline.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Note |
|---|---|---|
| I. Strip and patch | pass | Code changes are two widgets in `build_synth_db.py` and one in `change_generator.py`. T095 can delete `tenant_id` from `source_adapters.AUTH_FIELDS`. No new module. |
| II. Cursor only | pass | FR-005. `enable_change_tracking=false` skips the CT statements. The control row is `load_type cursor`. |
| III. Everything runs inside Databricks | pass | The source is external by nature. The engine reads it over Spark JDBC from a job cluster. Lakeflow Connect is a baseline, not a dependency (call 13). |
| IV. SQL Server and Oracle first-class | pass | This feature closes the SQL Server half of the live proof. Oracle ran on 2026-10-31. |
| V. One expression per table | pass | `cursor_expression = 'row_version'`. |
| VI. Checkpoint after commit | pass | No engine change. |
| VII. Keep what is tested | pass | If T095 deletes `tenant_id`, `source_adapters.py` changes, with a test change first. |
| VIII. STE docs | pass | Runbook, research, and this plan follow the sentence limits. `tests/docs_test.py` does not scan `specs/`, so the review checks it by hand. |
| IX. Own names | pass | Secrets `sqlserver_test_*` in scope `lakefed-cursor`. Azure names start with `lakefed`. No `lakefed_ingest` or `edp`. |
| Call 13 baseline | pass | US4 records cost and lag, then deletes the pipeline. |

Post-design re-check: unchanged. No violation to justify.

## Project Structure

### Documentation (this feature)

```text
specs/002-azure-sqlserver-test-source/
├── plan.md              # This file
├── research.md          # R-01 to R-12
├── data-model.md        # Entities, Terraform inputs and outputs, states
├── quickstart.md        # The runbook: build, prove, record, tear down
├── contracts/
│   └── terraform_toggle.md   # Variables, outputs, resources, teardown check
└── tasks.md             # $speckit-tasks output
```

### Source Code

```text
/Users/dwtorres/src/ding-dong-ditch-azure-terraform/      # Terraform project (outside this repo)
├── sqlserver_test.tf        # NEW: every resource of this feature, gated by enable_sqlserver_test_source
├── variables.tf             # EDIT: enable_sqlserver_test_source, sqlserver_test_cidr,
│                            #       sqlserver_test_resource_group_name, sqlserver_test_budget_usd,
│                            #       sqlserver_test_budget_start
├── network.tf               # EDIT: address_space adds var.sqlserver_test_cidr when the toggle is on
├── outputs.tf               # EDIT: sqlserver_test_* outputs (host, database, logins, sensitive passwords)
├── terraform.tfvars         # EDIT: enable_sqlserver_test_source = false (documented toggle)
└── tests/plan.tftest.hcl    # EDIT: one run block with the toggle on

/Users/dwtorres/src/work/lakefed-cursor/                    # this repo
├── tests/synthetic/build_synth_db.py     # EDIT: widgets tables, enable_change_tracking
├── tests/synthetic/change_generator.py   # EDIT: widget tables
├── tests/synthetic/README.md             # EDIT: the two widgets
├── src/lakefed_ingest/source_adapters.py # EDIT only if FR-009 says tenant_id is not needed (T095)
├── specs/001-cursor-only-engine/contracts/secret_json.md  # EDIT with the tenant_id answer
├── docs/baseline.md                      # EDIT: SQL Server sections (T012a, T093, T095)
└── specs/001-cursor-only-engine/tasks.md # EDIT: mark T012a, T093, T095 done with a link
```

**Structure Decision**: infrastructure lives in the Terraform project that owns the devtest VNet
(R-01). This repo holds the runbook, the notebook widgets, and the records.

## Plan steps

Each step ends with a check. Every step must pass before the next starts.

1. Terraform toggle. Add `sqlserver_test.tf`, the variables, the `address_space` term, the outputs,
   and the test run block. Check: `terraform validate`, `terraform test`, and `terraform plan` with
   the toggle `false` shows no change.
2. Build. Set the toggle `true`, `terraform apply`. Grant Directory Readers, enable Entra
   authentication (R-03), create the service principal (R-04), create the Entra login in SQL.
   Check: US1/AC1 (SQL login from a job cluster), US1/AC3 (refused from outside), US1/AC5 (budget
   exists with one 80 percent notification).
3. Notebook widgets. Add `tables` and `enable_change_tracking` to `build_synth_db.py`, `tables` to
   `change_generator.py`. Check: `pytest tests` zero skips. Build `doc_attachment_079` at
   `scale_factor` 0.01 with CT off, add `row_version`. US1/AC2: 10,058 rows.
4. Secrets and control row. Store the three secrets (R-12). Insert one control row (data-model.md).
   Check: `select count(*) from lakefed_cursor.control where src_host = '<private ip>'` is 1.
5. Cursor proof (US2, T093). Quickstart steps 5 to 9 with `sqlserver_test_sql`. Check: SC-002, zero
   rows differ. Record in `docs/baseline.md`.
6. Auth modes (US3, T095). Switch `secret_key` to `sqlserver_test_ntlm` and run. Switch to
   `sqlserver_test_aad_sp` and run. Remove `tenant_id` from that secret and run. Check: SC-003. Apply the
   `tenant_id` answer to `source_adapters.py` and `contracts/secret_json.md` with a test first.
7. Baseline (US4, T012a). Private network gateway on. UC connection, pipeline, seed run, change
   run, record lag, delete the pipeline. Next day: record cost. Check: SC-004, `pipelines get`
   returns not found.
8. Close and tear down. Mark T012a, T093, T095 done in feature 001 `tasks.md` with a link
   (SC-005). Toggle `false`, `terraform apply`, delete the service principal app, delete the three
   secrets. Check: SC-007, `az resource list --tag Feature=002-azure-sqlserver-test-source` is `[]`.

## Complexity Tracking

No constitution violation. Nothing to justify.

Deviation from the spec, recorded: FR-002 says every resource lives in one resource group. The
subnet lives in the VNet's group (R-07). The toggle removes it, and the SC-007 check covers it.
