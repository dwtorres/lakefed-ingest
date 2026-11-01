---
ontology: true
type: workflow-tasks
domain: lakefed-cursor
summary: Dependency-ordered task list for the disposable Azure SQL Server test source, Terraform toggle, notebook widgets, live proof, baseline, teardown
tags: [tasks, azure, sqlserver, terraform, devtest, live-proof, ntlm, entra-id, lakeflow-connect, teardown]
related: [plan, spec, research, data-model, quickstart, contracts/terraform_toggle, ../001-cursor-only-engine/tasks, ../001-cursor-only-engine/quickstart, ../001-cursor-only-engine/contracts/secret_json, ../../docs/baseline, ../../.specify/memory/constitution]
status: active
discovered: 2026-10-31
verified: true
workspace: lakefed-cursor
---

# Tasks: Azure SQL Server Test Source

**Input**: Design documents from `/specs/002-azure-sqlserver-test-source/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/terraform_toggle.md, quickstart.md

**Tests**: The spec requests no unit tests. The plan requires three checks. `pytest -rs tests` passes with zero skips after the widget change. `terraform validate` and `terraform test` pass after the toggle. The live checks in quickstart.md pass. Those checks are tasks here. Principle VII requires a failing test before the `tenant_id` code change (T042).

**Organization**: Tasks are grouped by user story. The stories are sequential by nature. US2 needs the source and the table of US1. US3 needs the control row and the seeded sink of US2. US4 needs the loaded table of US1. The states diagram in data-model.md shows the order.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3, US4)
- Include exact file paths in descriptions
- Nested bullets under a task hold its values. They are part of that task.

## Path Conventions

- Terraform project (outside this repo): `/Users/dwtorres/src/ding-dong-ditch-azure-terraform/`. Written below as `TF/`.
- This repo: `/Users/dwtorres/src/work/lakefed-cursor/`. Paths below are relative to it.
- Every `databricks` command takes `--profile ding-dong-ditch`. Never auto-select a profile.
- No secret value goes into a notebook, a doc, a task note, or a chat (FR-006).

---

## Phase 1: Setup (Terraform toggle)

**Purpose**: One toggle-gated Terraform file in the ding-dong-ditch project, per contracts/terraform_toggle.md and R-01. Plan step 1. Nothing is applied in this phase.

- [X] T001 Add five variables to `TF/variables.tf`. Copy the validation pattern of `enable_private_network_gateway` at line 170.
  - `enable_sqlserver_test_source`: bool, default `false`, validation `!var.enable_sqlserver_test_source || var.create_new_vnet`
  - `sqlserver_test_cidr`: string, default `"10.0.17.0/24"`, validation: no overlap with `var.cidr` or `var.png_test_cidr`
  - `sqlserver_test_resource_group_name`: string, default `"rg-lakefed-sqlserver-test"`
  - `sqlserver_test_budget_usd`: number, default `50`, validation `> 0`
  - `sqlserver_test_budget_start`: string, default `"2026-10-01T00:00:00Z"`, validation: first day of a month
- [X] T002 Create `TF/sqlserver_test.tf` with the network resources. Start with `locals { sqlserver_test_count = var.enable_sqlserver_test_source ? 1 : 0 }` and a header comment like `TF/png.tf` lines 1 to 10. Every resource has `count = local.sqlserver_test_count` and tags `merge(var.tags, { Feature = "002-azure-sqlserver-test-source" })`.
  - `azurerm_resource_group.sqlserver_test`: name `var.sqlserver_test_resource_group_name`
  - `azurerm_subnet.sqlserver_test`: name `ding-dong-ditch-sqlserver-test-subnet`, `address_prefixes = [var.sqlserver_test_cidr]`, in `local.vnet`
  - `azurerm_subnet_nat_gateway_association.sqlserver_test`
  - `azurerm_network_security_group.sqlserver_test`: name `lakefedsqltest-nsg`. Allow TCP 1433 from `var.subnet_public_cidr` and `var.subnet_private_cidr`. If `var.enable_private_network_gateway` is true, also allow `local.png_gateway_cidr`. Deny all other inbound. No rule has `source_address_prefix = "*"`.
  - `azurerm_subnet_network_security_group_association.sqlserver_test`
- [X] T003 In `TF/sqlserver_test.tf` add the passwords, the NIC, the VM, and the SQL IaaS registration.
  - `random_password.sqlserver_test_admin`, `random_password.sqlserver_test_sql`, `random_password.sqlserver_test_ntlm`: `length = 24`, `special = true`
  - `azurerm_network_interface.sqlserver_test`: dynamic private IP, no public IP
  - `azurerm_windows_virtual_machine.sqlserver_test`: name `lakefedsqltest` (15 characters or fewer), `size = "Standard_D2ds_v4"`, `admin_username = "sqladmin"`, `admin_password = random_password.sqlserver_test_admin[0].result`, `identity { type = "SystemAssigned" }`
  - Image: publisher `MicrosoftSQLServer`, offer `sql2022-ws2022`, sku `sqldev-gen2`, version `latest`
  - `azurerm_mssql_virtual_machine.sqlserver_test`: `sql_license_type = "PAYG"`, `sql_connectivity_type = "PRIVATE"`, `sql_connectivity_port = 1433`, `sql_connectivity_update_username = "sqladmin"`, `sql_connectivity_update_password = random_password.sqlserver_test_admin[0].result`
- [X] T004 In `TF/sqlserver_test.tf` add `azurerm_virtual_machine_extension.sqlserver_test_setup`. Publisher `Microsoft.Compute`, type `CustomScriptExtension`, `depends_on` the mssql VM. Pass the passwords through `protected_settings`, never `settings`. Do not enable Change Tracking (FR-005). The PowerShell does these steps:
  - Create local Windows user `lakefed_ntlm` with `random_password.sqlserver_test_ntlm`
  - Create database `lakefed_test`
  - Create SQL login `lakefed_sql` with `random_password.sqlserver_test_sql`
  - Create Windows login `[lakefedsqltest\lakefed_ntlm]`
  - Create both users in `lakefed_test` and add both to `db_owner`
- [X] T005 In `TF/sqlserver_test.tf` add `azurerm_consumption_budget_resource_group.sqlserver_test` (FR-013, R-08).
  - `resource_group_id = azurerm_resource_group.sqlserver_test[0].id`, `amount = var.sqlserver_test_budget_usd`, `time_grain = "Monthly"`
  - `time_period { start_date = var.sqlserver_test_budget_start }`
  - One `notification`: `enabled = true`, `threshold = 80`, `operator = "GreaterThanOrEqualTo"`, `threshold_type = "Actual"`, `contact_emails = [var.admin_user]`
- [X] T006 Edit `TF/network.tf` line 7. Set `address_space` to `concat([var.cidr], var.enable_private_network_gateway ? [var.png_test_cidr] : [], var.enable_sqlserver_test_source ? [var.sqlserver_test_cidr] : [])`.
- [X] T007 Add six outputs to `TF/outputs.tf`. If the toggle is off, each output is `null`.
  - `sqlserver_test_host`: NIC private IP
  - `sqlserver_test_database`: `"lakefed_test"`
  - `sqlserver_test_ntlm_domain`: `"lakefedsqltest"`
  - `sqlserver_test_sql_secret_json`: sensitive, `jsonencode({ user = "lakefed_sql", password = ... })`
  - `sqlserver_test_ntlm_secret_json`: sensitive, `jsonencode({ auth_mode = "ntlm", domain = "lakefedsqltest", user = "lakefed_ntlm", password = ... })`
  - `sqlserver_test_admin_password`: sensitive
- [X] T008 Add `enable_sqlserver_test_source = false` to `TF/terraform.tfvars`. Add a one-line comment that names this feature and the teardown rule.
- [X] T009 Add one `run` block to `TF/tests/plan.tftest.hcl` with `enable_sqlserver_test_source = true` and `create_new_vnet = true`. Assert `azurerm_windows_virtual_machine.sqlserver_test[0].name == "lakefedsqltest"`. Assert no NSG rule on port 1433 has `source_address_prefix == "*"`.
- [X] T010 In `TF/` run `terraform fmt -check`, `terraform validate`, and `terraform test`. Then run `terraform plan` with the toggle `false`. Expected: no change. Record the results in the task note.
  - Result 2026-10-31: `fmt -check` clean, `validate` valid, `test` 7 passed 0 failed, `plan` with the toggle `false`: "No changes". The plan needs `TF_VAR_databricks_account_id`.

**Checkpoint**: The toggle exists and is off. The devtest state is unchanged.

---

## Phase 2: Foundational (notebook widgets)

**Purpose**: `tests/synthetic` can build one table without Change Tracking and change one table. Plan step 3, R-10. Blocks US1/AC2 and US2.

- [X] T011 [P] Add two widgets to `tests/synthetic/build_synth_db.py` in the widget list at lines 28 to 35. Update the markdown header at lines 3 to 8.
  - `tables`: default `""`, comma list, empty = every table. If set, filter `catalog` to the named tables. If a name is not in the catalog, fail with the unknown names.
  - `enable_change_tracking`: default `"true"`. Wrap the Change Tracking cell (lines 144 to 151) in `if W["enable_change_tracking"].lower() == "true":`. Keep the `ct_db` and `ct_tables` summary fields. With CT off they report 0.
- [X] T012 [P] Add widget `tables` (default `""`) to `tests/synthetic/change_generator.py` in the widget list at lines 24 to 33. If set, replace `picked = ch.pick_tables(...)` at line 66 with the catalog entries in the list. If a name is not in the catalog, fail with the unknown names. `synth_catalog.py`, `synth_sql.py`, and `synth_changes.py` do not change.
- [X] T013 [P] In the widget table of `tests/synthetic/README.md` (lines 50 to 62) add a row for `tables` (both notebooks). Add a row for `enable_change_tracking` (build only). Keep each sentence to 25 words or fewer.
- [X] T014 Run `.venv/bin/pytest -rs tests`. Expected: pass, zero skips. `tests/synthetic_test.py` still passes because the pure modules did not change.
  - Result 2026-10-31: 482 passed, 0 skipped.

**Checkpoint**: Widgets exist. Unit tests pass. The source does not exist yet.

---

## Phase 3: User Story 1 - Stand up and tear down the source (Priority: P1) 🎯 MVP

**Goal**: One SQL Server 2022 VM in the devtest VNet with three logins. One synthetic table of 10,058 rows with a `rowversion` cursor. A budget alert at 80 percent. Quickstart steps 1 to 15. Teardown itself is the last phase, because US2 to US4 need the source.

**Independent Test**: Create the source. Connect from a devtest notebook with `lakefed_sql`. Read `COUNT(*)` = 10,058. Run the teardown phase. The teardown check lists zero resources.

### Build (quickstart steps 1 to 10)

- [X] T015 [US1] Note the start time (SC-001, US1/AC1). In `TF/terraform.tfvars` set `enable_sqlserver_test_source = true`. Run `terraform plan`. Expected: adds only, zero destroys. Run `terraform apply`. Expected: done in under 30 minutes. Note `terraform output sqlserver_test_host`.
  - Result 2026-10-31: start 16:52:21 EDT, apply complete 17:03:43 EDT. Two fixes: DSv5 quota 0 (size `Standard_D2ds_v4`), provider `Microsoft.SqlVirtualMachine` registered. Host `10.0.17.4`.
- [X] T016 [US1] Grant the VM identity `lakefedsqltest` the Entra role Directory Readers (R-03, needs Privileged Role Administrator). Portal path: Microsoft Entra ID > Roles and administrators > Directory Readers > Add assignments.
  - Result 2026-10-31: assigned through Graph `roleManagement/directory/roleAssignments`, assignment id recorded in the session only.
- [X] T017 [US1] Enable Entra authentication on the SQL instance: `Update-AzSqlVM -ResourceGroupName rg-lakefed-sqlserver-test -Name lakefedsqltest -IdentityType 'SystemAssigned'`. Fallback: portal, SQL virtual machines > Security Configuration. Expected: Entra authentication shows enabled.
  - Result 2026-10-31: `az sql vm enable-azure-ad-auth` (no PowerShell needed). `validate-azure-ad-auth` passed first. Setting present with empty `clientId` (system-assigned).
- [X] T018 [US1] Create the service principal: `az ad sp create-for-rbac --name sp-lakefed-sqlserver-test --skip-assignment`. Keep `appId`, `password`, and `tenant` in the terminal only (R-04).
  - Result 2026-10-31: created by the operator, piped straight into secret `sqlserver_test_aad_sp` (T037 merged). No value printed.
- [X] T019 [US1] Open a devtest notebook connected as `sqladmin` with `trust_server_certificate=true`. Run the four statements of quickstart step 7.
  - Result 2026-10-31: through `az vm run-command invoke` with `sqlcmd` as `sqladmin` on the VM (the admin password never left the shell). `sys.database_principals` shows `sp-lakefed-sqlserver-test EXTERNAL_USER`.
  - `CREATE LOGIN [sp-lakefed-sqlserver-test] FROM EXTERNAL PROVIDER`
  - `USE lakefed_test`
  - `CREATE USER [sp-lakefed-sqlserver-test] FROM EXTERNAL PROVIDER`
  - `ALTER ROLE db_owner ADD MEMBER [sp-lakefed-sqlserver-test]`
- [X] T020 [US1] Store the SQL secret (R-12, quickstart step 12): `cd TF && terraform output -raw sqlserver_test_sql_secret_json | databricks secrets put-secret lakefed-cursor sqlserver_test_sql --profile ding-dong-ditch`.
- [X] T021 [US1] Check US1/AC1. From a devtest job cluster, run a Spark JDBC read of `SELECT 1` with secret `sqlserver_test_sql` and `trustServerCertificate=true`. Expected: one row. Record the elapsed time since T015 (expected under 30 minutes).
  - Result 2026-10-31: run 482598982522514 returned `[{"one": 1}]` at 17:11 EDT, 19 minutes after T015 start.
- [X] T022 [P] [US1] Check US1/AC3. From the laptop run `nc -z -w 3 <sqlserver_test_host> 1433`. Expected: the connection fails (private IP, FR-003).
  - Result 2026-10-31: `nc` exit 1, connection refused.
- [X] T023 [P] [US1] Check US1/AC5. Run `az consumption budget list -g rg-lakefed-sqlserver-test -o table`. Expected: one budget, amount 50 (or the ceiling set), one notification at threshold 80.
  - Result 2026-10-31: one budget `lakefedsqltest-budget`, amount 50, Monthly, one notification at 80 GreaterThanOrEqualTo.

### Load the table (quickstart steps 11 to 15)

- [X] T024 [US1] Import `tests/synthetic` into the devtest workspace. Keep the `.py` modules next to the notebooks: `databricks workspace import-dir tests/synthetic <workspace path> --profile ding-dong-ditch`.
  - Result 2026-10-31: `/Workspace/Users/<me>/lakefed_synthetic`, plus a one-off check notebook `sqlserver_check`.
- [X] T025 [US1] Run `build_synth_db.py` on a devtest job cluster. Expected: summary `tables_present` 1, `ct_db` 0, `ct_tables` 0, zero failures.
  - Result 2026-10-31: run 1098436613622719, `tables_present` 1, `built` 1, `ct_db` 0, `ct_tables` 0, `total_rows` 10058, 3 s load.
  - Connection widgets: `host=<sqlserver_test_host>`, `port=1433`, `database=lakefed_test`, `secret_scope=lakefed-cursor`, `secret_key=sqlserver_test_sql`, `trust_server_certificate=true`
  - Build widgets: `scale_factor=0.01`, `tables=doc_attachment_079`, `enable_change_tracking=false`
- [X] T026 [US1] From a notebook as `lakefed_sql`, run `ALTER TABLE dbo.doc_attachment_079 ADD row_version rowversion;` (R-10).
  - Result 2026-10-31: executed through the `sqlserver_check` notebook `exec` widget as `lakefed_sql`.
- [X] T027 [US1] Check US1/AC2. `SELECT COUNT(*) FROM dbo.doc_attachment_079` returns 10058. `SELECT COUNT(*) FROM sys.change_tracking_databases WHERE database_id = DB_ID()` returns 0 (FR-005). Record the elapsed time since T015 for SC-001 (expected under 2 hours).
  - Result 2026-10-31 17:22 EDT: `rows_079` 10058, `ct_db` 0, `row_version` present. 30 minutes after T015 start.

**Checkpoint**: Source state `loaded`. US1/AC1, AC2, AC3, AC5 pass. AC4 is the teardown phase.

---

## Phase 4: User Story 2 - Prove the cursor engine on a live table (Priority: P1)

**Goal**: Feature 001 quickstart steps 5 to 9 pass against the live table with the SQL login. Zero rows differ. Record in `docs/baseline.md`. Closes T093.

**Independent Test**: Start with the source in state `loaded` and an empty sink. Run seed, change set, increment, reconcile, no-change run. The reconcile reports zero differences.

- [X] T028 [US2] Insert the control row from data-model.md into `lakefed_cursor.control`. Check: `select count(*) from lakefed_cursor.control where src_host = '<sqlserver_test_host>'` is 1.
  - Result 2026-10-31: inserted with local `psql` and `databricks database generate-database-credential`. Count 1.
  - `task_collection='devtest'`, `src_type='sqlserver'`, `src_host=<sqlserver_test_host>`, `src_port=1433`, `secret_key='sqlserver_test_sql'`
  - `src_database='lakefed_test'`, `src_schema='dbo'`, `src_table='doc_attachment_079'`, `primary_key='id'`, `select_list` the 16 catalog columns plus `row_version` (the engine quotes each name, so `*` fails)
  - `sink_catalog='my_catalog'`, `sink_schema='lakefed_cursor_devtest'`, `sink_table='sqlserver_doc_attachment_079'`
  - `load_type='cursor'`, `cursor_expression='row_version'`, `cursor_kind='rowversion'`, `delete_mode='pk_snapshot'`, `delete_min_interval_s=0`
- [X] T029 [US2] Seed (feature 001 quickstart step 5): `databricks bundle run lakefed_cursor -t devtest -p ding-dong-ditch --params task_collection=devtest,trust_server_certificate=true`. Expected: sink `sqlserver_doc_attachment_079` holds 10,058 rows, checkpoint phase `incremental`. Note the run duration and the `capture` event count in `lakefed_cursor.ingest_event`.
  - Result 2026-10-31: three runs. Run 13 failed at `ensure_sink`: `select_list='*'` is not valid, the engine quotes each name (control row and docs fixed). Run 14 failed at `read`: `KeyError: 'src_now'` in `cursor_engine.high_watermark` for a numeric kind on a Spark `Row` (fixed, test `test_numeric_high_watermark_reads_no_clock_column`). Run 15 seeded: sink 10058 rows, checkpoint `incremental`, position 12099, in-engine 24 s, wall clock 4 min 2 s, `capture` events 0.
- [X] T030 [US2] Change set (step 6): run `change_generator.py` with the T025 connection widgets. Expected: summary inserted 10, updated 5, deleted 5. Note the wall-clock time of the last change.
  - Result 2026-10-31: run_label `chg-us2`, inserted 10, updated 5, deleted 5, last change 17:41:25 EDT.
  - Change widgets: `tables=doc_attachment_079`, `inserts=10`, `updates=5`, `deletes=5`, `manifest_table=my_catalog.lakefed_cursor_devtest.synth_manifest`
- [X] T031 [US2] Increment (step 7): run the job again with the T029 params. Expected: MERGE 10 inserted and 5 updated, snapshot DELETE 5, `incremented 1`. Note the duration. Note the lag from the T030 change time to the sink commit time.
  - Result 2026-10-31: run 16, `incremented 1`, position 12099 to 12109, MERGE inserted 10 updated 0, snapshot DELETE 5. Updated 0 is correct: `UPDATE TOP (5)` and `DELETE TOP (5)` with no ORDER BY hit the same rows. Wall clock 17:41:37 to 17:46:01 (4 min 24 s), in-engine 34 s. Lag change to MERGE commit 4 min 11 s. One `capture` event.
- [X] T032 [US2] Reconcile (step 8): run `reconcile.py` with the T025 connection widgets and the same manifest table. Expected: zero differences on `id` (SC-002).
  - Result 2026-10-31: `tables_checked` 1, `matched` 1, `mismatched` []. Through a temporary view `doc_attachment_079` over the sink, dropped after.
- [X] T033 [US2] No-change run (step 9): run the job again. Expected: no new version in `DESCRIBE HISTORY my_catalog.lakefed_cursor_devtest.sqlserver_doc_attachment_079`. The checkpoint `freshness` moves.
  - Result 2026-10-31: run 17, `unchanged 1`. No MERGE version. One empty snapshot DELETE version (version 4, 0 rows), from `delete_min_interval_s 0`. `last_success_at` moved to 21:54:42Z.
- [X] T034 [US2] In `docs/baseline.md` add section `## Devtest run (T093)` after the T089 section. Keep sentences to 25 words or fewer. Record these values:
  - Result 2026-10-31: section written. Cost per run pending billing.
  - Date, source and sink row counts, `capture` event count
  - Seed and increment durations, the lag from T031
  - The four FR-011 values for the cursor engine
  - Cost per run: job cluster DBU from `system.billing.usage` on `usage_metadata.job_id`, filled the next day

**Checkpoint**: Source state `proven`. SC-002 holds. T093 evidence is in `docs/baseline.md`.

---

## Phase 5: User Story 3 - Prove the three auth modes (Priority: P2)

**Goal**: One increment each over `ntlm` and `aad_sp`. The `tenant_id` answer is recorded and applied to the code. Closes T095.

**Independent Test**: Switch the control row `secret_key` to each secret in turn and run the job. Each run reports `incremented 1`. A failure names the auth mode and holds no secret value.

- [X] T035 [US3] Store the NTLM secret: `cd TF && terraform output -raw sqlserver_test_ntlm_secret_json | databricks secrets put-secret lakefed-cursor sqlserver_test_ntlm --profile ding-dong-ditch`.
- [X] T036 [US3] Run `update lakefed_cursor.control set secret_key = 'sqlserver_test_ntlm' where src_host = '<sqlserver_test_host>'`. Run the job with the T029 params. Expected: `incremented 1` (US3/AC1). If it fails, record the error text with no secret value. Then read the edge cases in spec.md: the domain is the machine name, and TLS needs `trust_server_certificate`.
  - Result 2026-10-31: run 18, `ok 1`, outcome `unchanged 1` (quiet table, so no MERGE). Capture and PK snapshot ran over NTLM. Wall clock 4 min 3 s.
- [X] T037 [US3] Store the Entra secret: `databricks secrets put-secret lakefed-cursor sqlserver_test_aad_sp --profile ding-dong-ditch`. Type into the hidden prompt `{"auth_mode": "aad_sp", "client_id": "<appId>", "client_secret": "<password>", "tenant_id": "<tenant>"}` with the values from T018.
  - Result 2026-10-31: stored with T018 by pipe.
- [X] T038 [US3] Set `secret_key = 'sqlserver_test_aad_sp'`. Run the job. Expected: `incremented 1` (US3/AC2). If a certificate chain error appears, check that `trust_server_certificate=true` is in the params. If a login error appears, check that T019 ran.
  - Result 2026-10-31: run 19, `ok 1`, `unchanged 1`. Wall clock 9 min 20 s.
- [X] T039 [US3] Store `sqlserver_test_aad_sp` again without `tenant_id`. Run the job. If it connects, the answer is `not needed`. If it fails, the answer is `needed`. R-05 expects `not needed` (FR-009, US3/AC3).
  - Result 2026-10-31: run 20 failed in `extract_creds`: `missing tenant_id` (engine check, not the driver). Run 19 already proved the driver needs no tenant. Run 21 after T043: `ok 1`. Answer `not needed`.
- [X] T040 [US3] Check US3/AC4. Download the three run logs with `databricks jobs get-run-output <run_id> --profile ding-dong-ditch`. Run `rg -c 'password|client_secret' <log>` and search for the literal secret values. Expected: no secret value in any log or `ingest_event.message`.
  - Result 2026-10-31: 10 job run logs plus `ingest_event.error` since run 13. Key words 0, literal values 0.
- [X] T041 [US3] In `docs/baseline.md` add section `## Auth modes (T095)`. Record one line per mode (`sql`, `ntlm`, `aad_sp`) with date, result, and duration. Record the `tenant_id` answer, `needed` or `not needed`. Record the AC4 check result.
  - Result 2026-10-31: section written.
- [X] T042 [US3] Answer `not needed`: change the tests that pin `tenant_id` first (Principle VII). Run `.venv/bin/pytest -rs tests`. Expected: the changed tests fail. Answer `needed`: skip this task.
  - Result 2026-10-31: tests changed, suite 1 failed 482 passed (`missing tenant_id`).
  - `tests/source_adapters_test.py` lines 261 and 272: drop `tenant_id` from the `aad_sp` secret
  - `tests/source_io_test.py` line 252: `test_extract_creds_aad_sp_needs_tenant_id` becomes a test that an `aad_sp` secret without `tenant_id` is accepted
  - `tests/worker_test.py` line 291: drop `tenant_id`
- [X] T043 [US3] Answer `not needed`: apply it to the code as listed below. Run `.venv/bin/pytest -rs tests`. Expected: pass, zero skips. Answer `needed`: keep `AUTH_FIELDS`. Write a failing test first. Then pass `tenant_id` to the driver in `source_adapters.auth_options` as the user form `<client_id>@<tenant_id>`.
  - Result 2026-10-31: `AUTH_FIELDS` and the comment changed. Suite 483 passed.
  - `src/lakefed_ingest/source_adapters.py` line 47: `"aad_sp": ("client_id", "client_secret")`. Delete the shortcut comment at line 231.
  - `src/lakefed_ingest/source_io.py`: remove the `extract_creds` branch that raises on a missing `tenant_id`.
- [X] T044 [US3] Apply the answer to `specs/001-cursor-only-engine/contracts/secret_json.md` lines 26 and 31. Drop `tenant_id` from the `aad_sp` row and example, or add a note that the driver takes it in the user form. If `CLAUDE.md` or `README.md` lists `tenant_id` for `aad_sp`, update that sentence. Run `.venv/bin/pytest tests/docs_test.py`.
  - Result 2026-10-31: `secret_json.md`, `README.md`, `CLAUDE.md` updated. `docs_test.py` passes.

**Checkpoint**: Source state `modes proven`. SC-003 holds. T095 evidence is in `docs/baseline.md`. The code matches the driver.

---

## Phase 6: User Story 4 - Record the Lakeflow Connect baseline (Priority: P3)

**Goal**: The same table through a Lakeflow Connect query-based pipeline, one seed and one change. Cost and lag recorded. Pipeline deleted. Closes T012a. Fixed decision call 13.

**Independent Test**: Create the pipeline, run it twice, read the lag and the cost, delete it. `databricks pipelines get <id>` returns not found.

- [X] T045 [US4] In `TF/terraform.tfvars` set `enable_private_network_gateway = true`. Run `terraform apply`. Follow `TF/docs/private-network-gateway-runbook.md` to attach the devtest workspace. Expected: the NSG from T002 now allows `local.png_gateway_cidr` on 1433 (R-06). Note the extra daily cost against the ceiling.
  - Result 2026-10-31: apply 21:54:30 to 21:57:44 EDT, 14 added 3 changed. Gateway `ESTABLISHED`, `ALL_TRAFFIC`, on `ding-dong-ditch-ncc`, the NCC the devtest workspace already binds to. No account console switch needed. NSG gained `allow-sql-2` for `10.0.16.0/28`.
- [X] T046 [US4] Create a UC connection named `lakefed_sqlserver_test` with `databricks connections create --profile ding-dong-ditch`. Pipe the password from the `sqlserver_test_sql` secret.
  - Result 2026-10-31: `lakefed_sqlserver_test`, SQLSERVER, `10.0.17.4:1433`, user `lakefed_sql`, password piped from Terraform output.
  - Type `SQLSERVER`, host `<sqlserver_test_host>`, port 1433, `trustServerCertificate` true, user `lakefed_sql`
- [X] T047 [US4] Create a query-based ingestion pipeline with `databricks pipelines create --json @<spec> --profile ding-dong-ditch`. Copy the spec shape from the Oracle section of `docs/baseline.md` (lines 23 to 39). Save the spec with no secret value at `specs/002-azure-sqlserver-test-source/contracts/lakeflow_pipeline.json`. Note the pipeline id.
  - Result 2026-10-31: pipeline id `e5303890-c006-4396-b01e-c3be376b3e67`. Spec saved in `contracts/lakeflow_pipeline.json` (no secret). Cursor `row_version`, `primary_keys [id]`, hard deletion scan at 60 s.
  - Serverless, triggered, one table object `dbo.doc_attachment_079`, cursor column `row_version`
  - Target `my_catalog.lakefed_cursor_devtest.sqlserver_baseline_doc_attachment_079`
- [X] T048 [US4] Seed run: `databricks pipelines start-update <id> --profile ding-dong-ditch`. Expected: target holds 10,058 rows (US4/AC1). Note the wall-clock duration and the lag to the Delta commit time.
  - Result 2026-10-31: update `f6a0ad5e` COMPLETED, 68 s wall clock, 10,063 rows (source count after US2), MERGE commit 51 s after `start-update`. Needed a DNS A record first: serverless refuses IPv4 literals through the gateway.
- [X] T049 [US4] Run `change_generator.py` once more with the T030 widgets and a new `run_label`. Note the change time. Start the pipeline update. Expected: the target shows 10 inserts, 5 updates, 5 deletes (US4/AC2). Note the lag from the change time to the commit time.
  - Result 2026-10-31: `chg-us4` at 22:22:22 EDT. Update COMPLETED in 84 s. Target 10,068: MERGE 10 inserted, MERGE 5 updated, DELETE. Reconcile by id and rowversion: 0 differences. Lag 1 min 41 s.
- [X] T050 [US4] Delete the pipeline: `databricks pipelines delete <id> --profile ding-dong-ditch`. Check: `databricks pipelines get <id> --profile ding-dong-ditch` returns not found (FR-012, US4/AC4). Delete the UC connection `lakefed_sqlserver_test`.
  - Result 2026-10-31: pipeline and connection deleted. `pipelines get` returns not found.
- [X] T051 [US4] In `docs/baseline.md` fill section `## SQL Server (T012a)` (line 19). Record date, row counts, the two run durations, the two lags, the pipeline id, and the four FR-011 values. Leave the cost line as "pending billing" until T052.
  - Result 2026-10-31: section written. Cost pending billing.
- [X] T052 [US4] The next day, query `system.billing.usage` with `usage_metadata.dlt_pipeline_id = '<id>'` and with the cursor engine job id from T034. Record cost per run for both engines side by side in `docs/baseline.md` (US4/AC3, SC-004). Sections: `## SQL Server (T012a)` and `## Devtest run (T093)`.
  - Result 2026-11-01 07:00 EDT: Lakeflow 0.339 DBU over 2 updates on the free SKU (0 USD). Cursor job 0.444 DBU over 9 runs, 0.049 DBU per run, 0.015 USD. Both in `docs/baseline.md`.

**Checkpoint**: Source state `baselined`. SC-004 holds. T012a evidence is in `docs/baseline.md`.

---

## Phase 7: Close feature 001 and tear down

**Purpose**: SC-005, SC-006, SC-007, US1/AC4. Teardown is last because every proof needs the source.

WARNING: T056 to T060 delete the VM, its disk, the database, the table, the secrets, and the sink tables. Finish T034, T041, T051, and T052 first.

- [X] T053 [P] In `specs/001-cursor-only-engine/tasks.md` mark T012a (line 67), T093 (line 376), and T095 (line 378) as `[X]`. Append a link to the matching `docs/baseline.md` section (SC-005).
  - Result 2026-10-31: marked with links to the three baseline sections.
- [X] T054 [P] Check that the `docs/baseline.md` frontmatter follows MOP. Set `verified: true`. Add this feature to `related`.
  - Result 2026-10-31: frontmatter has every MOP field, `verified: true`, feature 002 added to `related`.
  - Fields: `ontology`, `type`, `domain`, `summary`, `tags`, `related`, `status`, `discovered`, `verified`, `workspace`
- [X] T055 [P] Hand-check STE in the feature docs and the new `docs/baseline.md` sections. `tests/docs_test.py` does not scan `specs/`. Record the check in the task note. Set `status: active` and `verified: true` in each spec file's frontmatter.
  - Result 2026-10-31: scripted check on the four files: no banned word; one 35-word sentence in R-02 split. The other flags were frontmatter and list artifacts. Frontmatter set to `status: active`, `verified: true`.
  - Files: `specs/002-azure-sqlserver-test-source/quickstart.md`, `research.md`, `plan.md`, `tasks.md`
  - Rules: instructions 20 words or fewer, descriptions 25 words or fewer
  - Banned words: `utilize`, `ensure`, `facilitate`, `leverage`, `commence`, `terminate`
- [X] T056 In `TF/terraform.tfvars` set `enable_sqlserver_test_source = false`. If T045 turned the gateway on only for this test, set `enable_private_network_gateway = false` too. Run `terraform plan`. Expected: destroys only the feature resources, changes nothing else. Run `terraform apply`.
  - Result 2026-11-01: gateway off applied first (00:09 to 07:01 EDT, the Databricks gateway took 7 hours to release its NIC). Source toggle off: plan 13 destroys 1 change, apply 07:11:19 to 07:14:53 EDT.
- [X] T057 Delete the service principal (R-04): `az ad app delete --id "$(az ad app list --display-name sp-lakefed-sqlserver-test --query '[0].appId' -o tsv)"`.
  - Result 2026-11-01: app deleted. `az ad app list` and `az ad sp list` return no line.
- [X] T058 Delete the three secrets: `for k in sqlserver_test_sql sqlserver_test_ntlm sqlserver_test_aad_sp; do databricks secrets delete-secret lakefed-cursor $k --profile ding-dong-ditch; done`.
  - Result 2026-11-01: three secrets deleted. Scope holds `oracle_devtest` only.
- [X] T059 Delete the control row: `delete from lakefed_cursor.control where src_host = '<sqlserver_test_host>'`. Delete its checkpoint row too. Delete the sink tables in `my_catalog.lakefed_cursor_devtest`: `sqlserver_doc_attachment_079`, `sqlserver_baseline_doc_attachment_079`, `synth_manifest`.
  - Result 2026-11-01: checkpoint and control row deleted (count 0). `sqlserver_doc_attachment_079` and `synth_manifest` dropped. Schema holds `oracle_lakefed_devtest` only.
- [X] T060 Run the teardown check from `contracts/terraform_toggle.md`. Record the four results in the task note (SC-007, US1/AC4).
  - Result 2026-11-01: tagged resources 0, `az group exists` false, feature subnets 0, app 0. VNet address space back to `10.0.0.0/20`. Terraform state has no `sqlserver_test` or `png` entry.
  - `az resource list --tag Feature=002-azure-sqlserver-test-source -o tsv`: expect no line
  - `az group exists -n rg-lakefed-sqlserver-test`: expect `false`
  - `az network vnet subnet list -g rg-databricks-network --vnet-name ding-dong-ditch-vnet --query "[?contains(name,'sqlserver-test')]" -o tsv`: expect no line
  - `az ad app list --display-name sp-lakefed-sqlserver-test -o tsv`: expect no line
- [ ] T061 Read the actual spend. Before T056, read the budget. After T056, read the subscription cost analysis filtered on tag `Feature=002-azure-sqlserver-test-source` the next day. Record total spend against the ceiling in `docs/baseline.md` (SC-006).
  - Partial 2026-11-01: budget `currentSpend` read 0.0 USD before teardown (Azure cost lags a day). Cost Management by tag: read on 2026-11-02 and record in `docs/baseline.md`.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: no dependency. T001 to T009 edit the Terraform project. T010 checks them. Nothing is applied.
- **Foundational (Phase 2)**: no dependency on Phase 1. T011, T012, T013 run in parallel. T014 runs after them.
- **US1 (Phase 3)**: needs Phase 1 (T015 applies the toggle) and Phase 2 (T025 uses the widgets).
- **US2 (Phase 4)**: needs US1 in state `loaded` (T027) and the SQL secret (T020).
- **US3 (Phase 5)**: needs US2 (control row T028, seeded sink T029). T042 to T044 need the answer from T039.
- **US4 (Phase 6)**: needs US1 in state `loaded`. It can run before or after US2 and US3. It shares the source table and `change_generator.py`. Do not run it at the same time as T030 or T031. T052 runs the next day.
- **Close and tear down (Phase 7)**: T053 to T055 need T034, T041, T051. T056 to T061 need T052 (the billing read). Teardown is last.

### User Story Dependencies

- **US1 (P1)**: Terraform toggle and notebook widgets only.
- **US2 (P1)**: US1. Independent of US3 and US4.
- **US3 (P2)**: US2 (the seeded sink and the control row). Independent of US4.
- **US4 (P3)**: US1 (the loaded table). Independent of US2 and US3, except the shared table.

### Within Each User Story

- US1: T015 → T016 → T017 → T018 → T019 → T020 → T021. T022 and T023 run in parallel after T015. T024 → T025 → T026 → T027 run after T020.
- US2: T028 → T029 → T030 → T031 → T032 → T033 → T034.
- US3: T035 → T036 → T037 → T038 → T039 → T040 → T041 → T042 → T043 → T044.
- US4: T045 → T046 → T047 → T048 → T049 → T050 → T051 → T052.

### Parallel Opportunities

- Phase 1 and Phase 2 can run at the same time (different projects).
- Within Phase 2: T011, T012, T013.
- Within US1: T022 and T023 while T016 to T019 run.
- Within Phase 7: T053, T054, T055.

---

## Parallel Example: Foundational

```bash
# Three files, no shared lines:
Task: "Add widgets tables and enable_change_tracking to tests/synthetic/build_synth_db.py"
Task: "Add widget tables to tests/synthetic/change_generator.py"
Task: "Document the two widgets in tests/synthetic/README.md"
# Then: .venv/bin/pytest -rs tests
```

## Parallel Example: User Story 1

```bash
# While the operator does the portal and PowerShell steps (T016 to T019):
Task: "nc -z -w 3 <sqlserver_test_host> 1433 from the laptop, expect failure"
Task: "az consumption budget list -g rg-lakefed-sqlserver-test, expect one budget at 80"
```

---

## Implementation Strategy

### MVP First (User Story 1 only)

1. Complete Phase 1. The toggle passes `terraform validate`. With the toggle off, `terraform plan` shows no change.
2. Complete Phase 2. The widgets exist and `pytest` passes with zero skips.
3. Complete Phase 3. The source is up, the table holds 10,058 rows, the budget exists.
4. **STOP and CHECK**: US1/AC1, AC2, AC3, AC5. Record the elapsed time for SC-001.
5. If the proof must wait, run Phase 7 T056 to T060 now to stop the spend. Rebuild later.

### Incremental Delivery

1. US1 → source `loaded`.
2. US2 → `proven`, T093 recorded.
3. US3 → `modes proven`, T095 recorded, code matches the driver.
4. US4 → `baselined`, T012a recorded, pipeline deleted.
5. Phase 7 → feature 001 tasks closed, subscription clean.

### Cost Control

- `Standard_D2ds_v4` plus a 128 GB Premium SSD is about 3 USD per day (R-08). The gateway (T045) adds more. Keep US4 short. Turn the gateway off in T056.
- If the budget alert fires before T052, tear down (T056 to T060). Record the partial result in `docs/baseline.md`.

---

## Notes

- [P] tasks = different files, no dependencies.
- The source is one shared resource. The live tasks of one story run one at a time.
- Commit after each phase. Terraform changes are in the ding-dong-ditch project. This repo's changes are in this feature branch.
- Never paste a Terraform output, a `password`, or a `client_secret` into a chat or a task note. Pipe them with `terraform output -raw ... | databricks secrets put-secret`.
- Stop at any checkpoint. Every state in data-model.md can go to `absent` through T056 to T060.

---

## Phase 8: Convergence

- [X] T062 Remove `tenant_id` from the `aad_sp` secret example in `docs/deploy.md` line 103. Run `.venv/bin/pytest tests/docs_test.py` per FR-009 (partial)
- [X] T063 Amend FR-035 in `specs/001-cursor-only-engine/spec.md` line 235 so `aad_sp` requires `client_id` and `client_secret` only. Link the T095 answer in `docs/baseline.md` per FR-009 (contradicts)
- [X] T064 Add `azurerm_private_dns_a_record.sqlserver_test` (`TF/sqlserver_test.tf` line 222, gated on both toggles) to the resource list in `contracts/terraform_toggle.md`. In `quickstart.md` step 27, point the UC connection at the private DNS name, not the IP, because serverless refuses an IPv4 literal through the gateway per FR-014 (partial)

---

## Phase 9: Convergence

- [X] T065 Update `tests/synthetic/README.md` line 9 so the `build_synth_db.py` row says Change Tracking is enabled only when `enable_change_tracking` is `true`. Run `.venv/bin/pytest tests/docs_test.py` per plan: tests/synthetic/README.md edit (partial)
