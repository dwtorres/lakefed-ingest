---
ontology: true
type: spec
domain: lakefed-cursor
summary: Entities of the Azure SQL Server test source, their fields, and their states from build to teardown
tags: [data-model, azure, sqlserver, terraform, secret, control-row, baseline]
related: [spec, plan, research, contracts/terraform_toggle, ../001-cursor-only-engine/contracts/control_row, ../001-cursor-only-engine/contracts/secret_json]
status: draft
discovered: 2026-10-31
verified: false
workspace: lakefed-cursor
---

# Data Model: Azure SQL Server Test Source

## Test source (Azure)

| Field | Value | Where |
|---|---|---|
| Resource group | `rg-lakefed-sqlserver-test` | `var.sqlserver_test_resource_group_name` |
| VM name | `lakefedsqltest` (15 characters max, it is also the NTLM domain) | `sqlserver_test.tf` |
| Image | `MicrosoftSQLServer` / `sql2022-ws2022` / `sqldev-gen2` / `latest` | `sqlserver_test.tf` |
| Size | `Standard_D2ds_v4` | `sqlserver_test.tf` |
| Subnet | `ding-dong-ditch-sqlserver-test-subnet`, `10.0.17.0/24`, in `rg-databricks-network` | `var.sqlserver_test_cidr` |
| Address | private IP only, output `sqlserver_test_host` | `outputs.tf` |
| Port | 1433 | `azurerm_mssql_virtual_machine.sql_connectivity_port` |
| Allowed clients | `10.0.8.0/21`, `10.0.0.0/21`. With the private network gateway on, also the gateway subnet | NSG `lakefedsqltest-nsg` |
| Identity | system-assigned managed identity, Directory Readers (runbook) | `identity { type = "SystemAssigned" }` |
| Database | `lakefed_test` | Custom Script Extension |
| Tags | `Feature = 002-azure-sqlserver-test-source` plus `var.tags` | every resource |

Validation: the VM name is 15 characters or fewer. The CIDR does not overlap `var.cidr` or
`var.png_test_cidr`. The toggle needs `create_new_vnet = true` (same rule as the gateway toggle).

## Logins

| Login | Type | Created by | Role |
|---|---|---|---|
| `sqladmin` | SQL, sysadmin | `azurerm_mssql_virtual_machine` | operator only |
| `lakefed_sql` | SQL | Custom Script Extension | `db_owner` on `lakefed_test` |
| `lakefedsqltest\lakefed_ntlm` | Windows local user | Custom Script Extension | `db_owner` on `lakefed_test` |
| `sp-lakefed-sqlserver-test` | Entra service principal | runbook, `CREATE LOGIN ... FROM EXTERNAL PROVIDER` | `db_owner` on `lakefed_test` |

Passwords are `random_password` results, 24 characters, outputs marked `sensitive`. The service
principal secret comes from `az ad sp create-for-rbac` and is never stored in Terraform.

## Synthetic table

| Field | Value |
|---|---|
| Name | `dbo.doc_attachment_079` |
| Catalog | `synth_catalog.build_catalog(42)`, base rows 1,005,761, 16 columns |
| Scale factor | 0.01, so 10,058 rows |
| Primary key | `id` |
| Cursor column | `row_version rowversion`, added after the load |
| Change Tracking | off (`enable_change_tracking = false`) |

## Secrets (scope `lakefed-cursor`)

| Key | Shape |
|---|---|
| `sqlserver_test_sql` | `{"user": "lakefed_sql", "password": "..."}` |
| `sqlserver_test_ntlm` | `{"auth_mode": "ntlm", "domain": "lakefedsqltest", "user": "lakefed_ntlm", "password": "..."}` |
| `sqlserver_test_aad_sp` | `{"auth_mode": "aad_sp", "client_id": "...", "client_secret": "...", "tenant_id": "..."}`. The FR-009 run stores it again without `tenant_id` |

Rules per [contracts/secret_json.md](../001-cursor-only-engine/contracts/secret_json.md). The
secrets are deleted at teardown.

## Control row

One row in `lakefed_cursor.control`, `task_collection = 'devtest'`:

| Column | Value |
|---|---|
| `src_type` | `sqlserver` |
| `src_host`, `src_port` | output `sqlserver_test_host`, 1433 |
| `secret_key` | `sqlserver_test_sql`, then `sqlserver_test_ntlm`, then `sqlserver_test_aad_sp` |
| `src_database`, `src_schema`, `src_table` | `lakefed_test`, `dbo`, `doc_attachment_079` |
| `primary_key`, `select_list` | `id`, `id,c000,c001,c002,c003,c004,c005,c006,c007,c008,c009,c010,c011,c012,c013,c014,row_version` |
| `sink_catalog`, `sink_schema`, `sink_table` | `my_catalog`, `lakefed_cursor_devtest`, `sqlserver_doc_attachment_079` |
| `load_type`, `cursor_expression`, `cursor_kind` | `cursor`, `row_version`, `rowversion` |
| `delete_mode`, `delete_min_interval_s` | `pk_snapshot`, `0` |

Job parameter `trust_server_certificate = true` (self-signed certificate on the VM).

## Baseline record (`docs/baseline.md`)

Three sections: "SQL Server (T012a)" for Lakeflow Connect, "Devtest run (T093)" for the cursor
engine, "Auth modes (T095)". Each engine section holds the four FR-011 values: row counts, run
duration, lag, cost per run. The T095 section holds one line per mode and the `tenant_id` answer,
`needed` or `not needed`.

## Cost ceiling

`azurerm_consumption_budget_resource_group`: `amount = var.sqlserver_test_budget_usd` (default 50),
`time_grain = Monthly`, `start_date = var.sqlserver_test_budget_start`, one notification,
`threshold = 80`, `GreaterThanOrEqualTo`, `Actual`, `contact_emails = [var.admin_user]`.

## States

```text
absent --(toggle true, apply)--> built --(runbook: Entra, SP, logins)--> ready
ready --(build_synth_db, ALTER TABLE)--> loaded --(seed)--> seeded
seeded --(change set, increment, reconcile, no-change run)--> proven (T093)
proven --(ntlm run, aad_sp run, aad_sp run without tenant_id)--> modes proven (T095)
modes proven --(pipeline seed, change, record, delete)--> baselined (T012a)
baselined --(toggle false, apply; az ad app delete; secrets delete)--> absent (SC-007)
```

Any state can go to `absent` through the teardown step. A failed build repeats `apply`.
