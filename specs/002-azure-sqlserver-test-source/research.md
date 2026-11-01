---
ontology: true
type: research
domain: lakefed-cursor
summary: Decisions for the disposable Azure SQL Server test source, built with the ding-dong-ditch Terraform project
tags: [research, azure, sqlserver, terraform, entra-id, ntlm, lakeflow-connect, budget]
related: [spec, plan, data-model, quickstart, ../../docs/baseline, ../001-cursor-only-engine/contracts/secret_json]
status: draft
discovered: 2026-10-31
verified: false
workspace: lakefed-cursor
---

# Research: Azure SQL Server Test Source

Sources read on 2026-10-31:

- The Terraform project at `/Users/dwtorres/src/ding-dong-ditch-azure-terraform`: `network.tf`, `png.tf`, `variables.tf`, `outputs.tf`, `terraform.tfvars`.
- The azurerm provider docs for `azurerm_mssql_virtual_machine` and `azurerm_consumption_budget_resource_group` (Context7).
- Microsoft Learn: "Enable Microsoft Entra authentication for SQL Server on Azure VMs" and "Connect using Microsoft Entra authentication" (JDBC).
- `tests/synthetic` in this repo.

## R-01 Where the Terraform lives

- Decision: add one file, `sqlserver_test.tf`, to the ding-dong-ditch Terraform project. Gate every
  resource with `var.enable_sqlserver_test_source` (default `false`), the same pattern as `png.tf`
  and `enable_private_network_gateway`.
- Rationale: that project already owns the devtest workspace, its VNet (`10.0.0.0/20`), the NAT
  gateway, the state backend, and a test-resource toggle. Teardown is `enable_sqlserver_test_source =
  false` and `terraform apply`. No second state, no second backend.
- Alternatives: a separate Terraform root in this repo. Rejected: it needs the VNet id and subnet
  CIDRs as inputs and a second state file. A subnet in a VNet that another state owns is fragile.

## R-02 SQL Server platform

- Decision: SQL Server 2022 Developer on Windows Server 2022, Azure Marketplace image
  `MicrosoftSQLServer` / `sql2022-ws2022` / `sqldev-gen2`, size `Standard_D2ds_v4`, registered with
  the SQL IaaS Agent extension through `azurerm_mssql_virtual_machine` (`sql_connectivity_type =
  "PRIVATE"`, port 1433, `sql_license_type = "PAYG"`).
- Size: the first apply on 2026-10-31 used `Standard_D2s_v5` and failed. The subscription has 0
  DSv5 cores in eastus and 10 DDSv4 cores, so the VM uses `Standard_D2ds_v4`.
- Rationale: FR-001 needs a Windows (NTLM) login. Azure SQL Database has no Windows login. A SQL
  Server 2022 VM accepts all three: SQL login, local Windows login over NTLM, and an Entra service
  principal login. Developer edition has no license cost.
- Alternatives: Azure SQL Database for `sql` and `aad_sp` plus a VM for `ntlm`. Rejected: two
  sources, two secrets per mode, double the teardown.

## R-03 Entra authentication on the VM

- Decision: the VM gets a system-assigned managed identity in Terraform. After `terraform apply`,
  the runbook grants that identity the Entra **Directory Readers** role and enables Entra
  authentication on the registered instance with Azure PowerShell:
  `Update-AzSqlVM -ResourceGroupName <rg> -Name <vm> -IdentityType 'SystemAssigned'`. The portal
  page "SQL virtual machines > Security Configuration" is the fallback.
- Rationale: `azurerm_mssql_virtual_machine` has no Entra authentication argument (azurerm 4.x docs).
  Microsoft Learn documents the PowerShell and portal paths. Entra authentication cannot be
  disabled once enabled, but the VM is deleted at teardown, so this does not matter.
- Facts: the identity needs Directory Readers, or the three Graph app roles `User.Read.All`,
  `GroupMember.Read.All`, `Application.Read.All`. Granting Directory Readers needs Privileged Role
  Administrator in the tenant. The service principal login is created in SQL with
  `CREATE LOGIN [<app display name>] FROM EXTERNAL PROVIDER` after Entra authentication is on.
- Alternatives: the `azapi` provider to call the SqlVirtualMachine REST API. Rejected: a new
  provider in the project for one setting on a disposable VM.

## R-04 The service principal for `aad_sp`

- Decision: `az ad sp create-for-rbac --name sp-lakefed-sqlserver-test --skip-assignment` in the
  runbook. It returns `appId`, `password`, `tenant`. Teardown deletes the app with
  `az ad app delete --id <appId>`.
- Rationale: the Terraform project has no `azuread` provider. One CLI call is smaller than adding
  one. The service principal is an Entra object, not a resource-group member, so FR-002 teardown
  needs this explicit step and SC-007 checks it.

## R-05 JDBC facts for the three modes

- `sql`: `user`, `password`. Unchanged.
- `ntlm`: `authenticationScheme=NTLM`, `integratedSecurity=true`, `domain=<VM computer name>`,
  `user`, `password`. Works from a Linux cluster (driver 7.4 or later). The VM name is at most 15
  characters: `lakefedsqltest`.
- `aad_sp`: `authentication=ActiveDirectoryServicePrincipal`, `user=<client_id>`,
  `password=<client_secret>` (driver 10.2 or later). The driver takes no tenant property for this
  mode. The expected answer to FR-009 is "not needed". The live run tests this. If "not needed",
  T095 deletes `tenant_id` from `AUTH_FIELDS` in `source_adapters.py` and from
  `contracts/secret_json.md`.
- Both `ntlm` and `aad_sp` force TLS. The VM has a self-signed certificate, so the job runs with
  `trust_server_certificate=true`.

## R-06 Network path

- Decision: the VM has a private IP only, in a new subnet `10.0.17.0/24` of the devtest VNet
  (`var.sqlserver_test_cidr`, added to `address_space` like `png_test_cidr`). An NSG on that subnet
  allows TCP 1433 from the Databricks subnets, `10.0.8.0/21` and `10.0.0.0/21`. With the private
  network gateway on, it also allows the gateway subnet. Every other inbound rule is denied.
- Rationale: FR-003 (only the devtest workspace connects) and US1 scenario 3. The job cluster is
  classic compute in the VNet, so it reaches the private IP directly.
- Live finding 2026-10-31: serverless reaches the VM through the gateway by host name only. An
  IPv4 literal is refused (NAT64). `sqlserver_test.tf` adds the A record `sqlserver` in the gateway
  private DNS zone when both toggles are on. The UC connection uses that name.
- Lakeflow Connect (US4): the pipeline runs on serverless compute, which has no address in the VNet.
  The project's private network gateway (`enable_private_network_gateway = true`, `ALL_TRAFFIC`)
  sends serverless traffic into the VNet. US4 needs it on. Azure serverless has no stable public IP
  to allowlist, so a public IP on the VM does not help.
- Alternatives: public IP with an NSG allowlist of `nat_gateway_public_ip`. Rejected: serverless
  cannot be allowlisted by IP, and a public SQL endpoint is a wider surface than the test needs.

## R-07 Resource group and teardown

- Decision: VM, NIC, OS disk, NSG, and budget live in a new resource group
  `rg-lakefed-sqlserver-test` (`var.sqlserver_test_resource_group_name`). The subnet is a child of
  the VNet, so it lives in `rg-databricks-network`. Every resource carries the tag
  `Feature = "002-azure-sqlserver-test-source"`.
- Teardown: set `enable_sqlserver_test_source = false`, `terraform apply`, delete the service
  principal app, then `az resource list --tag Feature=002-azure-sqlserver-test-source` returns `[]`
  and `az group exists -n rg-lakefed-sqlserver-test` returns `false`.
- Deviation from FR-002: the subnet is outside the feature resource group. The toggle removes it.
  The SC-007 check uses the tag and the subnet list, not the group alone.

## R-08 Cost ceiling

- Decision: `azurerm_consumption_budget_resource_group` on the feature group, `amount =
  var.sqlserver_test_budget_usd` (default 50), `time_grain = "Monthly"`, one notification at
  `threshold = 80`, `operator = "GreaterThanOrEqualTo"`, `threshold_type = "Actual"`,
  `contact_emails = [var.admin_user]`. `start_date` is a variable, first of the current month.
- Rationale: FR-013. A budget is created with the first resource because it is in the same apply.
- Estimate: `Standard_D2ds_v4` plus a 128 GB Premium SSD is about 3 USD per day in eastus. Two weeks
  is under 50 USD.

## R-09 Logins and database on the VM

- Decision: a Custom Script Extension (PowerShell) runs once at create. It creates the local Windows
  user `lakefed_ntlm`, the database `lakefed_test`, the SQL login `lakefed_sql`, the Windows login
  `[lakefedsqltest\lakefed_ntlm]`, and grants `db_owner` on `lakefed_test` to both. Passwords come
  from `random_password` resources and are Terraform outputs marked `sensitive`. The Entra login is
  created by a runbook step, because Entra authentication is enabled after apply (R-03).
- Rationale: `db_owner` is what `build_synth_db.py` needs (CREATE TABLE). The sysadmin login from
  `sql_connectivity_update_username` is for the operator only.

## R-10 The synthetic table and the cursor

- Decision: table `doc_attachment_079` from `synth_catalog.build_catalog(42)`: base rows 1,005,761,
  primary key `id`, 16 columns. At `scale_factor` 0.01 it holds 10,058 rows (FR-004: at least
  10,000). After the load, `ALTER TABLE dbo.doc_attachment_079 ADD row_version rowversion`. The
  control row uses `cursor_expression = 'row_version'`, `cursor_kind = 'rowversion'`.
- Rationale: `rowversion` moves on every update, so the change set from `synth_changes` is seen by
  the cursor without any knowledge of which column changed. A `datetime2` cursor needs an update
  expression that touches that column.
- Code changes (Principle I starting points): `build_synth_db.py` gains widgets `tables` (comma
  list, empty = every table) and `enable_change_tracking` (default `true`). With `false`, the CT
  statements do not run (FR-005). `change_generator.py` gains the same `tables` widget, so the 10/5/5 change set
  targets this one table. `synth_catalog`, `synth_sql`, `synth_changes` are not changed.

## R-11 Lakeflow Connect baseline method

- Decision: repeat the Oracle method in `docs/baseline.md`. Create a UC connection of type `SQLSERVER`
  to the private IP. Create a query-based ingestion pipeline (serverless, triggered) with one table
  object. Run it once for the seed and once after the change set. Then `pipelines delete`. Cost comes from
  `system.billing.usage` filtered on `usage_metadata.dlt_pipeline_id` the next day.
- Rationale: fixed decision call 13. The Oracle run showed billing lags by hours, so the cost line
  is filled on the day after.

## R-12 Secrets

- Decision: three secrets in scope `lakefed-cursor`: `sqlserver_test_sql`, `sqlserver_test_ntlm`,
  `sqlserver_test_aad_sp`, each bare JSON per `contracts/secret_json.md`. Stored with
  `databricks secrets put-secret --profile ding-dong-ditch` reading the value from
  `terraform output -raw` through a pipe, never typed into a chat or a notebook.
- The control row's `secret_key` is switched between the three names for US3. One control row,
  three runs.
