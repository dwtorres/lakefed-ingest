---
ontology: true
type: spec
domain: lakefed-cursor
summary: Contract for the sqlserver_test toggle in the ding-dong-ditch Terraform project, inputs, outputs, resources, teardown check
tags: [contract, terraform, azure, sqlserver, toggle, teardown]
related: [../plan, ../research, ../data-model, ../quickstart]
status: draft
discovered: 2026-10-31
verified: false
workspace: lakefed-cursor
---

# Contract: Terraform toggle `enable_sqlserver_test_source`

Project: `/Users/dwtorres/src/ding-dong-ditch-azure-terraform`. File: `sqlserver_test.tf`. Pattern:
`png.tf` with `local.png_count`.

## Inputs (`variables.tf`)

| Variable | Type | Default | Rule |
|---|---|---|---|
| `enable_sqlserver_test_source` | bool | `false` | needs `create_new_vnet = true` |
| `sqlserver_test_cidr` | string | `"10.0.17.0/24"` | no overlap with `cidr` or `png_test_cidr` |
| `sqlserver_test_resource_group_name` | string | `"rg-lakefed-sqlserver-test"` | |
| `sqlserver_test_budget_usd` | number | `50` | greater than 0 |
| `sqlserver_test_budget_start` | string | `"2026-10-01T00:00:00Z"` | first day of a month |

## Resources (all `count = local.sqlserver_test_count`)

1. `azurerm_resource_group.sqlserver_test`
2. `azurerm_subnet.sqlserver_test` in `local.vnet`, with `azurerm_subnet_nat_gateway_association`
3. `azurerm_network_security_group.sqlserver_test`: allow TCP 1433 from `var.subnet_public_cidr`
   and `var.subnet_private_cidr`. With the gateway on, also from `local.png_gateway_cidr`. Deny the rest
4. `azurerm_subnet_network_security_group_association.sqlserver_test`
5. `random_password.sqlserver_test_admin`, `random_password.sqlserver_test_sql`,
   `random_password.sqlserver_test_ntlm`
6. `azurerm_network_interface.sqlserver_test` (private IP, no public IP)
7. `azurerm_windows_virtual_machine.sqlserver_test`: name `lakefedsqltest`, `Standard_D2ds_v4`,
   image `MicrosoftSQLServer` / `sql2022-ws2022` / `sqldev-gen2`, `identity { type = "SystemAssigned" }`
8. `azurerm_mssql_virtual_machine.sqlserver_test`: `sql_license_type = "PAYG"`,
   `sql_connectivity_type = "PRIVATE"`, port 1433, `sql_connectivity_update_username = "sqladmin"`
9. `azurerm_virtual_machine_extension.sqlserver_test_setup` (CustomScriptExtension): creates the
   Windows user, the database `lakefed_test`, the two logins, the `db_owner` grants
10. `azurerm_consumption_budget_resource_group.sqlserver_test`: 80 percent notification

With the toggle on, `address_space` in `network.tf` adds `var.sqlserver_test_cidr`.

## Outputs (`outputs.tf`)

| Output | Sensitive | Value |
|---|---|---|
| `sqlserver_test_host` | no | private IP of the NIC |
| `sqlserver_test_database` | no | `lakefed_test` |
| `sqlserver_test_ntlm_domain` | no | `lakefedsqltest` |
| `sqlserver_test_sql_secret_json` | yes | `{"user": "lakefed_sql", "password": "..."}` |
| `sqlserver_test_ntlm_secret_json` | yes | `{"auth_mode": "ntlm", "domain": ..., "user": "lakefed_ntlm", "password": "..."}` |
| `sqlserver_test_admin_password` | yes | sysadmin password |

With the toggle off, every output is `null`.

## Test (`tests/plan.tftest.hcl`)

One `run` block with `enable_sqlserver_test_source = true` and `create_new_vnet = true`. Assert
the VM name is `lakefedsqltest` and the NSG has no rule with `source_address_prefix = "*"` on
port 1433.

## Teardown check

```bash
terraform apply -var enable_sqlserver_test_source=false
az ad app delete --id "$(az ad app list --display-name sp-lakefed-sqlserver-test --query '[0].appId' -o tsv)"
az resource list --tag Feature=002-azure-sqlserver-test-source -o tsv   # expect no line
az group exists -n rg-lakefed-sqlserver-test                             # expect false
az network vnet subnet list -g rg-databricks-network --vnet-name ding-dong-ditch-vnet --query "[?contains(name,'sqlserver-test')]" -o tsv   # expect no line
```
