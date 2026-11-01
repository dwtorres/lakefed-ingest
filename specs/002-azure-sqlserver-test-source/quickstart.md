---
ontology: true
type: doc
domain: lakefed-cursor
summary: Runbook, build the Azure SQL Server test source, prove the cursor engine and the three auth modes, record the baseline, tear down
tags: [quickstart, runbook, azure, sqlserver, terraform, devtest, teardown]
related: [plan, research, data-model, contracts/terraform_toggle, ../001-cursor-only-engine/quickstart, ../../docs/baseline]
status: draft
discovered: 2026-10-31
verified: false
workspace: lakefed-cursor
---

# Quickstart: Azure SQL Server Test Source

This is the FR-014 runbook. A second operator can run it alone. Every command runs from the
directory named in its step. Replace `<...>` values. Do not paste a password into a chat.

## Prerequisites

- Azure CLI 2.48 or later, signed in: `az login`, `az account set --subscription 9d3145a6-2bd1-4cf7-9de4-f08f8c8a91b3`.
- Azure PowerShell with `Az.SqlVirtualMachine`, signed in with the same account.
- Privileged Role Administrator in the tenant, to grant Directory Readers (step 4).
- Terraform 1.9 or later. The ding-dong-ditch project at `/Users/dwtorres/src/ding-dong-ditch-azure-terraform`, with `terraform init` done.
- Databricks CLI 1.23, profile `ding-dong-ditch`. Never auto-select a profile.
- Feature 001 deployed to `devtest` and the Lakebase schema created (feature 001 quickstart, steps 1 and 2).
- A cost ceiling. The default is 50 USD. If you want another, set `sqlserver_test_budget_usd`.

## Build (US1)

1. In the Terraform project, open `terraform.tfvars`. Set `enable_sqlserver_test_source = true`.
2. Run `terraform plan`. Expected: resources to add, none to destroy.
3. Run `terraform apply`. Expected: done in under 30 minutes. Note `terraform output sqlserver_test_host`.
4. Grant the VM identity the Directory Readers role. In the portal, open "Microsoft Entra ID >
   Roles and administrators > Directory Readers > Add assignments". Select member `lakefedsqltest`.
5. Enable Entra authentication on the SQL instance:

   ```powershell
   Update-AzSqlVM -ResourceGroupName rg-lakefed-sqlserver-test -Name lakefedsqltest -IdentityType 'SystemAssigned'
   ```

6. Create the service principal. Keep the output in your terminal only:

   ```bash
   az ad sp create-for-rbac --name sp-lakefed-sqlserver-test --skip-assignment
   ```

7. Create the Entra login in SQL. Connect as `sqladmin` from a devtest notebook (the host is
   private). Run:

   ```sql
   CREATE LOGIN [sp-lakefed-sqlserver-test] FROM EXTERNAL PROVIDER
   USE lakefed_test
   CREATE USER [sp-lakefed-sqlserver-test] FROM EXTERNAL PROVIDER
   ALTER ROLE db_owner ADD MEMBER [sp-lakefed-sqlserver-test]
   ```

8. Check US1/AC1: from a devtest job cluster, a JDBC read with the `lakefed_sql` login returns one row.
9. Check US1/AC3: from your laptop, `nc -z -w 3 <sqlserver_test_host> 1433`. Expected: the connection fails.
10. Check US1/AC5: `az consumption budget list -g rg-lakefed-sqlserver-test` shows one budget with a notification at 80.

## Load the table (US1/AC2)

11. In the devtest workspace, import `tests/synthetic` (keep the `.py` modules next to the notebooks).
12. Store the SQL secret from the Terraform output:

    ```bash
    terraform output -raw sqlserver_test_sql_secret_json | databricks secrets put-secret lakefed-cursor sqlserver_test_sql --profile ding-dong-ditch
    ```

13. Run `build_synth_db.py` with widgets `host=<sqlserver_test_host>`, `database=lakefed_test`,
    `secret_scope=lakefed-cursor`, `secret_key=sqlserver_test_sql`, `trust_server_certificate=true`,
    `scale_factor=0.01`, `tables=doc_attachment_079`, `enable_change_tracking=false`.
14. Add the cursor column. From a notebook, as `lakefed_sql`:

    ```sql
    ALTER TABLE dbo.doc_attachment_079 ADD row_version rowversion;
    ```

15. Check: `SELECT COUNT(*) FROM dbo.doc_attachment_079` returns 10058. `sys.change_tracking_databases` has no row.

## Prove the cursor engine (US2, T093)

16. Insert the control row from [data-model.md](data-model.md) with `secret_key = 'sqlserver_test_sql'`.
17. Run feature 001 quickstart steps 5 to 9 with `--params task_collection=devtest,trust_server_certificate=true`.
    For step 6, run `change_generator.py` with the same connection widgets, `tables=doc_attachment_079`,
    `inserts=10`, `updates=5`, `deletes=5`, `manifest_table=my_catalog.lakefed_cursor_devtest.synth_manifest`.
    For step 8, run `reconcile.py` with the same widgets.
18. Expected: seed 10,058 rows and phase `incremental`; increment MERGE 10 inserted, 5 updated,
    snapshot DELETE 5; reconcile zero differences; no-change run adds no MERGE version.
19. Record date, row counts, `capture` event count, run durations, and lag in `docs/baseline.md`,
    section "Devtest run (T093)".

## Prove the auth modes (US3, T095)

20. Store the NTLM secret:

    ```bash
    terraform output -raw sqlserver_test_ntlm_secret_json | databricks secrets put-secret lakefed-cursor sqlserver_test_ntlm --profile ding-dong-ditch
    ```

21. Set the control row `secret_key = 'sqlserver_test_ntlm'`. Run the increment. Expected: `incremented 1`.
22. Store the Entra secret. Type the JSON into the hidden prompt, with the `appId`, `password`, and
    `tenant` from step 6 as `client_id`, `client_secret`, `tenant_id`:

    ```bash
    databricks secrets put-secret lakefed-cursor sqlserver_test_aad_sp --profile ding-dong-ditch
    ```

23. Set `secret_key = 'sqlserver_test_aad_sp'`. Run the increment. Expected: `incremented 1`.
24. Store the secret again without `tenant_id`. Run the increment. If it connects, record `not needed`.
    If it fails, record `needed`. Write the answer in `docs/baseline.md`, section "Auth modes (T095)".
25. Check US3/AC4: `rg -c 'password|client_secret' <run log>` finds no secret value.

## Record the Lakeflow Connect baseline (US4, T012a)

26. In the Terraform project, set `enable_private_network_gateway = true` and `terraform apply`.
    Follow `docs/private-network-gateway-runbook.md` there to attach the devtest workspace.
27. Create a UC connection of type `SQLSERVER` to `<sqlserver_test_host>:1433`, database
    `lakefed_test`, login `lakefed_sql`, trust server certificate on.
28. Create a query-based ingestion pipeline, serverless, triggered, one table object
    `dbo.doc_attachment_079`, cursor `row_version`, target `my_catalog.lakefed_cursor_devtest.sqlserver_baseline_doc_attachment_079`.
    The Oracle section of `docs/baseline.md` shows the spec shape.
29. Run it. Expected: 10,058 rows. Note the wall-clock duration and the lag to the data commit.
30. Run `change_generator.py` once more (step 17 widgets). Run the pipeline. Note the lag.
31. Delete the pipeline. Expected: `databricks pipelines get <id>` returns not found.
32. The next day, read `system.billing.usage` for the pipeline id. Record cost per run next to the
    cursor engine cost in `docs/baseline.md`, section "SQL Server (T012a)".

## Close feature 001 (SC-005)

33. In `specs/001-cursor-only-engine/tasks.md`, mark T012a, T093, T095 `[X]` with a link to the
    `docs/baseline.md` section.
34. Apply the `tenant_id` answer. If `not needed`, delete it from `AUTH_FIELDS` in
    `src/lakefed_ingest/source_adapters.py` and from `contracts/secret_json.md`. Write the test first.

## Tear down (US1/AC4, SC-007)

WARNING: this deletes the VM, its disk, the database, and the table. Finish steps 19, 24, and 32
first.

35. In the Terraform project, set `enable_sqlserver_test_source = false`. If you turned the gateway
    on in step 26 only for this test, set `enable_private_network_gateway = false` too.
36. Run `terraform apply`. Expected: the feature resources are destroyed, nothing else changes.
37. Delete the service principal:

    ```bash
    az ad app delete --id "$(az ad app list --display-name sp-lakefed-sqlserver-test --query '[0].appId' -o tsv)"
    ```

38. Delete the three secrets:

    ```bash
    for k in sqlserver_test_sql sqlserver_test_ntlm sqlserver_test_aad_sp; do databricks secrets delete-secret lakefed-cursor $k --profile ding-dong-ditch; done
    ```

39. Delete the control row and the two sink tables in `my_catalog.lakefed_cursor_devtest`.
40. Run the teardown check in [contracts/terraform_toggle.md](contracts/terraform_toggle.md).
    Expected: no resource, group `false`, no subnet.

## Acceptance

SC-001: steps 1 to 15 in under 2 hours. SC-002: step 18 zero differences. SC-003: steps 21, 23, 24
connect and the answer is recorded. SC-004: `docs/baseline.md` holds cost and lag for both engines.
SC-005: step 33. SC-006: the budget never alerts. SC-007: step 40.
