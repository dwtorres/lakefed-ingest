# Deploying lakefed_cursor

The bundle `lakefed_cursor` at the repo root owns every resource: the Lakebase instance, its UC
catalog, the secret scope, and the jobs `lakefed_cursor` and `lakefed_cursor_setup`. The file
`resources/lakefed_cursor.yml` defines the ingestion job. The file `resources/lakefed_cursor_setup.yml`
defines the Lakebase resources and the schema job. `databricks bundle deploy` is idempotent. This
page covers the steps the bundle cannot do. Run all commands from the repo root. Choose the profile
yourself with `-p <profile>`.

## 1. Add a target

`databricks.yml` has targets `dev`, `devtest` and `prod`. Add or edit a target for your workspace:

```yaml
targets:
  dev:
    mode: development
    workspace:
      host: https://<your-workspace-host>/
    variables:
      node_type_id: Standard_D8ds_v5          # ingestion workers, setup single node
      driver_node_type_id: Standard_D16ds_v5  # the ingestion job is driver-bound
      num_workers: 2
      # Optional overrides (defaults shown):
      # lakebase_instance: lakefed-cursor
      # lakebase_capacity: CU_1
      # lakebase_database: databricks_postgres
      # lakebase_catalog: lakefed_cursor_lakebase
      # secret_scope: lakefed-cursor
      # Ingestion job parameter defaults (README "Job parameters"):
      # cursor_lookback_s: "3600"
      # cursor_skew_s: "300"
      # max_seeds_per_run: "10000"
      # max_queries_per_source: "64"
      # table_retries: "2"
```

The default compute needs about 32 cores. A subscription with a small vCPU quota must use smaller
nodes or raise the quota first.

**Adopt existing resources.** Bind an existing Lakebase instance or secret scope instead of
creating a new one. Set the matching bundle variables in your target first. After the bind, the
bundle manages the resource.

```bash
databricks bundle deployment bind lakebase <instance-name>        -t dev -p <profile>
databricks bundle deployment bind lakebase_catalog <catalog-name> -t dev -p <profile>
databricks bundle deployment bind secret_scope <scope-name>       -t dev -p <profile>
```

Existing jobs bind the same way, as `lakefed_cursor` and `lakefed_cursor_setup`. Run
`databricks bundle plan -t dev -p <profile>` before the first deploy. Confirm that the plan deletes
nothing.

**Own names.** No resource name starts with `lakefed_ingest` or `edp`. The engine shares no job,
table, schema, secret scope or Lakebase instance with a `lakefed_ingest` deployment. Do not bind
one of those resources here.

**One catalog per Lakebase database.** One UC catalog can register a Lakebase database. If the
database is already registered, bind that catalog or set `lakebase_catalog` to its name. A second
catalog for the same database fails.

## 2. Deploy and apply the schema

1. Run the deploy gate. It runs the name test, then `databricks bundle validate`.

   ```bash
   scripts/validate.sh -t dev -p <profile>
   ```

2. Deploy the bundle.

   WARNING: the deploy creates a billable Lakebase instance unless you bound an existing one.

   ```bash
   databricks bundle deploy -t dev -p <profile>
   ```

3. Apply the schema. The job is idempotent.

   ```bash
   databricks bundle run lakefed_cursor_setup -t dev -p <profile>
   ```

## 3. Steps outside the bundle

1. **Network path from the cluster to Lakebase, port 5432.** Test this first. The workspace IP
   access list must allow the cluster egress, or use Private Link. A blocked connection fails with
   `External authorization failed ... blocked by Databricks IP ACL`.
2. **Postgres role.** The identity the jobs run as must be a Postgres role on the Lakebase instance.
   The deployer is one. If the jobs run as a service principal, add a role for it.
3. **Source credentials.** Store one secret per source database in scope `lakefed-cursor`. Use a
   hidden prompt. The value is JSON. See `specs/001-cursor-only-engine/contracts/secret_json.md`.

   ```bash
   databricks secrets put-secret lakefed-cursor <secret_key> -p <profile>
   ```

   | `auth_mode` | Value | Engine |
   |---|---|---|
   | `sql` or absent | `{"user": "...", "password": "..."}` | all |
   | `ntlm` | `{"auth_mode": "ntlm", "domain": "CORP", "user": "...", "password": "..."}` | SQL Server |
   | `aad_sp` | `{"auth_mode": "aad_sp", "client_id": "...", "client_secret": "...", "tenant_id": "..."}` | SQL Server |

   An Azure Key Vault-backed scope also works. AKV secret names allow only letters, digits and
   dashes. Each control row names its secret in `secret_key`.

   A row can read through an existing Unity Catalog connection instead. Set `src_connection` to the
   connection name and `use_remote_query` to `true`. The connection holds the credentials. Grant the
   job identity `USE CONNECTION` and `MANAGE` on the connection:

   ```sql
   GRANT USE CONNECTION, MANAGE ON CONNECTION <name> TO <principal>;
   ```

   `remote_query` needs Databricks Runtime 18.3 or later. The job runs `18.x`.
4. **Control rows.** Insert one row per source table into `lakefed_cursor.control`. See "Control
   rows" in `../README.md`, `specs/001-cursor-only-engine/contracts/control_row.md`, and
   `notebooks/load_control_example.ipynb`. A `cursor` row needs `primary_key`, `cursor_expression`
   and `cursor_kind`. A `full` row needs no primary key and no cursor columns. To share one sink
   across many source databases, give the rows the same sink and a distinct `client_id` each.
5. **Oracle or MySQL JDBC driver.** Databricks Runtime bundles the SQL Server and PostgreSQL
   drivers only. Uncomment the matching Maven library in `resources/lakefed_cursor.yml`, pin an
   approved version, and deploy again.
   - Oracle: `src_database` is the service name. `src_schema` is the owner. Use the stored
     upper-case names. The partitioned seed sizes tables from `ALL_TABLES`, which needs only
     `SELECT` on the table. TLS over TCPS needs a wallet and a custom URL. The adapter does not
     configure it.
   - PostgreSQL: TLS is always on. With `trust_server_certificate` `false` the mode is
     `sslmode=verify-full`. Otherwise it is `require`.
   - MySQL: `src_schema` is the database. The TLS modes are `VERIFY_IDENTITY` and `REQUIRED`.
   - MySQL supports `full` only. A `cursor` row on MySQL fails at plan time.

## 4. First run

```bash
databricks bundle run lakefed_cursor -t dev -p <profile> --params task_collection=<name>
```

The first run seeds the cursor tables and reloads the full tables. Later runs read the rows above
each watermark, or reload. Follow progress with the telemetry queries in [monitoring.md](monitoring.md).

**Upgrade an existing deployment.** Pull and deploy. The next run finds the database behind its
schema version and applies the DDL first, in one transaction. A failure changes nothing and fails
the run with the cause. To migrate before the next run, run `lakefed_cursor_setup`. The migration
needs the schema owner. The deployer is the owner, unless the jobs run as another identity. No data
migrates from a `lakefed_ingest` deployment.

**Schedule.** The ingestion job ships with a paused 15-minute schedule. A schedule cannot pass
parameters, so it ingests only the default `task_collection`. For per-collection cadence, deploy a
job per collection, or trigger runs per collection with `run-now` and `job_parameters`.

## 5. Live proof

The SQL Server live proof is on hold, because no SQL Server source is available. The Oracle live
proof is pending the Phase 10 run of `specs/001-cursor-only-engine/quickstart.md`. Record the
results in `tasks/todo.md`.

## Schema changes

Writes use schema evolution: `MERGE WITH SCHEMA EVOLUTION` and
`INSERT WITH SCHEMA EVOLUTION OVERWRITE ... BY NAME`. A column added to a control row
`select_list` is added to the sink. The job then reseeds that table, so old rows get the column.
New sinks get type widening. For an older sink run:

```sql
ALTER TABLE <sink> SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
```
