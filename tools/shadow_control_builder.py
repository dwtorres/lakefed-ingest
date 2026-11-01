# Databricks notebook source
# MAGIC %pip install "psycopg[binary]" databricks-sdk==0.145.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Build lakefed-cursor shadow control rows and source secrets from one edp
# source_to_bronze_<source>.json file. Run on the edp devtest workspace.
# Reads secrets from scope edp-akv, writes JSON secrets to scope
# lakefed-cursor, and replaces the control rows of one task_collection.
# Never prints a secret value.

import json
import re

ORACLE_DEFAULT_OWNER = "V500"
CURSOR_LOOKBACK_S = 600  # edp subtracts 10 minutes from the last watermark
BRONZE_METADATA_COLUMNS = {
    "_run_id", "_updated_datetime", "_file_path", "_file_name",
    "_file_size", "_file_modification_time",
}
SRC_TYPES = {
    "oracle": "oracle",
    "oracle_peoplesoft": "oracle",
    "mssql": "sqlserver",
    "fabric_cogito_change_tracking": "sqlserver",
    "fabric_direct_watermark": "sqlserver",
}
ORACLE_URL = re.compile(
    r"^jdbc:oracle:thin:@(?://)?(?P<host>[^:/]+):(?P<port>\d+)[/:](?P<service>[^?\s]+)$"
)


def parse_oracle_url(url: str) -> tuple:
    """Split a thin URL into host, port and service. A TNS descriptor fails."""
    match = ORACLE_URL.match(url.strip())
    if not match:
        raise ValueError(
            "oracle key_url is not jdbc:oracle:thin:@//host:port/service; "
            "set src_host, src_port and src_database by hand"
        )
    return match["host"], int(match["port"]), match["service"]


def split_ntlm_user(user: str, domain: str) -> tuple:
    """Return (domain, user). A DOMAIN\\user login carries its own domain."""
    if "\\" in user:
        login_domain, login_user = user.split("\\", 1)
        return login_domain, login_user
    if not domain:
        raise ValueError(
            "windows_auth source: login has no DOMAIN\\ prefix; "
            "set the ntlm_domain widget"
        )
    return domain, user


def key_columns(dataset: dict) -> str:
    keys = dataset.get("composite_primary_key") or dataset.get("primary_key")
    if isinstance(keys, list):
        return ", ".join(keys)
    return (keys or "").strip()


def cursor_expression(columns: list) -> str:
    """A NULL-tolerant GREATEST over 1 to 3 columns, without literals.

    COALESCE in each position replaces a NULL input with another
    column, so GREATEST returns NULL only when every input is NULL.
    """
    if len(columns) == 1:
        return columns[0]
    rotations = [columns[i:] + columns[:i] for i in range(len(columns))]
    return "GREATEST(" + ", ".join(
        "COALESCE(" + ", ".join(r) + ")" for r in rotations
    ) + ")"


def plan_dataset(name: str, dataset: dict, source_type: str) -> tuple:
    """Return (load_type, cursor_columns) or (None, reason) to skip."""
    if not dataset.get("enabled"):
        return None, "disabled in edp"
    mode = dataset.get("ingestion_mode")
    if mode == "generated_date_dimension":
        return None, "generated in Databricks, no source table"
    if source_type == "fabric_cogito_change_tracking":
        # Clarity waits for the change_keys read strategy (separate feature).
        return None, "Clarity: waits for the change_keys feature"
    if source_type == "fabric_direct_watermark":
        if mode == "incremental_watermark" and dataset.get("watermark_column"):
            return "cursor", [dataset["watermark_column"]]
        return "full", []
    if not dataset.get("incremental"):
        return "full", []
    columns = []
    for key in ("modified_column", "created_column", "additional_column_ts"):
        column = (dataset.get(key) or "").strip()
        if column and column not in columns:
            columns.append(column)
    if not columns or not key_columns(dataset):
        return "full", []
    return "cursor", columns[:3]


def build_rows(source_name: str, body: dict, connection: dict,
               columns_by_dataset: dict, collection: str,
               sink_catalog: str) -> tuple:
    """Map one edp source to lakefed control rows. Pure: no Spark, no IO."""
    config = body["source_config"]
    source_type = config["source_type"]
    src_type = SRC_TYPES[source_type]
    if src_type == "oracle":
        schema = config.get("schema") or ORACLE_DEFAULT_OWNER
    else:
        schema = config.get("schema") or "dbo"
    rows, skipped = [], {}
    for name, dataset in body["datasets"].items():
        load_type, detail = plan_dataset(name, dataset, source_type)
        if load_type is None:
            skipped[name] = detail
            continue
        columns = columns_by_dataset.get(name)
        if not columns:
            skipped[name] = "no edp bronze table to copy the column list from"
            continue
        if src_type == "oracle":
            table = name.upper()
        else:
            table = dataset.get("managed_table") or name
        row = {
            "task_collection": collection,
            "src_type": src_type,
            "src_host": connection["host"],
            "src_port": connection["port"],
            "secret_key": connection["secret_key"],
            "src_database": connection["database"],
            "src_schema": schema,
            "src_table": table,
            "sink_catalog": sink_catalog,
            "sink_schema": source_name,
            "sink_table": name.lower(),
            "primary_key": key_columns(dataset) or None,
            "select_list": ", ".join(columns),
            "load_type": load_type,
            "cursor_expression": None,
            "cursor_kind": None,
            "cursor_lookback_s": None,
            "null_cursor_policy": "reject",
        }
        if load_type == "cursor":
            row.update(
                cursor_expression=cursor_expression(detail),
                cursor_kind="app_timestamp",
                cursor_lookback_s=CURSOR_LOOKBACK_S,
                # edp also never reads a NULL-watermark row incrementally.
                null_cursor_policy="seed_only",
            )
        rows.append(row)
    return rows, skipped


# COMMAND ----------

def _run_notebook():
    import sys
    from databricks.sdk import WorkspaceClient

    def widget(name, default=""):
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name).strip()

    # The deployed bundle folder that holds state_store.py.
    sys.path.append(widget("engine_path"))
    from state_store import StateStore

    metadata_file = widget("metadata_file")  # /Workspace/.../source_to_bronze_<source>.json
    source_name = widget("source_name")      # e.g. rl_solutions
    collection = widget("task_collection")   # e.g. rl_solutions_shadow
    sink_catalog = widget("sink_catalog", "devtest_lakefed_shadow")
    bronze_catalog = widget("bronze_catalog", "devtest_edp_bronze")
    ntlm_domain = widget("ntlm_domain")
    db_instance = widget("db_instance", "lakefed-cursor")
    db_name = widget("db_name", "databricks_postgres")
    apply = widget("apply", "false") == "true"

    with open(metadata_file) as handle:
        body = json.load(handle)[source_name]
    config = body["source_config"]
    akv = lambda key: dbutils.secrets.get(scope="edp-akv", key=config[key])

    secret_key = f"lakefed-{source_name.replace('_', '-')}"
    if config["source_type"].startswith("oracle"):
        host, port, service = parse_oracle_url(akv("key_url"))
        secret = {"user": akv("key_user"), "password": akv("key_pass")}
        connection = {"host": host, "port": port, "database": service}
    elif config["source_type"] == "mssql":
        secret = {"user": akv("key_user"), "password": akv("key_pass")}
        if config.get("windows_auth"):
            domain, user = split_ntlm_user(secret["user"], ntlm_domain)
            secret = {"auth_mode": "ntlm", "domain": domain, "user": user,
                      "password": secret["password"]}
        connection = {"host": akv("key_host"), "port": config.get("port", 1433),
                      "database": config["database"]}
    else:  # Fabric: Entra ID service principal
        secret = {"auth_mode": "aad_sp", "client_id": akv("key_client_id"),
                  "client_secret": akv("key_client_secret")}
        connection = {"host": akv("key_host"), "port": config.get("port", 1433),
                      "database": config["database"]}
    connection["secret_key"] = secret_key

    columns_by_dataset = {}
    for name in body["datasets"]:
        table = f"{bronze_catalog}.{source_name}.{name}"
        if spark.catalog.tableExists(table):
            columns_by_dataset[name] = [
                c for c in spark.table(table).columns
                if c not in BRONZE_METADATA_COLUMNS
            ]

    rows, skipped = build_rows(source_name, body, connection,
                               columns_by_dataset, collection, sink_catalog)
    print(f"{len(rows)} rows ({sum(r['load_type'] == 'cursor' for r in rows)} cursor), "
          f"{len(skipped)} skipped, secret key {secret_key}, host {connection['host']}")
    for name, reason in sorted(skipped.items()):
        print(f"  skip {name}: {reason}")
    if not apply:
        print("Dry run. Set apply=true to write the secret and the control rows.")
        return

    # The engine creates sink tables, not schemas.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{sink_catalog}`.`{source_name}`")
    WorkspaceClient().secrets.put_secret(
        scope="lakefed-cursor", key=secret_key, string_value=json.dumps(secret))
    store = StateStore.connect_lakebase(db_instance, db_name)
    columns = list(rows[0])
    insert = (f"insert into lakefed_cursor.control ({', '.join(columns)}) "
              f"values ({', '.join(['%s'] * len(columns))})")
    with store._conn.transaction():
        cursor = store._conn.cursor()
        cursor.execute("delete from lakefed_cursor.control where task_collection = %s",
                       (collection,))
        cursor.executemany(insert, [tuple(r[c] for c in columns) for r in rows])
    print(f"Wrote {len(rows)} control rows for {collection}.")


if "dbutils" in globals():
    _run_notebook()
