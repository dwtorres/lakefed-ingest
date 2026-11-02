# Databricks notebook source
# MAGIC %pip install "psycopg[binary]" databricks-sdk==0.145.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Set these widgets, then run all cells. apply=false is a dry run.
dbutils.widgets.text("metadata_file", "", "1. edp source_to_bronze_<source>.json path")
dbutils.widgets.text("source_name", "", "2. edp source name, e.g. rl_solutions")
dbutils.widgets.text("task_collection", "", "3. task_collection, e.g. rl_solutions_shadow")
dbutils.widgets.text("sink_catalog", "devtest_team_dpe", "4. sink catalog")
dbutils.widgets.text("bronze_catalog", "devtest_edp_bronze", "5. edp bronze catalog")
dbutils.widgets.text("ntlm_domain", "cnmc", "6. NTLM domain (login without DOMAIN\\)")
dbutils.widgets.text("engine_path", "", "7. folder of state_store.py (empty: from this notebook)")
dbutils.widgets.text("db_instance", "lakefed-cursor", "8. Lakebase instance")
dbutils.widgets.text("db_name", "databricks_postgres", "9. Lakebase database")
dbutils.widgets.text("oracle_host", "", "11. Oracle host (empty: from key_url)")
dbutils.widgets.text("oracle_port", "", "12. Oracle port (empty: from key_url)")
dbutils.widgets.text("oracle_service", "", "13. Oracle service name (empty: from key_url)")
dbutils.widgets.dropdown("apply", "false", ["false", "true"], "10. apply")

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
    r"^jdbc:oracle:thin:@(?:tcp://|//)?(?P<host>[^:/@()?\s]+)(?::(?P<port>\d+))?"
    r"[/:](?P<service>[^:?/\s()]+)(?::[^?\s]*)?(?:\?.*)?$",
    re.I,
)
TNS_KEY = re.compile(r"\(\s*(HOST|PORT|SERVICE_NAME|SID)\s*=\s*([^)\s]+)\s*\)", re.I)


def describe_oracle_url(url: str) -> str:
    """The shape of a URL without host, service or credential values."""
    text = url.strip()
    keys = [k.upper() for k, _ in TNS_KEY.findall(text)]
    shape = "TNS descriptor" if "(" in text else "thin URL"
    parts = [shape, f"{keys.count('HOST')} HOST"]
    for key in ("SERVICE_NAME", "SID"):
        if key in keys:
            parts.append(key)
    if re.search(r"PROTOCOL\s*=\s*TCPS", text, re.I):
        parts.append("TCPS")
    if re.match(r"^jdbc:oracle:thin:[^@]+@", text, re.I):
        parts.append("credentials before @")
    # Structure only: each word becomes w and each number becomes N.
    template = re.sub(r"[A-Za-z0-9_\-]+",
                      lambda m: "N" if m.group().isdigit() else "w",
                      text.split("@", 1)[-1])
    parts.append(f"form @{template}")
    return ", ".join(parts)


def parse_oracle_url(url: str) -> tuple:
    """Host, port and service from a thin URL or a one-address TNS descriptor."""
    text = url.strip()
    match = ORACLE_URL.match(text)
    if match:
        return match["host"], int(match["port"] or 1521), match["service"]
    values = {}
    for key, value in TNS_KEY.findall(text):
        values.setdefault(key.upper(), []).append(value)
    hosts, ports = values.get("HOST", []), values.get("PORT", [])
    service = (values.get("SERVICE_NAME") or [None])[0]
    if len(hosts) == 1 and len(ports) == 1 and service and "TCPS" not in text.upper():
        return hosts[0], int(ports[0]), service
    raise ValueError(
        f"oracle key_url ({describe_oracle_url(text)}) has no single host, port "
        "and SERVICE_NAME; set the oracle_host, oracle_port and oracle_service widgets"
    )


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

    def widget(name):
        return dbutils.widgets.get(name).strip()

    # Default: <bundle files>/src/lakefed_ingest, next to <bundle files>/tools.
    engine_path = widget("engine_path")
    if not engine_path:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        tools_dir = "/Workspace" + context.notebookPath().get().rsplit("/", 1)[0]
        engine_path = tools_dir.rsplit("/", 1)[0] + "/src/lakefed_ingest"
    sys.path.append(engine_path)
    from state_store import StateStore

    metadata_file = widget("metadata_file")  # /Workspace/.../source_to_bronze_<source>.json
    source_name = widget("source_name")      # e.g. rl_solutions
    collection = widget("task_collection")   # e.g. rl_solutions_shadow
    sink_catalog = widget("sink_catalog")
    bronze_catalog = widget("bronze_catalog")
    ntlm_domain = widget("ntlm_domain")
    db_instance = widget("db_instance")
    db_name = widget("db_name")
    apply = widget("apply") == "true"

    with open(metadata_file) as handle:
        body = json.load(handle)[source_name]
    config = body["source_config"]
    akv = lambda key: dbutils.secrets.get(scope="edp-akv", key=config[key])

    secret_key = f"lakefed-{source_name.replace('_', '-')}"
    if config["source_type"].startswith("oracle"):
        if widget("oracle_host"):
            host = widget("oracle_host")
            port = int(widget("oracle_port") or 1521)
            service = widget("oracle_service")
            if not service:
                raise ValueError("set oracle_service with oracle_host")
        else:
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
    # Upsert on the sink key: an existing row keeps its id, so its checkpoint (keyed by
    # control_id) survives a second apply and the table is not seeded again.
    columns = list(rows[0])
    sink_key = ("sink_catalog", "sink_schema", "sink_table")
    updates = ", ".join(f"{c} = excluded.{c}" for c in columns if c not in sink_key)
    upsert = (f"insert into lakefed_cursor.control ({', '.join(columns)}) "
              f"values ({', '.join(['%s'] * len(columns))}) "
              "on conflict (lower(sink_catalog), lower(sink_schema), lower(sink_table), coalesce(client_id, '')) "
              f"do update set {updates}")
    tables = [r["sink_table"] for r in rows]
    with store._conn.transaction():
        cursor = store._conn.cursor()
        cursor.executemany(upsert, [tuple(r[c] for c in columns) for r in rows])
        cursor.execute("delete from lakefed_cursor.control where task_collection = %s "
                       "and not (sink_table = any(%s))", (collection, tables))
    print(f"Upserted {len(rows)} control rows for {collection}; "
          f"removed {cursor.rowcount} rows not in the metadata.")


if "dbutils" in globals():
    _run_notebook()
