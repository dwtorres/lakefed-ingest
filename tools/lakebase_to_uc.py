# Databricks notebook source
# MAGIC %pip install "psycopg[binary]" databricks-sdk==0.145.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Copy the lakefed-cursor Lakebase tables into Unity Catalog Delta tables, for DBSQL monitoring
# without a database catalog. Each run replaces the copies. Run it again for fresh numbers.
dbutils.widgets.text("target_schema", "devtest_team_dpe.lakefed_monitor", "1. catalog.schema for the copies")
dbutils.widgets.text("db_instance", "lakefed-cursor", "2. Lakebase instance")
dbutils.widgets.text("db_name", "databricks_postgres", "3. Lakebase database")

# COMMAND ----------

import sys

from pyspark.sql.types import StringType, StructField, StructType

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
tools_dir = "/Workspace" + context.notebookPath().get().rsplit("/", 1)[0]
sys.path.append(tools_dir.rsplit("/", 1)[0] + "/src/lakefed_ingest")
from state_store import StateStore

TABLES = ["control", "checkpoint", "ingest_run", "ingest_event"]
target = dbutils.widgets.get("target_schema").strip()
store = StateStore.connect_lakebase(dbutils.widgets.get("db_instance").strip(),
                                    dbutils.widgets.get("db_name").strip())
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target}")

for table in TABLES:
    cursor = store._conn.cursor()
    cursor.execute(f"SELECT * FROM lakefed_cursor.{table}")
    columns = [c.name for c in cursor.description]
    rows = [tuple(None if v is None else str(v) for v in row) for row in cursor.fetchall()]
    # Text keeps JSON, timestamps and NULLs exact; cast in the query when you need a type.
    schema = StructType([StructField(c, StringType()) for c in columns])
    (spark.createDataFrame(rows, schema).write.mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(f"{target}.{table}"))
    print(f"{table}: {len(rows)} rows to {target}.{table}")
