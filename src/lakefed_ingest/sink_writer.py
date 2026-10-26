"""Sink writes: Delta DDL, MERGE text, seeds and full reloads, write-conflict retry.

Reads come from ``source_io``. Row counts come from Delta commit metrics, never from ``count()``
on the source DataFrame. No ``.cache()``: Delta MERGE materializes a JDBC source itself.

Consolidated sinks (``control.client_id``): many source databases write one sink table. Every row
carries the value in column ``__client_id__`` (``CLIENT_COLUMN``). MERGEs match on
(__client_id__, PK). Seeds and full reloads replace only that client's rows. A row with
client_id NULL owns its sink. Mixing the two on one sink is rejected.

Delta write conflicts retry the whole unit with exponential backoff and full jitter
(``with_write_retry``). Writes are idempotent, so a re-read is safe.

Heavy imports (pyspark) are lazy, so the pure helpers import without them.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import time
import uuid
from typing import List, Optional, Tuple

import source_adapters
import source_io

def merge_on(primary_key: str) -> str:
    """MERGE ON clause (Delta backtick-quoted) keyed on the (composite) PK."""
    return " AND ".join(f"tgt.`{c}` = s.`{c}`" for c in source_io._split(primary_key))


def merge_set(select_list: str, primary_key: str) -> str:
    """UPDATE SET of the non-PK columns; degenerate all-PK table -> harmless no-op SET on the PK.

    Targets are UNQUALIFIED (`` `c` = s.`c` ``): under ``MERGE WITH SCHEMA EVOLUTION`` a column the
    target doesn't have yet only resolves unqualified (``tgt.`c` `` fails on DBR 18)."""
    non_pk = source_io._without(source_io._split(select_list), source_io._split(primary_key))
    cols = non_pk or source_io._split(primary_key)
    return ", ".join(f"`{c}` = s.`{c}`" for c in cols)


def merge_insert_cols(select_list: str) -> str:
    return ", ".join(f"`{c}`" for c in source_io._split(select_list))


def merge_insert_vals(select_list: str) -> str:
    return ", ".join(f"s.`{c}`" for c in source_io._split(select_list))


def quote_delta_ident(name: str) -> str:
    """Backtick-quoted Databricks SQL identifier (`` ` `` doubled). A name that is already a quoted
    identifier is kept as it is: control rows written for earlier versions, which didn't quote sink
    names, had to quote those that needed it themselves."""
    name = str(name)
    if len(name) >= 2 and name[0] == name[-1] == "`" and "`" not in name[1:-1].replace("``", ""):
        return name
    return "`" + name.replace("`", "``") + "`"


def sink_fqn(cfg: dict) -> str:
    """The row's sink as a quoted three-part name, so catalogs, schemas and tables whose names need
    quoting (``01-sales-dev``) work in every statement that names the sink."""
    return ".".join(quote_delta_ident(cfg[k]) for k in ("sink_catalog", "sink_schema", "sink_table"))


def _with_table_suffix(fqn: str, suffix: str) -> str:
    """``fqn`` with ``suffix`` appended to its table name, inside the quotes when it is quoted."""
    return f"{fqn[:-1]}{suffix}`" if fqn.endswith("`") else f"{fqn}{suffix}"


def staging_table_name(sink_fqn: str, cfg: Optional[dict] = None) -> str:
    """Staging table a partitioned seed loads into before the atomic swap (same catalog.schema).
    Consolidated sinks get one per client — several clients may seed the same sink at once."""
    cid = client_id(cfg) if cfg else None
    if cid is None:
        return _with_table_suffix(sink_fqn, "__seed_staging")
    slug = re.sub(r"[^a-z0-9_]", "_", cid.lower())[:40]
    return _with_table_suffix(sink_fqn, f"__seed_staging__{slug}_{hashlib.sha1(cid.encode()).hexdigest()[:8]}")


CLIENT_COLUMN = "__client_id__"


_CLIENT_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")   # same rule as the control CHECK constraint


def client_id(cfg: dict) -> Optional[str]:
    """The row's client_id (consolidated sink), or None for a dedicated sink."""
    v = cfg.get("client_id")
    if v is None or str(v).strip() == "":
        return None
    v = str(v).strip()
    if not _CLIENT_ID.match(v):
        raise ValueError(f"invalid client_id {v!r}: use 1-128 of A-Z a-z 0-9 _ . : -")
    return v


_ATTR_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


def client_attrs(cfg: dict) -> List[Tuple[str, Optional[str]]]:
    """The row's identity columns: ``control.client_attrs``, a JSON object {column: value}, as
    [(column, value as text)] in the object's order. Every row the control row writes carries them,
    as STRING columns next to __client_id__ (e.g. the tenant name or the source server), so a
    shared sink says where each row came from. Names are letters, digits and ``_``, not starting with
    ``__`` (lakefed's own columns); values are scalars (NULL allowed)."""
    raw = cfg.get("client_attrs")
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return []
    attrs = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(attrs, dict):
        raise ValueError("client_attrs must be a JSON object {column: value}")
    out, seen = [], set()
    for name, value in attrs.items():
        if not _ATTR_NAME.match(str(name)) or str(name).startswith("__"):
            raise ValueError(f"invalid client_attrs column {name!r}: use letters, digits and _ "
                             "(1-128 characters, not starting with __)")
        if name.lower() in seen:
            raise ValueError(f"client_attrs column {name!r} is given twice")
        if isinstance(value, (dict, list)):
            raise ValueError(f"client_attrs column {name!r}: the value must be a string, number, boolean or null")
        seen.add(name.lower())
        out.append((name, None if value is None else str(value)))
    return out


MERGE = "merge"


def write_mode(cfg: dict) -> str:
    """The row's write mode: 'merge' (NULL or empty is merge). Any other value is an error."""
    mode = str(cfg.get("write_mode") or MERGE).strip().lower()
    if mode != MERGE:
        raise ValueError(f"invalid write_mode {cfg.get('write_mode')!r}: use merge")
    return mode


def write_columns(cfg: dict) -> List[str]:
    """Columns written to the sink: select_list, plus __client_id__ for a consolidated sink and
    the client_attrs columns."""
    cols = source_io._split(cfg["select_list"])
    cols += [CLIENT_COLUMN] if client_id(cfg) else []
    cols += [name for name, _ in client_attrs(cfg)]
    return cols


def merge_keys(cfg: dict) -> List[str]:
    """MERGE match keys: (__client_id__, PK) for a consolidated sink, else the PK."""
    pk = source_io._split(cfg["primary_key"])
    return [CLIENT_COLUMN] + pk if client_id(cfg) else pk


def client_predicate(cfg: dict) -> str:
    """Delta predicate selecting this client's rows (the value is validated by ``client_id``)."""
    return f"`{CLIENT_COLUMN}` = '{client_id(cfg)}'"


def _tag(df, cfg: dict):
    """Prepend the client_id literal (consolidated sink) and the client_attrs literals to a source
    DataFrame; no-op when there is nothing to add. FIRST, so a new sink
    has them within Delta's stats columns (first 32 by default); liquid clustering on __client_id__
    requires stats; wide tables would otherwise fail to cluster."""
    # Columns go in as df[name], never as bare names: PySpark turns a bare name into a column through a
    # JVM class lookup by short name, which misses the driver's whole classpath while holding the
    # notebook class loader's lock, once per column per table, and every table worker queues on it.
    cid = client_id(cfg)
    lead = [_lit(cid).alias(CLIENT_COLUMN)] if cid is not None else []
    lead += [_lit(value, "string").alias(name) for name, value in client_attrs(cfg)]
    if not lead:
        return df
    return df.select(*lead, *[df[c] for c in df.columns])


def _lit(value, dtype: Optional[str] = None):
    from pyspark.sql import functions as F
    col = F.lit(value)
    return col.cast(dtype) if dtype else col


def merge_on_cfg(cfg: dict) -> str:
    """MERGE ON for a control row: the match keys, plus — consolidated sink — a LITERAL client
    predicate on the target, so Delta prunes to this client's (clustered) files and scopes conflict
    detection to them instead of every client's rows."""
    on = merge_on(", ".join(merge_keys(cfg)))
    return f"{on} AND tgt.{client_predicate(cfg)}" if client_id(cfg) else on


def create_sink_sql(sink_fqn: str, view: str, cfg: dict) -> str:
    """ONE atomic statement creating the sink from a 0-row view: type widening on, and — consolidated
    sink — liquid clustering on __client_id__. A single CTAS means a sink can never exist half-configured
    (two clients racing to create it: one wins, the other sees it exists)."""
    cluster = f"CLUSTER BY (`{CLIENT_COLUMN}`) " if client_id(cfg) else ""
    # Type widening lets schema-evolving writes apply safe widenings (int->bigint, float->double,
    # decimal precision) when a client's column type is wider. Existing sinks:
    # ALTER TABLE <sink> SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true').
    return (f"CREATE TABLE IF NOT EXISTS {sink_fqn} {cluster}"
            f"TBLPROPERTIES ('delta.enableTypeWidening' = 'true') AS SELECT * FROM {view}")


WRITE_RETRY_ATTEMPTS = int(os.environ.get("LAKEFED_WRITE_RETRY_ATTEMPTS", "6"))


WRITE_RETRY_BASE_S = float(os.environ.get("LAKEFED_WRITE_RETRY_BASE_S", "1"))


WRITE_RETRY_MAX_S = float(os.environ.get("LAKEFED_WRITE_RETRY_MAX_S", "60"))


_CONFLICT_MARKERS = (
    "ConcurrentAppendException", "ConcurrentDeleteReadException", "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException", "MetadataChangedException", "ProtocolChangedException",
    "DELTA_CONCURRENT_", "DELTA_METADATA_CHANGED", "DELTA_PROTOCOL_CHANGED",
    "TABLE_OR_VIEW_ALREADY_EXISTS",            # two clients creating the same consolidated sink
)


def is_write_conflict(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}: {exc}"
    return any(m in text for m in _CONFLICT_MARKERS)


def backoff_delay(attempt: int, base: float = None, cap: float = None, rand=random.random) -> float:
    """Full-jitter exponential backoff for retry ``attempt`` (0-based)."""
    base = WRITE_RETRY_BASE_S if base is None else base
    cap = WRITE_RETRY_MAX_S if cap is None else cap
    return rand() * min(cap, base * (2 ** attempt))


def with_write_retry(fn, what: str = "write", attempts: int = None, sleep=time.sleep, rand=random.random):
    """Run ``fn()``; on a Delta write conflict retry up to ``attempts`` times in total. Other errors
    (and the last conflict) propagate unchanged."""
    attempts = WRITE_RETRY_ATTEMPTS if attempts is None else attempts
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            if not is_write_conflict(e) or i == attempts - 1:
                raise
            delay = backoff_delay(i, rand=rand)
            print(f"[retry] {what}: write conflict (attempt {i + 1}/{attempts}), retrying in {delay:.1f}s: "
                  f"{str(e)[:160]}")
            sleep(delay)


SINK_MODE_MISMATCH_MSG = (
    "sink {sink} {has} '{col}' column but this control row {is_} consolidated (client_id {cid}); "
    "a sink is either dedicated to one source table or shared by client_id — never both. Point the "
    "row at another sink or fix client_id.")


def check_sink_mode(cfg: dict, sink_fqn: str, sink_columns: List[str]) -> None:
    """Refuse to write a dedicated row into a consolidated sink (its overwrite/MERGE would hit every
    client's rows) or a consolidated row into a dedicated sink (rows without __client_id__)."""
    have = {c.lower() for c in sink_columns}
    selected = {c.lower() for c in source_io._split(cfg.get("select_list"))}
    has = CLIENT_COLUMN in have
    cid = client_id(cfg)
    native = CLIENT_COLUMN in selected
    if has and cid is None and not native:     # a dedicated table may have its own such column
        raise ValueError(SINK_MODE_MISMATCH_MSG.format(sink=sink_fqn, has="has a", col=CLIENT_COLUMN,
                                                       is_="is not", cid="NULL"))
    if not has and cid is not None:
        raise ValueError(SINK_MODE_MISMATCH_MSG.format(sink=sink_fqn, has="has no", col=CLIENT_COLUMN,
                                                       is_="is", cid=repr(cid)))


def ensure_sink_table(spark, cfg: dict, sink_fqn: str, slots=None) -> None:
    """Create the Delta bronze sink if absent, typed from the source via a 0-row JDBC read (so the
    JDBC dialect infers the Spark schema natively), then check the sink's mode (dedicated vs
    consolidated) matches the row. Idempotent. The 0-row read is one unit.

    A consolidated sink is created with the ``__client_id__`` column and liquid clustering on it, so
    each client's slice is cheap to find and concurrent clients' writes touch separate files."""
    if not spark.catalog.tableExists(sink_fqn):
        def create():
            with source_io._unit(slots, cfg):
                if spark.catalog.tableExists(sink_fqn):      # another client created it meanwhile
                    return
                empty = _tag(source_io._read(spark, cfg, source_adapters.for_cfg(cfg).select_sql(cfg, where="1=0")), cfg)
                view = f"sink_schema_{uuid.uuid4().hex}"
                empty.createOrReplaceTempView(view)
                try:
                    spark.sql(create_sink_sql(sink_fqn, view, cfg))
                finally:
                    spark.catalog.dropTempView(view)
        with_write_retry(create, what=f"create {sink_fqn}")
    check_sink_mode(cfg, sink_fqn, spark.table(sink_fqn).columns)


def _is_partitioned(cfg: dict) -> bool:
    v = cfg.get("load_partitioned")
    return v is True or str(v).lower() == "true"


def insert_sql(mode: str, target: str, cols: List[str], source: str, evolve: bool = False) -> str:
    """``INSERT [WITH SCHEMA EVOLUTION] {INTO|OVERWRITE} <target> BY NAME SELECT `a`, `b` FROM <source>``.

    BY NAME maps columns by name, so a reordered/edited select_list can never shift values into the
    wrong columns; with ``evolve`` the target gains any select_list column it lacks (spike-validated
    on DBR 18 — an explicit column list instead would require the columns to already exist)."""
    collist = ", ".join(f"`{c}`" for c in cols)
    evo = "WITH SCHEMA EVOLUTION " if evolve else ""
    return f"INSERT {evo}{mode} {target} BY NAME SELECT {collist} FROM {source}"


def insert_overwrite_sql(sink_fqn: str, cols: List[str], source: str, cfg: Optional[dict] = None) -> str:
    """Data-only atomic replace with schema evolution: the whole target, or — consolidated sink —
    only this client's rows (``INSERT WITH SCHEMA EVOLUTION INTO <sink> BY NAME REPLACE WHERE
    __client_id__ = '<id>' SELECT ...``; validated on DBR: other clients untouched, BY NAME mapping,
    new columns added. BY NAME must precede REPLACE WHERE)."""
    if cfg is not None and client_id(cfg):
        collist = ", ".join(f"`{c}`" for c in cols)
        return (f"INSERT WITH SCHEMA EVOLUTION INTO {sink_fqn} BY NAME REPLACE WHERE {client_predicate(cfg)} "
                f"SELECT {collist} FROM {source}")
    return insert_sql("OVERWRITE", sink_fqn, cols, source, evolve=True)


def missing_sink_columns(select_list: str, sink_columns: List[str]) -> List[str]:
    """select_list columns absent from the sink (case-insensitive) — pure, unit-tested. A non-empty
    result in the incremental phase means the source table gained a column: the sweep reseeds that table so
    historical rows get the new column (a cursor read only returns changed rows)."""
    have = {c.lower() for c in sink_columns}
    return [c for c in source_io._split(select_list) if c.lower() not in have]


def _insert_overwrite(spark, sink_fqn: str, source: str, cols: List[str], cfg: Optional[dict] = None) -> int:
    """Atomically replace the target's DATA from ``source`` (a view or table) in one Delta commit.

    ``INSERT OVERWRITE`` is data-only: unlike ``saveAsTable(mode="overwrite")`` — which commits as
    ``CREATE OR REPLACE TABLE AS SELECT`` — it leaves the table definition alone (liquid clustering
    keys, table properties, grants, row filters/column masks), which consolidated bronze depends on.
    Columns map BY NAME from select_list, and ``WITH SCHEMA EVOLUTION`` adds any the target lacks
    (a client/table whose schema gained columns); existing columns are never dropped."""
    row = spark.sql(insert_overwrite_sql(sink_fqn, cols, source, cfg)).first()
    d = row.asDict() if row is not None else {}
    return int(d.get("num_inserted_rows", d.get("num_affected_rows", 0)) or 0)


def seed(spark, cfg: dict, sink_fqn: str, slots=None, executor=None, guard=None) -> int:
    """Full copy of the current table state into the Delta bronze sink: the cursor seed and every
    cycle of the ``full`` strategy (any source adapter).

    * Non-partitioned: ONE unit — one JDBC read streams straight into a data-only ``INSERT OVERWRITE``
      (a single atomic commit); rows come from the INSERT's own metrics.
    * Partitioned (``load_partitioned``): size-bounded range partitions load into a staging table,
      then swap into the target atomically — see ``_partitioned_seed``.
    No ``.cache()``/``count()``: every source range is read exactly once.

    Consolidated sink (``client_id`` set): the overwrite is ``INSERT WITH SCHEMA EVOLUTION INTO sink
    BY NAME REPLACE WHERE __client_id__ = ...``, so only this client's rows are replaced."""
    if _is_partitioned(cfg):
        return _partitioned_seed(spark, cfg, sink_fqn, slots, executor, guard)

    def once():
        with source_io._unit(slots, cfg):
            df = _tag(source_io._read(spark, cfg, source_adapters.for_cfg(cfg).select_sql(cfg)), cfg)
            view = f"seed_full_{uuid.uuid4().hex}"
            df.createOrReplaceTempView(view)
            try:
                return _insert_overwrite(spark, sink_fqn, view, write_columns(cfg), cfg)  # ONE read, ONE commit
            finally:
                spark.catalog.dropTempView(view)
    return with_write_retry(once, what=f"seed {sink_fqn}")


def _partitioned_seed(spark, cfg: dict, sink_fqn: str, slots, executor, guard=None) -> int:
    """Size-bounded, staged, atomic partitioned seed (the accelerator's partitioning strategy).

    1. table size (source catalog, per adapter) and MIN/MAX of ``partition_col`` — one unit each;
    2. ``num_partitions = max(size_mb / partition_size_mb, 2)``, clamped to the bound range — can be
       thousands; they are NOT limited by cores;
    3. create/replace the EMPTY staging table ``<sink>__seed_staging`` with the source schema (unit);
    4. submit every partition to the shared ``executor``; each partition is ONE unit: one JDBC range
       read -> unique temp view -> ``INSERT INTO`` staging (blind appends don't conflict). The
       sweep's ``slots`` budget (``parallelism``) decides how many run at once — across ALL tables;
    5. this coordinating thread waits holding NO slot. On the first failure it cancels the
       not-yet-started partitions, lets in-flight ones settle, and raises: the TARGET IS UNTOUCHED
       (readers keep the previous complete data); the table stays ``seeding`` and the next attempt
       replaces the staging table and starts over;
    6. on success, swap as one unit: overwrite the target from staging in ONE atomic Delta commit
       (same table — grants, row filters and lineage are preserved; never rename tables), then drop
       staging. A consolidated sink replaces only this client's rows (``REPLACE WHERE __client_id__``)
       and stages in a per-client table.
    Returns rows in the target after the swap."""
    from concurrent.futures import ThreadPoolExecutor, as_completed, wait
    import partitions

    schema, table, pcol = cfg["src_schema"], cfg["src_table"], cfg.get("partition_col")
    if not pcol:
        raise ValueError("load_partitioned is set but partition_col is empty")
    adapter = source_adapters.for_cfg(cfg)
    with source_io._unit(slots, cfg):
        size_row = source_io._read(spark, cfg, partitions.table_size_mb_query(schema, table, adapter)).first()
    size_mb = float(size_row["table_size_mb"] or 0) if size_row else 0.0
    with source_io._unit(slots, cfg):
        b = source_io._read(spark, cfg, partitions.bounds_query(schema, table, pcol, adapter)).first()
    lb, ub = (b["lb"], b["ub"]) if b else (None, None)
    if lb is None or ub is None:  # empty table (or all-NULL column): nothing to partition
        print(f"[seed] {schema}.{table}: no {pcol} bounds -> non-partitioned seed")
        return seed(spark, dict(cfg, load_partitioned=False), sink_fqn, slots=slots)  # single unit

    n = partitions.num_partitions_for(size_mb, cfg.get("partition_size_mb"))
    n = partitions.effective_num_partitions(lb, ub, n)
    plist = partitions.get_partition_list(pcol, lb, ub, n, adapter)
    staging = staging_table_name(sink_fqn, cfg)
    print(f"[seed] {schema}.{table}: size={size_mb} MB, partition_size_mb={cfg.get('partition_size_mb')}, "
          f"{pcol} in [{lb}, {ub}], num_partitions={len(plist)}, staging={staging}")

    cols = write_columns(cfg)
    with source_io._unit(slots, cfg):
        _tag(source_io._read(spark, cfg, adapter.select_sql(cfg, where="1=0")), cfg).write.mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(staging)

    def load_partition(part):
        if guard is not None:
            guard()                                # e.g. raises if the sweep lost its collection lock
        with source_io._unit(slots, cfg):
            df = _tag(source_io._read(spark, cfg, adapter.select_sql(cfg, where=part["where_clause"])), cfg)
            view = f"seed_part_{uuid.uuid4().hex}"
            df.createOrReplaceTempView(view)
            try:
                row = spark.sql(insert_sql("INTO", staging, cols, view)).first()
            finally:
                spark.catalog.dropTempView(view)
        d = row.asDict() if row is not None else {}
        return int(d.get("num_inserted_rows", d.get("num_affected_rows", 0)) or 0)

    own_executor = executor is None
    ex = executor or ThreadPoolExecutor(max_workers=slots.capacity if slots is not None else 8)
    try:
        futures = [ex.submit(load_partition, p) for p in plist]
        first_err = None
        for f in as_completed(futures):            # waiting here holds NO slot
            if f.exception() is not None:
                first_err = f.exception()
                for g in futures:
                    g.cancel()                     # only not-yet-started partitions cancel
                break
        if first_err is not None:
            wait(futures)                          # let in-flight partitions settle
            failed = sum(1 for g in futures if not g.cancelled() and g.exception() is not None)
            cancelled = sum(1 for g in futures if g.cancelled())
            raise RuntimeError(
                f"partitioned seed of {schema}.{table}: {failed}/{len(plist)} partitions failed, "
                f"{cancelled} cancelled; target untouched; first error: {str(first_err)[:300]}") from first_err
    finally:
        if own_executor:
            ex.shutdown(wait=True)

    if guard is not None:
        guard()                                    # don't swap if the sweep lost its lock
    def swap():
        with source_io._unit(slots):                         # atomic swap: ONE data-only Delta commit
            return _insert_overwrite(spark, sink_fqn, staging, write_columns(cfg), cfg)
    rows = with_write_retry(swap, what=f"seed swap {sink_fqn}")
    with source_io._unit(slots):
        spark.sql(f"DROP TABLE IF EXISTS {staging}")
    return int(rows or 0)


# ---- the cursor expression on the sink side (research R-07) -----------------------------------------
SINK_FUNCTIONS = frozenset({"GREATEST", "COALESCE"})   # the same text runs on the source and in Spark SQL
_LITERAL = re.compile(r"N?'(?:[^']|'')*'")
_CALL = re.compile(r"\b([A-Za-z_]\w*)\s*\(")


def strip_literals(text: str) -> str:
    """``text`` with every ``'...'`` (and ``N'...'``) string literal replaced by ``''``."""
    return _LITERAL.sub("''", text)


def expression_functions(expression: str) -> List[str]:
    """Upper-cased names of the functions an expression calls."""
    return [m.group(1).upper() for m in _CALL.finditer(strip_literals(expression or ""))]


def sink_expr(expression: str) -> str:
    """The cursor expression for Spark SQL on the sink: GREATEST and COALESCE map one to one, so the
    text passes through; any other function raises (it has no sink counterpart)."""
    bad = sorted(set(expression_functions(expression)) - SINK_FUNCTIONS)
    if bad:
        raise ValueError(f"cursor_expression uses {', '.join(bad)}, which Spark SQL cannot run on the sink: "
                         "only GREATEST and COALESCE are allowed")
    return expression.strip()


def merge_sql(cfg: dict, sink_fqn: str, view: str) -> str:
    """Idempotent I/U/D MERGE of staged change rows (temp view ``view``, column ``op``) into the sink, by PK.

    ``WITH SCHEMA EVOLUTION`` (per statement — never the session-wide autoMerge conf, which the
    threaded sweep would share): select_list columns the sink lacks are added, and with type widening
    enabled on the sink, safe widenings (int->bigint, ...) apply. Explicit column lists keep the
    ``op`` column out of bronze. ON keeps tgt./s. qualifiers; SET/INSERT targets are unqualified."""
    sl, keys = ", ".join(write_columns(cfg)), ", ".join(merge_keys(cfg))
    return (
        f"MERGE WITH SCHEMA EVOLUTION INTO {sink_fqn} AS tgt USING {view} AS s ON {merge_on_cfg(cfg)} "
        "WHEN MATCHED AND s.op = 'D' THEN DELETE "
        f"WHEN MATCHED THEN UPDATE SET {merge_set(sl, keys)} "
        "WHEN NOT MATCHED AND s.op <> 'D' THEN "
        f"INSERT ({merge_insert_cols(sl)}) "
        f"VALUES ({merge_insert_vals(sl)})"
    )
