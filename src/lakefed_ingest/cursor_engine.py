"""Cursor (high-watermark) strategy, SQL Server: incremental reads on a monotonically increasing
column (``control.cursor_column``), MERGEd into the Delta sink by PK.

Per cycle, for a seeded table (checkpoint phase ``incremental``):
  1. ``hi`` = the column's current high watermark (one small source query);
  2. ``hi`` == checkpoint position -> unchanged (no read, no MERGE);
  3. otherwise read ``cursor_column > position AND cursor_column <= hi`` (ONE source read) and
     upsert-MERGE by PK; the checkpoint advances to ``hi`` only after the MERGE commits
     (at-least-once — re-reading a window is harmless, the MERGE is idempotent).
The seed captures ``hi`` BEFORE the full copy, so rows written during the seed are re-read by the
first increment (no gap).

Supported column types (SQL Server system type of ``cursor_column``):
  * integers / exact numerics: tinyint, smallint, int, bigint, decimal, numeric (identity keys);
  * temporal: date, smalldatetime, datetime, datetime2, datetimeoffset (an application-maintained
    ``updated_at``);
  * ``rowversion`` (``timestamp``): recommended — SQL Server bumps it on every insert AND update,
    and ``hi`` is capped below ``MIN_ACTIVE_ROWVERSION()`` so rows from still-open transactions are
    never skipped.
Float / real / string columns are rejected (lossy or not reliably ordered).

Limits (by design of a watermark): deletes are not captured (use ``ct``, or a periodic ``full``);
updates are captured only if they move the cursor column (``rowversion`` always does, an identity
never does); for non-rowversion columns, a transaction that commits a value BELOW an already-read
``hi`` after the read is missed — the reason ``rowversion`` is recommended.

Position (``checkpoint.position``, text) is JSON ``{"col", "type", "value"}``: the column and its
type are recorded so a changed ``cursor_column`` re-seeds and every later cycle needs no type
lookup. ``value`` is the SQL Server string form (ISO 8601 for temporal, decimal text otherwise).

Pure builders are unit-tested in tests/cursor_engine_test.py; source access reuses ``ct_engine``'s
JDBC helpers (one slot per query unit, lazy Spark imports).
"""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timedelta
from typing import Optional, Tuple

import ct_engine
import source_adapters

# SQL Server names (TYPE_NAME) and PostgreSQL names (information_schema.columns.data_type, which
# never reports a bare 'timestamp' — that name stays SQL Server's rowversion alias).
NUMERIC_TYPES = {"tinyint", "smallint", "int", "integer", "bigint", "decimal", "numeric"}
TEMPORAL_TYPES = {"date", "smalldatetime", "datetime", "datetime2", "datetimeoffset",
                  "timestamp without time zone", "timestamp with time zone"}
ROWVERSION_TYPES = {"timestamp", "rowversion"}  # TYPE_NAME(system_type_id) reports 'timestamp'
SUPPORTED_TYPES = NUMERIC_TYPES | TEMPORAL_TYPES | ROWVERSION_TYPES

_NUMERIC_VALUE = re.compile(r"^-?\d+(\.\d+)?$")
_TEMPORAL_VALUE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2}(\.\d{1,7})?)?)?([+-]\d{2}:\d{2}|Z)?$")
_INTEGER_VALUE = re.compile(r"^-?\d+$")


def _kind(typ: str) -> str:
    t = (typ or "").strip().lower()
    if t in NUMERIC_TYPES:
        return "numeric"
    if t in TEMPORAL_TYPES:
        return "temporal"
    if t in ROWVERSION_TYPES:
        return "rowversion"
    raise ValueError(f"cursor_column type '{typ}' is not supported for load_type 'cursor' "
                     f"(supported: {', '.join(sorted(SUPPORTED_TYPES - {'rowversion'}))} / rowversion)")


# ---- position --------------------------------------------------------------------------------------

def encode_position(col: str, typ: str, value: Optional[str]) -> str:
    return json.dumps({"col": col, "type": typ, "value": value}, separators=(",", ":"))


def decode_position(text: Optional[str]) -> Optional[dict]:
    """{"col", "type", "value"} or None (no / unreadable position -> the planner re-seeds)."""
    if not text:
        return None
    try:
        d = json.loads(text)
    except (TypeError, ValueError):
        return None
    if not isinstance(d, dict) or not d.get("col") or not d.get("type"):
        return None
    return {"col": d["col"], "type": d["type"], "value": d.get("value")}


# ---- SQL builders (pure) ---------------------------------------------------------------------------

def _obj(cfg: dict) -> str:
    return ct_engine.quote_nstr(f"{ct_engine.quote_ident(cfg['src_schema'])}.{ct_engine.quote_ident(cfg['src_table'])}")


def _is_pg(cfg: dict) -> bool:
    return source_adapters.for_cfg(cfg).src_type == source_adapters.POSTGRESQL


def _col(cfg: dict) -> str:
    """``cursor_column`` quoted for the source dialect."""
    a = source_adapters.for_cfg(cfg)
    return a.quote_ident(cfg["cursor_column"]) if _is_pg(cfg) else ct_engine.quote_ident(cfg["cursor_column"])


def _pg_column_where(cfg: dict) -> str:
    q = source_adapters.for_cfg(cfg).quote_str
    return (f"FROM information_schema.columns WHERE table_schema = {q(cfg['src_schema'])} "
            f"AND table_name = {q(cfg['src_table'])} AND column_name = {q(cfg['cursor_column'])}")


def column_type_sql(cfg: dict) -> str:
    """System type name and nullability of ``cursor_column`` (no row when the column doesn't exist)."""
    if _is_pg(cfg):
        return f"SELECT data_type AS typ, (is_nullable = 'YES') AS nullable {_pg_column_where(cfg)}"
    return ("SELECT TYPE_NAME(c.system_type_id) AS typ, c.is_nullable AS nullable FROM sys.columns c "
            f"WHERE c.object_id = OBJECT_ID({_obj(cfg)}) AND c.name = {ct_engine.quote_nstr(cfg['cursor_column'])}")


def nullable_sql(cfg: dict) -> str:
    """Scalar subquery: 1 if ``cursor_column`` is nullable (checked on every run, not just at seed —
    a column can be altered to NULL after the table was seeded)."""
    if _is_pg(cfg):
        return f"(SELECT CASE WHEN is_nullable = 'YES' THEN 1 ELSE 0 END {_pg_column_where(cfg)})"
    return ("(SELECT CAST(c.is_nullable AS INT) FROM sys.columns c "
            f"WHERE c.object_id = OBJECT_ID({_obj(cfg)}) AND c.name = {ct_engine.quote_nstr(cfg['cursor_column'])})")


_PG_ISO = "'YYYY-MM-DD\"T\"HH24:MI:SS.US'"


def high_watermark_sql(cfg: dict, typ: str) -> str:
    """One row ``hi``, ``nullable``: the cursor column's high watermark as source text (NULL on an
    empty table) and its nullability (one query per run for both). rowversion is capped below
    MIN_ACTIVE_ROWVERSION() so open transactions are never skipped. PostgreSQL emits ISO 8601 via
    to_char; a timestamptz is rendered in UTC with an explicit offset so the literal round-trips
    whatever the session time zone is."""
    col = _col(cfg)
    kind = _kind(typ)
    if _is_pg(cfg):
        a = source_adapters.for_cfg(cfg)
        if kind == "numeric":
            expr = f"MAX({col})::text"
        elif "with time zone" in typ.lower():
            expr = f"to_char(MAX({col}) AT TIME ZONE 'UTC', {_PG_ISO}) || '+00:00'"
        else:
            expr = f"to_char(MAX({col}), {_PG_ISO})"
        return f"SELECT {expr} AS hi, {nullable_sql(cfg)} AS nullable FROM {a.table_ref(cfg['src_schema'], cfg['src_table'])}"
    if kind == "numeric":
        expr = f"CONVERT(VARCHAR(64), MAX({col}))"
    elif kind == "temporal":
        expr = f"CONVERT(VARCHAR(64), MAX({col}), 126)"
    else:
        mx, mar = f"CAST(MAX({col}) AS BIGINT)", "CAST(MIN_ACTIVE_ROWVERSION() AS BIGINT)"
        expr = f"CONVERT(VARCHAR(20), CASE WHEN {mx} < {mar} THEN {mx} ELSE {mar} - 1 END)"
    table = ct_engine.quote_ident(cfg["src_schema"]) + "." + ct_engine.quote_ident(cfg["src_table"])
    return f"SELECT {expr} AS hi, {nullable_sql(cfg)} AS nullable FROM {table}"


def literal(value: str, typ: str, pg: bool = False) -> str:
    """A typed source literal for a position value (validated — values come from our own
    checkpoint, but are never spliced unchecked). ``pg``: PostgreSQL CAST form."""
    kind = _kind(typ)
    v = str(value).strip()
    if kind == "numeric":
        if not _NUMERIC_VALUE.match(v):
            raise ValueError(f"invalid numeric cursor position {value!r}")
        return v
    if kind == "temporal":
        if not _TEMPORAL_VALUE.match(v):
            raise ValueError(f"invalid temporal cursor position {value!r}")
        if pg:
            return f"CAST('{v}' AS {typ.strip().lower()})"
        return f"CONVERT({typ.strip().lower()}, {ct_engine.quote_nstr(v)}, 126)"
    if not _INTEGER_VALUE.match(v):
        raise ValueError(f"invalid rowversion cursor position {value!r}")
    return f"CAST(CAST({v} AS BIGINT) AS BINARY(8))"


def read_where(cfg: dict, typ: str, lo: Optional[str], hi: str) -> str:
    """``cursor_column > lo AND cursor_column <= hi`` (no lower bound when ``lo`` is None — a table
    that was empty at seed time). Sargable: the literal is cast to the column's type, not vice versa.

    A temporal cursor is assigned when the row is written, not when it commits, so a row can commit
    after a read whose ``hi`` is above it. The lower bound is pulled back by ``CURSOR_OVERLAP_S`` for
    temporal kinds (the MERGE makes the re-read harmless); rowversion and numeric stay exact."""
    col, pg = _col(cfg), _is_pg(cfg)
    upper = f"{col} <= {literal(hi, typ, pg)}"
    if lo is None:
        return upper
    if _kind(typ) == "temporal":
        lo = _minus_seconds(lo, CURSOR_OVERLAP_S)
    return f"{col} > {literal(lo, typ, pg)} AND {upper}"


CURSOR_OVERLAP_S = int(os.environ.get("LAKEFED_CURSOR_OVERLAP_S") or "600")


def seed_position(typ: str, hi: Optional[str], seed_seconds: float) -> Optional[str]:
    """The position to record after a seed. ``hi`` was captured before the copy; a temporal cursor
    can still gain rows BELOW ``hi`` that commit while the copy runs (their timestamp predates the
    capture), so the recorded position is pulled back by the seed's duration and the first increment
    re-reads that span. Numeric and rowversion keep ``hi``."""
    if hi is None or _kind(typ) != "temporal":
        return hi
    return _minus_seconds(hi, int(seed_seconds) + 1)


def _minus_seconds(value: str, seconds: int) -> str:
    """``value`` (ISO 8601, as the source returned it) minus ``seconds``, in the same ISO form.
    SQL Server emits up to 7 fractional digits; ``fromisoformat`` takes at most 6, so the fraction
    is truncated (a 100 ns loss on a bound that is already pulled back by minutes). A ``date``
    column gains at most one day from this (a date carries no commit order anyway)."""
    v = str(value).strip()
    if not _TEMPORAL_VALUE.match(v):
        raise ValueError(f"invalid temporal cursor position {value!r}")
    v = re.sub(r"(\.\d{6})\d+", r"\1", v).replace("Z", "+00:00")
    return (datetime.fromisoformat(v) - timedelta(seconds=seconds)).isoformat()


def upsert_merge_sql(cfg: dict, sink_fqn: str, view: str) -> str:
    """Idempotent upsert MERGE by PK — (__client_id__, PK) for a consolidated sink — with schema
    evolution. A watermark read carries no deletes."""
    sl, keys = ", ".join(ct_engine.write_columns(cfg)), ", ".join(ct_engine.merge_keys(cfg))
    return (
        f"MERGE WITH SCHEMA EVOLUTION INTO {sink_fqn} AS tgt USING {view} AS s "
        f"ON {ct_engine.merge_on_cfg(cfg)} "
        f"WHEN MATCHED THEN UPDATE SET {ct_engine.merge_set(sl, keys)} "
        f"WHEN NOT MATCHED THEN INSERT ({ct_engine.merge_insert_cols(sl)}) "
        f"VALUES ({ct_engine.merge_insert_vals(sl)})"
    )


# ---- source access ---------------------------------------------------------------------------------

def column_type(cfg: dict, slots=None) -> str:
    """The cursor column's SQL Server type; raises a clear error if missing, unsupported or nullable.

    A nullable cursor column is refused: incremental reads select ``col > lo AND col <= hi``, so a
    row whose cursor value is NULL would never be read (the seed copies it, later changes to it
    would be silently missed)."""
    with ct_engine._unit(slots, cfg):
        row = ct_engine._read(ct_engine._spark(), cfg, column_type_sql(cfg)).first()
    if row is None or row["typ"] is None:
        raise ValueError(f"cursor_column '{cfg['cursor_column']}' not found on "
                         f"{cfg['src_schema']}.{cfg['src_table']}")
    typ = str(row["typ"]).lower()
    _kind(typ)
    _refuse_nullable(cfg, row["nullable"])
    return typ


def _refuse_nullable(cfg: dict, nullable) -> None:
    if nullable:
        raise ValueError(f"cursor_column '{cfg['cursor_column']}' on {cfg['src_schema']}.{cfg['src_table']} "
                         "is nullable: rows with a NULL value would never be read incrementally. Use a NOT NULL "
                         "column (rowversion recommended), or load_type ct or full")


def high_watermark(cfg: dict, typ: str, slots=None) -> Optional[str]:
    with ct_engine._unit(slots, cfg):
        row = ct_engine._read(ct_engine._spark(), cfg, high_watermark_sql(cfg, typ)).first()
    if row is not None:
        _refuse_nullable(cfg, row["nullable"])
    return None if row is None or row["hi"] is None else str(row["hi"])


def read_and_merge(spark, cfg: dict, sink_fqn: str, typ: str, lo: Optional[str], hi: str,
                   slots=None) -> Optional[int]:
    """Read (lo, hi] and upsert-MERGE it into the sink — ONE source read feeding the MERGE (no
    ``.cache()``/``count()``). Returns rows affected from the MERGE metrics (None if unavailable)."""
    query = source_adapters.for_cfg(cfg).select_sql(cfg, where=read_where(cfg, typ, lo, hi))
    def once():
        with ct_engine._unit(slots, cfg):
            df = ct_engine._tag(ct_engine._read(spark, cfg, query), cfg)
            view = f"cursor_staged_{uuid.uuid4().hex}"
            df.createOrReplaceTempView(view)
            try:
                return spark.sql(upsert_merge_sql(cfg, sink_fqn, view)).first()
            finally:
                spark.catalog.dropTempView(view)
    result = ct_engine.with_write_retry(once, what=f"merge {sink_fqn}")
    if result is not None and "num_affected_rows" in result.asDict():
        return int(result["num_affected_rows"] or 0)
    return None


def plan_increment(position: dict, hi: Optional[str]) -> Tuple[str, Optional[str]]:
    """('unchanged' | 'read', new position value). An empty source (hi NULL) means nothing to read;
    the position is kept (never moved to NULL). hi equal to the checkpoint is unchanged for numeric
    and rowversion kinds; a temporal cursor still reads its overlap window, because a row can commit
    late BELOW the high watermark without moving it (a quiet table would otherwise hold that row
    until its next change)."""
    if hi is None:
        return "unchanged", position.get("value")
    if hi == position.get("value") and _kind(position.get("type")) != "temporal":
        return "unchanged", position.get("value")
    return "read", hi
