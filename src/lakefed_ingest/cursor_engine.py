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

Supported column types (SQL Server system type of ``cursor_column``), by kind:
  * ``identity``: tinyint, smallint, int, bigint, decimal, numeric (identity keys / sequences). A
    value is taken at insert, not at commit, so a late commit below a read ``hi`` is lost: the kind
    is inserts-only (``plan_increment`` says so) and reads exact bounds;
  * ``temporal``: date, smalldatetime, datetime, datetime2, datetimeoffset (an application-maintained
    ``updated_at``). ``lo`` is pulled back by ``lookback_s`` (job parameter ``cursor_lookback_s``,
    the row's ``cursor_lookback_s`` overrides it) for late commits; ``hi`` is capped at the source
    clock plus ``skew_s`` so one row stamped in 2099 cannot pin the window (``max_above_clock``
    alert); ``datetime`` / ``smalldatetime`` literals carry at most three fractional digits;
  * ``rowversion`` (``timestamp``): recommended — SQL Server bumps it on every insert AND update,
    and ``hi`` is capped below ``MIN_ACTIVE_ROWVERSION()`` so rows from still-open transactions are
    never skipped. Exact bounds.
Float / real / string columns are rejected (lossy or not reliably ordered).

Limits (by design of a watermark): deletes are not captured (use a periodic ``full``);
updates are captured only if they move the cursor column (``rowversion`` always does, an identity
never does). A ``hi`` below the position (a source truncate or reload) fails the table; the position
never moves backwards.

Position (``checkpoint.position``, text) is JSON ``{"col", "type", "value"}``: the column and its
type are recorded so a changed ``cursor_column`` re-seeds and every later cycle needs no type
lookup. ``value`` is the SQL Server string form (ISO 8601 for temporal, decimal text otherwise).

Pure builders are unit-tested in tests/cursor_engine_test.py; source access reuses ``source_io``
(one slot per query unit, lazy Spark imports) and sink writes reuse ``sink_writer``.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Tuple

import sink_writer
import source_adapters
import source_io

# SQL Server names (TYPE_NAME) and PostgreSQL names (information_schema.columns.data_type, which
# never reports a bare 'timestamp' — that name stays SQL Server's rowversion alias).
IDENTITY_TYPES = {"tinyint", "smallint", "int", "integer", "bigint", "decimal", "numeric"}
TEMPORAL_TYPES = {"date", "smalldatetime", "datetime", "datetime2", "datetimeoffset",
                  "timestamp without time zone", "timestamp with time zone"}
ROWVERSION_TYPES = {"timestamp", "rowversion"}  # TYPE_NAME(system_type_id) reports 'timestamp'
SUPPORTED_TYPES = IDENTITY_TYPES | TEMPORAL_TYPES | ROWVERSION_TYPES
MILLISECOND_TYPES = {"datetime", "smalldatetime"}   # CONVERT(..., 126) takes at most 3 fractional digits

_NUMERIC_VALUE = re.compile(r"^-?\d+(\.\d+)?$")
_TEMPORAL_VALUE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2}(\.\d{1,7})?)?)?([+-]\d{2}:\d{2}|Z)?$")
_INTEGER_VALUE = re.compile(r"^-?\d+$")


def _kind(typ: str) -> str:
    t = (typ or "").strip().lower()
    if t in IDENTITY_TYPES:
        return "identity"
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
    return source_io.quote_nstr(f"{source_io.quote_ident(cfg['src_schema'])}.{source_io.quote_ident(cfg['src_table'])}")


def _is_pg(cfg: dict) -> bool:
    return source_adapters.for_cfg(cfg).src_type == source_adapters.POSTGRESQL


def _col(cfg: dict) -> str:
    """``cursor_column`` quoted for the source dialect."""
    a = source_adapters.for_cfg(cfg)
    return a.quote_ident(cfg["cursor_column"]) if _is_pg(cfg) else source_io.quote_ident(cfg["cursor_column"])


def _pg_column_where(cfg: dict) -> str:
    q = source_adapters.for_cfg(cfg).quote_str
    return (f"FROM information_schema.columns WHERE table_schema = {q(cfg['src_schema'])} "
            f"AND table_name = {q(cfg['src_table'])} AND column_name = {q(cfg['cursor_column'])}")


def column_type_sql(cfg: dict) -> str:
    """System type name and nullability of ``cursor_column`` (no row when the column doesn't exist)."""
    if _is_pg(cfg):
        return f"SELECT data_type AS typ, (is_nullable = 'YES') AS nullable {_pg_column_where(cfg)}"
    return ("SELECT TYPE_NAME(c.system_type_id) AS typ, c.is_nullable AS nullable FROM sys.columns c "
            f"WHERE c.object_id = OBJECT_ID({_obj(cfg)}) AND c.name = {source_io.quote_nstr(cfg['cursor_column'])}")


def nullable_sql(cfg: dict) -> str:
    """Scalar subquery: 1 if ``cursor_column`` is nullable (checked on every run, not just at seed —
    a column can be altered to NULL after the table was seeded)."""
    if _is_pg(cfg):
        return f"(SELECT CASE WHEN is_nullable = 'YES' THEN 1 ELSE 0 END {_pg_column_where(cfg)})"
    return ("(SELECT CAST(c.is_nullable AS INT) FROM sys.columns c "
            f"WHERE c.object_id = OBJECT_ID({_obj(cfg)}) AND c.name = {source_io.quote_nstr(cfg['cursor_column'])})")


_PG_ISO = "'YYYY-MM-DD\"T\"HH24:MI:SS.US'"


def high_watermark_sql(cfg: dict, typ: str) -> str:
    """One row ``hi``, ``nullable`` (and ``src_now`` for a temporal kind): the cursor column's high
    watermark as source text (NULL on an empty table), its nullability, and the source clock in the
    same rendering as ``hi`` (one query per run for all). rowversion is capped below
    MIN_ACTIVE_ROWVERSION() so open transactions are never skipped. PostgreSQL emits ISO 8601 via
    to_char; a timestamptz is rendered in UTC with an explicit offset so the literal round-trips
    whatever the session time zone is."""
    col = _col(cfg)
    kind = _kind(typ)
    mx = f"MAX({col})"
    if _is_pg(cfg):
        a = source_adapters.for_cfg(cfg)
        table = a.table_ref(cfg['src_schema'], cfg['src_table'])
        if kind == "identity":
            render, clock = (lambda e: f"{e}::text"), None
        elif "with time zone" in typ.lower():
            render, clock = (lambda e: f"to_char({e} AT TIME ZONE 'UTC', {_PG_ISO}) || '+00:00'"), "now()"
        else:
            render, clock = (lambda e: f"to_char({e}, {_PG_ISO})"), "LOCALTIMESTAMP"
    else:
        table = source_io.quote_ident(cfg["src_schema"]) + "." + source_io.quote_ident(cfg["src_table"])
        if kind == "identity":
            render, clock = (lambda e: f"CONVERT(VARCHAR(64), {e})"), None
        elif kind == "temporal":
            render = lambda e: f"CONVERT(VARCHAR(64), {e}, 126)"
            clock = "SYSDATETIMEOFFSET()" if typ.strip().lower() == "datetimeoffset" else "SYSDATETIME()"
        else:
            mar = "CAST(MIN_ACTIVE_ROWVERSION() AS BIGINT)"
            render, clock = (lambda e: f"CONVERT(VARCHAR(20), CASE WHEN CAST({e} AS BIGINT) < {mar} "
                                       f"THEN CAST({e} AS BIGINT) ELSE {mar} - 1 END)"), None
    src_now = f", {render(clock)} AS src_now" if kind == "temporal" else ""
    return f"SELECT {render(mx)} AS hi, {nullable_sql(cfg)} AS nullable{src_now} FROM {table}"


def literal(value: str, typ: str, pg: bool = False) -> str:
    """A typed source literal for a position value (validated — values come from our own
    checkpoint, but are never spliced unchecked). ``pg``: PostgreSQL CAST form."""
    kind = _kind(typ)
    v = str(value).strip()
    if kind == "identity":
        if not _NUMERIC_VALUE.match(v):
            raise ValueError(f"invalid numeric cursor position {value!r}")
        return v
    if kind == "temporal":
        if not _TEMPORAL_VALUE.match(v):
            raise ValueError(f"invalid temporal cursor position {value!r}")
        v = _trim_fraction(v, typ)
        if pg:
            return f"CAST('{v}' AS {typ.strip().lower()})"
        return f"CONVERT({typ.strip().lower()}, {source_io.quote_nstr(v)}, 126)"
    if not _INTEGER_VALUE.match(v):
        raise ValueError(f"invalid rowversion cursor position {value!r}")
    return f"CAST(CAST({v} AS BIGINT) AS BINARY(8))"


def read_where(cfg: dict, typ: str, lo: Optional[str], hi: str, lookback_s: int) -> str:
    """``cursor_column > lo AND cursor_column <= hi`` (no lower bound when ``lo`` is None — a table
    that was empty at seed time). Sargable: the literal is cast to the column's type, not vice versa.

    A temporal cursor is assigned when the row is written, not when it commits, so a row can commit
    after a read whose ``hi`` is above it. The lower bound is pulled back by ``lookback_s`` for
    temporal kinds (the MERGE makes the re-read harmless); rowversion and identity stay exact (an
    identity is inserts-only: a late commit below ``hi`` is lost whatever the lookback)."""
    col, pg = _col(cfg), _is_pg(cfg)
    upper = f"{col} <= {literal(hi, typ, pg)}"
    if lo is None:
        return upper
    if _kind(typ) == "temporal":
        lo = _minus_seconds(lo, lookback_s, typ)
    return f"{col} > {literal(lo, typ, pg)} AND {upper}"


def lookback_seconds(cfg: dict, default_s: int) -> int:
    """The row's ``cursor_lookback_s`` when set, else the job parameter."""
    row = cfg.get("cursor_lookback_s")
    return default_s if row is None else int(row)


def seed_position(typ: str, hi: Optional[str], seed_seconds: float) -> Optional[str]:
    """The position to record after a seed. ``hi`` was captured before the copy; a temporal cursor
    can still gain rows BELOW ``hi`` that commit while the copy runs (their timestamp predates the
    capture), so the recorded position is pulled back by the seed's duration and the first increment
    re-reads that span. Numeric and rowversion keep ``hi``."""
    if hi is None or _kind(typ) != "temporal":
        return hi
    return _minus_seconds(hi, int(seed_seconds) + 1, typ)


def _trim_fraction(value: str, typ: str) -> str:
    """At most 3 fractional digits for datetime / smalldatetime, else 6 (``fromisoformat`` and
    PostgreSQL take at most 6; a 100 ns loss on a bound that is already pulled back by minutes)."""
    digits = 3 if (typ or "").strip().lower() in MILLISECOND_TYPES else 6
    return re.sub(r"(\.\d{%d})\d+" % digits, r"\1", value)


def _minus_seconds(value: str, seconds: int, typ: str = "") -> str:
    """``value`` (ISO 8601, as the source returned it) minus ``seconds`` (negative: plus), in the same
    ISO form, with the fraction trimmed for ``typ`` (``_trim_fraction``). A ``date`` column gains at
    most one day from this (a date carries no commit order anyway)."""
    v = str(value).strip()
    if not _TEMPORAL_VALUE.match(v):
        raise ValueError(f"invalid temporal cursor position {value!r}")
    v = _trim_fraction(v, "").replace("Z", "+00:00")
    return _trim_fraction((datetime.fromisoformat(v) - timedelta(seconds=seconds)).isoformat(), typ)


def _parse(kind: str, value: str):
    """A comparable Python value for a position / watermark text of ``kind``."""
    if kind == "temporal":
        return datetime.fromisoformat(_trim_fraction(str(value).strip(), "").replace("Z", "+00:00"))
    return Decimal(str(value).strip())


def upsert_merge_sql(cfg: dict, sink_fqn: str, view: str) -> str:
    """Idempotent upsert MERGE by PK — (__client_id__, PK) for a consolidated sink — with schema
    evolution. A watermark read carries no deletes."""
    sl, keys = ", ".join(sink_writer.write_columns(cfg)), ", ".join(sink_writer.merge_keys(cfg))
    return (
        f"MERGE WITH SCHEMA EVOLUTION INTO {sink_fqn} AS tgt USING {view} AS s "
        f"ON {sink_writer.merge_on_cfg(cfg)} "
        f"WHEN MATCHED THEN UPDATE SET {sink_writer.merge_set(sl, keys)} "
        f"WHEN NOT MATCHED THEN INSERT ({sink_writer.merge_insert_cols(sl)}) "
        f"VALUES ({sink_writer.merge_insert_vals(sl)})"
    )


# ---- source access ---------------------------------------------------------------------------------

def column_type(cfg: dict, slots=None) -> str:
    """The cursor column's SQL Server type; raises a clear error if missing, unsupported or nullable.

    A nullable cursor column is refused: incremental reads select ``col > lo AND col <= hi``, so a
    row whose cursor value is NULL would never be read (the seed copies it, later changes to it
    would be silently missed)."""
    with source_io._unit(slots, cfg):
        row = source_io._read(source_io._spark(), cfg, column_type_sql(cfg)).first()
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


def high_watermark(cfg: dict, typ: str, skew_s: int, slots=None) -> Tuple[Optional[str], bool]:
    """``(hi, alert)``: the high watermark as source text (None on an empty table). A temporal ``hi``
    is capped at the source clock plus ``skew_s`` — a row stamped in the future would otherwise pin
    the window above every later row — and ``alert`` (``max_above_clock``) says the cap applied; the
    capped rows are read once the clock passes them."""
    with source_io._unit(slots, cfg):
        row = source_io._read(source_io._spark(), cfg, high_watermark_sql(cfg, typ)).first()
    if row is None or row["hi"] is None:
        return None, False
    _refuse_nullable(cfg, row["nullable"])
    hi = str(row["hi"])
    if _kind(typ) != "temporal":
        return hi, False
    cap = _minus_seconds(str(row["src_now"]), -int(skew_s), typ)
    if _parse("temporal", hi) > _parse("temporal", cap):
        return cap, True
    return hi, False


def read_and_merge(spark, cfg: dict, sink_fqn: str, typ: str, lo: Optional[str], hi: str,
                   lookback_s: int, slots=None) -> Optional[int]:
    """Read (lo, hi] and upsert-MERGE it into the sink — ONE source read feeding the MERGE (no
    ``.cache()``/``count()``). Returns rows affected from the MERGE metrics (None if unavailable)."""
    query = source_adapters.for_cfg(cfg).select_sql(cfg, where=read_where(cfg, typ, lo, hi, lookback_s))
    def once():
        with source_io._unit(slots, cfg):
            df = sink_writer._tag(source_io._read(spark, cfg, query), cfg)
            view = f"cursor_staged_{uuid.uuid4().hex}"
            df.createOrReplaceTempView(view)
            try:
                return spark.sql(upsert_merge_sql(cfg, sink_fqn, view)).first()
            finally:
                spark.catalog.dropTempView(view)
    result = sink_writer.with_write_retry(once, what=f"merge {sink_fqn}")
    if result is not None and "num_affected_rows" in result.asDict():
        return int(result["num_affected_rows"] or 0)
    return None


def plan_increment(position: dict, hi: Optional[str]) -> Tuple[str, Optional[str], Optional[str]]:
    """('unchanged' | 'read', new position value, note). An empty source (hi NULL) means nothing to
    read; the position is kept (never moved to NULL). hi equal to the checkpoint is unchanged for
    identity and rowversion kinds; a temporal cursor still reads its lookback window, because a row
    can commit late BELOW the high watermark without moving it (a quiet table would otherwise hold
    that row until its next change). The note is ``inserts_only`` for an identity cursor (the worker
    logs it once per run per table). A ``hi`` below the position is a source truncate or reload:
    the table fails and the position stays."""
    kind = _kind(position.get("type"))
    note = "inserts_only" if kind == "identity" else None
    value = position.get("value")
    if hi is None:
        return "unchanged", value, note
    if value is not None and _parse(kind, hi) < _parse(kind, value):
        raise ValueError(f"high watermark {hi} of cursor_column '{position.get('col')}' is below the checkpoint "
                         f"position {value}: a source truncate or reload? The position stays; reseed the "
                         "table to start over (enqueue a reseed)")
    if hi == value and kind != "temporal":
        return "unchanged", value, note
    return "read", hi, note
