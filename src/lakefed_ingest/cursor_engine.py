"""Cursor strategy: incremental reads above a high watermark, merged into the Delta sink by primary key.

The watermark is ``control.cursor_expression``: one SQL expression over one to three source columns
of one type, folded with GREATEST over COALESCE inputs. ``control.cursor_kind`` names its clock:
``rowversion``, ``db_timestamp``, ``app_timestamp``, or ``identity``. Every dialect difference is a
``SourceAdapter`` method. This module never branches on ``src_type``.

One cycle of a seeded table (checkpoint phase ``incremental``):

1. ``hi`` is the current value of the expression. It comes from the batched capture
   (``capture_batch``, one ``max_batch_sql`` query per source database and cursor type) or from
   ``high_watermark`` (one query per table).
2. A temporal ``hi`` is capped at the source clock plus ``skew_s``. The cap raises the
   ``max_above_clock`` alert.
3. ``plan_increment`` decides: unchanged, or read. A temporal kind always reads, because ``lo`` is
   the position minus ``lookback_s``. An identity or rowversion kind reads exact bounds.
4. The read is ``expression > lo AND expression <= hi``, one source query. An upsert MERGE by
   primary key applies it. A matched row is updated only when its expression changed.
5. The checkpoint advances to ``hi`` after the MERGE commits. Delivery is at-least-once. The MERGE
   is idempotent.

The seed captures ``hi`` before the full copy. The first increment reads the rows written during
the seed again. An identity value is set at insert time, so a late commit below ``hi`` is lost. The
kind is inserts-only, and ``plan_increment`` returns the note ``inserts_only``. A ``hi`` below the
position fails the table. The position never moves backwards.

``validate_expression`` checks a row before any source read: one to three columns, GREATEST and
COALESCE only, one shared type, and no nullable input unless ``null_cursor_policy`` is
``seed_only``. The seed copies a row with a NULL expression. No increment reads it again.

``delete_missing`` runs the ``pk_snapshot`` delete mode: read every source primary key once per
``delete_min_interval_s``, then delete the sink rows absent from it, scoped to ``client_id``.

The position (``checkpoint.position``) is JSON ``{"expr", "kind", "type", "value"}``. See
contracts/position.md. ``encode_position`` and ``decode_position`` own the format. A position with
a different ``expr`` or ``kind`` than the control row reseeds the table. ``value`` is ISO 8601 for a
temporal type and decimal text otherwise. A ``datetime`` or ``smalldatetime`` literal carries at most
three fractional digits.

Pure builders are unit-tested in tests/cursor_engine_test.py against the SQL Server, Oracle, and
PostgreSQL adapters. Source access reuses ``source_io``. Sink writes reuse ``sink_writer``.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import sink_writer
import source_adapters
import source_io

CURSOR_KINDS = ("rowversion", "db_timestamp", "app_timestamp", "identity")
NULL_CURSOR_POLICIES = ("reject", "seed_only")
_MILLISECOND_TYPES = ("datetime", "smalldatetime")

_NUMERIC_VALUE = re.compile(r"^-?\d+(\.\d+)?$")
_TEMPORAL_VALUE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2}(\.\d{1,7})?)?)?([+-]\d{2}:\d{2}|Z)?$")
_INTEGER_VALUE = re.compile(r"^-?\d+$")
_VALUE_SHAPES = {"identity": (_NUMERIC_VALUE, "numeric"), "temporal": (_TEMPORAL_VALUE, "temporal"),
                 "rowversion": (_INTEGER_VALUE, "rowversion")}


def _kind(cfg: dict, typ: str) -> str:
    """Cursor kind of a source type (``SourceAdapter.type_class_of``): the ``numeric`` class is the
    ``identity`` kind (inserts-only semantics), ``temporal`` and ``rowversion`` keep their names."""
    cls = source_adapters.for_cfg(cfg).type_class_of(typ)
    return "identity" if cls == "numeric" else cls


# ---- position --------------------------------------------------------------------------------------

def encode_position(expr: str, kind: str, typ: str, value: Optional[str]) -> str:
    """contracts/position.md. A ``datetime`` / ``smalldatetime`` value carries at most three fractional
    digits (the source holds no more)."""
    if value is not None and str(typ).lower() in _MILLISECOND_TYPES:
        value = source_adapters.trim_fraction(str(value), 3)
    return json.dumps({"expr": expr, "kind": kind, "type": typ, "value": value}, separators=(",", ":"))


def decode_position(text: Optional[str]) -> Optional[dict]:
    """{"expr", "kind", "type", "value"} or None (no / unreadable position, or the old ``{col, ...}``
    shape -> the planner re-seeds)."""
    if not text:
        return None
    try:
        d = json.loads(text)
    except (TypeError, ValueError):
        return None
    if not isinstance(d, dict) or not all(d.get(k) for k in ("expr", "kind", "type")):
        return None
    return {"expr": d["expr"], "kind": d["kind"], "type": d["type"], "value": d.get("value")}


# ---- the expression (pure) -------------------------------------------------------------------------

_TOKEN, _KEYWORDS = sink_writer.EXPRESSION_TOKEN, sink_writer.EXPRESSION_KEYWORDS


def expression_columns(expression: str) -> List[str]:
    """The source columns an expression references, as written, each once (case-insensitive)."""
    text = expression or ""
    out: List[str] = []
    for m in _TOKEN.finditer(text):
        name = next((g for g in m.groups() if g is not None), None)
        if name is None:   # a string literal
            continue
        if m.group(3) and (name.upper() in _KEYWORDS or text[m.end():].lstrip().startswith("(")):
            continue
        if name.lower() not in {c.lower() for c in out}:
            out.append(name)
    return out


def null_cursor_policy(cfg: dict) -> str:
    return str(cfg.get("null_cursor_policy") or "reject").strip().lower()


def validate_expression(cfg: dict, column_types: Optional[Dict[str, Tuple[str, bool]]] = None) -> Optional[str]:
    """Reject a bad ``cursor_expression`` with a message that names the problem (FR-011, FR-012,
    FR-015, FR-009): only GREATEST / COALESCE, one to three columns, a ``rowversion`` kind over one
    column. With ``column_types`` = {column: (type_name, nullable)} from ``column_type_sql``, also: every
    column exists, all share one type, and no input is nullable under ``null_cursor_policy`` ``reject``.
    Returns the shared type name (lower case), or None for the pure checks alone."""
    expr, where = cfg.get("cursor_expression") or "", f"{cfg['src_schema']}.{cfg['src_table']}"
    bad = sorted(set(sink_writer.expression_functions(expr)) - sink_writer.SINK_FUNCTIONS)
    if bad:
        raise ValueError(f"cursor_expression on {where} uses {', '.join(bad)}: only GREATEST and COALESCE are allowed")
    cols = expression_columns(expr)
    if not 1 <= len(cols) <= 3:
        raise ValueError(f"cursor_expression on {where} references {len(cols)} columns; it must reference 1 to 3")
    if str(cfg.get("cursor_kind") or "").lower() == "rowversion" and len(cols) > 1:
        raise ValueError(f"cursor_kind rowversion on {where} takes one column; the expression references {len(cols)}")
    if column_types is None:
        return None
    types = {c.lower(): (str(t).lower(), bool(n)) for c, (t, n) in column_types.items()}
    missing = [c for c in cols if c.lower() not in types]
    if missing:
        raise ValueError(f"cursor_expression column '{missing[0]}' not found on {where}")
    by_type = {}
    for c in cols:
        by_type.setdefault(types[c.lower()][0], c)
    if len(by_type) > 1:
        raise ValueError(f"cursor_expression on {where} mixes types: "
                         + ", ".join(f"{c} {t}" for t, c in by_type.items()) + "; all columns must share one type")
    nullable = [c for c in cols if types[c.lower()][1]]
    if nullable and null_cursor_policy(cfg) == "reject":
        raise ValueError(f"cursor_expression input '{nullable[0]}' on {where} is nullable: rows with a NULL value would "
                         "never be read incrementally. Wrap it in COALESCE, or set null_cursor_policy seed_only")
    typ = next(iter(by_type))
    _kind(cfg, typ)   # a supported cursor type
    return typ


def _split_args(body: str) -> Optional[List[str]]:
    """Top-level comma-separated arguments of one call body; None when ``body`` is not one call."""
    args, depth, start, quoted = [], 0, 0, False
    for i, ch in enumerate(body):
        if ch == "'":
            quoted = not quoted
        elif quoted:
            continue
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return None
        elif ch == "," and depth == 0:
            args.append(body[start:i].strip())
            start = i + 1
    args.append(body[start:].strip())
    return args


# ---- SQL builders (pure; every dialect difference is a SourceAdapter method) ----------------------

def _expr(cfg: dict, typ: str, server_version=None) -> str:
    """``cursor_expression`` for the source. A top-level GREATEST goes through the adapter
    (``greatest_expr``: SQL Server before 2022 has none, so it becomes a CASE); anything else is the
    operator's text as written."""
    # shortcut: only an outermost GREATEST is rewritten; a nested one stays, upgrade when a source needs it
    text = str(cfg["cursor_expression"]).strip()
    m = re.match(r"^GREATEST\s*\((.*)\)$", text, re.I | re.S)
    args = _split_args(m.group(1)) if m else None
    if args is None:
        return text
    return source_adapters.for_cfg(cfg).greatest_expr(args, typ, server_version)


def column_type_sql(cfg: dict) -> str:
    """One row ``column_name``, ``type_name``, ``is_nullable`` (0/1) per column the expression
    references (no row for a column that doesn't exist)."""
    return source_adapters.for_cfg(cfg).cursor_type_sql(cfg["src_schema"], cfg["src_table"],
                                                        expression_columns(cfg["cursor_expression"]))


def nullable_sql(cfg: dict) -> str:
    """Scalar subquery: 1 if any referenced column is nullable (checked on every run, not just at
    seed — a column can be altered to NULL after the table was seeded)."""
    alias = source_adapters.for_cfg(cfg).result_alias("is_nullable")
    return f"(SELECT MAX({alias}) FROM ({column_type_sql(cfg)}) t)"


def high_watermark_sql(cfg: dict, typ: str, server_version=None) -> str:
    """One row ``hi``, ``nullable`` (and ``src_now`` for a temporal kind): the expression's high
    watermark as canonical source text (NULL on an empty table), its inputs' nullability, and the
    source clock in the same rendering as ``hi`` (one query per run for all). The adapter renders the
    text (``max_text_sql``: ISO 8601 for temporal, decimal text otherwise; SQL Server caps a rowversion
    below MIN_ACTIVE_ROWVERSION() so open transactions are never skipped)."""
    a = source_adapters.for_cfg(cfg)
    src_now = ""
    if _kind(cfg, typ) == "temporal":
        src_now = f", {a.cursor_text_sql(a.source_now_sql(typ), typ)} AS {a.result_alias('src_now')}"
    # Scalar subqueries, no aggregate in the outer select: Oracle rejects a subquery or a clock
    # beside MAX() with no GROUP BY (ORA-00937). Same shape as max_batch_sql.
    return (f"SELECT (SELECT {a.max_text_sql(_expr(cfg, typ, server_version), typ)} "
            f"FROM {a.table_ref(cfg['src_schema'], cfg['src_table'])}) AS {a.result_alias('hi')}, "
            f"{nullable_sql(cfg)} AS {a.result_alias('nullable')}{src_now}{a.scalar_from}")


def literal(cfg: dict, value: str, typ: str) -> str:
    """A typed source literal for a position value (validated — values come from our own
    checkpoint, but are never spliced unchecked)."""
    pattern, what = _VALUE_SHAPES[_kind(cfg, typ)]
    v = str(value).strip()
    if not pattern.match(v):
        raise ValueError(f"invalid {what} cursor position {value!r}")
    return source_adapters.for_cfg(cfg).cursor_literal(v, typ)


def read_where(cfg: dict, typ: str, lo: Optional[str], hi: str, lookback_s: int, server_version=None) -> str:
    """``expr > lo AND expr <= hi`` (no lower bound when ``lo`` is None — a table that was empty at
    seed time). Sargable: the literal is cast to the expression's type, not vice versa. A row whose
    expression is NULL is never in the window (``seed_only``: the seed copied it).

    A temporal cursor is assigned when the row is written, not when it commits, so a row can commit
    after a read whose ``hi`` is above it. The lower bound is pulled back by ``lookback_s`` for
    temporal kinds (the MERGE makes the re-read harmless); rowversion and identity stay exact (an
    identity is inserts-only: a late commit below ``hi`` is lost whatever the lookback)."""
    col = _expr(cfg, typ, server_version)
    upper = f"{col} <= {literal(cfg, hi, typ)}"
    if lo is None:
        return upper
    if _kind(cfg, typ) == "temporal":
        lo = _minus_seconds(lo, lookback_s)
    return f"{col} > {literal(cfg, lo, typ)} AND {upper}"


def lookback_seconds(cfg: dict, default_s: int) -> int:
    """The row's ``cursor_lookback_s`` when set, else the job parameter."""
    row = cfg.get("cursor_lookback_s")
    return default_s if row is None else int(row)


def seed_position(cfg: dict, typ: str, hi: Optional[str], seed_seconds: float) -> Optional[str]:
    """The position to record after a seed. ``hi`` was captured before the copy; a temporal cursor
    can still gain rows BELOW ``hi`` that commit while the copy runs (their timestamp predates the
    capture), so the recorded position is pulled back by the seed's duration and the first increment
    re-reads that span. Numeric and rowversion keep ``hi``."""
    if hi is None or _kind(cfg, typ) != "temporal":
        return hi
    return _minus_seconds(hi, int(seed_seconds) + 1)


def _minus_seconds(value: str, seconds: int) -> str:
    """``value`` (ISO 8601, as the source returned it) minus ``seconds`` (negative: plus), in the same
    ISO form with at most 6 fractional digits (``fromisoformat`` takes at most 6; a 100 ns loss on a
    bound that is already pulled back by minutes; the adapter's literal trims further for its type).
    A ``date`` column gains at most one day from this (a date carries no commit order anyway)."""
    v = str(value).strip()
    if not _TEMPORAL_VALUE.match(v):
        raise ValueError(f"invalid temporal cursor position {value!r}")
    return (_parse("temporal", v) - timedelta(seconds=seconds)).isoformat()


def _parse(kind: str, value: str):
    """A comparable Python value for a position / watermark text of ``kind``."""
    if kind == "temporal":
        return datetime.fromisoformat(source_adapters.trim_fraction(str(value).strip(), 6).replace("Z", "+00:00"))
    return Decimal(str(value).strip())


def upsert_merge_sql(cfg: dict, sink_fqn: str, view: str) -> str:
    """Idempotent upsert MERGE by PK — (__client_id__, PK) for a consolidated sink — with schema
    evolution. A matched row is updated only when its cursor value moved (FR-026: a lookback re-read
    rewrites nothing). A watermark read carries no deletes."""
    sl, keys = ", ".join(sink_writer.write_columns(cfg)), ", ".join(sink_writer.merge_keys(cfg))
    expr = cfg["cursor_expression"]
    return (
        f"MERGE WITH SCHEMA EVOLUTION INTO {sink_fqn} AS tgt USING {view} AS s "
        f"ON {sink_writer.merge_on_cfg(cfg)} "
        f"WHEN MATCHED AND {sink_writer.sink_expr(expr, 's')} <> {sink_writer.sink_expr(expr, 'tgt')} "
        f"THEN UPDATE SET {sink_writer.merge_set(sl, keys)} "
        f"WHEN NOT MATCHED THEN INSERT ({sink_writer.merge_insert_cols(sl)}) "
        f"VALUES ({sink_writer.merge_insert_vals(sl)})"
    )


def changed_rows_sql(cfg: dict, sink_fqn: str, view: str) -> str:
    """One row when any window row is absent from the sink or carries another cursor value (the rows
    the MERGE would write), else none. Null-safe on the cursor value."""
    expr = cfg["cursor_expression"]
    return (f"SELECT 1 FROM {view} AS s LEFT ANTI JOIN {sink_fqn} AS tgt "
            f"ON {sink_writer.merge_on_cfg(cfg)} "
            f"AND {sink_writer.sink_expr(expr, 's')} <=> {sink_writer.sink_expr(expr, 'tgt')} LIMIT 1")


# ---- source access ---------------------------------------------------------------------------------

def column_type(cfg: dict, slots=None) -> str:
    """The expression's source type name (``validate_expression`` over the referenced columns' types);
    raises a clear error for a missing, unsupported or mixed-type column, or a nullable input under
    ``null_cursor_policy`` ``reject``: incremental reads select ``expr > lo AND expr <= hi``, so a row
    whose expression is NULL would never be read (the seed copies it, later changes to it would be
    silently missed)."""
    with source_io._unit(slots, cfg):
        rows = source_io._read(source_io._spark(), cfg, column_type_sql(cfg)).collect()
    types = {str(r["column_name"]): (str(r["type_name"]), bool(r["is_nullable"]))
             for r in rows if r["type_name"] is not None}
    return validate_expression(cfg, types)


def _refuse_nullable(cfg: dict, nullable) -> None:
    if nullable and null_cursor_policy(cfg) == "reject":
        raise ValueError(f"a cursor_expression input on {cfg['src_schema']}.{cfg['src_table']} is nullable: rows with a "
                         "NULL value would never be read incrementally. Wrap it in COALESCE, or set "
                         "null_cursor_policy seed_only")


def high_watermark(cfg: dict, typ: str, skew_s: int, slots=None) -> Tuple[Optional[str], bool]:
    """``(hi, alert)``: the high watermark as source text (None on an empty table). A temporal ``hi``
    is capped at the source clock plus ``skew_s`` — a row stamped in the future would otherwise pin
    the window above every later row — and ``alert`` (``max_above_clock``) says the cap applied; the
    capped rows are read once the clock passes them."""
    version = source_io.server_version(cfg, slots)
    with source_io._unit(slots, cfg):
        row = source_io._read(source_io._spark(), cfg, high_watermark_sql(cfg, typ, version)).first()
    if row is None or row["hi"] is None:
        return None, False
    _refuse_nullable(cfg, row["nullable"])
    return _capped(cfg, typ, str(row["hi"]), row.get("src_now") if hasattr(row, "get") else row["src_now"], skew_s)


def _capped(cfg: dict, typ: str, hi: str, src_now, skew_s: int) -> Tuple[str, bool]:
    """``(hi, alert)``: a temporal ``hi`` capped at ``src_now`` plus ``skew_s``."""
    if _kind(cfg, typ) != "temporal":
        return hi, False
    cap = _minus_seconds(str(src_now), -int(skew_s))
    if _parse("temporal", hi) > _parse("temporal", cap):
        return cap, True
    return hi, False


def capture_batch(tables, skew_s: int, chunk: int, slots=None, log=print) -> Dict[int, Tuple[Optional[str], bool]]:
    """``{control id: (hi, alert)}`` for the cursor tables of ONE source database (FR-023, research
    R-06): one ``max_batch_sql`` query per ``chunk`` tables that share a cursor type, with the source
    clock from the same row. A failed chunk falls back to ``high_watermark`` per table; a table whose
    fallback fails is left out, logged, and read again by its worker, which reports the failure.
    ``tables`` = [(cfg, type name)]. The batch skips the per-table nullable check; the fallback and
    the seed keep it."""
    out: Dict[int, Tuple[Optional[str], bool]] = {}
    by_type: Dict[str, list] = {}
    for cfg, typ in tables:
        by_type.setdefault(str(typ).lower(), []).append((cfg, typ))
    for group in by_type.values():
        for start in range(0, len(group), max(int(chunk), 1)):
            part = group[start:start + max(int(chunk), 1)]
            cfg0, typ0 = part[0]
            temporal = _kind(cfg0, typ0) == "temporal"
            version = source_io.server_version(cfg0, slots)
            a = source_adapters.for_cfg(cfg0)
            sql = a.max_batch_sql([(f"c{i}", c["src_schema"], c["src_table"], _expr(c, t, version))
                                   for i, (c, t) in enumerate(part)], typ0)
            try:
                with source_io._unit(slots, cfg0):
                    row = source_io._read(source_io._spark(), cfg0, sql).first()
                src_now = row["src_now"] if temporal else None
                for i, (c, t) in enumerate(part):
                    hi = row[f"c{i}"]
                    out[c["id"]] = (None, False) if hi is None else _capped(c, t, str(hi), src_now, skew_s)
            except Exception as e:
                log(f"[capture] {cfg0.get('src_database')}: batch of {len(part)} tables failed ({str(e)[:200]}); "
                    "reading each table's MAX on its own")
                for c, t in part:
                    try:
                        out[c["id"]] = high_watermark(c, t, skew_s, slots)
                    except Exception as te:
                        log(f"[capture] {c.get('src_database')}.{c['src_schema']}.{c['src_table']}: MAX read failed, "
                            f"the worker reads it again: {str(te)[:200]}")
    return out


def read_and_merge(spark, cfg: dict, sink_fqn: str, typ: str, lo: Optional[str], hi: str,
                   lookback_s: int, slots=None) -> Optional[int]:
    """Read (lo, hi] and upsert-MERGE it into the sink — ONE source read feeding the MERGE (no
    ``.cache()``). A window with nothing to write (``changed_rows_sql``: the lookback re-reads rows
    the sink already holds) skips the MERGE and returns 0, because Delta records an empty MERGE as a
    new table version and a quiet table would grow one per run. The probe is a second source read of
    the same window, bounded to one row. Returns rows affected from the MERGE metrics (None if
    unavailable)."""
    version = source_io.server_version(cfg, slots)
    query = source_adapters.for_cfg(cfg).select_sql(cfg, where=read_where(cfg, typ, lo, hi, lookback_s, version))
    def once():
        with source_io._unit(slots, cfg):
            df = sink_writer._tag(source_io._read(spark, cfg, query), cfg)
            view = f"cursor_staged_{uuid.uuid4().hex}"
            df.createOrReplaceTempView(view)
            try:
                if spark.sql(changed_rows_sql(cfg, sink_fqn, view)).first() is None:
                    return _EMPTY_WINDOW
                return spark.sql(upsert_merge_sql(cfg, sink_fqn, view)).first()
            finally:
                spark.catalog.dropTempView(view)
    result = sink_writer.with_write_retry(once, what=f"merge {sink_fqn}")
    if result is _EMPTY_WINDOW:
        return 0
    if result is not None and "num_affected_rows" in result.asDict():
        return int(result["num_affected_rows"] or 0)
    return None


_EMPTY_WINDOW = object()


def delete_missing(spark, cfg: dict, sink_fqn: str, slots=None) -> Optional[int]:
    """Delete-mode ``pk_snapshot`` (FR-028): read every source primary key and delete the sink rows
    (this client's, when ``client_id`` is set) that have none — ONE source read feeding one DELETE,
    under one slot and one source permit like any read. Returns rows deleted from the Delta metrics
    (None if unavailable)."""
    def once():
        with source_io._unit(slots, cfg):
            view = f"pk_snapshot_{uuid.uuid4().hex}"
            source_io.read_pk_snapshot(spark, cfg).createOrReplaceTempView(view)
            try:
                return spark.sql(sink_writer.delete_missing_sql(sink_fqn, view, cfg)).first()
            finally:
                spark.catalog.dropTempView(view)
    result = sink_writer.with_write_retry(once, what=f"delete {sink_fqn}")
    if result is not None and "num_affected_rows" in result.asDict():
        return int(result["num_affected_rows"] or 0)
    return None


def plan_increment(cfg: dict, position: dict, hi: Optional[str]) -> Tuple[str, Optional[str], Optional[str]]:
    """('unchanged' | 'read', new position value, note). An empty source (hi NULL) means nothing to
    read; the position is kept (never moved to NULL). hi equal to the checkpoint is unchanged for
    identity and rowversion kinds; a temporal cursor still reads its lookback window, because a row
    can commit late BELOW the high watermark without moving it (a quiet table would otherwise hold
    that row until its next change). The note is ``inserts_only`` for an identity cursor (the worker
    logs it once per run per table). A ``hi`` below the position is a source truncate or reload:
    the table fails and the position stays."""
    kind = _kind(cfg, position.get("type"))
    note = "inserts_only" if kind == "identity" else None
    value = position.get("value")
    if hi is None:
        return "unchanged", value, note
    if value is not None and _parse(kind, hi) < _parse(kind, value):
        raise ValueError(f"high watermark {hi} of cursor_expression '{position.get('expr')}' is below the checkpoint "
                         f"position {value}: a source truncate or reload? The position stays; reseed the "
                         "table to start over (enqueue a reseed)")
    if hi == value and kind != "temporal":
        return "unchanged", value, note
    return "read", hi, note
