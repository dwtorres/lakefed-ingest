"""Load strategies: HOW a table is kept in sync (``control.load_type``), independent of the source
engine (``control.src_type`` -> ``source_adapters``).

Each strategy owns the meaning of the generic checkpoint row (``lakefed_ingest.checkpoint``):
``strategy`` names it and ``position`` (text) is its opaque resume position, encoded and decoded
only by the strategy that wrote it.

=========  ============================================================  ==========================
load_type  behaviour                                                     position
=========  ============================================================  ==========================
cursor     High-watermark column (every adapter with cursor methods:        JSON {col, type, value}
           SQL Server, Oracle, PostgreSQL): seed, then (position, hi]
           reads upsert-MERGEd by PK; no deletes
full       Reload the whole table every cycle (data-only atomic replace;  NULL (not resumable)
           partitioned tables use the staged swap). No PK required
=========  ============================================================  ==========================

``supported(src_type, load_type)`` is the support matrix the planner enforces: a control row whose
pair isn't supported fails on its own (clear message, stage ``plan``) without affecting the run.

Pure Python — unit-tested in tests/load_strategies_test.py.
"""

from __future__ import annotations

from typing import Optional

import source_adapters as sa

FULL, CURSOR = "full", "cursor"
LOAD_TYPES = (CURSOR, FULL)


class LoadStrategy:
    name: str = ""
    requires_pk: bool = False
    implemented: bool = False

    def encode_position(self, value) -> Optional[str]:
        return None if value is None else str(value)

    def decode_position(self, text: Optional[str]):
        return text


class FullStrategy(LoadStrategy):
    """Full reload every cycle: one JDBC read streams into a data-only ``INSERT OVERWRITE`` (or the
    staged, atomic partitioned swap when ``load_partitioned``). The table definition (id, grants,
    clustering, properties) is preserved. No PK and no position."""

    name = FULL
    implemented = True

    def load(self, spark, cfg: dict, sink_fqn: str, slots=None, executor=None, guard=None):
        import sink_writer
        sink_writer.ensure_sink_table(spark, cfg, sink_fqn, slots=slots)
        return sink_writer.seed(spark, cfg, sink_fqn, slots=slots, executor=executor, guard=guard)


class CursorStrategy(LoadStrategy):
    """Cursor (high-watermark) increments (``cursor_engine``) for every adapter that implements the
    cursor methods of ``SourceAdapter`` (SQL Server, Oracle, PostgreSQL; MySQL raises).

    * control: ``cursor_expression`` (one to three columns of one monotonic type: rowversion, identity,
      or an application-maintained updated_at, folded with GREATEST / COALESCE), ``cursor_kind`` and ``primary_key`` (rows are MERGEd by PK; a DB constraint enforces both);
    * first cycle: capture ``hi`` BEFORE a full seed, checkpoint position = hi;
    * each cycle: ``hi'``; equal -> unchanged, else read ``(position, hi']`` (one source read),
      upsert-MERGE by PK, then advance to hi' (at-least-once; the MERGE is idempotent);
    * position = JSON ``{expr, kind, type, value}`` (``cursor_engine.encode_position``) — a changed
      expression or kind re-seeds;
    * deletes are NOT captured (use a periodic ``full``).
    """

    name = CURSOR
    requires_pk = True
    implemented = True

    def encode_position(self, value):
        return value                      # already encoded by cursor_engine.encode_position

    def decode_position(self, text):
        import cursor_engine
        return cursor_engine.decode_position(text)


_STRATEGIES = {s.name: s for s in (FullStrategy(), CursorStrategy())}

def _implements_cursor(adapter: sa.SourceAdapter) -> bool:
    return type(adapter).cursor_type_sql is not sa.SourceAdapter._cursor_not_implemented


# (src_type, load_type) pairs that run today. FULL works for every adapter; CURSOR runs where the
# adapter implements the cursor methods. PLACEHOLDER pairs are planned (clear "not implemented"
# error); anything else is "not supported".
SUPPORT = {(st, FULL) for st in sa.SRC_TYPES} | {
    (st, CURSOR) for st in sa.SRC_TYPES if _implements_cursor(sa.get_adapter(st))}
PLACEHOLDER = {(st, CURSOR) for st in sa.SRC_TYPES} - SUPPORT


def get_strategy(load_type: Optional[str]) -> LoadStrategy:
    """Strategy for ``control.load_type`` (NULL/empty -> cursor, the column default)."""
    key = (load_type or CURSOR).strip().lower()
    try:
        return _STRATEGIES[key]
    except KeyError:
        raise ValueError(
            f"unsupported load_type {load_type!r}; supported: {', '.join(LOAD_TYPES)}") from None


def supported(src_type: Optional[str], load_type: Optional[str]) -> bool:
    return unsupported_reason(src_type, load_type) is None


def unsupported_reason(src_type: Optional[str], load_type: Optional[str]) -> Optional[str]:
    """None when the pair runs today, else a clear, per-table error message."""
    try:
        adapter = sa.get_adapter(src_type)
        strategy = get_strategy(load_type)
    except ValueError as e:
        return str(e)
    if (adapter.src_type, strategy.name) in SUPPORT:
        return None
    if (adapter.src_type, strategy.name) in PLACEHOLDER:
        return (f"load_type '{strategy.name}' is not implemented for src_type '{adapter.src_type}': "
                f"{type(adapter).__name__} raises NotImplementedError from its cursor methods")
    if not strategy.implemented:
        return f"load_type '{strategy.name}' is not implemented yet (src_type '{adapter.src_type}')"
    ok = sorted(lt for st, lt in SUPPORT if st == adapter.src_type)
    return (f"load_type '{strategy.name}' is not supported for src_type '{adapter.src_type}' "
            f"(supported: {', '.join(ok)})")


def support_matrix() -> dict:
    """{src_type: {load_type: 'yes' | 'placeholder' | 'no'}} — for docs and tests."""
    out = {}
    for st in sa.SRC_TYPES:
        out[st] = {}
        for lt in LOAD_TYPES:
            if (st, lt) in SUPPORT:
                out[st][lt] = "yes"
            elif (st, lt) in PLACEHOLDER or not _STRATEGIES[lt].implemented:
                out[st][lt] = "placeholder"
            else:
                out[st][lt] = "no"
    return out
