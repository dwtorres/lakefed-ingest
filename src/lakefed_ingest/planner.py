"""Run planning: strategy routing. Pure and unit-testable (no Spark, no DB).

* ``route_tables``: per control row, by its load strategy (``load_type``) and source adapter
  (``src_type``). An unsupported pair, or a placeholder strategy, routes to ``unsupported``: the
  row fails on its own with a clear message. ``full`` routes to ``full`` (reload every cycle).
  ``cursor`` routes to ``cursor_seed``, ``schema_reseed`` or ``cursor`` (per-table high-watermark
  increment).
* ``retry_plan``: the next attempt of a table after a transient failure, from its fresh checkpoint.
* ``apply_seed_budget`` and ``bulk_rows_to_write``: scale controls.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple

import cursor_engine
import load_strategies as ls
import sink_writer
import source_io
import state_store as ss


def position_from(plan: dict):
    """The resume position a worker starts from (telemetry ``position_from``): the cursor value for
    a cursor increment, else None (full reload, seeds and reseeds)."""
    if plan["route"] == "cursor":
        return plan["position"]["value"]
    return None


def sink_key(cfg: dict) -> tuple:
    return tuple(str(cfg.get(k) or "").lower() for k in ("sink_catalog", "sink_schema", "sink_table"))


def config_error(cfg: dict, mixed_sinks=frozenset()) -> Optional[str]:
    """Sink-ownership problems that would let one row overwrite another's data — checked per table
    before any work: an invalid client_id, a consolidated row whose select_list already has a
    __client_id__ column, or a sink shared by a dedicated row and consolidated rows. Also a missing
    primary key for a strategy that MERGEs by it (the database CHECK constraints enforce this too;
    this gives a clear per-table error if a row ever gets past them), a load_type other than
    cursor or full, an invalid write_mode or
    client_attrs, client_attrs column names clashing with select_list, and a read through a
    connection without one."""
    try:
        strategy = ls.get_strategy(cfg.get("load_type"))
    except ValueError as e:
        return str(e)   # names the valid values: cursor, full
    if strategy.requires_pk and not source_io._split(cfg.get("primary_key")):
        return f"load_type {cfg.get('load_type') or ls.CURSOR} needs a primary_key (it MERGEs by it)"
    if source_io.uses_remote_query(cfg) and not cfg.get("src_connection"):
        return "use_remote_query reads through the Unity Catalog connection in src_connection, which is not set"
    try:
        cid = sink_writer.client_id(cfg)
        sink_writer.write_mode(cfg)
        attrs = [name.lower() for name, _ in sink_writer.client_attrs(cfg)]
    except ValueError as e:
        return str(e)
    selected = {c.lower() for c in source_io._split(cfg.get("select_list"))}
    if cid is not None and sink_writer.CLIENT_COLUMN in selected:
        return (f"select_list includes '{sink_writer.CLIENT_COLUMN}', which a consolidated sink (client_id set) "
                "adds itself; remove it from select_list")
    clash = [c for c in attrs if c in selected]
    if clash:
        return (f"select_list includes {', '.join(clash)}, which the sink adds itself (client_attrs); "
                "rename the client_attrs column or remove it from select_list")
    if mixed_sinks and sink_key(cfg) in mixed_sinks:
        return ("sink {}.{}.{} is claimed by a dedicated row (client_id NULL) and by other rows; a sink is "
                "either dedicated to one source table or shared by client_id".format(*sink_key(cfg)))
    return None


def route_tables(configs: List[dict], checkpoints: Dict[int, dict], mixed_sinks=frozenset(),
                 reseed_on_new_columns: bool = True) -> List[dict]:
    """One plan per control row, ready for the worker: unsupported (pair not in the support matrix,
    or a sink-ownership problem — see ``config_error``; carries ``error``), full (reload), or
    cursor_seed / schema_reseed / cursor (carries the decoded ``position``) for load_type cursor.

    ``reseed_on_new_columns`` False: a table whose select_list gained columns carries on from its
    checkpoint instead of reseeding (the writes add the columns; rows written before stay NULL in
    them)."""
    plans = []
    for cfg in configs:
        cp = checkpoints.get(int(cfg["id"]))
        reason = config_error(cfg, mixed_sinks) or ls.unsupported_reason(cfg.get("src_type"), cfg.get("load_type"))
        if reason:
            plans.append({"cfg": cfg, "route": "unsupported", "cp": cp, "error": reason})
        elif ss.strategy_name(cfg) == ls.FULL:
            plans.append({"cfg": cfg, "route": "full", "cp": cp})
        else:
            plans.append(_route_cursor(cfg, cp, reseed_on_new_columns))
    return plans


def _route_cursor(cfg: dict, cp: Optional[dict], reseed_on_new_columns: bool = True) -> dict:
    """Cursor tables: seed when there's no usable cursor checkpoint (none, phase seeding, another
    strategy's, or recorded for a different cursor_column); reseed when select_list gained columns
    (unless ``reseed_on_new_columns`` is off); else a per-table high-watermark increment."""
    pos = cursor_engine.decode_position(cp.get("position")) if cp else None
    if (cp is None or cp.get("phase") != ss.INCREMENTAL or (cp.get("strategy") or ls.CURSOR) != ls.CURSOR
            or pos is None or pos["col"].lower() != str(cfg.get("cursor_column") or "").lower()):
        return {"cfg": cfg, "route": "cursor_seed", "cp": cp}
    added = ss.select_list_added_columns(cfg["select_list"], cp.get("select_list"))
    if added and reseed_on_new_columns:
        return {"cfg": cfg, "route": "schema_reseed", "cp": cp, "added": added}
    return {"cfg": cfg, "route": "cursor", "cp": cp, "position": pos}


# ---- table retries ------------------------------------------------------------------------------------
def retry_plan(plan: dict, cp: Optional[dict]) -> dict:
    """The next attempt of a table whose attempt failed (``table_retry``): the same route on the fresh
    checkpoint ``cp``. Every write is idempotent (MERGE by key, replace) and the checkpoint advances
    only after the commit, so doing it again is harmless."""
    return dict(plan, cp=cp)


# ---- scale controls -----------------------------------------------------------------------------------
SEED_ROUTES = ("cursor_seed",)


def apply_seed_budget(plans: List[dict], max_seeds: int) -> Tuple[List[dict], List[dict]]:
    """(kept, deferred): at most ``max_seeds`` seeds (route cursor_seed) run in this run; the rest
    wait, untouched, for a later run (their checkpoints stay seeding). Onboarding many tables at
    once then takes several bounded runs, and the incremental work of tables already seeded keeps
    running in each. Seeds whose last attempt failed go last, so a few broken tables can't use up the
    budget every run. ``max_seeds`` 0 = no limit."""
    seeds = [p for p in plans if p["route"] in SEED_ROUTES]
    if not max_seeds or max_seeds <= 0 or len(seeds) <= max_seeds:
        return list(plans), []
    ordered = sorted(seeds, key=lambda p: (p.get("cp") or {}).get("status") == "failed")   # stable
    run = {id(p) for p in ordered[:max_seeds]}
    return ([p for p in plans if p["route"] not in SEED_ROUTES or id(p) in run],
            [p for p in seeds if id(p) not in run])


def bulk_rows_to_write(unchanged: list, checkpoints: Dict[int, dict], interval_s: float, now=None) -> list:
    """The unchanged rows the bulk advance has to write: rows = [(control_id, position, select_list|None)].
    With ``interval_s`` > 0, a row is left out when writing it would only refresh ``last_success_at``
    and that stamp is under ``interval_s`` old: the checkpoint is already at the captured position,
    ok, and has no select_list to record. At scale nearly every table is quiet nearly every run, so
    this removes most of a run's Lakebase writes; ``last_success_at`` of a quiet table is then at
    most ``interval_s`` stale. ``interval_s`` 0 = write every row (stamped every run)."""
    if not interval_s or interval_s <= 0:
        return list(unchanged)
    now = now or datetime.now(timezone.utc)
    out = []
    for row in unchanged:
        cid, cur, select_list = row[:3]
        cp = checkpoints.get(int(cid)) or {}
        stamp = cp.get("last_success_at")
        quiet = (ss.position_text(cp.get("position")) == ss.position_text(cur) and select_list is None
                 and cp.get("status") == "ok" and stamp is not None
                 and (now - stamp).total_seconds() < interval_s)
        if not quiet:
            out.append(row)
    return out
