"""Per-(tenant db, table) worker with Lakebase telemetry, moved out of ingest.ipynb.

Source reads live in source_io, sink writes in sink_writer; everything here is orchestration +
checkpointing + telemetry, keyed by the control id (cfg["id"]). Each table arrives with a ROUTE
decided by the planner:
  full             - load_type 'full': reload the whole table (data-only atomic replace; staged
                     swap when partitioned), checkpoint {strategy full, phase loaded, no position}
  cursor_seed      - load_type 'cursor', no usable checkpoint: capture the high watermark `hi`,
                     seed, checkpoint {cursor position hi, incremental}
  cursor           - load_type 'cursor': read `hi` unless the plan carries it from the batched capture
                     (a temporal `hi` is capped at the source clock plus
                     cursor_skew_s; logged as alert=max_above_clock); unchanged if equal to the
                     checkpoint, else read (position - lookback, hi] + upsert MERGE by PK, advance
                     to `hi`. An identity cursor logs `inserts_only` once per run per table. Then,
                     when planner.snapshot_due (delete_mode pk_snapshot, interval passed): read every
                     source PK, delete the sink rows absent from it (client-scoped), record the time
  schema_reseed    - control.select_list gained columns vs the recorded one, or the cursor_expression
                     / cursor_kind changed (plan["reason"]): enqueue a reseed
  (retry)          - table_retries: the same route again on the fresh checkpoint
The `stage` variable records how far a table got, so a failure pinpoints read vs merge vs
checkpoint. The collection lock is re-checked (``deps.guard``, cached, <=1 query/30s) right before
every commit point — seed, each partition + the swap, MERGE, reseed enqueue, checkpoint advance —
so a run that lost its lock stops writing; skipped work is redone by the next sweep (at-least-once).

``deps`` (research R-03): ``spark``, ``store``, ``slots``, ``executor``, ``log``, ``params``
(``table_retries``, ``cursor_lookback_s``, ``cursor_skew_s``), plus ``guard`` (the lock check) and ``retry_policy`` (table_retry.RetryPolicy).
The checkpoint advances only after the sink write commits (tests/worker_test.py).
"""
import time

import cursor_engine
import load_strategies
import planner
import sink_writer
import source_io
import state_store as ss
import table_retry

_ACTION = {"full": "full", "cursor_seed": "seed", "cursor": "increment", "schema_reseed": "reseed"}


def ingest_one(plan, run_id, deps, attempt=0):
    spark, store, slots, log, assert_lock_held = deps.spark, deps.store, deps.slots, deps.log, deps.guard
    assert_lock_held()
    cfg, route, cp = plan["cfg"], plan["route"], plan.get("cp")
    db, schema, table = cfg["src_database"], cfg["src_schema"], cfg["src_table"]
    sink = sink_writer.sink_fqn(cfg)
    pool = ""  # FAIR pools follow work slots (one per unit); the ones used are recorded at finish
    slots.start_tracking()
    phase = cp["phase"] if cp else ss.SEEDING
    v_from = planner.position_from(plan)
    action = _ACTION[route]
    event_id = store.record_event_start(run_id, cfg, action, phase, pool)

    stage = "plan"
    rows_read = rows_merged = v_to = None
    try:
        if route == "full":
            stage = "load"
            assert_lock_held()
            loaded = load_strategies.get_strategy("full").load(
                spark, cfg, sink, slots=slots, executor=deps.executor, guard=assert_lock_held)
            rows_merged = loaded if isinstance(loaded, int) else None
            stage = "checkpoint"
            assert_lock_held()
            store.advance_checkpoint(cfg, position=None, phase=ss.LOADED, detail="full load complete",
                                     select_list=cfg["select_list"])
            outcome = "loaded"
        elif route == "cursor_seed":
            cursor_engine.validate_expression(cfg)   # the pure rules, before any source read
            stage = "ensure_sink"
            sink_writer.ensure_sink_table(spark, cfg, sink, slots=slots)
            stage = "read"
            typ = cursor_engine.column_type(cfg, slots=slots)
            hi, alert = cursor_engine.high_watermark(cfg, typ, deps.params["cursor_skew_s"], slots=slots)  # BEFORE the seed
            _log_alert(log, alert, db, schema, table)
            seed_started = time.monotonic()
            stage = "seed"
            assert_lock_held()
            seeded = sink_writer.seed(spark, cfg, sink, slots=slots, executor=deps.executor,
                                      guard=assert_lock_held)
            rows_merged = seeded if isinstance(seeded, int) else None
            stage = "checkpoint"
            assert_lock_held()
            hi = cursor_engine.seed_position(cfg, typ, hi, time.monotonic() - seed_started)  # temporal: pulled back by the seed duration
            store.advance_checkpoint(cfg, position=cursor_engine.encode_position(cfg["cursor_expression"], cfg["cursor_kind"], typ, hi),
                                     phase=ss.INCREMENTAL, detail=_detail("seed complete", alert),
                                     select_list=cfg["select_list"])
            v_to = hi
            outcome = "seeded"
        elif route == "cursor":
            pos = plan["position"]
            cursor_engine.validate_expression(cfg)
            stage = "read"
            if "hi" in plan:   # captured for the whole source database (planner.apply_captured)
                hi, alert = plan["hi"]
            else:
                hi, alert = cursor_engine.high_watermark(cfg, pos["type"], deps.params["cursor_skew_s"], slots=slots)  # BEFORE the read
            _log_alert(log, alert, db, schema, table)
            decision, new_value, note = cursor_engine.plan_increment(cfg, pos, hi)
            if note and attempt == 0:
                log(f"[{note}] {db}.{schema}.{table}: an identity cursor captures inserts only")
            if decision == "read":
                stage = "ensure_sink"
                sink_writer.ensure_sink_table(spark, cfg, sink, slots=slots)  # no-op when the sink exists
                stage = "merge"
                assert_lock_held()
                lookback_s = cursor_engine.lookback_seconds(cfg, deps.params["cursor_lookback_s"])
                merged = cursor_engine.read_and_merge(spark, cfg, sink, pos["type"], pos["value"], hi,
                                                      lookback_s, slots=slots)
                rows_merged = merged if isinstance(merged, int) else None
                outcome = "incremented"
            else:
                outcome = "unchanged"
            stage = "checkpoint"
            assert_lock_held()
            store.advance_checkpoint(cfg, position=cursor_engine.encode_position(pos["expr"], pos["kind"], pos["type"], new_value),
                                     detail=_detail(None, alert), select_list=cfg["select_list"])
            v_to = new_value
            if planner.snapshot_due(cfg, cp):   # deletes move no cursor: a quiet table snapshots too
                stage = "snapshot"
                assert_lock_held()
                deleted = cursor_engine.delete_missing(spark, cfg, sink, slots=slots)
                store.record_delete_snapshot(cfg)   # after the DELETE committed
                log(f"[snapshot] {db}.{schema}.{table}: deleted {deleted} rows absent from the source")
        else:  # schema_reseed
            stage = "checkpoint"
            assert_lock_held()
            reason = plan.get("reason", "schema_change")
            store.enqueue_reseed(cfg, reason=reason)  # flips phase -> seeding
            log(f"[reseed:{reason}] {db}.{schema}.{table}")
            outcome = "reseed_enqueued"

    except Exception as e:
        err = source_io.redact(table_retry.describe(e), source_io.known_creds(cfg))
        pool = _pools_label(cfg, action, slots)
        delay = deps.retry_policy.claim(cfg, e, attempt)   # None: fail now, as without retries
        try:
            store.record_event_finish(event_id, "failed" if delay is None else "retried", stage=stage,
                                      rows_read=rows_read, rows_merged=rows_merged, position_from=v_from,
                                      position_to=v_to, error=err, pool=pool, retry_count=attempt)
        except Exception:
            pass
        if delay is not None:   # checkpoint status untouched: the next attempt decides it
            log(f"[retry] {db}.{schema}.{table}: {table_retry.error_class(e)} at stage {stage} "
                f"(attempt {attempt + 2} of {deps.params['table_retries'] + 1}), retrying in {delay:.1f}s: {err[:200]}")
            raise table_retry.Retry(delay, e) from e
        try:
            store.set_status(cfg, "failed", err[:500])
        except Exception:
            pass
        log(f"[failed@{stage}] {db}.{schema}.{table} pool={pool}: {err[:200]}")
        raise

    # Success-path telemetry runs AFTER the try: the ingest (and its checkpoint) already committed,
    # so a telemetry write error must not flip this table to failed — log it and move on.
    # A reseed copied no data: record it as 'skipped' (action 'reseed') so last_success_at is NOT
    # stamped and the freshness/lag view keeps showing the table as stale until it re-seeds.
    pool = _pools_label(cfg, action, slots)
    try:
        store.record_event_finish(event_id, "skipped" if action == "reseed" else "ok", stage=stage,
                                  rows_read=rows_read, rows_merged=rows_merged,
                                  position_from=v_from, position_to=v_to, action=action, pool=pool,
                                  retry_count=attempt)
    except Exception as te:
        log(f"[telemetry] {db}.{schema}.{table}: record_event_finish failed (ingest unaffected): {str(te)[:200]}")
    log(f"[ok:{outcome}] {db}.{schema}.{table} pool={pool}")
    return outcome


ALERT_MAX_ABOVE_CLOCK = "alert=max_above_clock"


def _detail(base, alert):
    """Checkpoint detail: ``base`` plus the max_above_clock alert when the watermark was capped."""
    return " ".join(x for x in (base, ALERT_MAX_ABOVE_CLOCK if alert else None) if x) or None


def _log_alert(log, alert, db, schema, table):
    if alert:
        log(f"[{ALERT_MAX_ABOVE_CLOCK}] {db}.{schema}.{table}: the cursor MAX is above the source clock; "
            "hi capped at clock + cursor_skew_s")


def _pools_label(cfg, action, slots):
    """FAIR pools this table's own units ran in (e.g. 'pool3,pool17'). A partitioned seed's partitions
    run on the shared executor in whatever pools are free, so they're summarized, not listed."""
    used = ",".join(slots.tracked_pools()) or "-"
    return used + ("+partitions" if action in ("seed", "full") and cfg.get("load_partitioned") else "")


def _replan(plan, run_id, deps):
    """The next attempt of a retried table, from its checkpoint as it is now (planner.retry_plan)."""
    cfg = plan["cfg"]
    try:
        cp = deps.store.get_checkpoints([cfg["id"]]).get(cfg["id"])
    except Exception as e:
        try:
            deps.store.record_failed_tables(run_id, [(cfg, plan.get("cp"), f"retry not started, its checkpoint "
                                                                           f"could not be read: {str(e)[:300]}")],
                                            stage="plan")
        except Exception:
            pass
        raise
    return planner.retry_plan(plan, cp)
