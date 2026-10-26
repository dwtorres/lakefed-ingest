"""worker.ingest_one with fakes: the sink write commits before the checkpoint advances."""
from types import SimpleNamespace

import pytest

import table_retry
import worker


def _cfg():
    return {"id": 7, "src_database": "db", "src_schema": "dbo", "src_table": "t", "load_type": "cursor",
            "cursor_expression": "mod", "cursor_kind": "identity", "select_list": None, "sink_catalog": "c", "sink_schema": "s",
            "sink_table": "t"}


class FakeStore:
    def __init__(self, calls):
        self.calls = calls

    def record_event_start(self, run_id, cfg, action, phase, pool):
        self.calls.append(("event_start", action))
        return 1

    def record_event_finish(self, event_id, status, **kw):
        self.calls.append(("event_finish", status, kw))

    def advance_checkpoint(self, cfg, position=None, **kw):
        self.calls.append(("advance", position, kw))

    def enqueue_reseed(self, cfg, reason):
        self.calls.append(("enqueue_reseed", reason))

    def set_status(self, cfg, status, detail):
        self.calls.append(("set_status", status))


class FakeSlots:
    def start_tracking(self):
        pass

    def tracked_pools(self):
        return ["pool0"]


@pytest.fixture
def calls():
    return []


@pytest.fixture
def deps(calls):
    return SimpleNamespace(spark=object(), store=FakeStore(calls), slots=FakeSlots(), executor=None,
                           log=lambda *a: None, guard=lambda: None,
                           params={"table_retries": 0, "cursor_lookback_s": 3600, "cursor_skew_s": 300},
                           retry_policy=table_retry.RetryPolicy(retries=0, budget=0))


@pytest.fixture
def fakes(monkeypatch, calls):
    """Fake the source/sink modules the worker calls; ``fail`` makes every write raise."""
    state = {"fail": False, "alert": False}

    def write(name):
        def _w(*a, **kw):
            calls.append((name,))
            if state["fail"]:
                raise RuntimeError("sink write failed")
            return 3
        return _w

    def high_watermark(cfg, typ, skew_s, slots=None):
        calls.append(("high_watermark", skew_s))
        return "42", state["alert"]

    monkeypatch.setattr(worker, "sink_writer", SimpleNamespace(
        sink_fqn=lambda cfg: "c.s.t", ensure_sink_table=lambda *a, **kw: None, seed=write("seed")))
    monkeypatch.setattr(worker, "cursor_engine", SimpleNamespace(
        column_type=lambda cfg, slots=None: "int", high_watermark=high_watermark,
        validate_expression=worker.cursor_engine.validate_expression,
        seed_position=worker.cursor_engine.seed_position, encode_position=worker.cursor_engine.encode_position,
        plan_increment=worker.cursor_engine.plan_increment, lookback_seconds=worker.cursor_engine.lookback_seconds,
        read_and_merge=write("merge")))
    monkeypatch.setattr(worker, "load_strategies", SimpleNamespace(
        get_strategy=lambda lt: SimpleNamespace(load=write("full_load"))))
    return state


PLANS = {
    "cursor_seed": ({"route": "cursor_seed", "cfg": _cfg(), "cp": None}, "seed"),
    "cursor": ({"route": "cursor", "cfg": _cfg(), "cp": {"phase": "incremental"},
                "position": {"expr": "mod", "kind": "identity", "type": "int", "value": "10"}}, "merge"),
    "full": ({"route": "full", "cfg": dict(_cfg(), load_type="full"), "cp": None}, "full_load"),
}


def _names(calls):
    return [c[0] for c in calls]


@pytest.mark.parametrize("route", list(PLANS))
def test_sink_write_precedes_checkpoint_advance(route, deps, fakes, calls):
    plan, write = PLANS[route]
    worker.ingest_one(plan, run_id=1, deps=deps)
    names = _names(calls)
    assert names.index(write) < names.index("advance")
    assert names[-1] == "event_finish" and calls[-1][1] == "ok"


@pytest.mark.parametrize("route", list(PLANS))
def test_failed_write_leaves_checkpoint_untouched(route, deps, fakes, calls):
    plan, _ = PLANS[route]
    fakes["fail"] = True
    with pytest.raises(RuntimeError):
        worker.ingest_one(plan, run_id=1, deps=deps)
    names = _names(calls)
    assert "advance" not in names
    assert ("set_status", "failed") in calls
    assert [c for c in calls if c[0] == "event_finish"][0][1] == "failed"


def test_schema_reseed_enqueues_reseed(deps, fakes, calls):
    plan = {"route": "schema_reseed", "cfg": _cfg(), "cp": {"phase": "incremental"}, "added": ["x"]}
    assert worker.ingest_one(plan, run_id=1, deps=deps) == "reseed_enqueued"
    assert ("enqueue_reseed", "schema_change") in calls
    assert "advance" not in _names(calls)


def test_failure_before_any_write_records_failed_event_and_no_position(deps, fakes, calls, monkeypatch):
    # Nothing can fail at stage "plan" inside the worker's try (the position is read before the event
    # starts), so the earliest failure is the high-watermark read: failed event, no position, no advance.
    def boom(cfg, typ, *a, **kw):
        raise RuntimeError("source unreachable")
    monkeypatch.setattr(worker.cursor_engine, "high_watermark", boom)
    plan, _ = PLANS["cursor"]
    with pytest.raises(RuntimeError):
        worker.ingest_one(plan, run_id=1, deps=deps)
    finish = [c for c in calls if c[0] == "event_finish"][0]
    assert finish[1] == "failed"
    assert finish[2]["stage"] == "read"
    assert finish[2]["position_to"] is None
    assert "advance" not in _names(calls)


def test_cursor_seed_captures_hi_before_seed_and_advances_to_it(deps, fakes, calls):
    plan, _ = PLANS["cursor_seed"]
    assert worker.ingest_one(plan, run_id=1, deps=deps) == "seeded"
    names = _names(calls)
    assert names.index("high_watermark") < names.index("seed")
    advance = [c for c in calls if c[0] == "advance"][0]
    assert advance[1] == worker.cursor_engine.encode_position("mod", "identity", "int", "42")
    assert advance[2]["phase"] == "incremental"


def test_cursor_route_plans_merges_then_advances_to_hi(deps, fakes, calls, monkeypatch):
    seen = []
    real = worker.cursor_engine.plan_increment

    def plan_increment(cfg, pos, hi, *a, **kw):
        seen.append(("plan_increment", pos["value"], hi))
        return real(cfg, pos, hi, *a, **kw)
    monkeypatch.setattr(worker.cursor_engine, "plan_increment", plan_increment)
    plan, _ = PLANS["cursor"]
    assert worker.ingest_one(plan, run_id=1, deps=deps) == "incremented"
    names = _names(calls)
    assert seen == [("plan_increment", "10", "42")]
    assert names.index("high_watermark") < names.index("merge") < names.index("advance")
    advance = [c for c in calls if c[0] == "advance"][0]
    assert advance[1] == worker.cursor_engine.encode_position("mod", "identity", "int", "42")


def test_max_timeout_fails_table_and_keeps_position(deps, fakes, calls, monkeypatch):
    def timeout(cfg, typ, *a, **kw):
        raise RuntimeError("The query has timed out")
    monkeypatch.setattr(worker.cursor_engine, "high_watermark", timeout)
    plan, _ = PLANS["cursor"]
    with pytest.raises(RuntimeError, match="timed out"):
        worker.ingest_one(plan, run_id=1, deps=deps)
    assert "advance" not in _names(calls) and ("set_status", "failed") in calls
    finish = [c for c in calls if c[0] == "event_finish"]
    assert len(finish) == 1 and finish[0][1] == "failed"
    assert finish[0][2]["position_from"] == "10" and finish[0][2]["position_to"] is None


def test_capped_watermark_logs_the_alert_and_records_it_on_the_checkpoint(deps, fakes, calls):
    logged = []
    deps.log = logged.append
    fakes["alert"] = True
    plan, _ = PLANS["cursor"]
    worker.ingest_one(plan, run_id=1, deps=deps)
    assert ("high_watermark", 300) in calls
    assert any(worker.ALERT_MAX_ABOVE_CLOCK in line for line in logged)
    assert [c for c in calls if c[0] == "advance"][0][2]["detail"] == worker.ALERT_MAX_ABOVE_CLOCK


def test_identity_cursor_logs_inserts_only_once_per_run(deps, fakes, calls):
    logged = []
    deps.log = logged.append
    plan, _ = PLANS["cursor"]
    worker.ingest_one(plan, run_id=1, deps=deps)
    worker.ingest_one(plan, run_id=1, deps=deps, attempt=1)      # a retry does not repeat it
    assert sum("[inserts_only]" in line for line in logged) == 1


def test_plan_with_captured_hi_performs_no_max_read(deps, fakes, calls):
    plan = dict(PLANS["cursor"][0], hi=("42", False))
    assert worker.ingest_one(plan, run_id=1, deps=deps) == "incremented"
    names = _names(calls)
    assert "high_watermark" not in names and names.index("merge") < names.index("advance")
    assert [c for c in calls if c[0] == "advance"][0][1] == worker.cursor_engine.encode_position("mod", "identity", "int", "42")
