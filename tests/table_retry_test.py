"""Table retries: the classifier, the backoff, the run's limits, and retries with injected faults.

The per-table worker lives in ingest.ipynb, so the end-to-end tests drive ``table_retry.with_retries``
and ``planner.retry_plan`` with ``_Worker``, a fake that keeps ingest_one's contract: it records an
event per attempt, asks the policy on failure and raises ``Retry`` when one is granted, and advances
the checkpoint after the commit. ``_Sink`` records writes tagged (control id, run) and can be told
to fail before writing.
"""
import threading

import pytest

import planner
import state_store as ss
import table_retry as tr

CONNECTION = "[FAILED_JDBC.CONNECTION] Failed JDBC <redacted> on the operation: Failed to connect to the database. SQLSTATE: HV000"


class _JavaError:
    """A py4j Java throwable: class name, message, SQL error code and SQLSTATE, and its cause."""
    def __init__(self, name, message, code=None, state=None, cause=None):
        self.name, self.message, self.code, self.state, self.cause = name, message, code, state, cause

    def getClass(self):
        return type("C", (), {"getName": lambda _: self.name})()

    def getMessage(self):
        return self.message

    def getErrorCode(self):
        if self.code is None:
            raise AttributeError("not a SQLException")
        return self.code

    def getSQLState(self):
        return self.state

    def getCause(self):
        return self.cause


def _spark_error(message=CONNECTION, cause=None):
    e = RuntimeError(message)
    e._origin = _JavaError("org.apache.spark.SparkException", message, cause=cause)
    return e

def _cfg(cid, load_type="cursor", host="h1"):
    return {"id": cid, "src_host": host, "src_port": 1433, "src_database": f"db{cid}", "src_schema": "dbo",
            "src_table": "orders", "secret_key": f"k{cid}", "select_list": "id, v", "primary_key": "id",
            "sink_catalog": "main", "sink_schema": "bronze", "sink_table": "orders", "load_type": load_type,
            "cursor_column": "v"}


def _plan(cid, route="cursor_seed", cp=None, **kw):
    return {"cfg": _cfg(cid, **kw), "route": route, "cp": cp, "hi": 150}


class _Sink:
    """Writes as (control id, run). ``faults[id]``: a list of 'before' (fail, nothing written), used
    up one per write."""
    def __init__(self, faults=None, error=CONNECTION):
        self.rows, self.faults, self.error = [], faults or {}, error

    def write(self, cfg, run_id):
        fault = (self.faults.get(cfg["id"]) or [None]).pop(0) if self.faults.get(cfg["id"]) else None
        if fault == "before":
            raise _spark_error(self.error)
        self.rows.append((cfg["id"], run_id))

    def writes(self, cfg, run_id):
        return self.rows.count((cfg["id"], run_id))


class _Worker:
    """ingest_one's retry contract, over ``_Sink`` and an in-memory checkpoint table."""
    def __init__(self, sink, policy, run_id=42, merge_faults=None):
        self.sink, self.policy, self.run_id = sink, policy, run_id
        self.checkpoints, self.events, self.merges = {}, [], []
        self.merge_faults = merge_faults or {}

    def _advance(self, cfg, position, phase):
        cp = self.checkpoints.setdefault(cfg["id"], {})
        cp.update(position=position, phase=phase, strategy=cfg["load_type"])

    def __call__(self, plan, attempt=0):
        cfg, route, run_id = plan["cfg"], plan["route"], self.run_id
        stage = "plan"
        try:
            if route == "cursor":
                stage = "merge"
                if self.merge_faults.get(cfg["id"]):
                    self.merge_faults[cfg["id"]] -= 1
                    raise _spark_error()
                self.merges.append((cfg["id"], plan["hi"]))
                stage = "checkpoint"
                self._advance(cfg, plan["hi"], ss.INCREMENTAL)
                outcome = "incremented"
            else:
                stage = "seed"
                self.sink.write(cfg, run_id)
                stage = "checkpoint"
                cursor = cfg["load_type"] == "cursor"
                self._advance(cfg, plan["hi"] if cursor else None, ss.INCREMENTAL if cursor else ss.LOADED)
                outcome = "seeded"
        except Exception as e:
            delay = self.policy.claim(cfg, e, attempt)
            self.events.append((cfg["id"], "retried" if delay is not None else "failed", stage, attempt))
            if delay is not None:
                raise tr.Retry(delay, e) from e
            raise
        self.events.append((cfg["id"], "ok", stage, attempt))
        return outcome

    def replan(self, plan):
        cp = self.checkpoints.get(plan["cfg"]["id"])
        return planner.retry_plan(plan, dict(cp) if cp is not None else None)


def _run(worker, policy, plans, sleeps):
    run = tr.with_retries(worker, policy, worker.replan, sleep=sleeps.append)
    out = []
    for p in plans:
        try:
            out.append(("ok", run(p)))
        except Exception as e:
            out.append(("failed", str(e)))
    return out


def _policy(retries=2, budget=20, per_source=5):
    return tr.RetryPolicy(retries=retries, budget=budget, per_source=per_source, base_s=10, max_s=120,
                          rand=lambda: 0.5)


# ---- 1. classify ---------------------------------------------------------------------------------------

@pytest.mark.parametrize("error, expected", [
    (_spark_error(), tr.TRANSIENT),                                          # the exact text Lakebase recorded today
    (_spark_error(cause=_JavaError("com.microsoft.sqlserver.jdbc.SQLServerException",
                                   "Login failed for user 'lfc_ingest'.", 18456, "S0001")), tr.PERMANENT),
    (_spark_error("[FAILED_JDBC.UNCLASSIFIED] x", cause=_JavaError(
        "com.microsoft.sqlserver.jdbc.SQLServerException", "Resource ID : 1. The request limit for the database "
        "is 400 and has been reached.", 10928, "S0001")), tr.TRANSIENT),
    (_spark_error("[FAILED_JDBC.UNCLASSIFIED] x", cause=_JavaError(
        "com.microsoft.sqlserver.jdbc.SQLServerException", "Connection reset", 0, "08S01")), tr.TRANSIENT),
    (RuntimeError("java.net.SocketTimeoutException: Read timed out"), tr.TRANSIENT),
    (RuntimeError("Invalid object name 'dbo.orders'."), tr.PERMANENT),
    (RuntimeError("The SELECT permission was denied on the object 'orders'"), tr.PERMANENT),
    (RuntimeError("collection lock lost; table deferred to the next sweep"), tr.PERMANENT),
    (ValueError("control row 3: Connection reset"), tr.PERMANENT),            # a configuration error
    (RuntimeError("[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'v'"), tr.PERMANENT),
    (RuntimeError("something nobody has seen before"), tr.PERMANENT),         # unknown: as today
    # the other source adapters: Spark wraps their refused logins in FAILED_JDBC.CONNECTION too
    (_spark_error(cause=_JavaError("java.sql.SQLException", "ORA-01017: invalid username/password; logon denied",
                                   1017, "72000")), tr.PERMANENT),
    (RuntimeError("java.sql.SQLSyntaxErrorException: ORA-00942: table or view does not exist"), tr.PERMANENT),
    (_spark_error(cause=_JavaError("org.postgresql.util.PSQLException",
                                   'FATAL: password authentication failed for user "x"', 0, "28P01")), tr.PERMANENT),
    (RuntimeError('org.postgresql.util.PSQLException: ERROR: relation "orders" does not exist'), tr.PERMANENT),
    (_spark_error(cause=_JavaError("java.sql.SQLException", "Access denied for user 'x'@'h'", 1045, "28000")),
     tr.PERMANENT),
    (RuntimeError("java.sql.SQLSyntaxErrorException: Table 'db.orders' doesn't exist"), tr.PERMANENT),
    (RuntimeError("java.sql.SQLRecoverableException: ORA-03113: end-of-file on communication channel"),
     tr.TRANSIENT),
    (_spark_error(cause=_JavaError("org.postgresql.util.PSQLException", "FATAL: sorry, too many clients already",
                                   0, "53300")), tr.TRANSIENT),
    (RuntimeError("com.mysql.cj.jdbc.exceptions.CommunicationsException: Communications link failure"),
     tr.TRANSIENT),
    # SQL Server: a session killed in the middle of a fetch (live run r3b, Spark task retries off)
    (_spark_error("An error occurred while calling o543.sql.\n: org.apache.spark.SparkException: Job aborted",
                  cause=_JavaError("com.microsoft.sqlserver.jdbc.SQLServerException",
                                   "An error occurred during the current command (Done status 0).", 0, "S0001")),
     tr.TRANSIENT),
    (RuntimeError("com.microsoft.sqlserver.jdbc.SQLServerException: An error occurred during the current command "
                  "(Done status 0)."), tr.TRANSIENT),
    (RuntimeError("An error occurred during the current command (Done status 0)."), tr.PERMANENT),   # no driver
    (RuntimeError("com.microsoft.sqlserver.jdbc.SQLServerException: An error occurred during the current command "
                  "(Done status 16)."), tr.PERMANENT),
    (RuntimeError("com.microsoft.sqlserver.jdbc.SQLServerException: Invalid object name 'gen.h0800'. "
                  "An error occurred during the current command (Done status 0)."), tr.PERMANENT),  # deny wins
])
def test_classify_retries_transient_source_errors_only(error, expected):
    assert tr.classify(error) == expected


def test_ct_markers_are_gone_and_ct_errors_are_permanent_as_unknown_errors():
    # Change Tracking is not a load strategy any more: no deny rule names it. Its errors are
    # permanent the way every unknown error is.
    assert not any("CT_RETENTION_LAPSED" in m or "CHANGE TRACKING" in m for m in tr._NEVER + tr._PERMANENT)
    assert tr.classify(RuntimeError("CT_RETENTION_LAPSED: change tracking retention has passed the "
                                    "checkpoint")) == tr.PERMANENT
    assert tr.classify(RuntimeError("The VIEW CHANGE TRACKING permission was denied on 'orders'")) == tr.PERMANENT


def test_describe_adds_the_java_cause_spark_leaves_out():
    e = _spark_error(cause=_JavaError("com.microsoft.sqlserver.jdbc.SQLServerException", "Connection reset", 0,
                                      "08S01"))
    assert tr.describe(e) == (CONNECTION + " | caused by com.microsoft.sqlserver.jdbc.SQLServerException"
                                           "[code=0,state=08S01]: Connection reset")
    assert tr.describe(RuntimeError("plain")) == "plain"
    assert tr.error_class(e) == "FAILED_JDBC.CONNECTION" and tr.error_class(KeyError("k")) == "KeyError"


# ---- 2. transient, then success ------------------------------------------------------------------------

def test_a_transient_failure_is_retried_after_a_pause_and_recorded_per_attempt():
    sink, policy, sleeps = _Sink(faults={1: ["before"]}), _policy(), []
    worker = _Worker(sink, policy)
    assert _run(worker, policy, [_plan(1)], sleeps) == [("ok", "seeded")]
    assert worker.events == [(1, "retried", "seed", 0), (1, "ok", "checkpoint", 1)]
    assert sleeps == [7.5] and sink.writes(_cfg(1), 42) == 1
    assert policy.summary() == {"retries": 1, "recovered": 1, "failed_after_retry": 0, "budget": 20, "budget_left": 19,
                                "budget_exhausted": False, "sources_stopped": [],
                                "by_class": {"FAILED_JDBC.CONNECTION": 1}}


# ---- 3. a permanent failure fails at once ----------------------------------------------------------------

def test_a_permanent_failure_fails_at_once_without_a_pause():
    sink, policy, sleeps = _Sink(faults={1: ["before"]}, error="Invalid object name 'dbo.orders'."), _policy(), []
    worker = _Worker(sink, policy)
    assert _run(worker, policy, [_plan(1)], sleeps)[0][0] == "failed"
    assert worker.events == [(1, "failed", "seed", 0)] and sleeps == []
    assert policy.summary()["retries"] == 0 and policy.summary()["budget_left"] == 20


# ---- 4. the run's budget -----------------------------------------------------------------------------------

def test_once_the_budget_is_spent_failures_fail_at_once():
    sink, policy, sleeps = _Sink(faults={i: ["before"] for i in range(1, 6)}), _policy(budget=2), []
    worker = _Worker(sink, policy)
    out = _run(worker, policy, [_plan(i, host=f"h{i}") for i in range(1, 6)], sleeps)
    assert [o[0] for o in out] == ["ok", "ok", "failed", "failed", "failed"] and len(sleeps) == 2
    summary = policy.summary()
    assert (summary["retries"], summary["budget_left"], summary["budget_exhausted"]) == (2, 0, True)


# ---- 5. a source that keeps failing stops retrying ---------------------------------------------------------

def test_a_source_stops_retrying_after_its_failed_retries_and_others_carry_on():
    down = {i: ["before"] * 3 for i in range(1, 11)}                    # source h1: every attempt fails
    sink = _Sink(faults={**down, 11: ["before"]})               # source h2: fails once
    policy, sleeps = _policy(retries=2, budget=100, per_source=3), []
    worker = _Worker(sink, policy)
    out = _run(worker, policy, [_plan(i) for i in range(1, 11)] + [_plan(11, host="h2")], sleeps)
    assert [o[0] for o in out[:10]] == ["failed"] * 10 and out[10] == ("ok", "seeded")
    assert policy.summary()["sources_stopped"] == ["host:h1:1433"]
    retried_on_h1 = sum(1 for e in worker.events if e[0] <= 10 and e[3] > 0)
    assert retried_on_h1 == 3                                            # 3 failed retries, then no more


def test_failures_at_once_on_one_source_are_bounded_by_the_runs_budget():
    # A source outage fails many tables at the same moment, before any retry has failed: the source
    # stop can't act yet, so the run's budget is what bounds the extra queries.
    policy, start = _policy(retries=2, budget=20, per_source=5), threading.Barrier(32)
    granted = []

    def fail_first_attempt(cid):
        start.wait()
        granted.append(policy.claim(_cfg(cid), _spark_error(), 0))
    threads = [threading.Thread(target=fail_first_attempt, args=(cid,)) for cid in range(1, 33)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(d is not None for d in granted) == 20 and policy.summary()["budget_left"] == 0


def test_a_source_keeps_retrying_while_its_retries_succeed():
    # A blip that fails many tables of one server at once is what retries are for: only failed
    # retries count towards the source's stop.
    policy = _policy(retries=2, budget=100, per_source=1)
    for cid in range(1, 11):
        assert policy.claim(_cfg(cid), _spark_error(), 0) is not None
    for cid in range(1, 11):
        policy.finished(_cfg(cid), True, 1)
    assert policy.claim(_cfg(11), _spark_error(), 0) is not None and policy.summary()["sources_stopped"] == []


def test_a_run_stopped_during_a_pause_counts_the_table_as_failed():
    sink, policy = _Sink(faults={1: ["before"]}), _policy(per_source=1)
    worker = _Worker(sink, policy)

    def cancelled(seconds):
        raise KeyboardInterrupt("the run was cancelled")
    run = tr.with_retries(worker, policy, worker.replan, sleep=cancelled)
    with pytest.raises(KeyboardInterrupt):
        run(_plan(1))
    assert worker.events == [(1, "retried", "seed", 0)] and sink.rows == []
    summary = policy.summary()
    assert (summary["retries"], summary["recovered"], summary["failed_after_retry"]) == (1, 0, 1)

# ---- 8. merge mode -----------------------------------------------------------------------------------------

def test_merge_mode_retries_the_same_bounded_window_and_advances_once():
    sink, policy, sleeps = _Sink(), _policy(), []
    worker = _Worker(sink, policy, merge_faults={1: 1})
    plan = _plan(1, route="cursor", cp={"position": "100", "phase": ss.INCREMENTAL, "strategy": "cursor"})
    assert _run(worker, policy, [plan], sleeps) == [("ok", "incremented")]
    assert worker.merges == [(1, 150)] and worker.checkpoints[1]["position"] == 150


# ---- 9. retries off ------------------------------------------------------------------------------------------

def test_with_retries_off_a_failure_fails_as_today():
    sink, policy, sleeps = _Sink(faults={1: ["before"]}), tr.RetryPolicy(retries=0), []
    worker = _Worker(sink, policy)
    replans = []
    run = tr.with_retries(worker, policy, lambda p: replans.append(p) or p, sleep=sleeps.append)
    with pytest.raises(RuntimeError, match="FAILED_JDBC.CONNECTION"):
        run(_plan(1))
    assert worker.events == [(1, "failed", "seed", 0)] and sleeps == [] and replans == []
    assert policy.summary()["retries"] == 0


# ---- 10. the backoff -------------------------------------------------------------------------------------------

def test_each_pause_doubles_with_jitter_up_to_the_cap_and_never_near_zero():
    assert [tr.backoff_delay(k, 10, 120, rand=lambda: 0.0) for k in range(5)] == [5, 10, 20, 40, 60]
    assert [tr.backoff_delay(k, 10, 120, rand=lambda: 1.0) for k in range(5)] == [10, 20, 40, 80, 120]

# ---- retry_plan ----------------------------------------------------------------------------------------------

def test_retry_plan_keeps_the_route_on_the_fresh_checkpoint():
    fresh = {"position": "100", "phase": ss.INCREMENTAL, "strategy": "cursor"}
    plan = _plan(1, route="cursor")
    assert planner.retry_plan(plan, fresh) == dict(plan, cp=fresh)
    assert planner.retry_plan(_plan(2), None) == dict(_plan(2), cp=None)


def test_a_policy_that_cannot_judge_a_failure_fails_it_as_today():
    def no_source(cfg):
        raise ValueError("unknown src_type")
    policy = tr.RetryPolicy(retries=2, source_of=no_source)
    assert policy.claim(_cfg(1), _spark_error(), 0) is None and policy.summary()["retries"] == 0


def test_the_spark_condition_is_read_once_preferring_the_current_name():
    class _Captured(RuntimeError):
        calls = []

        def getCondition(self):
            self.calls.append("getCondition")
            return "FAILED_JDBC.CONNECTION"

        def getErrorClass(self):
            self.calls.append("getErrorClass")
            return "FAILED_JDBC.CONNECTION"

        def getSqlState(self):
            return "08001"
    error = _Captured("Failed to connect to the database.")
    assert tr.classify(error) == tr.TRANSIENT and _Captured.calls == ["getCondition"]
