"""Planner: strategy routing, sink-ownership checks, retries, and the scale controls."""
import planner
import state_store as ss


def _cfg(cid, db, table, host="h1", sl="id, v", load_type="cursor", col="updated_at", kind="app_timestamp"):
    return {"id": cid, "src_host": host, "src_port": 1433, "src_database": db, "secret_key": f"k-{db}",
            "src_schema": "dbo", "src_table": table, "select_list": sl, "primary_key": "id",
            "load_type": load_type, "cursor_expression": col, "cursor_kind": kind}


def _cp(v, phase=ss.INCREMENTAL, sl="id, v", strategy="full"):
    return {"position": None if v is None else str(v), "phase": phase, "status": "ok",
            "select_list": sl, "strategy": strategy}


def _cur_cfg(cid=1, col="updated_at", sl="id, v"):
    return _cfg(cid, "d1", "t", sl=sl, col=col)


def _cur_cp(expr="updated_at", value="2026-01-01T00:00:00", sl="id, v", phase=ss.INCREMENTAL, strategy="cursor",
            kind="app_timestamp"):
    import cursor_engine
    return {"position": cursor_engine.encode_position(expr, kind, "datetime2", value), "phase": phase,
            "status": "ok", "select_list": sl, "strategy": strategy}


def test_route_tables_mixed_strategies_and_sources():
    cfgs = [
        _cfg(1, "d1", "a"),                                                   # cursor, no checkpoint
        dict(_cfg(2, "d1", "b"), load_type="full"),                            # sqlserver full
        dict(_cfg(3, "svc", "EMP"), src_type="oracle", load_type="full"),     # oracle full
        dict(_cfg(4, "d1", "c"), src_type="postgresql"),                      # cursor on postgres
        dict(_cfg(5, "d1", "d"), src_type="mysql"),                           # placeholder (mysql)
        dict(_cfg(6, "d1", "e"), load_type="ct"),                              # gone
        dict(_cfg(7, "d1", "f"), src_type="db2", load_type="full"),            # unknown adapter
        _cfg(8, "d1", "g"),                                                   # cursor, incremental phase
    ]
    cps = {2: _cp(None, phase=ss.LOADED), 8: _cur_cp()}
    plans = planner.route_tables(cfgs, cps)
    routes = {p["cfg"]["id"]: p["route"] for p in plans}
    assert routes == {1: "cursor_seed", 2: "full", 3: "full", 4: "cursor_seed", 5: "unsupported",
                      6: "unsupported", 7: "unsupported", 8: "cursor"}
    errors = {p["cfg"]["id"]: p["error"] for p in plans if p["route"] == "unsupported"}
    assert "'cursor' is not implemented" in errors[5] and "unsupported load_type 'ct'" in errors[6]
    assert "unsupported src_type 'db2'" in errors[7]


def test_full_routes_every_cycle_regardless_of_checkpoint():
    cfg = dict(_cfg(1, "d1", "a"), load_type="full")
    for cp in ({}, {1: _cp(None, phase=ss.LOADED)}, {1: _cur_cp()}):
        assert [p["route"] for p in planner.route_tables([cfg], cp)] == ["full"]


# ---- cursor (high-watermark) routing ---------------------------------------------------------------

def test_cursor_routes_seed_then_increment():
    plans = planner.route_tables([_cur_cfg()], {})
    assert [p["route"] for p in plans] == ["cursor_seed"]
    plans = planner.route_tables([_cur_cfg()], {1: _cur_cp()})
    assert [p["route"] for p in plans] == ["cursor"]
    assert plans[0]["position"] == {"expr": "updated_at", "kind": "app_timestamp", "type": "datetime2",
                                    "value": "2026-01-01T00:00:00"}


def test_cursor_reseeds_on_unusable_or_foreign_checkpoint():
    for cp in (_cur_cp(phase=ss.SEEDING),                        # reseed queued
               _cur_cp(strategy="full"),                         # load_type switched
               dict(_cur_cp(), position="12345"),                # not a cursor position
               dict(_cur_cp(), position='{"col": "updated_at", "type": "datetime2", "value": "1"}')):   # the old shape
        assert [p["route"] for p in planner.route_tables([_cur_cfg()], {1: cp})] == ["cursor_seed"], cp


def test_cursor_select_list_growth_routes_to_schema_reseed():
    plans = planner.route_tables([_cur_cfg(sl="id, v, w")], {1: _cur_cp()})
    assert [(p["route"], p["added"]) for p in plans] == [("schema_reseed", ["w"])]


def test_new_columns_carry_on_when_reseed_on_new_columns_is_off():
    cfgs, cps = [_cur_cfg(cid=2, sl="id, v, w")], {2: _cur_cp()}
    assert [p["route"] for p in planner.route_tables(cfgs, cps, reseed_on_new_columns=False)] == ["cursor"]
    assert [p["route"] for p in planner.route_tables(cfgs, cps)] == ["schema_reseed"]


def test_position_from_is_the_cursor_value_only_for_an_increment():
    import cursor_engine
    cur_cp = _cur_cp()
    assert planner.position_from({"route": "cursor", "cp": cur_cp,
                                  "position": cursor_engine.decode_position(cur_cp["position"])}) == "2026-01-01T00:00:00"
    for route in ("cursor_seed", "schema_reseed", "full"):
        assert planner.position_from({"route": route, "cp": cur_cp}) is None


# ---- consolidated sinks: sink-ownership checks before any work -------------------------------------

def _sink(cfg, table="orders", client=None):
    return dict(cfg, sink_catalog="Main", sink_schema="Bronze", sink_table=table, client_id=client)


def test_mixed_sink_rows_fail_at_plan_others_proceed():
    cfgs = [_sink(_cfg(1, "d1", "a"), client="c1"), _sink(_cfg(2, "d2", "a"), table="other")]
    plans = planner.route_tables(cfgs, {}, mixed_sinks={("main", "bronze", "orders")})
    routes = {p["cfg"]["id"]: p["route"] for p in plans}
    assert routes == {1: "unsupported", 2: "cursor_seed"}
    assert "claimed by a dedicated row" in plans[0]["error"]


def test_consolidated_row_must_not_select_client_id_and_must_be_valid():
    bad_sl = _sink(_cfg(1, "d1", "a", sl="id, __Client_ID__, v"), client="c1")
    bad_id = _sink(_cfg(2, "d1", "b"), client="c 1; drop")
    ok = _sink(_cfg(3, "d1", "c", sl="id, client_id, v"))         # dedicated: a native client_id is fine
    ok_cons = _sink(_cfg(4, "d1", "d", sl="id, client_id, v"), client="c1")   # ...and when consolidated
    plans = planner.route_tables([bad_sl, bad_id, ok, ok_cons], {})
    errs = {p["cfg"]["id"]: p.get("error") for p in plans}
    assert "remove it from select_list" in errs[1] and "invalid client_id" in errs[2]
    assert errs[3] is None and errs[4] is None


def test_missing_primary_key_fails_at_plan_for_merge_strategies_only():
    cursor = dict(_cfg(2, "d1", "b"), primary_key=None)
    full = dict(_cfg(3, "d1", "c"), load_type="full", primary_key=None)
    assert "needs a primary_key" in planner.config_error(cursor)
    assert planner.config_error(full) is None
    plans = planner.route_tables([cursor, full], {})
    assert {p["cfg"]["id"]: p["route"] for p in plans} == {2: "unsupported", 3: "full"}


def test_config_error_rejects_any_load_type_but_cursor_and_full():
    for bad in ("ct", "cdc", "bogus"):
        reason = planner.config_error(dict(_cfg(1, "d1", "a"), load_type=bad))
        assert f"unsupported load_type '{bad}'" in reason and "cursor" in reason and "full" in reason
    assert "needs a primary_key" in planner.config_error(dict(_cfg(2, "d1", "b"), primary_key=None))


def test_sqlserver_cursor_row_routes_by_its_checkpoint():
    cfg = dict(_cur_cfg(), src_type="sqlserver")
    assert planner.route_tables([cfg], {})[0]["route"] == "cursor_seed"
    plan = planner.route_tables([cfg], {1: _cur_cp(value="2026-05-01T00:00:00")})[0]
    assert plan["route"] == "cursor" and plan["position"]["value"] == "2026-05-01T00:00:00"


def test_write_mode_and_client_attrs_config_errors():
    ok = dict(_cfg(1, "d1", "a"), client_attrs='{"tenant_name": "x"}')
    assert planner.config_error(ok) is None and planner.config_error(dict(ok, load_type="full")) is None
    assert "invalid write_mode" in planner.config_error(dict(ok, write_mode="append"))
    assert "invalid client_attrs column" in planner.config_error(dict(ok, client_attrs='{"__x": 1}'))
    clash = planner.config_error(dict(ok, select_list="id, Tenant_Name"))
    assert "tenant_name" in clash and "rename" in clash


def test_a_read_through_a_connection_needs_the_connection():
    remote = dict(_cfg(1, "d1", "a"), use_remote_query=True, src_connection="c1", secret_key=None)
    assert planner.config_error(remote) is None
    assert "src_connection" in planner.config_error(dict(remote, src_connection=None))


# ---- retries and scale controls ---------------------------------------------------------------------

def test_retry_plan_keeps_the_route_on_the_fresh_checkpoint():
    plan = {"cfg": _cur_cfg(), "route": "cursor", "cp": _cur_cp(value="1"), "position": {"value": "1"}}
    fresh = _cur_cp(value="2")
    assert planner.retry_plan(plan, fresh) == dict(plan, cp=fresh)


def test_seed_budget_defers_the_rest_and_puts_failed_seeds_last():
    plans = [{"cfg": _cfg(i, "d1", "t"), "route": r, "cp": cp} for i, r, cp in (
        (1, "cursor_seed", {"status": "failed"}), (2, "cursor", {}), (3, "cursor_seed", None),
        (4, "cursor_seed", None), (5, "cursor_seed", None))]
    kept, deferred = planner.apply_seed_budget(plans, 2)
    assert [p["cfg"]["id"] for p in kept] == [2, 3, 4] and [p["cfg"]["id"] for p in deferred] == [1, 5]
    assert planner.apply_seed_budget(plans, 0) == (plans, []) and planner.apply_seed_budget(plans, 4) == (plans, [])


def test_freshness_interval_skips_quiet_rows_whose_stamp_is_recent():
    from datetime import datetime, timedelta, timezone
    now = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    recent, old = now - timedelta(minutes=10), now - timedelta(hours=3)
    cps = {1: dict(_cp(100), last_success_at=recent),                    # quiet, stamped recently: skip
           2: dict(_cp(100), last_success_at=old),                       # stamp too old
           3: dict(_cp(90), last_success_at=recent),                     # checkpoint moves
           4: dict(_cp(100), last_success_at=recent, status="failed"),   # status to clear
           5: dict(_cp(100), last_success_at=recent)}                    # records a select_list
    rows = [(1, 100, None), (2, 100, None), (3, 100, None), (4, 100, None), (5, 100, "id, v")]
    assert [r[0] for r in planner.bulk_rows_to_write(rows, cps, 3600, now=now)] == [2, 3, 4, 5]
    assert planner.bulk_rows_to_write(rows, cps, 0) == rows


# ---- expression cursor (US3) --------------------------------------------------------------------------

def test_changed_expression_or_kind_routes_to_schema_reseed():
    cfg = _cur_cfg()
    assert planner.route_tables([cfg], {1: _cur_cp()})[0]["route"] == "cursor"
    for cp in (_cur_cp(expr="GREATEST(updated_at, created_at)"), _cur_cp(expr="Updated_At"), _cur_cp(kind="db_timestamp")):
        plan = planner.route_tables([cfg], {1: cp})[0]
        assert plan["route"] == "schema_reseed" and plan["reason"] == "expression_change", cp


def test_config_error_rejects_unimplemented_delete_modes_and_unknown_kinds():
    ok = _cfg(1, "d1", "a")
    assert planner.config_error(dict(ok, delete_mode="none")) is None
    assert planner.config_error(dict(ok, delete_mode="pk_snapshot")) is None
    for mode in ("soft_column", "full_compare"):
        assert "not implemented" in planner.config_error(dict(ok, delete_mode=mode))
    assert "delete_mode" in planner.config_error(dict(ok, delete_mode="bogus"))
    assert "cursor_kind" in planner.config_error(dict(ok, cursor_kind="sequence"))
    assert "cursor_kind" in planner.config_error(dict(ok, cursor_kind=None))
    assert "cursor_expression" in planner.config_error(dict(ok, cursor_expression=None))
    assert planner.config_error(dict(ok, load_type="full", cursor_expression=None, cursor_kind=None)) is None


# ---- quiet tables (US6, gap 10) ---------------------------------------------------------------------

def _id_pos(v):
    import cursor_engine
    return cursor_engine.encode_position("id_seq", "identity", "int", v)


def _id_cps(ids, value="100", **cp):
    return {i: dict(_cp(None, strategy="cursor"), position=_id_pos(value), **cp) for i in ids}


def test_gap_10_captured_hi_routes_unchanged_to_bulk_and_moved_to_cursor():
    cfgs = [_cfg(i, "d1", f"t{i}", col="id_seq", kind="identity") for i in (1, 2, 3, 4)]
    cps = _id_cps((1, 2, 3, 4))
    plans = planner.route_tables(cfgs, cps)
    captured = {1: ("100", False), 2: ("120", False), 4: ("90", False)}   # 3: not captured, the worker reads MAX
    plans, unchanged = planner.apply_captured(plans, cps, captured)
    assert unchanged == [(1, _id_pos("100"), None)]
    by_id = {p["cfg"]["id"]: p for p in plans}
    assert sorted(by_id) == [2, 3, 4]
    assert by_id[2]["hi"] == ("120", False) and "hi" not in by_id[3]
    assert "hi" not in by_id[4]                     # hi below the position: the worker reports the failure


def test_gap_10_unchanged_row_records_a_grown_select_list():
    cfg = _cfg(1, "d1", "t", col="id_seq", kind="identity", sl="id, v, w")
    cps = _id_cps((1,))
    plans = planner.route_tables([cfg], cps, reseed_on_new_columns=False)
    plans, unchanged = planner.apply_captured(plans, cps, {1: ("100", False)})
    assert plans == [] and unchanged == [(1, _id_pos("100"), "id, v, w")]


def test_gap_10_bulk_rows_to_write_skips_fresh_quiet_tables():
    from datetime import datetime, timedelta, timezone
    now = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    cps = _id_cps((1, 2), last_success_at=now - timedelta(minutes=10))
    cps[2]["last_success_at"] = now - timedelta(hours=2)
    rows = [(1, _id_pos("100"), None), (2, _id_pos("100"), None)]
    assert planner.bulk_rows_to_write(rows, cps, 3600, now=now) == [rows[1]]
