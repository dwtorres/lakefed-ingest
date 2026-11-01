"""Load strategies: registry, support matrix, cursor position round trip, FULL load, placeholders."""
import pytest

import load_strategies as ls
import sink_writer as sw


def test_registry_and_default():
    assert ls.LOAD_TYPES == ("cursor", "full")
    assert ls.get_strategy(None).name == "cursor"                 # the column default
    assert ls.get_strategy(" FULL ").name == "full"
    for gone in ("ct", "cdc", "incremental"):
        with pytest.raises(ValueError, match=f"unsupported load_type '{gone}'"):
            ls.get_strategy(gone)


def test_supported_is_cursor_and_full_only():
    assert ls.supported("sqlserver", "cursor") and ls.supported("sqlserver", "full")
    assert ls.supported(None, None) and ls.supported("mysql", "full")
    for lt in ("ct", "cdc", "weekly"):
        assert not ls.supported("sqlserver", lt)
    assert ls.support_matrix() == {
        "sqlserver":  {"cursor": "yes",         "full": "yes"},
        "oracle":     {"cursor": "yes",         "full": "yes"},
        "postgresql": {"cursor": "yes",         "full": "yes"},
        "mysql":      {"cursor": "placeholder", "full": "yes"},
    }


def test_unsupported_reasons_are_clear():
    assert "unsupported load_type 'ct'" in ls.unsupported_reason("sqlserver", "ct")
    assert "unsupported src_type 'db2'" in ls.unsupported_reason("db2", "full")
    assert "unsupported load_type 'weekly'" in ls.unsupported_reason("sqlserver", "weekly")


def test_pk_requirements():
    assert ls.get_strategy("cursor").requires_pk and not ls.get_strategy("full").requires_pk


def test_cursor_runs_where_the_adapter_implements_it():
    assert ls.supported("sqlserver", "cursor") and ls.get_strategy("cursor").implemented
    assert ls.supported("oracle", "cursor") and ls.supported("postgresql", "cursor")
    reason = ls.unsupported_reason("mysql", "cursor")
    assert "load_type 'cursor' is not implemented for src_type 'mysql'" in reason
    assert "MySqlAdapter" in reason and "NotImplementedError" in reason


def test_cursor_position_round_trip():
    import cursor_engine as cur
    s = ls.get_strategy("cursor")
    text = cur.encode_position("rv", "rowversion", "timestamp", "2001")
    assert s.decode_position(s.encode_position(text)) == {"expr": "rv", "kind": "rowversion", "type": "timestamp", "value": "2001"}
    assert s.decode_position(None) is None and s.decode_position("287493") is None


def test_full_load_ensures_sink_then_seeds(monkeypatch):
    calls = []
    monkeypatch.setattr(sw, "ensure_sink_table", lambda sp, cfg, sink, slots=None: calls.append(("ensure", sink)))
    monkeypatch.setattr(sw, "seed", lambda sp, cfg, sink, slots=None, executor=None, guard=None:
                        calls.append(("seed", sink, guard)) or 42)
    g = object()
    assert ls.get_strategy("full").load("spark", {"id": 1}, "c.s.t", guard=g) == 42
    assert calls == [("ensure", "c.s.t"), ("seed", "c.s.t", g)]
