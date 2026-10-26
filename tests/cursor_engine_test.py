"""Cursor (high-watermark) strategy, SQL Server: position encoding, typed SQL, increment decision."""
import re
from datetime import datetime

import pytest

import cursor_engine as cur

CFG = {"src_schema": "dbo", "src_table": "orders", "cursor_column": "updated_at",
       "select_list": "id, amount, updated_at", "primary_key": "id"}


def test_position_round_trip_and_garbage():
    text = cur.encode_position("updated_at", "datetime2", "2026-01-01T10:00:00.1234567")
    assert cur.decode_position(text) == {"col": "updated_at", "type": "datetime2",
                                         "value": "2026-01-01T10:00:00.1234567"}
    assert cur.decode_position(cur.encode_position("id", "int", None))["value"] is None
    for bad in (None, "", "287493", "not json", '{"col": "x"}', "[1]"):
        assert cur.decode_position(bad) is None


def test_type_lookup_sql_quotes_names():
    sql = cur.column_type_sql(dict(CFG, src_table="odd]name", cursor_column="it's"))
    assert "OBJECT_ID(N'[dbo].[odd]]name]')" in sql and "c.name = N'it''s'" in sql


@pytest.mark.parametrize("typ,expr,clock", [
    ("bigint", "CONVERT(VARCHAR(64), MAX([updated_at]))", ""),
    ("datetime2", "CONVERT(VARCHAR(64), MAX([updated_at]), 126)", ", CONVERT(VARCHAR(64), SYSDATETIME(), 126) AS src_now"),
    ("datetimeoffset", "CONVERT(VARCHAR(64), MAX([updated_at]), 126)", ", CONVERT(VARCHAR(64), SYSDATETIMEOFFSET(), 126) AS src_now"),
])
def test_high_watermark_sql(typ, expr, clock):
    assert cur.high_watermark_sql(CFG, typ) == f"SELECT {expr} AS hi, {cur.nullable_sql(CFG)} AS nullable{clock} FROM [dbo].[orders]"


@pytest.mark.parametrize("typ", ["timestamp", "rowversion"])
def test_rowversion_high_watermark_capped_below_min_active_rowversion(typ):
    # Never past the oldest open transaction: rows it commits later must not fall below the watermark.
    mx, mar = "CAST(MAX([updated_at]) AS BIGINT)", "CAST(MIN_ACTIVE_ROWVERSION() AS BIGINT)"
    assert cur.high_watermark_sql(CFG, typ) == (
        f"SELECT CONVERT(VARCHAR(20), CASE WHEN {mx} < {mar} THEN {mx} ELSE {mar} - 1 END) AS hi, "
        f"{cur.nullable_sql(CFG)} AS nullable FROM [dbo].[orders]")


@pytest.mark.parametrize("typ", ["float", "real", "nvarchar", "uniqueidentifier", "bit"])
def test_unsupported_types_rejected(typ):
    with pytest.raises(ValueError, match="not supported for load_type 'cursor'"):
        cur.high_watermark_sql(CFG, typ)


def test_typed_literals_are_validated_and_sargable():
    assert cur.literal("42", "int") == "42"
    assert cur.literal("12.50", "decimal") == "12.50"
    assert cur.literal("2026-01-01T10:00:00.123", "datetime") == "CONVERT(datetime, N'2026-01-01T10:00:00.123', 126)"
    assert cur.literal("2026-01-01T10:00:00.1234567+05:30", "datetimeoffset").startswith("CONVERT(datetimeoffset, N'")
    assert cur.literal("2026-01-01", "date") == "CONVERT(date, N'2026-01-01', 126)"
    assert cur.literal("2001", "timestamp") == "CAST(CAST(2001 AS BIGINT) AS BINARY(8))"
    for value, typ in (("1; DROP TABLE x", "int"), ("2026-01-01' OR 1=1 --", "datetime2"), ("0x07D1", "timestamp")):
        with pytest.raises(ValueError, match="invalid"):
            cur.literal(value, typ)


def test_read_where_bounds():
    assert cur.read_where(CFG, "int", "10", "20", 600) == "[updated_at] > 10 AND [updated_at] <= 20"
    assert cur.read_where(CFG, "int", None, "20", 600) == "[updated_at] <= 20"   # empty at seed


def test_temporal_lower_bound_is_pulled_back_by_the_lookback():
    where = cur.read_where(CFG, "datetime2", "2026-01-01T10:00:00.1234567", "2026-01-01T11:00:00", 600)
    assert "N'2026-01-01T09:50:00.123456'" in where            # 10 min back, 7th digit dropped
    assert where.endswith("<= CONVERT(datetime2, N'2026-01-01T11:00:00', 126)")
    # exact bounds for the other kinds
    assert cur.read_where(CFG, "int", "10", "20", 600).startswith("[updated_at] > 10 ")
    assert cur.read_where(CFG, "rowversion", "10", "20", 600).startswith("[updated_at] > CAST(CAST(10")
    assert cur.read_where(CFG, "datetime2", None, "2026-01-01T11:00:00", 600).startswith("[updated_at] <=")


def test_seed_position_pulls_a_temporal_cursor_back_by_the_seed_duration():
    assert cur.seed_position("datetime2", "2026-01-01T12:00:00", 7200.4) == "2026-01-01T09:59:59"
    assert cur.seed_position("int", "500", 7200) == "500"
    assert cur.seed_position("rowversion", "500", 7200) == "500"
    assert cur.seed_position("datetime2", None, 7200) is None   # empty at seed


PG = {**CFG, "src_type": "postgresql", "src_schema": "public"}


def test_postgres_cursor_sql():
    assert cur.column_type_sql(PG) == ("SELECT data_type AS typ, (is_nullable = 'YES') AS nullable FROM "
                                       "information_schema.columns WHERE table_schema = 'public' AND "
                                       "table_name = 'orders' AND column_name = 'updated_at'")
    hw = cur.high_watermark_sql(PG, "timestamp with time zone")
    assert hw.startswith("SELECT to_char(MAX(\"updated_at\") AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US') || '+00:00' AS hi")
    assert hw.endswith(", to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US') || '+00:00' AS src_now "
                       'FROM "public"."orders"')
    assert cur.high_watermark_sql(PG, "bigint") == f'SELECT MAX("updated_at")::text AS hi, {cur.nullable_sql(PG)} AS nullable FROM "public"."orders"'
    assert cur.high_watermark_sql(PG, "timestamp without time zone").startswith(
        "SELECT to_char(MAX(\"updated_at\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.US') AS hi")
    assert ", to_char(LOCALTIMESTAMP, 'YYYY-MM-DD\"T\"HH24:MI:SS.US') AS src_now" in cur.high_watermark_sql(PG, "timestamp without time zone")
    where = cur.read_where(PG, "timestamp without time zone", "2026-01-01T10:00:00", "2026-01-01T11:00:00", 600)
    assert where == ('"updated_at" > CAST(\'2026-01-01T09:50:00\' AS timestamp without time zone) AND '
                     '"updated_at" <= CAST(\'2026-01-01T11:00:00\' AS timestamp without time zone)')
    assert cur.read_where(PG, "integer", "10", "20", 600) == '"updated_at" > 10 AND "updated_at" <= 20'
    with pytest.raises(ValueError):
        cur.literal("2026-01-01'; drop table x--", "timestamp without time zone", pg=True)


def test_temporal_cursor_still_reads_its_lookback_when_hi_is_unchanged():
    pos = {"col": "updated_at", "type": "datetime2", "value": "2026-01-01T10:00:00"}
    assert cur.plan_increment(pos, "2026-01-01T10:00:00") == ("read", "2026-01-01T10:00:00", None)
    assert cur.plan_increment(pos, None) == ("unchanged", "2026-01-01T10:00:00", None)
    assert cur.plan_increment({**pos, "type": "int", "value": "5"}, "5") == ("unchanged", "5", "inserts_only")
    assert cur.plan_increment({**pos, "type": "rowversion", "value": "5"}, "5") == ("unchanged", "5", None)


def test_minus_seconds_keeps_offset_and_rejects_garbage():
    assert cur._minus_seconds("2026-01-01T00:00:30Z", 60) == "2025-12-31T23:59:30+00:00"
    assert cur._minus_seconds("2026-01-01", 1) == "2025-12-31T23:59:59"
    with pytest.raises(ValueError):
        cur._minus_seconds("1 OR 1=1", 1)


def test_upsert_merge_has_no_delete_branch():
    sql = cur.upsert_merge_sql(CFG, "c.s.orders", "v")
    assert sql.startswith("MERGE WITH SCHEMA EVOLUTION INTO c.s.orders AS tgt USING v AS s ON tgt.`id` = s.`id`")
    assert "DELETE" not in sql and "s.op" not in sql
    assert "UPDATE SET `amount` = s.`amount`, `updated_at` = s.`updated_at`" in sql
    assert "INSERT (`id`, `amount`, `updated_at`) VALUES (s.`id`, s.`amount`, s.`updated_at`)" in sql


def test_plan_increment():
    pos = {"col": "c", "type": "int", "value": "20"}
    assert cur.plan_increment(pos, "20")[:2] == ("unchanged", "20")
    assert cur.plan_increment(pos, None)[:2] == ("unchanged", "20")          # source emptied: keep position
    assert cur.plan_increment(pos, "25")[:2] == ("read", "25")
    assert cur.plan_increment(dict(pos, value=None), "5")[:2] == ("read", "5")


class _Rows:
    def __init__(self, rows): self._rows = rows
    def first(self): return self._rows[0] if self._rows else None


def test_column_type_lookup_errors_clearly(monkeypatch):
    import sink_writer
    import source_io
    monkeypatch.setattr(source_io, "_spark", lambda: None)
    monkeypatch.setattr(source_io, "_read", lambda sp, cfg, q, **kw: _Rows([]))
    with pytest.raises(ValueError, match="cursor_column 'updated_at' not found on dbo.orders"):
        cur.column_type(CFG)
    monkeypatch.setattr(source_io, "_read", lambda sp, cfg, q, **kw: _Rows([{"typ": "float", "nullable": False}]))
    with pytest.raises(ValueError, match="not supported"):
        cur.column_type(CFG)
    monkeypatch.setattr(source_io, "_read", lambda sp, cfg, q, **kw: _Rows([{"typ": "DateTime2", "nullable": False}]))
    assert cur.column_type(CFG) == "datetime2"


def test_nullable_cursor_column_is_refused(monkeypatch):
    # A NULL cursor value is never > lo, so such rows would be silently missed by every increment.
    import sink_writer
    import source_io
    monkeypatch.setattr(source_io, "_spark", lambda: None)
    monkeypatch.setattr(source_io, "_read", lambda sp, cfg, q, **kw: _Rows([{"typ": "datetime2", "nullable": True}]))
    with pytest.raises(ValueError, match="is nullable"):
        cur.column_type(CFG)
    assert "c.is_nullable AS nullable" in cur.column_type_sql(CFG)


# ---- read_and_merge: one read feeding one MERGE, per-call temp view, always dropped -----------------

class _Result:
    def __init__(self, d): self._d = d
    def asDict(self): return dict(self._d)
    def __getitem__(self, k): return self._d[k]


class _Spark:
    def __init__(self, result=None, fail=False):
        self.result, self.fail, self.views, self.statements = result, fail, {}, []
        spark = self

        class _Catalog:
            def dropTempView(self, name): spark.views.pop(name, None)
        self.catalog = _Catalog()

    conflicts = 0

    def sql(self, stmt):
        self.statements.append(stmt)
        if self.conflicts:
            self.conflicts -= 1
            raise RuntimeError("ConcurrentAppendException: files added by a concurrent update")
        if self.fail:
            raise RuntimeError("merge failed")
        return type("R", (), {"first": lambda s_: self.result})()


class _DF:
    columns = ["id", "amount", "updated_at"]
    def __init__(self, spark, query): self.spark, self.query, self.tagged = spark, query, None
    def select(self, *cols):
        d = _DF(self.spark, self.query); d.tagged = cols; return d
    def __getitem__(self, name): return f"col({name})"     # df[name]: a column object, as PySpark's DataFrame
    def createOrReplaceTempView(self, name): self.spark.views[name] = (self.query, self.tagged)


def _merge(monkeypatch, spark, lo="10", hi="20", cfg=CFG):
    import sink_writer
    import source_io
    queries = []
    monkeypatch.setattr(source_io, "_read", lambda sp, cfg, q, **kw: (queries.append(q), _DF(sp, q))[1])
    monkeypatch.setattr(sink_writer, "_lit", lambda v: type("L", (), {"alias": lambda s_, n: f"lit({v}) AS {n}"})())
    monkeypatch.setattr(sink_writer, "WRITE_RETRY_BASE_S", 0.0)
    return cur.read_and_merge(spark, cfg, "c.s.t", "int", lo, hi, 600), queries


def test_read_and_merge_reads_window_once_and_drops_view(monkeypatch):
    spark = _Spark(result=_Result({"num_affected_rows": 7}))
    n, queries = _merge(monkeypatch, spark)
    assert n == 7 and spark.views == {}
    assert queries == ["SELECT [id], [amount], [updated_at] FROM [dbo].[orders] "
                       "WHERE [updated_at] > 10 AND [updated_at] <= 20"]
    assert len(spark.statements) == 1 and "USING cursor_staged_" in spark.statements[0]


def test_read_and_merge_views_are_unique_per_call(monkeypatch):
    spark = _Spark(result=_Result({"num_affected_rows": 0}))
    _merge(monkeypatch, spark); _merge(monkeypatch, spark)
    views = [st.split(" USING ")[1].split(" ")[0] for st in spark.statements]
    assert len(set(views)) == 2


def test_read_and_merge_failure_propagates_and_drops_view(monkeypatch):
    spark = _Spark(fail=True)
    with pytest.raises(RuntimeError, match="merge failed"):
        _merge(monkeypatch, spark)
    assert spark.views == {}


def test_read_and_merge_without_metrics_returns_none(monkeypatch):
    assert _merge(monkeypatch, _Spark(result=_Result({})))[0] is None
    assert _merge(monkeypatch, _Spark(result=None))[0] is None


def test_consolidated_cursor_merge_matches_on_client_and_pk():
    sql = cur.upsert_merge_sql(dict(CFG, client_id="t1"), "c.s.orders", "v")
    assert "ON tgt.`__client_id__` = s.`__client_id__` AND tgt.`id` = s.`id` " in sql
    assert "INSERT (`id`, `amount`, `updated_at`, `__client_id__`)" in sql


def test_read_and_merge_retries_whole_unit_on_conflict(monkeypatch):
    spark = _Spark(result=_Result({"num_affected_rows": 4}))
    spark.conflicts = 1
    n, queries = _merge(monkeypatch, spark)
    views = {st.split(" USING ")[1].split(" ")[0] for st in spark.statements}
    assert n == 4 and len(queries) == 2 and len(views) == 2 and spark.views == {}


def test_consolidated_cursor_read_is_tagged_and_merged_on_client(monkeypatch):
    spark = _Spark(result=_Result({"num_affected_rows": 1}))
    seen = {}
    real_sql = spark.sql
    def sql(stmt):
        view = stmt.split(" USING ")[1].split(" ")[0]
        seen["tagged"] = spark.views[view][1]
        return real_sql(stmt)
    spark.sql = sql
    _merge(monkeypatch, spark, cfg=dict(CFG, client_id="t1"))
    assert seen["tagged"] == ("lit(t1) AS __client_id__", "col(id)", "col(amount)", "col(updated_at)")
    assert "AND tgt.`__client_id__` = 't1' " in spark.statements[0]


def test_every_run_rechecks_nullability(monkeypatch):
    # A column altered to NULL after the seed is caught by the steady-state run's watermark query.
    import sink_writer
    import source_io
    monkeypatch.setattr(source_io, "_spark", lambda: None)
    monkeypatch.setattr(source_io, "_read", lambda sp, cfg, q, **kw: _Rows([{"hi": "20", "nullable": 0}]))
    assert cur.high_watermark(CFG, "int", 300) == ("20", False)
    monkeypatch.setattr(source_io, "_read", lambda sp, cfg, q, **kw: _Rows([{"hi": "20", "nullable": 1}]))
    with pytest.raises(ValueError, match="is nullable"):
        cur.high_watermark(CFG, "int", 300)
    assert "c.is_nullable" in cur.nullable_sql(CFG) and "N'updated_at'" in cur.nullable_sql(CFG)


# ---- safe window bounds (analysis 3.4 gaps 2, 3, 4, 5, 7, 8) ---------------------------------------
# Phase 5 adds the oracle adapter here.
DIALECTS = [pytest.param(CFG, "datetime2", id="sqlserver"),
            pytest.param(PG, "timestamp without time zone", id="postgresql")]
_ISO = re.compile(r"'(\d{4}-\d{2}-\d{2}T[^']+)'")


def _source_rows(monkeypatch, rows):
    import source_io
    monkeypatch.setattr(source_io, "_spark", lambda: None)
    monkeypatch.setattr(source_io, "_read", lambda sp, cfg, q, **kw: _Rows(rows))


def _in_window(rows, where):
    """The row keys a ``(lo, hi]`` WHERE selects, by its two temporal literals."""
    lo, hi = (datetime.fromisoformat(v) for v in _ISO.findall(where))
    return {k for k, v in rows.items() if lo < datetime.fromisoformat(v) <= hi}


@pytest.mark.parametrize("typ", ["datetime", "smalldatetime"])
def test_gap_5_datetime_literal_three_digits(typ):
    assert cur.literal("2026-01-01T10:00:00.1234567", typ) == f"CONVERT({typ}, N'2026-01-01T10:00:00.123', 126)"
    assert cur._minus_seconds("2026-01-01T10:00:00.1234567", 600, typ) == "2026-01-01T09:50:00.123"
    assert cur._minus_seconds("2026-01-01T10:00:00.1234567", 600, "datetime2") == "2026-01-01T09:50:00.123456"
    where = cur.read_where(CFG, typ, "2026-01-01T10:00:00.1234567", "2026-01-01T11:00:00.9999999", lookback_s=600)
    assert re.findall(r"\.\d+", where) == [".123", ".999"]


def test_gap_2_lookback_argument_replaces_env():
    assert not hasattr(cur, "CURSOR_OVERLAP_S") and not hasattr(cur, "os")
    where = cur.read_where(CFG, "datetime2", "2026-01-01T10:00:00", "2026-01-01T11:00:00", lookback_s=3600)
    assert "N'2026-01-01T09:00:00'" in where
    assert cur.lookback_seconds(CFG, 3600) == 3600                                   # job default
    assert cur.lookback_seconds(dict(CFG, cursor_lookback_s=120), 3600) == 120        # the row wins
    assert cur.lookback_seconds(dict(CFG, cursor_lookback_s=None), 3600) == 3600


@pytest.mark.parametrize("cfg,typ", DIALECTS)
def test_gap_4_hi_capped_at_source_clock_plus_skew(monkeypatch, cfg, typ):
    assert " AS src_now FROM" in cur.high_watermark_sql(cfg, typ)
    _source_rows(monkeypatch, [{"hi": "2099-01-01T00:00:00", "nullable": 0, "src_now": "2026-10-26T10:00:00"}])
    assert cur.high_watermark(cfg, typ, skew_s=300) == ("2026-10-26T10:05:00", True)
    _source_rows(monkeypatch, [{"hi": "2026-10-26T09:59:00", "nullable": 0, "src_now": "2026-10-26T10:00:00"}])
    assert cur.high_watermark(cfg, typ, skew_s=300) == ("2026-10-26T09:59:00", False)
    _source_rows(monkeypatch, [{"hi": None, "nullable": 0, "src_now": "2026-10-26T10:00:00"}])
    assert cur.high_watermark(cfg, typ, skew_s=300) == (None, False)


@pytest.mark.parametrize("cfg", [pytest.param(CFG, id="sqlserver"), pytest.param(PG, id="postgresql")])
def test_gap_3_identity_reads_without_lookback(cfg):
    assert cur._kind("bigint") == "identity"
    col = cur._col(cfg)
    assert cur.read_where(cfg, "bigint", "10", "20", lookback_s=3600) == f"{col} > 10 AND {col} <= 20"
    assert cur.plan_increment({"col": "id", "type": "bigint", "value": "10"}, "20") == ("read", "20", "inserts_only")
    assert cur.plan_increment({"col": "id", "type": "bigint", "value": "10"}, "10") == ("unchanged", "10", "inserts_only")
    temporal = {"col": "updated_at", "type": "datetime2", "value": "2026-01-01T00:00:00"}
    assert cur.plan_increment(temporal, "2026-01-01T00:00:00")[2] is None


@pytest.mark.parametrize("typ,pos,hi", [("int", "20", "19"), ("rowversion", "20", "19"),
                                        ("datetime2", "2026-01-02T00:00:00", "2026-01-01T00:00:00")])
def test_gap_8_hi_below_position_raises(typ, pos, hi):
    position = {"col": "c", "type": typ, "value": pos}
    with pytest.raises(ValueError, match="truncate or reload"):
        cur.plan_increment(position, hi)
    assert position["value"] == pos
    assert cur.plan_increment({"col": "c", "type": "int", "value": "9"}, "10")[:2] == ("read", "10")   # not text order


@pytest.mark.parametrize("cfg,typ", DIALECTS)
def test_gap_7_tie_rows_land_in_later_window(cfg, typ):
    rows = {"a": "2026-01-01T10:00:00", "b": "2026-01-01T10:00:00", "old": "2026-01-01T08:00:00"}
    first = cur.read_where(cfg, typ, "2026-01-01T09:00:00", "2026-01-01T10:00:00", lookback_s=600)
    assert _in_window(rows, first) == {"a", "b"}
    second = cur.read_where(cfg, typ, "2026-01-01T10:00:00", "2026-01-01T10:30:00", lookback_s=600)
    assert _in_window(rows, second) == {"a", "b"}
    sql = cur.upsert_merge_sql(cfg, "c.s.orders", "v")
    assert "ON tgt.`id` = s.`id` WHEN MATCHED THEN UPDATE SET" in sql and "DELETE" not in sql
    sink = {}
    for _ in (first, second):           # the MERGE keys on the PK: the second pass changes nothing
        sink.update({k: rows[k] for k in _in_window(rows, first)})
    assert sink == {"a": rows["a"], "b": rows["b"]}


@pytest.mark.parametrize("cfg,typ", DIALECTS)
def test_rows_below_cap_read_after_capped_hi(monkeypatch, cfg, typ):
    rows = {"late": "2099-01-01T00:00:00", "r1": "2026-10-26T10:03:00", "r2": "2026-10-26T10:08:00"}
    pos = {"col": "updated_at", "type": typ, "value": "2026-10-26T10:00:00"}
    _source_rows(monkeypatch, [{"hi": "2099-01-01T00:00:00", "nullable": 0, "src_now": "2026-10-26T10:00:00"}])
    hi, alert = cur.high_watermark(cfg, typ, skew_s=300)
    assert (hi, alert) == ("2026-10-26T10:05:00", True)
    assert _in_window(rows, cur.read_where(cfg, typ, pos["value"], hi, lookback_s=0)) == {"r1"}
    pos["value"] = hi
    _source_rows(monkeypatch, [{"hi": "2099-01-01T00:00:00", "nullable": 0, "src_now": "2026-10-26T10:10:00"}])
    hi, _ = cur.high_watermark(cfg, typ, skew_s=300)
    assert cur.plan_increment(pos, hi) == ("read", "2026-10-26T10:15:00", None)
    assert _in_window(rows, cur.read_where(cfg, typ, pos["value"], hi, lookback_s=0)) == {"r2"}
    _source_rows(monkeypatch, [{"hi": "2099-01-01T00:00:00", "nullable": 0, "src_now": "2099-01-01T00:00:00"}])
    hi, alert = cur.high_watermark(cfg, typ, skew_s=300)
    assert (hi, alert) == ("2099-01-01T00:00:00", False)
    assert "late" in _in_window(rows, cur.read_where(cfg, typ, "2026-10-26T10:15:00", hi, lookback_s=0))


@pytest.mark.parametrize("cfg", [pytest.param(dict(CFG, src_type="sqlserver"), id="sqlserver")])   # rowversion is SQL Server only
def test_rowversion_hi_stays_below_min_active_rowversion_and_reads_exact(cfg):
    sql = cur.high_watermark_sql(cfg, "rowversion")
    assert "ELSE CAST(MIN_ACTIVE_ROWVERSION() AS BIGINT) - 1 END" in sql and "src_now" not in sql
    assert cur.read_where(cfg, "rowversion", "10", "20", lookback_s=3600) == (
        "[updated_at] > CAST(CAST(10 AS BIGINT) AS BINARY(8)) AND [updated_at] <= CAST(CAST(20 AS BIGINT) AS BINARY(8))")
