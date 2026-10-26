"""sink_writer: MERGE and INSERT text, consolidated sinks, seeds, the write-conflict retry."""
import pytest

import sink_writer as sw
import source_io as sio


class _FakeLit:
    """Stands in for pyspark F.lit (and the cast) in sink_writer._lit."""
    def __init__(self, value, dtype=None): self.value, self.dtype, self.name = value, dtype, None
    def alias(self, name): self.name = name; return self
    def __repr__(self): return f"lit({self.value!r}) AS {self.name}"


class _Col:
    """Stands in for a pyspark Column taken from a DataFrame (``df[name]``)."""
    def __init__(self, name): self.name = name
    def __repr__(self): return f"col({self.name})"


class _Row(dict):
    def asDict(self):
        return dict(self)


class _Writer:
    def __init__(self, spark):
        self.spark = spark

    def mode(self, *_):
        return self

    def format(self, *_):
        return self

    def option(self, *_):
        return self

    def saveAsTable(self, name):
        self.spark.statements.append(f"SAVE {name}")


class _DF:
    columns = ["id", "v"]

    def __init__(self, spark, query):
        self.spark, self.query = spark, query
        self.write = _Writer(spark)

    def select(self, *cols):                       # sink_writer._tag: records the projection
        self.spark.tagged.append(cols)
        return _DF(self.spark, self.query)

    def __getitem__(self, name):                   # df[name]: a column object, as PySpark's DataFrame
        return _Col(name)

    def unionByName(self, other, allowMissingColumns=False):   # unionByName
        assert allowMissingColumns
        return _DF(self.spark, f"{self.query} UNION {other.query}")

    def first(self):
        if "table_size_mb" in self.query:
            return _Row(table_size_mb=10)
        if " AS lb" in self.query:
            return _Row(lb=1, ub=100)
        return None

    def createOrReplaceTempView(self, name):
        self.spark.views[name] = self.query


class _Catalog:
    def __init__(self, spark):
        self.spark = spark

    def dropTempView(self, name):
        self.spark.views.pop(name, None)


class _Table:
    columns = ["id", "v"]


class _FakeSpark:
    def __init__(self, fail_where=None):
        import threading
        self.statements, self.views, self.fail_where, self.tagged = [], {}, fail_where, []
        self.catalog, self._mu = _Catalog(self), threading.Lock()

    def table(self, name):
        return _Table()

    def sql(self, stmt):
        with self._mu:
            self.statements.append(stmt)
        if self.fail_where and stmt.startswith("INSERT INTO"):
            view = stmt.rsplit(" FROM ", 1)[1]
            if self.fail_where in self.views.get(view, ""):
                raise RuntimeError("partition read failed")
        return type("R", (), {"first": lambda self_: _Row(num_inserted_rows=1)})()


def _seed(monkeypatch, spark, **overrides):
    from concurrent.futures import ThreadPoolExecutor
    import parallel
    monkeypatch.setattr(sio, "_read", lambda sp, cfg, q, **kw: _DF(sp, q))
    monkeypatch.setattr(sw, "_lit", _FakeLit)
    cfg = {"id": 1, "src_schema": "dbo", "src_table": "big", "select_list": "id, v",
           "load_partitioned": True, "partition_col": "id", "partition_size_mb": 1, **overrides}
    with ThreadPoolExecutor(max_workers=4) as ex:
        return sw.seed(spark, cfg, "c.s.big", slots=parallel.WorkSlots(4), executor=ex)


_CONS = {"id": 1, "src_schema": "dbo", "src_table": "t", "select_list": "id, v", "primary_key": "id",
         "client_id": "tenant-001"}


class ConcurrentAppendException(Exception):
    pass


_ATTRS = dict(_CONS, client_attrs='{"tenant_name": "Acme", "source_server": "sql1", "app_version": 3}')


def test_merge_set_matches_keys_case_insensitively():
    assert sw.merge_set("orderLineID, note", "OrderLineID") == "`note` = s.`note`"


def test_sink_fqn_quotes_each_part():
    cfg = {"sink_catalog": "01-sales-dev", "sink_schema": "team-a-bronze",
           "sink_table": "we`ird"}
    assert sw.sink_fqn(cfg) == "`01-sales-dev`.`team-a-bronze`.`we``ird`"


def test_sink_fqn_keeps_names_that_are_already_quoted():
    # Earlier versions didn't quote, so control rows may hold quoted names already.
    cfg = {"sink_catalog": "`01-sales-dev`", "sink_schema": "bronze", "sink_table": "`we``ird`"}
    assert sw.sink_fqn(cfg) == "`01-sales-dev`.`bronze`.`we``ird`"
    assert sw.quote_delta_ident("`a`b`") == "```a``b```"   # not a quoted identifier: quote it
    assert sw.quote_delta_ident("`") == "````"


def test_staging_table_name_stays_valid_for_quoted_sinks():
    assert sw.staging_table_name("`c-1`.`s`.`t`") == "`c-1`.`s`.`t__seed_staging`"
    assert sw.staging_table_name("`c-1`.`s`.`t`", _CONS).startswith("`c-1`.`s`.`t__seed_staging__tenant_001_")
    assert sw.staging_table_name("`c-1`.`s`.`t`", _CONS).endswith("`")


def test_merge_set_excludes_pk():
    assert (sw.merge_set("order_id, amount, status", "order_id")
            == "`amount` = s.`amount`, `status` = s.`status`")      # unqualified targets (evolution)


def test_merge_set_all_pk_is_noop_safe():
    assert sw.merge_set("a, b", "a, b") == "`a` = s.`a`, `b` = s.`b`"


def test_merge_insert_cols_and_vals():
    assert sw.merge_insert_cols("order_id, amount") == "`order_id`, `amount`"
    assert sw.merge_insert_vals("order_id, amount") == "s.`order_id`, s.`amount`"


def test_merge_sql_uses_given_view_and_handles_deletes():
    cfg = {"select_list": "order_id, amount", "primary_key": "order_id"}
    q = sw.merge_sql(cfg, "c.s.orders", "ct_staged_abc")
    assert "USING ct_staged_abc AS s" in q
    assert "WHEN MATCHED AND s.op = 'D' THEN DELETE" in q
    assert "WHEN NOT MATCHED AND s.op <> 'D' THEN" in q
    assert q.startswith("MERGE WITH SCHEMA EVOLUTION INTO c.s.orders AS tgt")
    assert "ON tgt.`order_id` = s.`order_id`" in q                  # ON stays qualified
    assert "UPDATE SET `amount` = s.`amount`" in q                  # SET target unqualified
    assert "INSERT (`order_id`, `amount`) VALUES (s.`order_id`, s.`amount`)" in q
    assert "`op`" not in q                                          # CT op never lands in bronze


def test_engine_source_has_no_cache_or_persist():
    # Rule: read the source once, never .cache()/.persist() a source DataFrame.
    from pathlib import Path
    engine = Path(__file__).parent.parent / "src" / "lakefed_ingest"
    src = (engine / "source_io.py").read_text() + (engine / "sink_writer.py").read_text()
    code = "\n".join(l for l in src.splitlines() if not l.strip().startswith(("#", "\"", "``")))
    assert ".cache()" not in code.replace("``.cache()``", "")
    assert ".persist(" not in code


def test_is_partitioned_accepts_bool_and_string():
    assert sw._is_partitioned({"load_partitioned": True})
    assert sw._is_partitioned({"load_partitioned": "true"})
    assert not sw._is_partitioned({"load_partitioned": False})
    assert not sw._is_partitioned({"load_partitioned": None})
    assert not sw._is_partitioned({})


def test_staging_table_name():
    assert sw.staging_table_name("cat.sch.tbl") == "cat.sch.tbl__seed_staging"


def test_seed_has_no_partition_concurrency_param():
    import inspect
    params = inspect.signature(sw.seed).parameters
    assert "partition_concurrency" not in params
    assert "slots" in params and "executor" in params


def test_target_writes_are_data_only_insert_overwrite():
    # The seed + staging swap must replace DATA only (INSERT OVERWRITE), never re-create the target
    # (saveAsTable overwrite commits as CREATE OR REPLACE TABLE AS SELECT, which can reset liquid
    # clustering keys / properties / row filters that consolidated bronze relies on).
    import inspect
    src = inspect.getsource(sw.seed) + inspect.getsource(sw._partitioned_seed)
    assert "_insert_overwrite(" in src
    assert 'saveAsTable(sink_fqn)' not in src.replace('mode("ignore")', "")
    assert "INSERT OVERWRITE" in inspect.getsource(sw._insert_overwrite)


def test_insert_overwrite_sql_by_name_with_schema_evolution():
    sql = sw.insert_overwrite_sql("c.s.t", ["id", "name"], "v")
    assert sql == "INSERT WITH SCHEMA EVOLUTION OVERWRITE c.s.t BY NAME SELECT `id`, `name` FROM v"
    assert sw.insert_sql("INTO", "c.s.t_stage", ["id"], "v") == \
        "INSERT INTO c.s.t_stage BY NAME SELECT `id` FROM v"


def test_missing_sink_columns_is_case_insensitive():
    assert sw.missing_sink_columns("id, name", ["ID", "Name"]) == []
    assert sw.missing_sink_columns("id, city, updated_at", ["id"]) == ["city", "updated_at"]
    assert sw.missing_sink_columns("id", ["id", "extra"]) == []     # extra sink cols are fine


def test_partitioned_seed_failure_leaves_target_untouched(monkeypatch):
    import pytest
    spark = _FakeSpark(fail_where="[id] >= 37 ")      # one middle partition fails (stride 9)
    with pytest.raises(RuntimeError, match="target untouched") as raised:
        _seed(monkeypatch, spark)
    assert raised.value.__cause__ is not None     # the partition's own error, for table_retry.classify
    assert not any(s.startswith("INSERT WITH SCHEMA EVOLUTION OVERWRITE c.s.big ") for s in spark.statements)
    assert not any(s.startswith("DROP TABLE") for s in spark.statements)   # staging kept


def test_partitioned_seed_success_swaps_once_then_drops_staging(monkeypatch):
    spark = _FakeSpark()
    _seed(monkeypatch, spark)
    swaps = [s for s in spark.statements if s.startswith("INSERT WITH SCHEMA EVOLUTION OVERWRITE c.s.big ")]
    assert swaps == ["INSERT WITH SCHEMA EVOLUTION OVERWRITE c.s.big BY NAME SELECT `id`, `v` "
                     "FROM c.s.big__seed_staging"]
    assert spark.statements[-1] == "DROP TABLE IF EXISTS c.s.big__seed_staging"
    assert spark.statements.index(swaps[0]) < len(spark.statements) - 1


def test_partitioned_seed_empty_bounds_falls_back_to_single_read_seed(monkeypatch):
    # Empty table (or all-NULL partition column): no bounds -> plain one-read seed, no staging.
    class _EmptyDF(_DF):
        def first(self):
            return _Row(lb=None, ub=None) if " AS lb" in self.query else super().first()
    monkeypatch.setattr(sio, "_read", lambda sp, cfg, q, **kw: _EmptyDF(sp, q))
    spark = _FakeSpark()
    cfg = {"id": 1, "src_schema": "dbo", "src_table": "big", "select_list": "id, v",
           "load_partitioned": True, "partition_col": "id", "partition_size_mb": 1}
    sw.seed(spark, cfg, "c.s.big")
    overwrites = [s for s in spark.statements if s.startswith("INSERT WITH SCHEMA EVOLUTION OVERWRITE c.s.big ")]
    assert len(overwrites) == 1 and "__seed_staging" not in overwrites[0]
    assert not any("__seed_staging" in s for s in spark.statements)


def test_partitioned_seed_requires_partition_col(monkeypatch):
    import pytest
    monkeypatch.setattr(sio, "_read", lambda sp, cfg, q, **kw: _DF(sp, q))
    cfg = {"id": 1, "src_schema": "dbo", "src_table": "big", "select_list": "id, v",
           "load_partitioned": True, "partition_col": "", "partition_size_mb": 1}
    with pytest.raises(ValueError, match="partition_col"):
        sw.seed(_FakeSpark(), cfg, "c.s.big")


def test_partitioned_seed_guard_failure_leaves_target_untouched(monkeypatch):
    # A sweep that lost its collection lock must stop writing: the guard raising before
    # partitions / the swap fails the seed without touching the target.
    import pytest
    from concurrent.futures import ThreadPoolExecutor
    import parallel
    monkeypatch.setattr(sio, "_read", lambda sp, cfg, q, **kw: _DF(sp, q))
    cfg = {"id": 1, "src_schema": "dbo", "src_table": "big", "select_list": "id, v",
           "load_partitioned": True, "partition_col": "id", "partition_size_mb": 1}
    spark = _FakeSpark()

    def lost():
        raise RuntimeError("collection lock lost")

    with ThreadPoolExecutor(max_workers=4) as ex, pytest.raises(RuntimeError):
        sw.seed(spark, cfg, "c.s.big", slots=parallel.WorkSlots(4), executor=ex, guard=lost)
    assert not any(s.startswith("INSERT WITH SCHEMA EVOLUTION OVERWRITE c.s.big ") for s in spark.statements)


def test_ensure_sink_table_enables_type_widening_on_new_sinks(monkeypatch):
    monkeypatch.setattr(sio, "_read", lambda sp, cfg, q, **kw: _DF(sp, q))
    spark = _FakeSpark()
    spark.catalog.tableExists = lambda name: False                 # sink does not exist yet
    sw.ensure_sink_table(spark, {"src_schema": "dbo", "src_table": "t", "select_list": "id, v"}, "c.s.t")
    creates = [x for x in spark.statements if x.startswith("CREATE TABLE IF NOT EXISTS c.s.t ")]
    assert len(creates) == 1 and "TBLPROPERTIES ('delta.enableTypeWidening' = 'true') AS SELECT * FROM sink_schema_" in creates[0]
    assert spark.views == {}                                       # the 0-row view is dropped


def test_ensure_sink_table_is_noop_when_sink_exists(monkeypatch):
    spark = _FakeSpark()
    spark.catalog.tableExists = lambda name: True
    sw.ensure_sink_table(spark, {"src_schema": "dbo", "src_table": "t", "select_list": "id"}, "c.s.t")
    assert spark.statements == []


def test_seed_selects_in_the_sources_dialect(monkeypatch):
    queries = []
    monkeypatch.setattr(sio, "_read", lambda sp, cfg, q, **kw: queries.append(q) or _DF(sp, q))
    cfg = {"id": 1, "src_type": "postgresql", "src_schema": "public", "src_table": "orders",
           "select_list": "id, v"}
    sw.seed(_FakeSpark(), cfg, "c.s.orders")
    assert queries == ['SELECT "id", "v" FROM "public"."orders"']


def test_partitioned_full_load_on_mysql_uses_mysql_sql(monkeypatch):
    queries = []
    monkeypatch.setattr(sio, "_read", lambda sp, cfg, q, **kw: queries.append(q) or _DF(sp, q))
    cfg = {"id": 1, "src_type": "mysql", "load_type": "full", "src_schema": "app", "src_table": "big",
           "select_list": "id, v", "load_partitioned": True, "partition_col": "id", "partition_size_mb": 1}
    from concurrent.futures import ThreadPoolExecutor
    import parallel
    spark = _FakeSpark()
    with ThreadPoolExecutor(max_workers=4) as ex:
        sw.seed(spark, cfg, "c.s.big", slots=parallel.WorkSlots(4), executor=ex)
    assert "information_schema.tables" in queries[0]
    assert queries[1] == "SELECT MIN(`id`) AS lb, MAX(`id`) AS ub FROM `app`.`big`"
    parts = [q for q in queries[2:] if "1=0" not in q]
    assert len(parts) == 10 and all(q.startswith("SELECT `id`, `v` FROM `app`.`big` WHERE ") for q in parts)
    assert any("`id` >= " in q for q in parts)
    assert spark.statements[-1] == "DROP TABLE IF EXISTS c.s.big__seed_staging"   # staged swap


def test_client_id_validation():
    assert sw.client_id({"client_id": " tenant-001 "}) == "tenant-001"
    assert sw.client_id({"client_id": None}) is None and sw.client_id({"client_id": ""}) is None and sw.client_id({}) is None
    for bad in ("it's", "a b", "x" * 129, "a`b"):
        with pytest.raises(ValueError, match="invalid client_id"):
            sw.client_id({"client_id": bad})


def test_consolidated_columns_and_keys():
    assert sw.write_columns(_CONS) == ["id", "v", "__client_id__"]
    assert sw.merge_keys(_CONS) == ["__client_id__", "id"]
    ded = dict(_CONS, client_id=None)
    assert sw.write_columns(ded) == ["id", "v"] and sw.merge_keys(ded) == ["id"]


def test_consolidated_ct_merge_matches_on_client_and_pk():
    sql = sw.merge_sql(_CONS, "c.s.t", "v")
    assert "ON tgt.`__client_id__` = s.`__client_id__` AND tgt.`id` = s.`id` " in sql
    assert "UPDATE SET `v` = s.`v` " in sql                         # never rewrites the keys
    assert "INSERT (`id`, `v`, `__client_id__`) VALUES (s.`id`, s.`v`, s.`__client_id__`)" in sql
    assert "WHEN MATCHED AND s.op = 'D' THEN DELETE" in sql           # scoped by the ON keys


def test_consolidated_overwrite_replaces_only_this_client():
    sql = sw.insert_overwrite_sql("c.s.t", sw.write_columns(_CONS), "src", _CONS)
    assert sql == ("INSERT WITH SCHEMA EVOLUTION INTO c.s.t BY NAME REPLACE WHERE `__client_id__` = 'tenant-001' "
                   "SELECT `id`, `v`, `__client_id__` FROM src")
    assert sw.insert_overwrite_sql("c.s.t", ["id"], "src", dict(_CONS, client_id=None)).startswith(
        "INSERT WITH SCHEMA EVOLUTION OVERWRITE c.s.t BY NAME")


def test_staging_table_is_per_client():
    a = sw.staging_table_name("c.s.t", _CONS)
    b = sw.staging_table_name("c.s.t", dict(_CONS, client_id="tenant_001"))   # same slug, other client
    assert a.startswith("c.s.t__seed_staging__tenant_001_") and a != b
    assert sw.staging_table_name("c.s.t", dict(_CONS, client_id=None)) == "c.s.t__seed_staging"


def test_sink_mode_mismatch_is_refused():
    sw.check_sink_mode(_CONS, "c.s.t", ["id", "v", "__client_id__"])
    sw.check_sink_mode(dict(_CONS, client_id=None), "c.s.t", ["id", "v"])
    with pytest.raises(ValueError, match="has a '__client_id__' column but this control row is not consolidated"):
        sw.check_sink_mode(dict(_CONS, client_id=None), "c.s.t", ["id", "__CLIENT_ID__"])
    with pytest.raises(ValueError, match="has no '__client_id__' column but this control row is consolidated"):
        sw.check_sink_mode(_CONS, "c.s.t", ["id", "v"])


def test_write_retry_backs_off_on_conflicts_then_succeeds():
    calls, sleeps = [], []

    def fn():
        calls.append(1)
        if len(calls) < 3:
            raise ConcurrentAppendException("Files were added to the table by a concurrent update")
        return "done"
    assert sw.with_write_retry(fn, attempts=5, sleep=sleeps.append, rand=lambda: 1.0) == "done"
    assert len(calls) == 3 and sleeps == [sw.WRITE_RETRY_BASE_S, sw.WRITE_RETRY_BASE_S * 2]


def test_write_retry_gives_up_and_ignores_other_errors():
    sleeps = []
    with pytest.raises(RuntimeError, match="DELTA_CONCURRENT_APPEND"):
        sw.with_write_retry(lambda: (_ for _ in ()).throw(RuntimeError("[DELTA_CONCURRENT_APPEND] x")),
                            attempts=3, sleep=sleeps.append, rand=lambda: 0.5)
    assert len(sleeps) == 2
    calls = []
    with pytest.raises(ValueError):
        sw.with_write_retry(lambda: calls.append(1) or (_ for _ in ()).throw(ValueError("bad column")),
                            attempts=3, sleep=sleeps.append)
    assert calls == [1]


def test_backoff_is_capped_full_jitter():
    assert sw.backoff_delay(0, base=1, cap=60, rand=lambda: 1.0) == 1
    assert sw.backoff_delay(3, base=1, cap=60, rand=lambda: 1.0) == 8
    assert sw.backoff_delay(10, base=1, cap=60, rand=lambda: 1.0) == 60
    assert sw.backoff_delay(10, base=1, cap=60, rand=lambda: 0.0) == 0


def test_sink_created_in_one_statement_clustered_by_client():
    assert sw.create_sink_sql("c.s.t", "v", _CONS) == (
        "CREATE TABLE IF NOT EXISTS c.s.t CLUSTER BY (`__client_id__`) "
        "TBLPROPERTIES ('delta.enableTypeWidening' = 'true') AS SELECT * FROM v")
    assert sw.create_sink_sql("c.s.t", "v", dict(_CONS, client_id=None)) == (
        "CREATE TABLE IF NOT EXISTS c.s.t TBLPROPERTIES ('delta.enableTypeWidening' = 'true') AS SELECT * FROM v")


def test_consolidated_merge_has_literal_client_predicate():
    sql = sw.merge_sql(_CONS, "c.s.t", "v")
    assert ("ON tgt.`__client_id__` = s.`__client_id__` AND tgt.`id` = s.`id` AND tgt.`__client_id__` = 'tenant-001' "
            in sql)
    assert "AND tgt.`__client_id__` = '" not in sw.merge_sql(dict(_CONS, client_id=None), "c.s.t", "v")


def test_tag_prepends_client_id_literal_first(monkeypatch):
    monkeypatch.setattr(sw, "_lit", _FakeLit)
    spark = _FakeSpark()
    df = _DF(spark, "q")
    assert sw._tag(df, dict(_CONS, client_id=None)) is df and spark.tagged == []
    sw._tag(df, _CONS)
    (cols,) = spark.tagged
    assert isinstance(cols[0], _FakeLit) and cols[0].value == "tenant-001" and cols[0].name == "__client_id__"
    assert [c.name for c in cols[1:]] == ["id", "v"]      # original columns follow, in order


def test_dedicated_sink_may_have_a_native_client_id_column():
    sw.check_sink_mode(dict(_CONS, client_id=None, select_list="id, client_id, v"), "c.s.t", ["id", "client_id", "v"])


def test_ensure_sink_table_refuses_mode_mismatch(monkeypatch):
    spark = _FakeSpark()
    spark.catalog.tableExists = lambda name: True
    monkeypatch.setattr(_Table, "columns", ["id", "v", "__client_id__"])
    with pytest.raises(ValueError, match="has a '__client_id__' column"):
        sw.ensure_sink_table(spark, dict(_CONS, client_id=None), "c.s.t")
    assert not any(x.startswith("CREATE") for x in spark.statements)


def test_ensure_sink_table_consolidated_creates_clustered_in_one_statement(monkeypatch):
    monkeypatch.setattr(sio, "_read", lambda sp, cfg, q, **kw: _DF(sp, q))
    monkeypatch.setattr(sw, "_lit", _FakeLit)
    monkeypatch.setattr(_Table, "columns", ["__client_id__", "id", "v"])
    spark = _FakeSpark()
    spark.catalog.tableExists = lambda name: False
    sw.ensure_sink_table(spark, _CONS, "c.s.t")
    creates = [x for x in spark.statements if x.startswith("CREATE TABLE")]
    assert len(creates) == 1 and "CLUSTER BY (`__client_id__`)" in creates[0] and len(spark.tagged) == 1


def test_ensure_sink_table_skips_create_when_another_client_created_it(monkeypatch):
    monkeypatch.setattr(sio, "_read", lambda sp, cfg, q, **kw: _DF(sp, q))
    monkeypatch.setattr(_Table, "columns", ["__client_id__", "id", "v"])
    spark = _FakeSpark()
    answers = iter([False, True])                         # outer check: missing; inner re-check: exists
    spark.catalog.tableExists = lambda name: next(answers)
    sw.ensure_sink_table(spark, _CONS, "c.s.t")
    assert not any(x.startswith("CREATE") for x in spark.statements)


def test_consolidated_seed_replaces_only_this_client(monkeypatch):
    spark = _FakeSpark()
    _seed(monkeypatch, spark, load_partitioned=False, client_id="tenant-001")
    writes = [x for x in spark.statements if x.startswith("INSERT")]
    assert len(writes) == 1 and writes[0].startswith(
        "INSERT WITH SCHEMA EVOLUTION INTO c.s.big BY NAME REPLACE WHERE `__client_id__` = 'tenant-001' "
        "SELECT `id`, `v`, `__client_id__` FROM seed_full_")
    assert len(spark.tagged) == 1                         # the source read was tagged


def test_consolidated_partitioned_seed_uses_per_client_staging(monkeypatch):
    spark = _FakeSpark()
    _seed(monkeypatch, spark, client_id="tenant-001")
    staging = sw.staging_table_name("c.s.big", {"client_id": "tenant-001"})
    assert f"SAVE {staging}" in spark.statements
    parts = [x for x in spark.statements if x.startswith(f"INSERT INTO {staging} ")]
    assert parts and all("`__client_id__`" in x for x in parts)
    swaps = [x for x in spark.statements if "c.s.big BY NAME REPLACE WHERE" in x]
    assert swaps == [f"INSERT WITH SCHEMA EVOLUTION INTO c.s.big BY NAME REPLACE WHERE `__client_id__` = 'tenant-001' "
                     f"SELECT `id`, `v`, `__client_id__` FROM {staging}"]
    assert not any(x.startswith("INSERT WITH SCHEMA EVOLUTION OVERWRITE c.s.big") for x in spark.statements)
    assert f"DROP TABLE IF EXISTS {staging}" in spark.statements
    assert len(spark.tagged) == 1 + len(parts)            # staging schema + every partition read


def test_client_attrs_parse_in_order_as_text():
    assert sw.client_attrs(_ATTRS) == [("tenant_name", "Acme"), ("source_server", "sql1"),
                                     ("app_version", "3")]
    assert sw.client_attrs({"client_attrs": None}) == [] and sw.client_attrs({"client_attrs": " "}) == []
    assert sw.client_attrs({"client_attrs": {"a": None}}) == [("a", None)]


def test_client_attrs_validation():
    for bad, match in (('["a"]', "JSON object"), ('{"__x": 1}', "invalid client_attrs column"),
                       ('{"a b": 1}', "invalid client_attrs column"), ('{"A": 1, "a": 2}', "given twice"),
                       ('{"a": {"b": 1}}', "must be a string")):
        with pytest.raises(ValueError, match=match):
            sw.client_attrs({"client_attrs": bad})


def test_write_columns_are_select_list_then_identity_columns():
    assert sw.write_columns(_ATTRS) == ["id", "v", "__client_id__", "tenant_name", "source_server", "app_version"]
    assert sw.write_columns(dict(_CONS, client_attrs='{"tenant_name": "Acme"}')) == \
        ["id", "v", "__client_id__", "tenant_name"]
    assert sw.write_mode(_CONS) == "merge" and sw.write_mode(dict(_CONS, write_mode=" Merge ")) == "merge"
    with pytest.raises(ValueError, match="invalid write_mode 'append'"):
        sw.write_mode(dict(_CONS, write_mode="append"))


def test_sink_expr_maps_greatest_and_coalesce_and_refuses_other_functions():
    expr = "GREATEST(COALESCE(updated_at, DATE '1900-01-01'), COALESCE(created_at, DATE '1900-01-01'))"
    assert sw.sink_expr(expr) == expr and sw.sink_expr(" updated_at ") == "updated_at"
    assert sw.expression_functions("coalesce(a, N'x(', greatest(b, c))") == ["COALESCE", "GREATEST"]
    with pytest.raises(ValueError, match="CAST.*only GREATEST and COALESCE"):
        sw.sink_expr("GREATEST(CAST(a AS DATE), b)")
