"""Source adapters: connection, quoting, literals, and the size / bounds / select SQL per engine."""
from datetime import date, datetime
from decimal import Decimal

import pytest

import partitions
import source_adapters as sa

CFG = {"src_schema": "hr", "src_table": "emp", "select_list": "id, name"}


def test_registry_and_default():
    assert sa.SRC_TYPES == ("sqlserver", "oracle", "postgresql", "mysql")
    assert sa.get_adapter(None).src_type == "sqlserver"          # the column default
    assert sa.get_adapter(" Oracle ").src_type == "oracle"
    assert sa.for_cfg({"src_type": "mysql"}).src_type == "mysql"


def test_registry_rejects_unknown_src_type():
    with pytest.raises(ValueError, match="unsupported src_type 'db2'.*sqlserver, oracle"):
        sa.get_adapter("db2")


def test_jdbc_urls_and_drivers():
    ss, ora, pg, my = (sa.get_adapter(t) for t in sa.SRC_TYPES)
    assert ss.jdbc_url("h", 1433, "db", False) == \
        "jdbc:sqlserver://h:1433;databaseName=db;encrypt=true;trustServerCertificate=false"
    assert ora.jdbc_url("h", 1521, "ORCLPDB1", True) == "jdbc:oracle:thin:@//h:1521/ORCLPDB1"
    assert pg.jdbc_url("h", 5432, "app", True) == "jdbc:postgresql://h:5432/app?sslmode=require"
    assert pg.jdbc_url("h", 5432, "app", False) == ("jdbc:postgresql://h:5432/app?sslmode=verify-full"
                                                    "&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory")
    assert my.jdbc_url("h", 3306, "app", False) == \
        "jdbc:mysql://h:3306/app?sslMode=VERIFY_IDENTITY&useCursorFetch=true"
    assert (ss.driver, ora.driver, pg.driver, my.driver) == (
        "com.microsoft.sqlserver.jdbc.SQLServerDriver", "oracle.jdbc.OracleDriver",
        "org.postgresql.Driver", "com.mysql.cj.jdbc.Driver")
    assert (ss.default_port, ora.default_port, pg.default_port, my.default_port) == (1433, 1521, 5432, 3306)


def test_identifier_quoting_escapes_per_dialect():
    assert sa.get_adapter("sqlserver").quote_ident("a]b") == "[a]]b]"
    assert sa.get_adapter("oracle").quote_ident('A"B') == '"A""B"'
    assert sa.get_adapter("postgresql").quote_ident("order") == '"order"'
    assert sa.get_adapter("mysql").quote_ident("a`b") == "`a``b`"


def test_string_literals_escape_per_dialect():
    assert sa.get_adapter("sqlserver").quote_str("o'b") == "N'o''b'"
    assert sa.get_adapter("oracle").quote_str("o'b") == "'o''b'"
    assert sa.get_adapter("mysql").quote_str("a\\'b") == "'a\\\\''b'"   # backslash escaped too


def test_select_sql_per_dialect():
    assert sa.get_adapter("sqlserver").select_sql(CFG) == "SELECT [id], [name] FROM [hr].[emp]"
    assert sa.get_adapter("oracle").select_sql(CFG, where="1=0") == \
        'SELECT "id", "name" FROM "hr"."emp" WHERE 1=0'
    assert sa.get_adapter("mysql").select_sql(CFG) == "SELECT `id`, `name` FROM `hr`.`emp`"


def test_bounds_queries_quote_identifiers():
    assert sa.get_adapter("postgresql").bounds_query("s", "t", "id") == \
        'SELECT MIN("id") AS lb, MAX("id") AS ub FROM "s"."t"'
    assert sa.get_adapter("mysql").bounds_query("s", "t", "id") == \
        "SELECT MIN(`id`) AS lb, MAX(`id`) AS ub FROM `s`.`t`"


def test_table_size_queries_return_table_size_mb_and_escape():
    for t in sa.SRC_TYPES:
        q = sa.get_adapter(t).table_size_mb_query("s", "o'b")
        assert f"AS {sa.get_adapter(t).result_alias('table_size_mb')}" in q and "o''b" in q, t
    ora = sa.get_adapter("oracle").table_size_mb_query("HR", "EMP")
    assert "FROM all_tables" in ora and "dba_segments" not in ora.lower()   # least privilege
    assert "owner = 'HR' AND table_name = 'EMP'" in ora
    pg = sa.get_adapter("postgresql").table_size_mb_query("s", "t")
    assert "pg_total_relation_size(to_regclass('\"s\".\"t\"'))" in pg
    my = sa.get_adapter("mysql").table_size_mb_query("app", "t")
    assert "information_schema.tables" in my and "table_schema = 'app'" in my


def test_render_bound_literals_per_dialect():
    ts, d = datetime(2024, 1, 2, 3, 4, 5), date(2024, 1, 2)
    epoch_ts, epoch_d = partitions.get_internal_bound_value(ts), partitions.get_internal_bound_value(d)
    assert sa.get_adapter("sqlserver").render_bound(epoch_ts, ts) == "'2024-01-02 03:04:05'"
    assert sa.get_adapter("oracle").render_bound(epoch_ts, ts) == "TIMESTAMP '2024-01-02 03:04:05'"
    assert sa.get_adapter("oracle").render_bound(epoch_d, d) == "DATE '2024-01-02'"
    assert sa.get_adapter("postgresql").render_bound(epoch_d, d) == "DATE '2024-01-02'"
    assert sa.get_adapter("mysql").render_bound(epoch_ts, ts) == "'2024-01-02 03:04:05'"
    for t in sa.SRC_TYPES:
        assert sa.get_adapter(t).render_bound(42, Decimal("1.5")) == "42"
    with pytest.raises(ValueError):
        sa.get_adapter("oracle").render_bound(1, "x")


def test_partition_list_uses_the_adapter_dialect():
    ora = sa.get_adapter("oracle")
    parts = partitions.get_partition_list("HIRED", date(2024, 1, 1), date(2024, 1, 5), 2, ora)
    assert parts[0]["where_clause"] == '"HIRED" < DATE \'2024-01-03\' or "HIRED" is null'
    assert parts[1]["where_clause"] == '"HIRED" >= DATE \'2024-01-03\''
    # default stays SQL Server (backward compatible)
    assert partitions.get_partition_list("id", 1, 100, 2)[1]["where_clause"] == "[id] >= 50"


def test_read_options_defaults_and_control_overrides():
    assert sa.get_adapter("sqlserver").read_options({}) == {}
    assert sa.get_adapter("oracle").read_options({}) == {"fetchsize": "10000"}
    got = sa.get_adapter("postgresql").read_options({"jdbc_options": '{"fetchsize": 50000, "x": true}'})
    assert got == {"fetchsize": "50000", "x": "true"}


def test_jdbc_options_cannot_override_framework_options():
    for bad in ('{"url": "jdbc:x"}', '{"Password": "p"}', '{"query": "select 1"}', '{"driver": "d"}'):
        with pytest.raises(ValueError, match="may not set"):
            sa.parse_jdbc_options(bad)
    with pytest.raises(ValueError, match="JSON object"):
        sa.parse_jdbc_options("[1]")
    assert sa.parse_jdbc_options(None) == {} and sa.parse_jdbc_options("") == {}


def test_remote_query_names_the_database_per_engine():
    assert [sa.get_adapter(t).remote_database_option for t in sa.SRC_TYPES] == [
        "database", "service_name", "database", "database"]      # sqlserver, oracle, postgresql, mysql


# ---- cursor methods (contracts/source_adapter.md) ------------------------------------------------

CURSOR_DIALECTS = [pytest.param("sqlserver", "datetime2", id="sqlserver"),
                   pytest.param("oracle", "TIMESTAMP(6)", id="oracle")]
CURSOR_METHODS = ("cursor_type_sql", "type_class_of", "source_now_sql", "max_batch_sql", "cursor_literal",
                  "minus_seconds_sql", "greatest_expr", "server_version_sql", "pk_snapshot_sql",
                  "cursor_text_sql", "max_text_sql")


@pytest.mark.parametrize("src_type,typ", CURSOR_DIALECTS)
def test_cursor_type_sql_lists_the_columns(src_type, typ):
    sql = sa.get_adapter(src_type).cursor_type_sql("hr", "emp", ["upd", "o'c"])
    assert "column_name" in sql and "type_name" in sql and "is_nullable" in sql
    assert "o''c" in sql and sql.count("upd") == 1
    assert ("sys.columns" in sql) == (src_type == "sqlserver") and ("all_tab_columns" in sql) == (src_type == "oracle")
    assert "information_schema.columns" in sa.get_adapter("postgresql").cursor_type_sql("s", "t", ["c"])


def test_type_class_of_per_dialect():
    ss, ora = sa.get_adapter("sqlserver"), sa.get_adapter("oracle")
    assert ss.type_class_of("rowversion") == ss.type_class_of("timestamp") == "rowversion"
    assert ss.type_class_of("bigint") == "numeric" and ss.type_class_of(" DateTime2 ") == "temporal"
def test_oracle_quotes_result_aliases_the_engine_reads_by_name():
    # Oracle folds an unquoted alias to upper case; JDBC rows then lack row["type_name"], row["hi"].
    ora = sa.get_adapter("oracle")
    sql = ora.cursor_type_sql("HR", "EMP", ["UPD"])
    assert 'AS "column_name"' in sql and 'AS "type_name"' in sql and 'AS "is_nullable"' in sql
    assert 'AS "src_now"' in ora.max_batch_sql([("c1", "HR", "EMP", '"UPD"')], "TIMESTAMP(6)")
    assert 'AS "lb"' in ora.bounds_query("HR", "EMP", "ID") and 'AS "table_size_mb"' in ora.table_size_mb_query("HR", "EMP")
    assert ora.result_alias("hi") == '"hi"' and sa.get_adapter("sqlserver").result_alias("hi") == "hi"


    assert ora.type_class_of("NUMBER") == ora.type_class_of("NUMBER(10,0)") == ora.type_class_of("number(,0)") == "numeric"
    assert ora.type_class_of("TIMESTAMP(6)") == ora.type_class_of("DATE") == "temporal"
    assert ora.type_class_of("TIMESTAMP(6) WITH TIME ZONE") == "temporal"
def test_type_class_of_per_dialect():
    ss, ora = sa.get_adapter("sqlserver"), sa.get_adapter("oracle")
    assert ss.type_class_of("rowversion") == ss.type_class_of("timestamp") == "rowversion"
    assert ss.type_class_of("bigint") == "numeric" and ss.type_class_of(" DateTime2 ") == "temporal"
    pg = sa.get_adapter("postgresql")
    assert pg.type_class_of("timestamp without time zone") == "temporal" and pg.type_class_of("integer") == "numeric"
    for adapter, bad in ((ss, "float"), (ss, "nvarchar"), (ora, "NUMBER(10,2)"), (ora, "VARCHAR2"), (ora, "FLOAT"),
                         (pg, "timestamp"), (pg, "real")):
        with pytest.raises(ValueError, match="not supported for load_type 'cursor'"):
            adapter.type_class_of(bad)


def test_source_now_sql_matches_the_cursor_type():
    ss, ora, pg = (sa.get_adapter(t) for t in ("sqlserver", "oracle", "postgresql"))
    assert ss.source_now_sql("datetime2") == "SYSUTCDATETIME()"
    assert "SYSUTCDATETIME()" in ss.source_now_sql("datetimeoffset")      # UTC, with an offset like the column
    assert ss.source_now_sql("datetime") == ss.source_now_sql("smalldatetime") == "GETDATE()"
    assert ora.source_now_sql("TIMESTAMP(6)") == ora.source_now_sql("timestamp(6) with time zone") == "SYSTIMESTAMP"
    assert ora.source_now_sql("DATE") == "SYSDATE"
    assert pg.source_now_sql("timestamp with time zone") == "now()"
    with pytest.raises(ValueError):
        ss.source_now_sql("int")


@pytest.mark.parametrize("src_type,typ", CURSOR_DIALECTS)
def test_max_batch_sql_one_scalar_subquery_per_table_plus_the_clock(src_type, typ):
    a = sa.get_adapter(src_type)
    sql = a.max_batch_sql([("c1", "hr", "emp", a.quote_ident("upd")), ("c2", "hr", "dept", a.quote_ident("upd"))], typ)
    assert sql.startswith("SELECT (SELECT ") and sql.count("(SELECT ") == 2 and "MAX(" in sql
    assert f"FROM {a.table_ref('hr', 'emp')})" in sql and f"AS {a.result_alias('src_now')}" in sql
    assert sql.endswith(" FROM DUAL") == (src_type == "oracle")
    assert a.source_now_sql(typ) in sql
    numeric = a.max_batch_sql([("c1", "hr", "emp", a.quote_ident("seq"))], {"sqlserver": "int", "oracle": "NUMBER"}[src_type])
    assert "src_now" not in numeric and numeric.count("(SELECT ") == 1     # a numeric hi needs no clock


def test_cursor_literals_per_dialect():
    ss, ora, pg = (sa.get_adapter(t) for t in ("sqlserver", "oracle", "postgresql"))
    assert ss.cursor_literal("42", "int") == "42"
    assert ss.cursor_literal("2026-01-01T10:00:00.1234567", "datetime2") == "CONVERT(datetime2, N'2026-01-01T10:00:00.1234567', 126)"
    assert ss.cursor_literal("2026-01-01T10:00:00.1234567", "datetime") == "CONVERT(datetime, N'2026-01-01T10:00:00.123', 126)"
    assert ss.cursor_literal("2001", "timestamp") == "CAST(CAST(2001 AS BIGINT) AS BINARY(8))"
    assert ora.cursor_literal("42", "NUMBER") == "42"
    assert ora.cursor_literal("2026-01-01T10:00:00.1234567", "TIMESTAMP(6)") == \
        "TO_TIMESTAMP('2026-01-01T10:00:00.123456', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6')"
    assert ora.cursor_literal("2026-01-01T10:00:00+00:00", "TIMESTAMP(6) WITH TIME ZONE") == \
        "TO_TIMESTAMP_TZ('2026-01-01T10:00:00.000000+00:00', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM')"
    assert ora.cursor_literal("2026-01-01", "DATE") == "TO_DATE('2026-01-01T00:00:00', 'YYYY-MM-DD\"T\"HH24:MI:SS')"
    assert pg.cursor_literal("2026-01-01T10:00:00.1234567", "timestamp without time zone") == \
        "CAST('2026-01-01T10:00:00.123456' AS timestamp without time zone)"


def test_minus_seconds_sql_per_dialect():
    assert sa.get_adapter("sqlserver").minus_seconds_sql("[u]", 600, "datetime2") == "DATEADD(SECOND, -600, [u])"
    assert sa.get_adapter("oracle").minus_seconds_sql('"U"', 600, "DATE") == '"U" - NUMTODSINTERVAL(600, \'SECOND\')'
    assert sa.get_adapter("postgresql").minus_seconds_sql('"u"', 600, "date") == "\"u\" - INTERVAL '600 seconds'"


def test_greatest_expr_case_form_below_sqlserver_2022():
    ss, ora = sa.get_adapter("sqlserver"), sa.get_adapter("oracle")
    assert ss.greatest_expr(["a", "b"], "datetime2", 16) == "GREATEST(a, b)"
    assert ss.greatest_expr(["a", "b"], "datetime2", 15) == "CASE WHEN a >= b THEN a ELSE b END"
    assert ss.greatest_expr(["a", "b", "c"], "datetime2", None) == \
        "CASE WHEN a >= b AND a >= c THEN a WHEN b >= c THEN b ELSE c END"      # unknown version: safe form
    assert ss.greatest_expr(["a"], "datetime2", 15) == "a"
    assert ora.greatest_expr(["a", "b", "c"], "DATE", None) == "GREATEST(a, b, c)"
    assert sa.get_adapter("postgresql").greatest_expr(["a", "b"], "date", None) == "GREATEST(a, b)"


def test_server_version_sql_only_for_sqlserver():
    assert "SERVERPROPERTY('ProductMajorVersion')" in sa.get_adapter("sqlserver").server_version_sql()
    assert sa.get_adapter("oracle").server_version_sql() is None
    assert sa.get_adapter("postgresql").server_version_sql() is None


@pytest.mark.parametrize("src_type,typ", CURSOR_DIALECTS)
def test_pk_snapshot_sql_selects_the_pk(src_type, typ):
    a = sa.get_adapter(src_type)
    assert a.pk_snapshot_sql("hr", "emp", ["id", "ver"]) == \
        f"SELECT {a.quote_ident('id')}, {a.quote_ident('ver')} FROM {a.table_ref('hr', 'emp')}"


@pytest.mark.parametrize("method", CURSOR_METHODS)
def test_mysql_raises_not_implemented_from_every_cursor_method(method):
    args = {"cursor_type_sql": ("s", "t", ["c"]), "type_class_of": ("int",), "source_now_sql": ("datetime",),
            "max_batch_sql": ([], "datetime"), "cursor_literal": ("1", "int"), "minus_seconds_sql": ("c", 1, "datetime"),
            "greatest_expr": (["a"], "datetime", None), "server_version_sql": (), "pk_snapshot_sql": ("s", "t", ["c"]),
            "cursor_text_sql": ("c", "int"), "max_text_sql": ("c", "int")}[method]
    with pytest.raises(NotImplementedError, match="mysql"):
        getattr(sa.get_adapter("mysql"), method)(*args)


# ---- US7: auth_options per secret auth_mode (contracts/secret_json.md) -------------------------------
def test_sqlserver_auth_options_per_mode():
    ss = sa.get_adapter("sqlserver")
    assert ss.auth_options({"auth_mode": "sql", "user": "u", "password": "p"}) == {"user": "u", "password": "p"}
    assert ss.auth_options({"auth_mode": "ntlm", "user": "u", "password": "p", "domain": "CORP"}) == {
        "user": "u", "password": "p", "domain": "CORP", "authenticationScheme": "NTLM", "integratedSecurity": "true"}
    assert ss.auth_options({"auth_mode": "aad_sp", "client_id": "app", "client_secret": "cs", "tenant_id": "t"}) == {
        "authentication": "ActiveDirectoryServicePrincipal", "user": "app", "password": "cs"}


@pytest.mark.parametrize("src_type", ["oracle", "postgresql", "mysql"])
def test_other_adapters_accept_sql_only(src_type):
    a = sa.get_adapter(src_type)
    assert a.auth_options({"auth_mode": "sql", "user": "u", "password": "p"}) == {"user": "u", "password": "p"}
    for mode in ("ntlm", "aad_sp"):
        with pytest.raises(NotImplementedError, match=f"{src_type}.*{mode}") as e:
            a.auth_options({"auth_mode": mode, "user": "u", "password": "p", "domain": "d",
                            "client_id": "c", "client_secret": "cs", "tenant_id": "t"})
        assert "cs" not in str(e.value).replace("aad_sp", "")
