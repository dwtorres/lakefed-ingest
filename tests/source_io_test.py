"""source_io: credential and connection resolution, remote_query text, the query unit."""
import pytest

import source_io as sio


_SQL_CREDS = {"auth_mode": "sql", "user": "u", "password": "p", "host": "secret-host"}


def _resolve(monkeypatch, cfg, env_host=None):
    monkeypatch.setattr(sio, "_creds", lambda scope, key: _SQL_CREDS)
    monkeypatch.setattr(sio, "_uc_host", lambda name: ("uc-host", 1500))
    sio._config_cache.clear()
    if env_host is None:
        monkeypatch.delenv("LAKEFED_SQL_HOST", raising=False)
    return sio._jdbc_config(dict({"id": 1, "secret_key": "k"}, **cfg))


_MERGE_CFG = {"id": 1, "src_schema": "dbo", "src_table": "t", "select_list": "id, v",
              "primary_key": "id"}


class _Reader:
    def __init__(self):
        self.opts = {}

    def format(self, f):
        self.opts["format"] = f
        return self

    def option(self, k, v):
        self.opts[k] = v
        return self

    def options(self, **kw):
        self.opts.update(kw)
        return self

    def load(self):
        return self.opts


_REMOTE = dict(_MERGE_CFG, id=7, src_type="sqlserver", src_connection="sales_conn", use_remote_query=True,
               src_database="sales")


def test_split_handles_blanks_and_none():
    assert sio._split("a, b ,, c ") == ["a", "b", "c"]
    assert sio._split(None) == []


def test_validate_akv_secret_name():
    assert sio.validate_akv_secret_name("tenant-db-001") == "tenant-db-001"
    for bad in ("tenant_db_001", "", "a" * 128, "has space", None):
        try:
            sio.validate_akv_secret_name(bad)
        except ValueError:
            continue
        raise AssertionError(f"expected ValueError for {bad!r}")


def test_secret_ref_prefers_control_secret_key(monkeypatch):
    monkeypatch.delenv("LAKEFED_SQL_SECRET_KEY", raising=False)
    monkeypatch.setenv("LAKEFED_SQL_SECRET_SCOPE", "kv-scope")
    cfg = {"id": 1, "secret_key": "tenant-db-001", "src_connection": "legacy_conn"}
    assert sio.secret_ref(cfg) == ("kv-scope", "tenant-db-001")


def test_secret_ref_legacy_fallback_and_missing(monkeypatch):
    monkeypatch.delenv("LAKEFED_SQL_SECRET_KEY", raising=False)
    monkeypatch.delenv("LAKEFED_SQL_SECRET_SCOPE", raising=False)
    assert sio.secret_ref({"id": 1, "src_connection": "c1"}) == ("lakefed-cursor", "c1_json")
    try:
        sio.secret_ref({"id": 2})
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError when no secret key can be derived")


def test_creds_are_keyed_per_secret_not_per_server(monkeypatch):
    # Two DBs on the SAME server with different credentials must not share a cache entry.
    monkeypatch.delenv("LAKEFED_SQL_SECRET_KEY", raising=False)
    a = {"id": 1, "src_host": "sql1", "secret_key": "db-a"}
    b = {"id": 2, "src_host": "sql1", "secret_key": "db-b"}
    assert sio.secret_ref(a) != sio.secret_ref(b)
    sa, ka = sio.secret_ref(a)
    sb, kb = sio.secret_ref(b)
    assert sio.config_cache_key("sql1", 1433, sa, ka) != sio.config_cache_key("sql1", 1433, sb, kb)


def test_jdbc_url_trust_flag():
    assert sio.jdbc_url("h", 1433, "db", True).endswith("trustServerCertificate=true")
    assert sio.jdbc_url("h", 1433, "db", False).endswith("trustServerCertificate=false")
    assert "databaseName=db;encrypt=true" in sio.jdbc_url("h", 1433, "db", True)


def test_trust_server_certificate_env(monkeypatch):
    monkeypatch.setenv("LAKEFED_SQL_TRUST_SERVER_CERT", "false")
    assert sio.trust_server_certificate() is False
    monkeypatch.setenv("LAKEFED_SQL_TRUST_SERVER_CERT", "true")
    assert sio.trust_server_certificate() is True


def test_unit_without_slots_is_noop():
    with sio._unit(None):
        pass


def test_jdbc_config_src_host_wins_and_port_defaults_1433(monkeypatch):
    conf = _resolve(monkeypatch, {"src_host": "h1", "src_connection": "legacy"})
    assert (conf["host"], conf["port"]) == ("h1", 1433)


def test_jdbc_config_legacy_uc_connection_when_no_src_host(monkeypatch):
    conf = _resolve(monkeypatch, {"src_connection": "legacy"})
    assert (conf["host"], conf["port"]) == ("uc-host", 1500)


def test_jdbc_config_secret_host_last_resort(monkeypatch):
    conf = _resolve(monkeypatch, {})
    assert conf["host"] == "secret-host"


def test_jdbc_config_no_host_raises(monkeypatch):
    monkeypatch.setattr(sio, "_creds", lambda scope, key: dict(_SQL_CREDS, host=None))
    sio._config_cache.clear()
    monkeypatch.delenv("LAKEFED_SQL_HOST", raising=False)
    import pytest
    with pytest.raises(ValueError):
        sio._jdbc_config({"id": 1, "secret_key": "k"})


def test_quoting_doubles_closing_bracket_and_quote():
    assert sio.quote_ident("a]b") == "[a]]b]"
    assert sio.quote_nstr("o'k") == "N'o''k'"


def test_read_uses_the_rows_adapter_url_driver_and_options(monkeypatch):
    monkeypatch.setattr(sio, "_creds", lambda scope, key: dict(_SQL_CREDS, host=None))
    monkeypatch.setenv("LAKEFED_SQL_TRUST_SERVER_CERT", "false")
    sio._config_cache.clear()
    spark = type("S", (), {"read": property(lambda self: _Reader())})()
    cfg = {"id": 1, "secret_key": "k", "src_type": "oracle", "src_host": "ora1", "src_database": "SVC",
           "jdbc_options": '{"fetchsize": "50000"}'}
    got = sio._read(spark, cfg, "SELECT 1 FROM dual")
    assert got["url"] == "jdbc:oracle:thin:@//ora1:1521/SVC"          # adapter default port
    assert got["driver"] == "oracle.jdbc.OracleDriver" and got["fetchsize"] == "50000"
    assert "sessionInitStatement" not in got
    got = sio._read(spark, dict(cfg, src_type="sqlserver", jdbc_options=None, src_port=1500), "SELECT 1")
    assert got["url"].startswith("jdbc:sqlserver://ora1:1500;") and "fetchsize" not in got


def test_jdbc_config_port_defaults_per_adapter(monkeypatch):
    for src_type, port in (("postgresql", 5432), ("mysql", 3306), (None, 1433)):
        conf = _resolve(monkeypatch, {"src_host": "h", "src_type": src_type})
        assert conf["port"] == port


def test_uses_remote_query_reads_the_flag_as_stored():
    assert sio.uses_remote_query(_REMOTE) and sio.uses_remote_query(dict(_REMOTE, use_remote_query="true"))
    assert not sio.uses_remote_query(dict(_REMOTE, use_remote_query=False))
    assert not sio.uses_remote_query({"src_connection": "legacy"})     # the legacy host lookup stays JDBC


def test_remote_query_sql_passes_connection_database_query_and_options_as_parameters():
    sql, args = sio.remote_query_sql(dict(_REMOTE, jdbc_options='{"fetchsize": "50000"}'), "SELECT N'it''s' AS x")
    assert sql == "SELECT * FROM remote_query(:conn, database => :db, query => :q, `fetchsize` => :o0)"
    assert args == {"conn": "sales_conn", "db": "sales", "q": "SELECT N'it''s' AS x", "o0": "50000"}


def test_remote_query_sql_names_the_oracle_service():
    sql, args = sio.remote_query_sql(dict(_REMOTE, src_type="oracle", src_database="ORCLPDB1"), "SELECT 1 FROM dual")
    assert sql == "SELECT * FROM remote_query(:conn, service_name => :db, query => :q, `fetchsize` => :o0)"
    assert args["db"] == "ORCLPDB1" and args["o0"] == "10000"          # the adapter's default fetch size


def test_source_key_names_the_server_a_row_queries():
    assert sio.source_key(_REMOTE) == "connection:sales_conn"
    jdbc = dict(_MERGE_CFG, src_type="sqlserver", src_host="SQL01.example", src_port=1500)
    assert sio.source_key(jdbc) == "host:sql01.example:1500"
    assert sio.source_key(dict(jdbc, src_port=None)) == "host:sql01.example:1433"           # the adapter's port
    assert sio.source_key(dict(jdbc, src_connection="lookup")) == "host:sql01.example:1500"  # src_host wins
    assert sio.source_key({"src_type": "sqlserver", "src_connection": "Legacy"}) == "connection:legacy"


def test_units_take_a_permit_of_each_rows_source():
    import parallel
    limiter = parallel.SourceLimiter({"connection:sales_conn": 2})
    slots = parallel.WorkSlots(4, sources=limiter)
    with sio._unit(slots, _REMOTE):
        assert limiter.peak == {"connection:sales_conn": 1}
    with sio._units(slots, [_REMOTE, _REMOTE, dict(_REMOTE, src_connection="other")]):
        assert limiter.peak == {"connection:sales_conn": 2, "connection:other": 1}
    with sio._unit(slots):                                    # Delta-only units take no permit
        assert slots.in_flight == 1
    assert sio._unit(None, _REMOTE) is not None and sio._units(None, [_REMOTE]) is not None


def test_server_version_is_queried_once_per_source(monkeypatch):
    reads = []

    class _Rows:
        def __init__(self, rows): self._rows = rows
        def first(self): return self._rows[0] if self._rows else None
    monkeypatch.setattr(sio, "_spark", lambda: None)
    monkeypatch.setattr(sio, "_read", lambda sp, cfg, q, **kw: reads.append(q) or _Rows([{"server_version": 15}]))
    sio._server_versions.clear()
    a = dict(_MERGE_CFG, src_type="sqlserver", src_host="h1", src_database="d1")
    assert sio.server_version(a) == 15 and sio.server_version(dict(a, src_database="d2")) == 15
    assert len(reads) == 1 and "ProductMajorVersion" in reads[0]
    assert sio.server_version(dict(a, src_host="h2")) == 15 and len(reads) == 2     # another server
    assert sio.server_version(dict(a, src_type="oracle")) is None and len(reads) == 2  # no version-dependent SQL


def test_read_pk_snapshot_selects_only_the_pk_through_the_read_path(monkeypatch):
    reads = []
    monkeypatch.setattr(sio, "_read", lambda sp, cfg, q, **kw: reads.append((sp, cfg["id"], q)) or "df")
    cfg = dict(_MERGE_CFG, src_type="sqlserver", primary_key="id, ver", select_list="id, ver, v, w")
    assert sio.read_pk_snapshot("spark", cfg) == "df"
    assert reads == [("spark", 1, "SELECT [id], [ver] FROM [dbo].[t]")]
    ora = dict(cfg, src_type="oracle", src_schema="hr")
    sio.read_pk_snapshot("spark", ora)
    assert reads[-1][2] == 'SELECT "id", "ver" FROM "hr"."t"'


# ---- US7: auth modes in the secret JSON (contracts/secret_json.md, FR-034 to FR-036) -------------------
SECRET_VALUE, CLIENT_SECRET_VALUE = "p4ss-Value-9", "cs-Value-77"


def test_extract_creds_without_mode_is_sql():
    creds = sio._extract_creds({"user": "svc", "password": SECRET_VALUE})
    assert creds["auth_mode"] == "sql"
    assert (creds["user"], creds["password"]) == ("svc", SECRET_VALUE)


def test_extract_creds_ntlm_needs_domain():
    with pytest.raises(ValueError, match="domain") as e:
        sio._extract_creds('{"auth_mode": "ntlm", "user": "svc", "password": "%s"}' % SECRET_VALUE)
    assert SECRET_VALUE not in str(e.value)
    creds = sio._extract_creds({"auth_mode": "ntlm", "user": "svc", "password": SECRET_VALUE, "domain": "CORP"})
    assert (creds["auth_mode"], creds["domain"]) == ("ntlm", "CORP")


def test_extract_creds_aad_sp_needs_tenant_id():
    with pytest.raises(ValueError, match="tenant_id") as e:
        sio._extract_creds({"auth_mode": "aad_sp", "client_id": "app", "client_secret": CLIENT_SECRET_VALUE})
    assert CLIENT_SECRET_VALUE not in str(e.value)


def test_extract_creds_unknown_mode_lists_the_modes_and_hides_values():
    with pytest.raises(ValueError, match="sql.*ntlm.*aad_sp") as e:
        sio._extract_creds({"auth_mode": "kerberos", "user": "svc", "password": SECRET_VALUE,
                            "client_secret": CLIENT_SECRET_VALUE})
    assert SECRET_VALUE not in str(e.value) and CLIENT_SECRET_VALUE not in str(e.value)


def test_redact_hides_every_secret_value():
    creds = {"auth_mode": "aad_sp", "client_id": "app", "client_secret": CLIENT_SECRET_VALUE,
             "tenant_id": "t", "password": SECRET_VALUE}
    text = sio.redact(f"login failed for {SECRET_VALUE} / {CLIENT_SECRET_VALUE}", creds)
    assert SECRET_VALUE not in text and CLIENT_SECRET_VALUE not in text and "login failed" in text
    assert sio.redact("plain", None) == "plain"


def test_jdbc_config_carries_the_adapter_auth_options(monkeypatch):
    monkeypatch.setattr(sio, "_creds", lambda scope, key: {"auth_mode": "ntlm", "user": "u", "password": "p",
                                                           "domain": "CORP", "host": None})
    sio._config_cache.clear()
    conf = sio._jdbc_config({"id": 1, "secret_key": "k", "src_host": "h1"})
    assert conf["options"]["authenticationScheme"] == "NTLM" and conf["options"]["domain"] == "CORP"
