"""Source reads: Spark JDBC or ``remote_query`` through a Unity Catalog connection.

The row's source adapter (``source_adapters``, chosen by ``control.src_type``) gives the JDBC URL,
the driver, the quoting and the SELECT text. This module resolves the connection and runs the
query. ``sink_writer`` writes the result to Delta.

Connection (per control row ``cfg``):
  * host and port: ``cfg['src_host']`` and ``cfg['src_port']``, else the UC connection named in
    ``cfg['src_connection']``, else the secret's own host field, else env ``LAKEFED_SQL_HOST``.
  * port default: the adapter's default port.
  * credentials: secret ``scope``/``key`` holding JSON ``{user, password}``. The scope is env
    ``LAKEFED_SQL_SECRET_SCOPE`` (default ``lakefed_ingest``). The key is ``cfg['secret_key']``,
    else env ``LAKEFED_SQL_SECRET_KEY``, else legacy ``f"{src_connection}_json"``. Credentials are
    cached per (scope, key).
  * TLS: env ``LAKEFED_SQL_TRUST_SERVER_CERT`` (default ``true``), mapped per adapter.
  * extra read options: adapter defaults plus ``cfg['jdbc_options']`` (JSON).
  * ``cfg['use_remote_query']``: read through ``remote_query(<src_connection>, ...)`` instead. The
    connection holds the credentials.

Concurrency: a function that queries the source takes an optional ``slots``
(``parallel.WorkSlots``) and holds ONE slot per unit of work. With ``slots=None`` work runs
unthrottled.

Heavy imports (pyspark, databricks.sdk) are lazy, so the pure helpers import without them.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import threading
from typing import List, Optional, Tuple

import source_adapters

_DEFAULT_SECRET_SCOPE = "lakefed_ingest"


_SQLSERVER = source_adapters.get_adapter(source_adapters.SQLSERVER)


_USER_KEYS = ("user", "username", "userName", "login", "uid")


_PWD_KEYS = ("password", "pwd", "passwd")


_HOST_KEYS = ("host", "server", "dsn")


def _split(csv: Optional[str]) -> List[str]:
    """Split a comma-separated control-table field (primary_key / select_list) into names."""
    return [c.strip() for c in (csv or "").split(",") if c.strip()]


def _without(cols: List[str], names: List[str]) -> List[str]:
    """``cols`` minus ``names``, compared case-insensitively (SQL Server and Delta both resolve
    column names that way, so ``OrderLineID`` in primary_key and ``orderLineID`` in
    select_list are the same column).

    Raises when ``cols`` has two names that differ only by case: on a case-sensitive collation they
    are two columns, and matching one of them to the key would silently drop the other (Delta can't
    hold both anyway)."""
    seen = {}
    for c in cols:
        if seen.setdefault(c.lower(), c) != c:
            raise ValueError(f"select_list has columns that differ only by case ({seen[c.lower()]}, {c}); "
                             "Delta compares column names ignoring case, so they can't both be written")
    drop = {n.lower() for n in names}
    return [c for c in cols if c.lower() not in drop]


_AKV_NAME = re.compile(r"^[0-9A-Za-z-]{1,127}$")


def validate_akv_secret_name(name: str) -> str:
    """Return ``name`` if it is a valid Azure Key Vault secret name, else raise ValueError.

    AKV secret names allow only alphanumerics and dashes (1-127 chars) — e.g. ``tenant-db-001``,
    not ``tenant_db_001``. Databricks-backed scopes are more permissive, so this is not enforced at
    runtime (the scope type isn't cheaply detectable); use it when generating ``control.secret_key``
    values for an AKV-backed scope.
    """
    if not isinstance(name, str) or not _AKV_NAME.match(name):
        raise ValueError(
            f"invalid Azure Key Vault secret name {name!r}: use 1-127 alphanumerics or dashes")
    return name


def secret_ref(cfg: dict) -> Tuple[str, str]:
    """(scope, key) holding this task's SQL Server credentials — the creds cache key.

    key precedence: ``cfg['secret_key']`` -> env ``LAKEFED_SQL_SECRET_KEY`` -> legacy
    ``f"{src_connection}_json"``. Never derived from the server alone: each tenant database may
    carry its own credential even when it shares a server with others.
    """
    scope = os.environ.get("LAKEFED_SQL_SECRET_SCOPE", _DEFAULT_SECRET_SCOPE)
    key = cfg.get("secret_key") or os.environ.get("LAKEFED_SQL_SECRET_KEY")
    if not key and cfg.get("src_connection"):
        key = f"{cfg['src_connection']}_json"
    if not key:
        raise ValueError(f"control row {cfg.get('id')}: no secret_key (and no legacy src_connection)")
    return scope, key


def config_cache_key(host: str, port: int, scope: str, key: str) -> tuple:
    """Resolved-connection cache key: (host, port, scope, secret key)."""
    return (host, int(port), scope, key)


def trust_server_certificate() -> bool:
    return os.environ.get("LAKEFED_SQL_TRUST_SERVER_CERT", "true").strip().lower() in ("1", "true", "yes")


def jdbc_url(host: str, port: int, database: str, trust_cert: bool, src_type: str = None) -> str:
    return source_adapters.get_adapter(src_type).jdbc_url(host, port, database, trust_cert)


_lock = threading.Lock()
_creds_cache: dict = {}    # (scope, key) -> (user, password, host-from-secret)
_uc_host_cache: dict = {}  # legacy: UC connection name -> (host, port)
_config_cache: dict = {}   # config_cache_key(...) -> {host, port, user, password}


def _extract_creds(blob):
    creds = json.loads(blob) if isinstance(blob, str) else dict(blob)

    def pick(d, keys):
        return next((d[k] for k in keys if isinstance(d.get(k), str)), None)

    for cand in [creds] + [v for v in creds.values() if isinstance(v, dict)]:
        user, pwd = pick(cand, _USER_KEYS), pick(cand, _PWD_KEYS)
        if user and pwd:
            return user, pwd, pick(cand, _HOST_KEYS)
    raise ValueError("could not find user/password fields in the source credential secret")


def _creds(scope: str, key: str):
    with _lock:
        if (scope, key) not in _creds_cache:
            from databricks.sdk.runtime import dbutils  # driver-side secret read
            _creds_cache[(scope, key)] = _extract_creds(dbutils.secrets.get(scope, key))
        return _creds_cache[(scope, key)]


def _uc_host(conn_name: str):
    """Legacy fallback: host/port from a UC connection (no creds come from it)."""
    with _lock:
        if conn_name not in _uc_host_cache:
            host = port = None
            try:
                from databricks.sdk import WorkspaceClient
                opts = (WorkspaceClient().connections.get(conn_name).options or {})
                host, port = opts.get("host"), opts.get("port")
            except Exception:
                pass
            _uc_host_cache[conn_name] = (host, port)
        return _uc_host_cache[conn_name]


def _jdbc_config(cfg: dict) -> dict:
    """Resolve {host, port, user, password} for this control row (cached per host/port/secret)."""
    scope, key = secret_ref(cfg)
    user, pwd, secret_host = _creds(scope, key)
    host, port = cfg.get("src_host"), cfg.get("src_port")
    if not host and cfg.get("src_connection"):
        host, uc_port = _uc_host(cfg["src_connection"])
        port = port or uc_port
    host = host or secret_host or os.environ.get("LAKEFED_SQL_HOST")
    if not host:
        raise ValueError(f"control row {cfg.get('id')}: no src_host could be resolved")
    port = int(port or source_adapters.for_cfg(cfg).default_port)
    ck = config_cache_key(host, port, scope, key)
    with _lock:
        if ck not in _config_cache:
            _config_cache[ck] = {"host": host, "port": port, "user": user, "password": pwd}
        return _config_cache[ck]


def uses_remote_query(cfg: dict) -> bool:
    """The row reads through the Unity Catalog connection ``src_connection`` (``remote_query``), not
    Spark JDBC with a secret."""
    v = cfg.get("use_remote_query")
    return v is True or str(v).lower() == "true"


def remote_query_sql(cfg: dict, query: str) -> Tuple[str, dict]:
    """``SELECT * FROM remote_query(...)`` running ``query`` (in the source's dialect) through the row's
    connection, and its named parameters for ``spark.sql``. The connection, the database (Oracle: the
    service name) and the query go in as parameter markers, so the query needs no escaping. The read
    options (adapter defaults + ``jdbc_options``) become the function's options, e.g. ``fetchsize``;
    one it doesn't support fails the read."""
    import sink_writer
    adapter = source_adapters.for_cfg(cfg)
    args = {"conn": cfg["src_connection"], "db": cfg["src_database"], "q": query}
    named = [f"{adapter.remote_database_option} => :db", "query => :q"]
    for i, (name, value) in enumerate(adapter.read_options(cfg).items()):
        named.append(f"{sink_writer.quote_delta_ident(name)} => :o{i}")
        args[f"o{i}"] = value
    return f"SELECT * FROM remote_query(:conn, {', '.join(named)})", args


def _spark():
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _read(spark, cfg: dict, query: str, session_init: Optional[str] = None):
    """Run a query (in the source's dialect) via Spark JDBC, or through the row's Unity Catalog
    connection (``use_remote_query``); returns a natively-typed DataFrame. Lazy: the source is only
    queried when the DataFrame is consumed (once, by the write/MERGE)."""
    if uses_remote_query(cfg):
        if session_init:
            raise ValueError(f"control row {cfg.get('id')}: a read through a connection runs no session statement")
        sql, args = remote_query_sql(cfg, query)
        return spark.sql(sql, args=args)
    adapter = source_adapters.for_cfg(cfg)
    conf = _jdbc_config(cfg)
    return (
        spark.read.format("jdbc")
        .options(**adapter.read_options(cfg))
        .option("url", adapter.jdbc_url(conf["host"], conf["port"], cfg["src_database"],
                                        trust_server_certificate()))
        .option("query", query)
        .option("user", conf["user"])
        .option("password", conf["password"])
        .option("driver", adapter.driver)
        .options(**({"sessionInitStatement": session_init} if session_init else {}))
        .load()
    )


def _unit(slots, cfg: Optional[dict] = None):
    """One unit of query work: hold ONE slot of the sweep's budget (no-op when ``slots`` is None) and,
    for a unit that queries ``cfg``'s source, a permit of that source when the sweep caps queries per
    source (``max_queries_per_source``). Delta-only units pass no ``cfg``."""
    if slots is None:
        return contextlib.nullcontext()
    return slots.acquire(source_key(cfg) if cfg is not None else None)


def source_key(cfg: dict) -> str:
    """The server a control row queries, as ``max_queries_per_source`` counts it: its Unity Catalog
    connection when it reads through one (or takes its host from it), else ``host:port``. Rows that
    read one server through different names are counted separately."""
    if uses_remote_query(cfg) or (cfg.get("src_connection") and not cfg.get("src_host")):
        return f"connection:{str(cfg['src_connection']).lower()}"
    port = int(cfg.get("src_port") or source_adapters.for_cfg(cfg).default_port)
    return f"host:{str(cfg.get('src_host') or '').lower()}:{port}"


def quote_ident(name: str) -> str:
    """SQL Server bracket-quoted identifier (``]`` doubled), for SQL Server catalog queries."""
    return _SQLSERVER.quote_ident(name)


def quote_nstr(value: str) -> str:
    """SQL Server N'...' unicode string literal (``'`` doubled)."""
    return _SQLSERVER.quote_str(value)


def _units(slots, cfgs: List[dict]):
    """One unit per row of ``cfgs`` at once (a batch: one source query per database): hold that many
    slots, and a permit of each row's source when queries per source are capped."""
    if slots is None:
        return contextlib.nullcontext()
    return slots.acquire_many(len(cfgs), sources=[source_key(c) for c in cfgs])
