"""Source adapters: everything that depends on the SOURCE DATABASE ENGINE (``control.src_type``).

The engine (``source_io.py``, ``sink_writer.py``, ``cursor_engine.py``) and the partitioned seed
(``partitions.py``) are written once against
this interface; an adapter supplies the dialect-specific pieces:

* the JDBC URL, driver class, and default Spark JDBC read options (e.g. fetch size), and the
  ``remote_query`` option naming the database (a read through a Unity Catalog connection);
* identifier quoting and string / bound literals;
* the queries a load strategy needs: plain ``SELECT`` of the select_list, table size in MB and
  MIN/MAX bounds (partitioned seed);
* the cursor methods (contracts/source_adapter.md): cursor type lookup and class, source clock,
  canonical text rendering, typed literals, lookback, batched ``MAX``, ``GREATEST`` form. An adapter
  without them (MySQL) raises ``NotImplementedError``; ``load_strategies`` reports it at plan time.

``load_strategies.supported`` gates which (src_type, load_type) pairs can run.

Driver availability on Databricks Runtime (the sweep uses Spark JDBC on the cluster):

* SQL Server  — ``com.microsoft.sqlserver.jdbc.SQLServerDriver``, bundled in DBR.
* PostgreSQL  — ``org.postgresql.Driver``, bundled in DBR.
* Oracle      — ``oracle.jdbc.OracleDriver``, NOT bundled: add a cluster library, e.g. Maven
  ``com.oracle.database.jdbc:ojdbc11:<version>`` (see resources/lakefed_ingest.yml).
* MySQL       — ``com.mysql.cj.jdbc.Driver``, NOT bundled: add a cluster library, e.g. Maven
  ``com.mysql:mysql-connector-j:<version>`` (see resources/lakefed_ingest.yml).

Pure Python (no Spark / DB imports) — unit-tested in tests/source_adapters_test.py.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

SQLSERVER, ORACLE, POSTGRESQL, MYSQL = "sqlserver", "oracle", "postgresql", "mysql"

# Spark JDBC options a control row must NOT override via ``control.jdbc_options``: connection target,
# credentials, driver, and the query itself are owned by the framework.
_RESERVED_OPTIONS = {"url", "user", "password", "driver", "query", "dbtable"}


def _split(csv: Optional[str]) -> List[str]:
    return [c.strip() for c in (csv or "").split(",") if c.strip()]


def parse_jdbc_options(raw) -> Dict[str, str]:
    """``control.jdbc_options`` (JSON object text, a dict, or NULL) -> {option: str(value)}.

    Extra Spark JDBC read options per control row (e.g. ``{"fetchsize": "50000"}``). Reserved
    options (url, user, password, driver, query, dbtable) are rejected — they are framework-owned."""
    if raw is None or raw == "":
        return {}
    opts = json.loads(raw) if isinstance(raw, str) else dict(raw)
    if not isinstance(opts, dict):
        raise ValueError("jdbc_options must be a JSON object")
    bad = sorted(k for k in opts if str(k).lower() in _RESERVED_OPTIONS)
    if bad:
        raise ValueError(f"jdbc_options may not set {', '.join(bad)}")
    return {str(k): (str(v).lower() if isinstance(v, bool) else str(v)) for k, v in opts.items()}


class SourceAdapter:
    """Dialect for one source engine. Subclasses override the class attributes and the methods
    whose SQL differs; the defaults are ANSI (double-quoted identifiers, '' string escaping)."""

    src_type: str = ""
    default_port: int = 0
    driver: str = ""
    # Default Spark JDBC read options (control.jdbc_options overrides per row).
    default_options: Dict[str, str] = {}
    # The remote_query option naming src_database (a read through a Unity Catalog connection).
    remote_database_option: str = "database"

    # ---- connection ---------------------------------------------------------------------
    def jdbc_url(self, host: str, port: int, database: str, trust_cert: bool) -> str:
        raise NotImplementedError

    def read_options(self, cfg: dict) -> Dict[str, str]:
        """Adapter defaults overlaid with the control row's ``jdbc_options``."""
        return {**self.default_options, **parse_jdbc_options(cfg.get("jdbc_options"))}

    # ---- quoting / literals ---------------------------------------------------------------
    def quote_ident(self, name: str) -> str:
        return '"' + str(name).replace('"', '""') + '"'

    def quote_str(self, value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def table_ref(self, schema: str, table: str) -> str:
        return f"{self.quote_ident(schema)}.{self.quote_ident(table)}"

    def render_bound(self, epoch_or_number: int, original) -> str:
        """SQL literal for a partition bound. ``original`` is the MIN value read from the source (its
        type picks the literal form); numeric bounds pass through unchanged."""
        if isinstance(original, (int, float, Decimal)) and not isinstance(original, bool):
            return str(epoch_or_number)
        if isinstance(original, datetime):
            return self.timestamp_literal(_utc(epoch_or_number).strftime("%Y-%m-%d %H:%M:%S"))
        if isinstance(original, date):
            return self.date_literal(_utc(epoch_or_number).strftime("%Y-%m-%d"))
        raise ValueError(
            f"Unsupported data type: {type(original)}. Only int, date, and datetime are supported")

    def timestamp_literal(self, text: str) -> str:
        return f"TIMESTAMP '{text}'"

    def date_literal(self, text: str) -> str:
        return f"DATE '{text}'"

    # ---- queries -----------------------------------------------------------------------
    def select_cols(self, select_list: str) -> str:
        return ", ".join(self.quote_ident(c) for c in _split(select_list))

    def select_sql(self, cfg: dict, where: Optional[str] = None) -> str:
        """``SELECT <select_list> FROM <schema>.<table> [WHERE ...]`` — the seed / full-load read."""
        sql = f"SELECT {self.select_cols(cfg['select_list'])} FROM {self.table_ref(cfg['src_schema'], cfg['src_table'])}"
        return f"{sql} WHERE {where}" if where else sql

    def bounds_query(self, schema: str, table: str, partition_col: str) -> str:
        col = self.quote_ident(partition_col)
        return f"SELECT MIN({col}) AS lb, MAX({col}) AS ub FROM {self.table_ref(schema, table)}"

    def table_size_mb_query(self, schema: str, table: str) -> str:
        """One row ``table_size_mb`` (data + indexes) for the partitioned seed's partition count."""
        raise NotImplementedError

    # ---- cursor (load_type cursor; contracts/source_adapter.md) ------------------------------
    # Source type names by cursor type class (lower case). OracleAdapter parses its names instead.
    numeric_types: frozenset = frozenset()
    temporal_types: frozenset = frozenset()
    rowversion_types: frozenset = frozenset()
    scalar_from: str = ""      # a SELECT without a table (Oracle: " FROM DUAL")

    def _cursor_not_implemented(self, *args, **kwargs):
        raise NotImplementedError(f"src_type '{self.src_type}': cursor methods are not implemented")

    # Dialect-only methods. A subclass that supports the cursor strategy overrides all of them.
    cursor_type_sql = source_now_sql = cursor_text_sql = cursor_literal = minus_seconds_sql = _cursor_not_implemented

    def type_class_of(self, type_name: str) -> str:
        """``numeric`` | ``temporal`` | ``rowversion`` for a source type name, else ValueError."""
        t = (type_name or "").strip().lower()
        for cls, names in (("numeric", self.numeric_types), ("temporal", self.temporal_types),
                           ("rowversion", self.rowversion_types)):
            if t in names:
                return cls
        raise ValueError(self._type_error(type_name, sorted(self.numeric_types | self.temporal_types
                                                            | self.rowversion_types)))

    def _type_error(self, type_name, supported) -> str:
        return (f"cursor type '{type_name}' is not supported for load_type 'cursor' on src_type "
                f"'{self.src_type}' (supported: {', '.join(supported)})")

    def max_text_sql(self, expr: str, type_name: str) -> str:
        """The high watermark of ``expr`` as canonical text (``cursor_text_sql`` of ``MAX``)."""
        return self.cursor_text_sql(f"MAX({expr})", type_name)

    def max_batch_sql(self, tables, now_type: str) -> str:
        """One SELECT: a scalar ``MAX`` subquery per ``(alias, schema, table, expression)`` plus the
        source clock, all as canonical text in the cursor type ``now_type``."""
        cols = [f"(SELECT {self.max_text_sql(expr, now_type)} FROM {self.table_ref(schema, table)}) AS "
                f"{self.quote_ident(alias)}" for alias, schema, table, expr in tables]
        cols.append(f"{self.cursor_text_sql(self.source_now_sql(now_type), now_type)} AS src_now")
        return f"SELECT {', '.join(cols)}{self.scalar_from}"

    def greatest_expr(self, inputs, type_name: str, server_version) -> str:
        return inputs[0] if len(inputs) == 1 else f"GREATEST({', '.join(inputs)})"

    def server_version_sql(self) -> Optional[str]:
        """SQL for the major version as an integer; None when no SQL depends on the version."""
        return None

    def pk_snapshot_sql(self, schema: str, table: str, pk_columns) -> str:
        cols = ", ".join(self.quote_ident(c) for c in pk_columns)
        return f"SELECT {cols} FROM {self.table_ref(schema, table)}"


def trim_fraction(value: str, digits: int) -> str:
    """ISO 8601 text with at most ``digits`` fractional second digits."""
    return re.sub(r"(\.\d{%d})\d+" % digits, r"\1", value)


def _temporal_text(value: str) -> datetime:
    return datetime.fromisoformat(trim_fraction(str(value).strip(), 6).replace("Z", "+00:00"))


class SqlServerAdapter(SourceAdapter):
    src_type = SQLSERVER
    default_port = 1433
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    def jdbc_url(self, host, port, database, trust_cert):
        return (
            f"jdbc:sqlserver://{host}:{int(port)};databaseName={database};encrypt=true;"
            f"trustServerCertificate={'true' if trust_cert else 'false'}"
        )

    def quote_ident(self, name):
        return "[" + str(name).replace("]", "]]") + "]"

    def quote_str(self, value):
        return "N'" + str(value).replace("'", "''") + "'"

    # SQL Server converts plain string literals to the column's date/datetime type implicitly.
    def timestamp_literal(self, text):
        return f"'{text}'"

    def date_literal(self, text):
        return f"'{text}'"

    def table_size_mb_query(self, schema, table):
        # Data + index pages (as the original accelerator's _get_table_size_sqlserver).
        s = str(schema).replace("'", "''")
        t = str(table).replace("'", "''")
        return (
            "SELECT CAST(ROUND((SUM(a.total_pages) * 8) / 1024.0, 2) AS NUMERIC(36, 2)) AS table_size_mb"
            " FROM sys.tables t"
            " JOIN sys.indexes i ON t.object_id = i.object_id"
            " JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id"
            " JOIN sys.allocation_units a ON p.partition_id = a.container_id"
            " LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id"
            f" WHERE s.name = '{s}' AND t.name = '{t}'"
            " AND t.is_ms_shipped = 0 AND i.object_id > 255"
        )

    # TYPE_NAME(system_type_id) reports a rowversion as 'timestamp'.
    numeric_types = frozenset({"tinyint", "smallint", "int", "bigint", "decimal", "numeric"})
    temporal_types = frozenset({"date", "smalldatetime", "datetime", "datetime2", "datetimeoffset"})
    rowversion_types = frozenset({"timestamp", "rowversion"})
    _millisecond_types = frozenset({"datetime", "smalldatetime"})   # style 126 takes at most 3 fractional digits
    _clock = {"datetime2": "SYSUTCDATETIME()", "datetimeoffset": "TODATETIMEOFFSET(SYSUTCDATETIME(), 0)",
              "datetime": "GETDATE()", "smalldatetime": "GETDATE()", "date": "GETDATE()"}

    def cursor_type_sql(self, schema, table, columns):
        names = ", ".join(self.quote_str(c) for c in columns)
        return ("SELECT c.name AS column_name, TYPE_NAME(c.system_type_id) AS type_name, "
                "CAST(c.is_nullable AS INT) AS is_nullable FROM sys.columns c "
                f"WHERE c.object_id = OBJECT_ID({self.quote_str(self.table_ref(schema, table))}) AND c.name IN ({names})")

    def source_now_sql(self, type_name):
        try:
            return self._clock[type_name.strip().lower()]
        except KeyError:
            raise ValueError(self._type_error(type_name, sorted(self._clock))) from None

    def cursor_text_sql(self, expr, type_name):
        cls = self.type_class_of(type_name)
        if cls == "numeric":
            return f"CONVERT(VARCHAR(64), {expr})"
        if cls == "temporal":
            return f"CONVERT(VARCHAR(64), {expr}, 126)"
        return f"CONVERT(VARCHAR(20), CAST({expr} AS BIGINT))"

    def max_text_sql(self, expr, type_name):
        if self.type_class_of(type_name) != "rowversion":
            return super().max_text_sql(expr, type_name)
        # Capped below MIN_ACTIVE_ROWVERSION(): rows of still-open transactions are never skipped.
        mx, mar = f"CAST(MAX({expr}) AS BIGINT)", "CAST(MIN_ACTIVE_ROWVERSION() AS BIGINT)"
        return f"CONVERT(VARCHAR(20), CASE WHEN {mx} < {mar} THEN {mx} ELSE {mar} - 1 END)"

    def cursor_literal(self, value, type_name):
        """A typed literal for validated position text (``cursor_engine.literal`` checks the shape)."""
        t, cls = type_name.strip().lower(), self.type_class_of(type_name)
        if cls == "numeric":
            return value
        if cls == "temporal":
            return f"CONVERT({t}, {self.quote_str(trim_fraction(value, 3 if t in self._millisecond_types else 7))}, 126)"
        return f"CAST(CAST({value} AS BIGINT) AS BINARY(8))"

    def minus_seconds_sql(self, expr, seconds, type_name):
        return f"DATEADD(SECOND, -{int(seconds)}, {expr})"

    def greatest_expr(self, inputs, type_name, server_version):
        if server_version is not None and int(server_version) >= 16:   # GREATEST exists from SQL Server 2022
            return super().greatest_expr(inputs, type_name, server_version)
        if len(inputs) == 1:
            return inputs[0]
        whens = " ".join("WHEN " + " AND ".join(f"{x} >= {y}" for y in inputs[i + 1:]) + f" THEN {x}"
                         for i, x in enumerate(inputs[:-1]))
        return f"CASE {whens} ELSE {inputs[-1]} END"

    def server_version_sql(self):
        return "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) AS server_version"


class OracleAdapter(SourceAdapter):
    """Oracle via the thin driver. ``src_database`` is the SERVICE NAME; ``src_schema`` the owner.

    Identifiers are double-quoted, which makes them case-sensitive: control values must match the
    stored (normally UPPER-case) names, and the Delta sink columns take that case."""

    src_type = ORACLE
    default_port = 1521
    driver = "oracle.jdbc.OracleDriver"
    default_options = {"fetchsize": "10000"}   # the driver default (10 rows/round trip) is far too small
    remote_database_option = "service_name"

    def jdbc_url(self, host, port, database, trust_cert):
        # TLS (TCPS) needs a wallet / truststore and is configured per deployment; see docs/deploy.md.
        return f"jdbc:oracle:thin:@//{host}:{int(port)}/{database}"

    def table_size_mb_query(self, schema, table):
        # Least privilege: ALL_TABLES optimizer statistics (num_rows x avg_row_len) need only SELECT on
        # the table. A table without statistics reports 0 MB -> the 2-partition floor applies. Exact
        # segment sizes (DBA_SEGMENTS) need extra privileges — see docs/deploy.md.
        return (
            "SELECT ROUND(NVL(num_rows, 0) * NVL(avg_row_len, 0) / 1048576, 2) AS table_size_mb"
            f" FROM all_tables WHERE owner = {self.quote_str(schema)} AND table_name = {self.quote_str(table)}"
        )

    # all_tab_columns.data_type: NUMBER (rendered NUMBER(p,s) by cursor_type_sql when a scale is set),
    # DATE, TIMESTAMP(n)[ WITH [LOCAL] TIME ZONE]. No rowversion.
    scalar_from = " FROM DUAL"
    _number = re.compile(r"^NUMBER(\(\d*,\s*(-?\d+)\))?$")
    _temporal = re.compile(r"^(DATE|TIMESTAMP\(\d+\)( WITH( LOCAL)? TIME ZONE)?)$")
    _iso = "YYYY-MM-DD\"T\"HH24:MI:SS"

    def type_class_of(self, type_name):
        t = (type_name or "").strip().upper()
        m = self._number.match(t)
        if m and int(m.group(2) or 0) <= 0:
            return "numeric"
        if self._temporal.match(t):
            return "temporal"
        raise ValueError(self._type_error(type_name, ["NUMBER with scale 0", "DATE", "TIMESTAMP(n)",
                                                      "TIMESTAMP(n) WITH [LOCAL] TIME ZONE"]))

    def cursor_type_sql(self, schema, table, columns):
        names = ", ".join(self.quote_str(c) for c in columns)
        return ("SELECT column_name, CASE WHEN data_type = 'NUMBER' AND data_scale IS NOT NULL THEN "
                "'NUMBER(' || data_precision || ',' || data_scale || ')' ELSE data_type END AS type_name, "
                "CASE WHEN nullable = 'Y' THEN 1 ELSE 0 END AS is_nullable FROM all_tab_columns "
                f"WHERE owner = {self.quote_str(schema)} AND table_name = {self.quote_str(table)} AND column_name IN ({names})")

    def source_now_sql(self, type_name):
        if self.type_class_of(type_name) != "temporal":
            raise ValueError(self._type_error(type_name, ["DATE", "TIMESTAMP(n)"]))
        return "SYSDATE" if type_name.strip().upper() == "DATE" else "SYSTIMESTAMP"

    def cursor_text_sql(self, expr, type_name):
        t = type_name.strip().upper()
        if self.type_class_of(type_name) == "numeric":
            return f"TO_CHAR({expr})"
        if t == "DATE":
            return f"TO_CHAR({expr}, '{self._iso}')"
        if "TIME ZONE" in t:   # rendered in UTC with an explicit offset: round-trips in any session zone
            return f"TO_CHAR(SYS_EXTRACT_UTC({expr}), '{self._iso}.FF6') || '+00:00'"
        return f"TO_CHAR({expr}, '{self._iso}.FF6')"

    def cursor_literal(self, value, type_name):
        if self.type_class_of(type_name) == "numeric":
            return value
        dt = _temporal_text(value)
        if type_name.strip().upper() == "DATE":
            return f"TO_DATE('{dt.replace(tzinfo=None).isoformat(timespec='seconds')}', '{self._iso}')"
        if dt.tzinfo is not None:
            return f"TO_TIMESTAMP_TZ('{dt.isoformat(timespec='microseconds')}', '{self._iso}.FF6TZH:TZM')"
        return f"TO_TIMESTAMP('{dt.isoformat(timespec='microseconds')}', '{self._iso}.FF6')"

    def minus_seconds_sql(self, expr, seconds, type_name):
        return f"{expr} - NUMTODSINTERVAL({int(seconds)}, 'SECOND')"


class PostgresAdapter(SourceAdapter):
    src_type = POSTGRESQL
    default_port = 5432
    driver = "org.postgresql.Driver"
    # A fetch size > 0 makes the driver stream with a cursor (Spark turns autocommit off for it);
    # with the default 0 the whole result is buffered in executor memory.
    default_options = {"fetchsize": "10000"}

    def jdbc_url(self, host, port, database, trust_cert):
        if trust_cert:
            return f"jdbc:postgresql://{host}:{int(port)}/{database}?sslmode=require"
        # verify-full against the JVM truststore (public CAs) rather than ~/.postgresql/root.crt; a
        # private CA can be supplied per row with jdbc_options {"sslrootcert": ..., "sslfactory": ...}.
        return (f"jdbc:postgresql://{host}:{int(port)}/{database}?sslmode=verify-full"
                "&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory")

    def table_size_mb_query(self, schema, table):
        rel = self.quote_str(self.table_ref(schema, table))
        return (f"SELECT ROUND(CAST(pg_total_relation_size(to_regclass({rel})) AS NUMERIC) / 1048576, 2) "
                "AS table_size_mb")

    # information_schema.columns.data_type names (never a bare 'timestamp').
    numeric_types = frozenset({"smallint", "integer", "bigint", "numeric", "decimal"})
    temporal_types = frozenset({"date", "timestamp without time zone", "timestamp with time zone"})
    _iso = "'YYYY-MM-DD\"T\"HH24:MI:SS.US'"

    def cursor_type_sql(self, schema, table, columns):
        names = ", ".join(self.quote_str(c) for c in columns)
        return ("SELECT column_name, data_type AS type_name, CASE WHEN is_nullable = 'YES' THEN 1 ELSE 0 END "
                f"AS is_nullable FROM information_schema.columns WHERE table_schema = {self.quote_str(schema)} "
                f"AND table_name = {self.quote_str(table)} AND column_name IN ({names})")

    def source_now_sql(self, type_name):
        if self.type_class_of(type_name) != "temporal":
            raise ValueError(self._type_error(type_name, sorted(self.temporal_types)))
        return "now()"

    def cursor_text_sql(self, expr, type_name):
        if self.type_class_of(type_name) == "numeric":
            return f"{expr}::text"
        if "with time zone" in type_name.lower():   # UTC with an explicit offset: round-trips in any session zone
            return f"to_char({expr} AT TIME ZONE 'UTC', {self._iso}) || '+00:00'"
        return f"to_char({expr}, {self._iso})"

    def cursor_literal(self, value, type_name):
        if self.type_class_of(type_name) == "numeric":
            return value
        return f"CAST('{trim_fraction(value, 6)}' AS {type_name.strip().lower()})"

    def minus_seconds_sql(self, expr, seconds, type_name):
        return f"{expr} - INTERVAL '{int(seconds)} seconds'"


class MySqlAdapter(SourceAdapter):
    """MySQL: a schema IS a database — set ``src_schema`` to the database name (``src_database`` is
    the connection's default database and is normally the same)."""

    src_type = MYSQL
    default_port = 3306
    driver = "com.mysql.cj.jdbc.Driver"
    default_options = {"fetchsize": "10000"}   # cursor fetch is enabled in the URL

    def jdbc_url(self, host, port, database, trust_cert):
        ssl = "REQUIRED" if trust_cert else "VERIFY_IDENTITY"
        return f"jdbc:mysql://{host}:{int(port)}/{database}?sslMode={ssl}&useCursorFetch=true"

    def quote_ident(self, name):
        return "`" + str(name).replace("`", "``") + "`"

    def quote_str(self, value):
        # Backslash is an escape character in MySQL string literals (unless NO_BACKSLASH_ESCAPES).
        return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"

    def timestamp_literal(self, text):
        return f"'{text}'"

    def date_literal(self, text):
        return f"'{text}'"

    def table_size_mb_query(self, schema, table):
        return (
            "SELECT ROUND((data_length + index_length) / 1048576, 2) AS table_size_mb"
            " FROM information_schema.tables"
            f" WHERE table_schema = {self.quote_str(schema)} AND table_name = {self.quote_str(table)}"
        )

    # The cursor strategy is not implemented for MySQL (load_strategies.unsupported_reason says so).
    type_class_of = max_text_sql = max_batch_sql = greatest_expr = server_version_sql = pk_snapshot_sql = \
        SourceAdapter._cursor_not_implemented


_ADAPTERS = {a.src_type: a for a in (SqlServerAdapter(), OracleAdapter(), PostgresAdapter(), MySqlAdapter())}
SRC_TYPES = tuple(_ADAPTERS)


def get_adapter(src_type: Optional[str]) -> SourceAdapter:
    """Adapter for ``control.src_type`` (NULL/empty -> sqlserver, the column default)."""
    key = (src_type or SQLSERVER).strip().lower()
    try:
        return _ADAPTERS[key]
    except KeyError:
        raise ValueError(
            f"unsupported src_type {src_type!r}; supported: {', '.join(SRC_TYPES)}") from None


def for_cfg(cfg: dict) -> SourceAdapter:
    return get_adapter(cfg.get("src_type"))


def _utc(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)
