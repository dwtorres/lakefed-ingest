---
ontology: true
type: spec
domain: lakefed-cursor
summary: SourceAdapter contract, the methods every dialect provides for the cursor engine
tags: [contract, adapter, dialect, sqlserver, oracle, postgresql]
related: [../plan, ../research, ../data-model]
status: draft
discovered: 2026-10-25
verified: false
workspace: lakefed-cursor
---

# Contract: `SourceAdapter`

File: `src/lakefed_ingest/source_adapters.py`. Every dialect difference is a method here. No engine
module branches on `src_type`.

## Existing methods, unchanged

- Connection: `jdbc_url`, `read_options`, `default_port`, `driver`
- Quoting: `quote_ident`, `quote_str`, `table_ref`
- Literals: `render_bound`, `timestamp_literal`, `date_literal`
- Reads: `select_cols`, `select_sql`, `bounds_query`, `table_size_mb_query`

## New methods

| Method | Returns | Notes |
|---|---|---|
| `cursor_type_sql(schema, table, columns)` | SQL that returns `(column_name, type_name, is_nullable)` for the listed columns | SQL Server reads `sys.columns`. Oracle reads `all_tab_columns`. PostgreSQL reads `information_schema.columns`. |
| `type_class_of(type_name)` | the type class: `numeric`, `temporal`, `rowversion`, or raises. Not the control row `cursor_kind` | Oracle has no `rowversion`. `NUMBER` with scale 0 is `numeric`. |
| `source_now_sql(type_name)` | SQL expression for the server clock in the cursor type | See research R-05. |
| `max_batch_sql(tables, now_type)` | one `SELECT` with one scalar subquery per table plus the clock | `tables` is a list of `(alias, schema, table, expression)`. Oracle appends `FROM DUAL`. |
| `cursor_literal(value, type_name)` | a typed literal | SQL Server `datetime` and `smalldatetime` carry at most three fractional digits. Oracle uses `TO_TIMESTAMP` or `TO_DATE`. |
| `minus_seconds_sql(expr, seconds, type_name)` | SQL expression `expr - seconds` in the cursor type | Used for the lookback. Oracle uses `NUMTODSINTERVAL`. |
| `server_version_sql()` | SQL that returns the major version as an integer | SQL Server: `SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int)`. Oracle and PostgreSQL return `None`: no version-dependent SQL. `source_io.server_version(cfg, deps)` runs it once per source and caches by `config_cache_key`. |
| `greatest_expr(inputs, type_name, server_version)` | the dialect form of `GREATEST(COALESCE(...))` | `server_version` comes from `server_version_sql`. SQL Server below 16 (2022) returns a `CASE` form. Oracle and PostgreSQL return `GREATEST`. |
| `pk_snapshot_sql(schema, table, pk_columns)` | `SELECT pk columns FROM table` | Used by the delete snapshot. |
| `auth_options(creds)` | JDBC options for the auth mode | SQL Server supports `sql`, `ntlm`, `aad_sp`. Oracle and PostgreSQL support `sql` only and raise on the others. |

## Behavior rules

1. A method that a dialect does not support raises `NotImplementedError` with the dialect name.
2. `load_strategies.unsupported_reason` turns that error into a plan-stage failure.
3. `MySqlAdapter` raises from every new method.
4. No method prints a credential value.

## Tests

`tests/source_adapters_test.py` parametrizes every new method over `sqlserver` and `oracle`.
`tests/cursor_engine_test.py` parametrizes the window tests over both. A test asserts that
`src/lakefed_ingest/cursor_engine.py` and `src/lakefed_ingest/worker.py` contain no `_is_pg` and no
`src_type ==`.
