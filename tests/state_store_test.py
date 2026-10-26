"""Unit tests for the pure decision helpers and SQL builders in state_store.py.

No database or psycopg needed — psycopg is imported lazily inside StateStore.connect.
"""
import json
import re
from pathlib import Path

import pytest

import state_store as ss


# ---- next_phase --------------------------------------------------------------
def test_next_phase_seeding_to_incremental():
    assert ss.next_phase(ss.SEEDING) == ss.INCREMENTAL


def test_next_phase_incremental_is_terminal():
    assert ss.next_phase(ss.INCREMENTAL) == ss.INCREMENTAL


# ---- config (control table) builders -----------------------------------------
# All metadata is consolidated in Lakebase; config reads run on the cluster. Task selection is
# strategy-agnostic: it selects every enabled task in the collection (per-task strategy is
# decided later, in the sweep's per-table branch).

def test_list_task_configs_sql_selects_full_rows_filters_and_orders():
    sql = ss.list_task_configs_sql()
    assert ss.CONTROL_TABLE in sql
    for col in ss.CONTROL_COLUMNS:
        assert col in sql                              # selects the full config row
    assert "task_collection = %s" in sql and "task_enabled = true" in sql
    # strategy-agnostic selection: the WHERE must not hardcode a strategy predicate
    # (load_type/ct_enabled are selected columns, so check the predicates, not the bare names).
    assert "load_type = 'ct'" not in sql and "ct_enabled = true" not in sql
    assert "order by tier nulls last, priority nulls last, id" in sql
    assert sql.count("%s") == 1  # task_collection


def test_get_task_sql_selects_all_control_columns():
    sql = ss.get_task_sql()
    assert ss.CONTROL_TABLE in sql
    for col in ss.CONTROL_COLUMNS:
        assert col in sql
    assert sql.count("%s") == 1  # id


# ---- SQL builders ------------------------------------------------------------
# Builders return parameterized (%s) statements against the Lakebase hot-state tables.

def test_get_checkpoint_sql_keyed_by_control_id():
    sql = ss.get_checkpoint_sql()
    assert ss.CHECKPOINT_TABLE in sql
    assert "where control_id = %s" in sql
    assert sql.count("%s") == 1  # control_id


def test_upsert_checkpoint_conflicts_on_control_id():
    # Keyed by control id so same-named DBs on different servers never share a checkpoint.
    sql = ss.upsert_checkpoint_sql()
    assert "on conflict (control_id) do update" in sql
    assert "src_database, src_schema, src_table" in sql   # readable identity still written
    # control_id, db, schema, table, strategy, position, phase, status, detail, select_list
    assert sql.count("%s") == 10
    assert "strategy = excluded.strategy, position = excluded.position" in sql
    assert "select_list = coalesce(excluded.select_list, checkpoint.select_list)" in sql


def test_checkpoint_sql_has_no_pending_or_ct_begin_version_columns():
    # Append mode and Change Tracking are gone: the checkpoint row has no pending append and no
    # CT begin version to carry. The reseed queue is gone with them. The default strategy is cursor.
    for sql in (ss.get_checkpoints_sql(), ss.upsert_checkpoint_sql(), ss.bulk_advance_unchanged_sql()):
        assert "pending_" not in sql and "ct_begin_version" not in sql
    assert not hasattr(ss, "record_pending_sql") and not hasattr(ss, "RESEED_TABLE")
    assert not hasattr(ss.StateStore, "record_pending") and not hasattr(ss.StateStore, "record_ct_begin_version")
    assert ss.strategy_name({}) == "cursor"


def test_set_status_sql_upserts_on_control_id():
    sql = ss.set_status_sql()
    assert "on conflict (control_id)" in sql
    assert sql.count("%s") == 7  # control_id, db, schema, table, strategy, status, detail
    # an existing row keeps its own strategy / position; only status + detail change
    assert "strategy = excluded" not in sql and "position" not in sql


def test_task_identity_from_control_row():
    cfg = {"id": "7", "src_database": "db1", "src_schema": "dbo", "src_table": "t"}
    assert ss.task_identity(cfg) == (7, "db1", "dbo", "t")


def test_touch_last_success_joins_on_control_id():
    sql = ss.touch_last_success_sql()
    assert "cc.control_id = e.control_id" in sql


def test_control_columns_include_jdbc_and_secret_fields():
    for col in ("src_host", "src_port", "secret_key"):
        assert col in ss.CONTROL_COLUMNS
        assert col in ss.list_task_configs_sql()


# ---- Lakebase DDL (string checks) --------------------------------------------
_DDL = (Path(__file__).parent.parent / "src" / "lakefed_ingest" / "lakebase_schema.sql").read_text()


def test_ddl_a_source_is_a_host_or_a_connection():
    con = _DDL[_DDL.index("add constraint valid_source"):]
    con = con[:con.index(";")]
    assert "use_remote_query" not in con
    assert "src_host is not null or src_connection is not null" in con


def test_ddl_a_read_through_a_connection_needs_the_connection():
    assert "drop constraint if exists valid_remote_query" in _DDL
    con = _DDL[_DDL.index("add constraint valid_remote_query"):]
    assert con[:con.index(";")].split("check", 1)[1].split() == [
        "(", "not", "use_remote_query", "or", "src_connection", "is", "not", "null", ")"]


def test_ddl_checkpoint_is_keyed_by_control_id():
    assert "constraint checkpoint_control_pk primary key (control_id)" in _DDL


def test_ddl_has_no_dollar_quoted_blocks():
    # apply_lakebase_schema.ipynb splits statements on ';' — DO $$ ... $$ blocks would break it.
    code = "\n".join(l for l in _DDL.splitlines() if not l.strip().startswith("--"))
    assert "$$" not in code


# ---- observability / telemetry builders --------------------------------------
# One ingest_run row per run, one ingest_event row per (run, table): INSERT at start,
# UPDATE at finish. These re-create per-table visibility that the single-task run hides.

def test_start_run_sql_inserts_running_and_returns_id():
    sql = ss.start_run_sql()
    assert ss.RUN_TABLE == "lakefed_cursor.ingest_run" and ss.RUN_TABLE in sql
    assert "'running'" in sql
    assert "returning run_id" in sql
    assert sql.count("%s") == 4  # task_collection, job_run_id, cluster_id, parallelism


def test_record_event_start_sql_inserts_running_and_returns_id():
    sql = ss.record_event_start_sql()
    assert ss.EVENT_TABLE in sql
    assert "'running'" in sql
    assert "returning id" in sql
    assert "(run_id, task_collection" in sql
    # run_id, task_collection, control_id, src_database, src_schema, src_table,
    # sink_fqn, action, phase, pool
    assert sql.count("%s") == 10


def test_record_event_finish_sql_updates_event_by_id():
    sql = ss.record_event_finish_sql()
    assert sql.startswith(f"update {ss.EVENT_TABLE} set")
    assert "finished_at = now()" in sql and "duration_ms" in sql
    assert "where id = %s" in sql
    # status, stage, rows_read, rows_merged, position_from, position_to, error, action, pool, retry_count, id
    assert sql.count("%s") == 11
    assert "position_from = %s, position_to = %s" in sql
    assert "action = coalesce(%s, action)" in sql and "pool = coalesce(%s, pool)" in sql
    assert "retry_count = coalesce(%s, retry_count)" in sql


def test_touch_last_success_sql_stamps_checkpoint_from_event():
    sql = ss.touch_last_success_sql()
    assert ss.CHECKPOINT_TABLE in sql and ss.EVENT_TABLE in sql
    assert "last_success_at = now()" in sql
    assert sql.count("%s") == 1  # event_id


def test_finish_run_sql_updates_run_by_id():
    sql = ss.finish_run_sql()
    assert sql.startswith(f"update {ss.RUN_TABLE} set")
    assert "finished_at = now()" in sql
    assert "where run_id = %s" in sql
    # status, total, ok, failed, skipped, reseeded, detail, run_id
    assert sql.count("%s") == 8


def test_close_abandoned_runs_sql_closes_the_collections_other_open_runs_and_their_open_events():
    # A run that was cancelled or lost its cluster never finishes its ingest_run row or its in-flight
    # ingest_event rows. The next run holds the collection's lock, so any other open run of the
    # collection is dead: close it as failed, with its events, in one statement.
    sql = ss.close_abandoned_runs_sql()
    run_update, event_update = sql.split("), events as (")
    assert f"update {ss.RUN_TABLE} set status = 'failed'" in run_update
    assert "where task_collection = %s and status = 'running' and run_id <> %s" in run_update
    assert f"update {ss.EVENT_TABLE} e set status = 'failed'" in event_update
    assert "e.run_id = dead.run_id and (e.status = 'running'" in event_update
    # A table waiting for its retry when the run died: its last attempt is 'retried' and no later one
    # exists, so it is closed as failed too, keeping its error and timing.
    assert ("e.status = 'retried' and not exists (select 1 from " + ss.EVENT_TABLE + " later "
            "where later.run_id = e.run_id and later.control_id = e.control_id and later.id > e.id)") in event_update
    assert "finished_at = coalesce(e.finished_at, now())" in event_update
    assert "when e.status = 'retried' then concat(e.error, ' | ', %s)" in event_update
    assert sql.rstrip().endswith("select (select count(*) from dead), (select count(*) from events)")
    # detail, task_collection, run_id, error (twice: a running event's, appended to a retried one's)
    assert sql.count("%s") == 5


# ---- per-collection run lock --------------------------------------------------------------
# Different task_collections sweep concurrently; the same collection must not overlap. A
# session-level advisory lock keyed by a namespaced, 64-bit hash of the collection name.

def test_collection_lock_key_is_namespaced():
    assert ss.collection_lock_key("t1") == ss.LOCK_NAMESPACE + "t1"
    assert ss.collection_lock_key("t1") != ss.collection_lock_key("t2")


def test_try_lock_collection_sql_is_non_blocking_64bit():
    sql = ss.try_lock_collection_sql()
    assert "pg_try_advisory_lock" in sql        # non-blocking: a duplicate run skips, never waits
    assert "hashtextextended" in sql            # 64-bit key => negligible cross-collection collisions
    assert sql.count("%s") == 1


def test_unlock_collection_sql_matches_lock_key():
    sql = ss.unlock_collection_sql()
    assert "pg_advisory_unlock" in sql and "hashtextextended" in sql
    assert sql.count("%s") == 1


def test_lock_application_name_is_labeled_and_capped():
    name = ss.lock_application_name("smoke_test", "12345")
    assert name == "lakefed_cursor:smoke_test:12345"
    assert len(ss.lock_application_name("c" * 200, "r" * 50)) == 63   # Postgres limit


# ---- partitioned seed control settings ----------------------------------------------------
def test_control_columns_include_partition_settings():
    for col in ("load_partitioned", "partition_col", "partition_size_mb"):
        assert col in ss.CONTROL_COLUMNS


def test_partitioning_ddl_and_idempotent_migration():
    assert "partition_col         text" in _DDL and "partition_size_mb     int" in _DDL
    assert "drop constraint if exists valid_partitioning" in _DDL
    assert ("not load_partitioned or (partition_col is not null and "
            "coalesce(partition_size_mb, 0) > 0)") in _DDL


# ---- StateStore methods against a fake connection ----------------------------------------
# Builders are shape-tested above; these check the BOUND PARAMETER ORDER each method sends, so a
# swapped argument can't silently corrupt checkpoints, locks, or telemetry.

class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if self.conn.raise_on_execute:
            raise RuntimeError("connection lost")
        self.conn.calls.append((sql, params))

    def executemany(self, sql, seq):
        if self.conn.raise_on_execute:
            raise RuntimeError("connection lost")
        self.conn.calls.append((sql, list(seq)))

    def fetchone(self):
        return self.conn.fetch.pop(0) if self.conn.fetch else None

    def fetchall(self):
        rows, self.conn.fetch = list(self.conn.fetch), []
        return rows


class FakeConn:
    def __init__(self, fetch=None, closed=False, raise_on_execute=False, raise_on_close=False):
        self.calls, self.fetch = [], list(fetch or [])
        self.closed, self.raise_on_execute, self.raise_on_close = closed, raise_on_execute, raise_on_close

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        if self.raise_on_close:
            raise RuntimeError("already gone")
        self.closed = True


CFG = {"id": 7, "src_database": "d", "src_schema": "s", "src_table": "t"}


def test_advance_checkpoint_binds_identity_strategy_then_text_position():
    conn = FakeConn(); ss.StateStore(conn).advance_checkpoint(CFG, 42)
    # strategy defaults to the row's load_type (cursor when unset); the position is stored as text
    assert conn.calls == [(ss.upsert_checkpoint_sql(),
                           (7, "d", "s", "t", "cursor", "42", "incremental", "ok", None, None))]


def test_advance_checkpoint_full_strategy_has_no_position():
    conn = FakeConn()
    ss.StateStore(conn).advance_checkpoint(dict(CFG, load_type="full"), None, phase=ss.LOADED)
    assert conn.calls[0][1][4:7] == ("full", None, "loaded")


def test_set_status_binds_identity_then_status():
    conn = FakeConn(); ss.StateStore(conn).set_status(CFG, "failed", "boom")
    assert conn.calls == [(ss.set_status_sql(), (7, "d", "s", "t", "cursor", "failed", "boom"))]


def test_enqueue_reseed_flips_phase_to_seeding():
    conn = FakeConn(); ss.StateStore(conn).enqueue_reseed(CFG, "schema_change")
    assert conn.calls == [
        (ss.upsert_checkpoint_sql(),
         (7, "d", "s", "t", "cursor", None, "seeding", "reseed_queued", "schema_change", None)),
    ]


def test_get_checkpoint_binds_control_id_and_defaults_to_seeding():
    conn = FakeConn(fetch=[None]); cp = ss.StateStore(conn).get_checkpoint("7")
    assert conn.calls == [(ss.get_checkpoint_sql(), (7,))]
    assert cp == {"position": None, "phase": "seeding", "status": "ok", "strategy": None}
    conn = FakeConn(fetch=[("287493", "incremental", "ok", "cursor")])
    assert ss.StateStore(conn).get_checkpoint(7) == {"position": "287493", "phase": "incremental",
                                                     "status": "ok", "strategy": "cursor"}


def test_try_lock_collection_reads_result_and_binds_key():
    for got in (True, False):
        conn = FakeConn(fetch=[(got,)])
        assert ss.StateStore(conn).try_lock_collection("c") is got
        assert conn.calls == [(ss.try_lock_collection_sql(), (ss.collection_lock_key("c"),))]


def test_close_abandoned_runs_binds_detail_collection_run_then_error_and_returns_counts():
    conn = FakeConn(fetch=[(1, 48)])
    assert ss.StateStore(conn).close_abandoned_runs("c", 9) == (1, 48)
    (sql, params), = conn.calls
    assert sql == ss.close_abandoned_runs_sql()
    detail, collection, run_id, retried_error, error = params
    assert json.loads(detail) == {"closed_by_run": 9, "reason": ss.ABANDONED_RUN_REASON}
    assert (collection, run_id) == ("c", 9)
    assert error == retried_error == f"{ss.ABANDONED_RUN_REASON}; closed by run 9"


def test_is_alive_false_when_closed_or_erroring_true_otherwise():
    assert ss.StateStore(FakeConn(closed=True)).is_alive() is False
    assert ss.StateStore(FakeConn(raise_on_execute=True)).is_alive() is False
    assert ss.StateStore(FakeConn()).is_alive() is True


def test_close_swallows_exceptions():
    ss.StateStore(FakeConn(raise_on_close=True)).close()   # must not raise


def test_record_event_finish_ok_stamps_last_success():
    conn = FakeConn()
    ss.StateStore(conn).record_event_finish(9, "ok", stage="merge", rows_merged=3,
                                            position_from=1, position_to=2, action="increment")
    assert conn.calls == [
        (ss.record_event_finish_sql(), ("ok", "merge", None, 3, "1", "2", None, "increment", None, None, 9)),
        (ss.touch_last_success_sql(), (9,)),
    ]


def test_record_event_finish_failed_truncates_error_and_skips_stamp():
    conn = FakeConn()
    ss.StateStore(conn).record_event_finish(9, "failed", stage="read", error="x" * 3000)
    assert len(conn.calls) == 1
    assert len(conn.calls[0][1][6]) == 2000          # error param truncated


def test_record_event_finish_skipped_reseed_does_not_stamp():
    conn = FakeConn()
    ss.StateStore(conn).record_event_finish(9, "skipped", stage="checkpoint", action="reseed")
    assert len(conn.calls) == 1 and conn.calls[0][1][7] == "reseed"


def test_record_event_finish_records_pool():
    conn = FakeConn()
    ss.StateStore(conn).record_event_finish(9, "ok", stage="merge", pool="pool3,pool17")
    assert conn.calls[0][1][8] == "pool3,pool17" and conn.calls[0][1][10] == 9


def test_record_event_finish_records_a_retried_attempt_without_stamping():
    conn = FakeConn()
    ss.StateStore(conn).record_event_finish(9, "retried", stage="seed", error="x", retry_count=1)
    assert len(conn.calls) == 1 and conn.calls[0][1][0] == "retried" and conn.calls[0][1][9] == 1


# ---- skip-no-change: bulk checkpoint load, select_list recording, bulk advance -------------

def test_get_checkpoints_one_query_any_array():
    sql = ss.get_checkpoints_sql()
    assert "control_id = any(%s)" in sql and "select_list" in sql and sql.count("%s") == 1


def test_get_checkpoints_maps_rows_by_control_id():
    conn = FakeConn(fetch=[(7, "100", "incremental", "ok", "a, b", "cursor", "2026-10-14", "2026-10-13"),
                           (8, None, "loaded", "ok", None, "full", None, None)])
    got = ss.StateStore(conn).get_checkpoints(["7", 8])
    assert conn.calls == [(ss.get_checkpoints_sql(), ([7, 8],))]
    assert got == {7: {"position": "100", "phase": "incremental", "status": "ok", "select_list": "a, b",
                       "strategy": "cursor", "last_success_at": "2026-10-14", "last_delete_snapshot_at": "2026-10-13"},
                   8: {"position": None, "phase": "loaded", "status": "ok", "select_list": None,
                       "strategy": "full", "last_success_at": None, "last_delete_snapshot_at": None}}
    assert ss.StateStore(FakeConn()).get_checkpoints([]) == {}


def test_advance_checkpoint_records_select_list():
    conn = FakeConn(); ss.StateStore(conn).advance_checkpoint(CFG, 42, select_list="a, b")
    assert conn.calls[0][1][-1] == "a, b"


def test_bulk_advance_stamps_an_unchanged_table_without_touching_its_position():
    # A quiet table's captured hi equals its position (JSON text, not a number): the bulk write only
    # clears the status, records a grown select_list and stamps freshness, in the incremental phase.
    sql = ss.bulk_advance_unchanged_sql()
    assert "position" not in sql.split(" set ", 1)[1]
    assert "where control_id = %s and phase = 'incremental'" in sql
    assert "status = 'ok', detail = null" in sql
    assert "last_success_at = now()" in sql and "select_list = coalesce(%s, select_list)" in sql
    assert sql.count("%s") == 2


def test_bulk_advance_binds_select_list_and_control_id():
    conn = FakeConn()
    n = ss.StateStore(conn).bulk_advance_unchanged([(7, '{"value":"500"}', None), (9, '{"value":"12"}', "a, b")])
    assert n == 2
    assert conn.calls == [(ss.bulk_advance_unchanged_sql(), [(None, 7), ("a, b", 9)])]
    assert ss.StateStore(FakeConn()).bulk_advance_unchanged([]) == 0


def test_select_list_added_columns():
    assert ss.select_list_added_columns("a, b", None) is None                 # legacy: not recorded
    assert ss.select_list_added_columns("b,A", "a, b") == []                  # reorder/case: no reseed
    assert ss.select_list_added_columns("a, b, C", "a,b") == ["c"]            # added -> reseed
    assert ss.select_list_added_columns("a", "a, b") == []                    # removed: no reseed


def test_record_failed_tables_bulk_events_and_status():
    conn = FakeConn()
    cfg = dict(CFG, task_collection="tc", sink_catalog="c", sink_schema="s", sink_table="t")
    n = ss.StateStore(conn).record_failed_tables(3, [(cfg, {"phase": "incremental"}, "db unreachable")])
    assert n == 1
    (ev_sql, ev_rows), (st_sql, st_rows) = conn.calls
    assert ev_sql == ss.record_event_failed_sql() and "'failed'" in ev_sql
    assert ev_rows == [(3, "tc", 7, "d", "s", "t", "c.s.t", "increment", "incremental", "detect", "db unreachable")]
    assert st_sql == ss.set_status_sql() and st_rows == [(7, "d", "s", "t", "cursor", "failed", "db unreachable")]


def test_record_failed_tables_full_rows_and_plan_stage():
    conn = FakeConn()
    cfg = dict(CFG, load_type="full", task_collection="tc", sink_catalog="c", sink_schema="s", sink_table="t")
    ss.StateStore(conn).record_failed_tables(3, [(cfg, None, "unsupported")], stage="plan")
    (_, ev_rows), (_, st_rows) = conn.calls
    assert ev_rows[0][7:10] == ("full", None, "plan") and st_rows[0][4] == "full"


# ---- generic checkpoint + renames: guarded migration and the applier's statement splitter ------



def test_no_rename_is_pending_for_the_new_schema():
    # lakefed_cursor is a new schema: nothing to rename, whatever the catalog holds.
    assert ss.RENAMES == []
    assert ss.rename_statements({"control", "ct_checkpoint"}, {("ct_checkpoint", "ct_version")}) == []


def test_split_sql_statements_strips_comments_with_semicolons():
    ddl = "create table a (x int); -- note; with a semicolon\n\n-- whole-line; comment\nalter table a add y int ;\n"
    assert ss.split_sql_statements(ddl) == ["create table a (x int)", "alter table a add y int"]


def test_split_sql_statements_on_the_real_schema_file():
    stmts = ss.split_sql_statements(_DDL)
    assert stmts[0] == "create schema if not exists lakefed_cursor"
    assert all("--" not in s for s in stmts) and len(stmts) > 40


# ---- schema version and migration --------------------------------------------------------------

# The DDL fingerprint each SCHEMA_VERSION was released with. Changed lakebase_schema.sql or RENAMES?
# Bump state_store.SCHEMA_VERSION and add its fingerprint here: deployments then migrate on their
# next ingestion run.
SCHEMA_FINGERPRINTS = {
    1: "c035b85032e0df245775f4858dcc2e450ae737e885f326071c7b8546527a96e1",   # lakefed_ingest root version
    2: "6bf325557a775ca52cd3a83b4e01f8fd6a24c06a884af4a4e0ec04ddb3d84b85",   # lakefed_cursor: expression cursor, own names
}


def test_schema_version_is_bumped_with_every_ddl_change():
    got = ss.schema_fingerprint(_DDL)
    assert SCHEMA_FINGERPRINTS.get(ss.SCHEMA_VERSION) == got, (
        f"lakebase_schema.sql or RENAMES changed: bump state_store.SCHEMA_VERSION to "
        f"{max(SCHEMA_FINGERPRINTS) + 1} and add {{{max(SCHEMA_FINGERPRINTS) + 1}: {got!r}}} to SCHEMA_FINGERPRINTS")
    assert ss.SCHEMA_VERSION == max(SCHEMA_FINGERPRINTS)


def test_schema_fingerprint_ignores_comments_and_whitespace():
    assert ss.schema_fingerprint("create table a (x int); -- note\n") == ss.schema_fingerprint("create  table a\n(x int);")
    assert ss.schema_fingerprint("create table a (x int);") != ss.schema_fingerprint("create table a (x bigint);")


def test_ddl_creates_the_schema_version_table_and_runs_in_a_transaction():
    stmts = ss.split_sql_statements(_DDL)
    assert any(s.startswith("create table if not exists lakefed_cursor.schema_version") for s in stmts)
    assert not any("concurrently" in s.lower() for s in stmts)       # not allowed in a transaction


# Every upgrade re-applies the WHOLE lakebase_schema.sql (plus RENAMES) in one transaction, from
# whatever version the database is at, so every statement must be idempotent, additive and valid on
# any earlier version's schema (docs/lakebase_schema.md, "Changing the schema"). These tests check
# the statement forms with no database: a statement that would fail or do harm when re-applied fails here
# instead of failing the next upgrade on every deployment.

# Statements that change existing columns or rows, or drop objects, each reviewed as a no-op once
# applied and valid from any earlier version. Matched by the start of the normalized statement. A new
# statement of these kinds fails test_ddl_statements_are_safe_to_re_apply until it's reviewed and listed
# here. The drops remove the root version's Change Tracking, CDC and append-mode columns (gap 11): the
# schema is new, so no row holds them, and `if exists` makes each a no-op once applied.
REVIEWED_NON_ADDITIVE = tuple(
    [f"alter table lakefed_cursor.control drop column if exists {c}" for c in (
        "job_name", "src_catalog", "enable_iceberg_reads", "sink_cluster_cols", "source_instance_group",
        "warehouse_id", "ct_enabled", "cdc_capture_instance", "cursor_column", "write_mode")]
    + [f"alter table lakefed_cursor.checkpoint drop column if exists {c}" for c in (
        "ct_begin_version", "pending_position", "pending_run_id")]
    + ["drop table if exists lakefed_cursor.reseed_queue"]
)
_NON_ADDITIVE = re.compile(r"^(update|insert|delete)\b|\balter column\b|\bdrop (default|not null)\b|\bset not null\b")


def ddl_problems(statements, reviewed=REVIEWED_NON_ADDITIVE):
    """Why each statement isn't safe to re-apply on any earlier version's schema ([] = all safe)."""
    out, dropped = [], set()
    for raw in statements:
        s = " ".join(raw.split()).lower()
        short = s[:100]
        if re.search(r"\brename\b", s):
            out.append(f"rename (put it in state_store.RENAMES, which runs only pending renames): {short}")
        elif re.match(r"(drop|truncate)\b", s) or re.search(r"\bdrop (column|table|schema|index|view)\b", s):
            if not (re.search(r"\bif exists\b", s) and s.startswith(reviewed)):
                out.append(f"destructive (needs a per-version migration step, not the cumulative re-apply): {short}")
        elif s.startswith("create"):
            if not re.match(r"create (schema|table|(unique )?index) if not exists\b|create or replace (view|function)\b", s):
                out.append(f"create without if not exists / or replace: {short}")
        elif s.startswith("alter table"):
            table = s.split()[2]
            for name in re.findall(r"\bdrop constraint if exists (\w+)", s):
                dropped.add((table, name))
            for name in re.findall(r"\badd constraint (\w+)", s):
                if (table, name) not in dropped:
                    out.append(f"add constraint {name} without an earlier drop constraint if exists {name}: {short}")
            if re.search(r"\badd (?!column if not exists\b|constraint\b)", s):
                out.append(f"add without column if not exists: {short}")
            if _NON_ADDITIVE.search(s) and not s.startswith(reviewed):
                out.append(f"changes an existing column; review it and add it to REVIEWED_NON_ADDITIVE: {short}")
        elif _NON_ADDITIVE.search(s):
            if not s.startswith(reviewed):
                out.append(f"changes existing rows; make it a no-op once applied, then add it to REVIEWED_NON_ADDITIVE: {short}")
        elif not s.startswith("comment on"):
            out.append(f"unrecognized statement kind: {short}")
    return out


def test_ddl_statements_are_safe_to_re_apply():
    assert ddl_problems(ss.split_sql_statements(_DDL)) == []
    # Every reviewed statement still exists (remove entries for statements taken out of the file).
    stmts = [" ".join(s.split()).lower() for s in ss.split_sql_statements(_DDL)]
    assert [r for r in REVIEWED_NON_ADDITIVE if not any(s.startswith(r) for s in stmts)] == []


@pytest.mark.parametrize("stmt, why", [
    ("create table lakefed_ingest.t (a int)", "create without if not exists"),
    ("create index t_idx on lakefed_ingest.t (a)", "create without if not exists"),
    ("alter table lakefed_ingest.t add column b int", "add without column if not exists"),
    ("alter table lakefed_ingest.t add b int", "add without column if not exists"),
    ("alter table lakefed_ingest.t add constraint c check (a > 0)", "without an earlier drop constraint if exists c"),
    ("alter table lakefed_ingest.t drop column a", "destructive"),
    ("drop table if exists lakefed_ingest.t", "destructive"),
    ("truncate lakefed_ingest.t", "destructive"),
    ("alter table lakefed_ingest.t rename column a to b", "rename"),
    ("alter table lakefed_ingest.t alter column a type bigint", "REVIEWED_NON_ADDITIVE"),
    ("alter table lakefed_ingest.t alter column a set not null", "REVIEWED_NON_ADDITIVE"),
    ("update lakefed_ingest.t set a = 1", "REVIEWED_NON_ADDITIVE"),
    ("delete from lakefed_ingest.t where a = 1", "REVIEWED_NON_ADDITIVE"),
    ("grant select on lakefed_ingest.t to app", "unrecognized"),
])
def test_ddl_check_flags_statements_that_are_unsafe_to_re_apply(stmt, why):
    problems = ddl_problems([stmt])
    assert len(problems) == 1 and why in problems[0], problems


def test_ddl_check_accepts_the_idempotent_forms():
    assert ddl_problems(["alter table lakefed_cursor.control drop column if exists job_name"]) == []   # reviewed
    assert ddl_problems(["alter table lakefed_cursor.control drop column if exists other"]) != []      # not reviewed
    assert ddl_problems([
        "create table if not exists lakefed_ingest.t (a int)",
        "create unique index if not exists t_uq on lakefed_ingest.t (a)",
        "alter table lakefed_ingest.t add column if not exists b int, add column if not exists c text",
        "alter table lakefed_ingest.t drop constraint if exists c",
        "alter table lakefed_ingest.t add constraint c check (a > 0)",
        "comment on table lakefed_ingest.t is 'x'",
    ]) == []


class MigrationDb:
    """A scripted Postgres for migrate(): answers the catalog queries, records statements and
    whether they ran inside the transaction."""

    def __init__(self, version=None, schemas=("lakefed_ingest",), relations=(), columns=(), fail_on=None):
        self.version, self.schemas, self.relations, self.columns = version, set(schemas), set(relations), set(columns)
        self.fail_on, self.statements, self.in_tx, self.committed, self.rows = fail_on, [], False, None, []
        self.connection = self

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def transaction(self):
        db = self

        class Tx:
            def __enter__(self):
                db.in_tx = True

            def __exit__(self, exc_type, *exc):
                db.in_tx, db.committed = False, exc_type is None
                return False
        return Tx()

    def execute(self, sql, params=None):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("check constraint violated")
        if sql.startswith("select pg_advisory_xact_lock"):
            self.lock = params
        self.statements.append((sql, self.in_tx))
        if sql.startswith("select to_regclass"):
            self.rows = [(None if self.version is None else "schema_version",)]
        elif sql.startswith("select coalesce(max(version)"):
            self.rows = [(self.version,)]
        elif sql.startswith("select nspname"):
            self.rows = [(s,) for s in self.schemas]
        elif sql.startswith("select c.relname"):
            self.rows = [(r,) for r in self.relations]
        elif sql.startswith("select table_name"):
            self.rows = list(self.columns)

    def fetchone(self):
        return self.rows[0]

    def fetchall(self):
        return self.rows

    def ran(self):
        return [s for s, _ in self.statements if not s.startswith(("select", "set local"))]


def test_schema_version_is_zero_before_versions_were_recorded():
    assert ss.StateStore(MigrationDb()).schema_version() == 0
    assert ss.StateStore(MigrationDb(version=1)).schema_version() == 1


def test_migrate_from_an_unversioned_database_applies_renames_ddl_and_version_in_one_transaction():
    db = MigrationDb(relations={"ct_checkpoint", "checkpoint_control_pk"}, columns={("ct_checkpoint", "ct_version")})
    out = ss.StateStore(db).migrate("create table if not exists lakefed_cursor.x (a int); alter table lakefed_cursor.x add b int;")
    assert out == {"from": 0, "renames": []}
    assert db.ran() == ["create table if not exists lakefed_cursor.x (a int)", "alter table lakefed_cursor.x add b int",
                            "insert into lakefed_cursor.schema_version (version) values (%s) on conflict (version) "
                            "do update set applied_at = now(), applied_by = current_user"]
    assert all(in_tx for _, in_tx in db.statements) and db.committed is True
    # Serialized by the schema lock, taken before the version is read; DDL waits are bounded.
    first = [s for s, _ in db.statements[:3]]
    assert first[0] == "select pg_advisory_xact_lock(%s::int, %s::int)" and first[1] == "set local lock_timeout = '60s'"
    assert first[2].startswith("select to_regclass")
    # Two int keys: never the one-key lock of a collection (even one named "schema"), nor a source lock.
    assert db.lock == ss.SCHEMA_LOCK and len(db.lock) == 2 and db.lock[0] != ss.SOURCE_LOCK_CLASS


def test_migrate_is_a_noop_at_the_current_version_unless_forced():
    db = MigrationDb(version=ss.SCHEMA_VERSION)
    assert ss.StateStore(db).migrate("create table t (a int);") is None and db.ran() == []
    out = ss.StateStore(db).migrate("create table t (a int);", force=True)      # the setup job
    assert out == {"from": ss.SCHEMA_VERSION, "renames": []} and "create table t (a int)" in db.ran()



def test_migrate_failure_rolls_back_and_a_newer_database_is_refused():
    db = MigrationDb(fail_on="valid_remote_query")
    with pytest.raises(RuntimeError):
        ss.StateStore(db).migrate("alter table c add constraint valid_remote_query check (x);")
    assert db.committed is False and not any("schema_version (version)" in s for s in db.ran())
    newer = MigrationDb(version=ss.SCHEMA_VERSION + 1)
    with pytest.raises(RuntimeError, match="newer than this code"):
        ss.StateStore(newer).migrate("create table t (a int);", force=True)
    assert newer.ran() == [] and newer.committed is False


def test_ddl_generic_checkpoint_and_telemetry_columns():
    for frag in ("create table if not exists lakefed_cursor.checkpoint",
                 "strategy                text        not null default 'cursor'",
                 "phase in ('seeding', 'incremental', 'loaded')",
                 "create table if not exists lakefed_cursor.ingest_run",
                 "run_id          bigserial   primary key",
                 "position_from   text", "position_to     text"):
        assert frag in _DDL, frag
    code = "\n".join(l.split("--")[0] for l in _DDL.splitlines())
    for old in ("ct_checkpoint (", "sweep_run (", "sweep_id ", "ct_version "):
        assert old not in code, old



def test_strategy_name_and_position_text():
    assert ss.strategy_name({}) == "cursor" and ss.strategy_name({"load_type": " FULL "}) == "full"
    assert ss.position_text(None) is None and ss.position_text(12) == "12"


def test_mixed_sinks_query_starts_from_dedicated_rows_and_maps_tuples():
    sql = ss.mixed_sinks_sql()
    # Every collection, enabled or not; no GROUP BY over the whole control table.
    assert "task_enabled" not in sql and "group by" not in sql
    assert "where d.client_id is null and exists (" in sql and "and o.id <> d.id)" in sql
    assert "lower(o.sink_table) = lower(d.sink_table)" in sql
    conn = FakeConn(fetch=[("main", "bronze", "orders")])
    assert ss.StateStore(conn).mixed_sinks() == {("main", "bronze", "orders")}


def test_ddl_indexes_dedicated_rows_by_sink():
    assert ("create index if not exists control_dedicated_sink_idx\n"
            "    on lakefed_cursor.control (lower(sink_catalog), lower(sink_schema), lower(sink_table))\n"
            "    where client_id is null;") in _DDL


def test_control_columns_include_client_id():
    assert "client_id" in ss.CONTROL_COLUMNS


class _FakeConn:
    def __init__(self, name):
        self.name, self.closed, self.broken = name, False, False
    def cursor(self):
        return self.name


def test_dropped_connection_is_replaced_before_the_next_statement():
    opened = []
    def reopen():
        opened.append(_FakeConn(f"c{len(opened) + 2}"))
        return opened[-1]
    store = ss.StateStore(_FakeConn("c1"), reconnect=reopen)
    assert store._cursor() == "c1" and not opened          # healthy: no new connection
    store._conn.broken = True
    assert store._cursor() == "c2" and len(opened) == 1    # dropped: reopened once
    assert store._cursor() == "c2"


def test_without_reconnect_a_dropped_connection_stays_dropped():
    # The lock connection: losing it means the advisory lock is gone, which must not be hidden.
    store = ss.StateStore(_FakeConn("c1"))
    store._conn.closed = True
    assert store._cursor() == "c1" and store.is_alive() is False


def test_ddl_client_attrs_column_and_check():
    assert "client_attrs          text" in _DDL and "client_attrs" in ss.CONTROL_COLUMNS
    assert "jsonb_typeof(client_attrs::jsonb) = 'object'" in _DDL


# ---- bulk checkpoint and telemetry writes ---------------------------------------

def test_bulk_batch_operations_bind_one_row_per_member():
    conn = FakeConn(); store = ss.StateStore(conn)
    a, b = dict(CFG, select_list="a, b"), dict(CFG, id=8, select_list="a")
    store.advance_many([(a, 150, "incremental", None), (b, None, "loaded", "full load complete")])
    store.advance_many([])                                  # nothing to send: no round trip
    assert conn.calls == [
        (ss.upsert_checkpoint_sql(), [(7, "d", "s", "t", "cursor", "150", "incremental", "ok", None, "a, b"),
                                      (8, "d", "s", "t", "cursor", None, "loaded", "ok", "full load complete", "a")]),
    ]


def test_bulk_batch_operations_fill_every_placeholder():
    # The bulk writes reuse the single-row statements. If one of those gains a parameter, the bulk
    # rows must gain it too, or every batched write raises after its data committed.
    conn = FakeConn(); store = ss.StateStore(conn)
    a = dict(CFG, select_list="a, b")
    store.advance_many([(a, 150, "incremental", None)])
    for sql, rows in conn.calls:
        assert all(len(row) == sql.count("%s") for row in rows), sql


def test_record_finished_events_in_bulk_and_stamps_success():
    conn = FakeConn()
    cfg = dict(CFG, task_collection="tc", sink_catalog="c", sink_schema="s", sink_table="t")
    ss.StateStore(conn).record_finished_events(3, [
        (cfg, "increment", "incremental", "merge", "ok", 100, 150, "batch2", None),
        (dict(cfg, id=8), "increment", "incremental", "merge", "failed", 100, None, "batch2", "boom")],
        duration_ms=1234)
    (ev_sql, rows), (stamp_sql, stamp_params) = conn.calls
    assert ev_sql == ss.record_event_finished_sql() and "now() - %s * interval '1 millisecond'" in ev_sql
    assert ev_sql.count("%s") == len(rows[0]) == 17
    assert rows[0] == (3, "tc", 7, "d", "s", "t", "c.s.t", "increment", "incremental", "merge", "ok", "100", "150",
                       "batch2", None, 1234, 1234)
    assert rows[1][10] == "failed" and rows[1][14] == "boom"
    assert stamp_sql == ss.touch_last_success_many_sql() and stamp_params == ([7],)


def test_record_failed_tables_action_override():
    conn = FakeConn()
    cfg = dict(CFG, task_collection="tc", sink_catalog="c", sink_schema="s", sink_table="t")
    ss.StateStore(conn).record_failed_tables(3, [(cfg, None, "x")], stage="checkpoint", action="seed")
    (_, ev_rows), _ = conn.calls
    assert ev_rows[0][7:10] == ("seed", None, "checkpoint")


def test_list_task_configs_shares_repeated_strings():
    def row(i):
        values = {"id": i, "select_list": "".join(["id, ", "v"]), "src_database": f"db{i}"}
        return tuple(values.get(c) for c in ss.CONTROL_COLUMNS)
    rows = [row(1), row(2)]
    sl = ss.CONTROL_COLUMNS.index("select_list")
    assert rows[0][sl] is not rows[1][sl]
    got = ss.StateStore(FakeConn(fetch=rows)).list_task_configs("tc")
    assert got[0]["select_list"] == "id, v" and got[0]["select_list"] is got[1]["select_list"]
    assert [g["src_database"] for g in got] == ["db1", "db2"] and got[1]["id"] == 2


# ---- per-source query caps: shared locks on the lock connection ------------------------------------

def test_source_shares_divide_the_cap_between_the_runs_reading_a_source():
    keys = {"connection:a": 11, "connection:b": 22}
    assert ss.source_shares(64, keys, {11: 4}) == {"connection:a": 16, "connection:b": 64}  # b: this run alone
    assert ss.source_shares(10, keys, {11: 40, 22: 3}) == {"connection:a": 1, "connection:b": 3}   # at least 1


def test_source_lock_keys_are_stable_31_bit_hashes():
    key = ss.source_lock_key("connection:sales_conn")
    assert key == ss.source_lock_key("connection:sales_conn") and 0 <= key < 2 ** 31
    assert key != ss.source_lock_key("connection:other") and 0 < ss.SOURCE_LOCK_CLASS < 2 ** 31


def test_join_sources_holds_one_shared_lock_per_source_and_counts_their_holders():
    conn = FakeConn()
    store = ss.StateStore(conn)
    keys = store.join_sources(["connection:b", "connection:a", "connection:b"])
    assert list(keys) == ["connection:a", "connection:b"]
    assert conn.calls == [("select pg_advisory_lock_shared(%s::int, %s::int)", (ss.SOURCE_LOCK_CLASS, keys[s]))
                          for s in ("connection:a", "connection:b")]
    conn.calls, conn.fetch = [], [(keys["connection:a"], 3)]
    assert store.source_runs(keys.values()) == {keys["connection:a"]: 3}
    (sql, params), = conn.calls
    assert "mode = 'ShareLock'" in sql and "classid::bigint = %s" in sql and "objid::bigint = any(%s)" in sql
    assert params == (ss.SOURCE_LOCK_CLASS, list(keys.values()))


# ---- schema v2: lakefed_cursor, expression cursor columns (US3) ------------------------------------------

def test_schema_v2_names_and_columns():
    assert ss.SCHEMA == "lakefed_cursor" and ss.SCHEMA_VERSION == 2 and ss.RENAMES == []
    assert "lakefed_ingest." not in _DDL
    stmts = ss.split_sql_statements(_DDL)
    assert ddl_problems(stmts) == []
    for col in ("cursor_expression", "cursor_kind", "cursor_lookback_s", "delete_mode", "delete_min_interval_s",
                "null_cursor_policy"):
        assert col in _DDL and col in ss.CONTROL_COLUMNS, col
    assert "last_delete_snapshot_at" in _DDL and "last_delete_snapshot_at" in ss.get_checkpoints_sql()
    assert "load_type             text        not null default 'cursor'" in _DDL
    assert "load_type in ('cursor', 'full')" in _DDL
    for name in ("valid_load_type", "valid_src_type", "valid_cursor_kind", "valid_delete_mode", "valid_null_cursor_policy",
                 "valid_cursor_metadata", "valid_full_metadata", "valid_source", "valid_remote_query", "valid_intervals"):
        assert f"drop constraint if exists {name}" in _DDL and f"add constraint {name}" in _DDL, name
    assert "create unique index if not exists control_sink_client_uq" in _DDL
    code = "\n".join(l.split("--")[0] for l in _DDL.splitlines()).lower()
    for old in ("valid_ct_metadata", "valid_write_mode", "valid_cdc_metadata", "'ct'", "'cdc'", "'append'"):
        assert f"add constraint {old}" not in code and (old not in code or old.startswith("valid")), old
    assert "drop table if exists lakefed_cursor.reseed_queue" in _DDL


def test_gap_11_dead_columns_dropped():
    for col in ("job_name", "src_catalog", "enable_iceberg_reads", "sink_cluster_cols", "source_instance_group",
                "warehouse_id", "ct_enabled", "cdc_capture_instance", "cursor_column", "write_mode"):
        assert f"alter table lakefed_cursor.control drop column if exists {col}" in _DDL, col
        assert col not in ss.CONTROL_COLUMNS, col
    for col in ("ct_begin_version", "pending_position", "pending_run_id"):
        assert f"alter table lakefed_cursor.checkpoint drop column if exists {col}" in _DDL, col


def test_record_delete_snapshot_sql_stamps_one_control_id():
    sql = ss.record_delete_snapshot_sql()
    assert sql.startswith(f"update {ss.CHECKPOINT_TABLE} set last_delete_snapshot_at = now()")
    assert "where control_id = %s" in sql and sql.count("%s") == 1
    conn = FakeConn(); ss.StateStore(conn).record_delete_snapshot(CFG)
    assert conn.calls == [(sql, (7,))]
