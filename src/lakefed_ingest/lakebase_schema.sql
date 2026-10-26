-- Lakebase (OLTP Postgres) schema for lakefed_cursor: the control table (one row per source table),
-- the per-table checkpoint, and the run / event telemetry. Config reads and all state writes run on
-- the job cluster through the Postgres client. Warehouse-free: everything runs on the cluster.
--
-- Naming: `control.src_type` selects the SOURCE ADAPTER (source_adapters.py) and `control.load_type`
-- the LOAD STRATEGY (load_strategies.py: cursor | full). `checkpoint.position` is the cursor
-- position JSON {expr, kind, type, value} (contracts/position.md); a `full` row has no position.
--
-- Idempotent: re-applying this file migrates an existing instance in place. It is applied in one
-- transaction by state_store.StateStore.migrate: from the setup job, and from an ingestion run that
-- finds the database behind state_store.SCHEMA_VERSION (bump it with every change here). Statements
-- are split on ';', so no DO $$ blocks here, and every statement must be valid in a transaction (no
-- CREATE INDEX CONCURRENTLY). Every upgrade re-applies this whole file from any earlier version:
-- keep each statement idempotent and additive (docs/lakebase_schema.md, "Changing the schema";
-- checked by tests/state_store_test.py). The `drop column if exists` statements below remove the
-- Change Tracking, CDC and append-mode columns of the root version; each is reviewed there.

create schema if not exists lakefed_cursor;

-- Schema versions applied (state_store.SCHEMA_VERSION), one row each. An ingestion run whose code
-- needs a newer version than the newest here applies this file itself before it runs.
create table if not exists lakefed_cursor.schema_version (
    version     integer     primary key,
    applied_at  timestamptz not null default now(),
    applied_by  text        not null default current_user
);

-- Task config. One row per (source database, table). primary_key is comma-separated text.
-- Cursor rows (contracts/control_row.md): cursor_expression is ONE SQL expression over one to three
-- source columns of one type, folded with GREATEST / COALESCE; cursor_kind names its clock.
create table if not exists lakefed_cursor.control (
    id                    bigserial primary key,
    task_collection       text        not null,
    src_type              text        not null default 'sqlserver',  -- source adapter
    src_connection        text,                               -- UC connection: read through it (use_remote_query), else legacy host lookup
    src_host              text,                               -- source host (JDBC; no UC connection)
    src_port              int,                                -- NULL -> the adapter's default port
    secret_key            text,                               -- per-DB creds secret key (JSON {user,password})
    src_database          text,                               -- database (Oracle: service name)
    src_schema            text        not null,
    src_table             text        not null,
    sink_catalog          text        not null,
    sink_schema           text        not null,
    sink_table            text        not null,
    client_id             text,                               -- set: consolidated sink shared by client_id
    client_attrs          text,                               -- JSON {column: value}: identity columns on every row
    primary_key           text,                               -- comma-separated; required for cursor
    load_type             text        not null default 'cursor',  -- load strategy: cursor | full
    load_partitioned      boolean     not null default false,
    partition_col         text,                               -- partitioned seed: numeric/date/datetime col
    partition_size_mb     int,                                -- partitioned seed: target MB per partition
    select_list           text        not null,
    use_remote_query      boolean     not null default false,  -- read through src_connection (remote_query), not JDBC
    cursor_expression     text,                               -- cursor: the expression, in the source dialect
    cursor_kind           text,                               -- cursor: rowversion | db_timestamp | app_timestamp | identity
    cursor_lookback_s     int,                                -- cursor: lookback seconds (NULL: the job parameter)
    delete_mode           text        not null default 'none',   -- none | pk_snapshot (soft_column, full_compare: reserved)
    delete_min_interval_s int,                                -- pk_snapshot: seconds between snapshots (NULL: 86400)
    null_cursor_policy    text        not null default 'reject', -- reject | seed_only (NULL-expression rows)
    jdbc_options          text,                               -- extra Spark JDBC read options (JSON object)
    tier                  text,                               -- placement: cadence/SLA tier
    priority              int,                                -- placement: sweep priority
    task_enabled          boolean     not null default true
);

-- Columns of the root version this engine does not read (constitution, fixed decision 1; FR-004).
alter table lakefed_cursor.control drop column if exists job_name;
alter table lakefed_cursor.control drop column if exists src_catalog;
alter table lakefed_cursor.control drop column if exists enable_iceberg_reads;
alter table lakefed_cursor.control drop column if exists sink_cluster_cols;
alter table lakefed_cursor.control drop column if exists source_instance_group;
alter table lakefed_cursor.control drop column if exists warehouse_id;
alter table lakefed_cursor.control drop column if exists ct_enabled;
alter table lakefed_cursor.control drop column if exists cdc_capture_instance;
alter table lakefed_cursor.control drop column if exists cursor_column;
alter table lakefed_cursor.control drop column if exists write_mode;

-- Constraints are (re-)declared here so re-applying the file updates existing tables.
alter table lakefed_cursor.control drop constraint if exists valid_load_type;
alter table lakefed_cursor.control add constraint valid_load_type check (
    load_type in ('cursor', 'full')
);
alter table lakefed_cursor.control drop constraint if exists valid_src_type;
alter table lakefed_cursor.control add constraint valid_src_type check (
    src_type in ('sqlserver', 'oracle', 'postgresql', 'mysql')
);
alter table lakefed_cursor.control drop constraint if exists valid_cursor_kind;
alter table lakefed_cursor.control add constraint valid_cursor_kind check (
    cursor_kind is null or cursor_kind in ('rowversion', 'db_timestamp', 'app_timestamp', 'identity')
);
alter table lakefed_cursor.control drop constraint if exists valid_delete_mode;
alter table lakefed_cursor.control add constraint valid_delete_mode check (
    delete_mode in ('none', 'soft_column', 'pk_snapshot', 'full_compare')
);
alter table lakefed_cursor.control drop constraint if exists valid_null_cursor_policy;
alter table lakefed_cursor.control add constraint valid_null_cursor_policy check (
    null_cursor_policy in ('reject', 'seed_only')
);
-- Cursor tasks need a PK (increments are MERGEd by PK), the expression and its kind.
alter table lakefed_cursor.control drop constraint if exists valid_cursor_metadata;
alter table lakefed_cursor.control add constraint valid_cursor_metadata check (
    load_type <> 'cursor'
    or (primary_key is not null and cursor_expression is not null and cursor_kind is not null)
);
-- A full reload carries no cursor.
alter table lakefed_cursor.control drop constraint if exists valid_full_metadata;
alter table lakefed_cursor.control add constraint valid_full_metadata check (
    load_type <> 'full' or (cursor_expression is null and cursor_kind is null)
);
-- A source to read: a host (JDBC), or a UC connection.
alter table lakefed_cursor.control drop constraint if exists valid_source;
alter table lakefed_cursor.control add constraint valid_source check (
    src_host is not null or src_connection is not null
);
-- A read through a Unity Catalog connection (remote_query) needs the connection; no host or secret.
alter table lakefed_cursor.control drop constraint if exists valid_remote_query;
alter table lakefed_cursor.control add constraint valid_remote_query check (
    not use_remote_query or src_connection is not null
);
alter table lakefed_cursor.control drop constraint if exists valid_intervals;
alter table lakefed_cursor.control add constraint valid_intervals check (
    coalesce(cursor_lookback_s, 0) >= 0 and coalesce(delete_min_interval_s, 0) >= 0
);
-- Partitioned seed needs a partition column and a positive partition size.
alter table lakefed_cursor.control drop constraint if exists valid_partitioning;
alter table lakefed_cursor.control add constraint valid_partitioning check (
    not load_partitioned or (partition_col is not null and coalesce(partition_size_mb, 0) > 0)
);
-- jdbc_options and client_attrs, when set, must be JSON objects.
alter table lakefed_cursor.control drop constraint if exists valid_jdbc_options;
alter table lakefed_cursor.control add constraint valid_jdbc_options check (
    jdbc_options is null or jsonb_typeof(jdbc_options::jsonb) = 'object'
);
alter table lakefed_cursor.control drop constraint if exists valid_client_attrs;
alter table lakefed_cursor.control add constraint valid_client_attrs check (
    client_attrs is null or jsonb_typeof(client_attrs::jsonb) = 'object'
);
-- client_id becomes a Delta predicate literal: a restricted character set (same rule as sink_writer).
alter table lakefed_cursor.control drop constraint if exists valid_client_id;
alter table lakefed_cursor.control add constraint valid_client_id check (
    client_id is null or client_id ~ '^[A-Za-z0-9_.:-]{1,128}$'
);
-- One writer per sink slice: a dedicated sink (client_id NULL) has exactly one control row, and a
-- consolidated sink at most one row per client. (A sink mixing a dedicated row with consolidated
-- rows is rejected at plan time, per table.) Fails to apply if existing rows already collide.
create unique index if not exists control_sink_client_uq
    on lakefed_cursor.control (lower(sink_catalog), lower(sink_schema), lower(sink_table),
                               coalesce(client_id, ''));

create index if not exists control_collection_idx
    on lakefed_cursor.control (task_collection)
    where task_enabled = true;
-- The dedicated rows, by sink: each run's mixed-sink check starts here (state_store.mixed_sinks_sql).
create index if not exists control_dedicated_sink_idx
    on lakefed_cursor.control (lower(sink_catalog), lower(sink_schema), lower(sink_table))
    where client_id is null;

-- Checkpoint, one row per control task (keyed by control.id). Keying by the control id (not
-- db/schema/table) keeps same-named databases on different servers from colliding; the src_*
-- columns are kept for readability. `position` is the cursor position JSON (NULL for a full reload).
create table if not exists lakefed_cursor.checkpoint (
    control_id              bigint      not null,
    src_database            text        not null,
    src_schema              text        not null,
    src_table               text        not null,
    strategy                text        not null default 'cursor',  -- load strategy that wrote the position
    position                text,                              -- resume position (contracts/position.md)
    phase                   text        not null default 'seeding',  -- seeding | incremental (cursor) | loaded (full)
    status                  text        not null default 'ok', -- ok | running | failed | quarantined
    detail                  text,                              -- last error / note
    select_list             text,                              -- the select_list the sink was last written with
    last_success_at         timestamptz,                       -- freshness: last successful ingest
    last_delete_snapshot_at timestamptz,                       -- delete_mode pk_snapshot: last snapshot
    updated_at              timestamptz not null default now(),
    constraint checkpoint_control_pk primary key (control_id)
);

-- Change Tracking and append mode are gone (constitution II; gap 11).
alter table lakefed_cursor.checkpoint drop column if exists ct_begin_version;
alter table lakefed_cursor.checkpoint drop column if exists pending_position;
alter table lakefed_cursor.checkpoint drop column if exists pending_run_id;
drop table if exists lakefed_cursor.reseed_queue;

alter table lakefed_cursor.checkpoint drop constraint if exists valid_phase;
alter table lakefed_cursor.checkpoint add constraint valid_phase check (
    phase in ('seeding', 'incremental', 'loaded')
);

-- ---------------------------------------------------------------------------------------
-- Observability / telemetry. One ingestion run processes many tables inside ONE job task, so
-- per-table progress is not visible in the Jobs UI. These tables re-create that visibility via
-- concurrent point writes (one ingest_event row per (run, table): INSERT at start, UPDATE at
-- finish), queryable live through the Lakebase UC catalog (see docs/monitoring.md).
-- ---------------------------------------------------------------------------------------

-- One row per ingestion run (one lakefed_cursor run over a task_collection).
create table if not exists lakefed_cursor.ingest_run (
    run_id          bigserial   primary key,
    task_collection text        not null,
    job_run_id      text,
    cluster_id      text,
    parallelism     int,
    started_at      timestamptz not null default now(),
    finished_at     timestamptz,
    status          text        not null default 'running',   -- running | completed | failed | skipped
    total           int,
    ok              int,
    failed          int,
    skipped         int,
    reseeded        int,
    detail          text
);

create index if not exists ingest_run_collection_idx
    on lakefed_cursor.ingest_run (task_collection, started_at);

-- One row per (run, table): per-table telemetry for a run.
create table if not exists lakefed_cursor.ingest_event (
    id              bigserial   primary key,
    run_id          bigint,                                   -- -> ingest_run.run_id
    task_collection text,
    control_id      bigint,                                   -- -> control.id
    src_database    text,
    src_schema      text,
    src_table       text,
    sink_fqn        text,
    action          text,                                     -- seed | increment | reseed | full
    phase           text,
    stage           text,                                     -- plan | ensure_sink | read | seed | merge | load | checkpoint
    status          text        not null default 'running',   -- running | ok | failed | skipped | retried (an attempt tried again)
    started_at      timestamptz not null default now(),
    finished_at     timestamptz,
    duration_ms     bigint,
    rows_read       bigint,
    rows_merged     bigint,
    position_from   text,                                     -- checkpoint position before / after
    position_to     text,
    pool            text,
    error           text,
    retry_count     int         not null default 0          -- earlier attempts of this table in the run (table_retries)
);

create index if not exists ingest_event_run_idx
    on lakefed_cursor.ingest_event (run_id);
create index if not exists ingest_event_failed_idx
    on lakefed_cursor.ingest_event (run_id)
    where status = 'failed';
create index if not exists ingest_event_collection_idx
    on lakefed_cursor.ingest_event (task_collection, finished_at);
