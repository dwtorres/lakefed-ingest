---
ontology: true
type: decision
domain: lakefed-ingest
summary: Pattern analysis of edp-data-engineering and lakefed-ingest, plus a survey of cursor-only ingestion options on Databricks, as input to a clean orchestration engine
tags: [ingestion, cursor, watermark, orchestration, databricks, analysis]
related: [lakebase_schema, deploy, monitoring]
status: active
discovered: 2026-10-25
verified: false
workspace: lakefed-ingest
---

# Cursor-only orchestration engine: pattern analysis

Date: 2026-10-25. Scope: the two in-house frameworks and the market. Constraints from the owner: every load uses a cursor column. A cursor column is a column whose value grows on every change. Change Tracking is not available. Everything runs inside Databricks unless a capability is missing.

Three audits feed this document. One read `/Users/dwtorres/src/work/edp-data-engineering`. One read the root version of `/Users/dwtorres/src/work/lakefed-ingest` at commit 72255e0. One surveyed the web with Tavily and Firecrawl plus the Databricks docs. The runbook at `~/Downloads/Lakefed Ingest Runbook.html` (commit 4499739) was read too. No code was changed. No live database was queried, so every "likely bug" below needs a live test.

## 1. Short answer

Build the new engine from the lakefed-ingest root version, not from edp (decided 2026-10-25). The engine must work for SQL Server and Oracle from day one. Oracle cursor support in lakefed is a placeholder today, so the team must implement it. The known SQL Server bugs and caveats in section 3.4 must be patched. Keep about 60 percent of lakefed (reads, writes, seeds, thread pool, retries, locks, telemetry, Lakebase state). Rewrite the planner, the notebook worker, and the state defaults, because they are shaped around Change Tracking. Take four ideas from edp: one visible unit of work per table, resume by (run_key, table), `empty` as a status, and the backfill-then-cutover switch. Take the cursor rules from the market survey (section 5). Lakeflow Connect query-based connectors, which Databricks shipped GA on 2026-05-29 for exactly this problem, are a comparison baseline and not a dependency. The connector allows exactly one cursor column. The Oracle tables can have up to three candidate cursor columns. Design call 14 covers this.

## 2. What edp actually does

### 2.1 Orchestration

Two generations run side by side.

The legacy path is YAML jobs. A scheduler job runs a wrapper job. The wrapper runs leg jobs. Each leg reads a JSON metadata file into a task value. Then a `for_each` runs one notebook per table with concurrency 10 and three retries (`batch_source_to_bronze.yml:14-31`). Compute is a classic job cluster, D8ads_v5, autoscale 2 to 20.

The chunked path is Python-generated jobs (`bundles/data_engineering/resources/src/python_dab/chunk_jobs.py`). One job per source. Tasks are split_bronze, then N bronze bands, then split_silver, then N silver bands, then cleanup, then a summary. Bands run in two lanes. Band i+2 waits on band i (`chunk_jobs.py:260-264`). Bronze iterations run on a job cluster with concurrency 8 per band, so 16 source sessions at most. Silver iterations run on serverless with concurrency 20.

A non-Fabric table on the chunked path makes five hops: job, band, `chunk_worker.py`, `ingest/notebook.py`, then `dbutils.notebook.run` on the legacy notebook with a 3600 second timeout. No thread pool exists in any ingestion path. `ingest/pool.py` is a leftover name and holds only a dataclass and an error sanitizer.

### 2.2 The "chunking" that works

The chunking does not split a table into row ranges. It splits the list of tables into `for_each` bands. Each band's input must fit under the 48 KiB task-value cap. So keys are shortened to `i` and `d`. The full manifest goes to a UC Volume as JSON (`ingest/chunking.py:107-136`, `notebooks/ingest/split_items.py:83-108`). The band count is fixed at deploy time from the repo metadata. If demand exceeds the deployed bands, the run fails (`split_items.py:76-81`).

Why it works: one table is one visible, repairable iteration. State per (run_key, source, dataset, stage) makes a rerun skip finished tables. If any table failed, the summary task fails the run.

Why it is hacked: the topology is frozen at deploy time. Lane count and concurrency are module constants. One slow table blocks every later band in its lane. The work list exists twice. Every iteration pays notebook startup, a `CREATE TABLE IF NOT EXISTS`, and a parse of a 62,658 line JSON file. `DEFAULT_BAND_COUNT_SOURCE = "clarity_poc"` and the `zc_` prefix are hardcoded.

The only three places that split a table by range are manual or broken. `source_to_bronze_with_bounds.py` does not parse (line 132). The legacy `numPartitions` option is set without `partitionColumn`, `lowerBound`, or `upperBound`, so Spark reads each table on one connection (`source_to_bronze.py:201, 242, 261`). The comment "peak JDBC connections = concurrency x num_partitions" is false.

### 2.3 Incremental mechanics

Legacy path:

1. The watermark is `max(watermark_time)` over an append-only history table, minus a hardcoded 10 minutes (`source_to_bronze.py:88-91`).
2. The read has no upper bound. Oracle uses `modified > wm OR created > wm`. SQL Server compares the timestamp as a string (`source_to_bronze.py:214-221`). Values are truncated to seconds.
3. Bronze is append-only with a new `_run_id` per run. Overlap rows are duplicated into bronze. A retry appends the same rows again.
4. The new watermark is `MAX(COALESCE(modified, created))` of the rows landed. When rows = 0, no watermark is written. A row stamped before the watermark but committed after the read is lost.
5. Silver "upsert" means SCD2 through a staged MERGE with no per-PK dedup (`silver/temp_helpers.py:56-189`). Overlap rows can produce several current rows or a multiple-match failure. `ALTER TABLE CLUSTER BY` and `OPTIMIZE` run on every batch.

Fabric path (`ingest/fabric.py`):

1. The watermark is the Databricks clock, not a source value (`fabric.py:453, 498`). Clock skew to the source is unverified.
2. The read is a half-open window `col > start AND col <= end` with a configurable overlap. This part is correct.
3. Bronze overwrite uses `replaceWhere _run_id = run_key`, so a retry is idempotent. This part is correct.
4. Silver "upsert" is a Type-1 MERGE with no change check and no delete branch. The same metadata word means two different things in the two engines.
5. 1,560 of 3,714 Clarity datasets are `full_current_state`. Each run appends a full copy to bronze. No code deletes old bronze runs.

Deletes are never handled. `_hard_delete` is always false. Two fact tables are disabled after a 27 hour full load (`docs/TODO_full_load_fact_table_optimization.md:17-37`).

### 2.4 State and metadata

Control metadata is repo JSON, read by relative path from notebooks. The poc and tst files are 62,658 line copies that differ in two values. Several keys are never read. `environment.json` is loaded in nine notebooks and never used.

Run state lives in two stores: `orchestration.watermarks` for the legacy path and `orchestration.ingest_state` for the chunked path. Both are blind appends. Watermark lookups run `max()` over tables that are never compacted and grow by about 7,400 rows per Clarity run.

### 2.5 Top smells, ranked by impact

1. Two engines, two watermark stores, two meanings of "upsert".
2. No delete handling, which forces full reloads on fact tables.
3. Full-copy appends with no retention, and MERGE without a change check, across about 1,560 tables.
4. Legacy reads with no upper bound, string timestamp compares, second truncation, and non-idempotent retries.
5. Six forked copies of `source_to_bronze`, one broken.
6. Cosmetic JDBC parallelism.
7. Deploy-time topology with hand-tuned constants and head-of-line blocking.
8. Non-atomic three-statement silver snapshot, OPTIMIZE per batch.
9. Copied metadata and drifted docs that cite deleted files.
10. Dead and invalid code (`dlaccel/ingestion/sql.py` is not valid Python).

Tests exist for the chunked path (about 1,800 lines, including 33 fake-based behavior tests for `fabric.py`). The legacy notebooks are tested only through source-text assertions, which pin how the code looks, not what it does.

### 2.6 Keep from edp

- One table per visible iteration, with resume keyed by (run_key, source, dataset, stage).
- `empty` as a first-class status.
- Silver promotion in bronze run_key order, stopped at the first failure.
- `replaceWhere` on `_run_id` for an idempotent bronze write.
- Half-open windows `> start AND <= end`.
- Dedup to the latest row per PK before MERGE.
- The injectable context with read and execute callables, which is what makes `fabric.py` testable.
- The explicit backfill-then-cutover metadata switch.
- One job per source as the concurrency lock, and the summary task that fails the run.

## 3. What lakefed-ingest actually does

### 3.1 Cursor mechanics

The cursor path is `src/lakefed_ingest/cursor_engine.py`, about 300 lines. Type lookup reads `sys.columns` on SQL Server and `information_schema.columns` on PostgreSQL. Kinds are numeric, temporal, rowversion. Float, real, and strings are rejected. Nullable columns are refused at seed and rechecked every run.

The high watermark is one `MAX(col)` query per table per run. For rowversion, `hi` is capped at `MIN_ACTIVE_ROWVERSION() - 1`, which never passes an open transaction (`cursor_engine.py:155-156`). This cap is the most important line in the module. The position is JSON `{col, type, value}` in `checkpoint.position`.

The read is `(lo, hi]` with the literal cast to the column type, so the predicate stays sargable. Temporal reads pull `lo` back by `LAKEFED_CURSOR_OVERLAP_S`, default 600 seconds. The overlap is a cluster environment variable, not a job parameter. The seed records `hi - seed_duration`. The read is one JDBC query with no partitioning, so a large backlog after an outage is a single-connection read.

The sink write is `MERGE WITH SCHEMA EVOLUTION` on the PK, or on (`__client_id__`, PK) for consolidated sinks. `WHEN MATCHED` is unconditional. There is no delete branch. The checkpoint advances only after the MERGE commits. These ordering invariants live in the notebook and have no unit test.

Support matrix: SQL Server real, PostgreSQL real since commit 72255e0, Oracle and MySQL placeholders. Dialect handling is `if _is_pg` branching inside `cursor_engine.py`, not adapter methods.

### 3.2 Orchestration

One job task ingests a whole task collection in process. A workspace allows at most 2,000 concurrent task runs and 10,000 submits per hour. That limit is why one task per table was rejected (runbook, section 1). `parallel.py` holds the thread pool, one FAIR pool per slot, the `WorkSlots` budget, and the per-source `SourceLimiter`. It is generic, tested, and holds no slot while waiting, so nested fan-out cannot deadlock. Cross-run source shares come from `pg_locks` counts of shared advisory locks. `set_pool` calls `spark.sparkContext`, which works on classic single-user clusters only.

The collection lock is `pg_try_advisory_lock` on a dedicated session. A duplicate run exits `skipped`. The lost-lock guard checks every 30 seconds. `table_retry.py` classifies transient and permanent errors with a per-run budget and a per-source stop.

Cursor rows bypass every CT structure in the planner. They never enter `group_by_source`, `detect_group`, or the bulk advance. Each cursor table costs one source query plus four Lakebase writes per run. When nothing changed, the cost is the same. `freshness_interval_s` does not apply to cursor tables.

### 3.3 Partitioned seed

`partitions.py` and `ct_engine.py:987-1082`. Size from catalog stats. N = max(size_mb / partition_size_mb, 2). MIN and MAX of `partition_col`. Spark-JDBC stride. Each range runs as one unit through the slots into a staging table, then one `INSERT OVERWRITE` swap. On failure the target is untouched and every range restarts next run. The stride is uniform over value space, so skewed keys give uneven chunks. If `partition_col` is mutable, a row can land in two ranges and give a duplicate PK that the MERGE never fixes. Use the PK as the partition column.

### 3.4 Gaps for a cursor-only world

1. Deletes are never captured. The docstring says to use CT or a periodic full reload.
2. Late commits on temporal cursors are covered only within 600 seconds of `MAX(col)`, anchored to the data, not to the source clock. A transaction longer than that is missed for good.
3. Identity and sequence cursors lose rows. A value is allocated at insert, not at commit. Transaction A takes id 100, B takes 101 and commits, the run reads hi = 101, then A commits. Row 100 is gone. The code comment that numeric bounds "stay exact" is false. Only rowversion is safe.
4. No upper sanity cap. One row stamped 2099 pins `hi` there, and every later row below 2099 minus the overlap is skipped silently. DST on a local-time `datetime` moves the clock back 3600 seconds, more than the 600 second overlap.
5. Likely bug: `_minus_seconds` emits six fractional digits (`cursor_engine.py:210-219`, local output `...09:40:00.123000`). `CONVERT(datetime, N'...', 126)` accepts three. Any SQL Server `datetime` cursor with nonzero milliseconds likely fails every increment with error 241. Tests cover `datetime2` only. Not live-tested.
6. No multi-column, expression, or tuple cursor. `COALESCE(updated_at, created_at)` is impossible because nullable columns are refused.
7. Ties at a non-unique numeric boundary are lost. No (cursor, pk) paging.
8. A source truncate or reload is not detected. A smaller `hi` moves the position backwards and reads an empty window.
9. Append mode is forbidden for cursor, so a change-log or SCD2 sink is not possible today.
10. Every quiet temporal table does a source read plus a MERGE with an unconditional UPDATE per run. Each run costs a Delta commit and file rewrites for the overlap rows.
11. Dead control columns: `job_name`, `src_catalog`, `enable_iceberg_reads`, `sink_cluster_cols`, `source_instance_group`, `warehouse_id`. `reseed_queue` is written and never read.
12. Docs drift. README and module headers still say cursor is SQL Server only.

### 3.5 Survival estimate

| Module | Survives a cursor-only engine |
|---|---|
| `parallel.py` | 100% |
| `partitions.py` | 100% |
| `source_adapters.py` | 100%, needs cursor methods |
| `table_retry.py` | about 90%, drop CT markers |
| `cursor_engine.py` | about 85%, after dialect refactor and bug fixes |
| `state_store.py` | about 75% |
| `ct_engine.py` | about 55%, rename to source I/O and sink writer |
| `load_strategies.py` | about 30% |
| `planner.py` | about 25% |
| `batch_append.py` | 0% |
| `ingest.ipynb` | about 35% |

Total about 2,750 of 4,381 lines. About a quarter of those need a move or rename, not a rewrite.

## 4. Market survey: what others do

### 4.1 Databricks-native options

Lakeflow Connect query-based connectors. Public Preview 2026-04-15, GA 2026-05-29. Sources in foreign-connection mode: Oracle, Teradata, SQL Server, MySQL, MariaDB, PostgreSQL. Mechanism: store `max(cursor)` after each run, then read `cursor > high-water mark`. For SCD type 1 and 2 with a timestamp cursor it re-reads "a short window of rows just below the high-water mark". The window size is not documented. Rules: exactly one cursor column, monotonic, NULL rows never ingested, types timestamp, date, numeric, binary (rowversion works), string. Soft deletes via `deletion_condition`. Hard deletes via a periodic PK snapshot (Beta, API only, needs primary keys, not with SCD type 2). Triggered only. Serverless by default. Parallel read partition column is auto-picked, override via the account team. Billed as `LAKEFLOW_CONNECT` DBUs.
https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-overview
https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-reference
https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-limits

Lakeflow Connect CDC connectors need Change Tracking, CDC, binlog, or logical replication, plus a gateway on classic compute. Out of scope.

Lakehouse Federation pushes simple predicates and supports `fetchSize` and the four partition options, but cannot send dialect SQL such as `MIN_ACTIVE_ROWVERSION()`. Databricks positions it for ad hoc use.
https://docs.databricks.com/aws/en/query-federation/performance-recommendations

`remote_query()` pushes a literal dialect query through a UC connection, with fetchsize and the partition options. Needs DBR 18.3+ or warehouse 2025.35+. Serverless compatible. This is the best Databricks-only path for dialect-exact cursor reads without secrets in code.
https://docs.databricks.com/aws/en/sql/language-manual/functions/remote_query

Spark JDBC. All four partition options are needed to parallelize. Keep `numPartitions` small, sized to the source's connection limit. On serverless, raw URL plus secret JDBC is not allowed. Serverless needs a UC JDBC connection and DBR 17.3 LTS or later. Driver JARs come from a volume. Driver memory is capped at 400 MiB. The connection is not usable inside Lakeflow pipelines.
https://docs.databricks.com/aws/en/connect/jdbc-connection

Lakeflow Spark Declarative Pipelines `AUTO CDC FROM SNAPSHOT` diffs in-order snapshots, gives SCD 1 and 2, and finds hard deletes. It reads the full table every run, so it fits small tables only.
https://docs.databricks.com/aws/en/ldp/cdc

Lakeflow Jobs `for_each` plus control table. Databricks now ships an official watermark tutorial with this pattern. The tutorial itself warns that a strict `>` can skip rows that share the stored timestamp.
https://docs.databricks.com/aws/en/jobs/how-to/foreach-watermark-tutorial

dlt-meta drives Spark Declarative Pipelines from JSON, but no JDBC cursor source was found in its docs. Unverified. Zerobus is push only.

### 4.2 Cursor rules from the field

Sources: dlthub incremental, Airbyte, Singer and Meltano, Fivetran, Estuary, Azure Data Factory, Debezium, Flink CDC, and the SQL Server rowversion docs. URLs are in the survey appendix.

1. Capture `hi` before the read and read `(lo, hi]`. Never advance to max of rows written. Azure Data Factory and lakefed do this. edp legacy does not.
2. When the cursor is unique and strictly monotonic (rowversion, identity), use an exclusive lower bound. In all other cases, use an inclusive lower bound plus PK dedup (dlt, Singer, Airbyte).
3. Timestamp cursors are not unique. Use a composite (updated_at, pk) with lexicographic paging, or overlap plus idempotent MERGE.
4. A row stamped before but committed after the watermark is lost without a lookback. Size the lookback to the longest source transaction (dlt `lag`, Lakeflow "short window", Airbyte resend-when-ambiguous). Overlap does not fix app-server clock skew. Prefer DB-generated defaults or engine counters.
5. Cursor ranking, best first: engine counter (rowversion, `ORA_ROWSCN`, `xmin` with wraparound caveat). Then DB-side timestamp with overlap. Then app timestamp. Then auto-increment id (inserts only). Then created_at (inserts only).
6. Every managed tool skips NULL cursor rows. Decide this per table and seed NULL rows through the full load.
7. Updates that do not bump the cursor are invisible to every tool. Document the contract per table.
8. Deletes, pick one per table: soft-delete column pushed through MERGE as delete, periodic PK snapshot anti-join with a minimum interval, periodic full compare, or accept none.
9. A newly selected column needs a full refresh in Lakeflow. Type changes force a full reload everywhere.
10. Appends are not idempotent. Record a pending marker before the write.
11. `MIN_ACTIVE_ROWVERSION()` is the correct cap. `@@DBTS` misses active transactions.
https://learn.microsoft.com/en-us/sql/t-sql/functions/min-active-rowversion-transact-sql

### 4.3 Large initial loads

Range stride partitioning is skewed on sparse or clustered keys. Estimate with a `width_bucket` histogram first. Or use a hash or modulo surrogate as the partition column. Or use a predicate list by PK chunk (Debezium, Flink CDC). Capture V0 before the chunked read, stage to a temp table, swap atomically, then run the first increment from V0 with overlap. dlt backfills with stateless half-open ranges in parallel, then resumes from the final end value.

### 4.4 Trade-off matrix

| Option | Fit | Scale | Ops burden | Choose or reject because |
|---|---|---|---|---|
| Lakeflow Connect query-based | High | Good, auto partitioning | Lowest | If one cursor column, a PK, the default lookback, and no multi-tenant tagging suffice, choose it. If you need composite cursors, rowversion caps, a custom lookback, per-source throttling, or your own state, reject it. |
| Spark JDBC on a job cluster plus Lakebase state (lakefed root) | High | Best control | Highest, your code | When correctness guards and multi-client sinks matter, choose it. |
| `remote_query` plus `for_each` plus Delta control table (lakefed job_orchestration) | High | Good | Medium | Choose it for dialect features on serverless. If a warehouse dependency is unwanted, reject it. |
| Lakehouse Federation plus MERGE | Medium | Simple predicates only | Low | Reject for correctness-critical cursors. |
| SDP `AUTO CDC FROM SNAPSHOT` over JDBC | Medium | Poor on large tables | Low | Choose for small tables that need hard deletes and SCD2. |
| dlt-meta | Low | n/a | Medium | Reject, no JDBC cursor source found. |
| Zerobus | None for pull | High for push | Low | Reject unless an app pushes rows. |
| Fivetran, Airbyte, Estuary, dlt | Violates Databricks-only | n/a | External | Reject by constraint. Use as semantic reference only. |

## 5. Design decisions for the clean engine

Each one traces to a finding above. A call marked "decided 2026-10-25" was agreed by the owner. The other calls are recommendations.

1. Base: lakefed root version (decided 2026-10-25). Rename `ct_engine.py` to `source_io.py` and `sink_writer.py`. Delete `batch_append.py`, the CT branch of the planner, the pending and landed routes, the verify_run_id route, and the six dead control columns. Default `load_type` to `cursor`.
2. Move the per-table worker and its ordering invariants out of the notebook into a module with unit tests. Keep the notebook as a thin parameter shell.
3. Cursor contract per table in the control row: `cursor_column`, `cursor_kind` (rowversion, db_timestamp, app_timestamp, identity), `cursor_lookback_s`, `delete_mode` (none, soft_column, pk_snapshot, full_compare), `null_cursor_policy`. Make the lookback a job parameter with a per-row override, not a cluster environment variable.
4. For temporal cursors, cap `hi` at `min(MAX(col), source_now() + skew)`. When `MAX(col) > source_now()`, raise an alert. Keep the `MIN_ACTIVE_ROWVERSION()` cap. Apply the lookback to identity cursors as well, or mark them inserts-only and unsafe.
5. Fix the `datetime` precision rendering. Round to three digits for SQL Server `datetime` and `smalldatetime`.
6. Add `WHEN MATCHED AND source.cursor <> target.cursor` or a row-hash predicate, so quiet tables stop rewriting files.
7. Batch the `MAX(col)` queries per source database into one query, and bulk-advance unchanged cursor checkpoints the way CT tables do today.
8. Chunk large increments by cursor range with the existing `partitions.py`. Checkpoint per chunk during a seed. Then a failure at chunk 990 of 1000 does not restart from zero.
9. Deletes: implement `pk_snapshot` first. Read the source PK list through the same slots, anti-join against the sink, delete, on a per-table minimum interval. This is what Lakeflow's hard-deletion Beta and Fivetran's ctid snapshot do.
10. Allow append mode for cursor with dedup on (PK, cursor value), so an SCD2 silver can be fed from a change log.
11. Put dialect SQL in `source_adapters.py`. Add Oracle (`ORA_ROWSCN` or a timestamp) and MySQL (decided 2026-10-25, Oracle required from day one). Oracle notes: `ORA_ROWSCN` is block-level unless the table was created with ROWDEPENDENCIES, so it is not a safe row cursor by default. Timestamps and sequence IDs are the realistic Oracle cursors. Sequence IDs have the same late-commit hole as SQL Server identity (section 3.4, gap 3).
12. Keep the in-process thread pool and the one-task-per-collection shape, for both SQL Server and Oracle sources (decided 2026-10-25). It is the right answer to the 2,000 concurrent task-run limit. Plan the serverless question now: `set_pool` and raw JDBC do not run there. The serverless path is `remote_query` through a UC connection.
13. Before building any of this for a given source, run one table through a Lakeflow Connect query-based pipeline and compare cost and lag (decided 2026-10-25, adjusted). Lakeflow Connect is a comparison baseline only, not a dependency, because the connector allows exactly one cursor column.
14. Express the cursor as one SQL expression that picks the winner over up to three columns (decided 2026-10-25). Example: `GREATEST(COALESCE(c1, DATE '1900-01-01'), COALESCE(c2, DATE '1900-01-01'), COALESCE(c3, DATE '1900-01-01'))`. Caveats: Oracle `GREATEST` returns NULL if any input is NULL, so wrap each input in `COALESCE`. A predicate on an expression is not index-friendly unless a function-based index exists on the source, so expect a scan or ask the DBA for the index. All three columns must share one type and one clock. SQL Server has `GREATEST` only from SQL Server 2022. On older versions, use a `CASE` expression. The position stores the expression text, so a changed expression must trigger a reseed. The same expression can be handed to Lakeflow Connect only if the connector accepts an expression. The docs do not state this, so it is unverified.

## 6. Unverified claims

- The SQL Server `datetime` error 241 on the overlap literal. Needs a live run.
- `batch_source_to_silver.yml:9-10` hardcodes `source: ""`, so the millennium scheduler's source value can fail to reach `get_datasets`. Needs one run's parameters.
- Clock alignment between the Databricks clock and the Fabric `_LastUpdatedInstant` column.
- dlt-meta JDBC support, either way.
- The Lakeflow "short window" size, and whether it applies to binary and numeric cursors.
- Lakeflow Connect pricing numbers. The pricing page is a JavaScript widget and did not scrape.
- Whether a Lakeflow Connect query-based connector accepts a SQL expression as the cursor (design call 14).

## Appendix: survey sources

- https://docs.databricks.com/aws/en/release-notes/product/2026/may
- https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-overview
- https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-reference
- https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-limits
- https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-limits
- https://docs.databricks.com/aws/en/query-federation/performance-recommendations
- https://docs.databricks.com/gcp/en/query-federation/remote-queries
- https://docs.databricks.com/aws/en/sql/language-manual/functions/remote_query
- https://docs.databricks.com/aws/en/connect/external-systems/jdbc
- https://docs.databricks.com/aws/en/connect/jdbc-connection
- https://docs.databricks.com/aws/en/compute/serverless/limitations
- https://docs.databricks.com/aws/en/ldp/cdc
- https://docs.databricks.com/aws/en/ldp/database-replication
- https://docs.databricks.com/aws/en/jobs/how-to/foreach-watermark-tutorial
- https://docs.databricks.com/aws/en/jobs/how-to/foreach-sql-lookup-tutorial
- https://docs.databricks.com/aws/en/jobs/for-each
- https://www.databricks.com/blog/how-move-apache-airflowr-databricks-lakeflow-jobs
- https://www.databricks.com/blog/chaos-scale-templatizing-spark-declarative-pipelines-dlt-meta
- https://github.com/databricks-solutions/lakefed-ingest
- https://docs.databricks.com/aws/en/oltp
- https://docs.databricks.com/aws/en/ingestion/zerobus-overview
- https://docs.databricks.com/aws/en/admin/system-tables/serverless-billing
- https://dlthub.com/docs/general-usage/incremental/cursor
- https://dlthub.com/docs/general-usage/incremental/lag
- https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/incremental-append
- https://docs.airbyte.com/understanding-airbyte/connections/incremental-append-deduped
- https://sdk.meltano.com/en/latest/incremental_replication.html
- https://fivetran.com/docs/connectors/databases/sql-server
- https://fivetran.com/docs/connectors/databases/postgresql
- https://docs.estuary.dev/reference/Connectors/capture-connectors/SQLServer/sqlserver-batch
- https://docs.estuary.dev/reference/Connectors/capture-connectors/OracleDB/oracle-batch
- https://learn.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-overview
- https://learn.microsoft.com/en-us/azure/data-factory/copy-data-tool-metadata-driven
- https://debezium.io/blog/2021/10/07/incremental-snapshots
- https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/sqlserver-cdc
- https://learn.microsoft.com/en-us/sql/t-sql/functions/min-active-rowversion-transact-sql
- https://www.sunnydata.ai/blog/lakeflow-connect-query-based-capture-incremental-ingestion
- https://rebricked.org/databricks/query-based-connectors/
- https://luminousmen.com/post/spark-tips-optimizing-jdbc-data-source-reads
