# speckit-implement: 001-cursor-only-engine (2026-10-26)

Driver: one subagent per phase, sequential. Gate per phase: `pytest tests` green, zero skips, grep check, one Conventional Commit, task lines marked [X] in specs/001-cursor-only-engine/tasks.md.

- [X] Phase 1 Setup (T001-T012; T012a blocked: live Databricks run needs operator --profile)
- [X] Phase 2 Worker module (T013-T016)
- [X] Phase 3 US1 SQL Server cursor (T017-T021)
- [X] Phase 4 US4 Safe window bounds (T022-T034)
- [ ] Phase 5 US2 Oracle (T034a blocked: live baseline)
- [ ] Phase 6 US3 Expression cursor, schema v2, names
- [ ] Phase 7 US6 Quiet tables
- [ ] Phase 8 US5 PK snapshot deletes
- [ ] Phase 9 US7 Auth modes
- [ ] Phase 10 Polish + docs (devtest run tasks blocked)

## Checkpoints
- Phase 1 done c3a4ca9: CT, CDC, append, pending paths deleted; ct_engine split into source_io + sink_writer; 324 tests, 0 skips; src 4381 -> 3106 lines
- Phase 2 done 3b1e79f: ingest_one, _replan, _pools_label moved to worker.py with a deps object; 9 worker tests pin write-before-advance; 333 tests, 0 skips
- Phase 3 done 1720487: config_error rejects any load_type but cursor/full; cursor_seed -> cursor routes and worker hi -> merge -> advance order pinned; 336 tests, 0 skips
- Phase 4 done 7a39705: lookback_s argument (job param cursor_lookback_s, row override) replaces the env var; temporal hi capped at source clock + cursor_skew_s (alert=max_above_clock in the log and checkpoint detail); identity kind reads exact and logs inserts_only; hi below position fails the table; datetime literals at 3 digits; 355 tests, 0 skips; dialect tests cover sqlserver + postgresql (oracle in Phase 5)
