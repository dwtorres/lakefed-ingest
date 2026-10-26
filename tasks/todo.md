# speckit-implement: 001-cursor-only-engine (2026-10-26)

Driver: one subagent per phase, sequential. Gate per phase: `pytest tests` green, zero skips, grep check, one Conventional Commit, task lines marked [X] in specs/001-cursor-only-engine/tasks.md.

- [X] Phase 1 Setup (T001-T012; T012a blocked: live Databricks run needs operator --profile)
- [ ] Phase 2 Worker module (T013-T016)
- [ ] Phase 3 US1 SQL Server cursor (T017-T021)
- [ ] Phase 4 US4 Safe window bounds (T022-T034)
- [ ] Phase 5 US2 Oracle (T034a blocked: live baseline)
- [ ] Phase 6 US3 Expression cursor, schema v2, names
- [ ] Phase 7 US6 Quiet tables
- [ ] Phase 8 US5 PK snapshot deletes
- [ ] Phase 9 US7 Auth modes
- [ ] Phase 10 Polish + docs (devtest run tasks blocked)

## Checkpoints
- Phase 1 done (sha below): CT, CDC, append, pending paths deleted; ct_engine split into source_io + sink_writer; 324 tests, 0 skips; src 4381 -> 3106 lines
