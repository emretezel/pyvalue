# Agent Lessons

Use this file to capture recurring mistake patterns after user corrections so future work is more reliable.

## Entry Template
- Date:
- User correction:
- Recurring pattern:
- Preventive rule:
- Resulting action:

## Lessons

- Date: 2026-03-22
- User correction: `pyvalue report-ingest-progress` crashed after a quota helper signature change.
- Recurring pattern: Generalizing a shared helper without explicitly verifying every runtime call site can leave one CLI path behind even when related tests look adequate.
- Preventive rule: After changing a shared helper signature, grep all call sites, run the affected CLI commands directly in the target conda env, and add or keep a regression test that exercises each public command path using that helper.
- Resulting action: Recorded the rule here and rechecked the helper call sites plus the `report-ingest-progress` command path in the `pyvalue` env.

- Date: 2026-03-22
- User correction: Market-data and fundamentals quota behavior for the user's EODHD account did not match the assumption I inferred from prior implementation reasoning.
- Recurring pattern: Treating provider quota semantics as settled without reconciling account-specific user feedback against the public API contract can lead to confident but incomplete explanations.
- Preventive rule: When quota or billing behavior affects execution decisions, verify both the official provider docs and the actual API fields available in the integration path, and clearly separate documented shared limits from any account-specific behavior reported by the user.
- Resulting action: Recorded the distinction here and rechecked the public EODHD API Limits and User API docs before answering.

- Date: 2026-03-22
- User correction: Even with separate paid EODHD products, the practical daily limit for this setup is shared between fundamentals and market-data requests.
- Recurring pattern: Reading plan/product wording too literally can lead to the wrong quota model if actual usage accounting still rolls up into one shared daily counter.
- Preventive rule: For provider quota logic, prefer the authoritative runtime counter used by the integration path over marketing/billing wording unless the API exposes separate counters that can actually be enforced in code.
- Resulting action: Corrected the lesson to treat EODHD's current integration as a shared daily-budget model until distinct quota fields are available in the API response.

- Date: 2026-03-22
- User correction: `recalc-market-cap` should only update the latest market-data row for a ticker, not all historical rows for the security.
- Recurring pattern: Reusing a security-level update helper without checking time-series granularity can accidentally overwrite historical snapshots when the intended behavior is latest-row-only maintenance.
- Preventive rule: For snapshot tables keyed by `(entity_id, as_of)`, verify whether maintenance commands should update one row or all rows, and add a regression test with at least two dates before shipping the change.
- Resulting action: Narrowed `update_market_cap()` to the latest `as_of` row per security and added a regression test covering preserved historical market-cap rows.

- Date: 2026-03-30
- User correction: The accelerated `update-market-data` run crashed with `sqlite3.OperationalError: unable to open database file` after several thousand symbols.
- Recurring pattern: Assuming `with sqlite3.connect(...)` closes the connection can leave high-frequency code paths leaking file descriptors, especially when schema checks and point lookups open fresh connections in tight loops.
- Preventive rule: When touching SQLite performance paths, verify connection lifetime explicitly and remember that sqlite's context manager commits or rolls back but does not close; add a regression test that the repository helper closes the connection after the `with` block.
- Resulting action: Replaced the shared storage connection helper with a `sqlite3.Connection` subclass that closes on context exit, fixed the migration runner to close its connection explicitly, and added a regression test for closed connections.

- Date: 2026-03-30
- User correction: This repo should use `main` by default for commit and push operations, not a `codex/*` branch.
- Recurring pattern: Blindly applying the app's default branch workflow can conflict with a repo-specific policy the user has already stated in the thread, and that mistake can happen before commit time if work starts on a feature branch at all.
- Preventive rule: In this repo, do not create feature branches. Start and stay on `main` for all work unless the user explicitly asks for another branch, and treat that repo-specific rule as overriding the generic app default.
- Resulting action: Recorded the repo-specific branch policy here, updated `AGENTS.md`, and will use `main` for all future work in `pyvalue` unless instructed otherwise.

- Date: 2026-03-31
- User correction: `recalc-market-cap --all-supported` crashed with `sqlite3.OperationalError: database is locked` after I added a performance index in the facts schema path.
- Recurring pattern: Adding opportunistic DDL to a repository `initialize_schema()` method can turn a read-mostly runtime command into a write-locking code path and crash against a busy SQLite database.
- Preventive rule: Treat new indexes for hot paths as optional performance work unless the command truly depends on them; avoid explicit schema initialization in read-mostly commands, and make non-critical index creation tolerate a busy SQLite database.
- Resulting action: Removed the eager schema init from `recalc-market-cap`, made the new facts performance index best-effort when the DB is locked, and recorded the rule here.

- Date: 2026-03-31
- User correction: Process-based `compute-metrics` runs failed with `database is locked` for many symbols.
- Recurring pattern: Reusing normal repository read methods inside child processes can silently reintroduce migrations and `CREATE TABLE/INDEX` work on every worker, which collides with the parent writer on SQLite even when the worker is conceptually read-only.
- Preventive rule: Before parallelizing a SQLite read path with processes, warm the required schema once in the parent and route workers through schema-ready read repositories that never run migrations or DDL.
- Resulting action: Added parent-side schema warmup for `compute-metrics`, switched worker reads to schema-ready repositories, and added a regression that fails if worker-side schema initialization is needed.

- Date: 2026-03-31
- User correction: `compute-metrics` was still slow even after the symbol-level cache refactor.
- Recurring pattern: Treating a fast single-symbol microbenchmark as proof that the full command is fast can miss SQLite writer contention, especially when the parent still commits one small batch at a time in parallel mode.
- Preventive rule: After accelerating a CPU/read path on SQLite, benchmark the full end-to-end command with writes enabled and inspect journal mode plus transaction batching before claiming the runtime is fixed.
- Resulting action: Measured clean per-symbol compute separately from full-run behavior, identified parent-side write contention on a `DELETE`-journal DB, and added WAL enablement plus batched metric writes with serial fallback when WAL is unavailable.

- Date: 2026-03-31
- User correction: `compute-metrics` still crashed in serial fallback because one transient SQLite lock during a batched metric write aborted the whole run.
- Recurring pattern: Improving concurrency strategy without adding retry/backoff on the remaining write path leaves SQLite commands brittle whenever WAL setup is delayed or another short-lived connection briefly holds the lock.
- Preventive rule: For long-running SQLite batch commands, add retry/backoff to the final write path and verify transient `database is locked` errors do not abort the command.
- Resulting action: Added connection busy-timeout settings plus locked-error retry for WAL enablement and metric batch upserts, and added a regression for transient locked writes.

- Date: 2026-04-01
- User correction: `compute-metrics` warning suppression should be the default console behavior, not an opt-in flag.
- Recurring pattern: Turning a requested CLI noise reduction into an opt-in switch without confirming the desired default can invert the intended UX even when the underlying feature is correct.
- Preventive rule: When adding CLI output controls, explicitly decide whether the requested behavior is default-on or opt-in before naming flags, and prefer opt-out flags when the user asks for quieter default output.
- Resulting action: Switched `compute-metrics` to suppress metric warnings on the console by default, added `--show-metric-warnings` as the opt-out, and recorded the console-vs-log contract in tests and docs.

- Date: 2026-04-02
- User correction: `run-screen --output-csv data/output/...` failed because the parent folder was missing.
- Recurring pattern: Adding CLI file output paths without exercising a missing-parent-directory case leaves commands brittle and pushes path setup work onto the user.
- Preventive rule: For every CLI option that writes a file, create parent directories in the shared write helper and add a regression test for both non-empty and empty-result branches using a nested output path.
- Resulting action: Added a shared output-path preparation helper for CLI CSV writers, fixed `run-screen` nested output paths, and added regression coverage for passing and no-pass screen CSV writes.

- Date: 2026-04-02
- User correction: `data/output/` was a user-kept directory for saved screen results, not disposable scratch output.
- Recurring pattern: Treating untracked generated artifacts as safe to delete during cleanup can destroy user-owned local outputs when the repo intentionally keeps result files outside git.
- Preventive rule: Before deleting untracked files or directories in this repo, distinguish disposable test artifacts from user-kept local outputs; never remove a populated results directory such as `data/output/` just to clean the worktree unless the user explicitly asks.
- Resulting action: Recorded the rule here and will preserve local result directories even when they are untracked or generated.

- Date: 2026-04-03
- User correction: `CLAUDE.md` must stay as an exact copy of `AGENTS.md`, and updating `AGENTS.md` without syncing `CLAUDE.md` is a repo-specific mistake.
- Recurring pattern: Updating one instruction mirror file while assuming the other will stay aligned creates silent drift in agent guidance and leaves different assistants following different rules.
- Preventive rule: In this repo, whenever `AGENTS.md` changes, update `CLAUDE.md` in the same change and verify the two files are identical before finishing.
- Resulting action: Added the mirror rule to `AGENTS.md`, synced `CLAUDE.md` to match it exactly, and recorded the rule here.

- Date: 2026-04-03
- User correction: `refresh-security-metadata` was still effectively unusable because the earlier fix added progress output only after an expensive full-universe raw-payload preload.
- Recurring pattern: Improving visible loop progress without measuring the pre-loop setup can miss the true hot path and leave CLI startup latency unchanged.
- Preventive rule: For slow CLI commands, benchmark each major phase on the real data path before optimizing UX, and do not treat progress output as a performance fix unless the expensive pre-loop work has also been profiled or removed.
- Resulting action: Profiled `refresh-security-metadata` against the real SQLite DB, identified `FundamentalsRepository.fetch_many(...)` plus eager `json.loads(...)` as the startup bottleneck, and replaced that path with chunked extracted metadata reads and batched writes.
