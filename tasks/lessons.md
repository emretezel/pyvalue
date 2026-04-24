# Agent Lessons

Use this file to capture recurring mistake patterns after user corrections so future work is more reliable.

## Entry Template
- Date:
- User correction:
- Recurring pattern:
- Preventive rule:
- Resulting action:

## Lessons

- Date: 2026-04-24
- User correction: A broad `LIKE` scan over the 16.6 GiB `fundamentals_raw.data`
  table was running during a schema/currency refactor, and the user asked why
  it was running.
- Recurring pattern: Using exploratory SQL against wide raw JSON columns can
  create long-running I/O-heavy work when code and tests would answer the
  implementation question safely.
- Preventive rule: Before querying wide SQLite payload tables, check table size
  and access pattern; avoid unindexed raw JSON scans unless the user explicitly
  asks for live-data inspection or the scan is bounded and justified.
- Resulting action: Killed the scan, continued with code/test inspection, and
  recorded this rule before further verification.

- Date: 2026-04-21
- User correction: The catalog refactor must use the exact singular table names `provider`, `provider_exchange`, `listing`, and `provider_listing`, and provider-scoped raw/state tables must key by `provider_listing_id` rather than `(provider, provider_symbol)`.
- Recurring pattern: Preserving compatibility names too deeply can accidentally keep old physical identities alive after the user has asked for a real schema cutover.
- Preventive rule: For DB refactors, distinguish compatibility views/API aliases from physical tables; tests and docs must assert the new physical schema and only use old names where deliberately marked compatibility.
- Resulting action: Updated migrations, storage repositories, tests, docs, and this lesson to treat `listing_id` and `provider_listing_id` as the durable physical identities.

- Date: 2026-04-20
- User correction: The canonical exchange table must be named exactly `exchange`, and the new provider mapping layer should use enforced foreign keys back to `providers` and `exchange`.
- Recurring pattern: When refactoring schema structure, I can drift into inferred naming or prior repo conventions instead of locking onto the exact table names and constraint expectations the user already specified.
- Preventive rule: For schema refactors in `pyvalue`, treat user-provided table names and explicit FK requirements as fixed API unless I confirm a change first; do not pluralize, rename, or relax constraints by assumption.
- Resulting action: Renamed the canonical table plan to `exchange`, kept `exchange_provider` as the provider-owned mapping layer, added the two explicit foreign keys, and recorded the rule here.

- Date: 2026-04-13
- User correction: When investigating `opm_10y_min` failure causes here, only consider the `EODHD` provider rather than mixing provider-specific conclusions.
- Recurring pattern: Starting a broad cross-provider audit before locking the requested provider scope can waste time and introduce irrelevant findings.
- Preventive rule: For pyvalue data-quality and metric-failure investigations, confirm the provider scope first and keep code-path, raw-payload, and normalization conclusions provider-specific unless the user explicitly asks for cross-provider analysis.
- Resulting action: Narrowed this `opm_10y_min` investigation to `EODHD` only and recorded the scope rule here.

- Date: 2026-04-13
- User correction: When validating screen or metric failures with spot checks, use large-cap examples rather than the first alphabetical tickers.
- Recurring pattern: Defaulting to arbitrary or alphabetical samples can overweight illiquid microcaps and make a failure audit less representative of the names the user actually cares about.
- Preventive rule: For ticker spot checks, raw-payload audits, and exchange-level samples in this repo, default to the largest-market-cap names within the target failure bucket unless the user asks for another sampling method, and state the sampling basis explicitly.
- Resulting action: Recorded the sampling rule here and will use large-cap examples by default for future pyvalue failure investigations.

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

- Date: 2026-04-05
- User correction: Database and SQL work needs an explicit repo-level performance-first rule, not just ad hoc fixes when a slow query shows up.
- Recurring pattern: Treating database performance as an implementation detail instead of a default design constraint makes it too easy to accept weak schema, index, and query choices until they fail at scale.
- Preventive rule: In this repo, treat database and SQL performance as the default priority for schema and query design, and explicitly reason about access patterns, keys, indexes, scaling behavior, and trade-offs whenever touching persistence code.
- Resulting action: Added a dedicated `Database and SQL Design` section to both `AGENTS.md` and `CLAUDE.md` and kept the two files identical.

- Date: 2026-04-05
- User correction: "Follow AGENTS.md strictly" needed to be stated explicitly before this optimization task.
- Recurring pattern: After doing the technical investigation correctly, it is still easy to drift on repo-specific process rules unless they are re-applied consciously at the start of implementation.
- Preventive rule: For any non-trivial pyvalue task, restate the AGENTS-driven workflow before coding: keep a live plan, use measured evidence, prefer subagents for exploration, and do not treat prior analysis as permission to skip the repo process.
- Resulting action: Recorded this rule here and applied the implementation workflow with an explicit plan, measured bottlenecks, targeted tests, and end-to-end verification.

- Date: 2026-04-05
- User correction: `report-screen-failures` should suppress live warning spam and its progress indicator should reflect screened symbols, not a later internal recomputation phase.
- Recurring pattern: Reusing generic progress and logging behavior in a diagnostic CLI command can expose internal phases instead of the unit of work the user actually cares about.
- Preventive rule: For long-running CLI diagnostics, make progress track the primary user-visible unit of work and suppress incidental console warning noise unless the warnings are themselves the intended output.
- Resulting action: Scoped warning suppression to `report-screen-failures`, switched its progress display to a screening-only progress bar, and added regressions for both behaviors.

- Date: 2026-04-05
- User correction: A progress bar that starts only after the first unit completes is effectively invisible on commands with expensive setup or a slow first item.
- Recurring pattern: Time-throttled progress helpers are not enough on their own; if the first visible update depends on finishing work, the command still feels hung and users will interrupt it.
- Preventive rule: For long-running CLI commands where users expect live feedback, print an immediate `0/N` progress state before expensive pre-work and always force a final `N/N` update at the end of the primary phase.
- Resulting action: `report-screen-failures` now prints `0/N` immediately before metric preloading and always prints a final `100%` screening update, with tests covering both fast and throttled runs.

- Date: 2026-04-05
- User correction: A single progress bar that reaches `100%` before a long recomputation phase is misleading even if the first phase itself is implemented correctly.
- Recurring pattern: Treating one visible phase as the whole command can hide expensive downstream analysis and make a CLI look hung after reporting completion.
- Preventive rule: For multi-phase CLI work, either keep the visible progress bar below `100%` until all expensive phases are done or expose the later expensive phase with its own explicit progress indicator.
- Resulting action: `report-screen-failures` now shows a second progress phase for missing-symbol root-cause analysis after screening completes, so the command no longer appears finished while recomputation is still running.

- Date: 2026-04-06
- User correction: A commit I reported as done still left other tracked repo changes uncommitted in the worktree.
- Recurring pattern: Making a focused commit in a dirty tree without explicitly reporting the remaining tracked changes can leave the user thinking the repo is fully committed when only one slice was committed.
- Preventive rule: After every commit in a dirty worktree, run `git status --short`, call out any remaining tracked files explicitly, and never imply the repository is fully committed unless the worktree is actually clean.
- Resulting action: Recorded the rule here and will always report the remaining tracked files after a scoped commit in `pyvalue`.

- Date: 2026-04-08
- User correction: The FX cache plan must not use payload-level `fundamentals_raw.currency`; normalization should resolve whatever pair it needs from the preloaded FX cache because raw EODHD payloads can contain multiple currencies.
- Recurring pattern: Reusing a convenient schema hint as if it were authoritative for runtime planning can produce the wrong architecture when provider payloads are more heterogeneous than the coarse stored metadata.
- Preventive rule: For FX, currency, and normalization design in this repo, do not treat payload-level metadata columns as authoritative without checking the raw provider semantics first; if mixed currencies are possible, design lookup/cache behavior around actual conversion requests rather than precomputed payload hints.
- Resulting action: Reworked the FX plan and implementation to use full per-worker FX preload with on-demand in-memory pair resolution, removed the cache design dependency on `fundamentals_raw.currency`, and recorded the rule here.

- Date: 2026-04-08
- User correction: Missing-FX warning suppression worked in-process but still leaked to the terminal during spawned EODHD normalization workers.
- Recurring pattern: It is easy to validate logging and warning filters only in the parent process and forget that spawned worker processes start with different logging state and can fall back to `logging.lastResort`, bypassing both formatting and console-only filters.
- Preventive rule: Any CLI logging change that matters during multiprocessing must be verified in a real spawned worker path, not just inline or single-process tests; explicitly check worker handler initialization, console behavior, and file-log persistence together.
- Resulting action: Added worker logging initialization for pyvalue process pools only when pyvalue's rotating file logger is active, extended tests to cover spawned worker normalization output, and recorded the rule here.

- Date: 2026-04-10
- User correction: Trading currency for normalization and metric currency invariants must be strictly `market_data.currency`; do not infer it from exchange metadata, supported ticker currency, or payload-level currencies, and do not let `EnterpriseValue` inherit trading currency.
- Recurring pattern: When both listing/trading currency and payload/reporting currency exist in the same pipeline, it is easy to blur them together with convenient fallbacks and accidentally encode the wrong business meaning into normalization and metric validation.
- Preventive rule: In pyvalue, treat trading currency and payload currency as separate concepts. Trading currency resolves only from stored `market_data.currency`; payload-derived facts like `EnterpriseValue` must resolve currency only from provider payload fields and explicit payload traversal rules.
- Resulting action: Reworked ticker-currency resolution to use only `market_data.currency`, updated EODHD normalization to require it, kept `EnterpriseValue` payload-only, and aligned the metric invariants and tests with that split.

- Date: 2026-04-11
- User correction: Build the new skill from scratch instead of anchoring on an adjacent existing skill.
- Recurring pattern: When creating a net-new artifact, inspecting nearby existing artifacts too early can bias the design toward adaptation when the user wants a first-principles solution.
- Preventive rule: For new skills, designs, or workflows, start from the user’s requirements alone and only inspect existing neighboring artifacts if the user explicitly asks for comparison, reuse, or extension.
- Resulting action: Ignored the neighboring performance skill for the actual design, rewrote the new skill from first principles, and recorded the rule here.

- Date: 2026-04-17
- User correction: The installed `pyvalue-screen-failure-audit` skill should cover all failed exchanges with up to 50 symbols per exchange, run safely from `~/.codex/skills/...` without repo-relative path failures, and not leave a duplicate repo-side copy when the installed skill is the source of truth.
- Recurring pattern: Narrowing a failure audit to one inferred exchange or updating only one copy of a duplicated skill can silently break the intended scope and leave skill behavior drifting between locations.
- Preventive rule: For installed repo-specific skills, do not infer a single exchange from an example symbol when the requested audit should span all failed exchanges; derive the exchange list from live status rows or ask. When the installed skill is the source of truth, verify execution from the installed directory and remove duplicate copies in the repo in the same task.
- Resulting action: Updated the installed `pyvalue-screen-failure-audit` bundle to derive failed exchanges and sample per exchange from `metric_compute_status`, verified execution from `~/.codex/skills/pyvalue-screen-failure-audit`, and removed the duplicate repo-side skill directory.

- Date: 2026-04-23
- User correction: The `provider` registry should not carry a `status` column because no provider payload supplies it and the application does not use it.
- Recurring pattern: Adding lifecycle or config fields to narrow registry tables without a real source or runtime behavior creates schema noise and follow-up cleanup work.
- Preventive rule: For registry tables in `pyvalue`, do not persist status/config metadata unless it is sourced from real provider data or already drives application behavior; keep the registry minimal by default.
- Resulting action: Removed `provider.status` from migrations, storage schema, tests, and docs, and added a forward migration to drop the column from already-migrated databases.
