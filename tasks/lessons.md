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
- Recurring pattern: Blindly applying the app's default branch workflow can conflict with a repo-specific policy the user has already stated in the thread.
- Preventive rule: Before any commit or push in this repo, assume `main` is the default target unless the user explicitly asks for a separate branch, and treat that repo-specific rule as overriding the generic app default.
- Resulting action: Recorded the repo-specific branch policy here and will use `main` for future commit/push requests in `pyvalue` unless instructed otherwise.
