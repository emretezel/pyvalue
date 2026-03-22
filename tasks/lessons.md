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
- User correction: For this EODHD account, market data and fundamentals are on separate paid plans, so their daily limits should be treated separately.
- Recurring pattern: Assuming one provider-level quota model in implementation can be wrong when the account uses multiple paid plans/products with separate allowances.
- Preventive rule: Before enforcing quota gates in provider-specific workflows, confirm whether the account can have multiple plan-scoped limits and avoid collapsing them into one budget unless the integration exposes a single authoritative counter for the exact workflow.
- Resulting action: Recorded the account-specific constraint here so future quota changes distinguish plan-scoped budgets from provider-wide defaults.
