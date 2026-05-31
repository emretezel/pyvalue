# Refactor: `Money` type, `unit_kind`, and removing derived `market_cap`

Living status doc for the multi-phase refactor. Full design and rationale live in
the approved plan; this file tracks **what is done** and any deviations found
during implementation. Update the status table at the end of every phase.

## Goals
1. A `Money` value type (amount + currency travel together; cross-currency
   arithmetic raises) and **subunits never crossing the data boundary** —
   `market_data.price` and `financial_facts.value` are always in the *major*
   currency.
2. Rename `financial_facts.unit` → `unit_kind` (enum only; `currency` holds the
   ISO code) and drop it from the primary key.
3. Remove the derived `market_data.market_cap`; compute shares × price on demand.

## Locked decisions
- **Target currency for every metric = the listing currency.** Inputs are
  converted to it via the `fx_rates` table, **logging each conversion**; a
  missing rate skips the metric. This overrides the old "never FX-convert" rule
  (CLAUDE.md / AGENTS.md updated in Phase 5).
- `financial_facts` and `metrics` are **emptied and rebuilt** from
  `fundamentals_raw` via the CLI normalise/compute commands (run manually, once,
  after the refactor) ⇒ the schema migration is a fast empty-table swap, no
  100M-row remap/dedup.
- SEC path: kept compiling with minimal mechanical renames only.
- `Money.amount` is `float` (REAL-everywhere policy).

## Backup
A single pre-refactor backup covers the whole effort:
`data/backups/pyvalue-pre-refactor.db` (sqlite3 `.backup`, ~42.5 GB, gitignored).

## Status

| Phase | Scope | Status |
|------|-------|--------|
| 0 | Tracking doc | Landed |
| 1 | `Money` value type (additive) | In review |
| 2 | `market_data.price` → major currency + migration 069 | Not started |
| 3 | `unit` → `unit_kind` rebuild + migration 070 | Not started |
| 4 | Remove `market_data.market_cap` + migration 071 | Not started |
| 5 | Full `Money` adoption across metrics + docs/rule update | Not started |

## Notes & deviations
- **Test layout:** the repo uses a *flat* `tests/` tree (no `tests/unit|regression|integration/`),
  which diverges from CLAUDE.md's stated layout. New tests follow the actual
  flat convention (`tests/test_money.py`). Flag for the author whether to update
  CLAUDE.md or restructure tests.
- **Hypothesis** was installed in the env but undeclared; added to
  `pyproject.toml` `[project.optional-dependencies].dev` in Phase 1.
- **Python** is `>=3.12` per `pyproject.toml` (CLAUDE.md's ">=3.9" is stale).

### Phase 1 — `Money` value type
- Added `Money` (frozen dataclass) + `CurrencyMismatchError` to `src/pyvalue/money.py`,
  composing `currency.normalize_monetary_amount` (subunit collapse) and
  `FXService.convert_amount` (no duplicated logic). Currency-safe `+ - * / < <= > >=`,
  scalar mul/div, `Money/Money → float` ratio, `convert`/`convert_or_raise`.
- Tests: `tests/test_money.py` (example-based + Hypothesis property tests for
  subunit normalization, commutativity, and the cross-currency-raises invariant).
- Additive only — no consumers changed.
