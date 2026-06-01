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
| 1 | `Money` value type (additive) | Landed (`9f6b98c`) |
| 2 | `market_data.price` → major currency + migration 070 | Landed (`5cdfeeb`)\* |
| 2.6 | Purge currency-less listings + `listing.currency` NOT NULL + migration 069 | Landed (`df117d4`) |
| 3 | `unit` → `unit_kind` rebuild + migration 071 | Landed (`a1bf04d`) |
| 4 | Remove `market_data.market_cap` + migration 072 | In review |
| 5 | Full `Money` adoption across metrics + docs/rule update | Not started |

\* Phase 2 landed with the price migration numbered **069**; Phase 2.6 renumbers
it to **070** so the currency-less-listing purge (**069**) runs *first* — price
scaling must run after the purge. Migrations have not been applied to the
production DB, so renumbering an as-yet-unapplied migration is safe.

## Notes & deviations
- **Test layout:** the repo uses a *flat* `tests/` tree (no `tests/unit|regression|integration/`),
  which diverges from CLAUDE.md's stated layout. New tests follow the actual
  flat convention (`tests/test_money.py`). Flag for the author whether to update
  CLAUDE.md or restructure tests.
- **Hypothesis** was installed in the env but undeclared; added to
  `pyproject.toml` `[project.optional-dependencies].dev` in Phase 1.
- **Python** is `>=3.12` per `pyproject.toml` (CLAUDE.md's ">=3.9" is stale).

### Phase 3 — `financial_facts.unit` → `unit_kind`
The overloaded `unit` column (currency code *or* type token) is replaced by the
`unit_kind` enum; the ISO code lives in `currency` alone. This unifies the fact
vocabulary with the existing `metrics.unit_kind` enum (`MetricUnitKind`).
- Migration **071** (`_migration_071_financial_facts_unit_kind`): rebuilds
  `financial_facts` **empty** (data is regenerated from `fundamentals_raw` by the
  `normalise` CLI, per author decision), renames `unit`→`unit_kind` with an enum
  CHECK, drops `unit` from the PK (new PK `(listing_id, concept, fiscal_period,
  end_date)`), adds a **major-only** currency CHECK and a **coupled**
  `unit_kind ⇄ currency` CHECK, and clears `fundamentals_normalization_state` so
  every cached payload re-normalizes. `financial_facts` is a leaf table (nothing
  references it, no view selects from it), so the drop/recreate is self-contained.
  Promoted `_MAJOR_CURRENCY_CHECK` next to `_CURRENCY_FORMAT_CHECK`.
- `apply_migrations` gained a keyword-only `target_version` (default = head) so a
  single migration can be exercised in isolation — needed because migration 071 is
  destructive to legacy `financial_facts` rows that earlier migration regression
  tests assert on. Production callers omit it.
- `FactRecord.unit: str` → `unit_kind: MetricUnitKind`; the legacy `__post_init__`
  that derived `currency` from `unit` is removed (currency is now authoritative).
  All storage read/write SQL renamed `unit`→`unit_kind`; the share-count picker's
  `CASE` keys on `unit_kind = 'count'`.
- EODHD normalizer emits `unit_kind` directly: `count` for share concepts (currency
  `NULL`), `per_share` for EPS / dividends-per-share, `monetary` otherwise; monetary
  and per_share facts that cannot resolve a currency are now skipped (a latent
  currency-less-monetary row could previously be emitted). SEC normalizer classifies
  its us-gaap unit token into the enum via a local helper and reconstructs the token
  for its internal FY→Q4 / dedup keys.
- `money.normalize_fact_value` gates on `is_monetary_unit_kind(record.unit_kind)`
  and drops the `unit`-string currency fallback. The now-dead
  `currency.fact_currency_or_none` / `legacy_currency_from_unit` / `SHARES_UNIT`
  were removed.
- Test fallout: every fact fixture moved from `unit=` to `unit_kind=` + explicit
  `currency`; the FX-discovery test now stores one major-currency fact per listing
  (the new PK collapses three subunit facts under one listing into one row); five
  legacy-data migration tests were pinned to their own version via `target_version`
  so migration 071 no longer wipes their subjects; added migration-071 schema/CHECK
  and empty-rebuild regression tests. Quality gate green (ruff, mypy, 844 tests).
- **Known Phase 5 item:** several metrics still currency-validate share-count
  denominators (e.g. `fcf_per_share_cagr_10y`). That was masked before by the
  `unit`→currency derivation; the proper share-as-count handling lands with the
  Money rework in Phase 5.

### Phase 4 — remove derived `market_data.market_cap`; compute on demand
`market_cap` is shares-outstanding x price — a value derivable from other stored
facts — so persisting it duplicated state that could go stale relative to its
inputs. It is removed and computed on demand.

**Author decision (this phase reframed the original plan):** market cap pairs the
latest **share-count fact** with the `market_data` price *as of that fact's date*,
not the latest price. Co-dating the share count with its contemporaneous price
means a price and a share count are never multiplied across mismatched dates —
which in turn **obviates the cross-snapshot suspicious-jump guard**, so that guard
is removed entirely. Extending `update-market-data` to backfill a price at each
share-count date (plus the most recent day) is a **separate, later change**; this
phase assumes those co-dated prices exist and resolves market cap to `None` when
they do not.

- Migration **072** (`_migration_072_drop_market_data_market_cap`): rebuilds
  `market_data` without `market_cap`, **copying** the existing rows (price,
  volume, source_provider, updated_at) — unlike the financial_facts rebuild,
  `market_data` is not regenerated from raw. Leaf table (nothing references it,
  no view selects from it), so the drop/recreate is self-contained. Idempotent.
- `MarketDataRepository.price_as_of(symbol, on_or_before)` (new): the most recent
  snapshot with `as_of <= on_or_before`, reported in the listing's base currency.
  `update_market_cap` / `update_market_caps_many` and the `recalc-market-cap` CLI
  command are removed.
- `metrics.utils.market_cap_money(...) -> Optional[MarketCap]` (new): latest
  shares-outstanding fact (`EntityCommonStockSharesOutstanding`, then
  `CommonStockSharesOutstanding`) x `price_as_of(fact.end_date)`, as `Money`, plus
  the price date. Preserves the listing-currency invariant: a price currency that
  differs from the target raises `MetricCurrencyInvariantError` (Phase 5 will
  FX-convert instead). The 6 consumers (`market_capitalization`,
  `enterprise_value` EV fallback, `buyback_yield`, `owner_earnings_yield`,
  `price_to_fcf`, `profitability` dividend yield) cut over to it.
- **Concept declaration:** because the batch fact preload is restricted to each
  metric's `required_concepts` (and the cached reader does not fall back to the
  live DB on a miss), every market-cap-consuming metric now declares the
  share-count concepts in `required_concepts` (centralized via
  `EV_FALLBACK_REQUIRED_CONCEPTS` for the EV metrics). `MarketCapitalizationMetric`
  flips to `uses_financial_facts = True`.
- `PriceData` / `MarketDataUpdate` / `MarketSnapshotRecord` drop `market_cap`; the
  ingest path (`prepare_price_data`) just collapses the quote to its major
  currency (no market-cap derivation, no validation); the unused
  `MarketDataService` share/fundamentals helpers were removed. The
  metric-failure report's `market_cap` example column is now a cheap estimate
  (bulk latest-shares x latest-price), a diagnostic-only sizing heuristic.
- **Known follow-ups:** (1) `update-market-data` must be extended to store
  share-count-dated prices, else market cap resolves to `None` for most symbols
  after the rebuild; (2) in the batch path `market_cap_money` issues a per-symbol
  `price_as_of` query rather than reusing the preloaded latest snapshot (the
  snapshot is the latest day, not the as-of-share-date price) — a perf cost to
  revisit with the `update-market-data` change.
- **Behaviour change:** a >50x price move between refreshes is now stored without
  error (the guard is gone). Tests: removed the guard + market-cap-derivation +
  `recalc-market-cap` tests; added `price_as_of`, migration-072, share-fact batch,
  and a `market_cap_money` co-dating regression test. Quality gate green (ruff,
  mypy, 832 tests).

### Phase 2.6 — purge currency-less listings + `listing.currency` NOT NULL
Author decision: a listing's currency comes **only** from the
`refresh-supported-tickers` payload — no fallback/derivation. Currency-less
listings are deleted (not backfilled), and the column is made NOT NULL.
- Migration **069** (`_migration_069_purge_currencyless_listings`): deletes every
  listing with `currency IS NULL` plus all dependent rows (provider_listing and
  its fundamentals_raw/normalization_state/fetch_state + market_data_fetch_state
  children; financial_facts, financial_facts_refresh_state, market_data,
  metric_compute_status, metrics), then rebuilds `listing` with
  `currency TEXT NOT NULL`. Does NOT resurrect `idx_listing_exchange` (migration
  067 dropped it). Live impact: ~1,377 listings + ~989k facts + ~1.5k
  market_data + ~4k metrics rows (all currency-blind, non-rebuildable).
- Catalog gate (`SupportedTickerRepository._ensure_provider_listing`): returns
  None / creates nothing when the payload has no currency.
- `SecurityRepository.ensure(...)` / `ensure_from_symbol(...)` take a keyword
  `currency=` and **raise** if asked to create a listing without one (no
  fallback). Catalog paths thread the payload currency through; the
  fundamentals-raw store path skips a payload it cannot model.
- Test fallout: ~115 tests across 8 files minted listings without a currency;
  all updated to seed a currency-bearing listing (via the catalog) before
  creating facts/prices/metrics. Added a focused migration-069 purge regression
  test. No production code was weakened and no currency fallback was added.

### Phase 2 — `market_data.price` in major currency
- Ingest (`marketdata/service.py`): `prepare_price_data` now collapses the
  quoted price to its major currency via `normalize_monetary_amount` and stores
  that; removed the inverse helper `_quote_unit_price`.
- Read path (`storage.py`): `latest_snapshot_record` / `latest_snapshots_many`
  report `canonical_trading_currency(listing.currency)` so the (price, currency)
  pair is self-consistent and downstream normalization never divides twice.
- Migration **070** divides existing `market_data.price` by 100 for listings
  whose `listing.currency` is a subunit (GBX/GBP0.01/ZAC/ILA). Data-only;
  version-gated to run once; must deploy with the code.
- Metric values are unchanged end-to-end (previously: pence price ÷100 on read;
  now: major price, no division). Updated tests: market-data service/hint tests,
  the migration-039 chain test (069 now also runs), the recalc-market-cap CLI
  test; added a migration-069 regression test. Docs: market-data guide +
  data-model architecture doc.
- **Deploy note:** between deploying Phase-2 code and running migration 069,
  refreshing a subunit listing could trip the >50x suspicious-jump guard
  (new major vs old pence). Run 069 immediately after deploy.

### Phase 1 — `Money` value type
- Added `Money` (frozen dataclass) + `CurrencyMismatchError` to `src/pyvalue/money.py`,
  composing `currency.normalize_monetary_amount` (subunit collapse) and
  `FXService.convert_amount` (no duplicated logic). Currency-safe `+ - * / < <= > >=`,
  scalar mul/div, `Money/Money → float` ratio, `convert`/`convert_or_raise`.
- Tests: `tests/test_money.py` (example-based + Hypothesis property tests for
  subunit normalization, commutativity, and the cross-currency-raises invariant).
- Additive only — no consumers changed.
