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
| 5a | Typed fact read boundary (`MonetaryFact`/`ScalarFact` + `FactReader`) + metric sweep | In progress |
| 5b | FX-convert inputs to listing currency + docs/rule update | Not started |

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

### Phase 5 — full `Money` adoption + type-safe fact read boundary
Phase 5 is where `Money` stops being a metrics-only helper and becomes the **only**
way a monetary value reaches metric arithmetic. The design below was agreed with the
author before implementation; it supersedes the original plan's "wrap each input in
`Money` at the call site" sketch, which left the bare-float magnitude reachable.

> **5a progress — foundation landed in the working tree (pending review).**
> `src/pyvalue/facts.py` now defines `MonetaryFact` / `ScalarFact` (over a shared
> `_TypedFact`), the `to_monetary_fact` / `to_scalar_fact` mappers, the `FactReader`
> and `RawFactSource` protocols, and the four typed accessors on
> `RegionFactsRepository`. `tests/test_facts.py` (12 tests) covers minting,
> per-share-is-money, subunit collapse, wrong-kind raises, and the currency-less drop.
> Gate green: ruff, mypy (93 files), 845 pytest. **Next:** sweep the 36 metric files
> onto the typed accessors + `Money` arithmetic (task #8).

**Problem this closes.** `FactRecord` (`storage.py:319`) carries `value: float` next
to `currency: Optional[str]` and `unit_kind`, and the DAO hands that out unchanged.
Metrics read `record.value` directly as a float (~30 sites) with nothing coupling the
amount to its currency — `a.value + b.value` across two currencies type-checks fine
today. `Money` only buys safety if that float is *unreachable* for a monetary fact.

**Enforcement = a typed read layer in `facts.py` (the metric-facing boundary).**
The raw SQLite DAO (`FinancialFactsRepository`, `storage.py:4421`) is left
**unchanged** — it still returns `FactRecord` (bare `float` + `currency`). The
`facts.py` access layer that metrics actually receive is where `Money` is minted, so
`storage.py` stays a thin raw store and the cycle that would arise from a Money-bearing
`storage` type (`money.py` already imports `storage`) is avoided.
- **Kind-tagged read objects.** `MonetaryFact` carries a `Money` and has **no**
  `.value`; `ScalarFact` carries a bare `float`. The discriminant is the stored
  `unit_kind` (`monetary`/`per_share` → `MonetaryFact`; `count`/`ratio`/`percent`/
  `multiple`/`other` → `ScalarFact`). The monetary magnitude is unreachable as a float,
  so a metric cannot combine currencies without going through `Money` (which raises).
- **Intent-revealing typed accessors, not call-site `isinstance`.**
  `RegionFactsRepository` exposes `latest_monetary_fact` / `monetary_facts_for_concept`
  (→ `MonetaryFact`) and `latest_scalar_fact` / `scalar_facts_for_concept` (→
  `ScalarFact`). A metric picks the accessor for the kind it expects; the layer
  validates that against the stored `unit_kind` and **raises** on a real mismatch (a
  metric asking for money on a count concept). The accessors are defined over the raw
  `latest_fact` / `facts_for_concept`, so the batch-cache subclass
  (`_CachedRegionFactsRepository`, `cli.py:3239`) — which overrides only the raw readers
  — inherits them for free.
- **`FactReader` + `RawFactSource` protocols.** Metrics depend on `FactReader` (the four
  typed accessors), not a concrete repo, closing the duck-typed
  `hasattr(repo, "latest_fact")` hole (`metrics/utils.py:72`). `RegionFactsRepository`
  now wraps a `RawFactSource` (structural `latest_fact` / `facts_for_concept`), so
  in-memory fakes and the cache satisfy it without subclassing the SQLite DAO.
- **Write DTO vs read object.** `FactRecord` stays the storage/write row (bare `float`,
  built by the normalizer); the layer maps it to the kind-tagged read object, ending
  `FactRecord`'s double-duty without touching the write path.

**Where `Money` lives (and where it must not).**
- Used at three layers: **metric arithmetic** (the consumer), the **`facts.py` read
  layer** (where a stored float is minted into `Money` — the single conversion point),
  and **market data + FX** (a price is money; `Money.convert` already delegates to
  `FXService`).
- *Not* used in **storage rows or normalization internals**: SQLite stores `REAL value`
  + `currency` + `unit_kind` (a `Money` object cannot be stored), and normalization is
  the *producer* of the major amount `Money` requires (subunit→major collapse happens
  there). `Money` is a read-time, in-memory domain type and is never persisted.

**Metric rework (the locked currency rule, applied).** Each metric resolves
**target = listing currency** (`require_metric_ticker_currency`, `metrics/utils.py:162`),
then converts every `Money` input via `Money.convert(target, fx, as_of)` — **logging each
conversion** — before any arithmetic; a missing FX rate skips the metric with a
structured reason (`MetricCurrencyInvariantError`; add a `missing_fx_rate` reason code
alongside the existing `missing_input_currency` / `missing_trading_currency` /
`currency_mismatch`). This replaces the assert-based `normalize_metric_amount`
(`:273`) / `ensure_metric_currency` (`:221`) flow. Cross-currency mixing then becomes
impossible *by construction* (`Money` raises) rather than by convention.

**Share-count denominators.** Share *counts* are `ScalarFact` (`unit_kind = count`, no
currency); `per_share` values (EPS, DPS) are `MonetaryFact` (they carry a currency).
This fixes the Phase 3 known item — metrics that still currency-validate a share-count
denominator (e.g. `fcf_per_share_cagr_10y`) — by making the count a scalar the type
system will not let you treat as money.

**Docs/rule.** Update CLAUDE.md + AGENTS.md (byte-identical): metrics convert all
monetary inputs to the listing currency via `fx_rates`, logging each conversion; a
missing rate skips the metric; subunits never enter the data boundary.

**Suggested sub-phasing (each its own commit + break):**
- **5a — type model:** introduce `MonetaryFact` / `ScalarFact` + the `FactReader`
  protocol; the `facts.py` layer mints `Money` at the boundary; metrics use the typed
  accessors and keep their current single-currency arithmetic (no FX yet). Mechanical
  and mypy-driven.
- **5b — FX conversion:** each metric converts inputs to the listing currency via
  `Money.convert`, logging; missing-rate skip; remove the assert-based flow; update
  CLAUDE.md / AGENTS.md; add the reproducibility CSV test.

**Resolved decisions (author):**
1. **Two frozen subclasses.** The DAO returns `MonetaryFact | ScalarFact` —
   `MonetaryFact` carries a `Money`, `ScalarFact` carries a `float`. (Not a single record
   with a `Money | float` payload: separate classes give each record exactly the right
   fields and read cleaner when narrowing at the call site.)
2. **`per_share` is money.** EPS and dividends-per-share are `MonetaryFact` — a per-share
   amount is still money-with-a-currency — so the union stays **binary**. `MonetaryFact`
   keeps the source `unit_kind` (`monetary` / `per_share`) as a provenance/formatting
   field; the type system enforces *currency* safety but deliberately does **not** block
   mixing a per-share rate with a total (that dimensional check is out of scope here).

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
