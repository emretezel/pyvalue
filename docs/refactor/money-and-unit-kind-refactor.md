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
| 4 | Remove `market_data.market_cap` + migration 072 | Landed (`eec19bc`) |
| 5a | Typed fact read boundary (`MonetaryFact`/`ScalarFact` + `FactReader`) + metric sweep | Landed (`db4cacb`) |
| 5b | FX-convert inputs to listing currency + docs/rule update | Landed (`359af4f`) |

\* Phase 2 landed with the price migration numbered **069**; Phase 2.6 renumbers
it to **070** so the currency-less-listing purge (**069**) runs *first* — price
scaling must run after the purge. Migrations have not been applied to the
production DB, so renumbering an as-yet-unapplied migration is safe.

**All code phases (0–5b) are landed on `main`.** The only remaining work is the
author's one-time manual production rebuild (after the pre-refactor backup):
1. apply migrations **069–072** to `data/pyvalue.db`;
2. populate `fx_rates` (the FX refresh command) for the pairs/dates cross-currency
   listings need — otherwise those metrics skip with `missing_fx_rate`;
3. **normalise** — rebuild `financial_facts` from `fundamentals_raw` (now `unit_kind`
   + major currency, weighted-average shares as `count`);
4. clear `metrics` / `metric_compute_status`, then **compute** — rebuild metrics with
   `Money` + FX + on-demand market cap.

Until then the schema and code carry the new design while the 42.5 GB DB still holds
pre-refactor rows.

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

> **5a — full sweep landed (`db4cacb`).**
> `src/pyvalue/facts.py` defines `MonetaryFact` / `ScalarFact` (over a shared
> `_TypedFact`), the `to_monetary_fact` / `to_scalar_fact` mappers, the `FactReader`
> / `RawFactSource` protocols, the `TypedFactReaderMixin` (four typed accessors) and
> `RegionFactsRepository`. **All 36 metric files** now resolve `target = listing
> currency`, read through the typed accessors, align every input through the shared
> `require_metric_money` / `require_metric_amount_money` seam, and do `Money`
> arithmetic — no metric reads a bare monetary `float`. The dead assert-based helpers
> (`normalize_metric_amount`, `normalize_metric_record`, `ensure_metric_currency`,
> `align_metric_money_values`) have been removed from `metrics/utils.py`. Gate green:
> ruff, mypy (93 files), 846 pytest.
>
> **Data-modelling fix applied (weighted-average shares → `count`).** The EODHD
> normalizer previously tagged only `CommonStockSharesOutstanding` /
> `EntityCommonStockSharesOutstanding` as `count`; the weighted-average share
> concepts (`WeightedAverageNumberOfDilutedSharesOutstanding`,
> `WeightedAverageNumberOfSharesOutstandingBasic`) fell through the else-branch and
> were stored as **`monetary` with a currency** — a share count mislabeled as money.
> They are now in `SHARE_FACT_CONCEPTS` (`normalization/eodhd.py`), so they normalize
> to `unit_kind='count'` with NULL currency. `fcf_per_share_cagr_10y` reads diluted
> shares through the **scalar** accessor and computes per-share as
> `FCF (Money) / share-count (float)` → a per-share `Money`; the CAGR is then a
> same-currency `Money / Money` ratio. Regression test:
> `test_eodhd_normalizes_weighted_average_shares_as_count`. (Takes effect on the
> next `normalise`, which rebuilds `financial_facts` from raw — no migration; the
> schema already permits `count` + NULL currency.)

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
**target = listing currency** (`require_metric_ticker_currency`) and routes every
monetary input through one seam — `require_metric_money(fact.money, target_currency=…)`
(or `require_metric_amount_money` for a raw market price) — before any `Money`
arithmetic. The seam **converts** a non-target-currency input to the listing currency
via `Money.convert` (logging each conversion); if no rate is available it raises a
structured `MetricCurrencyInvariantError` (`missing_fx_rate`), which
`wrap_metric_currency_invariants` turns into an unavailable metric rather than letting
`Money` raise `CurrencyMismatchError` mid-batch. (In 5a this seam instead *rejected* a
mismatch with `currency_mismatch`; 5b swapped only the body, leaving call sites
untouched.) Either way cross-currency mixing is impossible *by construction*; the
assert-based `normalize_metric_amount` / `ensure_metric_currency` flow is gone.

**Share-count denominators.** All share *counts* the normalizer tags `count`
(`CommonStockSharesOutstanding` / `EntityCommonStockSharesOutstanding` *and* the
weighted-average concepts — see the data-modelling fix above) are `ScalarFact` (no
currency) and read through the scalar accessors (share-count-change, buyback,
on-demand market cap, and `fcf_per_share_cagr_10y`'s diluted-share denominator);
`per_share` values (EPS, DPS) are `MonetaryFact` (they carry a currency). So a
per-share metric divides money by a share quantity (`Money / float` → a per-share
`Money`) — the type system will not let a share count be treated as money.

**Docs/rule (done in 5b).** CLAUDE.md + AGENTS.md (byte-identical) now state: metrics
convert every monetary input to the listing currency via `fx_rates` through the single
`require_metric_money` seam, logging each conversion; a missing rate makes the metric
unavailable (`missing_fx_rate`); subunits never enter the data boundary.

**Sub-phasing (each its own commit + break):**
- **5a — type model (landed):** `MonetaryFact` / `ScalarFact` + the `FactReader`
  protocol; the `facts.py` layer mints `Money` at the boundary; all 36 metrics use the
  typed accessors and `Money` arithmetic, aligning every input through the
  `require_metric_money` seam (reject-on-mismatch).
- **5b — FX conversion (landed):** the seam body now *converts* the input to the
  listing currency via `Money.convert` instead of rejecting it, logging each conversion
  and raising `missing_fx_rate` when no rate exists -- **call sites unchanged**, exactly
  as the 5a seam docstring promised. The FX service is bound once per compute batch by
  the driver via `metric_fx_service_context` (a `ContextVar`), which the seam reads;
  when unbound (unit tests with no FX DB) it falls back to the no-fetch ephemeral
  service, so a cross-currency input with no rate degrades to an unavailable metric --
  the same observable outcome 5a produced, so the existing mismatch tests still pass.
  New tests: `test_current_ratio_converts_cross_currency_input_via_fx` (EUR->USD via a
  seeded rate), `test_current_ratio_skips_when_fx_rate_missing`, and
  `test_metric_fx_conversion_is_byte_reproducible` (fixed inputs + rate -> byte-identical
  CSV across runs). CLAUDE.md / AGENTS.md rule updated.

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
- **Known Phase 5 item — resolved in 5a:** `fcf_per_share_cagr_10y` no longer
  currency-validates its share-count denominator. The weighted-average share
  concepts are now tagged `count` (see Phase 5's data-modelling fix), so the metric
  reads them through the scalar accessor and divides `Money` by a share quantity.

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
