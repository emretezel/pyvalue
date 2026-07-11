# EODHD Provider Guide

## What EODHD Covers

EODHD is the only provider supported by `pyvalue`.

CLI commands that accept `--provider` already default to `EODHD`, so the flag
is optional.

It covers:
- global exchange universes
- global fundamentals
- all market data used by the project

For exactly how EODHD fundamentals payloads are mapped into `financial_facts` concepts, see
[EODHD Concept Normalization](../reference/eodhd-concept-normalization.md).

## Subscription Requirements

You need an active EODHD subscription for:
- fundamentals endpoints
- market data endpoints

## Universe Loading

`pyvalue` stores the EODHD supported-exchange catalog in SQLite and uses it for
exchange metadata lookups. Refresh it explicitly when you want the latest
exchange list from EODHD:

```bash
pyvalue refresh-supported-exchanges
```

`pyvalue` also stores a per-exchange `provider_listing` catalog for EODHD.
Refresh one exchange:

```bash
pyvalue refresh-supported-tickers --exchange-codes LSE
```

Refresh all stored exchanges:

```bash
pyvalue refresh-supported-tickers --all-supported
```

Ticker refresh keeps only `Common Stock`, `Preferred Stock`, and `Stock`.
ETF, fund, and other security types are excluded from the operational catalog.
When a ticker disappears from EODHD, only its provider layer goes: the
`provider_listing` mapping plus the provider-scoped raw fundamentals and
fetch/normalization state tied to it. Canonical rows (`listing`, `issuer`) and
canonical data (`financial_facts`, `market_data`, `metrics`, compute/refresh
state) are provider-independent and are never deleted by a refresh -- a payload
absence cannot distinguish a real delisting from a plan change, a provider
glitch, or a truncated response (2026-07-11 incident: a truncated 200-response
for a plan-dropped exchange nearly emptied it). A listing left with no provider
mapping is reported as orphaned and becomes unreachable through every
provider-joined scope until a provider maps it again.

Two guards protect the refresh against plan drift and bad payloads: an
exchange the plan no longer covers answers `exchange-symbol-list` with HTTP
404 and is warned about and skipped (stored data untouched), and a payload
that would remove >= 20 mappings *and* more than half of an exchange's
existing mappings is rolled back and skipped unless the operator passes
`--allow-mass-delisting`.

Example:

```bash
pyvalue refresh-supported-tickers --exchange-codes LSE
```

## Fundamentals Ingestion

Single symbol:

```bash
pyvalue ingest-fundamentals --symbols AAPL.US
```

Exchange-scoped:

```bash
pyvalue ingest-fundamentals --exchange-codes US
```

Quota-aware all-supported run across the stored supported-ticker catalog:

```bash
pyvalue ingest-fundamentals --all-supported
```

EODHD ingestion always reads from stored `provider_listing`, not from a live
symbol-list request. Refresh the ticker catalog before running it:

```bash
pyvalue refresh-supported-tickers --exchange-codes US
pyvalue ingest-fundamentals --exchange-codes US
```

Supported-ticker refresh stores catalog currency on `listing.currency` as the
listing quote unit. Tickers whose payload currency is absent or malformed
(anything other than three uppercase ASCII letters, e.g. the `'Unknown'`
placeholder) are skipped and reported in the refresh warning output.
Single-symbol fundamentals ingestion uses existing catalog
currency when one is already present and otherwise leaves listing currency
unset; it does not copy `General.CurrencyCode` from the raw payload into catalog
metadata.

For large multi-day runs:

```bash
pyvalue refresh-supported-exchanges
pyvalue refresh-supported-tickers --all-supported
pyvalue ingest-fundamentals --all-supported
```

If you upgrade an existing database and need to backfill the canonical
primary-vs-secondary listing classification without downloading anything
again, run:

```bash
pyvalue reconcile-listing-status --all-supported
```

Every other command (normalize, market-data, metrics, screening,
metadata-refresh, reports) only *reads* the cached classification -- it never
reconciles as a side effect. `ingest-fundamentals` keeps the cache
current (it reclassifies in the same transaction that stores each raw payload),
and migration 078 is the one-time backstop that resolves any leftover `unknown`
listing with stored fundamentals. Run `reconcile-listing-status` for an explicit
full re-derivation from stored raw fundamentals.

`ingest-fundamentals` checks the EODHD user/quota endpoint
before each multi-symbol run, subtracts the configured daily buffer, throttles
by requests per minute, and exits cleanly when the remaining daily allowance is
exhausted. Multi-symbol EODHD runs now use concurrent fetch workers with a
single batched SQLite writer, so exchange and all-supported runs can get much
closer to the configured request ceiling without relying on the Extended
Fundamentals bulk API. Rerun it the next day to continue from the remaining
eligible ticker set.

To see whether a multi-day run is actually complete for the current scope, use:

```bash
pyvalue report-fundamentals-progress
```

This report defaults to a 30-day freshness window. That means old
`fundamentals_raw` rows count as incomplete by default, and
`ingest-fundamentals --all-supported` now uses the same
30-day freshness window when `--max-age-days` is omitted. Use `--missing-only`
on the report if you only care whether each supported ticker has ever been
ingested once.
In the summary, `Stored` means a raw payload exists in the DB, while `Fresh`
means the ticker currently counts as complete for the selected mode/window.

Successful EODHD refreshes replace the stored raw payload for the same
provider-symbol in `fundamentals_raw`. Older historical periods remain
available through the newly stored payload and normalized downstream tables are
refreshed only when you run normalization again.

`pyvalue` also inspects `General.PrimaryTicker` on each stored EODHD payload
and stores whether that canonical listing is primary or secondary on
`listing.primary_listing_status`. Missing, blank, or
otherwise unusable `PrimaryTicker` values are treated as primary. Once a
listing is classified as secondary, downstream normalization, market-data,
metric, screening, metadata-refresh, and FX-discovery scopes exclude it.
Classification writes only the status column: a secondary listing keeps its
raw payload and everything it accumulated while primary (normalized facts,
market data, metrics, refresh state). Exclusion is purely scope-side, so a
listing that later flips back to primary re-enters those scopes with its
history intact.

Important fundamentals options:

- `--symbols`, `--exchange-codes`, or `--all-supported`: choose the scope
- `--rate`: EODHD uses symbols per minute; default `950`, capped at `1000`
- `--max-symbols`: limit one run
- `--max-age-days`: refresh stale or missing data; default `30`
- retry backoff is respected by default; use `--retry-failed-now` to bypass it

## FX Refresh

EODHD is also the default FX provider.

Refresh FX coverage explicitly with:

```bash
pyvalue refresh-fx-rates
```

Behavior:

- syncs the EODHD FOREX catalog into `fx_supported_pairs`
- refreshes canonical six-letter pairs such as `EURUSD`
- treats three-letter shorthands such as `EUR` as aliases for `USDEUR`
  and does not refresh those aliases separately
- stores direct provider rows only in `fx_rates`
- tracks pair coverage and retry state in `fx_refresh_state`
- backfills full available history on the first unbounded run, then refreshes
  only the missing older/newer outer ranges later

If you need to limit the first backfill window:

```bash
pyvalue refresh-fx-rates --start-date 2000-01-01
```

A later unbounded run can still fill the older missing history.

## Fundamentals Normalization

Single symbol:

```bash
pyvalue normalize-fundamentals --symbols AAPL.US
```

Exchange-scoped:

```bash
pyvalue normalize-fundamentals --exchange-codes US
```

All-supported:

```bash
pyvalue normalize-fundamentals --all-supported
```

Force re-normalization:

```bash
pyvalue normalize-fundamentals --all-supported --force
```

Normalization converts raw EODHD payloads into provider-agnostic
`financial_facts` records keyed by canonical `listing_id`.
Exchange and all-supported normalization runs parallelize automatically.
By default, normalization skips symbols whose raw payload has not changed since
the last successful EODHD normalization.
Listings already classified as secondary from `General.PrimaryTicker` are
excluded from normalization scopes.
EODHD normalization requires `listing.currency`. Raw payload currencies are
source currencies only.
Monetary fact currency lookup checks entry-level `currency`,
`currency_symbol`, or `CurrencyCode`, then direct statement-level currency, then
payload-level `General.CurrencyCode`; facts are converted to
base(`listing.currency`) when the source currency differs.
Normalization never fetches FX from the network. When a symbol needs currency
conversion, each worker process preloads the full selected-provider FX table
once and resolves direct, inverse, and USD/EUR triangulated rates from memory.
Run `refresh-fx-rates` before normalization when the database does not already
contain the required history.

## Market Data

Market data is always fetched from EODHD.

Single symbol:

```bash
pyvalue update-market-data --symbols AAPL.US
```

Exchange-scoped:

```bash
pyvalue update-market-data --exchange-codes US
```

Quota-aware all-supported run across the stored supported-ticker catalog:

```bash
pyvalue update-market-data --all-supported
```

For large multi-day runs:

```bash
pyvalue refresh-supported-exchanges
pyvalue refresh-supported-tickers --all-supported
pyvalue update-market-data --all-supported
```

`update-market-data` checks the EODHD user/quota endpoint
before each multi-symbol run, subtracts the configured daily buffer, throttles
by requests per minute, and exits cleanly when the remaining daily allowance is
exhausted. Market-data refreshes use hybrid accounting: per-symbol requests
cost one EODHD API call, while exchange-bulk refreshes cost `100` API calls for
the exchange. Large exchange and all-supported runs can therefore move through
the supported universe much faster than a pure per-symbol loop while staying
quota-aware.

To see whether a multi-day market-data run is actually complete for the current
scope, use:

```bash
pyvalue report-market-data-progress
```

This report defaults to a 30-day freshness window. A symbol is incomplete when
its latest stored `market_data.as_of` is missing or older than the selected
window. In the summary, `Stored` means a market-data snapshot exists in the DB,
while `Fresh` means the symbol currently counts as complete for the selected
window.

Important market-data options:

- `--symbols`, `--exchange-codes`, or `--all-supported`: choose the scope
- `--rate`: requests per minute, capped at the EODHD limit of `1000`
- `--max-symbols`: limit one run
- `--max-age-days`: refresh stale or missing market data; default `30`
- listings already classified as secondary from raw fundamentals are excluded
  before market-data refresh planning and progress accounting
- retry backoff is respected by default; use `--retry-failed-now` to bypass it

Market cap is not stored; it is computed on demand as the latest share-count fact
x the latest `market_data` price (`metrics.utils.market_cap_money`).

## EODHD-Oriented Metrics

Many newer metrics in the project are intentionally EODHD-oriented because they rely on normalized concepts or fallback logic designed around EODHD payload structure. The metrics catalog marks this in the calculation column where relevant.

## Caveats

- Exchange suffixes matter: use `AAPL.US`, `SHEL.LSE`, etc.
- Some fields are normalized through EODHD-specific fallback chains; compute metrics only after normalization.
- Market data freshness is independent from fundamentals freshness.

## Related Docs

- [Configuration](../configuration.md)
- [Market Data Guide](../guides/market-data.md)
- [Ingestion and Normalization Guide](../guides/ingestion-and-normalization.md)
