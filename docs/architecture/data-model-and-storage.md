# Data Model and Storage

## Storage Model

`pyvalue` stores operational data in SQLite.

The main persisted layers are:
- supported exchange catalogs
- canonical security identities
- supported ticker catalogs
- raw fundamentals
- fundamentals fetch state
- normalized financial facts
- market data snapshots
- FX rates
- FX provider catalogs
- FX refresh state
- market data fetch state
- computed metrics

## Core Tables

### `fundamentals_raw`

Raw provider payloads are stored here.

Purpose:
- preserve source payloads as received
- support re-normalization when normalization logic changes
- separate provider-fetch concerns from metric computation
- keep provider-local fetch keys such as `provider_symbol` and `provider_exchange_code`
- map each raw payload to canonical `security_id`

### `supported_exchanges`

Provider-published exchange catalogs live here.

Purpose:
- cache provider exchange metadata such as code, country, currency, and MIC
- map provider exchange codes to canonical exchange codes
- avoid re-fetching exchange-list metadata on every EODHD lookup
- support explicit catalog refreshes from the CLI

### `securities`

Canonical security identities live here.

Purpose:
- provide the provider-agnostic key used by downstream tables
- define security identity as `canonical_ticker + canonical_exchange_code`
- store display metadata such as `entity_name` and `description`
- keep canonical symbol display stable even when provider-specific symbol formats differ

### `supported_tickers`

Provider-published ticker catalogs live here, keyed by provider and provider symbol.

Purpose:
- cache provider-published symbol catalogs by provider and exchange
- store provider-local fetch keys such as `provider_symbol`, `provider_ticker`,
  and `provider_exchange_code`
- link each provider row to canonical `security_id`
- drive exchange-level and all-supported provider workflows from one operational catalog table
- store SEC US universe membership and EODHD exchange membership in the same layer

When a provider drops a symbol, it is removed from this operational catalog, but
historical raw and derived tables are retained.

### `fundamentals_fetch_state`

Operational fetch progress and retry backoff live here.

Purpose:
- track retry state for failed fundamentals fetches
- support resumable bulk ingestion
- avoid repeatedly hitting symbols that are still inside backoff

### `fundamentals_normalization_state`

Successful normalization watermarks live here.

Purpose:
- track which raw payload timestamp was last normalized for each
  `(provider, provider_symbol)`
- support incremental `normalize-fundamentals` runs that skip unchanged raw payloads
- keep provider-local normalization state even though `financial_facts` are canonical

### `financial_facts`

Normalized provider-agnostic facts live here.

Purpose:
- give metrics a common input model
- isolate metric logic from SEC vs EODHD raw schemas
- store concept, fiscal period, end date, unit/currency, and value
- key facts by canonical `security_id`
- retain `source_provider` for provenance

Currency and unit semantics:

- monetary facts store a real ISO `currency`
- non-monetary facts do not invent currencies; they keep meaningful `unit`
  values such as `shares`
- provider catalogs such as `supported_tickers.currency` keep the raw provider
  code for provenance; monetary facts, market data, metrics, and FX rows use
  normalized base currencies
- a narrow legacy fallback still treats exact currency-like `unit` values as the
  fact currency when the explicit `currency` column is empty
- configured subunit currencies are normalized before arithmetic and
  persistence: `GBX`/`GBP0.01` -> `GBP`, `ZAC` -> `ZAR`, `ILA` -> `ILS`

### `market_data`

Stores latest quote and market-cap snapshot information.

Purpose:
- support market-cap and EV-based valuation metrics
- decouple market refresh cadence from fundamentals cadence
- store canonical rows keyed by `security_id`
- retain `source_provider` for provenance

### `fx_rates`

Stores direct FX rates fetched from the configured provider.

Purpose:

- support direct, inverse, and triangulated FX conversion
- keep FX storage separate from market snapshots and financial facts
- enable historical as-of lookups using latest available rate on or before a
  requested date

Key semantics:

- stored direction is always `1 base_currency = rate quote_currency`
- only direct provider rows are stored
- inverse and triangulated rates are derived at lookup time
- a unique constraint protects `(provider, rate_date, base_currency, quote_currency)`
- lookup indexes are designed for pair/date searches ordered by newest

### `fx_supported_pairs`

Stores the provider-published FX instrument catalog.

Purpose:

- cache the current EODHD FOREX symbol list locally
- distinguish canonical refreshable six-letter pairs from alias symbols
- map three-letter EODHD shorthand symbols such as `EUR` to canonical pairs
  such as `USDEUR`
- avoid re-scraping provider docs to discover supported pairs

### `fx_refresh_state`

Stores FX refresh coverage and retry state per canonical provider symbol.

Purpose:

- track the stored min/max historical coverage for each canonical pair
- record whether full available history has already been backfilled
- support first-full-then-incremental refresh planning
- capture retry/error state for provider failures without inferring everything
  from `fx_rates`

### `market_data_fetch_state`

Operational market-data refresh progress and retry backoff live here.

Purpose:
- track retry state for failed market-data fetches
- support resumable global market-data refreshes
- avoid repeatedly hitting symbols that are still inside backoff

### `metrics`

Stores computed metric results.

Purpose:
- cache reusable metric outputs
- support bulk screen runs without recomputing everything on demand
- keep downstream computation provider-agnostic through `security_id`

Metric rows also persist unit metadata:

- `unit_kind`: one of `monetary`, `per_share`, `ratio`, `percent`, `multiple`,
  `count`, or `other`
- `currency`: present only for currency-bearing metric kinds
- `unit_label`: optional display/unit hint such as `x` or `per_share`

## Persistence Flow

A normal run looks like:

1. provider catalogs refreshed into `supported_exchanges`, `securities`, and `supported_tickers`
2. raw fundamentals fetched into `fundamentals_raw`
3. provider-specific normalization writes canonical `financial_facts`
4. market refresh writes canonical `market_data`
5. retry/backoff state updates `fundamentals_fetch_state` and `market_data_fetch_state`
6. metric computation writes `metrics`
7. screens read from canonical metrics

## Migration Notes

Schema and migrations are handled in the storage and migration modules, not in the docs layer. See the development docs if you are changing persistence behavior.

## Related Docs

- [Normalization and Facts](normalization-and-facts.md)
- [Ingestion and Normalization Guide](../guides/ingestion-and-normalization.md)
- [Development Guide](../development/local-development.md)
