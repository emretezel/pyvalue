# Data Model and Storage

## Storage Model

`pyvalue` stores operational data in SQLite.

For a human-readable table-by-table schema review, including columns, primary
keys, foreign keys, unique constraints, indexes, first-five sample rows, and
query hotspots, use the
[Database Review Guide](database/README.md).

The main persisted layers are:

- provider registry
- canonical exchange identities
- provider exchange catalogs
- issuer metadata
- canonical listings
- provider listings
- raw fundamentals
- listing classification state
- provider-scoped fetch and normalization state
- normalized financial facts
- market data snapshots
- FX rates and FX provider catalogs
- computed metrics and metric attempt status

## Core Tables

### `provider`

Global provider metadata lives here. `provider_id` is the physical FK key, while
`provider_code` remains the stable external namespace string such as `EODHD`,
`SEC`, or `FRANKFURTER`.

### `exchange`

Canonical exchange identities live here. Provider-owned exchange metadata does
not belong in this table.

### `provider_exchange`

Provider-published exchange catalogs live here. Each row maps a
provider-local exchange code to `exchange.exchange_id` and stores provider-owned
exchange metadata such as country, currency, and MIC.

### `issuer`

Issuer-level descriptive metadata lives here. Moving this data out of canonical
listing identity keeps `listing` narrow and lets metadata be refreshed without
changing canonical listing keys.

### `listing`

Canonical listing identity lives here. A listing is defined by
`(exchange_id, symbol)`, and user-facing canonical symbols such as `AAPL.US`
are derived from `listing.symbol + exchange.exchange_code`.

### `provider_listing`

Provider-facing listing identity lives here. Rows are unique by
`(provider_exchange_id, provider_symbol)`, where `provider_symbol` is the bare
provider catalog symbol such as `AAPL`, not `AAPL.US`.

`provider_listing.currency` is the first source of truth for a listing's
normalization and metric currency. If it is missing, code falls back to
`listing.currency`.

`provider_listing` intentionally does not store provider-side descriptive
columns such as security type, provider name, country, ISIN, or refresh
timestamp. ETF filtering remains a load-time decision before insert.

### `fundamentals_raw`

Raw provider payloads are stored by `provider_listing_id`. Canonical
`listing_id` is derived by joining through `provider_listing`.
The table intentionally does not store currency; raw payload currencies are
used only as source currencies for individual normalized facts.

Purpose:

- preserve source payloads as received
- support re-normalization when normalization logic changes
- separate provider-fetch concerns from metric computation
- preserve the provider-listing link needed to derive canonical `listing_id`

### `fundamentals_fetch_state`

Operational fundamentals fetch progress and retry backoff live here, keyed by
`provider_listing_id`.

### `security_listing_status`

Cached EODHD primary-vs-secondary listing classification lives here, keyed by
canonical `listing_id`. It lets downstream stages exclude secondary listings
without re-parsing `fundamentals_raw.data`.

### `fundamentals_normalization_state`

Successful normalization watermarks live here, keyed by `provider_listing_id`
with a canonical `listing_id` column for downstream joins.

### `financial_facts`

Normalized provider-agnostic facts live here, keyed by canonical `listing_id`.

Currency and unit semantics:

- monetary facts store a real ISO `currency`
- non-monetary facts keep meaningful `unit` values such as `shares`
- listing currency is resolved from `provider_listing.currency` first, then
  `listing.currency`; raw fundamentals and `market_data.currency` are not
  listing-currency sources
- configured subunit currencies are normalized before arithmetic and
  persistence: `GBX`/`GBP0.01` -> `GBP`, `ZAC` -> `ZAR`, `ILA` -> `ILS`

### `market_data`

Stores latest quote and market-cap snapshot information by `listing_id`.
`market_data.currency` stores the quote row currency for price and market-cap
snapshots. It is not used as listing-currency metadata for normalization or
metric currency invariants.

### `market_data_fetch_state`

Operational market-data refresh progress and retry backoff live here, keyed by
`provider_listing_id`.

### `fx_rates`, `fx_supported_pairs`, and `fx_refresh_state`

FX storage remains provider-code keyed. FX discovery reads currencies from
`provider_listing`, `financial_facts`, and `market_data`.

### `metrics` and `metric_compute_status`

Metrics and metric attempt status are keyed by canonical `listing_id`.

Metric rows also persist unit metadata:

- `unit_kind`: one of `monetary`, `per_share`, `ratio`, `percent`, `multiple`,
  `count`, or `other`
- `currency`: present only for currency-bearing metric kinds
- `unit_label`: optional display/unit hint such as `x` or `per_share`

## Persistence Flow

A normal run looks like:

1. Provider registry is seeded into `provider`.
2. Provider exchange catalogs are refreshed into `exchange` and `provider_exchange`.
3. Provider listing catalogs are refreshed into `issuer`, `listing`, and `provider_listing`.
4. Raw fundamentals are fetched into `fundamentals_raw`.
5. EODHD raw writes refresh `security_listing_status` from `General.PrimaryTicker`.
6. Provider-specific normalization writes canonical `financial_facts`.
7. Market refresh writes canonical `market_data`.
8. Retry/backoff state updates `fundamentals_fetch_state` and `market_data_fetch_state`.
9. Metric computation writes `metrics` and `metric_compute_status`.
10. Screens read from canonical metrics and derived canonical symbols.

If an EODHD listing is classified as secondary, downstream rows for that
`listing_id` are deleted from `financial_facts`, `market_data`, `metrics`, and
related downstream refresh-state tables.

## Migration Notes

Schema and migrations are handled in the storage and migration modules, not in
the docs layer. The catalog refactor migrates existing data in place so
fundamentals, market data, and FX rates are not re-downloaded just to adopt the
new keys.

## Related Docs

- [Normalization and Facts](normalization-and-facts.md)
- [Ingestion and Normalization Guide](../guides/ingestion-and-normalization.md)
- [Development Guide](../development/local-development.md)
