# `provider_listing`

## Purpose

Stores provider-facing listing identity and maps provider symbols to canonical listings.

## Grain

One row per `(provider_exchange_id, provider_symbol)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` `supported_tickers` catalog on `2026-04-21`
- Row count: `75,848`
- Table size: expected to be narrower than the old provider catalog because descriptive provider columns were dropped
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_listing_id` | `INTEGER` | no | PK | durable provider-listing identity for raw/state rows |
| `provider_id` | `INTEGER` | no | FK, idx | provider namespace |
| `provider_exchange_id` | `INTEGER` | no | FK, unique | provider exchange mapping |
| `provider_symbol` | `TEXT` | no | unique | bare provider symbol from catalog payloads such as `AAPL` |
| `currency` | `TEXT` | yes | partial idx | provider catalog currency hint |
| `listing_id` | `INTEGER` | no | FK, idx | canonical listing link |

## Keys And Relationships

- Primary key: `provider_listing_id`
- Unique constraint: `(provider_exchange_id, provider_symbol)`
- Physical foreign keys:
  - `provider_id -> provider.provider_id`
  - `provider_exchange_id -> provider_exchange.provider_exchange_id`
  - `listing_id -> listing.listing_id`
  - `(provider_exchange_id, provider_id) -> provider_exchange(provider_exchange_id, provider_id)`
- Physical references:
  - `fundamentals_raw.provider_listing_id`
  - `fundamentals_fetch_state.provider_listing_id`
  - `fundamentals_normalization_state.provider_listing_id`
  - `market_data_fetch_state.provider_listing_id`
  - `security_listing_status.provider_listing_id`

## Secondary Indexes

- `idx_provider_listing_provider (provider_id)`
- `idx_provider_listing_listing (listing_id)`
- `idx_provider_listing_currency_nonnull (currency) WHERE currency IS NOT NULL`

## Main Read Paths

- provider/exchange scope resolution for ingestion, market-data refreshes, metrics, and screens
- durable lookup from provider raw/state tables to canonical `listing`
- FX currency discovery from provider catalog hints

## Main Write Paths

- `refresh-supported-tickers`
- migration-time backfill from legacy provider catalog rows
- raw fundamentals upserts that need to materialize a minimal provider listing

## Review Notes

- Provider descriptive fields such as security type, name, country, ISIN, listing exchange, and refresh timestamp are intentionally not persisted here.
- Bare provider symbols are only unique inside a provider exchange. Symbols such as `MRK` can exist on multiple EODHD exchanges.
