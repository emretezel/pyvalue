# `provider_listing`

## Purpose

Stores provider-facing listing identity and maps provider symbols to canonical listings.

## Grain

One row per `(provider_exchange_id, provider_symbol)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Row count: `75,847`
- Table size: `1,413,120 bytes` (`1.3 MiB`)
- Approximate bytes per row: `18.6`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_listing_id` | `INTEGER` | no | PK | durable provider-listing identity for raw/state rows |
| `provider_exchange_id` | `INTEGER` | no | FK | provider exchange mapping; part of composite unique key. The owning provider is reachable via `provider_exchange.provider_id`. |
| `provider_symbol` | `TEXT` | no |  | bare provider symbol from catalog payloads such as `AAPL`; part of composite unique key |
| `listing_id` | `INTEGER` | no | FK, idx | canonical listing link |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `provider_listing_id`
- Physical foreign keys:
  - `listing_id` -> `listing`.`listing_id`
  - `provider_exchange_id` -> `provider_exchange`.`provider_exchange_id`
- Physical references from other tables:
  - `fundamentals_fetch_state`.`provider_listing_id` -> `provider_listing_id`
  - `fundamentals_normalization_state`.`provider_listing_id` -> `provider_listing_id`
  - `fundamentals_raw`.`provider_listing_id` -> `provider_listing_id`
  - `market_data_fetch_state`.`provider_listing_id` -> `provider_listing_id`
- Unique constraints beyond the primary key:
  - (`provider_exchange_id`, `provider_symbol`)
- Main logical refs: links provider catalog rows to canonical `listing_id`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_provider_listing_listing (listing_id)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- provider/exchange scope resolution for ingestion, market-data refreshes, metrics, and screens
- durable lookup from provider raw/state tables to canonical `listing`
- compatibility catalog views that expose `listing.currency` alongside provider symbols

## Main Write Paths

- `refresh-supported-tickers`
- migration-time backfill from legacy provider catalog rows
- raw fundamentals upserts that need to materialize a minimal provider listing

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Sample window: first `5` rows returned by SQLite ordered by `provider_listing_id ASC`

```json
[
  {
    "provider_listing_id": 1,
    "provider_exchange_id": 1,
    "provider_symbol": "AALB",
    "listing_id": 1
  },
  {
    "provider_listing_id": 2,
    "provider_exchange_id": 1,
    "provider_symbol": "ABN",
    "listing_id": 2
  },
  {
    "provider_listing_id": 3,
    "provider_exchange_id": 1,
    "provider_symbol": "ACOMO",
    "listing_id": 3
  },
  {
    "provider_listing_id": 4,
    "provider_exchange_id": 1,
    "provider_symbol": "AD",
    "listing_id": 4
  },
  {
    "provider_listing_id": 5,
    "provider_exchange_id": 1,
    "provider_symbol": "ADYEN",
    "listing_id": 5
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Provider descriptive fields such as security type, name, country, ISIN, listing exchange, and refresh timestamp are intentionally not persisted here.
- Bare provider symbols are only unique inside a provider exchange. Symbols such as `MRK` can exist on multiple EODHD exchanges.
- Provider-listing currency is not persisted here. Use `listing.currency` for
  the canonical quote unit; compatibility catalog APIs expose it as `currency`
  when needed.
- The owning `provider_id` is intentionally **not** stored here. It is
  always reachable via `provider_exchange.provider_id` (joined through
  `provider_exchange_id`). Migration 054 dropped the column to honour the
  *single source of truth* rule (audit P2 #9): replicating `provider_id`
  on every row was a denormalisation that the composite FK had to defend
  against drift, with no offsetting performance benefit on hot paths.
