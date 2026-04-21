# `fundamentals_raw`

## Purpose

Stores the latest raw fundamentals payload for each provider listing.

## Grain

One row per `provider_listing_id`; historical payload versions are not retained.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` raw payload table on `2026-04-21`
- Row count: `77,045`
- Table size: approximately `16.84 GiB` before the catalog-key refactor
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `payload_id` | `INTEGER` | no | PK | raw payload surrogate key |
| `provider_listing_id` | `INTEGER` | no | unique, FK | provider listing identity |
| `listing_id` | `INTEGER` | no | FK, idx | canonical listing link |
| `currency` | `TEXT` | yes |  | provider payload currency hint |
| `data` | `TEXT` | no |  | raw JSON payload |
| `fetched_at` | `TEXT` | no | idx | latest fetch timestamp |

## Keys And Relationships

- Primary key: `payload_id`
- Unique constraint: `provider_listing_id`
- Physical foreign keys:
  - `provider_listing_id -> provider_listing.provider_listing_id`
  - `listing_id -> listing.listing_id`

## Secondary Indexes

- `idx_fundamentals_raw_security (listing_id)`
- `idx_fundamentals_raw_provider_fetched (fetched_at)`

## Main Read Paths

- normalization reads by provider listing or canonical listing
- issuer metadata refresh from stored raw payloads
- primary-listing reconciliation

## Main Write Paths

- `ingest-fundamentals`
- migration-time backfill from legacy `(provider, provider_symbol)` raw rows

## Review Notes

- The `data` column is the widest row in the schema and a major I/O hotspot.
- The durable provider key is now `provider_listing_id`, not `(provider, provider_symbol)`.
