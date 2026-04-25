# `fundamentals_raw`

## Purpose

Stores the latest raw fundamentals payload for each provider listing.

## Grain

One row per `provider_listing_id`; historical payload versions are not retained.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-25`
- Row count: `75,848`
- Table size: `17,852,002,304 bytes` (`16.63 GiB`)
- Approximate bytes per row: `235,365.5`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `payload_id` | `INTEGER` | no | PK | raw payload surrogate key |
| `provider_listing_id` | `INTEGER` | no | unique, FK | provider listing identity |
| `data` | `TEXT` | no |  | raw JSON payload |
| `fetched_at` | `TEXT` | no | idx | latest fetch timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `payload_id`
- Physical foreign keys:
  - `provider_listing_id` -> `provider_listing`.`provider_listing_id`
- Physical references from other tables: none
- Unique constraints beyond the primary key:
  - `provider_listing_id`
- Main logical refs: `provider_listing_id` in `provider_listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_fundamentals_raw_provider_fetched (fetched_at)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- provider-scoped payload lookup through `provider_listing_id`
- canonical listing lookup by joining through `provider_listing`
- issuer metadata refresh and primary-listing reconciliation from stored raw payloads

## Main Write Paths

- `ingest-fundamentals`
- migration-time backfill from legacy `(provider, provider_symbol)` raw rows

## Sample Rows

<!-- BEGIN generated_sample_rows -->
Wide-table sample rows live in the [Sample Rows appendix](../sample-rows.md#fundamentals_raw).
<!-- END generated_sample_rows -->

## Review Notes

- The `data` column is the widest row in the schema and a major I/O hotspot.
- The durable provider key is now `provider_listing_id`, not `(provider, provider_symbol)`.
- Listing currency is intentionally not stored here. Use `listing.currency` for
  catalog quote-unit metadata; raw payload currencies are fact source
  currencies only.
