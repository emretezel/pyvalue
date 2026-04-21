# `security_listing_status`

## Purpose

Caches EODHD primary-vs-secondary listing classification so downstream scopes can exclude secondary listings without reparsing raw JSON.

## Grain

One row per canonical `listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` listing-status table on `2026-04-21`
- Row count: `75,848`
- Table size: approximately `9.5 MiB` before the catalog-key refactor
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK, FK | canonical listing identity |
| `source_provider` | `TEXT` | no |  | provider that supplied the classification |
| `provider_listing_id` | `INTEGER` | no | FK | provider listing used for the classification |
| `raw_fetched_at` | `TEXT` | no |  | raw payload timestamp |
| `is_primary_listing` | `INTEGER` | no | idx | `0` or `1` |
| `primary_provider_listing_id` | `INTEGER` | yes | FK | provider listing for the primary ticker when known |
| `classification_basis` | `TEXT` | no |  | match/mismatch/missing-primary reason |
| `updated_at` | `TEXT` | no |  | status update timestamp |

## Keys And Relationships

- Primary key: `listing_id`
- Physical foreign keys:
  - `listing_id -> listing.listing_id`
  - `provider_listing_id -> provider_listing.provider_listing_id`
  - `primary_provider_listing_id -> provider_listing.provider_listing_id`

## Secondary Indexes

- `idx_security_listing_status_primary (is_primary_listing, listing_id)`

## Main Read Paths

- primary-only scope resolution for normalization, market data, metrics, and screens
- cleanup planning when a listing is reclassified as secondary

## Main Write Paths

- `reconcile-listing-status`
- raw fundamentals upserts that classify EODHD listings

## Review Notes

- The table remains canonical-listing keyed because downstream exclusion filters start from canonical scopes.
