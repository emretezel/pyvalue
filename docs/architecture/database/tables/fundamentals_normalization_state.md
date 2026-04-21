# `fundamentals_normalization_state`

## Purpose

Tracks which raw payload timestamp has been normalized for a provider listing.

## Grain

One row per `provider_listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` normalization-state table on `2026-04-21`
- Row count: `61,092`
- Table size: approximately `6.0 MiB` before the catalog-key refactor
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_listing_id` | `INTEGER` | no | PK, FK | provider listing identity |
| `listing_id` | `INTEGER` | no | FK, idx | canonical listing identity |
| `raw_fetched_at` | `TEXT` | no |  | raw payload watermark |
| `last_normalized_at` | `TEXT` | no |  | normalization timestamp |

## Keys And Relationships

- Primary key: `provider_listing_id`
- Physical foreign keys:
  - `provider_listing_id -> provider_listing.provider_listing_id`
  - `listing_id -> listing.listing_id`

## Secondary Indexes

- `idx_fundamentals_norm_state_security (listing_id)`

## Main Read Paths

- incremental normalization planning
- stale normalization reporting

## Main Write Paths

- `normalize-fundamentals`
- migration-time backfill from legacy provider-symbol state rows

## Review Notes

- `listing_id` is retained because normalization writes canonical facts and needs a cheap canonical join.
