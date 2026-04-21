# `market_data_fetch_state`

## Purpose

Tracks retry state, backoff windows, and last fetch status for provider market-data refreshes.

## Grain

One row per `provider_listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` market-data fetch-state table on `2026-04-21`
- Row count: `61,092`
- Table size: approximately `4.3 MiB` before the catalog-key refactor
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_listing_id` | `INTEGER` | no | PK, FK | provider listing identity |
| `last_fetched_at` | `TEXT` | yes |  | last successful or attempted fetch time |
| `last_status` | `TEXT` | yes |  | latest status |
| `last_error` | `TEXT` | yes |  | latest provider error |
| `next_eligible_at` | `TEXT` | yes | idx | retry/backoff watermark |
| `attempts` | `INTEGER` | no |  | retry counter |

## Keys And Relationships

- Primary key: `provider_listing_id`
- Physical foreign key: `provider_listing_id -> provider_listing.provider_listing_id`

## Secondary Indexes

- `idx_market_data_fetch_next (next_eligible_at)`

## Main Read Paths

- stale market-data refresh planning
- market-data progress and recent-failure reporting

## Main Write Paths

- `update-market-data`
- retry/backoff updates

## Review Notes

- Provider and symbol values are resolved through `provider_listing`, not duplicated in this state table.
