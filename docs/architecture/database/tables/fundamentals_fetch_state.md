# `fundamentals_fetch_state`

## Purpose

Tracks retry state, backoff windows, and last fetch status for fundamentals ingestion.

## Grain

One row per `provider_listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` fetch-state table on `2026-04-21`
- Row count: `77,045`
- Table size: approximately `4.5 MiB` before the catalog-key refactor
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

- `idx_fundamentals_fetch_next (next_eligible_at)`

## Main Read Paths

- stale and missing ingest planning
- progress and recent-failure reporting

## Main Write Paths

- `ingest-fundamentals`
- retry/backoff updates

## Review Notes

- Provider and symbol values are resolved through `provider_listing`, keeping the state row narrow.
