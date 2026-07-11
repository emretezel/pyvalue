# `fundamentals_fetch_state`

## Purpose

Tracks active retry state and backoff windows for fundamentals ingestion
failures.

## Grain

One active failure row per `provider_listing_id`; no row means the listing is
not currently blocked by a fundamentals fetch failure.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Row count: `0`
- Table size: `4,096 bytes` (`4.0 KiB`)
- Approximate bytes per row: `0.0`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_listing_id` | `INTEGER` | no | PK, FK | provider listing identity |
| `failed_at` | `TEXT` | no |  | latest failure timestamp |
| `error` | `TEXT` | no |  | latest provider error |
| `next_eligible_at` | `TEXT` | no | idx | retry/backoff watermark |
| `attempts` | `INTEGER` | no |  | retry counter, always positive |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `provider_listing_id`
- Physical foreign keys:
  - `provider_listing_id` -> `provider_listing`.`provider_listing_id`
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `provider_listing_id` in `provider_listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- active backoff filtering for ingest planning
- progress and recent-failure reporting

## Main Write Paths

- `ingest-fundamentals`
- retry/backoff updates
- successful raw upserts delete active failure rows
- provider-layer prune: rows die with their `provider_listing` — the ticker
  refresh (removed tickers) and the dropped-venue cascade in
  `refresh-supported-exchanges` delete them

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Sample window: first `0` rows returned by SQLite ordered by `provider_listing_id ASC`

```json
[]
```
<!-- END generated_sample_rows -->

## Review Notes

- Provider and symbol values are resolved through `provider_listing`, keeping the state row narrow.
- Successful fetch progress is derived from `fundamentals_raw`, not duplicated here.
