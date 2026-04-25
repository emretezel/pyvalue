# `fundamentals_fetch_state`

## Purpose

Tracks retry state, backoff windows, and last fetch status for fundamentals ingestion.

## Grain

One row per `provider_listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-25`
- Row count: `75,848`
- Table size: `3,993,600 bytes` (`3.8 MiB`)
- Approximate bytes per row: `52.7`
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
- `idx_fundamentals_fetch_next (next_eligible_at)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- stale and missing ingest planning
- progress and recent-failure reporting

## Main Write Paths

- `ingest-fundamentals`
- retry/backoff updates

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-25`
- Sample window: first `5` rows returned by SQLite ordered by `provider_listing_id ASC`

```json
[
  {
    "provider_listing_id": 1,
    "last_fetched_at": "2026-03-22T13:53:47.395768+00:00",
    "last_status": "ok",
    "last_error": null,
    "next_eligible_at": null,
    "attempts": 0
  },
  {
    "provider_listing_id": 2,
    "last_fetched_at": "2026-03-22T13:53:47.618475+00:00",
    "last_status": "ok",
    "last_error": null,
    "next_eligible_at": null,
    "attempts": 0
  },
  {
    "provider_listing_id": 3,
    "last_fetched_at": "2026-03-22T13:53:47.915506+00:00",
    "last_status": "ok",
    "last_error": null,
    "next_eligible_at": null,
    "attempts": 0
  },
  {
    "provider_listing_id": 4,
    "last_fetched_at": "2026-03-22T13:53:48.242701+00:00",
    "last_status": "ok",
    "last_error": null,
    "next_eligible_at": null,
    "attempts": 0
  },
  {
    "provider_listing_id": 5,
    "last_fetched_at": "2026-03-22T13:53:48.462519+00:00",
    "last_status": "ok",
    "last_error": null,
    "next_eligible_at": null,
    "attempts": 0
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Provider and symbol values are resolved through `provider_listing`, keeping the state row narrow.
