# `market_data_fetch_state`

## Purpose

Tracks retry state, backoff windows, and last fetch status for provider market-data refreshes.

## Grain

One row per `provider_listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-05-11`
- Row count: `61,092`
- Table size: `2,961,408 bytes` (`2.8 MiB`)
- Approximate bytes per row: `48.5`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_listing_id` | `INTEGER` | no | PK, FK | provider listing identity |
| `last_fetched_at` | `TEXT` | yes |  | last successful or attempted fetch time |
| `last_status` | `TEXT` | yes |  | latest status; CHECK enforces `IN ('ok','error')` when not NULL |
| `last_error` | `TEXT` | yes |  | latest provider error. Row-level CHECK requires this to be NOT NULL when `last_status = 'error'` |
| `next_eligible_at` | `TEXT` | yes | idx | retry/backoff watermark |
| `attempts` | `INTEGER` | no |  | retry counter; CHECK enforces `>= 0` |

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

- stale market-data refresh planning
- market-data progress and recent-failure reporting

## Main Write Paths

- `update-market-data`
- retry/backoff updates

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-05-11`
- Sample window: first `5` rows returned by SQLite ordered by `provider_listing_id ASC`

```json
[
  {
    "provider_listing_id": 1,
    "last_fetched_at": "2026-04-11T08:25:55.378209+00:00",
    "last_status": "ok",
    "last_error": null,
    "next_eligible_at": null,
    "attempts": 0
  },
  {
    "provider_listing_id": 2,
    "last_fetched_at": "2026-04-11T08:25:55.378209+00:00",
    "last_status": "ok",
    "last_error": null,
    "next_eligible_at": null,
    "attempts": 0
  },
  {
    "provider_listing_id": 3,
    "last_fetched_at": "2026-04-11T08:25:55.378209+00:00",
    "last_status": "ok",
    "last_error": null,
    "next_eligible_at": null,
    "attempts": 0
  },
  {
    "provider_listing_id": 4,
    "last_fetched_at": "2026-04-11T08:25:55.378209+00:00",
    "last_status": "ok",
    "last_error": null,
    "next_eligible_at": null,
    "attempts": 0
  },
  {
    "provider_listing_id": 5,
    "last_fetched_at": "2026-04-11T08:25:55.378209+00:00",
    "last_status": "ok",
    "last_error": null,
    "next_eligible_at": null,
    "attempts": 0
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Provider and symbol values are resolved through `provider_listing`, not duplicated in this state table.
