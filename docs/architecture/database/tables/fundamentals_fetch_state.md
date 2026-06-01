# `fundamentals_fetch_state`

## Purpose

Tracks active retry state and backoff windows for fundamentals ingestion
failures.

## Grain

One active failure row per `provider_listing_id`; no row means the listing is
not currently blocked by a fundamentals fetch failure.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Row count: `16`
- Table size: `4,096 bytes` (`4.0 KiB`)
- Approximate bytes per row: `256.0`
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

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Sample window: first `5` rows returned by SQLite ordered by `provider_listing_id ASC`

```json
[
  {
    "provider_listing_id": 609,
    "failed_at": "2025-12-26T13:17:17.671188+00:00",
    "error": "('Connection broken: IncompleteRead(7630 bytes read, 2610 more expected)', IncompleteRead(7630 bytes read, 2610 more expected))",
    "next_eligible_at": "2026-04-01T17:19:48.488904+00:00",
    "attempts": 1
  },
  {
    "provider_listing_id": 669,
    "failed_at": "2025-12-26T13:17:36.847648+00:00",
    "error": "('Connection broken: IncompleteRead(5590 bytes read, 4650 more expected)', IncompleteRead(5590 bytes read, 4650 more expected))",
    "next_eligible_at": "2026-04-01T17:19:48.488904+00:00",
    "attempts": 1
  },
  {
    "provider_listing_id": 690,
    "failed_at": "2025-12-26T13:17:44.274069+00:00",
    "error": "('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))",
    "next_eligible_at": "2026-04-01T17:19:48.488904+00:00",
    "attempts": 1
  },
  {
    "provider_listing_id": 691,
    "failed_at": "2025-12-26T13:17:44.816215+00:00",
    "error": "('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))",
    "next_eligible_at": "2026-04-01T17:19:48.488904+00:00",
    "attempts": 1
  },
  {
    "provider_listing_id": 699,
    "failed_at": "2025-12-26T13:17:46.975611+00:00",
    "error": "('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))",
    "next_eligible_at": "2026-04-01T17:19:48.488904+00:00",
    "attempts": 1
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Provider and symbol values are resolved through `provider_listing`, keeping the state row narrow.
- Successful fetch progress is derived from `fundamentals_raw`, not duplicated here.
