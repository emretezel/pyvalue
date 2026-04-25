# `fundamentals_normalization_state`

## Purpose

Tracks which raw payload timestamp has been normalized for a provider listing.

## Grain

One row per `provider_listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-25`
- Row count: `61,092`
- Table size: `5,169,152 bytes` (`4.9 MiB`)
- Approximate bytes per row: `84.6`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_listing_id` | `INTEGER` | no | PK, FK | provider listing identity |
| `listing_id` | `INTEGER` | no | FK, idx | canonical listing identity |
| `raw_fetched_at` | `TEXT` | no |  | raw payload watermark |
| `last_normalized_at` | `TEXT` | no |  | normalization timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `provider_listing_id`
- Physical foreign keys:
  - `listing_id` -> `listing`.`listing_id`
  - `provider_listing_id` -> `provider_listing`.`provider_listing_id`
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `provider_listing_id` in `provider_listing`, `listing_id` in `listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_fundamentals_norm_state_security (listing_id)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- incremental normalization planning
- stale normalization reporting

## Main Write Paths

- `normalize-fundamentals`
- migration-time backfill from legacy provider-symbol state rows

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-25`
- Sample window: first `5` rows returned by SQLite ordered by `provider_listing_id ASC`

```json
[
  {
    "provider_listing_id": 1,
    "listing_id": 1,
    "raw_fetched_at": "2026-03-22T13:53:47.387172+00:00",
    "last_normalized_at": "2026-04-13T13:51:55.370224+00:00"
  },
  {
    "provider_listing_id": 2,
    "listing_id": 2,
    "raw_fetched_at": "2026-03-22T13:53:47.613748+00:00",
    "last_normalized_at": "2026-04-13T13:51:54.070234+00:00"
  },
  {
    "provider_listing_id": 3,
    "listing_id": 3,
    "raw_fetched_at": "2026-03-22T13:53:47.909077+00:00",
    "last_normalized_at": "2026-04-13T13:51:54.419968+00:00"
  },
  {
    "provider_listing_id": 4,
    "listing_id": 4,
    "raw_fetched_at": "2026-03-22T13:53:48.236603+00:00",
    "last_normalized_at": "2026-04-13T13:51:54.716059+00:00"
  },
  {
    "provider_listing_id": 5,
    "listing_id": 5,
    "raw_fetched_at": "2026-03-22T13:53:48.456762+00:00",
    "last_normalized_at": "2026-04-13T13:51:54.204930+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- `listing_id` is retained because normalization writes canonical facts and needs a cheap canonical join.
