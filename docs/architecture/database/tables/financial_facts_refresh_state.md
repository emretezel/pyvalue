# `financial_facts_refresh_state`

## Purpose

Tracks when normalized financial facts were last refreshed for a canonical listing.

## Grain

One row per `listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-05-11`
- Row count: `61,987`
- Table size: `2,564,096 bytes` (`2.4 MiB`)
- Approximate bytes per row: `41.4`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK | canonical listing identity |
| `refreshed_at` | `TEXT` | no |  | latest fact refresh timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `listing_id`
- Physical foreign keys:
  - `listing_id` -> `listing`.`listing_id`
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `listing_id` in `listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- metric freshness and failure-status writes
- refresh coverage reporting

## Main Write Paths

- `normalize-fundamentals`
- bulk normalization status updates

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-05-11`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC`

```json
[
  {
    "listing_id": 1,
    "refreshed_at": "2026-04-13T13:51:55.355558+00:00"
  },
  {
    "listing_id": 2,
    "refreshed_at": "2026-04-13T13:51:54.046069+00:00"
  },
  {
    "listing_id": 3,
    "refreshed_at": "2026-04-13T13:51:54.401028+00:00"
  },
  {
    "listing_id": 4,
    "refreshed_at": "2026-04-13T13:51:54.688817+00:00"
  },
  {
    "listing_id": 5,
    "refreshed_at": "2026-04-13T13:51:54.185290+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This table is intentionally narrow.
- Watermark partition (audit §3.6 — kept separate by deliberate decision).
  Three tables track the fundamentals pipeline; each owns a distinct stage,
  so consolidation would erase observable signal:
  - `fundamentals_fetch_state` — *raw fetch* attempts. One row per active
    failure; absence means the provider listing is not currently backed off.
  - `fundamentals_normalization_state` — *normalization* watermark. Records
    the raw payload hash that was successfully normalized for a provider
    listing, so re-normalization is skipped when the upstream payload has
    not changed.
  - `financial_facts_refresh_state` (this table) — *canonical fact write*
    watermark. Records when normalized facts were last written for the
    canonical `listing_id` (which can aggregate across multiple
    `provider_listing` rows).
- The grain difference is meaningful: normalization is keyed by
  `provider_listing_id`, this table is keyed by `listing_id`. Merging the
  two would force an arbitrary choice of grain and lose the per-provider
  vs canonical distinction.
