# `fundamentals_normalization_state`

## Purpose

Tracks which raw payload hash has been normalized for a provider listing.

## Grain

One row per `provider_listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
Live stats should be regenerated after applying schema migration 040 to the
database snapshot.
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_listing_id` | `INTEGER` | no | PK, FK | provider listing identity |
| `normalized_payload_hash` | `TEXT` | no |  | raw payload hash that was normalized |
| `normalized_at` | `TEXT` | no |  | normalization timestamp |

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
- none
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

Rows should be regenerated from a migrated database snapshot.
<!-- END generated_sample_rows -->

## Review Notes

- `listing_id` is derived through `provider_listing` when needed and is not
  duplicated here.
- Fetch timestamps are not used as normalization watermarks; payload hashes are.
- Watermark partition (audit §3.6 — kept separate by deliberate decision).
  This table sits between `fundamentals_fetch_state` (raw fetch attempts,
  keyed by `provider_listing_id`) and `financial_facts_refresh_state`
  (canonical fact write, keyed by `listing_id`). Each table owns a distinct
  pipeline stage. Consolidating them would either force a single grain
  (losing per-provider vs canonical distinction) or merge orthogonal
  signals (failure backoff vs payload-hash idempotency vs canonical
  refresh time).
