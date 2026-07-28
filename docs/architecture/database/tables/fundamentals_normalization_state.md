# `fundamentals_normalization_state`

## Purpose

Tracks which raw payload hash has been normalized for a provider listing.

## Grain

One row per `provider_listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-28`
- Row count: `56,814`
- Table size: `6,270,976 bytes` (`6.0 MiB`)
- Approximate bytes per row: `110.4`
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
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- incremental normalization planning
- stale normalization reporting

## Main Write Paths

- `normalize-fundamentals`
- migration-time backfill from legacy provider-symbol state rows
- provider-layer prune: rows die with their `provider_listing` — the ticker
  refresh (removed tickers) and the dropped-venue cascade in
  `refresh-supported-exchanges` delete them

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-07-28`
- Sample window: first `5` rows returned by SQLite ordered by `provider_listing_id ASC`

```json
[
  {
    "provider_listing_id": 1,
    "normalized_payload_hash": "37c8aa3c7790d68136d5efa02a2072b91392ca2af3f46a58aa0af682a8e19741",
    "normalized_at": "2026-07-20T20:00:00.863706+00:00"
  },
  {
    "provider_listing_id": 2,
    "normalized_payload_hash": "dd3e31be549af077921f5415f57ec595ed3d03ab5b27177fcdcaac63f49d90d0",
    "normalized_at": "2026-07-20T20:00:02.048675+00:00"
  },
  {
    "provider_listing_id": 3,
    "normalized_payload_hash": "fe1d333ada9ccbe04d085e0d7d33982af0c647a4fed0f4892ac718ccc8fa99d2",
    "normalized_at": "2026-07-20T20:00:03.779443+00:00"
  },
  {
    "provider_listing_id": 4,
    "normalized_payload_hash": "483efcbf00878652ef39af38b900e60d298251d3b80f4f005ef5ffc985a7cd4c",
    "normalized_at": "2026-07-20T20:00:05.527799+00:00"
  },
  {
    "provider_listing_id": 5,
    "normalized_payload_hash": "788f84a20594a8cf7c3604afd3367d25f4e8e8bb635d799d149c8f0d09e5441e",
    "normalized_at": "2026-07-20T20:00:06.982898+00:00"
  }
]
```
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
