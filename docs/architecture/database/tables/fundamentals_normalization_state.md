# `fundamentals_normalization_state`

## Purpose

Tracks which raw payload hash has been normalized for a provider listing.

## Grain

One row per `provider_listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Row count: `61,091`
- Table size: `7,208,960 bytes` (`6.9 MiB`)
- Approximate bytes per row: `118.0`
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
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Sample window: first `5` rows returned by SQLite ordered by `provider_listing_id ASC`

```json
[
  {
    "provider_listing_id": 1,
    "normalized_payload_hash": "dccd9d08007aa97929e92800dfd0ad5e5364d638e9b0a58f1bc9df3b5437c6c7",
    "normalized_at": "2026-07-04T16:33:33.768553+00:00"
  },
  {
    "provider_listing_id": 2,
    "normalized_payload_hash": "381be9035eb7de4f43b546ba6f99bda96ebf7c3cc6072aeeb5632a8cb2a7945a",
    "normalized_at": "2026-07-04T16:33:35.034111+00:00"
  },
  {
    "provider_listing_id": 3,
    "normalized_payload_hash": "c12f24b84cffcc6b767192d5aee1e7762e738d7212deca7ae427779074c4e8f3",
    "normalized_at": "2026-07-04T16:33:36.670617+00:00"
  },
  {
    "provider_listing_id": 4,
    "normalized_payload_hash": "8f65bd3b378e791e97b01fa0cfb0efc7345cdc5d69d946d326368c344002f224",
    "normalized_at": "2026-07-04T16:33:38.087693+00:00"
  },
  {
    "provider_listing_id": 5,
    "normalized_payload_hash": "aea318e6295f0aaa847c235b73c1aad5bab84b65dab11b0c1fa5f77903f6e460",
    "normalized_at": "2026-07-04T16:33:39.795148+00:00"
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
