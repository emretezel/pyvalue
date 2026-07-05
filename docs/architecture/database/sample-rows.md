# Sample Rows Appendix

This page holds deterministic first-5-row samples for wide tables that would
make the inline per-table docs difficult to read.

Wide payload columns are omitted here and replaced with payload size metadata so
the appendix stays readable.

<!-- BEGIN generated_sample_rows_appendix -->
## `fundamentals_raw`

- Snapshot source: `data/pyvalue.db` on `2026-07-05`
- Sample window: first `5` rows returned by SQLite ordered by `provider_listing_id ASC`
- Wide payload columns are omitted and replaced with payload size metadata.

```json
[
  {
    "provider_listing_id": 1,
    "data": "<omitted>",
    "data_bytes": 566660,
    "payload_hash": "dccd9d08007aa97929e92800dfd0ad5e5364d638e9b0a58f1bc9df3b5437c6c7",
    "last_fetched_at": "2026-03-22T13:53:47.387172+00:00"
  },
  {
    "provider_listing_id": 2,
    "data": "<omitted>",
    "data_bytes": 313966,
    "payload_hash": "381be9035eb7de4f43b546ba6f99bda96ebf7c3cc6072aeeb5632a8cb2a7945a",
    "last_fetched_at": "2026-03-22T13:53:47.613748+00:00"
  },
  {
    "provider_listing_id": 3,
    "data": "<omitted>",
    "data_bytes": 483195,
    "payload_hash": "c12f24b84cffcc6b767192d5aee1e7762e738d7212deca7ae427779074c4e8f3",
    "last_fetched_at": "2026-03-22T13:53:47.909077+00:00"
  },
  {
    "provider_listing_id": 4,
    "data": "<omitted>",
    "data_bytes": 593876,
    "payload_hash": "8f65bd3b378e791e97b01fa0cfb0efc7345cdc5d69d946d326368c344002f224",
    "last_fetched_at": "2026-03-22T13:53:48.236603+00:00"
  },
  {
    "provider_listing_id": 5,
    "data": "<omitted>",
    "data_bytes": 266574,
    "payload_hash": "aea318e6295f0aaa847c235b73c1aad5bab84b65dab11b0c1fa5f77903f6e460",
    "last_fetched_at": "2026-03-22T13:53:48.456762+00:00"
  }
]
```
<!-- END generated_sample_rows_appendix -->
