# Sample Rows Appendix

This page holds deterministic first-5-row samples for wide tables that would
make the inline per-table docs difficult to read.

Wide payload columns are omitted here and replaced with payload size metadata so
the appendix stays readable.

<!-- BEGIN generated_sample_rows_appendix -->
## `fundamentals_raw`

- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Sample window: first `5` rows returned by SQLite ordered by `provider_listing_id ASC`
- Wide payload columns are omitted and replaced with payload size metadata.

```json
[
  {
    "provider_listing_id": 1,
    "data": "<omitted>",
    "data_bytes": 535427,
    "payload_hash": "37c8aa3c7790d68136d5efa02a2072b91392ca2af3f46a58aa0af682a8e19741",
    "last_fetched_at": "2026-07-11T14:10:58.993614+00:00"
  },
  {
    "provider_listing_id": 2,
    "data": "<omitted>",
    "data_bytes": 334285,
    "payload_hash": "dd3e31be549af077921f5415f57ec595ed3d03ab5b27177fcdcaac63f49d90d0",
    "last_fetched_at": "2026-07-11T14:11:01.649999+00:00"
  },
  {
    "provider_listing_id": 3,
    "data": "<omitted>",
    "data_bytes": 453167,
    "payload_hash": "fe1d333ada9ccbe04d085e0d7d33982af0c647a4fed0f4892ac718ccc8fa99d2",
    "last_fetched_at": "2026-07-11T14:10:59.915664+00:00"
  },
  {
    "provider_listing_id": 4,
    "data": "<omitted>",
    "data_bytes": 584251,
    "payload_hash": "483efcbf00878652ef39af38b900e60d298251d3b80f4f005ef5ffc985a7cd4c",
    "last_fetched_at": "2026-07-11T14:10:59.122843+00:00"
  },
  {
    "provider_listing_id": 5,
    "data": "<omitted>",
    "data_bytes": 250671,
    "payload_hash": "788f84a20594a8cf7c3604afd3367d25f4e8e8bb635d799d149c8f0d09e5441e",
    "last_fetched_at": "2026-07-11T14:10:59.550289+00:00"
  }
]
```
<!-- END generated_sample_rows_appendix -->
