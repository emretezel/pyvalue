# Sample Rows Appendix

This page holds deterministic first-5-row samples for wide tables that would
make the inline per-table docs difficult to read.

Wide payload columns are omitted here and replaced with payload size metadata so
the appendix stays readable.

<!-- BEGIN generated_sample_rows_appendix -->
## `fundamentals_raw`

- Snapshot source: `data/pyvalue.db` on `2026-04-25`
- Sample window: first `5` rows returned by SQLite ordered by `provider_listing_id ASC`
- Wide payload columns are omitted and replaced with payload size metadata.

```json
[
  {
    "provider_listing_id": 52836,
    "data": "<omitted>",
    "data_bytes": 729107,
    "payload_hash": "<sha256>",
    "last_fetched_at": "2026-03-28T08:42:24.610037+00:00"
  },
  {
    "provider_listing_id": 52837,
    "data": "<omitted>",
    "data_bytes": 355545,
    "payload_hash": "<sha256>",
    "last_fetched_at": "2026-03-28T08:42:24.849539+00:00"
  },
  {
    "provider_listing_id": 52838,
    "data": "<omitted>",
    "data_bytes": 190638,
    "payload_hash": "<sha256>",
    "last_fetched_at": "2026-03-28T08:42:25.069017+00:00"
  },
  {
    "provider_listing_id": 52839,
    "data": "<omitted>",
    "data_bytes": 549568,
    "payload_hash": "<sha256>",
    "last_fetched_at": "2026-03-28T08:42:25.679656+00:00"
  },
  {
    "provider_listing_id": 52840,
    "data": "<omitted>",
    "data_bytes": 477300,
    "payload_hash": "<sha256>",
    "last_fetched_at": "2026-03-28T08:42:25.929485+00:00"
  }
]
```
<!-- END generated_sample_rows_appendix -->
