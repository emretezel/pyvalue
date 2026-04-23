# `metric_compute_status`

## Purpose

Stores the latest success or failure attempt for each listing/metric pair.

## Grain

One row per `(listing_id, metric_id)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Row count: `4,887,360`
- Table size: `924,540,928 bytes` (`881.7 MiB`)
- Approximate bytes per row: `189.2`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK | canonical listing identity |
| `metric_id` | `TEXT` | no | PK, idx | metric identifier |
| `status` | `TEXT` | no | idx | success or failure |
| `reason_code` | `TEXT` | yes |  | failure bucket |
| `reason_detail` | `TEXT` | yes |  | diagnostic detail |
| `attempted_at` | `TEXT` | no |  | attempt timestamp |
| `value_as_of` | `TEXT` | yes |  | metric value date when successful |
| `facts_refreshed_at` | `TEXT` | yes |  | fact freshness context |
| `market_data_as_of` | `TEXT` | yes |  | market-data freshness context |
| `market_data_updated_at` | `TEXT` | yes |  | market-data write timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`listing_id`, `metric_id`)
- Physical foreign keys: none
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `listing_id` in `listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_metric_compute_status_metric_status (metric_id, status)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- metric failure reports
- screen/report diagnostics

## Main Write Paths

- `compute-metrics`
- bulk metric recomputation

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC, metric_id ASC`

```json
[
  {
    "listing_id": 1,
    "metric_id": "accruals_ratio",
    "status": "success",
    "reason_code": null,
    "reason_detail": null,
    "attempted_at": "2026-04-17T18:56:28.316700+00:00",
    "value_as_of": "2025-12-31",
    "facts_refreshed_at": "2026-04-13T13:51:55.355558+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "listing_id": 1,
    "metric_id": "avg_ic",
    "status": "success",
    "reason_code": null,
    "reason_detail": null,
    "attempted_at": "2026-04-17T18:56:28.703489+00:00",
    "value_as_of": "2025-12-31",
    "facts_refreshed_at": "2026-04-13T13:51:55.355558+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "listing_id": 1,
    "metric_id": "cfo_to_ni_10y_median",
    "status": "success",
    "reason_code": null,
    "reason_detail": null,
    "attempted_at": "2026-04-17T18:56:28.314753+00:00",
    "value_as_of": "2025-12-31",
    "facts_refreshed_at": "2026-04-13T13:51:55.355558+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "listing_id": 1,
    "metric_id": "cfo_to_ni_ttm",
    "status": "success",
    "reason_code": null,
    "reason_detail": null,
    "attempted_at": "2026-04-17T18:56:28.314595+00:00",
    "value_as_of": "2025-12-31",
    "facts_refreshed_at": "2026-04-13T13:51:55.355558+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "listing_id": 1,
    "metric_id": "current_ratio",
    "status": "success",
    "reason_code": null,
    "reason_detail": null,
    "attempted_at": "2026-04-17T18:56:28.314074+00:00",
    "value_as_of": "2025-12-31",
    "facts_refreshed_at": "2026-04-13T13:51:55.355558+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This table is large relative to its data value. Keep failure-report query paths index-aligned before adding more diagnostic columns.
