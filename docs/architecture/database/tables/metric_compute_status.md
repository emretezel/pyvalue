# `metric_compute_status`

## Purpose

Stores the latest success or failure attempt for each listing/metric pair.

## Grain

One row per `(listing_id, metric_id)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` metric-status table on `2026-04-21`
- Row count: `4,887,360`
- Table size: approximately `936.0 MiB` before the `listing_id` rename
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

- Primary key: `(listing_id, metric_id)`
- Logical reference: `listing_id -> listing.listing_id`

## Secondary Indexes

- `idx_metric_compute_status_metric_status (metric_id, status)`

## Main Read Paths

- metric failure reports
- screen/report diagnostics

## Main Write Paths

- `compute-metrics`
- bulk metric recomputation

## Review Notes

- This table is large relative to its data value. Keep failure-report query paths index-aligned before adding more diagnostic columns.
