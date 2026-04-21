# `metrics`

## Purpose

Stores the latest computed metric value per canonical listing and metric.

## Grain

One row per `(listing_id, metric_id)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` metrics table on `2026-04-21`
- Row count: `2,422,916`
- Table size: approximately `158.9 MiB` before the `listing_id` rename
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK | canonical listing identity |
| `metric_id` | `TEXT` | no | PK, idx | metric identifier |
| `value` | `REAL` | no |  | computed metric value |
| `as_of` | `TEXT` | no |  | metric value date |
| `unit_kind` | `TEXT` | no |  | metric unit category |
| `currency` | `TEXT` | yes |  | metric currency when monetary |
| `unit_label` | `TEXT` | yes |  | display unit label |

## Keys And Relationships

- Primary key: `(listing_id, metric_id)`
- Logical reference: `listing_id -> listing.listing_id`

## Secondary Indexes

- `idx_metrics_metric_id (metric_id)`

## Main Read Paths

- screen and report queries by metric id
- per-symbol metric lookup

## Main Write Paths

- `compute-metrics`
- bulk metric recomputation

## Review Notes

- This table stores latest values only. Historical metric versions would require a separate table or an expanded key.
