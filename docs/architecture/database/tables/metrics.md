# `metrics`

## Purpose

Stores the latest computed metric value per canonical listing and metric.

## Grain

One row per `(listing_id, metric_id)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Row count: `2,422,916`
- Table size: `136,704,000 bytes` (`130.4 MiB`)
- Approximate bytes per row: `56.4`
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

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`listing_id`, `metric_id`)
- Physical foreign keys: none
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `listing_id` in `listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_metrics_metric_id (metric_id)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- screen and report queries by metric id
- per-symbol metric lookup

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
    "value": -0.1261966826029527,
    "as_of": "2025-12-31",
    "unit_kind": "ratio",
    "currency": null,
    "unit_label": null
  },
  {
    "listing_id": 1,
    "metric_id": "avg_ic",
    "value": 3125299999.5,
    "as_of": "2025-12-31",
    "unit_kind": "monetary",
    "currency": "EUR",
    "unit_label": null
  },
  {
    "listing_id": 1,
    "metric_id": "cfo_to_ni_10y_median",
    "value": 1.658532819876974,
    "as_of": "2025-12-31",
    "unit_kind": "ratio",
    "currency": null,
    "unit_label": null
  },
  {
    "listing_id": 1,
    "metric_id": "cfo_to_ni_ttm",
    "value": 3.0156833457804333,
    "as_of": "2025-12-31",
    "unit_kind": "ratio",
    "currency": null,
    "unit_label": null
  },
  {
    "listing_id": 1,
    "metric_id": "current_ratio",
    "value": 1.5236842105263158,
    "as_of": "2025-12-31",
    "unit_kind": "ratio",
    "currency": null,
    "unit_label": null
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This table stores latest values only. Historical metric versions would require a separate table or an expanded key.
