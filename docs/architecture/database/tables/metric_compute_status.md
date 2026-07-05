# `metric_compute_status`

## Purpose

Stores the latest success or failure attempt for each listing/metric pair.

## Grain

One row per `(listing_id, metric_id)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-05`
- Row count: `5,620,372`
- Table size: `1,093,685,248 bytes` (`1.02 GiB`)
- Approximate bytes per row: `194.6`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK | canonical listing identity |
| `metric_id` | `TEXT` | no | PK, idx | metric identifier |
| `status` | `TEXT` | no | idx | `'success'` or `'failure'`, enforced by CHECK |
| `reason_code` | `TEXT` | yes |  | failure bucket |
| `reason_detail` | `TEXT` | yes |  | diagnostic detail: untemplated first warning for guard failures, invariant/exception text for raised failures |
| `attempted_at` | `TEXT` | no |  | attempt timestamp |
| `value_as_of` | `TEXT` | yes |  | metric value date when successful |
| `facts_refreshed_at` | `TEXT` | yes |  | fact freshness context |
| `market_data_as_of` | `TEXT` | yes |  | market-data freshness context |
| `market_data_updated_at` | `TEXT` | yes |  | market-data write timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`listing_id`, `metric_id`)
- Physical foreign keys:
  - `listing_id` -> `listing`.`listing_id`
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `listing_id` in `listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- `report-metric-status` (summary aggregates; `--reasons` per-pair states)
- `report-screen-failures` / `run-screen` (status-shadowed metric reads and NA
  display)
- `explain-metric` (persisted attempt state)

## Main Write Paths

- `compute-metrics` (sole writer of attempt rows)
- `clear-metrics` / `clear-financial-facts` (bulk delete)

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-07-05`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC, metric_id ASC`

```json
[
  {
    "listing_id": 1,
    "metric_id": "accruals_ratio",
    "status": "success",
    "reason_code": null,
    "reason_detail": null,
    "attempted_at": "2026-07-04T21:39:33.468176+00:00",
    "value_as_of": "2025-12-31",
    "facts_refreshed_at": "2026-07-04T16:33:33.762781+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "listing_id": 1,
    "metric_id": "altman_z",
    "status": "success",
    "reason_code": null,
    "reason_detail": null,
    "attempted_at": "2026-07-04T21:39:33.471490+00:00",
    "value_as_of": "2025-12-31",
    "facts_refreshed_at": "2026-07-04T16:33:33.762781+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "listing_id": 1,
    "metric_id": "avg_ic",
    "status": "success",
    "reason_code": null,
    "reason_detail": null,
    "attempted_at": "2026-07-04T21:39:33.515295+00:00",
    "value_as_of": "2025-12-31",
    "facts_refreshed_at": "2026-07-04T16:33:33.762781+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "listing_id": 1,
    "metric_id": "cfo_to_ni_10y_median",
    "status": "success",
    "reason_code": null,
    "reason_detail": null,
    "attempted_at": "2026-07-04T21:39:33.462117+00:00",
    "value_as_of": "2025-12-31",
    "facts_refreshed_at": "2026-07-04T16:33:33.762781+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "listing_id": 1,
    "metric_id": "cfo_to_ni_ttm",
    "status": "success",
    "reason_code": null,
    "reason_detail": null,
    "attempted_at": "2026-07-04T21:39:33.459184+00:00",
    "value_as_of": "2025-12-31",
    "facts_refreshed_at": "2026-07-04T16:33:33.762781+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This table is large relative to its data value. Keep status-survey query paths index-aligned before adding more diagnostic columns.
- Since 2026-07-05 guard failures populate `reason_detail` (untemplated first
  warning, ~60–120 bytes per failure row) instead of leaving it NULL — accepted
  diagnostic-volume growth; no index impact (column was already present).
- Column audit (2026-07-05, after the diagnostics went read-only): every column
  is load-bearing — `status`/`reason_code`/`reason_detail` feed the status
  survey, screen diagnostics, and explain-metric; `attempted_at` feeds
  explain-metric's display; the four watermark columns (`value_as_of`,
  `facts_refreshed_at`, `market_data_as_of`, `market_data_updated_at`) are read
  only by the staleness comparison (`cli/_repos.py`) that run-screen,
  explain-metric, and `report-metric-status --reasons` all depend on. Dropping
  any of them requires removing staleness-shadowing first.
