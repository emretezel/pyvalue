# `metrics`

## Purpose

Stores the latest computed value for each metric and security.

## Grain

One row per `(security_id, metric_id)`.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `2,422,916`
- Table size: `166,580,224 bytes` (`158.9 MiB`)
- Approximate bytes per row: `68.8`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `security_id` | `INTEGER` | no | PK | canonical identity link |
| `metric_id` | `TEXT` | no | PK, idx | metric identifier |
| `value` | `REAL` | no |  | computed metric value |
| `as_of` | `TEXT` | no |  | metric timestamp |
| `unit_kind` | `TEXT` | no |  | monetary, ratio, percent, and so on |
| `currency` | `TEXT` | yes |  | only for currency-bearing metrics |
| `unit_label` | `TEXT` | yes |  | optional display suffix |

## Keys And Relationships

- Primary key: `(security_id, metric_id)`
- Logical references:
  - `security_id` to `securities`

## Secondary Indexes

- `idx_metrics_metric_id (metric_id)`

## Main Read Paths

- `run-screen`
- reporting commands that inspect stored metrics
- reusable metric cache reads

## Main Write Paths

- `compute-metrics`
- purge when a listing becomes secondary

## Column Usage Notes

- `security_id`: canonical scope key for symbol-metric lookups.
- `metric_id`: filter key for screen and reporting reads.
- `value`: stored metric result used directly by screens and reports.
- `as_of`: metric timestamp reused for freshness-sensitive reads.
- `unit_kind`: tells readers whether currency metadata should be interpreted.
- `currency`: present for monetary/per-share metrics and used by display and validation helpers.
- `unit_label`: optional presentation hint, mainly for display/reporting.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Sample window: first `5` rows returned by SQLite using `LIMIT` with no `ORDER BY`

```json
[
  {
    "security_id": 28127,
    "metric_id": "working_capital",
    "value": -278959824.0,
    "as_of": "2025-12-31",
    "unit_kind": "monetary",
    "currency": "USD",
    "unit_label": null
  },
  {
    "security_id": 28127,
    "metric_id": "current_ratio",
    "value": 0.9751913595536019,
    "as_of": "2025-12-31",
    "unit_kind": "ratio",
    "currency": null,
    "unit_label": null
  },
  {
    "security_id": 28127,
    "metric_id": "long_term_debt",
    "value": 4740028436.0,
    "as_of": "2025-12-31",
    "unit_kind": "monetary",
    "currency": "USD",
    "unit_label": null
  },
  {
    "security_id": 28127,
    "metric_id": "eps_streak",
    "value": 0.0,
    "as_of": "2025-12-31",
    "unit_kind": "count",
    "currency": null,
    "unit_label": null
  },
  {
    "security_id": 28127,
    "metric_id": "eps_ttm",
    "value": -1.9099350926193535,
    "as_of": "2025-12-31",
    "unit_kind": "per_share",
    "currency": "USD",
    "unit_label": "per_share"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This table is intentionally latest-only to keep screening fast
- Review whether metric-oriented scans need a stronger secondary index than `metric_id` alone as the universe grows
