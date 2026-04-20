# `metric_compute_status`

## Purpose

Stores the latest computation attempt and failure reason per metric and security.

## Grain

One row per `(security_id, metric_id)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Row count: `4,887,360`
- Table size: `981,430,272 bytes` (`936.0 MiB`)
- Approximate bytes per row: `200.8`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `security_id` | `INTEGER` | no | PK | canonical identity link |
| `metric_id` | `TEXT` | no | PK, idx | metric identifier |
| `status` | `TEXT` | no | idx | success or failure state |
| `reason_code` | `TEXT` | yes |  | machine-readable failure category |
| `reason_detail` | `TEXT` | yes |  | human detail |
| `attempted_at` | `TEXT` | no |  | attempt timestamp |
| `value_as_of` | `TEXT` | yes |  | fact or metric as-of date |
| `facts_refreshed_at` | `TEXT` | yes |  | fact refresh watermark |
| `market_data_as_of` | `TEXT` | yes |  | market-data date used |
| `market_data_updated_at` | `TEXT` | yes |  | market-data write timestamp |

## Keys And Relationships

- Primary key: `(security_id, metric_id)`
- Logical references:
  - `security_id` to `securities`

## Secondary Indexes

- `idx_metric_compute_status_metric_status (metric_id, status)`

## Main Read Paths

- metric failure reports
- screen failure diagnostics
- freshness-aware metric reuse logic

## Main Write Paths

- `compute-metrics`
- purge when a listing becomes secondary

## Column Usage Notes

- `security_id`: canonical scope key for metric attempt rows.
- `metric_id`: main grouping/filter key for failure analysis.
- `status`: used heavily by failure reports and coverage summaries.
- `reason_code`: machine-readable failure bucket used in diagnostics.
- `reason_detail`: human-readable detail for failure reports.
- `attempted_at`: audit timestamp for the latest metric attempt.
- `value_as_of`: tells readers which fact/metric date the computation used.
- `facts_refreshed_at`: lets callers decide whether cached status is still valid relative to fact refreshes.
- `market_data_as_of`: same idea for market-data date.
- `market_data_updated_at`: same idea for market-data write freshness.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Sample window: first `5` rows returned by SQLite using `LIMIT` with no `ORDER BY`

```json
[
  {
    "security_id": 38347,
    "metric_id": "net_debt_to_ebitda",
    "status": "failure",
    "reason_code": "net_debt_to_ebitda: missing D&A for quarter 2025-12-31 (<symbol>)",
    "reason_detail": null,
    "attempted_at": "2026-04-17T18:24:20.971203+00:00",
    "value_as_of": null,
    "facts_refreshed_at": "2026-04-13T14:59:41.320561+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "security_id": 38364,
    "metric_id": "net_debt_to_ebitda",
    "status": "failure",
    "reason_code": "net_debt_to_ebitda: missing D&A for quarter 2025-12-31 (<symbol>)",
    "reason_detail": null,
    "attempted_at": "2026-04-17T18:24:36.075627+00:00",
    "value_as_of": null,
    "facts_refreshed_at": "2026-04-13T14:59:45.387404+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "security_id": 38367,
    "metric_id": "net_debt_to_ebitda",
    "status": "failure",
    "reason_code": "net_debt_to_ebitda: missing D&A for quarter 2025-09-30 (<symbol>)",
    "reason_detail": null,
    "attempted_at": "2026-04-17T18:24:38.575026+00:00",
    "value_as_of": null,
    "facts_refreshed_at": "2026-04-13T14:59:44.895002+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "security_id": 38375,
    "metric_id": "net_debt_to_ebitda",
    "status": "failure",
    "reason_code": "net_debt_to_ebitda: missing D&A for quarter 2025-09-30 (<symbol>)",
    "reason_detail": null,
    "attempted_at": "2026-04-17T18:24:24.248185+00:00",
    "value_as_of": null,
    "facts_refreshed_at": "2026-04-13T14:59:47.275271+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "security_id": 38380,
    "metric_id": "net_debt_to_ebitda",
    "status": "failure",
    "reason_code": "net_debt_to_ebitda: missing D&A for quarter 2025-09-30 (<symbol>)",
    "reason_detail": null,
    "attempted_at": "2026-04-17T18:24:30.114811+00:00",
    "value_as_of": null,
    "facts_refreshed_at": "2026-04-13T14:59:48.409774+00:00",
    "market_data_as_of": "2026-04-10",
    "market_data_updated_at": "2026-04-13T16:12:29.084722+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Stores only the latest attempt, not a full attempt history
- Review whether some freshness columns duplicate state already available in `metrics`, `financial_facts_refresh_state`, or `market_data`
