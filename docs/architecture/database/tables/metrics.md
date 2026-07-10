# `metrics`

## Purpose

Stores the latest computed metric value per canonical listing and metric.

## Grain

One row per `(listing_id, metric_id)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Row count: `2,902,598`
- Table size: `166,232,064 bytes` (`158.5 MiB`)
- Approximate bytes per row: `57.3`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK | canonical listing identity |
| `metric_id` | `TEXT` | no | PK, idx | metric identifier |
| `value` | `REAL` | no |  | computed metric value |
| `as_of` | `TEXT` | no |  | metric value date |
| `unit_kind` | `TEXT` | no |  | metric unit category. Row-level CHECK enforces consistency with `currency`: `unit_kind = 'monetary'` requires a non-NULL `currency`; any other `unit_kind` (e.g. `'ratio'`, `'count'`) requires NULL `currency`. |
| `currency` | `TEXT` | yes |  | metric currency when monetary; NULL for non-monetary metrics (enforced by the `unit_kind` row-level CHECK above) |
| `unit_label` | `TEXT` | yes |  | display unit label |

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

- screen and report queries by metric id
- per-symbol metric lookup

## Main Write Paths

- `compute-metrics`
- bulk metric recomputation
- delisting purge in `refresh-supported-tickers` (deletes a fully delisted
  listing's rows)

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC, metric_id ASC`

```json
[
  {
    "listing_id": 1,
    "metric_id": "accruals_ratio",
    "value": -0.0669090228991572,
    "as_of": "2025-12-31",
    "unit_kind": "ratio",
    "currency": null,
    "unit_label": null
  },
  {
    "listing_id": 1,
    "metric_id": "altman_z",
    "value": 2.649516706136721,
    "as_of": "2025-12-31",
    "unit_kind": "other",
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
    "value": 4.226606538895152,
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
- Migration 041 added the `listing_id` FK, made `value` NOT NULL, and added a row-level CHECK pairing `unit_kind` with `currency` (monetary metrics must carry a currency; ratio/count metrics must not).
