# `market_data`

## Purpose

Stores daily quote snapshots and market-cap data keyed by canonical security.

## Grain

One row per `(security_id, as_of)` snapshot date.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `223,034`
- Table size: `21,000,192 bytes` (`20.0 MiB`)
- Approximate bytes per row: `94.2`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `security_id` | `INTEGER` | no | PK | canonical identity link |
| `as_of` | `DATE` | no | PK, idx | snapshot date |
| `price` | `REAL` | no |  | stored trading price |
| `volume` | `INTEGER` | yes |  | trading volume |
| `market_cap` | `REAL` | yes |  | market cap snapshot |
| `currency` | `TEXT` | yes | partial idx | trading currency |
| `source_provider` | `TEXT` | no |  | provenance |
| `updated_at` | `TEXT` | no |  | write timestamp |

## Keys And Relationships

- Primary key: `(security_id, as_of)`
- Logical references:
  - `security_id` to `securities`

## Secondary Indexes

- `idx_market_data_latest (security_id, as_of DESC)`
- `idx_market_data_currency_nonnull (currency) WHERE currency IS NOT NULL`

## Main Read Paths

- latest market snapshot lookup for metrics
- staleness planning for market-data refresh
- FX currency discovery

## Main Write Paths

- `update-market-data`
- market-cap normalization helpers
- purge when a listing becomes secondary

## Column Usage Notes

- `security_id`: canonical join key used in latest-snapshot and metric reads.
- `as_of`: latest-row ordering and freshness cutoff column.
- `price`: direct input to price-based metrics and market-cap updates.
- `volume`: stored for snapshot completeness; not central to value metrics.
- `market_cap`: direct input to market-cap and enterprise-value logic.
- `currency`: trading currency used by metric currency validation and FX discovery.
- `source_provider`: provenance marker only.
- `updated_at`: freshness/audit timestamp used in status tracking.

## Review Notes

- `idx_market_data_latest` is essential because many call sites need only the newest row
- Review history retention policy: if snapshots accumulate indefinitely, this table can become a major scan target
