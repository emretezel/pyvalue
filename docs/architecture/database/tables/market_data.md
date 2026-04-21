# `market_data`

## Purpose

Stores price, volume, market-cap, and trading-currency snapshots for canonical listings.

## Grain

One row per `(listing_id, as_of)` snapshot date.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` market-data table on `2026-04-21`
- Row count: `223,034`
- Table size: approximately `20.0 MiB` before the `listing_id` rename
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK, idx | canonical listing identity |
| `as_of` | `DATE` | no | PK, idx | snapshot date |
| `price` | `REAL` | no |  | latest close or provider price |
| `volume` | `INTEGER` | yes |  | provider volume |
| `market_cap` | `REAL` | yes |  | market capitalization |
| `currency` | `TEXT` | yes | partial idx | authoritative trading currency for metric arithmetic |
| `source_provider` | `TEXT` | no |  | provenance |
| `updated_at` | `TEXT` | no |  | write timestamp |

## Keys And Relationships

- Primary key: `(listing_id, as_of)`
- Logical reference: `listing_id -> listing.listing_id`

## Secondary Indexes

- `idx_market_data_latest (listing_id, as_of DESC)`
- `idx_market_data_currency_nonnull (currency) WHERE currency IS NOT NULL`

## Main Read Paths

- latest market data lookup for metrics
- market-cap recalculation
- FX currency discovery

## Main Write Paths

- `update-market-data`
- `recalc-market-cap`

## Review Notes

- `market_data.currency` is authoritative for price and market-cap arithmetic; catalog currencies are hints only.
