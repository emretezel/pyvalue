# `market_data`

## Purpose

Stores price, volume, market-cap, and quote-currency snapshots for canonical listings.

## Grain

One row per `(listing_id, as_of)` snapshot date.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Row count: `223,034`
- Table size: `18,681,856 bytes` (`17.8 MiB`)
- Approximate bytes per row: `83.8`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK, idx | canonical listing identity |
| `as_of` | `DATE` | no | PK, idx | snapshot date |
| `price` | `REAL` | no |  | latest close or provider price |
| `volume` | `INTEGER` | yes |  | provider volume |
| `market_cap` | `REAL` | yes |  | market capitalization |
| `currency` | `TEXT` | yes | partial idx | quote-row currency for this price/market-cap snapshot |
| `source_provider` | `TEXT` | no |  | provenance |
| `updated_at` | `TEXT` | no |  | write timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`listing_id`, `as_of`)
- Physical foreign keys: none
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `listing_id` in `listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_market_data_currency_nonnull (currency)` WHERE currency IS NOT NULL
- `idx_market_data_latest (listing_id, as_of DESC)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- latest market data lookup for price and market-cap metrics
- market-cap recalculation
- FX currency discovery

## Main Write Paths

- `update-market-data`
- `recalc-market-cap`

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC, as_of ASC`

```json
[
  {
    "listing_id": 1,
    "as_of": "2026-03-20",
    "price": 30.02,
    "volume": 349376,
    "market_cap": 3288961180.0,
    "currency": "EUR",
    "source_provider": "EODHD",
    "updated_at": "2026-04-02T14:21:31.509182+00:00"
  },
  {
    "listing_id": 1,
    "as_of": "2026-04-02",
    "price": 30.02,
    "volume": 350816,
    "market_cap": 3288961180.0,
    "currency": "EUR",
    "source_provider": "EODHD",
    "updated_at": "2026-04-06T12:14:35.451739+00:00"
  },
  {
    "listing_id": 1,
    "as_of": "2026-04-10",
    "price": 32.26,
    "volume": 387867,
    "market_cap": 3515662540.0,
    "currency": "EUR",
    "source_provider": "EODHD",
    "updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "listing_id": 2,
    "as_of": "2026-03-20",
    "price": 26.43,
    "volume": 11551525,
    "market_cap": 21926592300.0,
    "currency": "EUR",
    "source_provider": "EODHD",
    "updated_at": "2026-04-02T14:21:31.509182+00:00"
  },
  {
    "listing_id": 2,
    "as_of": "2026-04-02",
    "price": 27.94,
    "volume": 1975088,
    "market_cap": 23179303400.0,
    "currency": "EUR",
    "source_provider": "EODHD",
    "updated_at": "2026-04-06T12:14:34.283301+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- `market_data.currency` describes the stored quote row. Listing-currency
  metadata comes from `provider_listing.currency` first, then
  `listing.currency`.
