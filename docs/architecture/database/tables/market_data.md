# `market_data`

## Purpose

Stores price and volume snapshots for canonical listings.

## Grain

One row per `(listing_id, as_of)` snapshot date.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Row count: `221,186`
- Table size: `16,031,744 bytes` (`15.3 MiB`)
- Approximate bytes per row: `72.5`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK, idx | canonical listing identity |
| `as_of` | `DATE` | no | PK, idx | snapshot date |
| `price` | `REAL` | no |  | latest close or provider price, in the **major** currency (`canonical_trading_currency(listing.currency)`) |
| `volume` | `INTEGER` | yes |  | provider volume |
| `source_provider` | `TEXT` | no |  | provenance |
| `updated_at` | `TEXT` | no |  | write timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`listing_id`, `as_of`)
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

- latest price lookup for price-based metrics, including market cap on demand
  (latest share-count fact x latest price via
  `MarketDataRepository.latest_snapshot` / `metrics.utils.market_cap_money`)

## Main Write Paths

- `update-market-data`

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC, as_of ASC`

```json
[
  {
    "listing_id": 1,
    "as_of": "2026-03-20",
    "price": 30.02,
    "volume": 349376,
    "source_provider": "EODHD",
    "updated_at": "2026-04-02T14:21:31.509182+00:00"
  },
  {
    "listing_id": 1,
    "as_of": "2026-04-02",
    "price": 30.02,
    "volume": 350816,
    "source_provider": "EODHD",
    "updated_at": "2026-04-06T12:14:35.451739+00:00"
  },
  {
    "listing_id": 1,
    "as_of": "2026-04-10",
    "price": 32.26,
    "volume": 387867,
    "source_provider": "EODHD",
    "updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "listing_id": 2,
    "as_of": "2026-03-20",
    "price": 26.43,
    "volume": 11551525,
    "source_provider": "EODHD",
    "updated_at": "2026-04-02T14:21:31.509182+00:00"
  },
  {
    "listing_id": 2,
    "as_of": "2026-04-02",
    "price": 27.94,
    "volume": 1975088,
    "source_provider": "EODHD",
    "updated_at": "2026-04-06T12:14:34.283301+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- `market_data.price` is stored in the **major** currency
  (`canonical_trading_currency(listing.currency)`): subunit quotes
  (`GBX`/`ZAC`/`ILA`) are divided by their divisor before persistence
  (migration 070), so subunits never cross the data boundary.
- The derived `market_cap` column was **removed (migration 072)**: market cap is
  shares-outstanding x price, so it is computed on demand as the latest share-count
  `financial_facts` row x the latest `market_data` price
  (`metrics.utils.market_cap_money`) rather than stored. The generated Live
  Stats / Sample Rows above predate migration 072 and still show the column.
- Market-data rows do not persist a duplicate currency column.
