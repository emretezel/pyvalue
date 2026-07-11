# `market_data`

## Purpose

Stores the canonical, provider-free price and volume series for canonical
listings. Provider provenance (which provider reported each observation, under
which provider listing) lives in the provider layer, [`provider_market_data`](provider_market_data.md)
— the same provider/canonical split as `provider_exchange`/`exchange` and
`provider_listing`/`listing`.

## Grain

One row per `(listing_id, as_of)` snapshot date.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Row count: `217,451`
- Table size: `14,438,400 bytes` (`13.8 MiB`)
- Approximate bytes per row: `66.4`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK, idx | canonical listing identity |
| `as_of` | `DATE` | no | PK, idx | snapshot date |
| `price` | `REAL` | no |  | latest close or provider price, in the **major** currency (`canonical_trading_currency(listing.currency)`) |
| `volume` | `INTEGER` | yes |  | provider volume |
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
  `MarketDataRepository.latest_snapshot_by_id` / `metrics.utils.market_cap_money`)

## Main Write Paths

- `update-market-data` — `MarketDataRepository.upsert_prices` dual-writes each
  observation: the provider layer row (`provider_market_data`) and this
  canonical row, in one transaction. Single provider today, so the canonical
  row simply adopts the observation; a future multi-provider priority rule
  slots into the canonical upsert.
- `clear-market-data` wipes both layers together.
- never deleted by catalog refreshes: canonical, provider-independent data is
  retained even when a listing loses its last provider mapping (2026-07-11
  design). The delisting purge removes only the `provider_market_data` rows.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC, as_of ASC`

```json
[
  {
    "listing_id": 1,
    "as_of": "2026-03-20",
    "price": 30.02,
    "volume": 349376,
    "updated_at": "2026-04-02T14:21:31.509182+00:00"
  },
  {
    "listing_id": 1,
    "as_of": "2026-04-02",
    "price": 30.02,
    "volume": 350816,
    "updated_at": "2026-04-06T12:14:35.451739+00:00"
  },
  {
    "listing_id": 1,
    "as_of": "2026-04-10",
    "price": 32.26,
    "volume": 387867,
    "updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "listing_id": 2,
    "as_of": "2026-03-20",
    "price": 26.43,
    "volume": 11551525,
    "updated_at": "2026-04-02T14:21:31.509182+00:00"
  },
  {
    "listing_id": 2,
    "as_of": "2026-04-02",
    "price": 27.94,
    "volume": 1975088,
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
  (`metrics.utils.market_cap_money`) rather than stored.
- The `source_provider` tag was **removed (migration 082)**: provenance moved
  to the provider layer when migration 081 created `provider_market_data`. All
  canonical readers were already id-keyed and never read the tag. 14,822 rows
  belonging to canonical-only listings (provider layer purged, e.g. the
  2026-07 plan-drop remnants) were deliberately kept canonical-only.
- Market-data rows do not persist a duplicate currency column.
