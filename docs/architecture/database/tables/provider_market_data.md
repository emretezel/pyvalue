# `provider_market_data`

## Purpose

Provider-layer price and volume observations: what each provider reported for
its own listing entity. The canonical, provider-free series the application
reads lives in [`market_data`](market_data.md) — this table is the market-data
counterpart of `provider_listing` in the provider/canonical split, and joins
the family of `provider_listing_id`-keyed provider data tables
(`fundamentals_raw`, `market_data_fetch_state`).

Provider identity is transitive via
`provider_listing -> provider_exchange -> provider`; the table deliberately
carries no `provider_id` column (the same redundancy migration 054 removed
from `provider_listing`).

## Grain

One row per `(provider_listing_id, as_of)` observation date.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Row count: `202,629`
- Table size: `13,475,840 bytes` (`12.9 MiB`)
- Approximate bytes per row: `66.5`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_listing_id` | `INTEGER` | no | PK, idx | provider listing that reported the observation |
| `as_of` | `DATE` | no | PK, idx | observation date |
| `price` | `REAL` | no |  | provider price, in the **major** currency (same convention as `market_data.price`) |
| `volume` | `INTEGER` | yes |  | provider volume |
| `updated_at` | `TEXT` | no |  | write timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`provider_listing_id`, `as_of`)
- Physical foreign keys:
  - `provider_listing_id` -> `provider_listing`.`provider_listing_id`
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `provider_listing_id` in `provider_listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- None at runtime today: with a single provider the canonical `market_data`
  mirror answers every read. The table exists for provenance and for future
  multi-provider arbitration (each provider's series retained separately;
  canonical holds the chosen view).
- The delisting purge deletes by `provider_listing_id`, served by the PK
  prefix.

## Main Write Paths

- `update-market-data` — `MarketDataRepository.upsert_prices` dual-writes: a
  row here (keyed by the `provider_listing_id` threaded from the eligibility
  query) plus the canonical `market_data` upsert, in one transaction. Rows
  whose update carries no `provider_listing_id` are canonical-only writes.
- `clear-market-data` wipes both layers together.
- `purge_provider_listing_rows` (the delisting/exchange-drop cascade) deletes
  this table's rows with the other provider-layer children; canonical
  `market_data` is retained.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Sample window: first `5` rows returned by SQLite ordered by `provider_listing_id ASC, as_of ASC`

```json
[
  {
    "provider_listing_id": 1,
    "as_of": "2026-03-20",
    "price": 30.02,
    "volume": 349376,
    "updated_at": "2026-04-02T14:21:31.509182+00:00"
  },
  {
    "provider_listing_id": 1,
    "as_of": "2026-04-02",
    "price": 30.02,
    "volume": 350816,
    "updated_at": "2026-04-06T12:14:35.451739+00:00"
  },
  {
    "provider_listing_id": 1,
    "as_of": "2026-04-10",
    "price": 32.26,
    "volume": 387867,
    "updated_at": "2026-04-13T16:12:29.084722+00:00"
  },
  {
    "provider_listing_id": 2,
    "as_of": "2026-03-20",
    "price": 26.43,
    "volume": 11551525,
    "updated_at": "2026-04-02T14:21:31.509182+00:00"
  },
  {
    "provider_listing_id": 2,
    "as_of": "2026-04-02",
    "price": 27.94,
    "volume": 1975088,
    "updated_at": "2026-04-06T12:14:34.283301+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Created and backfilled by **migration 081** from the pre-split
  `market_data.source_provider` tag, joining through `provider_listing`.
  Canonical-only rows (listings with no surviving provider mapping) were
  deliberately not backfilled — retaining their history is the canonical
  table's job.
- No secondary indexes: the PK serves both the dual-write conflict target and
  the purge-by-provider-listing pattern.
