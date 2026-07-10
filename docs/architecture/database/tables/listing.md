# `listing`

## Purpose

Stores canonical exchange-specific listing identity and the authoritative
listing quote unit.

## Grain

One row per `(exchange_id, symbol)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-05`
- Row count: `75,847`
- Table size: `2,367,488 bytes` (`2.3 MiB`)
- Approximate bytes per row: `31.2`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK | canonical listing surrogate key |
| `issuer_id` | `INTEGER` | no | FK | issuer metadata link |
| `exchange_id` | `INTEGER` | no | FK, idx | canonical exchange link; part of composite unique key |
| `symbol` | `TEXT` | no |  | bare canonical listing symbol such as `AAPL`; part of composite unique key. CHECK enforces uppercase, no whitespace, and `[A-Z0-9.&^*-]` characters only |
| `currency` | `TEXT` | no |  | authoritative listing quote unit, including subunits such as `GBX`, `ZAC`, and `ILA`. NOT NULL since migration 069; CHECK enforces 3-char uppercase ASCII letters |
| `primary_listing_status` | `TEXT` | no |  | canonical primary-listing classification: `unknown`, `primary`, or `secondary` |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `listing_id`
- Physical foreign keys:
  - `exchange_id` -> `exchange`.`exchange_id`
  - `issuer_id` -> `issuer`.`issuer_id`
- Physical references from other tables:
  - `financial_facts`.`listing_id` -> `listing_id`
  - `financial_facts_refresh_state`.`listing_id` -> `listing_id`
  - `market_data`.`listing_id` -> `listing_id`
  - `metric_compute_status`.`listing_id` -> `listing_id`
  - `metrics`.`listing_id` -> `listing_id`
  - `provider_listing`.`listing_id` -> `listing_id`
- Unique constraints beyond the primary key:
  - (`exchange_id`, `symbol`)
- Main logical refs: canonical root for facts, prices, metrics, and primary-listing status
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- canonical-scope resolution joins `listing` to `exchange` and projects the
  canonical symbol `listing.symbol || '.' || exchange.exchange_code` as a display
  label only — never a filter/join key. Full / by-exchange scope scans the
  supported universe (`list_supported_listings`); an explicit `--symbols` request
  seeks only the requested rows (`list_supported_listings_for_symbols`: split the
  canonical symbol, seek `exchange` by `exchange_code`, then `listing` by the
  `(exchange_id, symbol)` UNIQUE index)
- downstream joins from facts, market data, metrics, and primary-listing status
- FX currency discovery and currency-scoped data checks

## Main Write Paths

- `refresh-supported-tickers` — the sole runtime writer of `listing` rows and
  of `listing.currency`
- migration-time backfill from legacy securities

`ingest-fundamentals` never writes here. It attaches each payload to a listing
that `refresh-supported-tickers` has already catalogued and skips any symbol
whose listing is absent (creating one would require writing the NOT NULL
`listing.currency`). Currency therefore has a single source of truth.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-07-05`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC`

```json
[
  {
    "listing_id": 1,
    "issuer_id": 1,
    "exchange_id": 1,
    "symbol": "AALB",
    "currency": "EUR",
    "primary_listing_status": "primary"
  },
  {
    "listing_id": 2,
    "issuer_id": 2,
    "exchange_id": 1,
    "symbol": "ABN",
    "currency": "EUR",
    "primary_listing_status": "primary"
  },
  {
    "listing_id": 3,
    "issuer_id": 3,
    "exchange_id": 1,
    "symbol": "ACOMO",
    "currency": "EUR",
    "primary_listing_status": "primary"
  },
  {
    "listing_id": 4,
    "issuer_id": 4,
    "exchange_id": 1,
    "symbol": "AD",
    "currency": "EUR",
    "primary_listing_status": "primary"
  },
  {
    "listing_id": 5,
    "issuer_id": 5,
    "exchange_id": 1,
    "symbol": "ADYEN",
    "currency": "EUR",
    "primary_listing_status": "primary"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Canonical user-facing symbols such as `AAPL.US` are derived, not stored.
- `listing.currency` is the only persisted listing-currency truth. It is a
  quote unit and is not collapsed to base currency at storage time. It is
  written solely by `refresh-supported-tickers`; fundamentals ingestion reads
  the catalog and never creates or mutates a listing's currency.
- Monetary normalization, market-cap calculations, FX discovery, and monetary
  metrics derive base currency from `listing.currency`.
- Unknown primary-listing status is treated as eligible; downstream
  primary-only scopes exclude only `secondary`.
- `primary_listing_status` is written only by `ingest-fundamentals` (as it
  stores each raw payload) and `reconcile-listing-status`; every other command
  reads it. A flip to `secondary` changes nothing but this column -- the
  listing keeps its facts/metrics/market-data and is excluded from universe
  work solely by the primary-only scope filters. Migration 078 is the one-time
  backfill that resolved any leftover `unknown` listing with stored
  fundamentals (it shipped with the eager purge that was policy at the time).
