# `listing`

## Purpose

Stores canonical exchange-specific listing identity.

## Grain

One row per `(exchange_id, symbol)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: expected post-refactor split from pre-refactor `securities` rows on `2026-04-21`
- Row count: approximately `77,484`
- Table size: smaller than the old `securities` table because descriptive metadata moved to `issuer`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK | canonical listing surrogate key |
| `issuer_id` | `INTEGER` | no | FK | issuer metadata link |
| `exchange_id` | `INTEGER` | no | FK, unique, idx | canonical exchange link |
| `symbol` | `TEXT` | no | unique | bare canonical listing symbol such as `AAPL` |
| `currency` | `TEXT` | yes |  | catalog currency hint only |

## Keys And Relationships

- Primary key: `listing_id`
- Unique constraint: `(exchange_id, symbol)`
- Physical foreign keys:
  - `issuer_id -> issuer.issuer_id`
  - `exchange_id -> exchange.exchange_id`
- Physical references:
  - `provider_listing.listing_id`
  - `fundamentals_raw.listing_id`
  - `fundamentals_normalization_state.listing_id`
  - `financial_facts.listing_id`
  - `financial_facts_refresh_state.listing_id`
  - `market_data.listing_id`
  - `metrics.listing_id`
  - `metric_compute_status.listing_id`
  - `security_listing_status.listing_id`

## Secondary Indexes

- `idx_listing_exchange (exchange_id)`

## Main Read Paths

- canonical symbol resolution through `listing.symbol || '.' || exchange.exchange_code`
- downstream joins from facts, market data, metrics, and listing status

## Main Write Paths

- provider-listing refreshes
- raw fundamentals upserts that need to materialize a canonical listing
- migration-time backfill from legacy securities

## Review Notes

- Canonical user-facing symbols such as `AAPL.US` are derived, not stored.
- `market_data.currency` remains authoritative for metric arithmetic; `listing.currency` is only a catalog hint.
