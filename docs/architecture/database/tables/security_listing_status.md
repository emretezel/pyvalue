# `security_listing_status`

## Purpose

Caches whether an EODHD listing is primary or secondary once raw fundamentals are available.

## Grain

One row per `security_id`.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `75,848`
- Table size: `9,912,320 bytes` (`9.5 MiB`)
- Approximate bytes per row: `130.7`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `security_id` | `INTEGER` | no | PK | canonical identity link |
| `source_provider` | `TEXT` | no |  | currently `EODHD` |
| `provider_symbol` | `TEXT` | no |  | listing whose raw payload was classified |
| `raw_fetched_at` | `TEXT` | no |  | raw payload timestamp used for classification |
| `is_primary_listing` | `INTEGER` | no | idx | `1` primary, `0` secondary |
| `primary_provider_symbol` | `TEXT` | yes |  | provider-reported primary listing symbol |
| `classification_basis` | `TEXT` | no |  | matched, different, or missing `PrimaryTicker` |
| `updated_at` | `TEXT` | no |  | classification timestamp |

## Keys And Relationships

- Primary key: `security_id`
- Logical references:
  - `security_id` to `securities`
  - `provider_symbol` to `supported_tickers`

## Secondary Indexes

- `idx_security_listing_status_primary (is_primary_listing, security_id)`

## Main Read Paths

- downstream scope filters for normalization, market data, FX discovery, metrics, and screening
- reconciliation of older raw payloads

## Main Write Paths

- EODHD raw fundamentals ingest
- `reconcile-listing-status`

## Column Usage Notes

- `security_id`: canonical filter key used in downstream primary-listing joins.
- `source_provider`: currently fixed to `EODHD`, but retained so the join stays provider-aware.
- `provider_symbol`: retained so purge logic can clear provider-symbol-scoped state.
- `raw_fetched_at`: reconciliation watermark that prevents reclassifying unchanged raw rows.
- `is_primary_listing`: the actual downstream filter bit used across normalization, market-data, FX, metrics, and screening scopes.
- `primary_provider_symbol`: audit trail for the provider-reported primary listing.
- `classification_basis`: explains why the row was classified as primary or secondary.
- `updated_at`: last classification timestamp.

## Review Notes

- This table exists to keep downstream scopes index-friendly instead of reparsing JSON in `fundamentals_raw`
- Check whether the table should remain provider-specific or whether future providers could share the same abstraction
