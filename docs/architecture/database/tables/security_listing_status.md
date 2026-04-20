# `security_listing_status`

## Purpose

Caches whether an EODHD listing is primary or secondary once raw fundamentals are available.

## Grain

One row per `security_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Row count: `75,848`
- Table size: `9,912,320 bytes` (`9.5 MiB`)
- Approximate bytes per row: `130.7`
<!-- END generated_live_stats -->

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

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Sample window: first `5` rows returned by SQLite using `LIMIT` with no `ORDER BY`

```json
[
  {
    "security_id": 1,
    "source_provider": "EODHD",
    "provider_symbol": "AALB.AS",
    "raw_fetched_at": "2026-03-22T13:53:47.387172+00:00",
    "is_primary_listing": 1,
    "primary_provider_symbol": "AALB.AS",
    "classification_basis": "matched_primary_ticker",
    "updated_at": "2026-04-19T00:09:24.569416+00:00"
  },
  {
    "security_id": 2,
    "source_provider": "EODHD",
    "provider_symbol": "ABN.AS",
    "raw_fetched_at": "2026-03-22T13:53:47.613748+00:00",
    "is_primary_listing": 1,
    "primary_provider_symbol": "ABN.AS",
    "classification_basis": "matched_primary_ticker",
    "updated_at": "2026-04-19T00:09:25.284663+00:00"
  },
  {
    "security_id": 3,
    "source_provider": "EODHD",
    "provider_symbol": "ACOMO.AS",
    "raw_fetched_at": "2026-03-22T13:53:47.909077+00:00",
    "is_primary_listing": 1,
    "primary_provider_symbol": "ACOMO.AS",
    "classification_basis": "matched_primary_ticker",
    "updated_at": "2026-04-19T00:09:26.129977+00:00"
  },
  {
    "security_id": 4,
    "source_provider": "EODHD",
    "provider_symbol": "AD.AS",
    "raw_fetched_at": "2026-03-22T13:53:48.236603+00:00",
    "is_primary_listing": 1,
    "primary_provider_symbol": "AD.AS",
    "classification_basis": "matched_primary_ticker",
    "updated_at": "2026-04-19T00:09:26.484400+00:00"
  },
  {
    "security_id": 5,
    "source_provider": "EODHD",
    "provider_symbol": "ADYEN.AS",
    "raw_fetched_at": "2026-03-22T13:53:48.456762+00:00",
    "is_primary_listing": 1,
    "primary_provider_symbol": "ADYEN.AS",
    "classification_basis": "matched_primary_ticker",
    "updated_at": "2026-04-19T00:09:27.344101+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This table exists to keep downstream scopes index-friendly instead of reparsing JSON in `fundamentals_raw`
- Check whether the table should remain provider-specific or whether future providers could share the same abstraction
