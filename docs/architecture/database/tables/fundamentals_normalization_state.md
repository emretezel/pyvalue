# `fundamentals_normalization_state`

## Purpose

Tracks which raw payload timestamp was last normalized for each provider symbol.

## Grain

One row per `(provider, provider_symbol)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Row count: `61,092`
- Table size: `6,266,880 bytes` (`6.0 MiB`)
- Approximate bytes per row: `102.6`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider` | `TEXT` | no | PK | provider namespace |
| `provider_symbol` | `TEXT` | no | PK | provider fetch key |
| `security_id` | `INTEGER` | no | idx | canonical identity link |
| `raw_fetched_at` | `TEXT` | no |  | raw payload timestamp last normalized |
| `last_normalized_at` | `TEXT` | no |  | normalization run timestamp |

## Keys And Relationships

- Primary key: `(provider, provider_symbol)`
- Logical references:
  - `security_id` to `securities`
  - `(provider, provider_symbol)` to `supported_tickers`

## Secondary Indexes

- `idx_fundamentals_norm_state_security (security_id)`

## Main Read Paths

- incremental normalization planning
- cleanup when a listing becomes secondary

## Main Write Paths

- `normalize-fundamentals`
- secondary-listing purge logic

## Column Usage Notes

- `provider`: provider namespace for the watermark row.
- `provider_symbol`: operational key used to skip already normalized raw payloads.
- `security_id`: canonical link used by cleanup and some scope joins.
- `raw_fetched_at`: compared against `fundamentals_raw.fetched_at` for incremental normalization.
- `last_normalized_at`: audit timestamp for the successful normalization pass.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Sample window: first `5` rows returned by SQLite using `LIMIT` with no `ORDER BY`

```json
[
  {
    "provider": "EODHD",
    "provider_symbol": "ADYEN.AS",
    "security_id": 5,
    "raw_fetched_at": "2026-03-22T13:53:48.456762+00:00",
    "last_normalized_at": "2026-04-13T13:51:54.204930+00:00"
  },
  {
    "provider": "EODHD",
    "provider_symbol": "ABN.AS",
    "security_id": 2,
    "raw_fetched_at": "2026-03-22T13:53:47.613748+00:00",
    "last_normalized_at": "2026-04-13T13:51:54.070234+00:00"
  },
  {
    "provider": "EODHD",
    "provider_symbol": "AALB.AS",
    "security_id": 1,
    "raw_fetched_at": "2026-03-22T13:53:47.387172+00:00",
    "last_normalized_at": "2026-04-13T13:51:55.370224+00:00"
  },
  {
    "provider": "EODHD",
    "provider_symbol": "ACOMO.AS",
    "security_id": 3,
    "raw_fetched_at": "2026-03-22T13:53:47.909077+00:00",
    "last_normalized_at": "2026-04-13T13:51:54.419968+00:00"
  },
  {
    "provider": "EODHD",
    "provider_symbol": "AJAX.AS",
    "security_id": 7,
    "raw_fetched_at": "2026-03-22T13:53:48.978815+00:00",
    "last_normalized_at": "2026-04-13T13:51:54.539664+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Small, narrow watermark table
- Review whether `security_id` is necessary here or whether provider-symbol scope alone would be enough for all call sites
