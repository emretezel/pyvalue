# `fundamentals_normalization_state`

## Purpose

Tracks which raw payload timestamp was last normalized for each provider symbol.

## Grain

One row per `(provider, provider_symbol)`.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `61,092`
- Table size: `6,266,880 bytes` (`6.0 MiB`)
- Approximate bytes per row: `102.6`

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

## Review Notes

- Small, narrow watermark table
- Review whether `security_id` is necessary here or whether provider-symbol scope alone would be enough for all call sites
