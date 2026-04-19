# `fundamentals_fetch_state`

## Purpose

Tracks retry state, backoff windows, and last fetch status for fundamentals ingestion.

## Grain

One row per `(provider, provider_symbol)`.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `77,045`
- Table size: `4,698,112 bytes` (`4.5 MiB`)
- Approximate bytes per row: `61.0`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider` | `TEXT` | no | PK | provider namespace |
| `provider_symbol` | `TEXT` | no | PK | provider fetch key |
| `last_fetched_at` | `TEXT` | yes | idx | last successful or attempted fetch time |
| `last_status` | `TEXT` | yes | idx | typically success or error |
| `last_error` | `TEXT` | yes |  | latest provider error |
| `next_eligible_at` | `TEXT` | yes | idx | retry/backoff watermark |
| `attempts` | `INTEGER` | no |  | retry counter |

## Keys And Relationships

- Primary key: `(provider, provider_symbol)`
- Logical references:
  - `(provider, provider_symbol)` to `supported_tickers`

## Secondary Indexes

- `idx_fundamentals_fetch_next (provider, next_eligible_at)`
- `idx_fundamentals_fetch_state_provider_fetched_symbol (provider, last_fetched_at, provider_symbol)`
- `idx_fundamentals_fetch_state_provider_status_next_symbol (provider, last_status, next_eligible_at, provider_symbol)`

## Main Read Paths

- stale and missing ingest planning
- progress reporting
- recent failure reporting

## Main Write Paths

- `ingest-fundamentals`

## Column Usage Notes

- `provider`: first filter in retry and progress queries.
- `provider_symbol`: join key back to `supported_tickers`.
- `last_fetched_at`: used to distinguish missing vs stored rows and to identify stale rows.
- `last_status`: used in progress summaries and recent failure reporting.
- `last_error`: stored for diagnostics only.
- `next_eligible_at`: central backoff watermark used by scheduling queries.
- `attempts`: retry counter surfaced in diagnostics and backoff logic.

## Review Notes

- This is a narrow operational table and likely cheap to keep
- The main review question is whether all three scheduling/progress indexes are justified
