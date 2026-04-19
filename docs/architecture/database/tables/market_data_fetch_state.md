# `market_data_fetch_state`

## Purpose

Tracks retry state, backoff windows, and last fetch status for market-data refreshes.

## Grain

One row per `(provider, provider_symbol)`.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `61,092`
- Table size: `4,476,928 bytes` (`4.3 MiB`)
- Approximate bytes per row: `73.3`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider` | `TEXT` | no | PK | provider namespace |
| `provider_symbol` | `TEXT` | no | PK | provider fetch key |
| `last_fetched_at` | `TEXT` | yes |  | last attempted market-data fetch |
| `last_status` | `TEXT` | yes |  | success or error |
| `last_error` | `TEXT` | yes |  | latest provider error |
| `next_eligible_at` | `TEXT` | yes | idx | retry/backoff watermark |
| `attempts` | `INTEGER` | no |  | retry counter |

## Keys And Relationships

- Primary key: `(provider, provider_symbol)`
- Logical references:
  - `(provider, provider_symbol)` to `supported_tickers`

## Secondary Indexes

- `idx_market_data_fetch_next (provider, next_eligible_at)`

## Main Read Paths

- market-data planning
- market-data progress reporting
- market-data failure reporting

## Main Write Paths

- `update-market-data`

## Column Usage Notes

- `provider`: first filter in market-data scheduling queries.
- `provider_symbol`: join key back to `supported_tickers`.
- `last_fetched_at`: used for progress reporting and freshness tracking.
- `last_status`: used when surfacing recent market-data failures.
- `last_error`: diagnostic detail only.
- `next_eligible_at`: core backoff watermark for market-data retry scheduling.
- `attempts`: retry counter for diagnostics and backoff progression.

## Review Notes

- Structurally similar to `fundamentals_fetch_state`
- Review whether keeping separate state tables is the simplest fast path, or whether a generic fetch-state table would reduce duplication without hurting clarity
