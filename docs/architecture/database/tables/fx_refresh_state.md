# `fx_refresh_state`

## Purpose

Tracks refresh coverage and retry state per canonical FX pair.

## Grain

One row per `(provider, canonical_symbol)`.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `937`
- Table size: `81,920 bytes` (`80.0 KiB`)
- Approximate bytes per row: `87.4`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider` | `TEXT` | no | PK | provider namespace |
| `canonical_symbol` | `TEXT` | no | PK | canonical six-letter FX pair |
| `min_rate_date` | `TEXT` | yes |  | earliest stored date |
| `max_rate_date` | `TEXT` | yes |  | latest stored date |
| `full_history_backfilled` | `INTEGER` | no |  | whether the full backfill completed |
| `last_fetched_at` | `TEXT` | yes |  | last refresh timestamp |
| `last_status` | `TEXT` | yes |  | success or error |
| `last_error` | `TEXT` | yes |  | provider error |
| `attempts` | `INTEGER` | no |  | retry counter |

## Keys And Relationships

- Primary key: `(provider, canonical_symbol)`
- Logical references:
  - canonical pair identity from `fx_supported_pairs`

## Secondary Indexes

- None beyond the primary key

## Main Read Paths

- FX backfill planning
- incremental FX refresh planning

## Main Write Paths

- `refresh-fx-rates`

## Column Usage Notes

- `provider`: provider namespace for refresh-state rows.
- `canonical_symbol`: canonical pair identifier used in refresh planning.
- `min_rate_date`: earliest stored date used to assess historical coverage.
- `max_rate_date`: latest stored date used to assess current coverage.
- `full_history_backfilled`: key planning flag separating first-time backfills from incremental refreshes.
- `last_fetched_at`: latest refresh timestamp.
- `last_status`: success/error marker for the last attempt.
- `last_error`: diagnostic detail only.
- `attempts`: retry counter.

## Review Notes

- This table exists because FX refresh needs coverage state, not just last-attempt state
- Review whether the coverage columns are enough to justify a dedicated state table instead of deriving from `fx_rates`
