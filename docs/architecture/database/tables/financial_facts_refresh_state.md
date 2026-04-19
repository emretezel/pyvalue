# `financial_facts_refresh_state`

## Purpose

Stores the latest time a security's canonical facts were refreshed.

## Grain

One row per `security_id`.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `61,987`
- Table size: `3,104,768 bytes` (`3.0 MiB`)
- Approximate bytes per row: `50.1`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `security_id` | `INTEGER` | no | PK | canonical identity link |
| `refreshed_at` | `TEXT` | no |  | last canonical fact refresh time |

## Keys And Relationships

- Primary key: `security_id`
- Logical references:
  - `security_id` to `securities`

## Secondary Indexes

- None beyond the primary key

## Main Read Paths

- metric freshness checks
- reporting around fact recency

## Main Write Paths

- `normalize-fundamentals`
- purge when a listing becomes secondary

## Column Usage Notes

- `security_id`: canonical join key for freshness checks.
- `refreshed_at`: compared in metric freshness logic and reused in diagnostics.

## Review Notes

- Very small table
- Review whether this table carries enough unique value compared with `fundamentals_normalization_state` to justify existing separately
