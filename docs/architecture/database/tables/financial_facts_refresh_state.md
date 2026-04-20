# `financial_facts_refresh_state`

## Purpose

Stores the latest time a security's canonical facts were refreshed.

## Grain

One row per `security_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Row count: `61,987`
- Table size: `3,104,768 bytes` (`3.0 MiB`)
- Approximate bytes per row: `50.1`
<!-- END generated_live_stats -->

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

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Sample window: first `5` rows returned by SQLite using `LIMIT` with no `ORDER BY`

```json
[
  {
    "security_id": 1,
    "refreshed_at": "2026-04-13T13:51:55.355558+00:00"
  },
  {
    "security_id": 2,
    "refreshed_at": "2026-04-13T13:51:54.046069+00:00"
  },
  {
    "security_id": 3,
    "refreshed_at": "2026-04-13T13:51:54.401028+00:00"
  },
  {
    "security_id": 4,
    "refreshed_at": "2026-04-13T13:51:54.688817+00:00"
  },
  {
    "security_id": 5,
    "refreshed_at": "2026-04-13T13:51:54.185290+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Very small table
- Review whether this table carries enough unique value compared with `fundamentals_normalization_state` to justify existing separately
