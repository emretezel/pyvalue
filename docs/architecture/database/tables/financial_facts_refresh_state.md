# `financial_facts_refresh_state`

## Purpose

Tracks when normalized financial facts were last refreshed for a canonical listing.

## Grain

One row per `listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` refresh-state table on `2026-04-21`
- Row count: `61,987`
- Table size: approximately `3.0 MiB` before the `listing_id` rename
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK | canonical listing identity |
| `refreshed_at` | `TEXT` | no |  | latest fact refresh timestamp |

## Keys And Relationships

- Primary key: `listing_id`
- Logical reference: `listing_id -> listing.listing_id`

## Secondary Indexes

- None.

## Main Read Paths

- metric freshness and failure-status writes
- refresh coverage reporting

## Main Write Paths

- `normalize-fundamentals`
- bulk normalization status updates

## Review Notes

- This table is intentionally narrow; consider whether it remains useful alongside `fundamentals_normalization_state`.
