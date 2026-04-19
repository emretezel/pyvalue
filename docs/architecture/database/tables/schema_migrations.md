# `schema_migrations`

## Purpose

Tracks the schema migration version applied to the database.

## Grain

Append-only version rows, though operationally the table is expected to behave like a single latest-version record.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `1`
- Table size: `4,096 bytes` (`4.0 KiB`)
- Approximate bytes per row: `4,096.0`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `version` | `INTEGER` | no |  | applied schema version |

## Keys And Relationships

- No primary key
- No foreign keys

## Secondary Indexes

- None

## Main Read Paths

- migration bootstrap

## Main Write Paths

- migration runner

## Column Usage Notes

- `version`: compared by the migration bootstrap to determine which schema upgrades still need to run.

## Review Notes

- Low priority for performance
- Check whether the table should enforce single-row semantics more explicitly
