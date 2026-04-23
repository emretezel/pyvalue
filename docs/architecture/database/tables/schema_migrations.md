# `schema_migrations`

## Purpose

Tracks the schema migration version applied to the database.

## Grain

Append-only version rows, though operationally the table is expected to behave like a single latest-version record.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Row count: `1`
- Table size: `4,096 bytes` (`4.0 KiB`)
- Approximate bytes per row: `4,096.0`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `version` | `INTEGER` | no |  | applied schema version |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: none
- Physical foreign keys: none
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: none
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- migration bootstrap

## Main Write Paths

- migration runner

## Column Usage Notes

- `version`: compared by the migration bootstrap to determine which schema upgrades still need to run.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Sample window: first `1` rows returned by SQLite ordered by `version ASC`

```json
[
  {
    "version": 35
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Low priority for performance
- Check whether the table should enforce single-row semantics more explicitly
