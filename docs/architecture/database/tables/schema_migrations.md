# `schema_migrations`

## Purpose

Tracks the schema migration version applied to the database.

## Grain

Exactly one row, pinned to ``id = 1`` after migration 063. Every
``_set_version`` call replaces the row in place; the PK + CHECK make
duplicate or stray-id rows impossible.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Row count: `1`
- Table size: `4,096 bytes` (`4.0 KiB`)
- Approximate bytes per row: `4,096.0`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | no | PK | always 1; ``CHECK (id = 1)`` enforces the single-row invariant |
| `version` | `INTEGER` | no |  | applied schema version |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `id`
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
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Sample window: first `1` rows returned by SQLite ordered by `version ASC`

```json
[
  {
    "id": 1,
    "version": 73
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Single-row semantics are now enforced by the schema (``id INTEGER
  PRIMARY KEY CHECK (id = 1)``). ``_set_version`` uses
  ``DELETE FROM schema_migrations; INSERT INTO schema_migrations
  (version) VALUES (?)`` — SQLite auto-picks ``id = 1`` for the
  insert because the table is empty, so the CHECK passes.
