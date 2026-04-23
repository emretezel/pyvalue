# `provider`

## Purpose

Stores the global registry of external data-provider namespaces used elsewhere in the schema.

## Grain

One row per provider.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Row count: `3`
- Table size: `4,096 bytes` (`4.0 KiB`)
- Approximate bytes per row: `1,365.3`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_id` | `INTEGER` | no | PK | surrogate key used by catalog FKs |
| `provider_code` | `TEXT` | no | unique | stable uppercase namespace such as `EODHD`, `SEC`, or `FRANKFURTER` |
| `display_name` | `TEXT` | no |  | human-readable provider name |
| `description` | `TEXT` | yes |  | optional provider summary |
| `created_at` | `TEXT` | no |  | initial seed timestamp |
| `updated_at` | `TEXT` | no |  | last metadata refresh timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `provider_id`
- Physical foreign keys: none
- Physical references from other tables:
  - `provider_exchange`.`provider_id` -> `provider_id`
  - `provider_listing`.`provider_id` -> `provider_id`
- Unique constraints beyond the primary key:
  - `provider_code`
- Main logical refs: referenced physically by `provider_exchange` and `provider_listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- provider-code validation and id resolution before provider catalog writes
- human-readable provider display in diagnostics

## Main Write Paths

- migration-time seed and future registry maintenance

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Sample window: first `3` rows returned by SQLite ordered by `provider_id ASC`

```json
[
  {
    "provider_id": 1,
    "provider_code": "EODHD",
    "display_name": "EOD Historical Data",
    "description": "Exchange, fundamentals, market-data, and FX provider.",
    "created_at": "2026-04-23T16:33:15.427807+00:00",
    "updated_at": "2026-04-23T16:33:15.427807+00:00"
  },
  {
    "provider_id": 2,
    "provider_code": "FRANKFURTER",
    "display_name": "Frankfurter FX",
    "description": "FX rates provider used for direct currency history refreshes.",
    "created_at": "2026-04-23T16:33:15.427807+00:00",
    "updated_at": "2026-04-23T16:33:15.427807+00:00"
  },
  {
    "provider_id": 3,
    "provider_code": "SEC",
    "display_name": "US SEC Company Facts",
    "description": "US issuer fundamentals provider backed by SEC company facts.",
    "created_at": "2026-04-23T16:33:15.427807+00:00",
    "updated_at": "2026-04-23T16:33:15.427807+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Keep this table narrow. Runtime config, API keys, rate limits, and provider capabilities belong outside this registry or in separate capability tables.
