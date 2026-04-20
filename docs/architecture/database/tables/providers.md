# `providers`

## Purpose

Stores the global registry of external data-provider namespaces used elsewhere in the schema.

## Grain

One row per provider code.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Row count: `3`
- Table size: `4,096 bytes` (`4.0 KiB`)
- Approximate bytes per row: `1,365.3`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_code` | `TEXT` | no | PK | stable uppercase provider namespace such as `EODHD` |
| `display_name` | `TEXT` | no |  | human-readable provider name |
| `description` | `TEXT` | yes |  | optional provider summary |
| `status` | `TEXT` | no |  | lifecycle state: `active`, `deprecated`, or `disabled` |
| `created_at` | `TEXT` | no |  | initial seed timestamp |
| `updated_at` | `TEXT` | no |  | last metadata refresh timestamp |

## Keys And Relationships

- Primary key: `provider_code`
- Physical references:
  - `exchange_provider.provider`
- Logical references:
  - most provider-scoped tables keyed by `provider`
  - provenance columns such as `financial_facts.source_provider` and `market_data.source_provider`
- No outbound foreign keys

## Secondary Indexes

- None

## Main Read Paths

- provider-code validation and future provider metadata joins
- human-readable provider display in reporting and debugging

## Main Write Paths

- migration-time seed and future registry maintenance

## Column Usage Notes

- `provider_code`: stable natural key used across the rest of the schema.
- `display_name`: readable label for CLI output, docs, and diagnostics.
- `description`: optional narrow metadata; avoid turning this table into runtime config storage.
- `status`: lifecycle switch for future provider deprecation handling.
- `created_at`: original insert timestamp.
- `updated_at`: last metadata maintenance timestamp.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Sample window: first `3` rows returned by SQLite using `LIMIT` with no `ORDER BY`

```json
[
  {
    "provider_code": "EODHD",
    "display_name": "EOD Historical Data",
    "description": "Exchange, fundamentals, market-data, and FX provider.",
    "status": "active",
    "created_at": "2026-04-20T19:42:44.073280+00:00",
    "updated_at": "2026-04-20T19:42:44.073280+00:00"
  },
  {
    "provider_code": "SEC",
    "display_name": "US SEC Company Facts",
    "description": "US issuer fundamentals provider backed by SEC company facts.",
    "status": "active",
    "created_at": "2026-04-20T19:42:44.073280+00:00",
    "updated_at": "2026-04-20T19:42:44.073280+00:00"
  },
  {
    "provider_code": "FRANKFURTER",
    "display_name": "Frankfurter FX",
    "description": "FX rates provider used for direct currency history refreshes.",
    "status": "active",
    "created_at": "2026-04-20T19:42:44.073280+00:00",
    "updated_at": "2026-04-20T19:42:44.073280+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Keep this table narrow and stable; runtime config and provider capabilities belong elsewhere.
- `exchange_provider` already references this registry physically; keep the rest of the schema on logical provider keys until a table is otherwise being rebuilt.
