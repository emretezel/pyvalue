# `provider`

## Purpose

Stores the global registry of external data-provider namespaces used elsewhere in the schema.

## Grain

One row per provider.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` provider registry on `2026-04-21`
- Row count: `3`
- Table size: carried forward from the old `providers` registry
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

- Primary key: `provider_id`
- Unique constraint: `provider_code`
- Physical references:
  - `provider_exchange.provider_id`
  - `provider_listing.provider_id`
- Logical references:
  - provenance columns such as `financial_facts.source_provider` and `market_data.source_provider`

## Secondary Indexes

- None beyond the unique provider-code constraint.

## Main Read Paths

- provider-code validation and id resolution before provider catalog writes
- human-readable provider display in diagnostics

## Main Write Paths

- migration-time seed and future registry maintenance

## Review Notes

- Keep this table narrow. Runtime config, API keys, rate limits, and provider capabilities belong outside this registry or in separate capability tables.
