# `providers`

## Purpose

Stores the global registry of external data-provider namespaces used elsewhere in the schema.

## Grain

One row per provider code.

## Live Stats

<!-- BEGIN generated_live_stats -->
Pending the next live database docs refresh after the provider-registry migration is applied to a snapshot.
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
- Logical references:
  - provider-scoped tables keyed by `provider`
  - provenance columns such as `financial_facts.source_provider` and `market_data.source_provider`
- No enforced foreign keys

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
Pending the next live database docs refresh after the provider-registry migration is applied to a snapshot.
<!-- END generated_sample_rows -->

## Review Notes

- Keep this table narrow and stable; runtime config and provider capabilities belong elsewhere.
- Prefer referencing `providers(provider_code)` only when a table is already being rebuilt for another reason.
