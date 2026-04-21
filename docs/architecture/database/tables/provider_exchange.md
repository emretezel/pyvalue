# `provider_exchange`

## Purpose

Stores provider-published exchange catalogs and maps provider exchange codes to canonical exchange identity.

## Grain

One row per `(provider_id, provider_exchange_code)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` `exchange_provider` mapping on `2026-04-21`
- Row count: `74`
- Table size: carried forward from the old provider exchange mapping
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_exchange_id` | `INTEGER` | no | PK | surrogate key for provider listing FKs |
| `provider_id` | `INTEGER` | no | FK, unique | provider namespace |
| `provider_exchange_code` | `TEXT` | no | unique | provider-local exchange code |
| `exchange_id` | `INTEGER` | no | FK, idx | canonical exchange identity |
| `name` | `TEXT` | yes |  | provider display name |
| `country` | `TEXT` | yes |  | provider country label |
| `currency` | `TEXT` | yes |  | provider exchange-currency hint |
| `operating_mic` | `TEXT` | yes |  | MIC when supplied by the provider |
| `country_iso2` | `TEXT` | yes |  | normalized country code |
| `country_iso3` | `TEXT` | yes |  | normalized country code |
| `updated_at` | `TEXT` | no |  | last refresh timestamp |

## Keys And Relationships

- Primary key: `provider_exchange_id`
- Unique constraints:
  - `(provider_id, provider_exchange_code)`
  - `(provider_exchange_id, provider_id)` for provider-listing consistency checks
- Physical foreign keys:
  - `provider_id -> provider.provider_id`
  - `exchange_id -> exchange.exchange_id`
  - `provider_listing(provider_exchange_id, provider_id) -> provider_exchange(provider_exchange_id, provider_id)`

## Secondary Indexes

- `idx_provider_exchange_exchange (exchange_id)`

## Main Read Paths

- provider exchange-code resolution during provider-listing refreshes
- canonical exchange lookup for ingest and metadata helpers

## Main Write Paths

- `refresh-supported-exchanges`
- migration-time backfill from legacy exchange-provider rows

## Review Notes

- Provider-owned descriptive metadata belongs here, not on `exchange`.
- Slice replacement must stay cheap because EODHD refresh rewrites provider exchange catalogs.
