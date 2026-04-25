# `provider_exchange`

## Purpose

Stores provider-published exchange catalogs and maps provider exchange codes to canonical exchange identity.

## Grain

One row per `(provider_id, provider_exchange_code)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-25`
- Row count: `74`
- Table size: `12,288 bytes` (`12.0 KiB`)
- Approximate bytes per row: `166.1`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_exchange_id` | `INTEGER` | no | PK | surrogate key for provider listing FKs |
| `provider_id` | `INTEGER` | no | FK | provider namespace; part of composite unique keys |
| `provider_exchange_code` | `TEXT` | no |  | provider-local exchange code; part of composite unique key |
| `exchange_id` | `INTEGER` | no | FK, idx | canonical exchange identity |
| `name` | `TEXT` | yes |  | provider display name |
| `country` | `TEXT` | yes |  | provider country label |
| `currency` | `TEXT` | yes |  | provider exchange-currency hint |
| `operating_mic` | `TEXT` | yes |  | MIC when supplied by the provider |
| `country_iso2` | `TEXT` | yes |  | normalized country code |
| `country_iso3` | `TEXT` | yes |  | normalized country code |
| `updated_at` | `TEXT` | no |  | last refresh timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `provider_exchange_id`
- Physical foreign keys:
  - `exchange_id` -> `exchange`.`exchange_id`
  - `provider_id` -> `provider`.`provider_id`
- Physical references from other tables:
  - `provider_listing`.`provider_exchange_id` -> `provider_exchange_id`
  - `provider_listing`.(`provider_exchange_id`, `provider_id`) -> (`provider_exchange_id`, `provider_id`)
- Unique constraints beyond the primary key:
  - (`provider_exchange_id`, `provider_id`)
  - (`provider_id`, `provider_exchange_code`)
- Main logical refs: maps provider exchange codes to canonical exchange identity
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_provider_exchange_exchange (exchange_id)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- provider exchange-code resolution during provider-listing refreshes
- canonical exchange lookup for ingest and metadata helpers

## Main Write Paths

- `refresh-supported-exchanges`
- migration-time backfill from legacy exchange-provider rows

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-25`
- Sample window: first `5` rows returned by SQLite ordered by `provider_exchange_id ASC`

```json
[
  {
    "provider_exchange_id": 1,
    "provider_id": 1,
    "provider_exchange_code": "AS",
    "exchange_id": 1,
    "name": "Euronext Amsterdam",
    "country": "Netherlands",
    "currency": "EUR",
    "operating_mic": "XAMS",
    "country_iso2": "NL",
    "country_iso3": "NLD",
    "updated_at": "2026-03-22T10:57:47.052304+00:00"
  },
  {
    "provider_exchange_id": 2,
    "provider_id": 1,
    "provider_exchange_code": "AT",
    "exchange_id": 2,
    "name": "Athens Exchange",
    "country": "Greece",
    "currency": "EUR",
    "operating_mic": "ASEX",
    "country_iso2": "GR",
    "country_iso3": "GRC",
    "updated_at": "2026-03-22T10:57:47.052304+00:00"
  },
  {
    "provider_exchange_id": 3,
    "provider_id": 1,
    "provider_exchange_code": "AU",
    "exchange_id": 3,
    "name": "Australian Securities Exchange",
    "country": "Australia",
    "currency": "AUD",
    "operating_mic": "XASX",
    "country_iso2": "AU",
    "country_iso3": "AUS",
    "updated_at": "2026-03-22T10:57:47.052304+00:00"
  },
  {
    "provider_exchange_id": 4,
    "provider_id": 1,
    "provider_exchange_code": "BA",
    "exchange_id": 4,
    "name": "Buenos Aires Exchange",
    "country": "Argentina",
    "currency": "ARS",
    "operating_mic": "XBUE",
    "country_iso2": "AR",
    "country_iso3": "ARG",
    "updated_at": "2026-03-22T10:57:47.052304+00:00"
  },
  {
    "provider_exchange_id": 5,
    "provider_id": 1,
    "provider_exchange_code": "BC",
    "exchange_id": 5,
    "name": "Casablanca Stock Exchange",
    "country": "Morocco",
    "currency": "MAD",
    "operating_mic": "XCAS",
    "country_iso2": "MA",
    "country_iso3": "MAR",
    "updated_at": "2026-03-22T10:57:47.052304+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Provider-owned descriptive metadata belongs here, not on `exchange`.
- Slice replacement must stay cheap because EODHD refresh rewrites provider exchange catalogs.
