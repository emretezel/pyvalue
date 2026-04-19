# `supported_exchanges`

## Purpose

Caches provider-published exchange metadata such as code, country, currency, and MIC.

## Grain

One row per provider exchange code.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `74`
- Table size: `12,288 bytes` (`12.0 KiB`)
- Approximate bytes per row: `166.1`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider` | `TEXT` | no | PK | provider namespace such as `EODHD` |
| `provider_exchange_code` | `TEXT` | no | PK | provider-local exchange code |
| `canonical_exchange_code` | `TEXT` | no |  | canonical exchange code used across the app |
| `name` | `TEXT` | yes |  | provider display name |
| `country` | `TEXT` | yes |  | provider country label |
| `currency` | `TEXT` | yes |  | provider currency code |
| `operating_mic` | `TEXT` | yes |  | MIC when provided |
| `country_iso2` | `TEXT` | yes |  | normalized country code |
| `country_iso3` | `TEXT` | yes |  | normalized country code |
| `updated_at` | `TEXT` | no |  | last refresh timestamp |

## Keys And Relationships

- Primary key: `(provider, provider_exchange_code)`
- Logical references:
  - `supported_tickers.provider_exchange_code`
- No enforced foreign keys

## Secondary Indexes

- `idx_supported_exchanges_canonical (canonical_exchange_code)`
  - supports canonical exchange lookups

## Main Read Paths

- exchange catalog refresh reporting
- canonical exchange filtering when resolving provider exchange codes

## Main Write Paths

- `refresh-supported-exchanges`

## Column Usage Notes

- `provider`: namespace key used in refresh and lookup methods.
- `provider_exchange_code`: join/filter key when resolving provider exchange slices.
- `canonical_exchange_code`: consumed when mapping provider exchange codes into canonical scopes.
- `name`: display metadata only.
- `country`: descriptive metadata; not part of hot query predicates.
- `currency`: exchange-level provider currency hint; not a common join key.
- `operating_mic`: descriptive metadata for exchange identity review.
- `country_iso2`: normalized metadata only.
- `country_iso3`: normalized metadata only.
- `updated_at`: refresh watermark, mainly useful for debugging and catalog freshness checks.

## Review Notes

- Low write volume and low read volume
- Check whether all country and MIC columns are actually consumed in production workflows
