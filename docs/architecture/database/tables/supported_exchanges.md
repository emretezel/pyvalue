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

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Sample window: first `5` rows returned by SQLite using `LIMIT` with no `ORDER BY`

```json
[
  {
    "provider": "EODHD",
    "provider_exchange_code": "AS",
    "canonical_exchange_code": "AS",
    "name": "Euronext Amsterdam",
    "country": "Netherlands",
    "currency": "EUR",
    "operating_mic": "XAMS",
    "country_iso2": "NL",
    "country_iso3": "NLD",
    "updated_at": "2026-03-22T10:57:47.052304+00:00"
  },
  {
    "provider": "EODHD",
    "provider_exchange_code": "AT",
    "canonical_exchange_code": "AT",
    "name": "Athens Exchange",
    "country": "Greece",
    "currency": "EUR",
    "operating_mic": "ASEX",
    "country_iso2": "GR",
    "country_iso3": "GRC",
    "updated_at": "2026-03-22T10:57:47.052304+00:00"
  },
  {
    "provider": "EODHD",
    "provider_exchange_code": "AU",
    "canonical_exchange_code": "AU",
    "name": "Australian Securities Exchange",
    "country": "Australia",
    "currency": "AUD",
    "operating_mic": "XASX",
    "country_iso2": "AU",
    "country_iso3": "AUS",
    "updated_at": "2026-03-22T10:57:47.052304+00:00"
  },
  {
    "provider": "EODHD",
    "provider_exchange_code": "BA",
    "canonical_exchange_code": "BA",
    "name": "Buenos Aires Exchange",
    "country": "Argentina",
    "currency": "ARS",
    "operating_mic": "XBUE",
    "country_iso2": "AR",
    "country_iso3": "ARG",
    "updated_at": "2026-03-22T10:57:47.052304+00:00"
  },
  {
    "provider": "EODHD",
    "provider_exchange_code": "BC",
    "canonical_exchange_code": "BC",
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

- Low write volume and low read volume
- Check whether all country and MIC columns are actually consumed in production workflows
