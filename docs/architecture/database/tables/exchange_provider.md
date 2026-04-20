# `exchange_provider`

## Purpose

Stores the provider-published exchange catalog and maps provider exchange codes to canonical exchange identity.

## Grain

One row per `(provider, provider_exchange_code)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Row count: `74`
- Table size: `12,288 bytes` (`12.0 KiB`)
- Approximate bytes per row: `166.1`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider` | `TEXT` | no | PK, FK | provider namespace such as `EODHD` or `SEC` |
| `provider_exchange_code` | `TEXT` | no | PK | provider-local exchange code |
| `exchange_id` | `INTEGER` | no | FK, idx | canonical exchange identity |
| `name` | `TEXT` | yes |  | provider display name |
| `country` | `TEXT` | yes |  | provider country label |
| `currency` | `TEXT` | yes |  | provider trading currency hint |
| `operating_mic` | `TEXT` | yes |  | MIC when supplied by the provider |
| `country_iso2` | `TEXT` | yes |  | normalized country code |
| `country_iso3` | `TEXT` | yes |  | normalized country code |
| `updated_at` | `TEXT` | no |  | last refresh timestamp |

## Keys And Relationships

- Primary key: `(provider, provider_exchange_code)`
- Physical references:
  - `provider` -> `providers.provider_code`
  - `exchange_id` -> `exchange.exchange_id`
- Logical references:
  - `(provider, provider_exchange_code)` is reused by `supported_tickers`

## Secondary Indexes

- `idx_exchange_provider_exchange (exchange_id)`
  - supports joins from provider exchange slices to canonical exchange identity

## Main Read Paths

- provider exchange-code resolution during supported-ticker refreshes
- canonical exchange lookup for provider-scoped ingest and metadata helpers

## Main Write Paths

- `refresh-supported-exchanges`
- migration-time backfill from legacy `supported_exchanges`

## Column Usage Notes

- `provider`: provider namespace and enforced link back to `providers`.
- `provider_exchange_code`: provider-scoped exchange filter used throughout catalog refresh workflows.
- `exchange_id`: canonical exchange link used to resolve `exchange.exchange_code`.
- `name`: provider-facing label only.
- `country`: provider-owned metadata.
- `currency`: provider exchange-currency hint; not the canonical trading-currency source for securities.
- `operating_mic`: descriptive exchange metadata for review and diagnostics.
- `country_iso2`: normalized metadata only.
- `country_iso3`: normalized metadata only.
- `updated_at`: provider catalog refresh watermark.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Sample window: first `5` rows returned by SQLite using `LIMIT` with no `ORDER BY`

```json
[
  {
    "provider": "EODHD",
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
    "provider": "EODHD",
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
    "provider": "EODHD",
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
    "provider": "EODHD",
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
    "provider": "EODHD",
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
- Slice replacement must stay cheap because EODHD refresh rewrites one provider's exchange catalog at a time.
