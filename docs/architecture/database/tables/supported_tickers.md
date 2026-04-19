# `supported_tickers`

## Purpose

Stores the provider-published ticker catalog and maps provider symbols to canonical `security_id`.

## Grain

One row per `(provider, provider_symbol)`.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `75,848`
- Table size: `9,736,192 bytes` (`9.3 MiB`)
- Approximate bytes per row: `128.4`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider` | `TEXT` | no | PK | provider namespace |
| `provider_symbol` | `TEXT` | no | PK | provider fetch key such as `SHEL.LSE` |
| `provider_ticker` | `TEXT` | no | unique idx | provider display ticker inside one exchange |
| `provider_exchange_code` | `TEXT` | no | idx | provider exchange code |
| `security_id` | `INTEGER` | no | idx | canonical identity link |
| `listing_exchange` | `TEXT` | yes |  | provider listing exchange label |
| `security_name` | `TEXT` | yes |  | provider name |
| `security_type` | `TEXT` | yes |  | common stock, ETF, and so on |
| `country` | `TEXT` | yes |  | provider country |
| `currency` | `TEXT` | yes | partial idx | provider trading currency |
| `isin` | `TEXT` | yes |  | provider ISIN |
| `updated_at` | `TEXT` | no |  | refresh timestamp |

## Keys And Relationships

- Primary key: `(provider, provider_symbol)`
- Unique index: `(provider, provider_exchange_code, provider_ticker)`
- Logical references:
  - `provider_exchange_code` to `supported_exchanges`
  - `security_id` to `securities`
  - `(provider, provider_symbol)` reused by fetch/state/raw tables

## Secondary Indexes

- `idx_supported_tickers_provider_exchange (provider, provider_exchange_code)`
- `idx_supported_tickers_provider_exchange_ticker UNIQUE (provider, provider_exchange_code, provider_ticker)`
- `idx_supported_tickers_security (security_id)`
- `idx_supported_tickers_currency_nonnull (currency) WHERE currency IS NOT NULL`

## Main Read Paths

- scope resolution for ingest, market-data refresh, metrics, and screen runs
- exchange-scoped provider planning
- FX currency discovery

## Main Write Paths

- `refresh-supported-tickers`
- cleanup commands that remove deprecated provider rows

## Column Usage Notes

- `provider`: first filter in provider-scoped catalog, ingest, and market-data queries.
- `provider_symbol`: operational fetch key reused by raw payload and fetch-state tables.
- `provider_ticker`: uniqueness guard within one provider exchange and display ticker source.
- `provider_exchange_code`: hot filter for exchange-scoped batch planning.
- `security_id`: canonical link back to `securities` and downstream tables.
- `listing_exchange`: descriptive metadata; not a hot predicate.
- `security_name`: used in CLI display lists and fallback naming.
- `security_type`: descriptive metadata for catalog review.
- `country`: descriptive provider metadata with light read use.
- `currency`: used by FX currency discovery and trading-currency lookup helpers.
- `isin`: provenance metadata with light read use.
- `updated_at`: refresh watermark and provider row freshness marker.

## Review Notes

- This is the highest-value catalog table to review because many stages start here
- Check whether descriptive columns such as `listing_exchange`, `security_name`, `security_type`, `country`, and `isin` are all used enough to justify storing them here
- Performance depends heavily on `provider` plus `provider_exchange_code` slices staying cheap
