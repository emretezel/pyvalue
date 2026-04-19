# Table Inventory

This page is the quickest way to inspect the current schema before going table by table.

All row counts and table sizes below come from the live `data/pyvalue.db` snapshot on `2026-04-19`. Sizes refer to the table object's own pages, not the size of its secondary indexes.

## Identity And Catalog

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [supported_exchanges](tables/supported_exchanges.md) | `74` | `12.0 KiB` | `(provider, provider_exchange_code)` | referenced by `supported_tickers.provider_exchange_code` | check whether provider metadata columns all earn their keep |
| [securities](tables/securities.md) | `77,484` | `77.5 MiB` | `security_id` | referenced logically by most downstream tables | check whether display metadata belongs here or in a separate cache |
| [supported_tickers](tables/supported_tickers.md) | `75,848` | `9.3 MiB` | `(provider, provider_symbol)` | links provider catalog rows to `security_id` | highest-priority catalog table; review duplicate metadata and scope indexes |

## Raw Ingestion And State

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [fundamentals_raw](tables/fundamentals_raw.md) | `77,045` | `16.84 GiB` | `(provider, provider_symbol)` | provider symbol in `supported_tickers`, `security_id` in `securities` | wide-row storage, JSON payload size, and latest-row-only semantics |
| [fundamentals_fetch_state](tables/fundamentals_fetch_state.md) | `77,045` | `4.5 MiB` | `(provider, provider_symbol)` | provider symbol in `supported_tickers` | retry/backoff query shape vs index set |
| [security_listing_status](tables/security_listing_status.md) | `75,848` | `9.5 MiB` | `security_id` | `security_id` in `securities` | primary-listing filter cost and purge trigger responsibilities |
| [fundamentals_normalization_state](tables/fundamentals_normalization_state.md) | `61,092` | `6.0 MiB` | `(provider, provider_symbol)` | provider symbol in `supported_tickers`, `security_id` in `securities` | whether this watermark table is minimal and sufficient |
| [market_data_fetch_state](tables/market_data_fetch_state.md) | `61,092` | `4.3 MiB` | `(provider, provider_symbol)` | provider symbol in `supported_tickers` | same pattern as fundamentals state; check duplication vs simplicity |

## Canonical Analytics

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [financial_facts](tables/financial_facts.md) | `103,188,287` | `8.68 GiB` | `(security_id, concept, fiscal_period, end_date, unit, accn)` | `security_id` in `securities` | hottest fact table; check row width, nullable PK parts, and latest-fact indexes |
| [financial_facts_refresh_state](tables/financial_facts_refresh_state.md) | `61,987` | `3.0 MiB` | `security_id` | `security_id` in `securities` | verify it still adds value beyond `fundamentals_normalization_state` |
| [market_data](tables/market_data.md) | `223,034` | `20.0 MiB` | `(security_id, as_of)` | `security_id` in `securities` | latest-snapshot access and time-series retention |
| [metrics](tables/metrics.md) | `2,422,916` | `158.9 MiB` | `(security_id, metric_id)` | `security_id` in `securities` | screen-read performance and lack of historical versions |
| [metric_compute_status](tables/metric_compute_status.md) | `4,887,360` | `936.0 MiB` | `(security_id, metric_id)` | `security_id` in `securities` | failure-report read shape and duplication with `metrics` freshness |

## FX

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [fx_supported_pairs](tables/fx_supported_pairs.md) | `990` | `100.0 KiB` | `(provider, symbol)` | canonical pair used by `fx_refresh_state` | alias vs canonical pair modeling |
| [fx_refresh_state](tables/fx_refresh_state.md) | `937` | `80.0 KiB` | `(provider, canonical_symbol)` | logical ref to canonical pairs in provider catalog | whether coverage state justifies a dedicated table |
| [fx_rates](tables/fx_rates.md) | `6,819,876` | `1.24 GiB` | `(provider, rate_date, base_currency, quote_currency)` | no enforced FK | largest FX table; pair/date access path and `rate_text` storage choice |

## Housekeeping

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [schema_migrations](tables/schema_migrations.md) | `1` | `4.0 KiB` | none; append-only version rows | none | low priority; check whether single-row semantics are guaranteed |
