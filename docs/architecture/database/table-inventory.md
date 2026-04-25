# Table Inventory

This page is the quickest way to inspect the current schema before going table by table.

<!-- BEGIN generated_table_inventory -->
All row counts and table sizes below come from the live `data/pyvalue.db` snapshot on `2026-04-25`. Sizes refer to the table object's own pages, not the size of its secondary indexes.

## Identity And Catalog

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [provider](tables/provider.md) | `3` | `4.0 KiB` | `provider_id` | referenced physically by `provider_exchange` and `provider_listing` | keep the registry narrow and avoid leaking runtime config into it |
| [exchange](tables/exchange.md) | `73` | `12.0 KiB` | `exchange_id` | referenced physically by `provider_exchange.exchange_id` and `listing.exchange_id` | keep the canonical exchange table narrow and indexed for provider-catalog resolution |
| [provider_exchange](tables/provider_exchange.md) | `74` | `12.0 KiB` | `provider_exchange_id` | maps provider exchange codes to canonical exchange identity | check whether provider-owned exchange metadata belongs here and whether exchange-slice rewrites stay cheap |
| [issuer](tables/issuer.md) | `77,484` | `65.6 MiB` | `issuer_id` | referenced physically by `listing.issuer_id` | separate issuer metadata from listing identity and keep updates cheap |
| [listing](tables/listing.md) | `77,484` | `2.5 MiB` | `listing_id` | canonical root for facts, prices, metrics, and primary-listing status | maintain fast lookup by `(exchange_id, symbol)` and keep canonical status semantics clear |
| [provider_listing](tables/provider_listing.md) | `75,848` | `1.7 MiB` | `provider_listing_id` | links provider catalog rows to canonical `listing_id` | highest-priority provider catalog table; review provider slice rewrites and lookup indexes |

## Raw Ingestion And State

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [fundamentals_raw](tables/fundamentals_raw.md) | `75,848` | `16.63 GiB` | `payload_id` | `provider_listing_id` in `provider_listing` | wide-row storage, JSON payload size, and latest-row-only semantics |
| [fundamentals_fetch_state](tables/fundamentals_fetch_state.md) | `75,848` | `3.8 MiB` | `provider_listing_id` | `provider_listing_id` in `provider_listing` | retry/backoff query shape vs index set |
| [fundamentals_normalization_state](tables/fundamentals_normalization_state.md) | `61,092` | `4.9 MiB` | `provider_listing_id` | `provider_listing_id` in `provider_listing`, `listing_id` in `listing` | whether this watermark table is minimal and sufficient |
| [market_data_fetch_state](tables/market_data_fetch_state.md) | `61,092` | `3.0 MiB` | `provider_listing_id` | `provider_listing_id` in `provider_listing` | same pattern as fundamentals state; check duplication vs simplicity |

## Canonical Analytics

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [financial_facts](tables/financial_facts.md) | `103,188,287` | `8.55 GiB` | `listing_id`, `concept`, `fiscal_period`, `end_date`, `unit`, `accn` | `listing_id` in `listing` | hottest fact table; check row width, nullable PK parts, and latest-fact indexes |
| [financial_facts_refresh_state](tables/financial_facts_refresh_state.md) | `61,987` | `2.4 MiB` | `listing_id` | `listing_id` in `listing` | verify it still adds value beyond `fundamentals_normalization_state` |
| [market_data](tables/market_data.md) | `223,034` | `17.8 MiB` | `listing_id`, `as_of` | `listing_id` in `listing` | latest-snapshot access and time-series retention |
| [metrics](tables/metrics.md) | `2,422,916` | `130.4 MiB` | `listing_id`, `metric_id` | `listing_id` in `listing` | screen-read performance and lack of historical versions |
| [metric_compute_status](tables/metric_compute_status.md) | `4,887,360` | `881.7 MiB` | `listing_id`, `metric_id` | `listing_id` in `listing` | failure-report read shape and duplication with `metrics` freshness |

## FX

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [fx_supported_pairs](tables/fx_supported_pairs.md) | `990` | `100.0 KiB` | `provider`, `symbol` | canonical pair used by `fx_refresh_state` | alias vs canonical pair modeling |
| [fx_refresh_state](tables/fx_refresh_state.md) | `937` | `80.0 KiB` | `provider`, `canonical_symbol` | logical ref to canonical pairs in provider catalog | whether coverage state justifies a dedicated table |
| [fx_rates](tables/fx_rates.md) | `6,819,876` | `1.24 GiB` | `provider`, `rate_date`, `base_currency`, `quote_currency` | no enforced FK | largest FX table; pair/date access path and `rate_text` storage choice |

## Housekeeping

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [schema_migrations](tables/schema_migrations.md) | `1` | `4.0 KiB` | `none` | none | low priority; check whether single-row semantics are guaranteed |
<!-- END generated_table_inventory -->
