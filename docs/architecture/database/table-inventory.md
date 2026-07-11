# Table Inventory

This page is the quickest way to inspect the current schema before going table by table.

<!-- BEGIN generated_table_inventory -->
All row counts and table sizes below come from the live `data/pyvalue.db` snapshot on `2026-07-11`. Sizes refer to the table object's own pages, not the size of its secondary indexes.

## Identity And Catalog

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [provider](tables/provider.md) | `1` | `4.0 KiB` | `provider_id` | referenced physically by `provider_exchange` and `provider_listing` | keep the registry narrow and avoid leaking runtime config into it |
| [exchange](tables/exchange.md) | `73` | `12.0 KiB` | `exchange_id` | referenced physically by `provider_exchange.exchange_id` and `listing.exchange_id` | keep the canonical exchange table narrow and indexed for provider-catalog resolution |
| [provider_exchange](tables/provider_exchange.md) | `70` | `12.0 KiB` | `provider_exchange_id` | maps provider exchange codes to canonical exchange identity | check whether provider-owned exchange metadata belongs here and whether exchange-slice rewrites stay cheap |
| [issuer](tables/issuer.md) | `70,564` | `61.9 MiB` | `issuer_id` | referenced physically by `listing.issuer_id` | separate issuer metadata from listing identity and keep updates cheap |
| [listing](tables/listing.md) | `75,926` | `2.3 MiB` | `listing_id` | canonical root for facts, prices, metrics, and primary-listing status | maintain fast lookup by `(exchange_id, symbol)` and keep canonical status semantics clear |
| [provider_listing](tables/provider_listing.md) | `71,543` | `1.4 MiB` | `provider_listing_id` | links provider catalog rows to canonical `listing_id` | highest-priority provider catalog table; review provider slice rewrites and lookup indexes |

## Raw Ingestion And State

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [fundamentals_raw](tables/fundamentals_raw.md) | `71,543` | `15.88 GiB` | `provider_listing_id` | `provider_listing_id` in `provider_listing` | wide-row storage, JSON payload size, hash versioning, and latest-row-only semantics |
| [fundamentals_fetch_state](tables/fundamentals_fetch_state.md) | `0` | `4.0 KiB` | `provider_listing_id` | `provider_listing_id` in `provider_listing` | active retry/backoff rows only; success is derived from raw payloads |
| [fundamentals_normalization_state](tables/fundamentals_normalization_state.md) | `53,969` | `6.4 MiB` | `provider_listing_id` | `provider_listing_id` in `provider_listing` | payload-hash watermark minimality |
| [market_data_fetch_state](tables/market_data_fetch_state.md) | `53,969` | `2.6 MiB` | `provider_listing_id` | `provider_listing_id` in `provider_listing` | same pattern as fundamentals state; check duplication vs simplicity |
| [provider_market_data](tables/provider_market_data.md) | `202,629` | `12.9 MiB` | `provider_listing_id`, `as_of` | `provider_listing_id` in `provider_listing` | provider-layer price observations; purged with the provider listing while canonical `market_data` is retained |

## Canonical Analytics

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [financial_facts](tables/financial_facts.md) | `101,576,296` | `7.11 GiB` | `listing_id`, `concept`, `fiscal_period`, `end_date` | `listing_id` in `listing` | hottest fact table; check row width, nullable PK parts, and latest-fact indexes |
| [financial_facts_refresh_state](tables/financial_facts_refresh_state.md) | `58,135` | `2.5 MiB` | `listing_id` | `listing_id` in `listing` | verify it still adds value beyond `fundamentals_normalization_state` |
| [market_data](tables/market_data.md) | `217,451` | `13.8 MiB` | `listing_id`, `as_of` | `listing_id` in `listing` | canonical, provider-free price series (provenance in `provider_market_data`); latest-snapshot access and time-series retention |
| [metrics](tables/metrics.md) | `2,764,510` | `157.8 MiB` | `listing_id`, `metric_id` | `listing_id` in `listing` | screen-read performance and lack of historical versions |
| [metric_compute_status](tables/metric_compute_status.md) | `5,464,690` | `1.34 GiB` | `listing_id`, `metric_id` | `listing_id` in `listing` | status-survey read shape and duplication with `metrics` freshness; written only by `compute-metrics` |

## FX

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [fx_supported_pairs](tables/fx_supported_pairs.md) | `990` | `100.0 KiB` | `provider`, `symbol` | canonical pair used by `fx_refresh_state` | alias vs canonical pair modeling |
| [fx_refresh_state](tables/fx_refresh_state.md) | `937` | `80.0 KiB` | `provider`, `canonical_symbol` | logical ref to canonical pairs in provider catalog | whether coverage state justifies a dedicated table |
| [provider_fx_rates](tables/provider_fx_rates.md) | `6,306,705` | `869.5 MiB` | `provider_id`, `base_currency`, `quote_currency`, `rate_date` | `provider_id` in `provider` | provider-layer rate observations; coverage planning reads its PK endpoint seeks |
| [fx_rates](tables/fx_rates.md) | `6,306,705` | `415.3 MiB` | `base_currency`, `quote_currency`, `rate_date` | no enforced FK | largest FX table; canonical provider-free series, pair/date PK access path and REAL `rate` storage |

## Housekeeping

| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |
| --- | --- | --- | --- | --- | --- |
| [schema_migrations](tables/schema_migrations.md) | `1` | `4.0 KiB` | `id` | none | single-row by construction (migration 063: `id INTEGER PRIMARY KEY CHECK (id = 1)`); low review priority |
<!-- END generated_table_inventory -->
