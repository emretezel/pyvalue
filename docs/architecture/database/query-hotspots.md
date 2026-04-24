# Query Hotspots

This page maps the end-to-end pipeline to the tables and indexes that matter most for performance.

## 1. Refresh Supported Exchanges

- Reads provider payloads and rewrites provider slices in `provider_exchange`
- Upserts canonical rows in `exchange`
- Critical structures:
  - `provider_exchange` unique `(provider_id, provider_exchange_code)`
  - `idx_provider_exchange_exchange`
- Review focus:
  - full-slice replace cost is small today, but the FK from `provider_listing` means provider exchanges with listings cannot be removed blindly

## 2. Refresh Supported Tickers

- Reads provider catalog input and rewrites provider slices in `provider_listing`
- Reads and writes `listing`, `issuer`, and `provider_exchange`
- Critical structures:
  - `provider_listing` unique `(provider_exchange_id, provider_symbol)`
  - `listing` unique `(exchange_id, symbol)`
  - `idx_provider_listing_provider`
  - `idx_provider_listing_listing`
- Review focus:
  - `provider_listing` is the root table for provider-scoped work
  - bare provider symbols are only unique within one provider exchange

## 3. Ingest Fundamentals

- Reads `provider_listing`
- Reads and writes `fundamentals_fetch_state`
- Upserts `fundamentals_raw`
- Reconciles `security_listing_status`
- Critical structures:
  - `provider_listing` unique `(provider_exchange_id, provider_symbol)`
  - `fundamentals_raw` unique `provider_listing_id`
  - `idx_fundamentals_fetch_next`
  - `idx_security_listing_status_primary`
- Review focus:
  - `fundamentals_raw.data` is the widest operational row in the schema
  - provider-scoped state tables are now keyed by `provider_listing_id`, so planning queries often join through `provider_listing`

## 4. Normalize Fundamentals

- Reads `fundamentals_raw`
- Reads and writes `fundamentals_normalization_state`
- Rewrites canonical rows in `financial_facts`
- Updates `financial_facts_refresh_state`
- Critical structures:
  - `fundamentals_raw` unique `provider_listing_id`
  - `idx_fundamentals_norm_state_security`
  - `financial_facts` PK and `idx_fin_facts_security_concept_latest`
- Review focus:
  - `financial_facts` is the main fact table for metrics
  - unnecessary columns or duplicate fact rows compound downstream cost quickly

## 5. Refresh Issuer Metadata

- Reads `fundamentals_raw`
- Writes display fields on `issuer`
- Critical structures:
  - `fundamentals_raw` lookup by provider listing
  - `listing -> issuer` join
- Review focus:
  - issuer metadata is separate from listing identity; avoid pushing provider-specific fields into `listing`

## 6. Update Market Data

- Reads `provider_listing`
- Applies primary-listing filter through `security_listing_status`
- Reads and writes `market_data_fetch_state`
- Upserts `market_data`
- Critical structures:
  - `idx_provider_listing_listing`
  - `idx_security_listing_status_primary`
  - `idx_market_data_fetch_next`
  - `idx_market_data_latest`
- Review focus:
  - latest-snapshot queries should hit `idx_market_data_latest`
  - stale planning aggregates by `listing_id`, making `market_data` a hotspot on large universes

## 7. Refresh FX Rates

- Discovers currencies from `provider_listing`, `financial_facts`, and `market_data`
- Reads and writes `fx_supported_pairs`, `fx_refresh_state`, and `fx_rates`
- Critical structures:
  - partial currency indexes on `provider_listing`, `financial_facts`, and `market_data`
  - `idx_fx_supported_pairs_refreshable`
  - `fx_refresh_state` PK
  - `idx_fx_rates_pair_date`
- Review focus:
  - check whether the separate FX state tables are justified by the refresh algorithm complexity

## 8. Compute Metrics

- Resolves canonical symbols through `listing` plus `exchange`
- Bulk-reads `financial_facts`
- Bulk-reads latest `market_data` when required
- Writes `metrics` and `metric_compute_status`
- Critical structures:
  - `listing` unique `(exchange_id, symbol)`
  - `idx_fin_facts_security_concept_latest`
  - `idx_market_data_latest`
  - `metrics` PK `(listing_id, metric_id)`
  - `idx_metric_compute_status_metric_status`
- Review focus:
  - this is the hottest read path in the system
  - `financial_facts` row width and index fit dominate runtime more than almost any other table

## 9. Screen And Report

- Reads canonical symbol scope from `provider_listing`, `listing`, and `exchange`
- Reads `metrics`
- Reads `metric_compute_status` for diagnostics
- Critical structures:
  - `provider_listing` joins back to `listing`
  - `idx_security_listing_status_primary`
  - `metrics` PK
  - `idx_metrics_metric_id`
  - `idx_metric_compute_status_metric_status`
- Review focus:
  - screening depends on `metrics` being a compact cache; if the table grows wider or more history is added, read costs change materially

## First Places To Look For Bottlenecks

- `financial_facts`: largest analytical read surface and most index-sensitive query path
- `provider_listing`: root of many scope-building joins and provider-stage scans
- `market_data`: latest-row access pattern must remain cheap as history grows
- `fundamentals_raw`: JSON payload width can dominate I/O even when only a small subset of columns is needed
