# Query Hotspots

This page maps the end-to-end pipeline to the tables and indexes that matter most for performance.

## 1. Refresh Supported Exchanges

- Reads provider payloads and rewrites provider slices in `exchange_provider`
- Upserts canonical rows in `exchange`
- Main concerns:
  - full-table replace cost is usually small
  - canonical exchange upserts should stay cheap because the table is intentionally narrow

## 2. Refresh Supported Tickers

- Reads provider catalog input and rewrites provider slices in `supported_tickers`
- Reads and writes `securities` to maintain canonical identity
- Critical structures:
  - `supported_tickers` PK `(provider, provider_symbol)`
  - `idx_supported_tickers_provider_exchange`
  - `idx_supported_tickers_provider_exchange_ticker`
  - `securities` unique constraints on canonical symbol and ticker/exchange
- Review focus:
  - `supported_tickers` is the root table for most later provider-scoped work
  - duplicated descriptive columns here can increase write cost and DB size

## 3. Ingest Fundamentals

- Reads `supported_tickers`
- Reads and writes `fundamentals_fetch_state`
- Upserts `fundamentals_raw`
- Reconciles `security_listing_status`
- Critical structures:
  - `idx_supported_tickers_provider_exchange`
  - `idx_fundamentals_fetch_state_provider_fetched_symbol`
  - `idx_fundamentals_fetch_state_provider_status_next_symbol`
  - `idx_fundamentals_raw_provider_fetched`
  - `idx_security_listing_status_primary`
- Review focus:
  - `fundamentals_raw.data` is the widest operational row in the schema
  - listing-status reconciliation should stay join-friendly and avoid JSON parsing in downstream scopes

## 4. Normalize Fundamentals

- Reads `fundamentals_raw`
- Reads and writes `fundamentals_normalization_state`
- Rewrites canonical rows in `financial_facts`
- Updates `financial_facts_refresh_state`
- Critical structures:
  - `fundamentals_raw` PK and `idx_fundamentals_raw_security`
  - `idx_fundamentals_norm_state_security`
  - `financial_facts` PK and `idx_fin_facts_security_concept_latest`
- Review focus:
  - `financial_facts` is the main fact table for metrics
  - unnecessary columns or duplicate fact rows will compound downstream cost quickly

## 5. Refresh Security Metadata

- Reads `fundamentals_raw`
- Writes display fields on `securities`
- Critical structures:
  - `fundamentals_raw` PK by provider symbol
  - `securities` PK and unique canonical symbol
- Review focus:
  - check whether `sector` and `industry` on `securities` are authoritative enough to belong on the identity table

## 6. Update Market Data

- Reads `supported_tickers`
- Applies primary-listing filter through `security_listing_status`
- Reads and writes `market_data_fetch_state`
- Upserts `market_data`
- Critical structures:
  - `idx_supported_tickers_provider_exchange`
  - `idx_security_listing_status_primary`
  - `idx_market_data_fetch_next`
  - `idx_market_data_latest`
- Review focus:
  - latest-snapshot queries should hit `idx_market_data_latest`
  - stale planning currently aggregates `MAX(as_of)` by `security_id`, which makes `market_data` a hotspot on large universes

## 7. Refresh FX Rates

- Discovers currencies from `supported_tickers`, `financial_facts`, and `market_data`
- Reads and writes `fx_supported_pairs`, `fx_refresh_state`, and `fx_rates`
- Critical structures:
  - partial currency indexes on `supported_tickers`, `financial_facts`, `market_data`
  - `idx_fx_supported_pairs_refreshable`
  - `fx_refresh_state` PK
  - `idx_fx_rates_pair_date`
- Review focus:
  - check whether the separate FX state tables are justified by the refresh algorithm complexity

## 8. Compute Metrics

- Resolves canonical symbols through `securities`
- Bulk-reads `financial_facts`
- Bulk-reads latest `market_data` when required
- Writes `metrics` and `metric_compute_status`
- Critical structures:
  - `idx_fin_facts_security_concept_latest`
  - `idx_market_data_latest`
  - `metrics` PK `(security_id, metric_id)`
  - `idx_metric_compute_status_metric_status`
- Review focus:
  - this is the hottest read path in the system
  - `financial_facts` row width and index fit dominate runtime more than almost any other table

## 9. Screen And Report

- Reads canonical symbol scope from `supported_tickers` plus `securities`
- Reads `metrics`
- Reads `metric_compute_status` for diagnostics
- Critical structures:
  - `supported_tickers` joins back to `securities`
  - `idx_security_listing_status_primary`
  - `metrics` PK
  - `idx_metrics_metric_id`
  - `idx_metric_compute_status_metric_status`
- Review focus:
  - screening depends on `metrics` being a compact cache; if the table grows wider or more history is added, read costs change materially

## First Places To Look For Bottlenecks

- `financial_facts`: largest analytical read surface and most index-sensitive query path
- `supported_tickers`: root of many scope-building joins and provider-stage scans
- `market_data`: latest-row access pattern must remain cheap as history grows
- `fundamentals_raw`: JSON payload width can dominate I/O even when only a small subset of columns is needed
