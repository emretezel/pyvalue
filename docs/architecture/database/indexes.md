# Indexes

This page lists the current secondary indexes from the live schema. Primary keys and table-level `UNIQUE` constraints are documented on each table page.

## Identity And Catalog

- `exchange_provider`
  - `idx_exchange_provider_exchange (exchange_id)`
    - supports joins from provider exchange codes to canonical exchange identity
- `securities`
  - `idx_securities_exchange (canonical_exchange_code)`
    - supports exchange-scoped symbol resolution
- `supported_tickers`
  - `idx_supported_tickers_provider_exchange (provider, provider_exchange_code)`
    - supports provider exchange slices during ingest and market-data planning
  - `idx_supported_tickers_provider_exchange_ticker UNIQUE (provider, provider_exchange_code, provider_ticker)`
    - protects per-exchange provider ticker uniqueness
  - `idx_supported_tickers_security (security_id)`
    - supports canonical-identity joins back into provider rows
  - `idx_supported_tickers_currency_nonnull (currency) WHERE currency IS NOT NULL`
    - narrows FX currency discovery scans

## Raw Ingestion And State

- `fundamentals_raw`
  - `idx_fundamentals_raw_security (security_id)`
    - supports canonical lookups from raw payloads
  - `idx_fundamentals_raw_provider_fetched (provider, fetched_at)`
    - supports staleness and reconciliation scans by provider
- `fundamentals_fetch_state`
  - `idx_fundamentals_fetch_next (provider, next_eligible_at)`
    - supports backoff scheduling
  - `idx_fundamentals_fetch_state_provider_fetched_symbol (provider, last_fetched_at, provider_symbol)`
    - supports stale/missing ingestion planning
  - `idx_fundamentals_fetch_state_provider_status_next_symbol (provider, last_status, next_eligible_at, provider_symbol)`
    - supports progress summaries and recent failure queries
- `fundamentals_normalization_state`
  - `idx_fundamentals_norm_state_security (security_id)`
    - supports canonical joins from normalization state
- `security_listing_status`
  - `idx_security_listing_status_primary (is_primary_listing, security_id)`
    - supports the primary-listing filter used across downstream scopes
- `market_data_fetch_state`
  - `idx_market_data_fetch_next (provider, next_eligible_at)`
    - supports market-data scheduling and backoff

## Canonical Analytics

- `financial_facts`
  - `idx_fin_facts_security_concept (security_id, concept)`
    - supports concept-scoped fact access
  - `idx_fin_facts_concept (concept)`
    - supports concept-wide scans and diagnostics
  - `idx_fin_facts_security_concept_latest (security_id, concept, end_date DESC, filed DESC)`
    - critical latest-fact index for `compute-metrics`
  - `idx_fin_facts_currency_nonnull (currency) WHERE currency IS NOT NULL`
    - narrows FX discovery scans
- `market_data`
  - `idx_market_data_latest (security_id, as_of DESC)`
    - critical latest-snapshot index for market-data reads and metrics
  - `idx_market_data_currency_nonnull (currency) WHERE currency IS NOT NULL`
    - narrows FX discovery scans
- `metrics`
  - `idx_metrics_metric_id (metric_id)`
    - supports metric-oriented scans across the universe
- `metric_compute_status`
  - `idx_metric_compute_status_metric_status (metric_id, status)`
    - supports failure reporting and metric coverage summaries

## FX

- `fx_supported_pairs`
  - `idx_fx_supported_pairs_refreshable (provider, is_refreshable, canonical_symbol)`
    - supports provider-catalog refresh planning
- `fx_rates`
  - `idx_fx_rates_pair_date (provider, base_currency, quote_currency, rate_date DESC)`
    - critical pair/date lookup index for direct FX retrieval

## Initial Index Review Questions

- Are both `idx_fundamentals_fetch_next` and `idx_fundamentals_fetch_state_provider_status_next_symbol` needed, or does one dominate the other?
- Does `idx_fin_facts_concept` justify its write cost, or are most reads already scoped by `security_id`?
- Is `idx_metrics_metric_id` enough for screening workloads, or would some metric-heavy reports benefit from `(metric_id, security_id)` ordering?
- Are the partial currency indexes still worth keeping if FX discovery is a minor share of runtime?
