# Indexes

This page lists the current secondary indexes from the post-refactor schema. Primary keys and table-level `UNIQUE` constraints are documented on each table page.

## Identity And Catalog

- `provider_exchange`
  - `idx_provider_exchange_exchange (exchange_id)`
    - supports joins from provider exchange codes to canonical exchange identity
- `listing`
  - `idx_listing_exchange (exchange_id)`
    - supports exchange-scoped canonical listing resolution
  - `idx_listing_currency_nonnull (currency) WHERE currency IS NOT NULL`
    - narrows FX currency discovery and currency-scoped validation scans
- `provider_listing`
  - `idx_provider_listing_provider (provider_id)`
    - supports provider-scoped catalog scans
  - `idx_provider_listing_listing (listing_id)`
    - supports canonical-listing joins back into provider rows

## Raw Ingestion And State

- `fundamentals_raw`
  - `idx_fundamentals_raw_last_fetched (last_fetched_at)`
    - supports staleness and reconciliation scans
- `fundamentals_fetch_state`
  - `idx_fundamentals_fetch_next (next_eligible_at)`
    - supports active failure backoff scheduling
- `market_data_fetch_state`
  - `idx_market_data_fetch_next (next_eligible_at)`
    - supports market-data scheduling and backoff

## Canonical Analytics

- `financial_facts`
  - `idx_fin_facts_security_concept (listing_id, concept)`
    - supports concept-scoped fact access
  - `idx_fin_facts_concept (concept)`
    - supports concept-wide scans and diagnostics
  - `idx_fin_facts_security_concept_latest (listing_id, concept, end_date DESC, filed DESC)`
    - critical latest-fact index for `compute-metrics`
  - `idx_fin_facts_currency_nonnull (currency) WHERE currency IS NOT NULL`
    - narrows FX discovery scans
- `market_data`
  - `idx_market_data_latest (listing_id, as_of DESC)`
    - critical latest-snapshot index for market-data reads and metrics
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

- Do provider-scoped fetch-state queries now need additional indexes through joins to `provider_listing`, or is the narrower `next_eligible_at` index enough?
- Does `idx_fin_facts_concept` justify its write cost, or are most reads already scoped by `listing_id`?
- Is `idx_metrics_metric_id` enough for screening workloads, or would some metric-heavy reports benefit from `(metric_id, listing_id)` ordering?
- Is the `listing.currency` partial index still worth keeping if FX discovery is
  a minor share of runtime?
