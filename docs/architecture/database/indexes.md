# Indexes

This page lists the current secondary indexes from the post-refactor schema. Primary keys and table-level `UNIQUE` constraints are documented on each table page.

## Identity And Catalog

- `provider_exchange`
  - `idx_provider_exchange_exchange (exchange_id)`
    - supports joins from provider exchange codes to canonical exchange identity
- `listing`
  - `idx_listing_currency_nonnull (currency) WHERE currency IS NOT NULL`
    - narrows FX currency discovery and currency-scoped validation scans
  - Migration 067 dropped `idx_listing_exchange (exchange_id)` because the existing `UNIQUE (exchange_id, symbol)` auto-index already leads with `exchange_id`.
- `provider_listing`
  - `idx_provider_listing_listing (listing_id)`
    - supports canonical-listing joins back into provider rows
  - Migration 054 dropped both `provider_id` and `idx_provider_listing_provider`: the owning provider is reachable via `provider_exchange.provider_id` through `provider_exchange_id`.

## Raw Ingestion And State

- Migration 067 dropped `idx_fundamentals_raw_last_fetched`, `idx_fundamentals_fetch_next`, and `idx_market_data_fetch_next`. All three tables are reached through `provider_listing_catalog` joins and resolved via PK in the join plan; no scan uses `last_fetched_at` or `next_eligible_at` as the leading predicate.

## Canonical Analytics

- `financial_facts`
  - `idx_fin_facts_security_concept_latest (listing_id, concept, end_date DESC, filed DESC)`
    - critical latest-fact index for `compute-metrics` (pinned via `INDEXED BY` in three read paths)
  - `idx_fin_facts_currency_nonnull (currency) WHERE currency IS NOT NULL`
    - narrows FX discovery scans
  - Migration 052 dropped `idx_fin_facts_security_concept (listing_id, concept)` because `idx_fin_facts_security_concept_latest` already covers the same `(listing_id, concept, ...)` prefix.
  - Migration 067 dropped `idx_fin_facts_concept (concept)` because every read pairs `concept` with `listing_id`, so the `_latest` index handles all queries. The standalone index occupied ~3.2 GB on the live DB.
- `market_data`
  - Migration 067 dropped `idx_market_data_latest (listing_id, as_of DESC)` because the PK `(listing_id, as_of)` already supports descending-date traversal; SQLite picks the PK with no plan change.
- `metrics`
  - Migration 067 dropped `idx_metrics_metric_id (metric_id)` because every read filters by `(listing_id, metric_id)` matching the PK; no query was bare-`metric_id`.
- `metric_compute_status`
  - Migration 067 dropped `idx_metric_compute_status_metric_status (metric_id, status)` for the same reason as above.

## FX

- `fx_supported_pairs`
  - `idx_fx_supported_pairs_refreshable (provider, is_refreshable, canonical_symbol)`
    - supports provider-catalog refresh planning
- `fx_rates`
  - `idx_fx_rates_pair_date (provider, base_currency, quote_currency, rate_date DESC)`
    - critical pair/date lookup index for direct FX retrieval — the PK leads with `rate_date` after `provider`, so this is required for "latest by pair" probes.

## UNIQUE Indexes

- `issuer`
  - `idx_issuer_name_country (name, country)` — UNIQUE, added by migration 060 after deduplicating ~4,696 `(name, country)` groups (~13,121 rows collapsed; ~8,425 listings remapped). SQLite treats NULLs as distinct, so name-less or country-less rows do not collide with one another or with fully-populated rows.

## Review Notes

- Migration 067 removed eight secondary indexes (idx_fin_facts_concept, idx_metric_compute_status_metric_status, idx_metrics_metric_id, idx_market_data_latest, idx_fundamentals_raw_last_fetched, idx_market_data_fetch_next, idx_listing_exchange, idx_fundamentals_fetch_next) that the post-audit index review confirmed were unused or strictly covered by a PK / UNIQUE auto-index. Combined they reclaimed roughly 3.4 GB on disk and removed write amplification from the hottest write paths (`financial_facts` ingest, `metrics` and `metric_compute_status` rewrites).
- `sqlite_stat1` is currently empty — running `ANALYZE;` once after the migration applies gives the optimizer real statistics for the eight retained indexes.
