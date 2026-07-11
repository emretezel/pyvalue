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
  - `idx_provider_listing_listing`
- Review focus:
  - `provider_listing` is the root table for provider-scoped work
  - bare provider symbols are only unique within one provider exchange
  - migration 054 dropped `provider_listing.provider_id` and its supporting index; the owning provider is reached via `provider_exchange.provider_id` through `provider_exchange_id`

## 3. Ingest Fundamentals

- Reads `provider_listing`
- Reads and writes `fundamentals_fetch_state`
- Upserts `fundamentals_raw`
- Updates `listing.primary_listing_status`
- Critical structures:
  - `provider_listing` unique `(provider_exchange_id, provider_symbol)`
  - `fundamentals_raw` primary key `provider_listing_id`
  - `fundamentals_fetch_state` primary key `provider_listing_id` (the join driver — migration 067 dropped the `next_eligible_at` and `last_fetched_at` indexes since they were never picked)
- Review focus:
  - `fundamentals_raw.data` is the widest operational row in the schema
  - `fundamentals_fetch_state` stores only active failures; successful fetch progress is derived from raw payload presence and age

## 4. Normalize Fundamentals

- Reads `fundamentals_raw`
- Reads and writes `fundamentals_normalization_state`
- Rewrites canonical rows in `financial_facts`
- Updates `financial_facts_refresh_state`
- Critical structures:
  - `fundamentals_raw` primary key `provider_listing_id`
  - `fundamentals_raw.payload_hash`
  - `financial_facts` PK and `idx_fin_facts_security_concept_latest`
- Review focus:
  - `financial_facts` is the main fact table for metrics
  - normalization freshness compares payload hashes, not fetch timestamps
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
- Applies primary-listing filter through `listing.primary_listing_status`
- Reads and writes `market_data_fetch_state`
- Dual-upserts `provider_market_data` (keyed by the `provider_listing_id`
  threaded from the eligibility query) and canonical `market_data`, in one
  transaction
- Critical structures:
  - `idx_provider_listing_listing`
  - `market_data` primary key `(listing_id, as_of)` (latest-snapshot probes traverse the PK backwards; migration 067 dropped the redundant `idx_market_data_latest`)
  - `provider_market_data` primary key `(provider_listing_id, as_of)` (dual-write conflict target)
  - `market_data_fetch_state` primary key `provider_listing_id`
- Review focus:
  - latest-snapshot queries should resolve against the `market_data` PK in a single seek per scoped row
  - stale planning aggregates by `listing_id`, making `market_data` a hotspot on large universes

## 7. Refresh FX Rates

- Discovers currencies from `listing` and `financial_facts`
- Reads and writes `fx_supported_pairs` and `fx_refresh_state`; dual-upserts
  `provider_fx_rates` and canonical `fx_rates` in one transaction
- Critical structures:
  - partial currency indexes on `listing` and `financial_facts`
  - `idx_fx_supported_pairs_refreshable`
  - `fx_refresh_state` PK
  - `provider_fx_rates` PK `(provider_id, base, quote, rate_date)` (coverage
    seeks); `fx_rates` PK `(base, quote, rate_date)` (converter reads)
- Per-pair coverage cost:
  - one `pair_coverage` read per refreshable pair (to plan missing ranges),
    issued as split MIN/MAX index-endpoint seeks off the `provider_fx_rates`
    PK autoindex rather than a full-group scan
  - after each range upsert, coverage is widened in-process from the upserted
    batch's own dates, so there is no second coverage scan per range
- Review focus:
  - check whether the separate FX state tables are justified by the refresh algorithm complexity

## 8. Compute Metrics

- Resolves the canonical scope to `(listing_id, canonical_symbol)` pairs and carries the `listing_id` through every read and write (no symbol->id re-resolution):
  - full / by-exchange scope scans the supported universe (`list_supported_listings`: `provider_listing -> listing -> exchange`)
  - an explicit `--symbols` scope seeks only the requested tickers (`list_supported_listings_for_symbols`): `exchange` by `exchange_code`, then `listing` by `(exchange_id, symbol)` — never a whole-universe scan
- Bulk-reads `financial_facts`
- Bulk-reads latest `market_data` when required
- Writes `metrics` and `metric_compute_status`
- Critical structures:
  - `listing` unique `(exchange_id, symbol)` and `exchange` unique `exchange_code` (the targeted `--symbols` seek)
  - `idx_provider_listing_listing` (scope joins back into provider rows)
  - `idx_fin_facts_security_concept_latest`
  - `market_data` primary key `(listing_id, as_of)`
  - `metrics` PK `(listing_id, metric_id)`
  - `metric_compute_status` PK `(listing_id, metric_id)` (migration 067 dropped the `(metric_id, status)` secondary because every read matches the PK)
- Review focus:
  - this is the hottest read path in the system
  - `financial_facts` row width and index fit dominate runtime more than almost any other table

## 9. Screen And Report

- Resolves the canonical scope to `(listing_id, canonical_symbol)` pairs from `provider_listing`, `listing`, and `exchange`, then carries the `listing_id` into the metric/fact/market reads (same two scope modes as Compute Metrics: full/by-exchange scan vs targeted `--symbols` seek via `list_supported_listings_for_symbols`)
- Reads `metrics`
- Reads `metric_compute_status` for diagnostics
- Critical structures:
  - `provider_listing` joins back to `listing`; `listing (exchange_id, symbol)` + `exchange (exchange_code)` for the targeted `--symbols` seek
  - `metrics` PK `(listing_id, metric_id)` (migration 067 dropped the bare `metric_id` index since it was never picked)
  - `metric_compute_status` PK `(listing_id, metric_id)`
- Review focus:
  - screening depends on `metrics` being a compact cache; if the table grows wider or more history is added, read costs change materially

## First Places To Look For Bottlenecks

- `financial_facts`: largest analytical read surface and most index-sensitive query path
- `provider_listing`: root of many scope-building joins and provider-stage scans
- `market_data`: latest-row access pattern must remain cheap as history grows
- `fundamentals_raw`: JSON payload width can dominate I/O even when only a small subset of columns is needed
