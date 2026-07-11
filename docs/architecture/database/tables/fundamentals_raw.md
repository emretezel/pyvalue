# `fundamentals_raw`

## Purpose

Stores the latest raw fundamentals payload for each provider listing. For how this payload is
normalized into `financial_facts` concepts (which sections/fields are read and how each fact is
built), see [EODHD Concept Normalization](../../../reference/eodhd-concept-normalization.md).

## Grain

One row per `provider_listing_id`; historical payload versions are not retained.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Row count: `75,847`
- Table size: `17,856,688,128 bytes` (`16.63 GiB`)
- Approximate bytes per row: `235,430.4`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_listing_id` | `INTEGER` | no | PK, FK | provider listing identity |
| `data` | `TEXT` | no |  | raw JSON payload |
| `payload_hash` | `TEXT` | no |  | SHA-256 hash of canonical raw JSON |
| `last_fetched_at` | `TEXT` | no | idx | latest fetch timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `provider_listing_id`
- Physical foreign keys:
  - `provider_listing_id` -> `provider_listing`.`provider_listing_id`
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `provider_listing_id` in `provider_listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_fundamentals_raw_last_fetched (last_fetched_at)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- provider-scoped payload lookup through `provider_listing_id`
- canonical listing lookup by joining through `provider_listing`
- stale-eligibility scan filtering/ordering on `last_fetched_at`
  (`idx_fundamentals_raw_last_fetched`)
- issuer metadata refresh and primary-listing reconciliation from stored raw payloads
- payload-hash comparison for incremental normalization

## Main Write Paths

- `ingest-fundamentals`
- migration-time backfill from legacy `(provider, provider_symbol)` raw rows
- provider-layer prune: rows die with their `provider_listing` — the ticker
  refresh (removed tickers) and the dropped-venue cascade in
  `refresh-supported-exchanges` delete them

## Sample Rows

<!-- BEGIN generated_sample_rows -->
Wide-table sample rows live in the [Sample Rows appendix](../sample-rows.md#fundamentals_raw).
<!-- END generated_sample_rows -->

## Review Notes

- The `data` column is the widest row in the schema and a major I/O hotspot.
- The durable provider key is now `provider_listing_id`, not `(provider, provider_symbol)`.
- A payload is stored only for a listing that `refresh-supported-tickers` has
  already catalogued. `ingest-fundamentals` never creates a listing (that would
  mean writing the NOT NULL `listing.currency`), so an uncatalogued symbol is
  skipped — run the catalog refresh first.
- `payload_hash` is the raw content version; `last_fetched_at` is not used as a
  normalization watermark.
- Listing currency is intentionally not stored here. Use `listing.currency` for
  catalog quote-unit metadata; raw payload currencies are fact source
  currencies only.
