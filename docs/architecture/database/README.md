# Database Review Guide

This section is the human-readable schema review area for `pyvalue`.

Use it in this order:

1. Start with [Table Inventory](table-inventory.md) for a fast scan of every table, its key, and its review priority.
2. Open the relevant file under [tables/](tables/) to inspect columns, primary keys, foreign keys, unique constraints, secondary indexes, and first-five sample rows.
3. Use [Indexes](indexes.md) and [Query Hotspots](query-hotspots.md) to judge whether the schema matches the real pipeline access patterns.
4. Fall back to [schema.snapshot.sql](schema.snapshot.sql) when you need the exact live DDL from `data/pyvalue.db`.

Snapshot caveat:

- The documented schema target is version `40`; the migrated `data/pyvalue.db`
  still carries a verified `fundamentals_raw` preservation discrepancy:
  `75,848` current rows versus `77,045` in the pre-migration backup.
- Treat the `fundamentals_raw` counts and first-five samples in this section as documentation of the current live file, not as proof that the migration preserved every raw payload.

Important structural notes:

- The catalog layer now uses enforced foreign keys across `provider`, `exchange`, `provider_exchange`, `issuer`, `listing`, and `provider_listing`.
- `listing` is the canonical identity root for downstream facts, market data, metrics, and listing status.
- `provider_listing` is the operational root for provider-scoped ingestion and market-data workflows.
- `fundamentals_raw`, `metrics`, and `metric_compute_status` each store the latest row per logical key, not a full history.

Table groups:

- Identity and catalog
  - [provider](tables/provider.md)
  - [exchange](tables/exchange.md)
  - [provider_exchange](tables/provider_exchange.md)
  - [issuer](tables/issuer.md)
  - [listing](tables/listing.md)
  - [provider_listing](tables/provider_listing.md)
- Raw ingestion and state
  - [fundamentals_raw](tables/fundamentals_raw.md)
  - [fundamentals_fetch_state](tables/fundamentals_fetch_state.md)
  - [fundamentals_normalization_state](tables/fundamentals_normalization_state.md)
  - [market_data_fetch_state](tables/market_data_fetch_state.md)
- Canonical analytics
  - [financial_facts](tables/financial_facts.md)
  - [financial_facts_refresh_state](tables/financial_facts_refresh_state.md)
  - [market_data](tables/market_data.md)
  - [metrics](tables/metrics.md)
  - [metric_compute_status](tables/metric_compute_status.md)
- FX
  - [fx_supported_pairs](tables/fx_supported_pairs.md)
  - [fx_refresh_state](tables/fx_refresh_state.md)
  - [fx_rates](tables/fx_rates.md)
- Housekeeping
  - [schema_migrations](tables/schema_migrations.md)

Supporting review pages:

- [Relationships](relationships.md)
- [Indexes](indexes.md)
- [Query Hotspots](query-hotspots.md)
- [Review Checklist](review-checklist.md)
- [Sample Rows Appendix](sample-rows.md)

Sample-row refresh notes:

- Sample rows are deterministic snapshots: first 5 rows ordered by primary key columns where available, `version ASC` for `schema_migrations`, and `rowid ASC` only as a fallback.
- Wide sample rows keep payload-sized fields readable by omitting the full payload and recording size metadata instead.
- Use `python scripts/generate_database_review_docs.py --sample-rows-only` to refresh sample rows without recomputing live table stats.
