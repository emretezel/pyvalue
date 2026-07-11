# Database Review Guide

This section is the human-readable schema review area for `pyvalue`.

Use it in this order:

1. Start with [Table Inventory](table-inventory.md) for a fast scan of every table, its key, and its review priority.
2. Open the relevant file under [tables/](tables/) to inspect columns, primary keys, foreign keys, unique constraints, secondary indexes, and first-five sample rows.
3. Use [Indexes](indexes.md) and [Query Hotspots](query-hotspots.md) to judge whether the schema matches the real pipeline access patterns.
4. Fall back to [schema.snapshot.sql](schema.snapshot.sql) when you need the exact live DDL from `data/pyvalue.db`.

Snapshot caveat:

- The documented schema target is version `66`. The live `data/pyvalue.db`
  carries a known `fundamentals_raw` preservation discrepancy from a pre-`043`
  migration (`75,848` current rows vs. `77,045` in the pre-migration backup).
- Treat the `fundamentals_raw` counts and first-five samples in this section as documentation of the current live file, not as proof that the migration preserved every raw payload.

Important structural notes:

- **Referential integrity is enforced at the database level**, not in application code. Migrations 041, 043, and 046–050 added the previously-missing physical FKs on `metrics`, `metric_compute_status`, `financial_facts`, `financial_facts_refresh_state`, `market_data`, and the FX state tables, on top of the FKs the catalog layer (`provider`, `exchange`, `provider_exchange`, `issuer`, `listing`, `provider_listing`) already carried. Migrations 081/083 added the provider-layer observation tables `provider_market_data` (FK to `provider_listing`) and `provider_fx_rates` (FK to `provider`).
- **Domain invariants are enforced by CHECK constraints**. Migration 041 added `metrics.unit_kind` + currency pairing, migrations 055–059 added enum/format/non-empty CHECKs across `metric_compute_status.status`, `*_fetch_state.last_status`, currency-code formats, listing symbol formats, `financial_facts.unit` non-empty, and the boolean INTEGER columns (`is_alias`, `is_refreshable`, `full_history_backfilled`), and migration 061 added the row-level `market_data_fetch_state` error-row invariant. (The 058-era `fx_rates.source_kind` CHECK is gone with the column: it allowed a single value and carried no information — migration 083.)
- **Market data and FX rates follow the provider/canonical split** (migrations 081–084, mirroring `provider_exchange`/`exchange` and `provider_listing`/`listing`): ingestion dual-writes `provider_market_data`→`market_data` and `provider_fx_rates`→`fx_rates` in one transaction, downstream readers consume only the provider-free canonical tables, and refresh purges touch only the provider layer.
- **`issuer (name, country)` is now UNIQUE** (migration 060) after a one-time dedup that collapsed ~4,696 duplicate groups (~13,121 rows) and remapped ~8,425 listings to canonical issuers. Rows with NULL name or NULL country remain non-colliding because SQLite UNIQUE indexes treat NULLs as distinct.
- **`listing` is the canonical identity root** for downstream facts, market data, metrics, and listing status.
- **`provider_listing` is the operational root** for provider-scoped ingestion and market-data workflows. Migration 054 dropped the denormalised `provider_listing.provider_id`; the owning provider is reachable via `provider_exchange.provider_id` through `provider_exchange_id`.
- **`fx_rates.rate` is `REAL`** under the project REAL-everywhere policy (migration 045 converted the legacy TEXT `rate_text` column).
- `fundamentals_raw`, `metrics`, and `metric_compute_status` each store the latest row per logical key, not a full history.
- **Migrations are the single source of truth for schema** (tables, indexes, and views). Migration 042 added `provider_listing_catalog` and `supported_tickers`, migration 044 added the `securities`, `providers`, and `exchange_provider` compat views, migration 062 added `primary_provider_listing_catalog`. Runtime code in the `persistence/storage/` package does not issue `CREATE TABLE` / `CREATE VIEW` outside the migration framework.
- **`schema_migrations` is single-row by construction** (migration 063): the column shape is `(id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)` so stray or duplicate version rows are impossible.

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
  - [provider_market_data](tables/provider_market_data.md)
- Canonical analytics
  - [financial_facts](tables/financial_facts.md)
  - [financial_facts_refresh_state](tables/financial_facts_refresh_state.md)
  - [market_data](tables/market_data.md)
  - [metrics](tables/metrics.md)
  - [metric_compute_status](tables/metric_compute_status.md)
- FX
  - [fx_supported_pairs](tables/fx_supported_pairs.md)
  - [fx_refresh_state](tables/fx_refresh_state.md)
  - [provider_fx_rates](tables/provider_fx_rates.md)
  - [fx_rates](tables/fx_rates.md)
- Housekeeping
  - [schema_migrations](tables/schema_migrations.md)

Views (all persisted in the schema and owned by migrations, never by runtime code):

- `provider_listing_catalog` — joins `provider_listing` to `provider`, `provider_exchange`, `listing`, `issuer`, and `exchange` to expose the canonical provider-scoped catalog used by ingestion, screening, and FX paths. Owned by migration 042.
- `supported_tickers` — projection of `provider_listing_catalog` retained for compatibility with code paths that read the historical name. Owned by migration 042.
- `primary_provider_listing_catalog` — `provider_listing_catalog` filtered to `listing.primary_listing_status <> 'secondary'`. Replaces the inline `_primary_listing_predicate()` filter that previously appeared at 11+ query sites in `storage.py`. Owned by migration 062.
- `securities`, `providers`, `exchange_provider` — backwards-compatibility views over the canonical identity tables. Owned by migration 044.

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
