# `listing`

## Purpose

Stores canonical exchange-specific listing identity and the authoritative
listing quote unit.

## Grain

One row per `(exchange_id, symbol)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Row count: `75,926`
- Table size: `2,412,544 bytes` (`2.3 MiB`)
- Approximate bytes per row: `31.8`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK | canonical listing surrogate key |
| `issuer_id` | `INTEGER` | no | FK | issuer metadata link |
| `exchange_id` | `INTEGER` | no | FK, idx | canonical exchange link; part of composite unique key |
| `symbol` | `TEXT` | no |  | bare canonical listing symbol such as `AAPL`; part of composite unique key. CHECK enforces uppercase, no whitespace, and `[A-Z0-9.&^*-]` characters only |
| `currency` | `TEXT` | no |  | authoritative listing quote unit, including subunits such as `GBX`, `ZAC`, and `ILA`. NOT NULL since migration 069; CHECK enforces 3-char uppercase ASCII letters |
| `isin` | `TEXT` | yes | idx | ISO 6166 security identifier. Added by migration 088. Nullable — EODHD publishes none for ~25% of listings and absence is a valid state. Deliberately **not** UNIQUE: every venue trading the same shares carries the same ISIN, which is what makes it the cross-listing grouping key. CHECK enforces 12 uppercase alphanumerics with a 2-letter country prefix and a numeric check digit |
| `lei` | `TEXT` | yes |  | ISO 17442 legal-entity identifier. Added by migration 088. Nullable (~26% populated). Shared by every listing of one entity, including separate share classes. CHECK enforces 20 uppercase alphanumerics |
| `primary_listing_status` | `TEXT` | no |  | canonical primary-listing classification: `unknown`, `primary`, or `secondary`. CHECK enforces the vocabulary since migration 088 |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `listing_id`
- Physical foreign keys:
  - `exchange_id` -> `exchange`.`exchange_id`
  - `issuer_id` -> `issuer`.`issuer_id`
- Physical references from other tables:
  - `financial_facts`.`listing_id` -> `listing_id`
  - `financial_facts_refresh_state`.`listing_id` -> `listing_id`
  - `market_data`.`listing_id` -> `listing_id`
  - `metric_compute_status`.`listing_id` -> `listing_id`
  - `metrics`.`listing_id` -> `listing_id`
  - `provider_listing`.`listing_id` -> `listing_id`
- Unique constraints beyond the primary key:
  - (`exchange_id`, `symbol`)
- Main logical refs: canonical root for facts, prices, metrics, and primary-listing status
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_listing_issuer (issuer_id)`
- `idx_listing_isin (isin) WHERE isin IS NOT NULL`
<!-- END generated_secondary_indexes -->

`idx_listing_isin` serves the primary-listing classifier's ISIN peer-group work:
the `GROUP BY isin` scan over the classified universe and the `WHERE isin = ?`
peer probe on the ingest path. `EXPLAIN QUERY PLAN` reports both as
`SEARCH listing USING COVERING INDEX idx_listing_isin`. It is partial because
~25% of rows carry no ISIN and are never probed — "listings with no ISIN" is not
a peer group.

## Main Read Paths

- canonical-scope resolution joins `listing` to `exchange` and projects the
  canonical symbol `listing.symbol || '.' || exchange.exchange_code` as a display
  label only — never a filter/join key. Full / by-exchange scope scans the
  supported universe (`list_supported_listings`); an explicit `--symbols` request
  seeks only the requested rows (`list_supported_listings_for_symbols`: split the
  canonical symbol, seek `exchange` by `exchange_code`, then `listing` by the
  `(exchange_id, symbol)` UNIQUE index)
- downstream joins from facts, market data, metrics, and primary-listing status
- FX currency discovery and currency-scoped data checks

## Main Write Paths

- `refresh-supported-tickers` — the sole runtime writer of `listing` rows, of
  `listing.currency`, and of `listing.isin`; it never deletes them: a prune
  removes only the
  provider layer (`provider_listing` + raw/fetch/normalization state), and a
  listing left with no provider mapping is retained, unreachable through the
  provider-joined scopes until a provider maps it again (2026-07-11 design)
- migration-time backfill from legacy securities

`ingest-fundamentals` never writes here. It attaches each payload to a listing
that `refresh-supported-tickers` has already catalogued and skips any symbol
whose listing is absent (creating one would require writing the NOT NULL
`listing.currency`). Currency therefore has a single source of truth.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC`

```json
[
  {
    "listing_id": 1,
    "issuer_id": 1,
    "exchange_id": 1,
    "symbol": "AALB",
    "currency": "EUR",
    "primary_listing_status": "primary"
  },
  {
    "listing_id": 2,
    "issuer_id": 2,
    "exchange_id": 1,
    "symbol": "ABN",
    "currency": "EUR",
    "primary_listing_status": "primary"
  },
  {
    "listing_id": 3,
    "issuer_id": 3,
    "exchange_id": 1,
    "symbol": "ACOMO",
    "currency": "EUR",
    "primary_listing_status": "primary"
  },
  {
    "listing_id": 4,
    "issuer_id": 4,
    "exchange_id": 1,
    "symbol": "AD",
    "currency": "EUR",
    "primary_listing_status": "primary"
  },
  {
    "listing_id": 5,
    "issuer_id": 5,
    "exchange_id": 1,
    "symbol": "ADYEN",
    "currency": "EUR",
    "primary_listing_status": "primary"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Canonical user-facing symbols such as `AAPL.US` are derived, not stored.
- `isin` and `lei` are identity evidence, not classification. `isin` answers
  "which security", `lei` answers "which legal entity"; a depositary receipt is
  legally a distinct security and carries its own ISIN, so ISIN groups
  cross-listings of one security but never an ADR with its underlying.
  `refresh-supported-tickers` is the primary source (`Isin` from the provider's
  exchange symbol list, which covers listings whose fundamentals payload omits
  it); migration 088 seeded both columns from stored `General.ISIN` /
  `General.LEI`. A refresh may correct a stored ISIN but never blanks one — a
  payload missing the field is treated as a provider gap, not a retraction.
  Shape normalization lives in `pyvalue.identifiers` (`shaped_isin`,
  `shaped_lei`) and mirrors the SQL CHECK predicates in `migrations.py`; keep
  the two in step.
- **`lei` has no runtime writer yet.** Migration 088 seeded it from stored
  payloads and nothing refreshes it, so a listing catalogued after that
  migration carries NULL until the issuer-identity reconcile command lands and
  takes ownership (the same relationship `reconcile-listing-status` has with
  `primary_listing_status`). The column ships now only because it shares
  migration 088's table rebuild — re-running that rebuild on the live database
  costs ~5.5 minutes and a full copy of a 43 GB file, so doing it twice for two
  adjacent columns would be waste, not caution.
- `listing.currency` is the only persisted listing-currency truth. It is a
  quote unit and is not collapsed to base currency at storage time. It is
  written solely by `refresh-supported-tickers`; fundamentals ingestion reads
  the catalog and never creates or mutates a listing's currency.
- Monetary normalization, market-cap calculations, FX discovery, and monetary
  metrics derive base currency from `listing.currency`.
- Unknown primary-listing status is treated as eligible; downstream
  primary-only scopes exclude only `secondary`. This is load-bearing, not
  incidental: the classifier returns `unknown` whenever no evidence decides a
  listing, and ~8,900 listings on domestic exchanges with no EODHD
  `PrimaryTicker` coverage rely on it to stay in the universe.
- `primary_listing_status` is written only by `ingest-fundamentals` (as it
  stores each raw payload) and `reconcile-listing-status`; every other command
  reads it. The two write different amounts of the rule: ingest sees one payload
  and can apply only the per-listing rules, leaving anything else `unknown`;
  reconcile holds the ISIN peer graph and applies all six. The rule itself lives
  in `pyvalue.universe.listing_classification` — a pure module both callers
  share, so there is one definition regardless of which path wrote the row. A flip to `secondary` changes nothing but this column -- the
  listing keeps its facts/metrics/market-data and is excluded from universe
  work solely by the primary-only scope filters. Migration 078 is the one-time
  backfill that resolved any leftover `unknown` listing with stored
  fundamentals (it shipped with the eager purge that was policy at the time).
