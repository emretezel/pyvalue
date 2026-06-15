# Data Model and Storage

## Storage Model

`pyvalue` stores operational data in SQLite.

For a human-readable table-by-table schema review, including columns, primary
keys, foreign keys, unique constraints, indexes, first-five sample rows, and
query hotspots, use the
[Database Review Guide](database/README.md).

The main persisted layers are:

- provider registry
- canonical exchange identities
- provider exchange catalogs
- issuer metadata
- canonical listings
- provider listings
- raw fundamentals
- listing classification state
- provider-scoped fetch and normalization state
- normalized financial facts
- market data snapshots
- FX rates and FX provider catalogs
- computed metrics and metric attempt status

## Core Tables

### `provider`

Global provider metadata lives here. `provider_id` is the physical FK key, while
`provider_code` remains the stable external namespace string; `EODHD` is the
only registered provider.

### `exchange`

Canonical exchange identities live here. Provider-owned exchange metadata does
not belong in this table.

### `provider_exchange`

Provider-published exchange catalogs live here. Each row maps a
provider-local exchange code to `exchange.exchange_id` and stores provider-owned
exchange metadata such as country, currency, and MIC.

### `issuer`

Issuer-level descriptive metadata lives here. Moving this data out of canonical
listing identity keeps `listing` narrow and lets metadata be refreshed without
changing canonical listing keys.

### `listing`

Canonical listing identity lives here. A listing is defined by
`(exchange_id, symbol)`, and user-facing canonical symbols such as `AAPL.US`
are derived from `listing.symbol + exchange.exchange_code`.
EODHD primary-vs-secondary classification is stored as
`primary_listing_status`; unknown listings remain eligible in primary-only
scopes, while secondary listings are excluded. It is written only by
`ingest-fundamentals` (step 5 below) and `reconcile-listing-status`; every other
command reads it without reconciling. Migration 078 backfills any leftover
`unknown` listing that already has stored fundamentals.

### `provider_listing`

Provider-facing listing identity lives here. Rows are unique by
`(provider_exchange_id, provider_symbol)`, where `provider_symbol` is the bare
provider catalog symbol such as `AAPL`, not `AAPL.US`.

Provider-listing rows do not store currency. The canonical listing quote unit
lives on `listing.currency`, and compatibility catalog APIs expose that value
when callers ask for provider-listing currency.

`provider_listing` intentionally does not store provider-side descriptive
columns such as security type, provider name, country, ISIN, or refresh
timestamp. ETF filtering remains a load-time decision before insert.

### `fundamentals_raw`

Raw provider payloads are stored by `provider_listing_id`. Canonical
`listing_id` is derived by joining through `provider_listing`.
The table intentionally does not store currency; raw payload currencies are
used only as source currencies for individual normalized facts.
`payload_hash` is the canonical JSON content version used to decide whether
normalization must run again; `last_fetched_at` is only a fetch observation
timestamp.

Purpose:

- preserve source payloads as received
- support re-normalization when normalization logic changes
- separate provider-fetch concerns from metric computation
- preserve the provider-listing link needed to derive canonical `listing_id`

### `fundamentals_fetch_state`

Active fundamentals fetch failures and retry backoff live here, keyed by
`provider_listing_id`. Successful fetch state is derived from
`fundamentals_raw`, so successful fetches do not leave rows in this table.

### `fundamentals_normalization_state`

Successful normalization watermarks live here, keyed by `provider_listing_id`.
The stored `normalized_payload_hash` records the exact raw payload version that
was normalized.

### `financial_facts`

Normalized provider-agnostic facts live here, keyed by canonical `listing_id`.

Currency and unit semantics:

- `unit_kind` (migration 071, renamed from `unit`) classifies every fact with the
  `MetricUnitKind` enum (`monetary` / `per_share` / `ratio` / `percent` / `multiple`
  / `count` / `other`); it is never a currency code
- monetary and per_share facts store a real ISO `currency`; the schema couples the
  two (currency non-NULL iff `unit_kind` is monetary/per_share, NULL otherwise)
- non-monetary counts such as shares are `unit_kind = 'count'` with a NULL `currency`
- `currency` is major-only: subunit codes never reach a stored fact
- `listing.currency` is the only persisted listing-currency truth and preserves
  the quote unit from catalog metadata (and may itself be a subunit)
- raw fundamentals and market-data rows are not listing-currency sources
- configured subunit currencies are normalized to their base before a monetary fact
  is built: `GBX`/`GBP0.01` -> `GBP`, `ZAC` -> `ZAR`, `ILA` -> `ILS`

### `market_data`

Stores latest quote snapshot information by `listing_id`.
`market_data.price` is stored in the **major** currency
(`canonical_trading_currency(listing.currency)`): subunit quotes (`GBX`/`GBP0.01`
-> `GBP`, `ZAC` -> `ZAR`, `ILA` -> `ILS`) are divided by their subunit divisor
before persistence, so subunits never cross the data boundary, and the snapshot
read path reports that same base currency. The table does not persist a
duplicate currency column.

The derived `market_cap` column was removed (migration 072): market cap is
shares-outstanding x price, so it is computed on demand as the latest share-count
`financial_facts` row x the latest `market_data` price
(`MarketDataRepository.latest_snapshot` via `metrics.utils.market_cap_money`), not
persisted. Using the latest price means market cap (and every metric built on it)
re-prices on every market-data refresh; shares outstanding move slowly, so a
share count up to a quarter stale adds negligible error.

### `market_data_fetch_state`

Operational market-data refresh progress and retry backoff live here, keyed by
`provider_listing_id`.

### `fx_rates`, `fx_supported_pairs`, and `fx_refresh_state`

FX storage remains provider-code keyed. FX discovery reads currencies from
`listing` and `financial_facts`.

### `metrics` and `metric_compute_status`

Metrics and metric attempt status are keyed by canonical `listing_id`.

Metric rows also persist unit metadata:

- `unit_kind`: one of `monetary`, `per_share`, `ratio`, `percent`, `multiple`,
  `count`, or `other`
- `currency`: present only for currency-bearing metric kinds
- `unit_label`: optional display/unit hint such as `x` or `per_share`

## Scope Resolution

Every CLI command that works over the security universe — `compute-metrics`,
`run-screen`, and the `report-*` commands — resolves its scope **from the
`listing` table** and carries the natural `listing_id` down into every read and
write. The single entry point is `_resolve_canonical_scope_listings`
(`cli/_common.py`), which returns ordered `(listing_id, canonical_symbol)` pairs
from `SecurityRepository.list_supported_listings`. Commands build a
`{canonical_symbol: listing_id}` map once and thread it (as `security_ids_by_symbol`
/ `ids_by_symbol`) through the fact, market, and metric reads/writes, so the id
the scope join already holds is never re-derived. The canonical symbol
(`symbol || '.' || exchange_code`) survives only as a display/CSV label and as
result-dict keys — it is never used as a database selection, filter, join, or
sort key. Downstream tables are filtered on `listing_id` (or a real PK such as
`exchange_id`), never on a computed/concatenated symbol.

Two categories legitimately deviate:

- **Provider-axis pipeline commands** (`ingest-fundamentals`,
  `normalize-fundamentals`, `update-market-data`, `reconcile-listing-status`)
  start from the provider catalog because they operate on provider symbols and
  `fundamentals_raw` keyed by `provider_listing_id`. Their scope rows already
  expose `security_id` (from `provider_listing_catalog`), which they carry into
  the canonical writes (`replace_fact_rows(security_id=…)`,
  `MarketDataUpdate(security_id=…)`); the only symbol matching is the inherent
  provider-symbol → raw-payload intersection.
- **Non-listing commands** — `refresh-fx-rates` (FX pairs), the `clear-*`
  maintenance commands (blanket `DELETE FROM`), and the `report-*-progress`
  commands (aggregated by the real `provider_exchange_code` column) — have no
  per-listing scope to carry.

## Persistence Flow

A normal run looks like:

1. Provider registry is seeded into `provider`.
2. Provider exchange catalogs are refreshed into `exchange` and `provider_exchange`.
3. Provider listing catalogs are refreshed into `issuer`, `listing`, and `provider_listing`.
4. Raw fundamentals are fetched into `fundamentals_raw`.
5. EODHD raw writes refresh `listing.primary_listing_status` from `General.PrimaryTicker`.
6. Provider-specific normalization writes canonical `financial_facts`.
7. Market refresh writes canonical `market_data`.
8. Retry/backoff state updates `fundamentals_fetch_state` and `market_data_fetch_state`.
9. Metric computation writes `metrics` and `metric_compute_status`.
10. Screens read from canonical metrics and derived canonical symbols.

If an EODHD listing is classified as secondary, downstream rows for that
`listing_id` are deleted from `financial_facts`, `market_data`, `metrics`, and
related downstream refresh-state tables.

## Migration Notes

Schema and migrations are handled in the storage and migration modules, not in
the docs layer. The catalog refactor migrates existing data in place so
fundamentals, market data, and FX rates are not re-downloaded just to adopt the
new keys.

## Related Docs

- [Normalization and Facts](normalization-and-facts.md)
- [Ingestion and Normalization Guide](../guides/ingestion-and-normalization.md)
- [Development Guide](../development/local-development.md)
