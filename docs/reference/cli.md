# CLI Reference

All commands default to `data/pyvalue.db` unless `--database` is provided.

## Scope Model

The stage commands from fundamentals ingestion onward use one shared scope model.
Exactly one of these selectors is required:

- `--symbols <symbols...>`: one or more fully qualified symbols such as `AAPL.US`
  or `SHEL.LSE`
- `--exchange-codes <codes...>`: one or more canonical exchange codes such as `US`
  or `LSE`
- `--all-supported`: the full current supported-ticker catalog

Provider rules:

- `refresh-supported-exchanges`, `refresh-supported-tickers`,
  `ingest-fundamentals`, `normalize-fundamentals`, and `update-market-data`
  require `--provider`
- `compute-metrics`, `run-screen`, `report-fact-freshness`,
  `report-metric-coverage`, `report-metric-failures`, and `recalc-market-cap`
  are provider-agnostic and operate on canonical symbols

## Catalog Commands

### `refresh-supported-exchanges`

Refresh and persist the provider-supported exchange catalog.

Key options:

- `--provider {SEC,EODHD}`
- `--database <path>`

Notes:

- `EODHD` refreshes the live exchange list from EODHD
- `SEC` creates the fixed `US` canonical exchange row used by the SEC/Nasdaq
  ticker catalog

### `refresh-supported-tickers`

Refresh and persist the provider-supported ticker catalog.

Key options:

- `--provider {SEC,EODHD}`
- `--exchange-codes <codes...>`
- `--all-supported`
- `--include-etfs` for SEC only
- `--database <path>`

Notes:

- `EODHD` reads `exchange-symbol-list/<EXCHANGE_CODE>` and keeps only
  `Common Stock`, `Preferred Stock`, and `Stock`
- `SEC` reads Nasdaq Trader symbol directories and materializes provider symbols
  as `TICKER.US`
- Removed provider symbols are deleted from `supported_tickers` and the relevant
  fetch-state tables; historical fundamentals, market data, and metrics remain

## Fundamentals Commands

### `ingest-fundamentals`

Download fundamentals for supported tickers from the chosen provider.

Key options:

- `--provider {SEC,EODHD}`
- scope selector: `--symbols`, `--exchange-codes`, or `--all-supported`
- `--user-agent <value>` for SEC
- `--cik <10-digit-cik>` optional SEC override
- `--rate <float>`
- `--max-symbols <int>`
- `--max-age-days <int>`
- `--resume`
- `--database <path>`

Notes:

- `SEC` rate is requests per second
- `EODHD` rate is symbols per minute
- `EODHD` uses the stored supported-ticker catalog plus daily quota checks and
  retry backoff for multi-day runs
- when `--max-age-days` is omitted, EODHD ingestion is bootstrap-first and
  prefers symbols with no stored raw fundamentals

### `report-fundamentals-progress`

Report EODHD fundamentals ingest progress across supported tickers.

Key options:

- `--provider {EODHD}`
- `--exchange-codes <codes...>`
- `--max-age-days <int>` default `30`
- `--missing-only`
- `--database <path>`

Notes:

- `Stored` means a raw fundamentals payload exists in the database
- `Fresh` means the symbol currently satisfies the selected completeness rule
- status is strict:
  - `COMPLETE`: no missing, stale, or blocked symbols remain
  - `BLOCKED_BY_BACKOFF`: only retry-blocked failures remain
  - `INCOMPLETE`: missing or stale symbols remain

### `normalize-fundamentals`

Normalize stored fundamentals into canonical `financial_facts`.

Key options:

- `--provider {SEC,EODHD}`
- scope selector: `--symbols`, `--exchange-codes`, or `--all-supported`
- `--database <path>`

## Market Data Commands

### `update-market-data`

Fetch latest market data for supported tickers and write directly into canonical
`market_data`.

Key options:

- `--provider {EODHD}`
- scope selector: `--symbols`, `--exchange-codes`, or `--all-supported`
- `--rate <float>`
- `--max-symbols <int>`
- `--max-age-days <int>` default `7`
- `--resume`
- `--database <path>`

Notes:

- market-data requests cost one EODHD API call per symbol
- the command is freshness-based by default and selects missing symbols first,
  then the oldest stale symbols
- progress across multiple days is tracked through `market_data_fetch_state`

### `report-market-data-progress`

Report EODHD market-data refresh progress across supported tickers.

Key options:

- `--provider {EODHD}`
- `--exchange-codes <codes...>`
- `--max-age-days <int>` default `7`
- `--database <path>`

Notes:

- `Stored` means a market-data snapshot exists in the database
- `Fresh` means the latest snapshot satisfies the selected freshness window

### `recalc-market-cap`

Recompute stored market caps using the latest price and latest share-count facts.

Key options:

- scope selector: `--symbols`, `--exchange-codes`, or `--all-supported`
- `--database <path>`

## Metric Commands

### `compute-metrics`

Compute one or more metrics for a canonical ticker scope.

Key options:

- scope selector: `--symbols`, `--exchange-codes`, or `--all-supported`
- `--metrics <metric-ids...>` default all registered metrics
- `--database <path>`

## Reporting Commands

### `report-fact-freshness`

List missing or stale financial facts required by metrics for the requested
canonical scope.

Key options:

- scope selector: `--symbols`, `--exchange-codes`, or `--all-supported`
- `--metrics <metric-ids...>`
- `--max-age-days <int>` default `365`
- `--output-csv <path>`
- `--show-all`
- `--database <path>`

### `report-metric-coverage`

Count how many symbols can compute all requested metrics without writing results.

Key options:

- scope selector: `--symbols`, `--exchange-codes`, or `--all-supported`
- `--metrics <metric-ids...>`
- `--database <path>`

### `report-metric-failures`

Summarize warning reasons for metric computation failures on the requested
canonical scope.

Key options:

- scope selector: `--symbols`, `--exchange-codes`, or `--all-supported`
- `--metrics <metric-ids...>`
- `--output-csv <path>`
- `--database <path>`

## Screening Commands

### `run-screen`

Evaluate a YAML screen against a canonical ticker scope.

Key options:

- positional `config`
- scope selector: `--symbols`, `--exchange-codes`, or `--all-supported`
- `--output-csv <path>`
- `--database <path>`

Notes:

- metrics must already be computed and stored
- when the scope is a single symbol, output includes entity details and
  criterion-by-criterion pass/fail rows
- when the scope contains multiple symbols, output lists only passing symbols

## Maintenance Commands

### `purge-us-nonfilers`

Identify or delete SEC US supported tickers with no stored 10-K or 10-Q filing
coverage.

Key options:

- `--apply`
- `--database <path>`

### `clear-financial-facts`

Delete all normalized facts.

### `clear-fundamentals-raw`

Delete all stored raw fundamentals.

### `clear-metrics`

Delete all stored metric rows.

### `clear-market-data`

Delete all stored market-data snapshots.

All clear commands take:

- `--database <path>`
