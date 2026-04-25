# CLI Reference

All commands default to `data/pyvalue.db` unless `--database` is provided.

## Scope Model

The stage commands from fundamentals ingestion onward use one shared scope model.
If you omit all scope selectors, the command defaults to the full supported
universe. When you do provide a selector, provide at most one of:

- `--symbols <symbols...>`: one or more fully qualified symbols such as `AAPL.US`
  or `SHEL.LSE`
- `--exchange-codes <codes...>`: one or more canonical exchange codes such as `US`
  or `LSE`
- `--all-supported`: the full current supported-ticker catalog

Provider rules:

- `refresh-supported-exchanges`, `refresh-supported-tickers`,
  `ingest-fundamentals`, `reconcile-listing-status`,
  `normalize-fundamentals`, and `update-market-data` accept `--provider` and
  default it to `EODHD`
- `compute-metrics`, `run-screen`, `report-fact-freshness`,
  `report-metric-coverage`, `report-metric-failures`,
  `report-screen-failures`, and `recalc-market-cap`
  are provider-agnostic and operate on canonical symbols

For EODHD-backed symbols, downstream stage commands and canonical-scope
commands also apply canonical primary-listing classification from raw
fundamentals. Listings classified as secondary through `General.PrimaryTicker`
are excluded from normalization, market-data refresh, metric, screening,
metadata-refresh, and canonical reporting scopes. Missing or unusable
`PrimaryTicker` values are treated as primary. Read-only canonical/report
commands backfill unknown listing statuses in scope; use
`reconcile-listing-status` when you want a full backfill sweep from stored raw
fundamentals.

## Catalog Commands

### `refresh-supported-exchanges`

Refresh and persist the provider-supported exchange catalog.

Key options:

- `--provider {SEC,EODHD}`
- default provider: `EODHD`
- `--database <path>`

Notes:

- `EODHD` refreshes the live exchange list from EODHD
- `SEC` creates the fixed `US` canonical exchange row used by the SEC/Nasdaq
  ticker catalog

### `refresh-supported-tickers`

Refresh and persist the provider-supported ticker catalog.

Key options:

- `--provider {SEC,EODHD}`
- default provider: `EODHD`
- `--exchange-codes <codes...>`
- `--all-supported`
- `--include-etfs` for SEC only
- `--database <path>`

Notes:

- omitting both `--exchange-codes` and `--all-supported` defaults to the full
  supported exchange catalog for the provider
- `EODHD` reads `exchange-symbol-list/<EXCHANGE_CODE>` and keeps only
  `Common Stock`, `Preferred Stock`, and `Stock`
- `SEC` reads Nasdaq Trader symbol directories and materializes provider symbols
  as `TICKER.US`
- Removed provider symbols are deleted from `provider_listing` and the relevant
  fetch-state tables; historical fundamentals, market data, and metrics remain

## Fundamentals Commands

### `ingest-fundamentals`

Download fundamentals for supported tickers from the chosen provider.

Key options:

- `--provider {SEC,EODHD}`
- default provider: `EODHD`
- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--user-agent <value>` for SEC
- `--cik <10-digit-cik>` optional SEC override
- `--rate <float>`
- `--max-symbols <int>`
- `--max-age-days <int>` default `30`
- `--retry-failed-now`
- `--database <path>`

Notes:

- `SEC` rate is requests per second
- `EODHD` rate is symbols per minute
- `EODHD` uses the stored supported-ticker catalog plus daily quota checks,
  a concurrent worker pool, and retry backoff for multi-day runs
- EODHD raw writes do not store or infer listing currency from
  `General.CurrencyCode`; listing currency remains catalog metadata on
  `listing`
- storing an EODHD raw payload also refreshes cached primary-vs-secondary
  listing classification for that symbol
- retry backoff is respected by default; use `--retry-failed-now` to ignore it
- the default EODHD fundamentals rate is `950 req/min`, leaving a small buffer
  under the `1000 req/min` provider limit
- omitted `--max-age-days` now means the same 30-day freshness window used by
  the other CLI freshness filters

### `reconcile-listing-status`

Backfill canonical EODHD primary-vs-secondary listing classification from stored
raw fundamentals only.

Key options:

- `--provider {EODHD}`
- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--database <path>`

Notes:

- this command does not download fundamentals or market data
- it reads existing `fundamentals_raw` payloads and writes
  `listing.primary_listing_status`
- use this command after upgrading an existing database if you want to refresh
  all stored listing statuses immediately instead of waiting for read-only
  commands to backfill unknown statuses on demand
- listings classified as secondary via `General.PrimaryTicker` trigger
  downstream cleanup of normalized facts, market data, metrics, and related
  refresh-state rows for that listing
- missing or unusable `General.PrimaryTicker` values are treated as primary

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
- default provider: `EODHD`
- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--force` to re-normalize even when stored raw fundamentals are already up to date
- `--database <path>`

Notes:

- bulk runs over `--exchange-codes` or `--all-supported` parallelize automatically
- only symbols with stored raw fundamentals are normalized
- EODHD listings already classified as secondary are excluded from the
  requested scope before normalization starts
- EODHD normalization resolves its target from base(`listing.currency`);
  `listing.currency` itself preserves the catalog quote unit, including
  subunits such as `GBX`, `ZAC`, and `ILA`
- raw payload currencies are used only as fact source currencies
- fact source-currency lookup uses entry-level currency keys, then direct
  statement-level currency, then payload-level `General.CurrencyCode`
- by default, normalization skips symbols whose raw `fundamentals_raw.payload_hash`
  matches the payload hash recorded by the last successful normalization for
  that provider
- bulk runs with `--force` skip the freshness scan and start re-normalizing the
  requested symbol set immediately
- normalization never fetches FX from the network; run `refresh-fx-rates`
  first when you need currency conversion coverage
- bulk normalization preloads the entire selected-provider FX table once per
  worker process, then resolves direct, inverse, and USD/EUR triangulated rates
  from memory only
- if a required conversion still cannot be resolved from stored FX, that symbol
  fails normalization explicitly

## Market Data Commands

### `update-market-data`

Fetch latest market data for supported tickers and write directly into canonical
`market_data`.

Key options:

- `--provider {EODHD}`
- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--rate <float>`
- `--max-symbols <int>`
- `--max-age-days <int>` default `30`
- `--retry-failed-now`
- `--database <path>`

Notes:

- market-data refreshes use hybrid EODHD accounting: per-symbol requests cost
  `1`, while exchange-bulk refreshes cost `100` for the exchange
- the command is freshness-based by default and selects missing symbols first,
  then the oldest stale symbols
- EODHD listings already classified as secondary are excluded before refresh
  planning and progress accounting
- retry backoff is respected by default; use `--retry-failed-now` to ignore it
- large exchange and all-supported runs may use exchange-bulk fetches and then
  fall back to individual symbols when needed
- progress across multiple days is tracked through `market_data_fetch_state`
- suspicious price jumps are rejected before persistence and are stored as
  symbol-level fetch failures in `market_data_fetch_state`

### `report-market-data-progress`

Report EODHD market-data refresh progress across supported tickers.

Key options:

- `--provider {EODHD}`
- `--exchange-codes <codes...>`
- `--max-age-days <int>` default `30`
- `--database <path>`

Notes:

- `Stored` means a market-data snapshot exists in the database
- `Fresh` means the latest snapshot satisfies the selected freshness window
- `Recent failures` includes rejected price anomalies as well as API/network
  failures

### `recalc-market-cap`

Recompute stored market caps using the latest price and latest share-count facts.

Key options:

- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--database <path>`

## Metric Commands

### `compute-metrics`

Compute one or more metrics for a canonical ticker scope.

Key options:

- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--metrics <metric-ids...>` default all registered metrics
- console output defaults to periodic symbol progress like `Progress: 1234/75848 symbols complete (1.6%)`
- metric/data-quality warnings are suppressed on the console by default but still written to `data/logs/pyvalue.log`
- `--show-metric-warnings` to show metric/data-quality warnings on the console again
- `--database <path>`

Notes:

- stored metric rows now include explicit `unit_kind`, optional `currency`, and
  optional `unit_label`
- canonical metric/screen/report scopes exclude EODHD listings already
  classified as secondary from raw fundamentals
- monetary and per-share metrics are FX-aware; ratio, percent, multiple, and
  count metrics remain non-monetary outputs
- every metric attempt also updates `metric_compute_status`, which stores the
  latest success or failure plus the input watermarks used for freshness checks

### `refresh-fx-rates`

Fetch and store direct FX rates for the configured provider.

Key options:

- `--database <path>`
- `--start-date <YYYY-MM-DD>` optional historical backfill start
- `--end-date <YYYY-MM-DD>` optional end date, default today

Notes:

- with the default `EODHD` provider, the command syncs the FOREX catalog into
  `fx_supported_pairs` first
- the legacy Frankfurter discovery path only considers supported-ticker
  currencies from listings that are primary after EODHD raw classification
- EODHD refresh iterates canonical six-letter pairs only; three-letter
  shorthand aliases such as `EUR` are tracked as aliases to `USDEUR` and are
  not refreshed separately
- stores direct provider rows in `fx_rates`
- EODHD stores per-pair coverage and retry state in `fx_refresh_state`
- the first EODHD run backfills full available history per canonical pair when
  `--start-date` is omitted; later runs top up only missing older/newer outer
  ranges
- `--start-date` limits the first requested window, but a later unbounded run
  can still complete the older missing history
- progress is reported pair-by-pair on the console
- later runtime lookups can use direct, inverse, or triangulated conversion from
  those stored rows

## Reporting Commands

### `report-fact-freshness`

List missing or stale financial facts required by metrics for the requested
canonical scope.

Key options:

- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--metrics <metric-ids...>`
- `--max-age-days <int>` default `400`
- `--output-csv <path>`
- `--show-all`
- `--database <path>`

### `report-metric-coverage`

Count how many symbols can compute all requested metrics without writing results.

Key options:

- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--metrics <metric-ids...>`
- `--database <path>`

### `report-metric-failures`

Summarize warning reasons for metric computation failures on the requested
canonical scope.

Key options:

- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--metrics <metric-ids...>`
- `--output-csv <path>`
- `--database <path>`

Notes:

- ROIC FY-series metrics now emit standardized root-cause buckets such as
  missing FY EBIT history, fewer than required FY EBIT years, missing current
  or prior FY invested capital, missing invested-capital debt/equity/cash
  inputs, currency conflict, zero average invested capital, and latest FY point
  too old.
- the command reads fresh persisted metric failure status first and only
  recomputes pairs whose status is missing or stale for the current inputs

### `report-screen-failures`

Rank which screen criteria and missing metrics exclude the most symbols for the
requested canonical scope.

Key options:

- `--config <path>` required
- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--output-csv <path>`
- `--database <path>`

Notes:

- evaluates every criterion for every symbol, so criterion ranking is not biased
  by YAML order
- metric NA counts are deduplicated by `(symbol, metric_id)`, even when the same
  metric appears in multiple criteria
- screen evaluation now treats fresh failed or stale metric status as
  unavailable, even if an older raw row still exists in `metrics`
- the console report has two sections:
  - `Metric NA impact`: missing stored metrics ranked by affected-symbol count,
    with recompute-time root-cause buckets
  - `Criterion fallout`: per-criterion fail counts split into `na_fails` versus
    `threshold_fails`
- if a metric is unavailable because its latest status is missing or stale, the
  command recomputes only that metric for the affected symbols to distinguish:
  - `stored_missing_but_computable_now`
  - warning-driven `None` results
  - `exception: <type>`
  - `unknown_metric_id` when the screen references an unregistered metric
- for ROIC FY-series metrics, those warning-driven `None` results now retain the
  same standardized root-cause buckets shown by `report-metric-failures`

## Screening Commands

### `run-screen`

Evaluate a YAML screen against a canonical ticker scope.

Key options:

- `--config <path>` required
- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- console output defaults to periodic symbol progress like `Progress: 1234/75848 symbols complete (1.6%)`
- metric/data-quality warnings are suppressed on the console by default but still written to `data/logs/pyvalue.log`
- `--show-metric-warnings` to show metric/data-quality warnings on the console again
- `--output-csv <path>`
- `--database <path>`

Notes:

- screen reads use the latest metric status when available; a fresh failed
  status or stale success status hides older raw metric rows until the metric is
  recomputed
- when the scope is a single symbol, output includes entity details and
  criterion-by-criterion pass/fail rows
- when the scope contains multiple symbols, output lists only passing symbols
- the multi-symbol console view is a compact preview with one passing symbol per
  row, a truncated description, and ranking columns when present
- large passing sets are previewed in the console and the command tells you how
  to save or inspect the full result set
- `--output-csv` writes a row-oriented file with one passing symbol per row,
  base columns such as `symbol`, `entity`, `description`, `price`, and
  `price_currency`, then ranking columns and one column per criterion.
  `price_currency` is the listing quote unit; monetary metrics and market-cap
  values use base(`listing.currency`)
- if the screen YAML defines a `ranking` block, multi-symbol output also adds
  ranking columns such as `qarp_rank` and `qarp_score`, and sorts passing
  symbols
  by the configured ranking rules
- ranked multi-symbol screens load ranking-only metrics only for passers after
  the initial criteria filter
- monetary and per-share comparisons apply FX only where needed; ratio-like
  metrics are compared directly
- monetary constants can optionally declare a currency in YAML

### `refresh-security-metadata`

Refresh canonical security metadata from stored raw fundamentals without
rewriting normalized facts.

Key options:

- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--database <path>`

Notes:

- offline only; makes no network requests
- updates canonical `issuer` metadata such as entity name, description,
  sector, and industry
- intended for metadata backfills after ingesting raw fundamentals

## Maintenance Commands

### `purge-us-nonfilers`

Identify or delete SEC US supported tickers with no stored 10-K or 10-Q filing
coverage.

Key options:

- `--apply`
- `--database <path>`

### `clear-financial-facts`

Delete all normalized facts, financial-facts refresh state, and metric attempt
status.

### `clear-fundamentals-raw`

Delete all stored raw fundamentals.

### `clear-metrics`

Delete all stored metric rows and metric attempt status.

### `clear-market-data`

Delete all stored market-data snapshots.

All clear commands take:

- `--database <path>`
