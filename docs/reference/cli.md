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
  `report-metric-status`, and `report-screen-failures`
  are provider-agnostic and operate on canonical symbols

For EODHD-backed symbols, downstream stage commands and canonical-scope
commands filter by canonical primary-listing classification: listings
classified as secondary through `General.PrimaryTicker` are excluded from
normalization, market-data refresh, metric, screening, metadata-refresh, and
canonical reporting scopes. Missing or unusable `PrimaryTicker` values are
treated as primary. That classification is *written* only by
`ingest-fundamentals` (in the same transaction that stores each raw payload) and
by `reconcile-listing-status`; every other command reads the cached
`listing.primary_listing_status` and never reconciles as a side effect.
Classification writes only that status column -- a secondary listing keeps its
stored data and is excluded from universe work by the primary-only scopes. A
one-time migration (078) resolves any leftover `unknown` listing that already
has stored fundamentals, so reads can trust the cache.

## Catalog Commands

### `refresh-supported-exchanges`

Refresh and persist the provider-supported exchange catalog.

Key options:

- `--provider {EODHD}`
- default provider: `EODHD`
- `--allow-mass-drop`
- `--database <path>`

Notes:

- `EODHD` refreshes the live exchange list from EODHD
- an exchange absent from the provider's list is dropped from
  `provider_exchange` together with its provider layer (`provider_listing`
  mappings plus their raw fundamentals and fetch/normalization state); each
  drop is printed with its purge size. Canonical rows (`exchange`, `listing`,
  `issuer`) and canonical data are never deleted -- listings that lose their
  mapping become unreachable through the provider-joined scopes, nothing more
- a payload that would drop at least 5 provider exchanges *and* more than
  half of the catalog looks like a truncated response: the sync is rolled
  back untouched and exits `1` unless `--allow-mass-drop` is passed
- run this after any EODHD plan change so `refresh-supported-tickers` (which
  iterates the provider's cataloged exchanges) stops visiting dropped venues

### `refresh-supported-tickers`

Refresh and persist the provider-supported ticker catalog.

Key options:

- `--provider {EODHD}`
- default provider: `EODHD`
- `--exchange-codes <codes...>`
- `--all-supported`
- `--allow-mass-delisting`
- `--database <path>`

Notes:

- omitting both `--exchange-codes` and `--all-supported` defaults to the full
  supported exchange catalog for the provider
- `EODHD` reads `exchange-symbol-list/<EXCHANGE_CODE>` and keeps only
  `Common Stock`, `Preferred Stock`, and `Stock`
- **Refreshes never delete canonical data.** Removed provider symbols lose
  only their provider layer: the `provider_listing` mapping, their raw
  fundamentals, and the relevant fetch/normalization-state tables. Canonical
  rows (`listing`, `issuer`) and canonical data (facts, market data, metrics,
  compute/refresh state) are provider-independent and are retained; a listing
  left with no provider mapping is reported as orphaned and simply becomes
  unreachable, because every scope resolver and catalog view joins through
  `provider_listing`
- an exchange the EODHD plan no longer covers answers the symbol list with
  HTTP 404; the run warns, skips it with stored data untouched, and continues
  with the remaining exchanges
- a payload that would remove at least 20 provider listings *and* more than
  half of the exchange's existing mappings looks like a truncated response or
  a plan change: the slice is rolled back untouched and skipped unless
  `--allow-mass-delisting` is passed
- exit code: `0` when every exchange refreshed or was only skipped as
  not-in-plan; `1` when any exchange hit the mass-delisting guard or another
  provider error (so cron jobs surface it)

## Fundamentals Commands

### `ingest-fundamentals`

Download fundamentals for supported tickers from the chosen provider.

Key options:

- `--provider {EODHD}`
- default provider: `EODHD`
- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--rate <float>`
- `--max-symbols <int>`
- `--max-age-days <int>` default `30`
- `--retry-failed-now`
- `--database <path>`

Notes:

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
- this is the only command (besides `ingest-fundamentals`, which reclassifies
  as it stores each raw payload) that writes `listing.primary_listing_status`;
  run it to re-derive classification on demand, e.g. to re-apply changed
  classification rules. Migration 078 performs the equivalent one-time backfill
  when upgrading an existing database
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

- `--provider {EODHD}`
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

Fetch latest market data for supported tickers and dual-write each observation:
the provider layer (`provider_market_data`, keyed by the provider listing) and
the canonical `market_data` series, in one transaction.

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
- no price-anomaly guard runs before persistence; per-symbol fetch errors are
  stored as fetch failures in `market_data_fetch_state`

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
- `Recent failures` lists API/network failures

## Metric Commands

### `compute-metrics`

Compute one or more metrics for a canonical ticker scope.

Key options:

- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--metrics <metric-ids...>` default all registered metrics
- console output defaults to periodic symbol progress like `Progress: 1234/75848 symbols complete (1.6%)`
- per-listing metric/data-quality diagnostics (warnings plus INFO notices such as documented-cap emissions) are suppressed on the console by default but still written to `data/logs/pyvalue.log`
- `--show-metric-warnings` to show those diagnostics on the console again
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
- EODHD refresh iterates canonical six-letter pairs only; three-letter
  shorthand aliases such as `EUR` are tracked as aliases to `USDEUR` and are
  not refreshed separately
- dual-writes each rate: the provider row in `provider_fx_rates` and the
  canonical provider-free rate in `fx_rates`
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

Notes:

- when the selection includes any `uses_market_data` metric, the report ends
  with a one-line market-data seam summary (fresh/stale/missing price
  snapshots over the scope) — concept coverage alone cannot explain NAs caused
  by a missing or stale price

### `report-metric-status`

Rank metrics by persisted NA share (failed or never-attempted) for the
requested canonical scope, and break the failures down by reason — the survey
side of the diagnostics. A pure read: nothing is recomputed and nothing is
written; run `compute-metrics` first to refresh the underlying state.

Key options:

- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--metrics <metric-ids...>` or `--config <path>` (mutually exclusive);
  `--config` restricts the report to the screen's criteria metrics — the set
  whose NA excludes a symbol from that screen
- `--reasons` breaks each metric down into per-reason buckets with a
  representative example
- `--output-csv <path>`
- `--database <path>`

Notes:

- the summary is a SQL aggregate over `metric_compute_status`, so a
  full-universe ranking finishes in seconds; `na_share = (failures +
  never_attempted) / total_symbols`, i.e. the share of the scope with no
  usable persisted value — what a screen effectively sees
- `never_attempted` counts scope listings with no persisted attempt at all for
  the metric (how a newly registered metric looks before its first
  `compute-metrics` run)
- `--reasons` classifies every (listing, metric) persisted state against the
  current input watermarks — the same staleness lens `run-screen` applies
  before trusting a stored value:
  - fresh failures bucket by their persisted `reason_code`; the example is the
    largest-market-cap listing in the bucket and appends
    `detail=<reason_detail>` (truncated on the console) when the attempt
    carried an untemplated detail — the CSV keeps the full text in
    `example_reason_detail`
  - pairs whose persisted state no longer matches the current inputs bucket
    under `stale_inputs (run compute-metrics)`, with the example detail
    summarizing the last (now untrustworthy) attempt
  - pairs with no persisted attempt bucket under `never_attempted (run
    compute-metrics)`
  - a large stale or never-attempted bucket means the summary counts are out
    of date — rerun `compute-metrics` before reading the reason mix
- `reason_code` is the first templated warning of the last failed attempt;
  `reason_detail` carries the same first warning **untemplated** (real years,
  counts, dates) for guard failures, and the invariant/exception text for
  raised failures — use `explain-metric` for a per-symbol live recompute with
  every guard warning untemplated

### `explain-metric`

Explain per (symbol, metric) why the metric computes or comes out NA — the
microscope next to the scope-wide report commands.

Key options:

- `--symbols <symbols...>` required (deliberately symbol-scoped; exchange and
  all-supported scopes are not accepted)
- `--metrics <metric-ids...>` or `--config <path>` (exactly one); `--config`
  expands to the screen's criteria metrics
- `--max-age-days <int>` default `400`
- `--database <path>`

Each (symbol, metric) block prints:

- the persisted attempt state — stored value or failure `reason_code` plus the
  otherwise-buried `reason_detail`, with a staleness verdict
- per required concept: latest stored point (end date, fiscal period, filing
  date, value, currency), fresh/STALE, and FY/quarterly/total row depth
- the market-data seam (latest price snapshot or its absence) for
  `uses_market_data` metrics
- a **write-free** live recompute: SUCCESS with the would-be value, or FAILURE
  with the templated `reason_code` plus every guard warning **untemplated**
  (real listing ids, dates, and counts)

Notes:

- never persists recomputed attempts, so it is safe to run mid-investigation
  without changing what screens or the report commands see

### `report-screen-failures`

Rank which screen groups and missing metrics exclude the most symbols for the
requested canonical scope — the fallout analyzer. A pure read: nothing is
recomputed and nothing is written.

Key options:

- `--config <path>` required
- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- `--output-csv <path>` (columns: `metric_id`, `missing_symbols`,
  `affected_criteria_count`, `affected_criteria`)
- `--database <path>`

Notes:

- evaluates every group for every symbol, so group ranking is not biased
  by YAML order
- metric NA counts are deduplicated by `(symbol, metric_id)`, even when the same
  metric appears in multiple groups
- screen evaluation treats fresh failed or stale metric status as
  unavailable, even if an older raw row still exists in `metrics`
- for OR / K-of-N groups a missing metric is attributed to NA fallout only when
  it actually blocked the group — i.e. no other arm produced a real answer
- the console report has two sections:
  - `Metric NA impact`: missing stored metrics ranked by affected-symbol count,
    with the groups each gap affects
  - `Criterion fallout`: per-group fail counts split into `na_fails` (the group
    was NA-blocked — no arm had data) versus `threshold_fails` (an arm had data
    and missed its bar) — the "relax the threshold or fix the data?" signal
- per-reason NA root causes intentionally live elsewhere: the report ends its
  NA-impact section with a `hint: pyvalue report-metric-status --config
  <screen> --reasons` drill-down instead of duplicating that survey; run
  `compute-metrics` first if statuses are missing or stale

## Screening Commands

### `run-screen`

Evaluate a YAML screen against a canonical ticker scope.

Key options:

- `--config <path>` required
- optional scope selector: `--symbols`, `--exchange-codes`, or
  `--all-supported` (defaults to the full supported universe)
- console output defaults to periodic symbol progress like `Progress: 1234/75848 symbols complete (1.6%)`
- per-listing metric/data-quality diagnostics (warnings plus INFO notices such as documented-cap emissions) are suppressed on the console by default but still written to `data/logs/pyvalue.log`
- `--show-metric-warnings` to show those diagnostics on the console again
- `--output-csv <path>`
- `--database <path>`

Notes:

- screen reads use the latest metric status when available; a fresh failed
  status or stale success status hides older raw metric rows until the metric is
  recomputed
- when the scope is a single symbol, output includes entity details and
  group-by-group pass/fail rows (a multi-member group also prints an indented
  per-member breakdown); when a group is NA-blocked, each missing metric is
  followed by its persisted NA reason (`reason_code`, never-attempted, or
  stale-state note) and the output ends with a paste-ready
  `hint: pyvalue explain-metric --symbols <symbol> --metrics <ids>` line
- when the scope contains multiple symbols, output lists only passing symbols
- the multi-symbol console view is a compact preview with one passing symbol per
  row, a truncated description, and ranking columns when present
- large passing sets are previewed in the console and the command tells you how
  to save or inspect the full result set
- `--output-csv` writes a row-oriented file with one passing symbol per row,
  base columns such as `symbol`, `entity`, `description`, `price`, and
  `price_currency`, then ranking columns and one column per group.
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

### `clear-financial-facts`

Delete all normalized facts, financial-facts refresh state, and metric attempt
status.

### `clear-fundamentals-raw`

Delete all stored raw fundamentals.

### `clear-metrics`

Delete all stored metric rows and metric attempt status.

### `clear-market-data`

Delete all stored market-data snapshots, both layers (`provider_market_data`
and canonical `market_data`) in one transaction.

All clear commands take:

- `--database <path>`
