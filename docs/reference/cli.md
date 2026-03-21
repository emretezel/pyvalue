# CLI Reference

All commands default to `data/pyvalue.db` unless `--database` is provided.

## Universe Commands

### `load-universe`

Download and persist an equity universe.

Key options:
- `--provider {SEC,EODHD}`
- `--database <path>`
- `--include-etfs`
- `--exchange-code <code>` for EODHD
- `--currencies <codes...>` for EODHD
- `--include-exchanges <values...>` for EODHD

## Fundamentals Commands

### `ingest-fundamentals`

Download fundamentals for one ticker.

Key options:
- positional `symbol`
- `--provider {SEC,EODHD}`
- `--exchange-code <code>` when symbol has no suffix
- `--user-agent <value>` for SEC
- `--cik <10-digit-cik>` for SEC
- `--database <path>`

### `ingest-fundamentals-bulk`

Download fundamentals in bulk for an exchange.

Key options:
- `--provider {SEC,EODHD}`
- `--exchange-code <code>`
- `--rate <float>`
- `--max-symbols <int>`
- `--max-age-days <int>`
- `--resume`
- `--user-agent <value>` for SEC
- `--database <path>`

### `normalize-fundamentals`

Normalize stored fundamentals for one symbol.

Key options:
- positional `symbol`
- `--provider {SEC,EODHD}`
- `--exchange-code <code>` when symbol has no suffix
- `--database <path>`

### `normalize-fundamentals-bulk`

Normalize stored fundamentals in bulk.

Key options:
- `--provider {SEC,EODHD}`
- `--exchange-code <code>`
- `--database <path>`

### `refresh-exchange`

Run exchange refresh steps in order: universe, fundamentals, normalization, market data, and metric computation.

Key options:
- `--provider {SEC,EODHD}`
- `--exchange-code <code>`
- `--include-etfs`
- `--currencies <codes...>` for EODHD
- `--include-exchanges <values...>` for EODHD
- `--fundamentals-rate <float>`
- `--market-rate <float>`
- `--max-symbols <int>`
- `--max-age-days <int>`
- `--resume`
- `--user-agent <value>` for SEC
- `--metrics <metric-ids...>`
- `--database <path>`

## Market Data Commands

### `update-market-data`

Fetch latest market data for one ticker.

Key options:
- positional `symbol`
- `--exchange-code <code>` when symbol has no suffix
- `--database <path>`

### `update-market-data-bulk`

Fetch latest market data for all stored listings on an exchange.

Key options:
- `--exchange-code <code>`
- `--rate <symbols-per-minute>`
- `--database <path>`

### `recalc-market-cap`

Recompute stored market caps using latest prices and share counts.

Key options:
- `--exchange-code <code>`
- `--database <path>`

## Metric Commands

### `compute-metrics`

Compute one or more metrics for one ticker.

Key options:
- positional `symbol`
- `--metrics <metric-ids...>`
- `--all`
- `--exchange-code <code>` when symbol has no suffix
- `--database <path>`

### `compute-metrics-bulk`

Compute metrics for all stored listings on an exchange.

Key options:
- `--exchange-code <code>`
- `--metrics <metric-ids...>`
- `--database <path>`

## Reporting Commands

### `report-fact-freshness`

Report missing or stale facts required by selected metrics.

Key options:
- `--exchange-code <code>`
- `--metrics <metric-ids...>`
- `--max-age-days <int>`
- `--output-csv <path>`
- `--show-all`
- `--database <path>`

### `report-metric-coverage`

Count how many symbols can compute all requested metrics without writing results.

Key options:
- `--exchange-code <code>`
- `--metrics <metric-ids...>`
- `--database <path>`

### `report-metric-failures`

Summarize warning reasons for metric computation failures.

Key options:
- `--exchange-code <code>`
- `--metrics <metric-ids...>`
- `--symbols <symbols...>`
- `--output-csv <path>`
- `--database <path>`

## Screening Commands

### `run-screen`

Evaluate a YAML screen for one symbol.

Key options:
- positional `symbol`
- positional `config`
- `--exchange-code <code>`
- `--database <path>`

### `run-screen-bulk`

Evaluate a YAML screen for all symbols in an exchange universe.

Key options:
- positional `config`
- `--exchange-code <code>`
- `--output-csv <path>`
- `--database <path>`

## Maintenance Commands

### `purge-us-nonfilers`

Identify or delete US listings with no stored 10-K/10-Q filing coverage.

Key options:
- `--apply`
- `--database <path>`

### `clear-listings`

Delete all stored listings.

### `clear-financial-facts`

Delete all normalized facts.

### `clear-fundamentals-raw`

Delete all raw fundamentals.

### `clear-metrics`

Delete all stored metric rows.

### `clear-market-data`

Delete all stored market-data snapshots.

All clear commands take:
- `--database <path>`
