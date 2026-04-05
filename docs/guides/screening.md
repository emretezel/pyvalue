# Screening Guide

## How Screening Works

`pyvalue` screens run against metric values already stored in SQLite.

Screens do not:
- read raw fundamentals directly
- compute metrics on demand
- support nested logic or custom formulas inside the YAML file

That means the normal workflow is:
1. ingest fundamentals
2. normalize fundamentals
3. update market data if your chosen metrics need it
4. compute the metrics your screen depends on
5. run the YAML screen

If a metric row is missing from the `metrics` table, the screen treats that criterion as failed.

## Screening Semantics

The current screening engine is intentionally simple:

- every criterion is combined with logical AND
- each criterion compares a left term to a right term
- if either term resolves to `None`, the criterion fails
- only these operators are supported: `<=`, `>=`, `<`, `>`, `==`

Screening is unit-aware:

- `ratio`, `percent`, `multiple`, and `count` terms compare directly
- `monetary` and `per_share` metric-vs-metric comparisons convert the right
  side into the left metric currency before comparison
- metric-vs-constant monetary comparisons interpret an unlabelled constant in
  the left metric currency; a constant can also declare an explicit currency
- mismatched unit kinds fail the criterion with a warning instead of aborting
  the whole run

There is no support for:
- OR logic
- nested groups
- inline arithmetic beyond a multiplier on the right-hand term

## YAML Schema

A screen definition is a YAML file with a top-level `criteria` list.

Each item has:
- `name`: human-readable label shown in output
- `left`: a metric term
- `operator`: one of `<=`, `>=`, `<`, `>`, `==`
- `right`: either a constant term or a metric term

Supported term forms:

```yaml
metric: <metric_id>
```

```yaml
value: <number>
```

Monetary constants can also declare a currency:

```yaml
value: 1_000_000_000
currency: USD
```

Optional right-hand multiplier:

```yaml
metric: working_capital
multiplier: 1.75
```

Important limitation: `multiplier` is applied only to the right-hand side because that is how the current evaluator is implemented.

## Canonical Example

This repo ships several example screens:

- [`screeners/basic_value.yml`](../../screeners/basic_value.yml): small learning example
- [`screeners/value.yml`](../../screeners/value.yml): larger opinionated value screen
- [`screeners/value_normalized.yml`](../../screeners/value_normalized.yml): compact value screen using normalized owner earnings and EV/EBIT
- [`screeners/quality.yml`](../../screeners/quality.yml): quality-focused screen with durability and balance-sheet checks
- [`screeners/quality_reasonable_price.yml`](../../screeners/quality_reasonable_price.yml): quality at a reasonable price screen combining durability with valuation discipline, plus post-screen `qarp_score` and `qarp_rank` output for passing symbols

The beginner example looks like this:

```yaml
criteria:
  - name: "Current ratio floor"
    left:
      metric: current_ratio
    operator: ">="
    right:
      value: 1.25

  - name: "Positive earnings yield"
    left:
      metric: earnings_yield
    operator: ">"
    right:
      value: 0

  - name: "Long-term debt vs working capital"
    left:
      metric: long_term_debt
    operator: "<="
    right:
      metric: working_capital
      multiplier: 1.75
```

This example shows both supported comparison patterns:

- metric vs constant
- metric vs metric multiplied on the right-hand side

## Compute Metrics First

Screens only read stored metric rows. Compute the needed metrics before you run the screen.

Single symbol:

```bash
pyvalue compute-metrics --symbols SHEL.LSE
```

Exchange-scoped:

```bash
pyvalue compute-metrics --exchange-codes US
```

Using `--all` is the easiest way to avoid missing metric rows while learning the workflow. In production you can compute only the metric ids your screen uses.

## Run a Screen for One Symbol

If the symbol already includes its exchange suffix, run:

```bash
pyvalue run-screen --config screeners/basic_value.yml --symbols SHEL.LSE
```

Single-symbol output prints:
- entity name
- description
- latest price if available
- one `PASS` or `FAIL` row per criterion

The command exits with status `0` only if all criteria pass.

## Run a Screen in Bulk

```bash
pyvalue run-screen --config screeners/basic_value.yml --exchange-codes US
```

Write passing symbols to a CSV:

```bash
pyvalue run-screen --config screeners/basic_value.yml --exchange-codes US --output-csv data/screen_results_basic_value.csv
```

Bulk output shows only symbols that satisfy every criterion. The table includes:
- symbol columns for passing names only
- entity name
- description
- price
- one row per criterion with the left-side metric value

Some screens also define a post-screen ranking block. In those cases:

- pass/fail behavior stays unchanged
- only passing symbols are ranked
- extra `qarp_rank` and `qarp_score` rows are added before the criterion rows
- passing symbols are ordered best to worst in the console table and CSV

Ranking notes:

- non-monetary ranking metrics work directly
- monetary or per-share ranking metrics should declare a comparison currency in
  the ranking block when the passing set contains mixed currencies
- if a mixed-currency monetary ranking metric omits that comparison currency,
  `pyvalue` skips that ranking metric instead of failing the screen

The bundled QARP screen uses sector-relative percentile subscores when enough
same-sector passers exist, and otherwise falls back to the full passing set.

If no symbols pass, the command prints `No symbols satisfied all criteria.` and exits non-zero.

## Diagnose Screen Fallout

If a bulk screen returns very few hits, run the dedicated diagnostics command on
the same scope:

```bash
pyvalue report-screen-failures --config screeners/basic_value.yml --exchange-codes US
```

The report separates two different problems:

- `Metric NA impact`: which metric ids are missing for the largest number of
  symbols, plus recompute-time root causes such as missing history, missing
  market data, unknown metric ids, or metrics that could be computed now
- `Criterion fallout`: which criteria eliminate the most symbols, split into
  `na_fails` versus `threshold_fails`

Use this when you want to decide whether to:

- relax a threshold that is filtering out too many otherwise-computable symbols
- amend a metric calculation so it returns non-`NA` for more symbols
- fix a wrong metric id in the YAML
- recompute metrics or refresh missing market data

## End-to-End Example

```bash
pyvalue refresh-supported-exchanges --provider EODHD
pyvalue refresh-supported-tickers --provider EODHD --exchange-codes US
pyvalue ingest-fundamentals --provider EODHD --exchange-codes US
pyvalue normalize-fundamentals --provider EODHD --exchange-codes US
pyvalue refresh-security-metadata --exchange-codes US
pyvalue update-market-data --provider EODHD --exchange-codes US
pyvalue compute-metrics --exchange-codes US
pyvalue run-screen --config screeners/quality_reasonable_price.yml --exchange-codes US --output-csv data/screen_results_qarp_US.csv
```

## Why a Screen Can Return No Results

Common causes:

- metrics were never computed for the symbols
- the YAML references a wrong metric id
- the wrong exchange scope was passed to `run-screen`
- a metric depends on market data, but market data was not updated
- the screen is simply too strict

Remember that a missing metric row causes that criterion to fail.

To see which criteria or missing metrics are doing the most damage, run:

```bash
pyvalue report-screen-failures --config screeners/basic_value.yml --exchange-codes US
```

## Related Docs

- [Metrics Catalog](../reference/metrics.md)
- [CLI Reference](../reference/cli.md)
- [Getting Started](../getting-started.md)
