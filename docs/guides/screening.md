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

Optional right-hand multiplier:

```yaml
metric: working_capital
multiplier: 1.75
```

Important limitation: `multiplier` is applied only to the right-hand side because that is how the current evaluator is implemented.

## Canonical Example

This repo ships two example screens:

- [`screeners/basic_value.yml`](../../screeners/basic_value.yml): small learning example
- [`screeners/value.yml`](../../screeners/value.yml): larger opinionated value screen

The beginner example looks like this:

```yaml
criteria:
  - name: "Minimum market cap"
    left:
      metric: market_cap
    operator: ">"
    right:
      value: 750000000

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
pyvalue compute-metrics SHEL.LSE --all
```

Bulk:

```bash
pyvalue compute-metrics-bulk --exchange-code US
```

Using `--all` is the easiest way to avoid missing metric rows while learning the workflow. In production you can compute only the metric ids your screen uses.

## Run a Screen for One Symbol

If the symbol already includes its exchange suffix, run:

```bash
pyvalue run-screen SHEL.LSE screeners/basic_value.yml
```

If the symbol does not include its exchange suffix, you must supply `--exchange-code`:

```bash
pyvalue run-screen SHEL screeners/basic_value.yml --exchange-code LSE
```

Single-symbol output prints:
- entity name
- description
- latest price if available
- one `PASS` or `FAIL` row per criterion

The command exits with status `0` only if all criteria pass.

## Run a Screen in Bulk

```bash
pyvalue run-screen-bulk screeners/basic_value.yml --exchange-code US
```

Write passing symbols to a CSV:

```bash
pyvalue run-screen-bulk screeners/basic_value.yml --exchange-code US --output-csv data/screen_results_basic_value.csv
```

Bulk output shows only symbols that satisfy every criterion. The table includes:
- symbol columns for passing names only
- entity name
- description
- price
- one row per criterion with the left-side metric value

If no symbols pass, the command prints `No symbols satisfied all criteria.` and exits non-zero.

## End-to-End Example

```bash
pyvalue load-universe --provider EODHD --exchange-code US
pyvalue ingest-fundamentals-bulk --provider EODHD --exchange-code US
pyvalue normalize-fundamentals-bulk --provider EODHD --exchange-code US
pyvalue update-market-data-bulk --exchange-code US
pyvalue compute-metrics-bulk --exchange-code US
pyvalue run-screen-bulk screeners/basic_value.yml --exchange-code US --output-csv data/screen_results_basic_value.csv
```

## Why a Screen Can Return No Results

Common causes:

- metrics were never computed for the symbols
- the YAML references a wrong metric id
- the wrong exchange was passed to `run-screen-bulk`
- a metric depends on market data, but market data was not updated
- the screen is simply too strict

Remember that a missing metric row causes that criterion to fail.

## Related Docs

- [Metrics Catalog](../reference/metrics.md)
- [CLI Reference](../reference/cli.md)
- [Getting Started](../getting-started.md)
