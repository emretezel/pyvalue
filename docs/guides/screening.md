# Screening Guide

## How Screening Works

`pyvalue` screens run against metric values already stored in SQLite.

Screens do not:
- read raw fundamentals directly
- compute metrics on demand
- support arbitrary boolean nesting or custom formulas inside the YAML file
  (grouping is limited to one level of OR / K-of-N — see below)

That means the normal workflow is:
1. ingest fundamentals
2. normalize fundamentals
3. update market data if your chosen metrics need it
4. compute the metrics your screen depends on
5. run the YAML screen

If a metric row is missing from the `metrics` table, the screen treats that criterion as failed.

## Screening Semantics

The screening engine combines criteria in **conjunctive normal form** — a
logical AND of groups, where each group is either a single criterion or an
OR / K-of-N set of criteria:

- the top-level `criteria` list is combined with logical AND: a symbol passes
  only if every entry passes
- a bare criterion compares a left term to a right term
- a group (`any_of`) passes when at least `at_least` of its member criteria
  pass. `at_least` defaults to `1`, so a plain `any_of` is OR ("any of a subset
  passes"); set `at_least: k` for a "pass at least k of n" scorecard, or
  `at_least: n` to AND a named subset under one output column
- if either term of a criterion resolves to `None`, that criterion fails; a
  group fails only when it cannot reach `at_least` passing members — so an OR
  group keeps a symbol whose missing metric is covered by another arm
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
- arbitrary boolean nesting (groups are one level deep — an AND of ORs; a group
  cannot contain another group, and there is no `NOT`)
- inline arithmetic beyond a multiplier on the right-hand term

## YAML Schema

A screen definition is a YAML file with a top-level `criteria` list. Each item
is either a **bare criterion** or a **group**.

A bare criterion has:
- `name`: human-readable label shown in output
- `left`: a metric term
- `operator`: one of `<=`, `>=`, `<`, `>`, `==`
- `right`: either a constant term or a metric term

A group has:
- `name`: the group's label — the reportable unit (one CSV column, one fallout
  row), so group names must be unique across the screen
- `any_of`: a non-empty list of bare criteria (its members)
- `at_least` (optional, default `1`): how many members must pass for the group
  to pass — `1` is OR, `len(any_of)` is AND, `k` is a "pass ≥ k of n" scorecard

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

Group form — two alternative debt-service tests, either of which keeps the
symbol in (a debt-free issuer with no interest line still clears it on
leverage):

```yaml
criteria:
  - name: "Debt-service capacity"
    any_of:
      - name: "Interest coverage >= 6x"
        left:
          metric: interest_coverage
        operator: ">="
        right:
          value: 6
      - name: "Net debt / EBITDA <= 2.5x"
        left:
          metric: net_debt_to_ebitda
        operator: "<="
        right:
          value: 2.5
```

Add `at_least: 2` alongside `any_of` to require any two members instead of any
one. The group's `name` (`Debt-service capacity`) becomes the output column and
fallout label; the member `name`s appear in the single-symbol per-arm breakdown.

## Canonical Example

This repo ships two screens:

- [`screeners/quality_reasonable_price_primary.yml`](../../screeners/quality_reasonable_price_primary.yml): the primary QARP (quality at a reasonable price) screen; hard-gates durability and valuation discipline plus reinvestment (`iroic_5y`), gross-margin level and stability, full-cycle earnings quality (`cfo_to_ni_10y_median`, `accruals_ratio`), and modernized Graham earnings stability, ranked sector-relative on a blend spanning quality/capital-efficiency, valuation, capital allocation, and earnings stability, with post-screen `qarp_score` and `qarp_rank` output for passing symbols
- [`screeners/deep_value_graham.yml`](../../screeners/deep_value_graham.yml): deep-value Graham screen; deliberately loose structural gates (positive 7Y ROIC, Piotroski F-Score >= 5, Altman Z >= 1.81, P/B <= 3, a USD 150M market-cap investability floor) that exclude broken businesses rather than demand quality, ranked sector-relative on a cheapness-weighted blend of valuation (`price_to_book`, `ev_to_sales`, `ev_to_ebit`), capped capital efficiency (`croic`, `roce`), and composite health (`piotroski_f_score`, `altman_z`)

A minimal screen definition looks like this:

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
pyvalue run-screen --config screeners/quality_reasonable_price_primary.yml --symbols SHEL.LSE
```

Single-symbol output prints:
- entity name
- description
- latest price if available
- one `PASS` or `FAIL` row per group; a multi-member group also prints an
  indented `PASS`/`FAIL` line per member so you can see which arm carried it

The command exits with status `0` only if all groups pass.

## Run a Screen in Bulk

```bash
pyvalue run-screen --config screeners/quality_reasonable_price_primary.yml --exchange-codes US
```

Write passing symbols to a CSV:

```bash
pyvalue run-screen --config screeners/quality_reasonable_price_primary.yml --exchange-codes US --output-csv data/screen_results_qarp.csv
```

Bulk output shows only symbols that satisfy every criterion.

The console now prints a compact preview table with one passing symbol per row.
It includes:
- ranking fields such as `qarp_rank` and `qarp_score` when present
- symbol
- entity name
- latest price
- a truncated description for readability

When many symbols pass, the console shows only the top slice and tells you to
use the CSV for the full result set.

The CSV is row-oriented as well, which is much easier to open in spreadsheet
tools than the older wide transposed layout. Each row is one passing symbol and
the columns include:
- `symbol`
- `entity`
- `description`
- `price`
- `price_currency` (the listing quote unit, such as `GBX` for a pence-quoted
  UK listing)
- ranking fields such as `qarp_rank` and `qarp_score` when present
- one column per screen group (its `name`), holding the stored left-side value
  of the member that carried the group

Some screens also define a post-screen ranking block. In those cases:

- pass/fail behavior stays unchanged
- only passing symbols are ranked
- extra `qarp_rank` and `qarp_score` columns are added before the group columns
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
pyvalue report-screen-failures --config screeners/quality_reasonable_price_primary.yml --exchange-codes US
```

The report separates two different problems:

- `Metric NA impact`: which metric ids are missing for the largest number of
  symbols, and which groups each gap affects. For OR / K-of-N groups a missing
  metric is counted only when it actually blocked the group — i.e. no other arm
  produced a real answer
- `Criterion fallout`: which groups eliminate the most symbols, split into
  `na_fails` (the group was NA-blocked — no arm had data) versus
  `threshold_fails` (at least one arm had data and missed its bar)

The command is a pure read of stored metrics and persisted attempt status —
nothing is recomputed or written. For the per-reason root causes behind the NA
counts, follow the printed hint:

```bash
pyvalue report-metric-status --config screeners/quality_reasonable_price_primary.yml --reasons --exchange-codes US
```

Use this when you want to decide whether to:

- relax a threshold that is filtering out too many otherwise-computable symbols
- amend a metric calculation so it returns non-`NA` for more symbols
- fix a wrong metric id in the YAML
- rerun `compute-metrics` (or refresh market data first) so persisted metrics
  reflect the current facts

## End-to-End Example

```bash
pyvalue refresh-supported-exchanges --provider EODHD
pyvalue refresh-supported-tickers --provider EODHD --exchange-codes US
pyvalue ingest-fundamentals --provider EODHD --exchange-codes US
pyvalue normalize-fundamentals --provider EODHD --exchange-codes US
pyvalue refresh-security-metadata --exchange-codes US
pyvalue update-market-data --provider EODHD --exchange-codes US
pyvalue compute-metrics --exchange-codes US
pyvalue run-screen --config screeners/quality_reasonable_price_primary.yml --exchange-codes US --output-csv data/screen_results_qarp_US.csv
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
pyvalue report-screen-failures --config screeners/quality_reasonable_price_primary.yml --exchange-codes US
```

## Related Docs

- [Metrics Catalog](../reference/metrics.md)
- [CLI Reference](../reference/cli.md)
- [Getting Started](../getting-started.md)
