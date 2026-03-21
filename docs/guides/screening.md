# Screening Guide

## Screening Model

`pyvalue` screens run against computed metrics stored in SQLite.

Typical flow:
1. ingest and normalize fundamentals
2. update market data
3. compute metrics
4. run one or more YAML screens

## Compute Metrics First

Single symbol:

```bash
pyvalue compute-metrics AAPL.US --all
```

Bulk:

```bash
pyvalue compute-metrics-bulk --exchange-code US
```

## Run a Screen for One Symbol

```bash
pyvalue run-screen AAPL.US screeners/value.yml
```

## Run a Screen in Bulk

```bash
pyvalue run-screen-bulk screeners/value.yml --exchange-code US
```

Optional CSV output:

```bash
pyvalue run-screen-bulk screeners/value.yml --exchange-code US --output-csv results.csv
```

## Screen Definitions

Screens are YAML files, for example:

```text
screeners/value.yml
```

The sample screen is value-oriented and uses multiple cached metrics rather than recomputing formulas inline.

## Practical Workflow

For repeatable screening runs:

```bash
pyvalue load-universe --provider EODHD --exchange-code US
pyvalue ingest-fundamentals-bulk --provider EODHD --exchange-code US
pyvalue normalize-fundamentals-bulk --provider EODHD --exchange-code US
pyvalue update-market-data-bulk --exchange-code US
pyvalue compute-metrics-bulk --exchange-code US
pyvalue run-screen-bulk screeners/value.yml --exchange-code US
```

## Related Docs

- [Metrics Catalog](../reference/metrics.md)
- [CLI Reference](../reference/cli.md)
- [Getting Started](../getting-started.md)
