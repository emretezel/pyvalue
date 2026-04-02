# Getting Started

## Prerequisites

- Python 3.12+
- Conda environment named `pyvalue`
- SQLite available locally
- EODHD credentials if you want global fundamentals or any market data
- SEC `User-Agent` if you want SEC fundamentals

## Install

```bash
python -m pip install -e .[dev]
conda activate pyvalue
```

## Default Database

Unless you pass `--database`, CLI commands use:

```text
data/pyvalue.db
```

## First-Time Local Setup

1. Create or update `private/config.toml`.
2. Put your provider credentials there.
3. Activate the `pyvalue` conda environment.
4. Decide which provider workflow you want to start with.

## Recommended First Workflow: EODHD

Commands that accept `--provider` default to `EODHD`, so the examples below
omit it. Commands that accept `--symbols`, `--exchange-codes`, or
`--all-supported` also default to the full supported universe when you omit all
three selectors.

```bash
pyvalue refresh-supported-exchanges
pyvalue refresh-supported-tickers --exchange-codes US
pyvalue ingest-fundamentals --exchange-codes US --max-symbols 100
pyvalue normalize-fundamentals --exchange-codes US
pyvalue update-market-data --exchange-codes US
pyvalue compute-metrics --exchange-codes US
```

Then inspect screens or run a screen config:

```bash
pyvalue run-screen --config screeners/value.yml --exchange-codes US
```

## Minimal Single-Symbol Workflow

```bash
pyvalue ingest-fundamentals --symbols AAPL.US
pyvalue normalize-fundamentals --symbols AAPL.US
pyvalue update-market-data --symbols AAPL.US
pyvalue compute-metrics --symbols AAPL.US
```

## Where to Go Next

- Provider setup: [Configuration](configuration.md)
- Raw-to-normalized workflow: [Ingestion and Normalization](guides/ingestion-and-normalization.md)
- Commands: [CLI Reference](reference/cli.md)
- Metrics: [Metrics Catalog](reference/metrics.md)
