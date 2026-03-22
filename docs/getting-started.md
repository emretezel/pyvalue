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

```bash
pyvalue refresh-supported-exchanges --provider EODHD
pyvalue refresh-supported-tickers --provider EODHD --exchange-codes US
pyvalue ingest-fundamentals --provider EODHD --exchange-codes US --max-symbols 100
pyvalue normalize-fundamentals --provider EODHD --exchange-codes US
pyvalue update-market-data --provider EODHD --exchange-codes US
pyvalue compute-metrics --exchange-codes US
```

Then inspect screens or run a screen config:

```bash
pyvalue run-screen screeners/value.yml --exchange-codes US
```

## Minimal Single-Symbol Workflow

```bash
pyvalue ingest-fundamentals --provider EODHD --symbols AAPL.US
pyvalue normalize-fundamentals --provider EODHD --symbols AAPL.US
pyvalue update-market-data --symbols AAPL.US
pyvalue compute-metrics --symbols AAPL.US
```

## Where to Go Next

- Provider setup: [Configuration](configuration.md)
- Raw-to-normalized workflow: [Ingestion and Normalization](guides/ingestion-and-normalization.md)
- Commands: [CLI Reference](reference/cli.md)
- Metrics: [Metrics Catalog](reference/metrics.md)
