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
pyvalue refresh-supported-tickers --provider EODHD --exchange-code US
pyvalue load-universe --provider EODHD --exchange-code US
pyvalue ingest-fundamentals-bulk --provider EODHD --exchange-code US
pyvalue normalize-fundamentals-bulk --provider EODHD --exchange-code US
pyvalue update-market-data-bulk --exchange-code US
pyvalue compute-metrics-bulk --exchange-code US
```

Then inspect screens or run a screen config:

```bash
pyvalue run-screen-bulk screeners/value.yml --exchange-code US
```

## Minimal Single-Symbol Workflow

```bash
pyvalue ingest-fundamentals --provider EODHD AAPL.US
pyvalue normalize-fundamentals --provider EODHD AAPL.US
pyvalue update-market-data AAPL.US
pyvalue compute-metrics AAPL.US --all
```

## Where to Go Next

- Provider setup: [Configuration](configuration.md)
- Raw-to-normalized workflow: [Ingestion and Normalization](guides/ingestion-and-normalization.md)
- Commands: [CLI Reference](reference/cli.md)
- Metrics: [Metrics Catalog](reference/metrics.md)
