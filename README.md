# pyvalue

pyvalue is a fundamental-data ingestion, normalization, metric-computation, and stock-screening toolkit built for value-oriented workflows. It supports SEC fundamentals for US issuers and EODHD for global fundamentals and market data, with everything persisted in SQLite for repeatable analysis.

## Disclaimer

This project is provided for educational and informational purposes only and does not constitute investment, financial, legal, or tax advice. Outputs may be inaccurate, incomplete, or delayed and are provided "as is" without warranties of any kind. You are solely responsible for any investment decisions and outcomes based on this software; use at your own risk. Nothing here is an offer or solicitation to buy or sell any security, and past performance is not indicative of future results. Consult a licensed professional before making investment decisions.

## 5-Minute Quickstart

```bash
conda create -n pyvalue python=3.12 -y
conda activate pyvalue
python -m pip install -e .[dev]
pyvalue refresh-supported-exchanges --provider EODHD
pyvalue refresh-supported-tickers --provider EODHD --exchange-codes US
pyvalue ingest-fundamentals --provider EODHD --exchange-codes US --max-symbols 100
pyvalue normalize-fundamentals --provider EODHD --exchange-codes US
pyvalue refresh-security-metadata --exchange-codes US
pyvalue update-market-data --provider EODHD --exchange-codes US
pyvalue compute-metrics --exchange-codes US
pyvalue run-screen --config screeners/value.yml --exchange-codes US --output-csv data/screen_results_value.csv
pyvalue report-screen-failures --config screeners/value.yml --exchange-codes US
```

Default database: `data/pyvalue.db`

Default CLI behavior:
- commands that accept `--provider` default to `EODHD`
- commands that accept `--max-age-days` default to `30`
- multi-symbol EODHD fundamentals ingestion defaults to `950 req/min` and runs
  through concurrent fetch workers with batched SQLite writes

## What pyvalue does

- Load canonical provider ticker catalogs into SQLite.
- Ingest raw fundamentals from SEC or EODHD.
- Normalize raw payloads into provider-agnostic financial facts.
- Refresh canonical security metadata such as sector and industry from stored
  raw fundamentals.
- Update market data and market caps.
- Compute value and quality metrics.
- Run YAML-based stock screens against stored data, including ranked QARP
  output for passing symbols.

## Currency and FX

`pyvalue` now applies one shared currency framework across normalization,
metrics, and screening.

Key rules:

- configured subunit currencies are normalized before any arithmetic:
  `GBX`/`GBP0.01` -> `GBP`, `ZAC` -> `ZAR`, `ILA` -> `ILS`
- EODHD monetary currency resolution uses explicit precedence:
  row currency, then statement currency, then payload currency, then a narrow
  documented repo fallback for legacy facts whose `unit` already stores the ISO
  currency code.
- Monetary facts retain a real `currency` value. Non-monetary facts keep a
  meaningful `unit` such as `shares`.
- Monetary metrics persist explicit unit metadata and currency; ratio, percent,
  multiple, count, and other non-monetary metrics do not carry fake currencies.
- FX lookup is DB-backed, uses latest available rate on or before the requested
  date, and supports direct, inverse, and triangulated lookup.
- `normalize-fundamentals` never fetches FX from the web. Refresh FX explicitly
  first; if a required conversion still cannot be resolved from stored direct,
  inverse, or USD/EUR triangulated rates, that symbol fails normalization.

Refresh FX rates independently with:

```bash
pyvalue refresh-fx-rates
```

With the default `EODHD` provider, the command syncs the provider FOREX
catalog, refreshes all canonical six-letter pairs, backfills full history on
the first run, and later tops up only the missing older/newer outer ranges.

## Supported Providers

- `SEC`: US-only fundamentals. Coverage is useful, but normalization and metric coverage are less complete than EODHD.
- `EODHD`: Global fundamentals and all market data. This is the recommended provider for most workflows, including US fundamentals.

## Documentation

Start here for the full docs: [Documentation Index](docs/index.md)

Core pages:
- [Getting Started](docs/getting-started.md)
- [Configuration](docs/configuration.md)
- [EODHD Provider Guide](docs/providers/eodhd.md)
- [SEC Provider Guide](docs/providers/sec.md)
- [CLI Reference](docs/reference/cli.md)
- [Metrics Catalog](docs/reference/metrics.md)
- [Screening Guide](docs/guides/screening.md)
- [Architecture Overview](docs/architecture/data-model-and-storage.md)
- [Normalization and Facts](docs/architecture/normalization-and-facts.md)
- [Development Guide](docs/development/local-development.md)
- [Testing and Quality Checks](docs/development/testing-and-quality.md)
- [Troubleshooting](docs/troubleshooting.md)

## Short End-to-End Example

```bash
pyvalue refresh-supported-tickers --provider EODHD --exchange-codes LSE
pyvalue ingest-fundamentals --provider EODHD --symbols SHEL.LSE
pyvalue normalize-fundamentals --provider EODHD --symbols SHEL.LSE
pyvalue update-market-data --symbols SHEL.LSE
pyvalue compute-metrics --symbols SHEL.LSE
pyvalue run-screen --config screeners/value.yml --symbols SHEL.LSE
```

## Developer Notes

For local setup, tests, linting, and static checks, use:
- [Development Guide](docs/development/local-development.md)
- [Testing and Quality Checks](docs/development/testing-and-quality.md)
