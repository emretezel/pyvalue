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
pyvalue update-market-data --provider EODHD --exchange-codes US
pyvalue compute-metrics --exchange-codes US
pyvalue run-screen screeners/value.yml --exchange-codes US --output-csv data/screen_results_value.csv
```

Default database: `data/pyvalue.db`

## What pyvalue does

- Load canonical provider ticker catalogs into SQLite.
- Ingest raw fundamentals from SEC or EODHD.
- Normalize raw payloads into provider-agnostic financial facts.
- Update market data and market caps.
- Compute value and quality metrics.
- Run YAML-based stock screens against stored data.

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
pyvalue run-screen screeners/value.yml --symbols SHEL.LSE
```

## Developer Notes

For local setup, tests, linting, and static checks, use:
- [Development Guide](docs/development/local-development.md)
- [Testing and Quality Checks](docs/development/testing-and-quality.md)
