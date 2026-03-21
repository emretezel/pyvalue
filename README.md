# pyvalue

pyvalue is a fundamental-data ingestion, normalization, metric-computation, and stock-screening toolkit built for value-oriented workflows. It supports SEC fundamentals for US issuers and EODHD for global fundamentals and market data, with everything persisted in SQLite for repeatable analysis.

## Disclaimer

This project is provided for educational and informational purposes only and does not constitute investment, financial, legal, or tax advice. Outputs may be inaccurate, incomplete, or delayed and are provided "as is" without warranties of any kind. You are solely responsible for any investment decisions and outcomes based on this software; use at your own risk. Nothing here is an offer or solicitation to buy or sell any security, and past performance is not indicative of future results. Consult a licensed professional before making investment decisions.

## 5-Minute Quickstart

```bash
python -m pip install -e .[dev]
conda activate pyvalue
pyvalue load-universe --provider EODHD --exchange-code US
pyvalue ingest-fundamentals-bulk --provider EODHD --exchange-code US
pyvalue normalize-fundamentals-bulk --provider EODHD --exchange-code US
pyvalue update-market-data-bulk --exchange-code US
pyvalue compute-metrics-bulk --exchange-code US
```

Default database: `data/pyvalue.db`

## What pyvalue does

- Load exchange universes into SQLite.
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
pyvalue load-universe --provider EODHD --exchange-code LSE
pyvalue ingest-fundamentals --provider EODHD SHEL.LSE
pyvalue normalize-fundamentals --provider EODHD SHEL.LSE
pyvalue update-market-data SHEL.LSE
pyvalue compute-metrics SHEL.LSE --all
pyvalue run-screen SHEL.LSE screeners/value.yml
```

## Developer Notes

For local setup, tests, linting, and static checks, use:
- [Development Guide](docs/development/local-development.md)
- [Testing and Quality Checks](docs/development/testing-and-quality.md)
