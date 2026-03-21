# Documentation Index

This directory is the main documentation home for `pyvalue`. Each file owns one topic and should be treated as the canonical source for that topic.

## Start Here

- [Getting Started](getting-started.md): first install, environment setup, and first successful workflow.
- [Configuration](configuration.md): credentials, environment variables, database path defaults, and local config rules.

## Provider Documentation

- [EODHD](providers/eodhd.md): global fundamentals, universe loading, and market-data behavior.
- [SEC](providers/sec.md): SEC-only fundamentals, `User-Agent` requirements, and provider limitations.

## Task Guides

- [Ingestion and Normalization](guides/ingestion-and-normalization.md): how raw payloads become normalized facts.
- [Market Data](guides/market-data.md): quote refresh, bulk market updates, and market-cap recalculation.
- [Screening](guides/screening.md): metric computation, YAML screens, and bulk screen runs.

## Reference

- [CLI Reference](reference/cli.md): command-by-command CLI reference.
- [Metrics Catalog](reference/metrics.md): all supported metrics in grouped tables.

## Architecture

- [Data Model and Storage](architecture/data-model-and-storage.md): SQLite tables and persistence flow.
- [Normalization and Facts](architecture/normalization-and-facts.md): provider-agnostic concept model and metric inputs.

## Development

- [Local Development](development/local-development.md): repo layout, editable install, and common dev workflows.
- [Testing and Quality Checks](development/testing-and-quality.md): pytest, ruff, mypy, and contribution expectations.

## Operations

- [Troubleshooting](troubleshooting.md): common setup, provider, freshness, and metric-computation failures.
