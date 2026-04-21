# Troubleshooting

## Missing EODHD API Key

Typical error:
- EODHD key missing

Fix:
- add `[eodhd].api_key` to `private/config.toml`

## Missing SEC User-Agent

Typical problem:
- SEC requests fail or are rejected

Fix:
- set `[sec].user_agent` in `private/config.toml`
- or export `PYVALUE_SEC_USER_AGENT`

## No Raw Fundamentals Found During Normalization

Typical cause:
- normalization run before ingestion

Fix:
- ingest first, then normalize

## Metrics Missing After Compute

Typical causes:
- required normalized facts are missing
- facts are stale
- market data is missing for valuation metrics
- provider coverage is insufficient for the requested metric

Useful commands:

```bash
pyvalue report-fact-freshness --exchange-codes US
pyvalue report-metric-coverage --exchange-codes US
pyvalue report-metric-failures --exchange-codes US
pyvalue report-screen-failures --config screeners/value.yml --exchange-codes US
```

## No Supported Tickers Found

Typical cause:
- you tried a provider/exchange bulk workflow before refreshing or loading the canonical catalog

Fix:
- SEC: run `refresh-supported-exchanges --provider SEC` and `refresh-supported-tickers --provider SEC --exchange-codes US`
- EODHD: run `refresh-supported-exchanges --provider EODHD` and `refresh-supported-tickers --provider EODHD --exchange-codes <CODE>`

## No Eligible Supported Tickers Found

Typical cause:
- you ran EODHD bulk fundamentals ingestion before refreshing the stored ticker catalog

Fix:

```bash
pyvalue refresh-supported-exchanges --provider EODHD
pyvalue refresh-supported-tickers --provider EODHD --exchange-codes US
pyvalue ingest-fundamentals --provider EODHD --exchange-codes US
```

To see whether a larger global run is done, stale, or blocked by retry backoff:

```bash
pyvalue report-fundamentals-progress --provider EODHD
```

In that summary, `Stored` means a fundamentals payload exists in the DB, while
`Fresh` means the ticker currently counts as complete for the selected mode and
freshness window.

## Market Data Global Refresh Progress

Typical causes:
- `provider_listing` was not refreshed first
- old `market_data.as_of` snapshots are outside the freshness window
- some symbols are still inside retry backoff
- today’s EODHD quota is exhausted

Useful commands:

```bash
pyvalue update-market-data --provider EODHD --all-supported
pyvalue report-market-data-progress --provider EODHD
```

In that summary, `Stored` means a market-data snapshot exists in the DB, while
`Fresh` means the symbol currently counts as complete for the selected
freshness window.

## Market Cap Looks Wrong or Missing

Typical causes:
- market data not refreshed yet
- share-count facts were missing when prices were first stored

Fix:

```bash
pyvalue update-market-data --provider EODHD --exchange-codes US
pyvalue recalc-market-cap --exchange-codes US
```

## Stale Data

Typical symptom:
- metric refuses to compute even though the symbol exists

Fix:
- refresh fundamentals and/or market data
- re-run normalization
- re-run metric computation

## Related Docs

- [CLI Reference](reference/cli.md)
- [Getting Started](getting-started.md)
- [Testing and Quality Checks](development/testing-and-quality.md)
