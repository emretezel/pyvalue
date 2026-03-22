# EODHD Provider Guide

## What EODHD Covers

EODHD is the recommended provider for most `pyvalue` workflows.

It covers:
- global exchange universes
- global fundamentals
- all market data used by the project

## Subscription Requirements

You need an active EODHD subscription for:
- fundamentals endpoints
- market data endpoints

## Universe Loading

`pyvalue` stores the EODHD supported-exchange catalog in SQLite and uses it for
exchange metadata lookups. Refresh it explicitly when you want the latest
exchange list from EODHD:

```bash
pyvalue refresh-supported-exchanges --provider EODHD
```

`pyvalue` also stores a per-exchange `supported_tickers` catalog for EODHD.
Refresh one exchange:

```bash
pyvalue refresh-supported-tickers --provider EODHD --exchange-codes LSE
```

Refresh all stored exchanges:

```bash
pyvalue refresh-supported-tickers --provider EODHD --all-exchanges
```

Ticker refresh keeps only `Common Stock`, `Preferred Stock`, and `Stock`.
ETF, fund, and other security types are excluded from the operational catalog.
When a ticker disappears from EODHD, it is removed from `supported_tickers` and
stale fetch-state rows, but historical fundamentals, market data, and derived
tables are kept.

Example:

```bash
pyvalue refresh-supported-tickers --provider EODHD --exchange-codes LSE
```

## Fundamentals Ingestion

Single symbol:

```bash
pyvalue ingest-fundamentals --provider EODHD --symbols AAPL.US
```

Exchange-scoped:

```bash
pyvalue ingest-fundamentals --provider EODHD --exchange-codes US
```

Quota-aware all-supported run across the stored supported-ticker catalog:

```bash
pyvalue ingest-fundamentals --provider EODHD --all-supported
```

EODHD ingestion always reads from stored `supported_tickers`, not from a live
symbol-list request. Refresh the ticker catalog before running it:

```bash
pyvalue refresh-supported-tickers --provider EODHD --exchange-codes US
pyvalue ingest-fundamentals --provider EODHD --exchange-codes US
```

For large multi-day runs:

```bash
pyvalue refresh-supported-exchanges --provider EODHD
pyvalue refresh-supported-tickers --provider EODHD --all-exchanges
pyvalue ingest-fundamentals --provider EODHD --all-supported --resume
```

`ingest-fundamentals --provider EODHD` checks the EODHD user/quota endpoint
before each multi-symbol run, subtracts the configured daily buffer, throttles
by requests per minute, and exits cleanly when the remaining daily allowance is
exhausted. Rerun it the next day to continue from the remaining eligible ticker
set.

To see whether a multi-day run is actually complete for the current scope, use:

```bash
pyvalue report-fundamentals-progress --provider EODHD
```

This report defaults to a 30-day freshness window. That means old
`fundamentals_raw` rows count as incomplete by default even though
`ingest-fundamentals --provider EODHD --all-supported` stays bootstrap-first
when `--max-age-days` is omitted. Use `--missing-only` on the report if you
only care whether each supported ticker has ever been ingested once.
In the summary, `Stored` means a raw payload exists in the DB, while `Fresh`
means the ticker currently counts as complete for the selected mode/window.

Successful EODHD refreshes replace the stored raw payload for the same
provider-symbol in `fundamentals_raw`. Older historical periods remain
available through the newly stored payload and normalized downstream tables are
refreshed only when you run normalization again.

Important fundamentals options:

- `--symbols`, `--exchange-codes`, or `--all-supported`: choose the scope
- `--rate`: EODHD uses symbols per minute
- `--max-symbols`: limit one run
- `--max-age-days`: refresh stale or missing data; when omitted on
  `--all-supported`, only missing payloads are selected
- `--resume`: skip symbols still in backoff

## Fundamentals Normalization

Single symbol:

```bash
pyvalue normalize-fundamentals --provider EODHD --symbols AAPL.US
```

Exchange-scoped:

```bash
pyvalue normalize-fundamentals --provider EODHD --exchange-codes US
```

Normalization converts raw EODHD payloads into provider-agnostic
`financial_facts` records keyed by canonical `security_id`.

## Market Data

Market data is always fetched from EODHD.

Single symbol:

```bash
pyvalue update-market-data --provider EODHD --symbols AAPL.US
```

Exchange-scoped:

```bash
pyvalue update-market-data --provider EODHD --exchange-codes US
```

Quota-aware all-supported run across the stored supported-ticker catalog:

```bash
pyvalue update-market-data --provider EODHD --all-supported
```

For large multi-day runs:

```bash
pyvalue refresh-supported-exchanges --provider EODHD
pyvalue refresh-supported-tickers --provider EODHD --all-exchanges
pyvalue update-market-data --provider EODHD --all-supported --resume
```

`update-market-data --provider EODHD` checks the EODHD user/quota endpoint
before each multi-symbol run, subtracts the configured daily buffer, throttles
by requests per minute, and exits cleanly when the remaining daily allowance is
exhausted. Market-data requests cost one EODHD API call per symbol, so this
workflow can usually move through the supported universe faster than
fundamentals ingestion.

To see whether a multi-day market-data run is actually complete for the current
scope, use:

```bash
pyvalue report-market-data-progress --provider EODHD
```

This report defaults to a 7-day freshness window. A symbol is incomplete when
its latest stored `market_data.as_of` is missing or older than the selected
window. In the summary, `Stored` means a market-data snapshot exists in the DB,
while `Fresh` means the symbol currently counts as complete for the selected
window.

Important market-data options:

- `--symbols`, `--exchange-codes`, or `--all-supported`: choose the scope
- `--rate`: requests per minute, capped at the EODHD limit of `1000`
- `--max-symbols`: limit one run
- `--max-age-days`: refresh stale or missing market data; default `7`
- `--resume`: skip symbols still in backoff

Market cap can be recalculated later from stored prices and latest share counts:

```bash
pyvalue recalc-market-cap --exchange-codes US
```

## EODHD-Oriented Metrics

Many newer metrics in the project are intentionally EODHD-oriented because they rely on normalized concepts or fallback logic designed around EODHD payload structure. The metrics catalog marks this in the calculation column where relevant.

## Caveats

- Exchange suffixes matter: use `AAPL.US`, `SHEL.LSE`, etc.
- Some fields are normalized through EODHD-specific fallback chains; compute metrics only after normalization.
- Market data freshness is independent from fundamentals freshness.

## Related Docs

- [Configuration](../configuration.md)
- [Market Data Guide](../guides/market-data.md)
- [Ingestion and Normalization Guide](../guides/ingestion-and-normalization.md)
