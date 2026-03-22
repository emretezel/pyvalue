# Ingestion and Normalization Guide

## Workflow Overview

The core data pipeline is:

1. load a universe into `listings`
2. ingest raw provider payloads into `fundamentals_raw`
3. normalize provider payloads into `financial_facts`
4. compute metrics from `financial_facts`

## Single-Symbol Workflow

EODHD example:

```bash
pyvalue ingest-fundamentals --provider EODHD AAPL.US
pyvalue normalize-fundamentals --provider EODHD AAPL.US
pyvalue compute-metrics AAPL.US --all
```

SEC example:

```bash
pyvalue ingest-fundamentals --provider SEC AAPL.US
pyvalue normalize-fundamentals --provider SEC AAPL.US
pyvalue compute-metrics AAPL.US --all
```

## Bulk Workflow

Typical exchange-level run:

```bash
pyvalue refresh-supported-exchanges --provider EODHD
pyvalue refresh-supported-tickers --provider EODHD --exchange-code US
pyvalue load-universe --provider EODHD --exchange-code US
pyvalue ingest-fundamentals-bulk --provider EODHD --exchange-code US
pyvalue normalize-fundamentals-bulk --provider EODHD --exchange-code US
pyvalue compute-metrics-bulk --exchange-code US
```

Typical all-exchange bootstrap over multiple days:

```bash
pyvalue refresh-supported-exchanges --provider EODHD
pyvalue refresh-supported-tickers --provider EODHD --all-exchanges
pyvalue ingest-fundamentals-global --provider EODHD --resume
```

Re-run the global command on later days to continue from the remaining eligible
tickers after the EODHD daily call budget resets.

Check progress between runs:

```bash
pyvalue report-ingest-progress --provider EODHD
```

To refresh stale symbols later instead of only filling missing payloads:

```bash
pyvalue ingest-fundamentals-global --provider EODHD --max-age-days 30 --resume
```

## What Ingestion Does

Ingestion stores raw provider payloads as received, keyed by:
- provider
- symbol
- metadata such as currency and exchange when available

This stage is useful because it preserves source payloads for later re-normalization.

For repeated EODHD fundamentals ingestion, the latest raw payload for a symbol
replaces the previously stored raw payload for that same symbol.

## What Normalization Does

Normalization converts provider-specific raw payloads into provider-agnostic facts in `financial_facts`.

That gives metrics a stable input model regardless of whether facts came from SEC or EODHD.

## Re-Normalization Behavior

Re-normalizing a symbol replaces any previously normalized facts for that symbol, regardless of provider.

That means:
- metrics always consume the latest normalized facts
- switching providers for the same symbol overwrites normalized facts for that symbol

## When to Re-Run Each Stage

Re-run ingestion when:
- provider data is stale
- you added new symbols
- you want fresh payloads from the source

For EODHD bulk workflows, refresh the stored ticker catalog when the provider's
supported symbols may have changed.

Re-run normalization when:
- raw payloads changed
- normalization rules changed
- you added new normalized concepts or fallback logic

## Related Docs

- [EODHD Provider Guide](../providers/eodhd.md)
- [SEC Provider Guide](../providers/sec.md)
- [Normalization and Facts](../architecture/normalization-and-facts.md)
- [CLI Reference](../reference/cli.md)
