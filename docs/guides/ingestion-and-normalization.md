# Ingestion and Normalization Guide

## Workflow Overview

The core data pipeline is:

1. refresh provider catalogs into `exchange`, `provider_exchange`, `issuer`, `listing`, and `provider_listing`
2. ingest raw provider payloads into `fundamentals_raw`
3. normalize provider payloads into canonical `financial_facts`
4. compute metrics from `financial_facts`

## Single-Symbol Workflow

EODHD example:

```bash
pyvalue ingest-fundamentals --provider EODHD --symbols AAPL.US
pyvalue normalize-fundamentals --provider EODHD --symbols AAPL.US
pyvalue compute-metrics --symbols AAPL.US
```

SEC example:

```bash
pyvalue refresh-supported-exchanges --provider SEC
pyvalue refresh-supported-tickers --provider SEC --exchange-codes US
pyvalue ingest-fundamentals --provider SEC --symbols AAPL.US
pyvalue normalize-fundamentals --provider SEC --symbols AAPL.US
pyvalue compute-metrics --symbols AAPL.US
```

## Exchange-Scoped Workflow

Typical exchange-level run:

```bash
pyvalue refresh-supported-exchanges --provider EODHD
pyvalue refresh-supported-tickers --provider EODHD --exchange-codes US
pyvalue ingest-fundamentals --provider EODHD --exchange-codes US
pyvalue normalize-fundamentals --provider EODHD --exchange-codes US
pyvalue compute-metrics --exchange-codes US
```

Typical all-exchange refresh over multiple days:

```bash
pyvalue refresh-supported-exchanges
pyvalue refresh-supported-tickers --all-supported
pyvalue ingest-fundamentals --all-supported
```

Re-run the global command on later days to continue from the remaining eligible
tickers after the EODHD daily call budget resets.

Check progress between runs:

```bash
pyvalue report-fundamentals-progress --provider EODHD
```

The default `--max-age-days` window is already 30 days. Use an explicit value
when you want a different refresh horizon:

```bash
pyvalue ingest-fundamentals --all-supported --max-age-days 90
```

## What Ingestion Does

Ingestion stores raw provider payloads as received, keyed by:
- `provider_listing_id`
- resolved canonical `listing_id`

This stage is useful because it preserves source payloads for later re-normalization.

For repeated fundamentals ingestion, the latest raw payload for the same
provider-symbol replaces the previous raw payload for that provider-symbol.

## What Normalization Does

Normalization converts provider-specific raw payloads into provider-agnostic
facts in `financial_facts`, keyed by canonical `listing_id`.

That gives metrics a stable input model regardless of whether facts came from SEC or EODHD.

Bulk normalization runs over `--exchange-codes` or `--all-supported`
parallelize automatically. The stage normalizes only symbols that already have
stored raw fundamentals in `fundamentals_raw`.

## Re-Normalization Behavior

Re-normalizing a symbol replaces any previously normalized facts for that
canonical security, regardless of provider.

That means:
- metrics always consume the latest normalized facts
- switching providers for the same canonical symbol overwrites normalized facts for that listing
- default normalization is incremental: a symbol is skipped unless its raw
  `fundamentals_raw.fetched_at` is newer than the last successful
  normalization for that provider, or the current facts are owned by a
  different provider
- use `pyvalue normalize-fundamentals --force ...` to bypass that skip logic
- bulk `--force` runs skip the freshness scan entirely and start reprocessing
  the selected symbols immediately

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

For large EODHD or SEC refreshes, `pyvalue normalize-fundamentals --all-supported`
is the fastest way to reprocess every stale stored raw payload in the catalog.
Add `--force` if you want to reprocess every stored raw payload regardless of
freshness.

## Related Docs

- [EODHD Provider Guide](../providers/eodhd.md)
- [SEC Provider Guide](../providers/sec.md)
- [Normalization and Facts](../architecture/normalization-and-facts.md)
- [CLI Reference](../reference/cli.md)
