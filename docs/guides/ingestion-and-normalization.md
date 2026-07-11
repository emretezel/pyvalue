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

When the EODHD subscription changes, run `refresh-supported-exchanges` first so
the exchange catalog matches the plan. An exchange the plan no longer covers
answers the ticker refresh with HTTP 404 and is warned about and skipped
(stored data untouched); refreshes never delete canonical data — a removed
ticker loses only its provider mapping, raw payloads, and fetch state, while
facts, prices, and metrics are retained on the (now unreachable) listing.

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

This stage is useful because it preserves source payloads for later re-normalization.

Ingestion only stores a payload for a listing that `refresh-supported-tickers`
has already catalogued — always refresh the catalog first. An uncatalogued
symbol is skipped, because ingestion never creates a listing (doing so would
mean writing `listing.currency`, which only the catalog refresh may do).

As it stores each payload, ingestion also refreshes the listing's
primary/secondary classification. Reclassifying a listing **secondary** only
flips `listing.primary_listing_status`: everything the listing accumulated
(facts, metrics, market data, refresh state) is retained, and the primary-only
scope filters keep it out of downstream work. The run summary reports how many
listings were reclassified. `reconcile-listing-status` runs the same
classification as a separate, full-scope pass.

The raw table does not store listing currency. Listing currency is catalog
metadata stored on `listing.currency` as the authoritative quote unit.

For repeated fundamentals ingestion, the latest raw payload for the same
provider-symbol replaces the previous raw payload for that provider-symbol.

## What Normalization Does

Normalization converts provider-specific raw payloads into provider-agnostic
facts in `financial_facts`, keyed by canonical `listing_id`.

That gives metrics a stable input model independent of the raw EODHD payload shape.

Bulk normalization runs over `--exchange-codes` or `--all-supported`
parallelize automatically. The stage normalizes only symbols that already have
stored raw fundamentals in `fundamentals_raw`.
EODHD normalization requires a stored listing currency and converts monetary
facts into base(`listing.currency`) when raw fact-level, statement-level, or
payload-level currencies differ. Raw `General.CurrencyCode` is never used as a
fallback listing currency.

## Re-Normalization Behavior

Re-normalizing a symbol replaces any previously normalized facts for that
canonical security, regardless of provider.

That means:
- metrics always consume the latest normalized facts
- switching providers for the same canonical symbol overwrites normalized facts for that listing
- default normalization is incremental: a symbol is skipped unless its raw
  `fundamentals_raw.payload_hash` differs from the payload hash recorded by
  the last successful normalization for that provider, or the current facts are
  owned by a different provider
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

For large EODHD refreshes, `pyvalue normalize-fundamentals --all-supported`
is the fastest way to reprocess every stale stored raw payload in the catalog.
Add `--force` if you want to reprocess every stored raw payload regardless of
freshness.

## Related Docs

- [EODHD Provider Guide](../providers/eodhd.md)
- [Normalization and Facts](../architecture/normalization-and-facts.md)
- [CLI Reference](../reference/cli.md)
