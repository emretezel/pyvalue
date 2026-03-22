# Data Model and Storage

## Storage Model

`pyvalue` stores operational data in SQLite.

The main persisted layers are:
- listings / universe data
- supported exchange catalogs
- supported ticker catalogs
- raw fundamentals
- fundamentals fetch state
- normalized financial facts
- market data snapshots
- market data fetch state
- computed metrics

## Core Tables

### `fundamentals_raw`

Raw provider payloads are stored here.

Purpose:
- preserve source payloads as received
- support re-normalization when normalization logic changes
- separate provider-fetch concerns from metric computation

### `supported_exchanges`

Provider-published exchange catalogs live here.

Purpose:
- cache provider exchange metadata such as code, country, currency, and MIC
- avoid re-fetching exchange-list metadata on every EODHD lookup
- support explicit catalog refreshes from the CLI

### `supported_tickers`

Provider-published ticker catalogs live here, keyed by provider and qualified symbol.

Purpose:
- cache the EODHD exchange symbol list by exchange code
- store the fetchable qualified ticker symbol such as `AAPL.US` or `SHEL.LSE`
- drive exchange-level and global EODHD fundamentals ingestion without a live symbol-list call
- mirror the currently supported EODHD equity ticker set into `listings` for exchange workflows

When a provider drops a symbol, it is removed from this operational catalog and
from mirrored `listings`, but historical raw and derived tables are retained.

### `fundamentals_fetch_state`

Operational fetch progress and retry backoff live here.

Purpose:
- track retry state for failed fundamentals fetches
- support resumable bulk ingestion
- avoid repeatedly hitting symbols that are still inside backoff

### `financial_facts`

Normalized provider-agnostic facts live here.

Purpose:
- give metrics a common input model
- isolate metric logic from SEC vs EODHD raw schemas
- store concept, fiscal period, end date, unit/currency, and value

### `market_data`

Stores latest quote and market-cap snapshot information.

Purpose:
- support market-cap and EV-based valuation metrics
- decouple market refresh cadence from fundamentals cadence

### `market_data_fetch_state`

Operational market-data refresh progress and retry backoff live here.

Purpose:
- track retry state for failed market-data fetches
- support resumable global market-data refreshes
- avoid repeatedly hitting symbols that are still inside backoff

### `metrics`

Stores computed metric results.

Purpose:
- cache reusable metric outputs
- support bulk screen runs without recomputing everything on demand

## Persistence Flow

A normal run looks like:

1. provider catalogs refreshed into `supported_exchanges` and `supported_tickers`
2. universe loaded or mirrored into `listings`
3. raw payload fetched into `fundamentals_raw`
4. provider-specific normalizer writes `financial_facts`
5. market refresh writes `market_data`
6. retry/backoff state updates `fundamentals_fetch_state` and `market_data_fetch_state`
7. metric computation writes `metrics`
8. screens read from stored metrics

## Migration Notes

Schema and migrations are handled in the storage and migration modules, not in the docs layer. See the development docs if you are changing persistence behavior.

## Related Docs

- [Normalization and Facts](normalization-and-facts.md)
- [Ingestion and Normalization Guide](../guides/ingestion-and-normalization.md)
- [Development Guide](../development/local-development.md)
