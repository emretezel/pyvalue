# Data Model and Storage

## Storage Model

`pyvalue` stores operational data in SQLite.

The main persisted layers are:
- listings / universe data
- raw fundamentals
- normalized financial facts
- market data snapshots
- computed metrics

## Core Tables

### `fundamentals_raw`

Raw provider payloads are stored here.

Purpose:
- preserve source payloads as received
- support re-normalization when normalization logic changes
- separate provider-fetch concerns from metric computation

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

### `metrics`

Stores computed metric results.

Purpose:
- cache reusable metric outputs
- support bulk screen runs without recomputing everything on demand

## Persistence Flow

A normal run looks like:

1. universe loaded into listings storage
2. raw payload fetched into `fundamentals_raw`
3. provider-specific normalizer writes `financial_facts`
4. market refresh writes `market_data`
5. metric computation writes `metrics`
6. screens read from stored metrics

## Migration Notes

Schema and migrations are handled in the storage and migration modules, not in the docs layer. See the development docs if you are changing persistence behavior.

## Related Docs

- [Normalization and Facts](normalization-and-facts.md)
- [Ingestion and Normalization Guide](../guides/ingestion-and-normalization.md)
- [Development Guide](../development/local-development.md)
