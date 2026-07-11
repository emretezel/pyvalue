# `provider_fx_rates`

## Purpose

Provider-layer direct FX rate observations: what each provider reported for
each pair and date, under which of its own pair symbols, fetched when. The
canonical, provider-free series the conversion paths read lives in
[`fx_rates`](fx_rates.md). FX pairs have no per-provider catalog entity
(nothing like `provider_listing`), so provider scoping is a direct integer
`provider_id` FK — the catalog-style reference, unlike the TEXT
`provider_code` FKs of the sibling `fx_supported_pairs`/`fx_refresh_state`
state tables.

## Grain

One row per `(provider_id, base_currency, quote_currency, rate_date)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Row count: `6,306,705`
- Table size: `911,785,984 bytes` (`869.5 MiB`)
- Approximate bytes per row: `144.6`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider_id` | `INTEGER` | no | PK, idx | reporting provider (FK to `provider`) |
| `base_currency` | `TEXT` | no | PK, idx | base currency. CHECK enforces 3-char uppercase ASCII letters |
| `quote_currency` | `TEXT` | no | PK, idx | quote currency. CHECK enforces 3-char uppercase ASCII letters |
| `rate_date` | `TEXT` | no | PK, idx | effective date |
| `rate` | `REAL` | no |  | rate as native float (per project REAL-everywhere policy) |
| `provider_symbol` | `TEXT` | no |  | the provider's own pair symbol the history was fetched under (EODHD: `EURUSD`; aliases may differ from `base+quote`) |
| `fetched_at` | `TEXT` | no |  | provider fetch timestamp |
| `created_at` | `TEXT` | no |  | insert timestamp (preserved on conflict updates) |
| `updated_at` | `TEXT` | no |  | update timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`provider_id`, `base_currency`, `quote_currency`, `rate_date`)
- Physical foreign keys:
  - `provider_id` -> `provider`.`provider_id`
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `provider_id` in `provider`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- refresh planning (provider-scoped by design — "what did THIS provider give
  us"): `pair_coverage` resolves MIN/MAX `rate_date` per pair as two
  single-aggregate endpoint seeks off the PK autoindex, and
  `fully_covered_quotes_for_window` checks day-complete coverage per quote.
  The PK fronts `(provider_id, base_currency, quote_currency)` exactly so
  these are covering seeks — no secondary index needed.
- conversion never reads this table; it consumes canonical `fx_rates`.

## Main Write Paths

- `refresh-fx-rates` — `FXRatesRepository.upsert_many` dual-writes: a row here
  (provider code resolved to `provider_id`; empty `provider_symbol` normalized
  to `base+quote`, the provider's own convention) plus the canonical
  `fx_rates` upsert, in one transaction. Conflict updates refresh
  `rate`/`provider_symbol`/`fetched_at`/`updated_at` and preserve
  `created_at`.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Sample window: first `5` rows returned by SQLite ordered by `provider_id ASC, base_currency ASC, quote_currency ASC, rate_date ASC`

```json
[
  {
    "provider_id": 1,
    "base_currency": "AED",
    "quote_currency": "AUD",
    "rate_date": "2009-12-28",
    "rate": 0.3069,
    "provider_symbol": "AEDAUD",
    "fetched_at": "Wed, 08 Apr 2026 21:23:59 GMT",
    "created_at": "2026-04-08T21:24:00.456429+00:00",
    "updated_at": "2026-04-08T21:24:00.456429+00:00"
  },
  {
    "provider_id": 1,
    "base_currency": "AED",
    "quote_currency": "AUD",
    "rate_date": "2009-12-29",
    "rate": 0.3041,
    "provider_symbol": "AEDAUD",
    "fetched_at": "Wed, 08 Apr 2026 21:23:59 GMT",
    "created_at": "2026-04-08T21:24:00.456429+00:00",
    "updated_at": "2026-04-08T21:24:00.456429+00:00"
  },
  {
    "provider_id": 1,
    "base_currency": "AED",
    "quote_currency": "AUD",
    "rate_date": "2009-12-30",
    "rate": 0.3045,
    "provider_symbol": "AEDAUD",
    "fetched_at": "Wed, 08 Apr 2026 21:23:59 GMT",
    "created_at": "2026-04-08T21:24:00.456429+00:00",
    "updated_at": "2026-04-08T21:24:00.456429+00:00"
  },
  {
    "provider_id": 1,
    "base_currency": "AED",
    "quote_currency": "AUD",
    "rate_date": "2009-12-31",
    "rate": 0.3031,
    "provider_symbol": "AEDAUD",
    "fetched_at": "Wed, 08 Apr 2026 21:23:59 GMT",
    "created_at": "2026-04-08T21:24:00.456429+00:00",
    "updated_at": "2026-04-08T21:24:00.456429+00:00"
  },
  {
    "provider_id": 1,
    "base_currency": "AED",
    "quote_currency": "AUD",
    "rate_date": "2010-01-01",
    "rate": 0.3031,
    "provider_symbol": "AEDAUD",
    "fetched_at": "Wed, 08 Apr 2026 21:23:59 GMT",
    "created_at": "2026-04-08T21:24:00.456429+00:00",
    "updated_at": "2026-04-08T21:24:00.456429+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Created and backfilled by **migration 083** from the pre-split provider-keyed
  `fx_rates`: the provider code joined to `provider_id`, the pair symbol
  extracted from `meta_json` (`json_extract(meta_json, '$.symbol')`, falling
  back to `base||quote`), and `fetched_at`/`created_at`/`updated_at` carried
  verbatim. The old `source_kind` column was dropped entirely — it was
  CHECK-constrained to the single value `'provider'` and carried no
  information (the identity/inverse/triangulated labels are runtime
  `money.fx.FXQuote` derivations, never persisted).
- Same size class as canonical `fx_rates` (~6.3M rows per provider): the
  pair/date access path matters more than any descriptive concern.
