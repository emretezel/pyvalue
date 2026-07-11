# `fx_rates`

## Purpose

Stores the canonical, provider-free direct FX rate series consumed by all
conversion paths (`FXService`). Provider provenance (which provider reported
each rate, under which pair symbol, fetched when) lives in the provider layer,
[`provider_fx_rates`](provider_fx_rates.md) — the same provider/canonical
split as `provider_exchange`/`exchange` and `provider_listing`/`listing`.

## Grain

One row per `(base_currency, quote_currency, rate_date)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Row count: `6,306,705`
- Table size: `435,523,584 bytes` (`415.3 MiB`)
- Approximate bytes per row: `69.1`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `base_currency` | `TEXT` | no | PK, idx | base currency. CHECK enforces 3-char uppercase ASCII letters |
| `quote_currency` | `TEXT` | no | PK, idx | quote currency. CHECK enforces 3-char uppercase ASCII letters |
| `rate_date` | `TEXT` | no | PK, idx | effective date |
| `rate` | `REAL` | no |  | rate as native float (per project REAL-everywhere policy) |
| `updated_at` | `TEXT` | no |  | update timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`base_currency`, `quote_currency`, `rate_date`)
- Physical foreign keys: none
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: no enforced FK
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- full-history preload (`FXRatesRepository.fetch_all`) and per-pair history
  (`fetch_pair_history`) for `FXService` conversion — provider-agnostic; the
  `ORDER BY base_currency, quote_currency, rate_date` is exactly the PK order,
  so both are index-order walks with no sort step
- point lookups (`latest_on_or_before`) — a descending PK seek
- inverse and triangulated FX lookup support (derived at runtime in
  `money.fx`, never persisted)
- provider coverage planning does **not** read this table: `pair_coverage` and
  `fully_covered_quotes_for_window` are provider-scoped and read
  `provider_fx_rates`

## Main Write Paths

- `refresh-fx-rates` — `FXRatesRepository.upsert_many` dual-writes each
  observation: the provider layer row (`provider_fx_rates`) and this canonical
  row, in one transaction. Single provider today, so the canonical row simply
  adopts the observation; a future multi-provider priority rule slots into the
  canonical upsert.

## Column Usage Notes

- `base_currency`: first pair component in all direct FX searches.
- `quote_currency`: second pair component in all direct FX searches.
- `rate_date`: date predicate and ordering column for direct-rate lookup.
- `rate`: REAL rate value consumed by FX conversion logic; migration 045 converted this column from TEXT (`rate_text`) to REAL under the project's REAL-everywhere policy.
- `updated_at`: update timestamp.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Sample window: first `5` rows returned by SQLite ordered by `base_currency ASC, quote_currency ASC, rate_date ASC`

```json
[
  {
    "base_currency": "AED",
    "quote_currency": "AUD",
    "rate_date": "2009-12-28",
    "rate": 0.3069,
    "updated_at": "2026-04-08T21:24:00.456429+00:00"
  },
  {
    "base_currency": "AED",
    "quote_currency": "AUD",
    "rate_date": "2009-12-29",
    "rate": 0.3041,
    "updated_at": "2026-04-08T21:24:00.456429+00:00"
  },
  {
    "base_currency": "AED",
    "quote_currency": "AUD",
    "rate_date": "2009-12-30",
    "rate": 0.3045,
    "updated_at": "2026-04-08T21:24:00.456429+00:00"
  },
  {
    "base_currency": "AED",
    "quote_currency": "AUD",
    "rate_date": "2009-12-31",
    "rate": 0.3031,
    "updated_at": "2026-04-08T21:24:00.456429+00:00"
  },
  {
    "base_currency": "AED",
    "quote_currency": "AUD",
    "rate_date": "2010-01-01",
    "rate": 0.3031,
    "updated_at": "2026-04-08T21:24:00.456429+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This can become large, so the pair/date access path matters more than almost
  any descriptive concern. The PK `(base_currency, quote_currency, rate_date)`
  IS the pair-history index; `idx_fx_rates_pair_date` was retired by
  migration 084 as redundant with it.
- Migration 084 rebuilt the table provider-free: `provider` (previously part
  of the PK, FK to `provider.provider_code`), `source_kind` (CHECK-constrained
  to the single value `'provider'` — it carried no information), `meta_json`
  (always `{"provider":…, "symbol":…}`) and the fetch/insert timestamps moved
  to `provider_fx_rates` (the pair symbol as a typed `provider_symbol`
  column). A pre-flight aborts the rebuild if two providers ever store the
  same pair/date without an arbitration rule.
- Migration 045 converted `rate_text` (TEXT) to `rate` (REAL) under the
  project REAL-everywhere policy. Numeric filtering inside SQLite is
  first-class.
- Migration 058 added the 3-char uppercase ASCII format CHECKs to
  `base_currency` and `quote_currency`; the 084 rebuild carries them forward.
