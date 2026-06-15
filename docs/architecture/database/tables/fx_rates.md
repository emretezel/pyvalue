# `fx_rates`

## Purpose

Stores direct FX rates fetched from the provider.

## Grain

One row per `(provider, rate_date, base_currency, quote_currency)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Row count: `6,306,705`
- Table size: `1,232,326,656 bytes` (`1.15 GiB`)
- Approximate bytes per row: `195.4`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider` | `TEXT` | no | PK, idx | provider namespace |
| `rate_date` | `TEXT` | no | PK, idx | effective date |
| `base_currency` | `TEXT` | no | PK, idx | base currency. CHECK enforces 3-char uppercase ASCII letters |
| `quote_currency` | `TEXT` | no | PK, idx | quote currency. CHECK enforces 3-char uppercase ASCII letters |
| `rate` | `REAL` | no |  | rate as native float (per project REAL-everywhere policy) |
| `fetched_at` | `TEXT` | no |  | provider fetch timestamp |
| `source_kind` | `TEXT` | no |  | direct provider source kind. CHECK enforces `IN ('provider')` — widen via a future migration when synthesized/derived sources are introduced |
| `meta_json` | `TEXT` | yes |  | optional metadata |
| `created_at` | `TEXT` | no |  | insert timestamp |
| `updated_at` | `TEXT` | no |  | update timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`provider`, `rate_date`, `base_currency`, `quote_currency`)
- Physical foreign keys:
  - `provider` -> `provider`.`provider_code`
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: no enforced FK
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_fx_rates_pair_date (provider, base_currency, quote_currency, rate_date DESC)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- direct pair/date lookups
- inverse and triangulated FX lookup support
- historical coverage checks (`pair_coverage`): MIN and MAX `rate_date` are
  read as two separate single-aggregate subqueries so each resolves to one
  index-endpoint seek on `idx_fx_rates_pair_date`. A combined `MIN(),MAX()` in
  one statement would defeat SQLite's min/max optimization and scan the whole
  pair group instead.

## Main Write Paths

- `refresh-fx-rates`

## Column Usage Notes

- `provider`: first filter in FX lookup queries.
- `rate_date`: date predicate and ordering column for direct-rate lookup.
- `base_currency`: first pair component in all direct FX searches.
- `quote_currency`: second pair component in all direct FX searches.
- `rate`: REAL rate value consumed by FX conversion logic; migration 045 converted this column from TEXT (`rate_text`) to REAL under the project's REAL-everywhere policy.
- `fetched_at`: provider fetch timestamp for auditability.
- `source_kind`: indicates how the direct rate was sourced.
- `meta_json`: optional provider metadata, not part of hot lookup predicates.
- `created_at`: insert timestamp.
- `updated_at`: update timestamp.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Sample window: first `5` rows returned by SQLite ordered by `provider ASC, rate_date ASC, base_currency ASC, quote_currency ASC`

```json
[
  {
    "provider": "EODHD",
    "rate_date": "1950-01-01",
    "base_currency": "EUR",
    "quote_currency": "EUR",
    "rate": 1.0,
    "fetched_at": "Wed, 08 Apr 2026 21:32:03 GMT",
    "source_kind": "provider",
    "meta_json": "{\"provider\": \"EODHD\", \"symbol\": \"EUREUR\"}",
    "created_at": "2026-04-08T21:32:04.346014+00:00",
    "updated_at": "2026-04-08T21:32:04.346014+00:00"
  },
  {
    "provider": "EODHD",
    "rate_date": "1950-01-02",
    "base_currency": "EUR",
    "quote_currency": "EUR",
    "rate": 1.0,
    "fetched_at": "Wed, 08 Apr 2026 21:32:03 GMT",
    "source_kind": "provider",
    "meta_json": "{\"provider\": \"EODHD\", \"symbol\": \"EUREUR\"}",
    "created_at": "2026-04-08T21:32:04.346014+00:00",
    "updated_at": "2026-04-08T21:32:04.346014+00:00"
  },
  {
    "provider": "EODHD",
    "rate_date": "1950-01-03",
    "base_currency": "EUR",
    "quote_currency": "EUR",
    "rate": 1.0,
    "fetched_at": "Wed, 08 Apr 2026 21:32:03 GMT",
    "source_kind": "provider",
    "meta_json": "{\"provider\": \"EODHD\", \"symbol\": \"EUREUR\"}",
    "created_at": "2026-04-08T21:32:04.346014+00:00",
    "updated_at": "2026-04-08T21:32:04.346014+00:00"
  },
  {
    "provider": "EODHD",
    "rate_date": "1950-01-04",
    "base_currency": "EUR",
    "quote_currency": "EUR",
    "rate": 1.0,
    "fetched_at": "Wed, 08 Apr 2026 21:32:03 GMT",
    "source_kind": "provider",
    "meta_json": "{\"provider\": \"EODHD\", \"symbol\": \"EUREUR\"}",
    "created_at": "2026-04-08T21:32:04.346014+00:00",
    "updated_at": "2026-04-08T21:32:04.346014+00:00"
  },
  {
    "provider": "EODHD",
    "rate_date": "1950-01-05",
    "base_currency": "EUR",
    "quote_currency": "EUR",
    "rate": 1.0,
    "fetched_at": "Wed, 08 Apr 2026 21:32:03 GMT",
    "source_kind": "provider",
    "meta_json": "{\"provider\": \"EODHD\", \"symbol\": \"EUREUR\"}",
    "created_at": "2026-04-08T21:32:04.346014+00:00",
    "updated_at": "2026-04-08T21:32:04.346014+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This can become large, so pair/date access path matters more than almost any descriptive concern.
- Migration 045 converted `rate_text` (TEXT) to `rate` (REAL) under the project REAL-everywhere policy. Numeric filtering inside SQLite is now first-class.
- Migration 048 added the physical FK `provider -> provider(provider_code)`.
- Migration 058 added 3-char uppercase ASCII format CHECKs to `base_currency` and `quote_currency`.
- Migration 055 added the `source_kind IN ('provider')` CHECK; widen it via a future migration when synthesized or derived sources are introduced.
