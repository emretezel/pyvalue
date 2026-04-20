# `fx_rates`

## Purpose

Stores direct FX rates fetched from the provider.

## Grain

One row per `(provider, rate_date, base_currency, quote_currency)`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Row count: `6,819,876`
- Table size: `1,328,381,952 bytes` (`1.24 GiB`)
- Approximate bytes per row: `194.8`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider` | `TEXT` | no | PK, idx | provider namespace |
| `rate_date` | `TEXT` | no | PK, idx | effective date |
| `base_currency` | `TEXT` | no | PK, idx | base currency |
| `quote_currency` | `TEXT` | no | PK, idx | quote currency |
| `rate_text` | `TEXT` | no |  | stored decimal text to preserve precision |
| `fetched_at` | `TEXT` | no |  | provider fetch timestamp |
| `source_kind` | `TEXT` | no |  | direct provider source kind |
| `meta_json` | `TEXT` | yes |  | optional metadata |
| `created_at` | `TEXT` | no |  | insert timestamp |
| `updated_at` | `TEXT` | no |  | update timestamp |

## Keys And Relationships

- Primary key: `(provider, rate_date, base_currency, quote_currency)`
- No enforced foreign keys

## Secondary Indexes

- `idx_fx_rates_pair_date (provider, base_currency, quote_currency, rate_date DESC)`

## Main Read Paths

- direct pair/date lookups
- inverse and triangulated FX lookup support
- historical coverage checks

## Main Write Paths

- `refresh-fx-rates`

## Column Usage Notes

- `provider`: first filter in FX lookup queries.
- `rate_date`: date predicate and ordering column for direct-rate lookup.
- `base_currency`: first pair component in all direct FX searches.
- `quote_currency`: second pair component in all direct FX searches.
- `rate_text`: stored decimal value consumed by FX conversion logic.
- `fetched_at`: provider fetch timestamp for auditability.
- `source_kind`: indicates how the direct rate was sourced.
- `meta_json`: optional provider metadata, not part of hot lookup predicates.
- `created_at`: insert timestamp.
- `updated_at`: update timestamp.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Sample window: first `5` rows returned by SQLite using `LIMIT` with no `ORDER BY`

```json
[
  {
    "provider": "FRANKFURTER",
    "rate_date": "2026-03-12",
    "base_currency": "EUR",
    "quote_currency": "USD",
    "rate_text": "1.1563",
    "fetched_at": "Sun, 05 Apr 2026 15:37:24 GMT",
    "source_kind": "provider",
    "meta_json": "{\"provider\": \"FRANKFURTER\"}",
    "created_at": "2026-04-05T15:37:24.141920+00:00",
    "updated_at": "2026-04-05T15:37:24.141920+00:00"
  },
  {
    "provider": "FRANKFURTER",
    "rate_date": "2026-03-13",
    "base_currency": "EUR",
    "quote_currency": "USD",
    "rate_text": "1.1495",
    "fetched_at": "Sun, 05 Apr 2026 15:37:24 GMT",
    "source_kind": "provider",
    "meta_json": "{\"provider\": \"FRANKFURTER\"}",
    "created_at": "2026-04-05T15:37:24.141920+00:00",
    "updated_at": "2026-04-05T15:37:24.141920+00:00"
  },
  {
    "provider": "FRANKFURTER",
    "rate_date": "2026-03-14",
    "base_currency": "EUR",
    "quote_currency": "USD",
    "rate_text": "1.1466",
    "fetched_at": "Sun, 05 Apr 2026 15:37:24 GMT",
    "source_kind": "provider",
    "meta_json": "{\"provider\": \"FRANKFURTER\"}",
    "created_at": "2026-04-05T15:37:24.141920+00:00",
    "updated_at": "2026-04-05T15:37:24.141920+00:00"
  },
  {
    "provider": "FRANKFURTER",
    "rate_date": "2026-03-15",
    "base_currency": "EUR",
    "quote_currency": "USD",
    "rate_text": "1.1476",
    "fetched_at": "Sun, 05 Apr 2026 15:37:24 GMT",
    "source_kind": "provider",
    "meta_json": "{\"provider\": \"FRANKFURTER\"}",
    "created_at": "2026-04-05T15:37:24.141920+00:00",
    "updated_at": "2026-04-05T15:37:24.141920+00:00"
  },
  {
    "provider": "FRANKFURTER",
    "rate_date": "2026-03-16",
    "base_currency": "EUR",
    "quote_currency": "USD",
    "rate_text": "1.1473",
    "fetched_at": "Sun, 05 Apr 2026 15:37:24 GMT",
    "source_kind": "provider",
    "meta_json": "{\"provider\": \"FRANKFURTER\"}",
    "created_at": "2026-04-05T15:37:24.141920+00:00",
    "updated_at": "2026-04-05T15:37:24.141920+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This can become large, so pair/date access path matters more than almost any descriptive concern
- Review the `rate_text` choice against any future need for numeric filtering inside SQLite
