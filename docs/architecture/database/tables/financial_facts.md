# `financial_facts`

## Purpose

Stores provider-agnostic normalized financial facts for metrics.

## Grain

One row per fact version keyed by listing, concept, period/date, unit, and accession number.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Row count: `103,188,287`
- Table size: `9,183,584,256 bytes` (`8.55 GiB`)
- Approximate bytes per row: `89.0`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK, idx | canonical listing link |
| `cik` | `TEXT` | yes |  | SEC identifier when available |
| `concept` | `TEXT` | no | PK, idx | normalized concept |
| `fiscal_period` | `TEXT` | yes | PK | FY, Q1, TTM, and so on |
| `end_date` | `TEXT` | no | PK, idx | fact period end |
| `unit` | `TEXT` | no | PK | unit or semantic label |
| `value` | `REAL` | no |  | numeric fact value |
| `accn` | `TEXT` | yes | PK | filing/accession discriminator |
| `filed` | `TEXT` | yes | idx | filing date for latest-row ordering |
| `frame` | `TEXT` | yes |  | provider frame string |
| `start_date` | `TEXT` | yes |  | period start for duration facts |
| `accounting_standard` | `TEXT` | yes |  | provider accounting basis |
| `currency` | `TEXT` | yes | partial idx | ISO currency for monetary facts |
| `source_provider` | `TEXT` | yes |  | provenance |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`listing_id`, `concept`, `fiscal_period`, `end_date`, `unit`, `accn`)
- Physical foreign keys: none
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `listing_id` in `listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_fin_facts_currency_nonnull (currency)` WHERE currency IS NOT NULL
- `idx_fin_facts_security_concept_latest (listing_id, concept, end_date DESC, filed DESC)`
- `idx_fin_facts_concept (concept)`
- `idx_fin_facts_security_concept (listing_id, concept)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- bulk fact preload for metric computation
- latest fact lookup by `listing_id + concept`
- FX currency discovery

## Main Write Paths

- `normalize-fundamentals`
- purge when a listing becomes secondary

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC, concept ASC, fiscal_period ASC, end_date ASC, unit ASC, accn ASC`

```json
[
  {
    "listing_id": 1,
    "cik": null,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "2000-12-31",
    "unit": "EUR",
    "value": 381357000.0,
    "accn": null,
    "filed": null,
    "frame": "CY2000",
    "start_date": null,
    "accounting_standard": null,
    "currency": "EUR",
    "source_provider": "EODHD"
  },
  {
    "listing_id": 1,
    "cik": null,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "2001-12-31",
    "unit": "EUR",
    "value": 536399000.0,
    "accn": null,
    "filed": null,
    "frame": "CY2001",
    "start_date": null,
    "accounting_standard": null,
    "currency": "EUR",
    "source_provider": "EODHD"
  },
  {
    "listing_id": 1,
    "cik": null,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "2002-12-31",
    "unit": "EUR",
    "value": 735651000.0,
    "accn": null,
    "filed": "2002-12-31",
    "frame": "CY2002",
    "start_date": null,
    "accounting_standard": null,
    "currency": "EUR",
    "source_provider": "EODHD"
  },
  {
    "listing_id": 1,
    "cik": null,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "2003-12-31",
    "unit": "EUR",
    "value": 699151000.0,
    "accn": null,
    "filed": "2003-12-31",
    "frame": "CY2003",
    "start_date": null,
    "accounting_standard": null,
    "currency": "EUR",
    "source_provider": "EODHD"
  },
  {
    "listing_id": 1,
    "cik": null,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "2004-12-31",
    "unit": "EUR",
    "value": 823703000.0,
    "accn": null,
    "filed": "2004-12-31",
    "frame": "CY2004",
    "start_date": null,
    "accounting_standard": null,
    "currency": "EUR",
    "source_provider": "EODHD"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This is one of the hottest tables. Keep `listing_id` and concept-based lookup paths indexed and avoid widening rows without a measured need.
