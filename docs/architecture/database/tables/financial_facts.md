# `financial_facts`

## Purpose

Stores provider-agnostic normalized financial facts for metrics.

## Grain

One row per fact version keyed by listing, concept, period/date, unit, and accession number.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: pre-refactor `data/pyvalue.db` facts table on `2026-04-21`
- Row count: `103,188,287`
- Table size: approximately `8.68 GiB` before the `listing_id` rename
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

- Primary key: `(listing_id, concept, fiscal_period, end_date, unit, accn)`
- Logical reference: `listing_id -> listing.listing_id`

## Secondary Indexes

- `idx_fin_facts_security_concept (listing_id, concept)`
- `idx_fin_facts_concept (concept)`
- `idx_fin_facts_security_concept_latest (listing_id, concept, end_date DESC, filed DESC)`
- `idx_fin_facts_currency_nonnull (currency) WHERE currency IS NOT NULL`

## Main Read Paths

- bulk fact preload for metric computation
- latest fact lookup by `listing_id + concept`
- FX currency discovery

## Main Write Paths

- `normalize-fundamentals`
- purge when a listing becomes secondary

## Review Notes

- This is one of the hottest tables. Keep `listing_id` and concept-based lookup paths indexed and avoid widening rows without a measured need.
