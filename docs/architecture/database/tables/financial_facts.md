# `financial_facts`

## Purpose

Stores provider-agnostic normalized financial facts for metrics.

## Grain

One row per fact version keyed by security, concept, period/date, unit, and accession number.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `103,188,287`
- Table size: `9,316,958,208 bytes` (`8.68 GiB`)
- Approximate bytes per row: `90.3`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `security_id` | `INTEGER` | no | PK | canonical identity link |
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

- Primary key: `(security_id, concept, fiscal_period, end_date, unit, accn)`
- Logical references:
  - `security_id` to `securities`
- No enforced foreign keys

## Secondary Indexes

- `idx_fin_facts_security_concept (security_id, concept)`
- `idx_fin_facts_concept (concept)`
- `idx_fin_facts_security_concept_latest (security_id, concept, end_date DESC, filed DESC)`
- `idx_fin_facts_currency_nonnull (currency) WHERE currency IS NOT NULL`

## Main Read Paths

- `compute-metrics` bulk fact preload
- latest fact lookup by `security_id + concept`
- FX currency discovery

## Main Write Paths

- `normalize-fundamentals`
- purge when a listing becomes secondary

## Column Usage Notes

- `security_id`: hottest join/filter key in metric fact loading.
- `cik`: mainly SEC provenance; not central to hot metric queries.
- `concept`: one of the two main fact lookup filters in metric queries.
- `fiscal_period`: used by period-aware metric logic such as FY, quarter, and TTM selection.
- `end_date`: part of latest-fact ordering and period selection.
- `unit`: distinguishes shares, currency amounts, and other fact units; part of the dedupe key.
- `value`: numeric payload consumed directly by metrics.
- `accn`: filing/accession discriminator inside the primary key; rarely used as a standalone filter.
- `filed`: secondary ordering key for latest-fact selection.
- `frame`: stored provider frame metadata with light read use.
- `start_date`: used for duration-aware facts where period start matters.
- `accounting_standard`: stored for provenance and edge-case interpretation, not hot-path filtering.
- `currency`: used by FX discovery and monetary-fact normalization logic.
- `source_provider`: provenance marker, not a common filter.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Sample window: first `5` rows returned by SQLite using `LIMIT` with no `ORDER BY`

```json
[
  {
    "security_id": 28479,
    "cik": null,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "2004-12-31",
    "unit": "EUR",
    "value": 47782000.0,
    "accn": null,
    "filed": "2004-12-31",
    "frame": "CY2004",
    "start_date": null,
    "accounting_standard": null,
    "currency": "EUR",
    "source_provider": null
  },
  {
    "security_id": 28479,
    "cik": null,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "2005-12-31",
    "unit": "EUR",
    "value": 47932000.0,
    "accn": null,
    "filed": "2005-12-31",
    "frame": "CY2005",
    "start_date": null,
    "accounting_standard": null,
    "currency": "EUR",
    "source_provider": null
  },
  {
    "security_id": 28479,
    "cik": null,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "2006-12-31",
    "unit": "EUR",
    "value": 51546000.0,
    "accn": null,
    "filed": "2006-12-31",
    "frame": "CY2006",
    "start_date": null,
    "accounting_standard": null,
    "currency": "EUR",
    "source_provider": null
  },
  {
    "security_id": 28479,
    "cik": null,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "2007-12-31",
    "unit": "EUR",
    "value": 54154000.0,
    "accn": null,
    "filed": "2007-12-31",
    "frame": "CY2007",
    "start_date": null,
    "accounting_standard": null,
    "currency": "EUR",
    "source_provider": null
  },
  {
    "security_id": 28479,
    "cik": null,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "2008-12-31",
    "unit": "EUR",
    "value": 55372000.0,
    "accn": null,
    "filed": "2008-12-31",
    "frame": "CY2008",
    "start_date": null,
    "accounting_standard": null,
    "currency": "EUR",
    "source_provider": null
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This is the hottest analytical table in the repo
- The `idx_fin_facts_security_concept_latest` index is central to metric performance
- Because this is a rowid table with nullable PK components such as `fiscal_period` and `accn`, duplicate-key behavior deserves explicit review
- Check whether rarely used columns like `frame` or `accounting_standard` justify their write and storage cost on the hot fact table
