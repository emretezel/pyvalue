# `security_listing_status`

## Purpose

Caches EODHD primary-vs-secondary listing classification so downstream scopes can exclude secondary listings without reparsing raw JSON.

## Grain

One row per canonical `listing_id`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Row count: `75,848`
- Table size: `8,540,160 bytes` (`8.1 MiB`)
- Approximate bytes per row: `112.6`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK, FK | canonical listing identity |
| `source_provider` | `TEXT` | no |  | provider that supplied the classification |
| `provider_listing_id` | `INTEGER` | no | FK | provider listing used for the classification |
| `raw_fetched_at` | `TEXT` | no |  | raw payload timestamp |
| `is_primary_listing` | `INTEGER` | no | idx | `0` or `1` |
| `primary_provider_listing_id` | `INTEGER` | yes | FK | provider listing for the primary ticker when known |
| `classification_basis` | `TEXT` | no |  | match/mismatch/missing-primary reason |
| `updated_at` | `TEXT` | no |  | status update timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `listing_id`
- Physical foreign keys:
  - `primary_provider_listing_id` -> `provider_listing`.`provider_listing_id`
  - `provider_listing_id` -> `provider_listing`.`provider_listing_id`
  - `listing_id` -> `listing`.`listing_id`
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `listing_id` in `listing`, `provider_listing_id` in `provider_listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_security_listing_status_primary (is_primary_listing, listing_id)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- primary-only scope resolution for normalization, market data, metrics, and screens
- cleanup planning when a listing is reclassified as secondary

## Main Write Paths

- `reconcile-listing-status`
- raw fundamentals upserts that classify EODHD listings

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC`

```json
[
  {
    "listing_id": 1,
    "source_provider": "EODHD",
    "provider_listing_id": 1,
    "raw_fetched_at": "2026-03-22T13:53:47.387172+00:00",
    "is_primary_listing": 1,
    "primary_provider_listing_id": 1,
    "classification_basis": "matched_primary_ticker",
    "updated_at": "2026-04-19T00:09:24.569416+00:00"
  },
  {
    "listing_id": 2,
    "source_provider": "EODHD",
    "provider_listing_id": 2,
    "raw_fetched_at": "2026-03-22T13:53:47.613748+00:00",
    "is_primary_listing": 1,
    "primary_provider_listing_id": 2,
    "classification_basis": "matched_primary_ticker",
    "updated_at": "2026-04-19T00:09:25.284663+00:00"
  },
  {
    "listing_id": 3,
    "source_provider": "EODHD",
    "provider_listing_id": 3,
    "raw_fetched_at": "2026-03-22T13:53:47.909077+00:00",
    "is_primary_listing": 1,
    "primary_provider_listing_id": 3,
    "classification_basis": "matched_primary_ticker",
    "updated_at": "2026-04-19T00:09:26.129977+00:00"
  },
  {
    "listing_id": 4,
    "source_provider": "EODHD",
    "provider_listing_id": 4,
    "raw_fetched_at": "2026-03-22T13:53:48.236603+00:00",
    "is_primary_listing": 1,
    "primary_provider_listing_id": 4,
    "classification_basis": "matched_primary_ticker",
    "updated_at": "2026-04-19T00:09:26.484400+00:00"
  },
  {
    "listing_id": 5,
    "source_provider": "EODHD",
    "provider_listing_id": 5,
    "raw_fetched_at": "2026-03-22T13:53:48.456762+00:00",
    "is_primary_listing": 1,
    "primary_provider_listing_id": 5,
    "classification_basis": "matched_primary_ticker",
    "updated_at": "2026-04-19T00:09:27.344101+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- The table remains canonical-listing keyed because downstream exclusion filters start from canonical scopes.
