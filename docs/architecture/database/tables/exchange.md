# `exchange`

## Purpose

Stores the canonical exchange identity registry shared across providers.

## Grain

One row per canonical exchange code.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-25`
- Row count: `73`
- Table size: `12,288 bytes` (`12.0 KiB`)
- Approximate bytes per row: `168.3`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `exchange_id` | `INTEGER` | no | PK | surrogate canonical exchange identifier |
| `exchange_code` | `TEXT` | no | unique | stable uppercase canonical exchange code such as `US` or `LSE` |
| `created_at` | `TEXT` | no |  | initial insert timestamp |
| `updated_at` | `TEXT` | no |  | last maintenance timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `exchange_id`
- Physical foreign keys: none
- Physical references from other tables:
  - `listing`.`exchange_id` -> `exchange_id`
  - `provider_exchange`.`exchange_id` -> `exchange_id`
- Unique constraints beyond the primary key:
  - `exchange_code`
- Main logical refs: referenced physically by `provider_exchange.exchange_id` and `listing.exchange_id`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- canonical exchange lookup during provider-catalog joins
- low-cardinality exchange review and debugging

## Main Write Paths

- migration-time backfill from legacy `supported_exchanges`
- canonical exchange upserts during provider exchange refreshes

## Column Usage Notes

- `exchange_id`: stable canonical key for new normalized exchange relationships.
- `exchange_code`: still the canonical exchange symbol used elsewhere in the app during this phase.
- `created_at`: original insert timestamp for the canonical row.
- `updated_at`: latest touch timestamp from migration or catalog maintenance.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-25`
- Sample window: first `5` rows returned by SQLite ordered by `exchange_id ASC`

```json
[
  {
    "exchange_id": 1,
    "exchange_code": "AS",
    "created_at": "2026-03-22T10:57:47.052304+00:00",
    "updated_at": "2026-04-23T16:33:15.433812+00:00"
  },
  {
    "exchange_id": 2,
    "exchange_code": "AT",
    "created_at": "2026-03-22T10:57:47.052304+00:00",
    "updated_at": "2026-04-23T16:33:15.433812+00:00"
  },
  {
    "exchange_id": 3,
    "exchange_code": "AU",
    "created_at": "2026-03-22T10:57:47.052304+00:00",
    "updated_at": "2026-04-23T16:33:15.433812+00:00"
  },
  {
    "exchange_id": 4,
    "exchange_code": "BA",
    "created_at": "2026-03-22T10:57:47.052304+00:00",
    "updated_at": "2026-04-23T16:33:15.433812+00:00"
  },
  {
    "exchange_id": 5,
    "exchange_code": "BC",
    "created_at": "2026-03-22T10:57:47.052304+00:00",
    "updated_at": "2026-04-23T16:33:15.433812+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Keep this table narrow; provider-owned exchange metadata belongs in `provider_exchange`.
- Avoid drifting provider-owned metadata into the canonical exchange layer.
