# `securities`

## Purpose

Stores canonical security identity and selected display metadata.

## Grain

One row per canonical symbol, defined by `canonical_ticker + canonical_exchange_code`.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `77,484`
- Table size: `81,268,736 bytes` (`77.5 MiB`)
- Approximate bytes per row: `1,048.8`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `security_id` | `INTEGER` | no | PK | canonical surrogate key |
| `canonical_ticker` | `TEXT` | no | unique | canonical ticker |
| `canonical_exchange_code` | `TEXT` | no | unique | canonical exchange code |
| `canonical_symbol` | `TEXT` | no | unique | `<ticker>.<exchange>` |
| `entity_name` | `TEXT` | yes |  | display name |
| `description` | `TEXT` | yes |  | long description |
| `created_at` | `TEXT` | no |  | creation timestamp |
| `updated_at` | `TEXT` | no |  | update timestamp |
| `sector` | `TEXT` | yes |  | cached business metadata |
| `industry` | `TEXT` | yes |  | cached business metadata |

## Keys And Relationships

- Primary key: `security_id`
- Unique constraints:
  - `(canonical_exchange_code, canonical_ticker)`
  - `(canonical_symbol)`
- Logical references:
  - most downstream tables reference `security_id`

## Secondary Indexes

- `idx_securities_exchange (canonical_exchange_code)`
  - supports exchange-scoped canonical symbol resolution

## Main Read Paths

- symbol scope resolution for normalization, market data, metrics, and screening
- metadata lookup for entity name, sector, and industry

## Main Write Paths

- `refresh-supported-tickers`
- `refresh-security-metadata`

## Column Usage Notes

- `security_id`: canonical join key for almost every downstream table.
- `canonical_ticker`: used when building canonical identity and unique constraints.
- `canonical_exchange_code`: used for exchange-scoped symbol resolution and filtering.
- `canonical_symbol`: the main canonical symbol exposed to CLI scopes and screen/report reads.
- `entity_name`: used in user-facing listings and reports.
- `description`: stored metadata with light read usage relative to the rest of the table.
- `created_at`: audit metadata, not a hot filter.
- `updated_at`: used as a metadata freshness marker.
- `sector`: refreshed from stored fundamentals and surfaced in metadata reads.
- `industry`: refreshed from stored fundamentals and surfaced in metadata reads.

## Review Notes

- This is the identity root of the schema, so key changes are expensive
- Check whether `description`, `sector`, and `industry` belong on the identity table or should live in a lighter metadata cache
