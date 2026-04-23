# `fx_supported_pairs`

## Purpose

Stores the provider FX instrument catalog, including aliases and canonical refreshable pairs.

## Grain

One row per provider FX symbol.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Row count: `990`
- Table size: `102,400 bytes` (`100.0 KiB`)
- Approximate bytes per row: `103.4`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider` | `TEXT` | no | PK | provider namespace |
| `symbol` | `TEXT` | no | PK | provider FX symbol |
| `canonical_symbol` | `TEXT` | no | idx | canonical six-letter pair |
| `base_currency` | `TEXT` | yes |  | base currency |
| `quote_currency` | `TEXT` | yes |  | quote currency |
| `name` | `TEXT` | yes |  | provider display name |
| `is_alias` | `INTEGER` | no |  | alias flag |
| `is_refreshable` | `INTEGER` | no | idx | canonical fetchable flag |
| `last_seen_at` | `TEXT` | no |  | catalog refresh timestamp |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`provider`, `symbol`)
- Physical foreign keys: none
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: canonical pair used by `fx_refresh_state`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_fx_supported_pairs_refreshable (provider, is_refreshable, canonical_symbol)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- FX catalog refresh planning
- alias-to-canonical pair resolution

## Main Write Paths

- `refresh-fx-rates`

## Column Usage Notes

- `provider`: provider namespace for the FX catalog.
- `symbol`: provider FX symbol used when refreshing or resolving aliases.
- `canonical_symbol`: normalized six-letter pair used by refresh state and canonical refresh planning.
- `base_currency`: parsed catalog metadata for the pair.
- `quote_currency`: parsed catalog metadata for the pair.
- `name`: display metadata only.
- `is_alias`: separates shorthand or alias rows from canonical instruments.
- `is_refreshable`: used directly by refresh planning to pick fetchable pairs.
- `last_seen_at`: catalog freshness marker.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-23`
- Sample window: first `5` rows returned by SQLite ordered by `provider ASC, symbol ASC`

```json
[
  {
    "provider": "EODHD",
    "symbol": "AED",
    "canonical_symbol": "USDAED",
    "base_currency": "USD",
    "quote_currency": "AED",
    "name": "US Dollar/United Arab Emirates dirham FX Spot Rate",
    "is_alias": 1,
    "is_refreshable": 0,
    "last_seen_at": "2026-04-11T09:54:34.997203+00:00"
  },
  {
    "provider": "EODHD",
    "symbol": "AEDAUD",
    "canonical_symbol": "AEDAUD",
    "base_currency": "AED",
    "quote_currency": "AUD",
    "name": "UAE Dirham/Australian Dollar",
    "is_alias": 0,
    "is_refreshable": 1,
    "last_seen_at": "2026-04-11T09:54:34.997203+00:00"
  },
  {
    "provider": "EODHD",
    "symbol": "AEDCAD",
    "canonical_symbol": "AEDCAD",
    "base_currency": "AED",
    "quote_currency": "CAD",
    "name": "UAE Dirham/Canadian Dollar",
    "is_alias": 0,
    "is_refreshable": 1,
    "last_seen_at": "2026-04-11T09:54:34.997203+00:00"
  },
  {
    "provider": "EODHD",
    "symbol": "AEDCHF",
    "canonical_symbol": "AEDCHF",
    "base_currency": "AED",
    "quote_currency": "CHF",
    "name": "UAE Dirham/Swiss Franc",
    "is_alias": 0,
    "is_refreshable": 1,
    "last_seen_at": "2026-04-11T09:54:34.997203+00:00"
  },
  {
    "provider": "EODHD",
    "symbol": "AEDEUR",
    "canonical_symbol": "AEDEUR",
    "base_currency": "AED",
    "quote_currency": "EUR",
    "name": "UAE Dirham/Euro FX Cross Rate",
    "is_alias": 0,
    "is_refreshable": 1,
    "last_seen_at": "2026-04-11T09:54:34.997203+00:00"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- Alias support is useful, but it also makes the model less obvious than a single canonical-pair table
- Review whether non-refreshable alias rows are worth retaining once canonical pairs are resolved
