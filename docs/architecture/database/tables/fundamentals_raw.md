# `fundamentals_raw`

## Purpose

Stores the latest raw fundamentals payload per provider symbol.

## Grain

One row per `(provider, provider_symbol)`, containing the latest fetched raw payload.

## Live Stats

- Snapshot source: `data/pyvalue.db` on `2026-04-19`
- Row count: `77,045`
- Table size: `18,079,010,816 bytes` (`16.84 GiB`)
- Approximate bytes per row: `234,655.2`

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `provider` | `TEXT` | no | PK | provider namespace |
| `provider_symbol` | `TEXT` | no | PK | provider fetch key |
| `security_id` | `INTEGER` | no | idx | canonical identity link |
| `provider_exchange_code` | `TEXT` | yes |  | provider exchange code at fetch time |
| `currency` | `TEXT` | yes |  | provider payload currency hint |
| `data` | `TEXT` | no |  | raw JSON payload |
| `fetched_at` | `TEXT` | no | idx | last fetch timestamp |

## Keys And Relationships

- Primary key: `(provider, provider_symbol)`
- Logical references:
  - `security_id` to `securities`
  - `(provider, provider_symbol)` to `supported_tickers`

## Secondary Indexes

- `idx_fundamentals_raw_security (security_id)`
- `idx_fundamentals_raw_provider_fetched (provider, fetched_at)`

## Main Read Paths

- normalization reads by provider symbol or canonical security
- security metadata refresh
- listing-status reconciliation

## Main Write Paths

- `ingest-fundamentals`
- `reconcile-listing-status` reads existing rows but does not re-download

## Column Usage Notes

- `provider`: first-stage filter for raw ingest and reconciliation.
- `provider_symbol`: operational key used to pull one stored provider payload back out for normalization and metadata refresh.
- `security_id`: canonical link used by normalization and purge logic.
- `provider_exchange_code`: retained for exchange-scoped reconciliation and audit context.
- `currency`: fallback payload currency hint; not a hot filter.
- `data`: the raw JSON blob read by normalization, listing classification, and metadata refresh logic.
- `fetched_at`: incremental watermark for normalization and listing-status reconciliation.

## Sample Rows

<!-- BEGIN generated_sample_rows -->
Wide-table sample rows live in the [Sample Rows appendix](../sample-rows.md#fundamentals_raw).
<!-- END generated_sample_rows -->

## Review Notes

- The `data` column is the widest row in the schema and a natural I/O hotspot
- This table stores only the latest raw payload, not a historical chain of payload versions
- Check whether `provider_exchange_code` and `currency` are still worth storing outside the JSON payload
