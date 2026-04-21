# `issuer`

## Purpose

Stores issuer-level descriptive metadata separately from exchange-specific listings.

## Grain

One row per issuer record created during catalog backfill or listing creation.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: expected post-refactor split from pre-refactor `securities` rows on `2026-04-21`
- Row count: approximately `77,484`
- Table size: depends on migrated issuer metadata volume
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `issuer_id` | `INTEGER` | no | PK | issuer surrogate key |
| `name` | `TEXT` | yes |  | display name |
| `description` | `TEXT` | yes |  | long provider-derived description |
| `sector` | `TEXT` | yes |  | cached business sector |
| `industry` | `TEXT` | yes |  | cached business industry |
| `country` | `TEXT` | yes |  | issuer or provider country hint |

## Keys And Relationships

- Primary key: `issuer_id`
- Physical references:
  - `listing.issuer_id -> issuer.issuer_id`

## Secondary Indexes

- None.

## Main Read Paths

- display metadata joins for reports, screen output, and diagnostics

## Main Write Paths

- migration-time backfill from legacy security metadata
- metadata refreshes from stored fundamentals

## Review Notes

- `issuer` intentionally has no provider key. Provider-specific descriptive metadata should remain in provider-owned tables or raw payloads unless promoted deliberately.
