# `issuer`

## Purpose

Stores issuer-level descriptive metadata separately from exchange-specific listings.

## Grain

One row per issuer record created during catalog backfill or listing creation.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Row count: `68,728`
- Table size: `64,876,544 bytes` (`61.9 MiB`)
- Approximate bytes per row: `944.0`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `issuer_id` | `INTEGER` | no | PK | issuer surrogate key |
| `name` | `TEXT` | no |  | display name; migration 064 dropped 260 legacy orphan NULL-name rows and tightened the column to NOT NULL. The runtime ingest path falls back to the canonical_symbol when the upstream catalog doesn't supply a name. |
| `description` | `TEXT` | yes |  | long provider-derived description |
| `sector` | `TEXT` | yes |  | cached business sector |
| `industry` | `TEXT` | yes |  | cached business industry |
| `country` | `TEXT` | yes |  | issuer or provider country hint |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `issuer_id`
- Physical foreign keys: none
- Physical references from other tables:
  - `listing`.`issuer_id` -> `issuer_id`
- Unique constraints beyond the primary key:
  - (`name`, `country`)
- Main logical refs: referenced physically by `listing.issuer_id`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- display metadata joins for reports, screen output, and diagnostics

## Main Write Paths

- `refresh-supported-tickers` — creates issuers while cataloguing listings;
  it never deletes them (canonical identity survives a prune that leaves the
  issuer's listings unmapped — 2026-07-11 design)
- migration-time backfill from legacy security metadata
- metadata refreshes from stored fundamentals
- runtime identity merge — both rename paths (the catalog refresh and the
  fundamentals metadata promotion) route through
  `SecurityRepository._apply_issuer_metadata`, which merges an issuer into an
  existing `(name, country)` row when a rename would collide with the UNIQUE
  index (see Review Notes)

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Sample window: first `5` rows returned by SQLite ordered by `issuer_id ASC`

```json
[
  {
    "issuer_id": 1,
    "name": "Aalberts Industries NV",
    "description": "Aalberts N.V., together with its subsidiaries, offers mission-critical technologies for building, industry, and semicon markets in Europe, the United States, the Asia Pacific, the Middle East, and Africa. The company operates through Building, Industry, and Semicon segments. It offers hydronic flow control systems for heating and cooling to enhance energy efficiency; integrated piping systems to d... <truncated; 966 bytes total>",
    "sector": "Industrials",
    "industry": "Specialty Industrial Machinery",
    "country": "Netherlands"
  },
  {
    "issuer_id": 2,
    "name": "ABN Amro Group NV",
    "description": "ABN AMRO Bank N.V. provides various banking products and financial services to retail, private, and corporate banking clients in the Netherlands, rest of Europe, the United States, Asia, and internationally. It operates through three segments: Personal & Business Banking, Wealth Management, and Corporate Banking. The company offers fixed deposits; home improvement; mortgage products; investment pr... <truncated; 925 bytes total>",
    "sector": "Financial Services",
    "industry": "Banks - Diversified",
    "country": "Netherlands"
  },
  {
    "issuer_id": 3,
    "name": "Amsterdam Commodities NV",
    "description": "Acomo N.V., together with its subsidiaries, engages in sourcing, trading, processing, packaging, and distributing conventional and organic food ingredients and solutions for the food and beverage industry in the Netherlands, Germany, other European countries, North America, and internationally. It operates through five segments: Spices and Nuts, Edible Seeds, Organic Ingredients, Tea, and Food Sol... <truncated; 1893 bytes total>",
    "sector": "Consumer Defensive",
    "industry": "Food Distribution",
    "country": "Netherlands"
  },
  {
    "issuer_id": 4,
    "name": "Koninklijke Ahold Delhaize NV",
    "description": "Koninklijke Ahold Delhaize N.V. operates retail food stores and e-commerce in the Netherlands, the United States, and internationally. The company's stores offer produce, dairy, meat, deli, bakery, seafood, and frozen products; grocery, beer, and wine; floral, pet food, health and beauty care, kitchen and cookware, gardening tools, general merchandise articles, electronics, newspapers and magazine... <truncated; 930 bytes total>",
    "sector": "Consumer Defensive",
    "industry": "Grocery Stores",
    "country": "Netherlands"
  },
  {
    "issuer_id": 5,
    "name": "Adyen NV",
    "description": "Adyen N.V. operates a payments platform in Europe, the Middle East, Africa, North America, the Asia Pacific, and Latin America. Its platform integrates payments stack, including gateway, risk management, processing, acquiring, and settlement services. The company offers a back-end infrastructure for authorizing. It accepts payment through online, in-person payments, cross channel, and Adyen for Pl... <truncated; 816 bytes total>",
    "sector": "Technology",
    "industry": "Software - Infrastructure",
    "country": "Netherlands"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- `issuer` intentionally has no provider key. Provider-specific descriptive metadata should remain in provider-owned tables or raw payloads unless promoted deliberately.
- Migration 064 deleted 260 legacy orphan rows (NULL name, no
  provider_listing, no fundamentals, no metrics — only stale
  market_data) and tightened `name` to NOT NULL. The runtime ingest
  path supplies `canonical_symbol` as a fallback when the upstream
  catalog doesn't carry an issuer name; downstream metadata refreshes
  can later promote the placeholder to the real entity name.
- Migration 060 deduplicated `(name, country)` groups before adding
  the UNIQUE INDEX. The pre-canonical-name ingest path
  (`SecurityRepository.ensure`) keyed its existence check on
  `(exchange_id, symbol)` rather than `(name, country)`, so the same
  real-world issuer (Petrobras across 22 German venues, dual-listed
  Korean tickers, etc.) accumulated one `issuer` row per listing
  instead of one row per entity. The migration kept the row with the
  lowest `issuer_id` per group as canonical, COALESCE-promoted any
  non-NULL `description` / `sector` / `industry` from the rest of the
  group onto it, remapped `listing.issuer_id` references, and deleted
  the losers. Rows with a NULL `name` or NULL `country` were left
  alone — SQLite's UNIQUE INDEX treats NULLs as distinct, and merging
  on a NULL key would conflate unrelated companies (e.g. 260 US
  closed-end-fund issuers whose listings are unrelated).
- The runtime rename paths enforce the same identity. `issuer.country`
  is written only by the migration-era backfill (the catalog path
  inserts NULL), so most issuers carry a country while providers keep
  restyling display names. When a rename would land on a `(name,
  country)` pair another issuer already holds, a blind `UPDATE` would
  violate `idx_issuer_name_country` and abort the whole refresh
  transaction (this killed `refresh-supported-tickers` on BE, 2026-07:
  ~2k Berlin listings renamed onto sibling-venue identities).
  `SecurityRepository._apply_issuer_metadata` therefore merges at
  runtime with migration 060's exact rules: the survivor's non-NULL
  metadata is never overwritten (backfill order: payload, then the
  merged-away row), all of the source issuer's listings are repointed,
  and the emptied source row is deleted. NULL-country renames never
  merge — they remain plain updates, and same-name NULL-country
  duplicates stay legitimate.
