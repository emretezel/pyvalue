# `issuer`

## Purpose

Stores issuer-level descriptive metadata separately from exchange-specific listings.

## Grain

One row per legal entity, as far as the stored identifiers can establish it.

This was aspirational until migration 089. Identity was `(name, country)`,
`country` is NULL on 65,752 of 70,564 rows, and SQLite treats NULLs as distinct
in a UNIQUE index — so in practice each listing got its own issuer row and the
154 QARP passers mapped to 151 issuers. `reconcile-issuer-identity` now collapses
listings that share an ISIN or an LEI onto one row.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-07-11`
- Row count: `70,564`
- Table size: `64,913,408 bytes` (`61.9 MiB`)
- Approximate bytes per row: `919.9`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `issuer_id` | `INTEGER` | no | PK | issuer surrogate key |
| `lei` | `TEXT` | yes | UNIQUE | ISO 17442 legal-entity identifier — the natural key. Added by migration 089, which ships it NULL; `reconcile-issuer-identity` derives it. Nullable because EODHD publishes an LEI for roughly a quarter of listings; the rest are identified only by the listings that point at them. CHECK enforces 20 uppercase alphanumerics |
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
  - `lei`
- Main logical refs: referenced physically by `listing.issuer_id`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- display metadata joins for reports, screen output, and diagnostics

## Main Write Paths

- `reconcile-issuer-identity` — the sole writer of `issuer.lei`, and the only
  path that merges issuer rows in bulk. It groups listings by the ISIN and LEI
  stored on `listing`, repoints them onto the group's lowest `issuer_id`, and
  deletes the emptied rows. Unscoped by design: a partial view would split
  entities rather than merge them
- `refresh-supported-tickers` — creates issuers while cataloguing listings;
  it never deletes them (canonical identity survives a prune that leaves the
  issuer's listings unmapped — 2026-07-11 design). It cannot group by LEI: the
  provider's symbol list does not carry one, so grouping has to happen later,
  where the fundamentals payload supplies it
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

- Grouping rests on **identifiers only** — never on names that look alike. The
  rule lives in `pyvalue.universe.issuer_identity`. The LEI is read from each
  listing's stored payload (`General.LEI`), not from a column on `listing`: a
  cached copy there duplicated this one, since after a converged reconcile a
  listing's LEI is functionally determined by its issuer. That is why this
  command is a payload scan rather than an indexed read. A shared LEI means the same
  legal entity (including different share classes, which is why Alphabet's
  `ABEA`/`ABEC` and Bank of America's common plus preferreds collapse into one
  row), and a shared ISIN means the same security, which implies the same
  entity. Links are transitive.
- **`(name, country)` is no longer unique, deliberately.** Migration 060 made it
  the issuer natural key; it was always an approximation, since two distinct
  legal entities can share a name and a country. Worse, treating it as identity
  is what caused the damage this table's grain now repairs: the runtime rename
  path *merged* issuers whenever a provider restyled one listing's name onto
  another's, fusing unrelated companies into shared parent rows. Migration 089
  drops the index outright rather than demoting it — nothing reads `issuer` by
  name, and its only consumers were collision probes that existed solely to
  avoid violating it.
- **A depositary receipt is not grouped with its underlying.** A receipt is a
  distinct security with its own ISIN (`DNLMY.US` is `US26543P1030` where
  `DNLM.LSE` is `GB00B1CKQ739`) and receipts rarely carry an LEI, so neither
  identifier bridges the pair — and no evidence pyvalue currently ingests does.
  This is a known limit, pinned by a regression test so it stays known.
- A shared ISIN spanning two different LEIs is contradictory evidence; the link
  is skipped rather than guessed at, since silently merging two real companies
  is far worse than leaving them apart.
- The surviving row is the group's lowest `issuer_id`, which makes repeated runs
  converge instead of reshuffling parents. It takes its name from the group's
  primary listing — a company is better described by the name on its primary
  line than by an ADR's `... PLC ADR` or a German regional line's abbreviation —
  and inherits any metadata only an absorbed row carried (migration 060's
  never-overwrite rule).


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
