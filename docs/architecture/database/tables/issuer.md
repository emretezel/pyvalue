# `issuer`

## Purpose

Stores issuer-level descriptive metadata separately from exchange-specific listings.

## Grain

One row per issuer record created during catalog backfill or listing creation.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-25`
- Row count: `77,484`
- Table size: `68,800,512 bytes` (`65.6 MiB`)
- Approximate bytes per row: `887.9`
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

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: `issuer_id`
- Physical foreign keys: none
- Physical references from other tables:
  - `listing`.`issuer_id` -> `issuer_id`
- Unique constraints beyond the primary key: none
- Main logical refs: referenced physically by `listing.issuer_id`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- None beyond the primary key and unique constraints.
<!-- END generated_secondary_indexes -->

## Main Read Paths

- display metadata joins for reports, screen output, and diagnostics

## Main Write Paths

- migration-time backfill from legacy security metadata
- metadata refreshes from stored fundamentals

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-25`
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
