# `securities`

## Purpose

Stores canonical security identity and selected display metadata.

## Grain

One row per canonical symbol, defined by `canonical_ticker + canonical_exchange_code`.

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Row count: `77,484`
- Table size: `81,268,736 bytes` (`77.5 MiB`)
- Approximate bytes per row: `1,048.8`
<!-- END generated_live_stats -->

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

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-04-20`
- Sample window: first `5` rows returned by SQLite using `LIMIT` with no `ORDER BY`

```json
[
  {
    "security_id": 1,
    "canonical_ticker": "AALB",
    "canonical_exchange_code": "AS",
    "canonical_symbol": "AALB.AS",
    "entity_name": "Aalberts Industries NV",
    "description": "Aalberts N.V., together with its subsidiaries, offers mission-critical technologies for building, industry, and semicon markets in Europe, the United States, the Asia Pacific, the Middle East, and Africa. The company operates through Building, Industry, and Semicon segments. It offers hydronic flow control systems for heating and cooling to enhance energy efficiency; integrated piping systems to d... <truncated; 966 bytes total>",
    "created_at": "2026-03-23T08:31:54.350977+00:00",
    "updated_at": "2026-04-13T13:51:55.367731+00:00",
    "sector": "Industrials",
    "industry": "Specialty Industrial Machinery"
  },
  {
    "security_id": 2,
    "canonical_ticker": "ABN",
    "canonical_exchange_code": "AS",
    "canonical_symbol": "ABN.AS",
    "entity_name": "ABN Amro Group NV",
    "description": "ABN AMRO Bank N.V. provides various banking products and financial services to retail, private, and corporate banking clients in the Netherlands, rest of Europe, the United States, Asia, and internationally. It operates through three segments: Personal & Business Banking, Wealth Management, and Corporate Banking. The company offers fixed deposits; home improvement; mortgage products; investment pr... <truncated; 925 bytes total>",
    "created_at": "2026-03-23T08:31:54.350977+00:00",
    "updated_at": "2026-04-13T13:51:54.063105+00:00",
    "sector": "Financial Services",
    "industry": "Banks - Diversified"
  },
  {
    "security_id": 3,
    "canonical_ticker": "ACOMO",
    "canonical_exchange_code": "AS",
    "canonical_symbol": "ACOMO.AS",
    "entity_name": "Amsterdam Commodities NV",
    "description": "Acomo N.V., together with its subsidiaries, engages in sourcing, trading, processing, packaging, and distributing conventional and organic food ingredients and solutions for the food and beverage industry in the Netherlands, Germany, other European countries, North America, and internationally. It operates through five segments: Spices and Nuts, Edible Seeds, Organic Ingredients, Tea, and Food Sol... <truncated; 1893 bytes total>",
    "created_at": "2026-03-23T08:31:54.350977+00:00",
    "updated_at": "2026-04-13T13:51:54.416295+00:00",
    "sector": "Consumer Defensive",
    "industry": "Food Distribution"
  },
  {
    "security_id": 4,
    "canonical_ticker": "AD",
    "canonical_exchange_code": "AS",
    "canonical_symbol": "AD.AS",
    "entity_name": "Koninklijke Ahold Delhaize NV",
    "description": "Koninklijke Ahold Delhaize N.V. operates retail food stores and e-commerce in the Netherlands, the United States, and internationally. The company's stores offer produce, dairy, meat, deli, bakery, seafood, and frozen products; grocery, beer, and wine; floral, pet food, health and beauty care, kitchen and cookware, gardening tools, general merchandise articles, electronics, newspapers and magazine... <truncated; 930 bytes total>",
    "created_at": "2026-03-23T08:31:54.350977+00:00",
    "updated_at": "2026-04-13T13:51:54.707695+00:00",
    "sector": "Consumer Defensive",
    "industry": "Grocery Stores"
  },
  {
    "security_id": 5,
    "canonical_ticker": "ADYEN",
    "canonical_exchange_code": "AS",
    "canonical_symbol": "ADYEN.AS",
    "entity_name": "Adyen NV",
    "description": "Adyen N.V. operates a payments platform in Europe, the Middle East, Africa, North America, the Asia Pacific, and Latin America. Its platform integrates payments stack, including gateway, risk management, processing, acquiring, and settlement services. The company offers a back-end infrastructure for authorizing. It accepts payment through online, in-person payments, cross channel, and Adyen for Pl... <truncated; 816 bytes total>",
    "created_at": "2026-03-23T08:31:54.350977+00:00",
    "updated_at": "2026-04-13T13:51:54.191594+00:00",
    "sector": "Technology",
    "industry": "Software - Infrastructure"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This is the identity root of the schema, so key changes are expensive
- Check whether `description`, `sector`, and `industry` belong on the identity table or should live in a lighter metadata cache
