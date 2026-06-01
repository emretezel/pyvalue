# `financial_facts`

## Purpose

Stores provider-agnostic normalized financial facts for metrics.

## Grain

One row per `(listing_id, concept, fiscal_period, end_date)` after migration 071.
Migration 043 first set the PK to `(listing_id, concept, fiscal_period, end_date, unit)`
(dropping the always-NULL `accn` discriminator); migration 071 then dropped `unit` from
the key as well, because `unit` was replaced by the `unit_kind` enum and a single concept
never carries more than one kind for a given period. Migration 073 then dropped the four
dead SEC columns `cik`, `accn`, `start_date`, and `accounting_standard` (all always NULL
under the EODHD-only provider model).

## Live Stats

<!-- BEGIN generated_live_stats -->
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Row count: `6,987`
- Table size: `638,976 bytes` (`624.0 KiB`)
- Approximate bytes per row: `91.5`
<!-- END generated_live_stats -->

## Columns

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `listing_id` | `INTEGER` | no | PK, idx | canonical listing link |
| `concept` | `TEXT` | no | PK, idx | normalized concept |
| `fiscal_period` | `TEXT` | no | PK | One of `FY`, `Q1`, `Q2`, `Q3`, `Q4`, `TTM`, `INSTANT`. Migration 065 tightened to NOT NULL; migration 068 added a CHECK pinning the enum after backfilling the 77,209 empty-string rows that earlier EODHD code persisted for snapshot facts. Runtime `FactRecord` default is `'INSTANT'`. |
| `end_date` | `TEXT` | no | PK, idx | fact period end |
| `unit_kind` | `TEXT` | no |  | Semantic kind (migration 071, renamed from `unit`). CHECK pins the enum `monetary` / `per_share` / `ratio` / `percent` / `multiple` / `count` / `other` (`MetricUnitKind`). No longer a currency code — the ISO code lives in `currency` alone. Dropped from the PK by 071. |
| `value` | `REAL` | no |  | numeric fact value |
| `filed` | `TEXT` | yes | idx | filing date for latest-row ordering |
| `frame` | `TEXT` | yes |  | provider frame string |
| `currency` | `TEXT` | yes | partial idx | ISO currency, *major-only* (migration 071 forbids subunit GBX/ZAC/ILA). CHECK couples it to `unit_kind`: non-NULL iff `unit_kind` is `monetary`/`per_share`, NULL otherwise. |
| `source_provider` | `TEXT` | yes |  | provenance |

## Keys And Relationships

<!-- BEGIN generated_keys_and_relationships -->
- Primary key: (`listing_id`, `concept`, `fiscal_period`, `end_date`)
- Physical foreign keys:
  - `listing_id` -> `listing`.`listing_id`
- Physical references from other tables: none
- Unique constraints beyond the primary key: none
- Main logical refs: `listing_id` in `listing`
<!-- END generated_keys_and_relationships -->

## Secondary Indexes

<!-- BEGIN generated_secondary_indexes -->
- `idx_fin_facts_currency_nonnull (currency)` WHERE currency IS NOT NULL
- `idx_fin_facts_security_concept_latest (listing_id, concept, end_date DESC, filed DESC)`
<!-- END generated_secondary_indexes -->

## Main Read Paths

- bulk fact preload for metric computation
- latest fact lookup by `listing_id + concept`
- FX currency discovery

## Main Write Paths

- `normalize-fundamentals`
- purge when a listing becomes secondary

## Sample Rows

<!-- BEGIN generated_sample_rows -->
- Snapshot source: `data/pyvalue.db` on `2026-06-01`
- Sample window: first `5` rows returned by SQLite ordered by `listing_id ASC, concept ASC, fiscal_period ASC, end_date ASC`

```json
[
  {
    "listing_id": 53177,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "1985-09-30",
    "unit_kind": "monetary",
    "value": 936200000.0,
    "filed": "1985-09-30",
    "frame": "CY1985",
    "currency": "USD",
    "source_provider": "EODHD"
  },
  {
    "listing_id": 53177,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "1986-09-30",
    "unit_kind": "monetary",
    "value": 1160100000.0,
    "filed": "1986-09-30",
    "frame": "CY1986",
    "currency": "USD",
    "source_provider": "EODHD"
  },
  {
    "listing_id": 53177,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "1987-09-30",
    "unit_kind": "monetary",
    "value": 1477900000.0,
    "filed": "1987-09-30",
    "frame": "CY1987",
    "currency": "USD",
    "source_provider": "EODHD"
  },
  {
    "listing_id": 53177,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "1988-09-30",
    "unit_kind": "monetary",
    "value": 2082100000.0,
    "filed": "1988-09-30",
    "frame": "CY1988",
    "currency": "USD",
    "source_provider": "EODHD"
  },
  {
    "listing_id": 53177,
    "concept": "Assets",
    "fiscal_period": "FY",
    "end_date": "1989-09-30",
    "unit_kind": "monetary",
    "value": 2743900000.0,
    "filed": "1989-09-30",
    "frame": "CY1989",
    "currency": "USD",
    "source_provider": "EODHD"
  }
]
```
<!-- END generated_sample_rows -->

## Review Notes

- This is one of the hottest tables. Keep `listing_id` and concept-based lookup paths indexed and avoid widening rows without a measured need.
- Migration 043 added `FOREIGN KEY (listing_id) REFERENCES listing(listing_id)` and tightened the PK to `(listing_id, concept, fiscal_period, end_date, unit)`. The migration's deduplication rule for the 24,837 colliding EODHD groups was: prefer rows with non-NULL `filed`, then most-recent `filed`, then lowest `rowid` for tie-break.
- Migration 052 dropped the redundant `idx_fin_facts_security_concept (listing_id, concept)` because `idx_fin_facts_security_concept_latest` already covers the `(listing_id, concept, ...)` prefix.
- Migration 059 added row-level CHECK constraints on `unit` (non-empty, no internal whitespace) and `currency` (3-char uppercase ASCII when present). One legacy malformed EODHD `EnterpriseValue` row (empty unit, ~193 trillion value) was deleted as part of the pre-flight cleanup.
- Migration 065 tightened `fiscal_period` to NOT NULL after an audit confirmed zero NULL rows on the live DB; the runtime `FactRecord` default is `'INSTANT'`.
- Migration 068 added `CHECK (fiscal_period IN ('FY','Q1','Q2','Q3','Q4','TTM','INSTANT'))` and backfilled the ~77K legacy rows where earlier EODHD code persisted `fiscal_period=''` for snapshot facts (EnterpriseValue, CommonStockDividendsPerShareCashPaid, and a dormant SharesStats writer). Backfill mapping: `CommonStockDividendsPerShareCashPaid` → `'TTM'`, every other empty-period concept → `'INSTANT'`. For the backfilled rows the migration re-dates `end_date` to `General.UpdatedAt` from the cached fundamentals payload (falling back to `DATE(fundamentals_raw.last_fetched_at)`), because the legacy `end_date = Highlights.MostRecentQuarter` was the balance-sheet quarter rather than the price/Valuation snapshot date. The corresponding normalizer changes in `eodhd.py` make every fresh ingest emit the correct enum/date so the empty-period bug cannot recur.
- Migration 071 renamed `unit` → `unit_kind` and dropped it from the PK (new PK `(listing_id, concept, fiscal_period, end_date)`). The legacy `unit` column conflated a currency code (`USD`, `GBX`, …) with a type token (`shares`, `EPS`, `USD/shares`); `unit_kind` now holds only the `MetricUnitKind` enum and the ISO code lives in `currency` alone. Two CHECKs encode the invariants: `currency` is **major-only** (no subunit GBX/ZAC/ILA — subunits are collapsed to the base currency before a fact is built), and `unit_kind` ⇄ `currency` are **coupled** (monetary/per_share ⇒ currency NOT NULL; every other kind ⇒ currency NULL). Because `financial_facts` is rebuilt from `fundamentals_raw` via the `normalise` CLI after the refactor, the migration rebuilds the table **empty** (no row copy) and clears `fundamentals_normalization_state` so every cached payload is re-normalized. The EODHD normalizer emits `unit_kind` directly (`count` for share concepts, `per_share` for EPS/DPS, `monetary` otherwise).
