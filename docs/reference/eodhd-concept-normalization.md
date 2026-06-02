# EODHD Concept Normalization Reference

This file is the single source of truth for **how each `financial_facts` concept is
normalized from a raw EODHD `fundamentals_raw` payload** — for every concept, where its
value comes from in the JSON, and how its `end_date`, `fiscal_period`, `currency`, and
`unit_kind` are decided, plus the fallbacks and the checks that make the normalizer skip or
rescale a value.

It complements two existing docs and does not repeat them:

- [Normalization and Facts](../architecture/normalization-and-facts.md) — the provider-agnostic
  concept model and *why* metrics read from facts.
- [`financial_facts` table](../architecture/database/tables/financial_facts.md) — the schema,
  constraints, and migration history of the destination table.

Source of truth in code: `src/pyvalue/normalization/eodhd.py` (the
`EODHDFactsNormalizer`) and `src/pyvalue/currency.py`. Worked examples below are real,
pruned payloads for **MSFT**, **GOOGL**, and **ADBE** pulled from `fundamentals_raw`.

> EODHD is the only provider, so every distinct `concept` in `financial_facts` came from
> this normalizer. The authoritative concept set is therefore whatever
> `SELECT DISTINCT concept FROM financial_facts` returns — **45 concepts** as of
> 2026-06-01 (the live DB currently holds one fully normalized symbol, **AAPL/USD**;
> MSFT/GOOGL/ADBE appear only as raw input payloads).

## Normalized fact shape

A fact row is `(listing_id, concept, fiscal_period, end_date, unit_kind, value, filed,
currency)`. Two invariants matter throughout this doc (enforced by the schema — see the
table doc):

- `fiscal_period` ∈ `FY`, `Q1`–`Q4`, `TTM`, `INSTANT`.
- `unit_kind` ∈ `monetary`, `per_share`, `ratio`, `percent`, `multiple`, `count`, `other`;
  and `currency` is **non-NULL iff `unit_kind` is `monetary` or `per_share`**, NULL
  otherwise. `currency` is always a *major* unit (no `GBX`/`ZAC`/`ILA`).

## Payload sections that are read (everything else is ignored)

| Section | Used for |
| --- | --- |
| `General.CurrencyCode` | payload-level currency fallback (precedence step 3) |
| `General.UpdatedAt` | `end_date` for all snapshot facts (EV, TTM DPS, INSTANT shares) |
| `Highlights.DividendShare` | `CommonStockDividendsPerShareCashPaid` (TTM) |
| `Valuation.EnterpriseValue` | `EnterpriseValue` (INSTANT) |
| `SharesStats.SharesOutstanding` / `.SharesFloat` | `CommonStockSharesOutstanding` (INSTANT) |
| `outstandingShares.{annual,quarterly}` | `CommonStockSharesOutstanding` (FY / Qn) |
| `Earnings.{History,Annual}` | `EarningsPerShareDiluted` (`epsActual`) |
| `Financials.{Balance_Sheet,Income_Statement,Cash_Flow}.{yearly,quarterly}` | all statement concepts |

Ignored sections include `Technicals`, `AnalystRatings`, `Holders`, `InsiderTransactions`,
`ESGScores`, `SplitsDividends`, and any statement line-item not listed in the per-family
tables below.

## Cross-cutting rules (apply to every concept)

1. **Value coercion** — `_to_float`; a non-numeric/absent value yields `None` and the fact
   is skipped.
2. **`end_date`** — `_extract_date` reads the entry's `date` (then `Date`, then `period`) and
   truncates to `YYYY-MM-DD`; an unparseable date skips the entry. Snapshot facts instead use
   `General.UpdatedAt` (see those families).
3. **`fiscal_period`** — `_infer_quarter`: an explicit `period` of `Q1`–`Q4` is honored,
   otherwise it is derived from the **calendar month** of `end_date` (`≤03→Q1`, `≤06→Q2`,
   `≤09→Q3`, else `Q4`); the `yearly` bucket is `FY`. ⚠ This is the *calendar* month, not the
   issuer's fiscal quarter — see [Caveats](#known-subtleties--caveats).
4. **Currency precedence** — `resolve_eodhd_currency`, in order:
   1. entry-level `currency` / `currency_symbol` / `CurrencyCode`;
   2. statement-level `currency_symbol` / `currency` / `CurrencyCode`;
   3. payload-level `General.CurrencyCode`;
   4. an optional caller-supplied fallback (used only by the snapshot families).
   There is no silent default.
5. **Missing currency on a monetary/`per_share` fact** → a structured warning
   (`warn_missing_monetary_currency`) and the fact is **dropped** (the schema forbids a
   currency-less monetary row).
6. **Subunit scaling** — `normalize_monetary_amount` collapses configured subunits to their
   base currency and divides the amount: `GBX`/`GBP0.01`→`GBP`, `ZAC`→`ZAR`, `ILA`→`ILS`,
   each `÷100`. A stored fact never holds a subunit code.
7. **Listing-currency FX alignment (optional)** — when a `target_currency` is supplied,
   `_convert_facts_to_target_currency` converts monetary/`per_share` facts to
   `base(listing.currency)`; counts/ratios pass through. Missing FX **skips that one fact**
   (`raise_on_missing_fx=False`). Derived same-period conversions use the same skip-on-missing
   behavior; see [Normalization and Facts](../architecture/normalization-and-facts.md) for the
   broader FX policy.

## Concept inventory (45)

`F` = directly read from a statement field, `D` = derived/aliased, `S` = snapshot.
"Periods" are the `fiscal_period` values actually observed in the live table.

| Concept | Family | unit_kind | Periods | Source |
| --- | --- | --- | --- | --- |
| Assets | Balance Sheet | monetary | FY, Q1–Q4 | F |
| AssetsCurrent | Balance Sheet | monetary | FY, Q1–Q4 | F (+component fallback) |
| CashAndCashEquivalents | Balance Sheet | monetary | FY, Q1–Q4 | F |
| CashAndShortTermInvestments | Balance Sheet | monetary | FY, Q1–Q4 | F |
| Goodwill | Balance Sheet | monetary | FY, Q1–Q4 | F |
| IntangibleAssetsNet | Balance Sheet | monetary | FY, Q1–Q4 | F |
| IntangibleAssetsNetExcludingGoodwill | Balance Sheet | monetary | FY, Q1–Q4 | D |
| Liabilities | Balance Sheet | monetary | FY, Q1–Q4 | F |
| LiabilitiesCurrent | Balance Sheet | monetary | FY, Q1–Q4 | F (+component fallback) |
| LongTermDebt | Balance Sheet | monetary | FY, Q1–Q4 | F (+subtraction fallback) |
| LongTermDebtNoncurrent | Balance Sheet | monetary | FY, Q1–Q4 | F |
| NetTangibleAssets | Balance Sheet | monetary | FY, Q1–Q4 | F |
| NoncontrollingInterestInConsolidatedEntity | Balance Sheet | monetary | FY, Q1–Q4 | F |
| PreferredStock | Balance Sheet | monetary | FY, Q1–Q4 | F |
| PropertyPlantAndEquipmentNet | Balance Sheet | monetary | FY, Q1–Q4 | F (+subtraction fallback) |
| ShortTermDebt | Balance Sheet | monetary | FY, Q1–Q4 | F |
| ShortTermInvestments | Balance Sheet | monetary | FY, Q1–Q4 | F |
| StockholdersEquity | Balance Sheet | monetary | FY, Q1–Q4 | F + D |
| CommonStockholdersEquity | Balance Sheet | monetary | FY, Q1–Q4 | D (override) |
| TotalDebtFromBalanceSheet | Balance Sheet | monetary | FY, Q1–Q4 | F |
| EntityCommonStockSharesOutstanding | Balance Sheet | count | FY, Q1–Q4 | F |
| CommonStockSharesOutstanding | Shares | count | FY, Q1–Q4, INSTANT | F + S + D |
| Revenues | Income Statement | monetary | FY, Q1–Q4 | F |
| CostOfRevenue | Income Statement | monetary | FY, Q1–Q4 | F |
| GrossProfit | Income Statement | monetary | FY, Q1–Q4 | F |
| OperatingIncomeLoss | Income Statement | monetary | FY, Q1–Q4 | F (+derivation fallback) |
| EBITDA | Income Statement | monetary | FY, Q1–Q4 | F |
| DepreciationDepletionAndAmortization | Income Statement | monetary | FY, Q1–Q4 | F |
| InterestExpense | Income Statement | monetary | FY, Q1–Q4 | F |
| InterestExpenseFromNetInterestIncome | Income Statement | monetary | FY, Q1–Q4 | D (in-statement) |
| IncomeBeforeIncomeTaxes | Income Statement | monetary | FY, Q1–Q4 | F |
| IncomeTaxExpense | Income Statement | monetary | FY, Q1–Q4 | F |
| NetIncomeLoss | Income Statement | monetary | FY, Q1–Q4 | F |
| NetIncomeLossAvailableToCommonStockholdersBasic | Income Statement | monetary | FY, Q1–Q4 | F + D |
| EarningsPerShareDiluted | Earnings | per_share | FY, Q1–Q4 | F (Earnings block / implied) |
| EarningsPerShare | Earnings | per_share | FY, Q1–Q4 | D (alias) |
| NetCashProvidedByUsedInOperatingActivities | Cash Flow | monetary | FY, Q1–Q4 | F (+derivation fallback) |
| CapitalExpenditures | Cash Flow | monetary | FY, Q1–Q4 | F (+derivation fallback) |
| DepreciationFromCashFlow | Cash Flow | monetary | FY, Q1–Q4 | F |
| CommonStockDividendsPaid | Cash Flow | monetary | FY, Q1–Q4 | F |
| StockBasedCompensation | Cash Flow | monetary | FY, Q1–Q4 | F |
| SalePurchaseOfStock | Cash Flow | monetary | FY, Q1–Q4 | F |
| IssuanceOfCapitalStock | Cash Flow | monetary | FY, Q1–Q4 | F |
| EnterpriseValue | Snapshot | monetary | INSTANT | S |
| CommonStockDividendsPerShareCashPaid | Snapshot | per_share | TTM | S |

Four concepts are mapped in code but **not present** in the live table — see
[Mapped but unpopulated](#mapped-but-currently-unpopulated).

## Statement families

All three statement families share the same header rules; only the leaf field(s) differ.

**Shared rules for `Financials.<Statement>`:**

- **Section / periods:** both `.yearly` and `.quarterly` are present, each a dict keyed by the
  period end-date. Value path: `Financials.<Statement>.<yearly|quarterly>[<end_date>].<leaf>`
  (each leaf tried in order; lookup is case-insensitive).
- **`end_date`:** the entry's own `date` (equals the dict key) — *per-entry*, never top-level.
- **`fiscal_period`:** `FY` for `yearly`; calendar-month-inferred `Qn` for `quarterly`.
- **`currency`:** entry `currency_symbol` → statement `currency_symbol` → `General.CurrencyCode`.
- **`unit_kind`:** `monetary`, except the share-count concepts (`count`, currency NULL).

### Balance Sheet — `Financials.Balance_Sheet`

| Concept | leaf field(s) tried | unit_kind | Fallback / skip·scale |
| --- | --- | --- | --- |
| AssetsCurrent | `totalCurrentAssets` | monetary | else `totalAssets − nonCurrentAssetsTotal` (if ≥0); else Σ(`cashAndShortTermInvestments` **or** `shortTermInvestments`+`cash`/`cashAndEquivalents`, `netReceivables`, `inventory`, `otherCurrentAssets`) |
| LiabilitiesCurrent | `totalCurrentLiabilities` | monetary | else `totalLiab − nonCurrentLiabilitiesTotal` (if ≥0); else Σ(`accountsPayable`, `otherCurrentLiab`, `currentDeferredRevenue`, `shortTermDebt` **or** `shortLongTermDebt`) |
| Assets | `totalAssets` | monetary | — |
| Liabilities | `totalLiabilities`, `totalLiab` | monetary | — |
| StockholdersEquity | `totalStockholderEquity`, `totalShareholderEquity` | monetary | also derived (see Derived) |
| CommonStockholdersEquity | `commonStockTotalEquity` | monetary | normally replaced by the derived override (see Derived) |
| PreferredStock | `preferredStockTotalEquity`, `preferredStockRedeemable`, `preferredStock` | monetary | genuine preferred fields only — `capitalStock` is **deliberately excluded** (see [Caveats](#known-subtleties--caveats)) |
| Goodwill | `goodWill`, `goodwill` | monetary | — |
| IntangibleAssetsNet | `intangibleAssets` | monetary | — |
| NetTangibleAssets | `netTangibleAssets` | monetary | — |
| NoncontrollingInterestInConsolidatedEntity | `noncontrollingInterestInConsolidatedEntity` | monetary | — |
| CashAndShortTermInvestments | `cashAndShortTermInvestments` | monetary | — |
| CashAndCashEquivalents | `cashAndEquivalents`, `cash` | monetary | — |
| ShortTermInvestments | `shortTermInvestments` | monetary | — |
| ShortTermDebt | `shortTermDebt`, `shortLongTermDebt` | monetary | — |
| LongTermDebtNoncurrent | `longTermDebtNoncurrent`, `longTermDebtTotal`, `longTermDebt` | monetary | — |
| LongTermDebt | `longTermDebtTotal`, `longTermDebt`, `longTermDebtNoncurrent` | monetary | else `totalLiab − totalCurrentLiabilities` (if ≥0) |
| TotalDebtFromBalanceSheet | `shortLongTermDebtTotal` | monetary | — |
| PropertyPlantAndEquipmentNet | `propertyPlantAndEquipmentNet`, `propertyPlantEquipment`, `netPropertyPlantAndEquipment`, `propertyPlantAndEquipment` | monetary | else `propertyPlantAndEquipmentGross − accumulatedDepreciation` (if ≥0) |
| EntityCommonStockSharesOutstanding | `shareIssued`, `commonStockSharesOutstanding` | count | currency NULL |
| CommonStockSharesOutstanding | `shareIssued`, `commonStockSharesOutstanding` | count | currency NULL; also from snapshot + historical + alias; share-record collapse dedups |

### Income Statement — `Financials.Income_Statement`

| Concept | leaf field(s) tried | unit_kind | Fallback / skip·scale |
| --- | --- | --- | --- |
| Revenues | `totalRevenue`, `revenue` | monetary | — |
| CostOfRevenue | `costOfRevenue` | monetary | — |
| GrossProfit | `grossProfit` | monetary | — |
| OperatingIncomeLoss | `operatingIncome`, `ebit` | monetary | else `incomeBeforeTax + interestExpense − interestIncome`; else `totalRevenue − totalOperatingExpenses` |
| EBITDA | `ebitda`, `EBITDA` | monetary | — |
| DepreciationDepletionAndAmortization | `depreciationAndAmortization`, `reconciledDepreciation` | monetary | — |
| InterestExpense | `interestExpense` | monetary | — |
| InterestExpenseFromNetInterestIncome | *(no direct key)* | monetary | derived `interestIncome − netInterestIncome` (only if > 0) |
| IncomeBeforeIncomeTaxes | `incomeBeforeTax` | monetary | — |
| IncomeTaxExpense | `incomeTaxExpense`, `taxProvision` | monetary | — |
| NetIncomeLoss | `netIncome`, `netIncomeFromContinuingOps` | monetary | — |
| NetIncomeLossAvailableToCommonStockholdersBasic | `netIncomeApplicableToCommonShares` | monetary | else `NetIncomeLoss`; derived adjustment subtracts preferred dividends (see Derived) |
| EarningsPerShareDiluted | `epsDiluted`, `epsdiluted`, `epsDilluted` | per_share | **in practice sourced from the Earnings block / implied calc** — see note below |
| EarningsPerShare | *(alias)* | per_share | diluted preferred, else basic (see Derived) |

> **EPS and weighted-average shares are not in the EODHD statement entries.** Verified on
> MSFT/GOOGL/ADBE: an `Income_Statement` entry carries no `eps*` and no `weightedAverageShsOut*`
> keys (MSFT's only share-related income key is `netIncomeApplicableToCommonShares`).
> Consequently `EarningsPerShareDiluted` is populated from the **`Earnings` block** (`epsActual`)
> or the implied-EPS calculation (see [Earnings EPS](#earnings-eps)), and the statement-sourced
> `EarningsPerShareBasic` and `WeightedAverageNumberOf…Shares` concepts stay
> [unpopulated](#mapped-but-currently-unpopulated).

### Cash Flow — `Financials.Cash_Flow`

| Concept | leaf field(s) tried | Fallback / skip·scale |
| --- | --- | --- |
| NetCashProvidedByUsedInOperatingActivities | `totalCashFromOperatingActivities` | else `freeCashFlow + capitalExpenditures` |
| CapitalExpenditures | `capitalExpenditures`, `capex` | else `totalCashFromOperatingActivities − freeCashFlow` |
| DepreciationFromCashFlow | `depreciation` | — |
| CommonStockDividendsPaid | `dividendsPaid` | — |
| StockBasedCompensation | `stockBasedCompensation` | — |
| SalePurchaseOfStock | `salePurchaseOfStock` | — |
| IssuanceOfCapitalStock | `issuanceOfCapitalStock` | — |

## Snapshot families

These are point-in-time facts dated by `General.UpdatedAt` (EODHD's own refresh date), **not**
by a fiscal quarter.

### Enterprise value — `Valuation.EnterpriseValue`

Scalar (no `yearly`/`quarterly`). `fiscal_period = INSTANT`, `unit_kind = monetary`,
`end_date = General.UpdatedAt`. Currency = `resolve_eodhd_currency(Valuation,
statement_currency = first usable currency among BS/IS/CF, payload = General.CurrencyCode)`.
Skipped if `UpdatedAt` is missing or no currency resolves.

### Dividends per share — `Highlights.DividendShare`

Scalar. Concept `CommonStockDividendsPerShareCashPaid`, `fiscal_period = TTM`,
`unit_kind = per_share`, `end_date = General.UpdatedAt`. Currency =
`resolve_eodhd_currency(Highlights, payload = General.CurrencyCode)`. Skipped if missing
(e.g. ADBE's `DividendShare` is `null` → no fact).

## Shares

Concept `CommonStockSharesOutstanding`, `unit_kind = count`, currency NULL, from up to three
sources; the share-record collapse keeps one best record per `(concept, end_date,
fiscal_period)`, preferring `unit_kind = count` and NULL currency.

- **INSTANT snapshot** — `SharesStats.SharesOutstanding` (fallback `SharesStats.SharesFloat`);
  `end_date = General.UpdatedAt`; skipped if `UpdatedAt` missing.
- **Historical** — `outstandingShares.<annual|quarterly>[<i>].shares` (fallback
  `sharesMln × 1e6`); `end_date = dateFormatted` (else `date`; a bare 4-digit year →
  `YYYY-12-31`); `FY` for the annual bucket, calendar-inferred `Qn` for quarterly. The annual
  entries are dated by `dateFormatted` (calendar `12-31`), which can differ from the
  balance-sheet fiscal year-end.
- **Balance sheet** — `EntityCommonStockSharesOutstanding` (above), aliased to
  `CommonStockSharesOutstanding`.

### Earnings EPS — `Earnings.{History,Annual}`

Concept `EarningsPerShareDiluted`, `unit_kind = per_share`. Value = entry `epsActual`;
`end_date` = entry `date` (or the key); `fiscal_period` inferred (`History` → `Qn`, `Annual`
→ `FY`). Currency = entry `currency` → most-recent non-null earnings currency → statement →
`General.CurrencyCode`.

Two consistency mechanisms:

- **Subunit unit-flip detection** (only when the base currency is a subunit family such as
  GBX/ZAC/ILA): the EPS series is scanned for a jump between consecutive values whose ratio is
  in `[40, 140]×` (`EPS_UNIT_FLIP_RATIO_MIN/MAX`); values below `0.05` are ignored. The
  larger-magnitude cluster is treated as the subunit denomination and rescaled `×0.01`.
- **Implied-EPS fallback** — when neither the Earnings block nor the statement supplies EPS for
  a period, EPS = `NetIncomeLoss / weighted-or-outstanding shares`, matching shares on the same
  date or the nearest within `120` days (quarterly) / `370` days (annual). The implied series
  also calibrates the unit-flip scale (median ratio in `[40,140]×` ⇒ `×0.01`).

## Derived concepts

Computed after the raw pass from already-normalized facts; monetary inputs in different
currencies are converted with `choose_target_currency` + `convert_money_value` (missing FX
skips the period). Only `CommonStockholdersEquity` is an *override* (it removes the
same-period raw read); the rest are additive/alias.

| Concept | Derivation |
| --- | --- |
| EarningsPerShare | alias of `EarningsPerShareDiluted`, else `EarningsPerShareBasic` |
| StockholdersEquity | `Assets − Liabilities` (if ≥0); else alias of `CommonStockholdersEquity` |
| CommonStockholdersEquity (override) | `StockholdersEquity − PreferredStock − NoncontrollingInterestInConsolidatedEntity` |
| IntangibleAssetsNetExcludingGoodwill | `IntangibleAssetsNet`; else `(Assets − Liabilities) − NetTangibleAssets − Goodwill` (if ≥0) |
| NetIncomeLossAvailableToCommonStockholdersBasic | `NetIncomeLoss − PreferredStockDividendsAndOtherAdjustments` |
| CommonStockSharesOutstanding | alias of `EntityCommonStockSharesOutstanding` |

A second tier of alias hooks exists for `NetCashProvidedByUsedInOperatingActivities`,
`CapitalExpenditures`, `OperatingIncomeLoss`, and `PropertyPlantAndEquipmentNet`, but their
fallback concept lists are currently empty, so they are no-ops today.

## Mapped but currently unpopulated

These concepts are defined in `EODHD_STATEMENT_FIELDS` but have **0 rows** in the live table,
because their source keys are absent from the EODHD payloads (verified on MSFT/GOOGL/ADBE):

| Concept | Intended unit_kind | Why absent |
| --- | --- | --- |
| EarningsPerShareBasic | per_share | statement `eps`/`epsBasic` keys not present; the Earnings block emits only diluted |
| WeightedAverageNumberOfDilutedSharesOutstanding | count | `weightedAverageShsOutDil*` not present in statement entries |
| WeightedAverageNumberOfSharesOutstandingBasic | count | `weightedAverageShsOut*` not present in statement entries |
| PreferredStockDividendsAndOtherAdjustments | monetary | `preferredStockAndOtherAdjustments` is `null` for sampled issuers |

This is the data EODHD currently returns, not necessarily a permanent state; if a future
payload carries these keys the normalizer will emit the concepts. (The live DB also predates
some of these mappings; re-normalization would confirm.)

## Worked examples (real pruned payloads)

The blocks below are the actual `fundamentals_raw` payloads for each ticker, **pruned to only
the sections/fields the normalizer reads**, with `// →` annotations added (so they are not
literal JSON). The three are USD reporters; the instructive difference is the fiscal year-end,
which drives quarter labeling.

### MSFT — fiscal year-end **June 30**

```jsonc
{
  "General":     { "CurrencyCode": "USD", "FiscalYearEnd": "June", "UpdatedAt": "2026-03-29" },
  "Highlights":  { "DividendShare": 3.48 },                  // → CommonStockDividendsPerShareCashPaid (per_share, TTM, end_date=UpdatedAt)
  "Valuation":   { "EnterpriseValue": 2803775975252 },       // → EnterpriseValue (monetary, INSTANT, end_date=UpdatedAt)
  "SharesStats": { "SharesOutstanding": 7425629076 },        // → CommonStockSharesOutstanding (count, INSTANT, end_date=UpdatedAt)
  "outstandingShares": {
    "annual":    { "0": { "dateFormatted": "2025-12-31", "shares": 7460000000 } },  // → CommonStockSharesOutstanding (count, FY, end_date 2025-12-31)
    "quarterly": { "0": { "dateFormatted": "2025-12-31", "shares": 7460000000 } }   // → CommonStockSharesOutstanding (count, Q4)
  },
  "Earnings": {
    "Annual":  { "2025-12-31": { "epsActual": 7.86 } },                       // → EarningsPerShareDiluted (per_share, FY)
    "History": { "2026-03-31": { "epsActual": null, "currency": "USD" } }     // skipped (epsActual null)
  },
  "Financials": {
    "Balance_Sheet": {
      "currency_symbol": "USD",
      "yearly": { "2025-06-30": {
        "date": "2025-06-30",                          // → end_date; FY (yearly bucket)
        "currency_symbol": "USD",                      // → entry-level currency (precedence #1)
        "totalAssets": "619003000000.00",              // → Assets
        "totalCurrentAssets": "191131000000.00",       // → AssetsCurrent
        "totalLiab": "275524000000.00",                // → Liabilities (totalLiabilities absent → leaf #2)
        "totalCurrentLiabilities": "141218000000.00",  // → LiabilitiesCurrent
        "totalStockholderEquity": "343479000000.00",   // → StockholdersEquity (also confirmed by derived Assets−Liabilities)
        "cashAndShortTermInvestments": "94555000000.00", // → CashAndShortTermInvestments
        "cashAndEquivalents": "30242000000.00",        // → CashAndCashEquivalents
        "shortTermInvestments": "64313000000.00",      // → ShortTermInvestments
        "goodWill": "119509000000.00",                 // → Goodwill
        "intangibleAssets": "22604000000.00",          // → IntangibleAssetsNet
        "shortTermDebt": "11595000000.00",             // → ShortTermDebt
        "longTermDebtTotal": "83152000000.00",         // → LongTermDebt and LongTermDebtNoncurrent (leaf #1 for both)
        "shortLongTermDebtTotal": "112184000000.00",   // → TotalDebtFromBalanceSheet
        "propertyPlantAndEquipmentNet": "229789000000.00", // → PropertyPlantAndEquipmentNet
        "commonStockSharesOutstanding": "7465000000.00" // → EntityCommonStockSharesOutstanding (count) → aliased CommonStockSharesOutstanding
        // (capitalStock is present in the raw payload but NOT read — see PreferredStock note)
      } }
      // .quarterly["2025-09-30"] would be labeled Q3 (calendar month 09), though it is MSFT's fiscal Q1
    },
    "Income_Statement": {
      "currency_symbol": "USD",
      "yearly": { "2025-06-30": {
        "totalRevenue": "281724000000.00",             // → Revenues
        "costOfRevenue": "87831000000.00",             // → CostOfRevenue
        "grossProfit": "193893000000.00",              // → GrossProfit
        "operatingIncome": "128528000000.00",          // → OperatingIncomeLoss
        "ebitda": "160165000000.00",                   // → EBITDA
        "depreciationAndAmortization": "34153000000.00", // → DepreciationDepletionAndAmortization
        "interestExpense": "2385000000.00",            // → InterestExpense
        "incomeBeforeTax": "123627000000.00",          // → IncomeBeforeIncomeTaxes
        "incomeTaxExpense": "21795000000.00",          // → IncomeTaxExpense
        "netIncome": "101832000000.00",                // → NetIncomeLoss
        "netIncomeApplicableToCommonShares": "101832000000.00" // → NetIncomeLossAvailableToCommonStockholdersBasic
        // note: no eps* or weightedAverageShsOut* keys → EPS comes from the Earnings block
      } }
    },
    "Cash_Flow": {
      "currency_symbol": "USD",
      "yearly": { "2025-06-30": {
        "totalCashFromOperatingActivities": "136162000000.00", // → NetCashProvidedByUsedInOperatingActivities
        "capitalExpenditures": "64551000000",          // → CapitalExpenditures
        "depreciation": "34153000000.00",              // → DepreciationFromCashFlow
        "dividendsPaid": "24082000000.00",             // → CommonStockDividendsPaid
        "stockBasedCompensation": "11974000000.00",    // → StockBasedCompensation
        "salePurchaseOfStock": "-18420000000.00",      // → SalePurchaseOfStock
        "issuanceOfCapitalStock": "2056000000.00"      // → IssuanceOfCapitalStock
      } }
    }
  }
}
```

Note MSFT exposes three different share counts at three different dates: `SharesStats`
7,425,629,076 (INSTANT @ 2026-03-29), balance-sheet `commonStockSharesOutstanding`
7,465,000,000 (FY @ 2025-06-30), and `outstandingShares` 7,460,000,000 (FY @ 2025-12-31).

### GOOGL — fiscal year-end **December 31** (calendar = fiscal quarters)

```jsonc
{
  "General": { "CurrencyCode": "USD", "FiscalYearEnd": "December", "UpdatedAt": "2026-03-29" },
  "Financials": { "Income_Statement": { "currency_symbol": "USD",
    "yearly": { "2025-12-31": {            // → end_date 2025-12-31, FY; quarter 12-31 → Q4 (matches fiscal)
      "totalRevenue": "402963000000.00",   // → Revenues
      "operatingIncome": "129166000000.00",// → OperatingIncomeLoss
      "netIncome": "132170000000.00"       // → NetIncomeLoss
    } } } }
  // balance sheet commonStockSharesOutstanding ≈ 12.23B (Class A); SharesStats.SharesOutstanding 5.82B is one class only
}
```

### ADBE — fiscal year-end **November 30**, pays no dividend

```jsonc
{
  "General":    { "CurrencyCode": "USD", "FiscalYearEnd": "November", "UpdatedAt": "2026-03-28" },
  "Highlights": { "DividendShare": null },     // → no CommonStockDividendsPerShareCashPaid fact (skipped)
  "Financials": { "Income_Statement": { "currency_symbol": "USD",
    "yearly":    { "2025-11-30": { "totalRevenue": "23769000000.00", "netIncome": "7130000000.00" } }, // → FY (yearly); 11-30 → Q4 if quarterly
    "quarterly": { "2026-02-28": {
      "currency_symbol": null,                 // entry currency null → falls back to statement "USD" (precedence #2)
      "totalRevenue": "6398000000.00",         // → Revenues (Q1: month 02 → Q1)
      "netIncome": "1889000000.00"             // → NetIncomeLoss
    } } } }
}
```

### Subunit scaling (illustrative — not one of the three)

A UK statement entry with `"currency_symbol": "GBX"` and `"totalRevenue": 500` stores
`value = 5.00`, `currency = "GBP"` — the `÷100` subunit collapse guards against mixing pence
and pounds.

## Known subtleties / caveats

- **`PreferredStock` excludes `capitalStock` (fixed).** The leaf chain is genuine
  preferred-equity fields only. `capitalStock` was previously a fallback, but for issuers
  without preferred stock it is common stock + additional paid-in capital; because the override
  `CommonStockholdersEquity = StockholdersEquity − PreferredStock − NCI`, that mislabel drove
  common equity **negative** (AAPL FY2025: `PreferredStock` 93.6B → `CommonStockholdersEquity`
  −19.8B, negative every year 2022–2025). The `capitalStock` fallback was removed, so with no
  real preferred field present no `PreferredStock` fact is emitted and common equity is no
  longer understated. Already-stored rows reflect the fix only after the cached payloads are
  re-normalized.
- **Calendar-month, not fiscal, quarter labels.** `fiscal_period` for quarterly rows is derived
  from the calendar month of `end_date`. Verified on AAPL (fiscal year-end Sep 30): the
  September-ending quarter is stored as `Q3` (month map `03→Q1, 06→Q2, 09→Q3, 12→Q4`), so a
  company's fiscal Q-number need not match the stored label.
- **EPS comes from the `Earnings` block, not the income statement** (statement `eps*` keys are
  absent from EODHD payloads — see the Income Statement note).
- **Snapshot facts are dated by `General.UpdatedAt`**, independent of the latest filed quarter.
- **One concept, several sources.** `CommonStockSharesOutstanding` is produced by the balance
  sheet, `SharesStats`, and `outstandingShares` at potentially different dates; the share-record
  collapse keeps one per `(concept, end_date, fiscal_period)`.
- **Stored `currency` is always a major unit**; subunits are collapsed before a fact is built.

## Related docs

- [Normalization and Facts](../architecture/normalization-and-facts.md)
- [`financial_facts` table](../architecture/database/tables/financial_facts.md)
- [`fundamentals_raw` table](../architecture/database/tables/fundamentals_raw.md)
- [EODHD Provider Guide](../providers/eodhd.md)
- [Metrics Catalog](metrics.md)
