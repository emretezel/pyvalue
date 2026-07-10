# QARP & DVG Metric Verification (2026-07)

Findings document for the metric-verification audit run on 2026-07-06: each
metric that drives **QARP** (`screeners/quality_reasonable_price_primary.yml`)
and **DVG** (`screeners/deep_value_graham.yml`) was independently recomputed for
the ten [anchor watchlist](../reference/watchlist.md) listings from primary
sources (SEC EDGAR filings for the nine US names; DART / company IR for SK
Hynix) and compared against `data/pyvalue.db`. The goal was catching **(a)**
garbage inputs from the EODHD provider and **(b)** residual pyvalue calc/as-of
errors. Ballpark agreement was the bar — only magnitude-level disagreement
(>~2x, sign flips) or clear data garbage counts as a defect.

This file is the durable record so a later session can plan and implement the
fixes. Companion research: [Screener NA Investigation](screener-na-investigation.md).

## Operating constraints (same as the NA investigation)

- **No EODHD API calls.** All analysis used the facts and price snapshots
  already in `data/pyvalue.db`; stale/missing source data is *documented*, never
  re-ingested.
- **As-of discipline.** TTM metrics were checked against the trailing four
  quarters ending on the metric's own `as_of`; strict-FY metrics against that
  fiscal year; price-based metrics against the **2026-04-10** price snapshot
  (not today's price). Prices are ~3 months stale as of this audit — a *context*
  note, not a defect.

## Bottom line

**The fundamental inputs are trustworthy.** For all ten names EODHD's raw
income-statement, balance-sheet and cash-flow figures matched the primary
filings **to the dollar** (SK Hynix <0.2%). Every alarming ratio is
economically real (AMD 106x EV/EBIT on Xilinx-amortization-depressed GAAP EBIT;
PLTR 72x EV/Sales; TSLA 274x EV/EBIT; NVDA 94% ROIC; INTC $62.38/$303B is a real
April-2026 "Terafab" rally plus 2025-26 share issuance). No garbage hides in the
numbers that drive the gates.

**Six defects were found; exactly one changes a screener metric at magnitude**
(GOOGL `price_to_book`, on DVG). The rest are latent, out-of-screener, or
immaterial to watchlist gates.

## Per-listing verdict

| Stock | listing_id | Fundamentals vs filing | Key issue |
|---|---|---|---|
| MSFT.US | 65235 | exact | clean (non-GAAP `eps_ttm` note) |
| GOOGL.US | 61081 | exact | **`price_to_book` 4.45 vs 9.34 (2.1x)**; `eps_ttm` phantom |
| ADBE.US | 53407 | exact | `interest_coverage` 65 vs 34 (sign flip; immaterial) |
| NVDA.US | 66246 | exact | `eps_ttm` 3.97 vs ~4.9 (phantom) |
| AMD.US | 54072 | exact | clean (106x EV/EBIT real) |
| C.US | 56282 | NI/cash exact | excluded by gate-failure not documented NA; P/B ~9% low |
| PLTR.US | 67393 | exact | `price_to_book` 1.12x; `eps_ttm` phantom |
| TSLA.US | 71610 | exact | `CashAndCashEquivalents` garbage → `roic`/`croic` ~20% low; `eps_ttm` phantom; `market_cap` ~6% low |
| INTC.US | 62587 | exact | clean ($62/$303B real; neg-EBIT NAs expected) |
| 000660.KO | 24625 | exact (<0.2%) | **price sentinel `999999.9999`** → all price metrics +13% |

## A. EODHD garbage inputs

### A1 — Price sentinel `999999.9999` (systematic; no ingestion guard)
EODHD emits exactly `999999.9999` as a placeholder price. In `data/pyvalue.db`
it appears in **45 `market_data` rows across 12 listings** (all KOSPI/KRW except
one Nigerian `AVAIF`/NGN), and it is the **latest (live) price for 11** of them.
`marketdata/eodhd.py:_price_data_from_entry` (~line 112) stores the first
non-null Close as-is; there is **no price-sanity guard** anywhere in ingestion.

- **SK Hynix (24625):** `market_data` 2026-04-10 = `999999.9999` (prior real
  closes ~873k-886k KRW). `market_cap` = shares 690,455,268 x 999,999.9999 =
  690.5T vs real ~611.7T KRW (**+13%**); every EV multiple +13%, EV *yields*
  -13%. Minor **only because Hynix trades near 1,000,000 KRW** by coincidence.
- **Universe risk:** a typical Korean name at ~50,000 KRW priced at
  `999,999.9999` gets a ~20x `market_cap` → magnitude distortion of every
  price-based metric. Masked in the watchlist by Hynix's price level.

### A2 — TSLA `CashAndCashEquivalents` = 1,890M vs SEC 16,513M (-89%)
Systematically broken across 2025 quarters (Q1 1,452 / Q2 963 / Q3 1,174 / FY
1,890 — all wrong). EODHD's `CashAndShortTermInvestments` (44,059M) is correct,
and `CashAndShortTermInvestments - ShortTermInvestments` (44,059 - 27,546) =
16,513 = the true cash figure — so the standalone `CashAndCashEquivalents` fact
is internally inconsistent garbage. See B3 for how pyvalue consumes it.

### A3 — ADBE Q1'26 `InterestExpense = -63M` (sign flip)
The four prior quarters are +62/+68/+67/+66 and sum to the FY total ~$263M; the
lone negative halves the signed TTM denominator → `interest_coverage` **64.9 vs
true ~34**. Both clear the QARP >=6x and DVG >=1.5x arms, so **immaterial to
gates** — but pyvalue guards negative D&A (`metrics/depreciation.py`) and has
**no equivalent guard on interest expense** (see B5).

### A4 — Citigroup (bank) fabrications
EODHD synthesizes `AssetsCurrent` ($738,120M, full quarterly history) and
`CostOfRevenue` for a bank that reports neither, so `altman_z` (-0.10),
`piotroski` (8), `gross_margin` (0.4455), `current_ratio` all compute.
`PreferredStock` is stale (latest 2023-03-31 = $20,245M), so the derived
`CommonStockholdersEquity` == `StockholdersEquity` == $212,291M (preferred not
stripped) → P/B understated ~9%. Revenue is gross ($168.3B) not net ($85.2B).
All immaterial because Citi is excluded — but the exclusion works by gate
failure, not the documented NA mechanism (see B4).

### A5 — INTC stray FY `OperatingIncomeLoss` = -23M
Contradicts its own quarterly sum (-2,244M) and the filing (-2,214M). **Unused**
— `operating_margin_ttm` correctly uses the quarterly TTM (-4.25%). Note only.

## B. pyvalue calc / design issues

### B1 — `price_to_book` multi-class share bug — the one magnitude case (DVG)
`metrics/price_to_book.py:37` sets `SHARE_CONCEPTS = ("CommonStockSharesOutstanding",)`
and `_latest_shares` (line 170) takes the **latest-by-date** row. For a
multi-class filer that is the **Class-A-only cover-page INSTANT**, while
`market_cap` (`metrics/utils.py:54` `SHARE_COUNT_CONCEPTS`) prefers
`EntityCommonStockSharesOutstanding` = the total.

| | P/B shares | mktcap shares | DB P/B | correct P/B |
|---|---|---|---|---|
| GOOGL | 5.822B (Class A INSTANT 2026-03-29) | 12.228B (total) | **4.45** | **9.34** |
| PLTR | 2.291B | 2.565B | 39.72 | 44.47 |

(GOOGL: equity 415,265M, price 317.24; 317.24 / (415,265/5,822) = 4.45 vs
317.24 / (415,265/12,228) = 9.34.) `price_to_book` is a **DVG gate** (`<=3.0x`
arm) and **ranking metric** (weight 0.15, lower = better), so an understated P/B
makes any dual-class issuer look cheaper than it is. GOOGL fails DVG on other
legs regardless, but every dual-class name in the universe is mis-ranked / can
be mis-gated. `price_to_tangible_book` shares the defect.

### B2 — `market_cap` uses a weighted-average share count for some filers (mirror image)
For TSLA and Citi, `EntityCommonStockSharesOutstanding` is a weighted-average-
diluted figure, so `market_cap` runs ~6% low (TSLA $1.23T vs ~$1.31T using the
period-end 3,752M; Citi $232B vs ~$218B using 1,749M) — while their
`price_to_book` correctly uses the period-end INSTANT. So the two metrics carry
**inconsistent** share counts and **neither concept is universally right**:

| | market_cap shares | P/B shares | correct |
|---|---|---|---|
| GOOGL / PLTR (multi-class) | Entity total (right) | Common INSTANT = single class (wrong) | market_cap |
| TSLA / Citi | Entity = weighted-avg (wrong, ~6% low) | Common INSTANT = period-end (right) | price_to_book |

This is why B1's fix is not a concept-list swap (see P1).

### B3 — `invested_capital` prefers the garbage-prone narrow cash concept
`metrics/invested_capital.py:32` sets `CASH_PRIMARY_CONCEPT =
"CashAndCashEquivalents"` (fallback `CashAndShortTermInvestments`), and
`_resolve_cash` uses the primary whenever it is present. For TSLA that is the
garbage 1,890M (A2), so `ic_fy = 88,623M` (verified = debt 8,376 + equity 82,137
- cash 1,890) instead of ~46-74B → **`roic_ttm` 0.037 / `croic` 0.073
understated ~20%**, and the historical `roic_*` series with it. Reaches QARP
roic gates (`roic_years_above_12pct`, `roic_10y_min`, `roic_7y_median`) and DVG
`croic` / `roic_10y_median_adaptive`. **No gate flips for Tesla** (its ROIC
fails QARP either way; `roic_10y_median_adaptive > 0` passes DVG regardless), and
**Tesla-only in this watchlist** (all other names' primary cash reconciles), but
the concept-preference is a latent universe-wide risk. `roce` (Assets - current
liabilities, no cash) is unaffected.

### B4 — Bank exclusion works by gate failure, not the documented mechanism
Both screener headers claim financials are excluded because they lack
`AssetsCurrent`/`LiabilitiesCurrent` → NA. EODHD fabricates those lines (A4), so
the metrics compute and Citi is excluded only by *failing* other gates (QARP >=8
hard gates; DVG solvency scorecard passes 1 of 4). Robust for Citi, but more
fragile than documented — a financial whose fabricated lines happen to clear the
gates could leak.

### B5 — `eps_ttm` phantom-quarter (out of screener, but systematic)
EODHD writes a forward EPS-only `0.00` row for the not-yet-reported quarter
(confirmed: GOOGL `2026-03-31` and NVDA `2026-04-30` carry only
`EarningsPerShare`/`EarningsPerShareDiluted` = 0.00, no revenue/NI companion).
The cadence-aware TTM metrics need a companion row and correctly stop at the
last real quarter; but `metrics/eps_quarterly.py` runs `resolve_ttm_window` on
the **single-concept** EPS series, so the 0.00 quarter forms a spurious window:

| | DB `eps_ttm` (as_of) | true trailing EPS |
|---|---|---|
| GOOGL | 8.00 (2026-03-31) | ~10.8 |
| NVDA | 3.97 (2026-04-30) | ~4.9 |
| PLTR | 0.61 (2026-03-31) | ~0.74 |
| TSLA | 1.22 (2026-03-31) | ~lower |

Propagates to `earnings_yield` / `peg_ratio` / `graham_multiplier`. **Neither
`eps_ttm` nor its derivatives is used by QARP or DVG**, so no screener impact —
but it is a real, systematic bug (4/10) affecting any earnings-yield analysis.

## C. Verified clean / by-design (not defects)

- Every name's revenue, gross profit, EBIT, net income, CFO, capex, equity,
  assets, shares — **exact to the filing** (or <0.2% for SK Hynix).
- Extreme multiples all real (AMD/PLTR/TSLA/NVDA/INTC as above); NVDA
  `cfo_to_ni` 0.855 real (receivables/inventory build on +65% revenue);
  Piotroski 4 real (YoY-improvement signals fail off a record base).
- `eps_ttm` on EODHD's adjusted `epsActual` basis and lease-inclusive debt —
  both already documented in [metrics.md](../reference/metrics.md).

## Fix backlog (prioritised)

| P | Fix | Locus | Screener effect | Regression sketch |
|---|---|---|---|---|
| **P1** | Unified period-end-total **share resolver** for both `market_cap` and `price_to_book`, rejecting single-class cover-page INSTANTs (GOOGL/PLTR) and weighted-average counts (TSLA/Citi) | `metrics/price_to_book.py`, `metrics/utils.py` (`market_cap_money`, `_latest_share_count_fact`) | **DVG P/B gate + ranking; all EV/mktcap** | GOOGL fixture (Class-A INSTANT 5.82B vs total 12.23B) → P/B ~9.34; TSLA fixture (Entity wtd-avg 3.53B vs INSTANT 3.75B) → not regressed |
| **P2** | **Price-sentinel guard**: reject `== 999999.9999` / implausible prices; skip the row so the last valid close stays latest (mirror the `depreciation.py` "treat as unavailable" pattern) | `marketdata/eodhd.py:_price_data_from_entry` (~L112) or `marketdata/service.py:prepare_price_data` | all price metrics; 11 live listings | Close=999999.9999 → no row stored / prior close remains latest |
| **P3** | **Invested-capital cash cross-check**: if `CashAndCashEquivalents` << `CashAndShortTermInvestments - ShortTermInvestments`, treat as unavailable and use the fallback | `metrics/invested_capital.py:32` (`_resolve_cash`) | QARP roic gates, DVG croic | TSLA fixture (Cash&Equiv 1,890, Cash+STI 44,059, STI 27,546) → IC uses ~16.5B, not 1.89B |
| P4 | `eps_ttm` phantom guard: at source don't emit an `EarningsPerShare` fact when `epsActual` is null (don't coerce 0.0); or require a `Revenues`/`NetIncomeLoss` companion at the anchor quarter | `normalization/eodhd.py` (Earnings section ~L974-1170) and/or `metrics/eps_quarterly.py` | none (outside screeners) | trailing companion-less 0.00 quarter → `eps_ttm` anchors on last real quarter |
| P5 | **Interest-expense sign guard**: drop a negative quarterly `InterestExpense` like negative D&A | `metrics/interest_coverage.py` | QARP/DVG coverage arms | quarters +62/+68/+67/-63 → coverage ~34 |
| P6 | **Explicit financial-sector exclusion** in both screeners rather than relying on gate failure; at minimum correct the header comments | `screeners/*.yml` + evaluator | correctness of documented exclusion | a bank with fabricated current lines is excluded pre-gate |

**P1 is the only fix that changes screener output (DVG).** It is *not* a
concept-list swap — aligning `price_to_book` to `market_cap`'s `Entity`-first
resolver would fix GOOGL/PLTR but regress TSLA/Citi (B2). It needs a resolver
that prefers the latest period-end **total** common shares, validated across the
GOOGL/PLTR (multi-class) and TSLA/Citi (weighted-avg-Entity) cases.

> **P1 RESOLVED (2026-07-10).** Root cause pinned deeper than B1's framing: the
> normalizer emits EODHD `SharesStats.SharesOutstanding` as a
> `CommonStockSharesOutstanding` INSTANT fact dated `General.UpdatedAt`, which
> always out-dates the filing history — so P/B's share basis was the SharesStats
> snapshot for *every* listing, and that snapshot is per-ticker-class for
> dual-class issuers. Inversely, TSLA/Citi's periodic rows are the artifact
> (weighted-average / issued-incl-treasury) and their snapshot is the only true
> count. Fixed by a shared resolver (`metrics/share_resolver.py`) consumed by
> `market_cap`/EV, P/B, P/TB and `graham_multiplier`: it arbitrates snapshot vs
> periodic via a new `ProviderMarketCapitalization` anchor fact
> (`Highlights.MarketCapitalization`, which EODHD always computes on the company
> total — verified by exact factorization on GOOGL/PLTR/TSLA/C/000660.KO) ÷ the
> stored close nearest its date. GOOGL P/B ≈ 9.34, PLTR on the 2,573M total,
> TSLA/C keep their true snapshots (TSLA market_cap moves from the 3,539M
> weighted-average basis to the true 3,752M). Regression tests:
> `tests/regression/test_share_count_snapshot_arbitration.py`,
> `tests/regression/test_price_to_book_dual_class_pipeline.py`. See
> `docs/reference/eodhd-concept-normalization.md` (Shares / Provider market
> capitalization).

> **P4 RESOLVED (2026-07-10).** Root cause differed from B5's framing: the
> normalizer already skips a null `epsActual` on every path — the phantoms are
> **literal `epsActual: 0` pre-fills from EODHD itself** (GOOGL 2026-03-31:
> `{"epsActual":0, "epsEstimate":2.53, "surprisePercent":-100,
> "reportDate":"2026-04-28"}` in a payload updated 2026-03-29; MSFT's forward
> row carries `null`, which is why MSFT was clean). Universe footprint: 3,676
> of 53,011 listings had a companion-less 0.0 as their latest quarterly EPS
> fact; in a 400-payload sample every one was a literal zero, 250 with
> `reportDate > General.UpdatedAt` and 150 lingering after the report date
> passed — so a temporal guard alone was insufficient. Fixed at source:
> `_filter_unreported_earnings_history` (`normalization/eodhd.py`) drops a
> `History` entry when its `reportDate` post-dates `General.UpdatedAt` (any
> value), or when `epsActual == 0.0` with no quarterly-statement companion
> (net income / statement EPS at the same date); filed breakeven quarters
> survive, `Annual` is untouched (verified clean, no `reportDate` field). No
> metric-layer change. Corrected watchlist values after `normalize-fundamentals
> --force` + `compute-metrics` (watchlist-scoped only, per author instruction —
> the ~3.7k other affected listings self-clean on their next re-normalization):
> GOOGL `eps_ttm` 8.00 → **10.81** (as_of 2025-12-31), NVDA 3.97 → **4.78**
> (2026-01-31), PLTR 0.61 → **0.74**, TSLA 1.22 → **1.34**; `earnings_yield` /
> `peg_ratio` / `graham_multiplier` move with it. Regression tests:
> `tests/regression/test_eps_ttm_phantom_quarter.py` plus placeholder-filter
> unit tests in `tests/test_eodhd_normalization.py`. See
> `docs/reference/eodhd-concept-normalization.md` (Earnings EPS).

> **P5 RESOLVED (2026-07-10).** Root cause confirmed at the source: EODHD's raw
> payload literally carries `Income_Statement.quarterly."2026-02-28"
> .interestExpense = -63,000,000` for ADBE (prior quarters +62/+68/+67/+66M sum
> exactly to the FY 263M; `interestIncome`/`netInterestIncome` null, so the
> sign-guarded derived fallback could not rescue the quarter), and
> `_compute_aligned` summed the TTM window *signed*, guarding only the total —
> a lone flipped quarter halved the denominator (8,961/138 = 64.93 vs
> 8,961/264 = 33.9 true). Universe footprint: 2,679 negative quarterly
> `InterestExpense` rows across 1,644 listings (plus 234 FY rows / 185
> listings); 623 listings had a negative row inside their latest-4 quarterly
> rows and 416 of those carried a live contaminated `interest_coverage`.
> Artifact taxonomy is identical to negative D&A — sign flips *and* scale
> blow-ups (worst row `-262.5B`), so `abs()` is unsafe. Fixed exactly as P5
> sketched: the D&A read guard was promoted to a neutral seam
> (`metrics/fact_guards.py`, `NON_NEGATIVE_CONCEPTS`) and `InterestExpense` +
> `InterestExpenseFromNetInterestIncome` joined the set; `interest_coverage`
> reads interest through it, so a negative quarter is treated as absent and the
> aligned window recedes to the last clean quarter (metric-layer only — no
> re-normalization needed). ADBE after watchlist-scoped `compute-metrics`:
> `interest_coverage` 64.93 → **33.10** (as_of 2025-11-30); both QARP >=6x and
> DVG >=1.5x arms still pass, and the other nine watchlist names carry no
> negative interest rows (provably unchanged). Residual note: the normalizer's
> *derived* `OperatingIncomeLoss = incomeBeforeTax + interestExpense -
> interestIncome` still consumes the raw flipped field for issuers with no
> direct operating-income line — out of scope here, candidate follow-up is
> skipping that derivation when `interestExpense < 0`. Regression tests:
> `tests/regression/test_interest_coverage_negative_quarter.py`; guard unit
> tests moved/extended in `tests/unit/test_fact_guards.py`. See
> `docs/reference/metrics.md` (Sign guard).

## Caveats / limitations

- Headline metrics were recomputed against >=1 primary filing per name; the deep-
  history 10Y medians/CAGRs/std were endpoint-spot-checked (latest FY plus one
  older year), not re-derived across the full decade.
- A few exact 2026-04-10 intraday closes were bracketed rather than pinned (free-
  source paywalls) but confirmed in-regime; the reference prices themselves were
  sanity-checked (INTC $62.38 and ADBE ~$225 both confirmed real; SK Hynix
  `999999.9999` confirmed garbage).
- Findings reflect `data/pyvalue.db` state on 2026-07-06. Re-running
  `compute-metrics` or a price refresh will move the price-based metrics.
