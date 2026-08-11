# MSFT QARP Review (2026-08)

Research memo, written 2026-08-10. Question posed by the author: Microsoft has
been considered as a **quality AI play** alongside the existing
[ADBE.US position](../portfolio/adbe-us.md). Review it from a QARP perspective
and give the pro/con case for adding it. A follow-up asked for a concrete
**maximum entry price** and the specific statement lines that would change the
verdict.

This memo supersedes the MSFT rows of the
[Watchlist AI Buy Ranking (2026-07)](watchlist-ai-buy-ranking-2026-07.md) for
valuation purposes. That memo's MSFT figures (EV/EBIT 18.8, FCF yield on EV
2.6%) were computed on a 2026-07-24 price of $381.70 against pre-Q4
fundamentals. **Both inputs have since moved materially** — see
[Section 1](#1-what-changed-since-the-july-memo).

Data vintages, kept explicit:

- **Trailing metrics**: `data/pyvalue.db` `metrics` rows for `listing_id`
  65235, computed 2026-08-10 from FY26 Q4 facts (`as_of` 2026-06-30, fiscal
  year ended 2026-06-30) and the **2026-08-07 close of $499.99**. Facts
  refreshed 2026-08-07 by the author; no ingestion was performed for this memo.
- **Forward/context data**: web research run 2026-08-10 (price verification,
  FY26 Q4 actuals, FY27 capex guidance, balance-sheet debt composition).
  Point-in-time; treat multiples as ±5% within their own day.

## Bottom line

**Microsoft passes 13 of the 14 QARP criteria with zero NAs.** It fails one:
the price gate — and it fails all three arms of it, the most generous by 38%.

The screen is not merely objecting to a high-quality mega-cap being dear. It is
catching a structural change in the business: Microsoft has converted itself
from a capital-light compounder into a capital-heavy one. Capex has gone from
9% of revenue to **34.9%**, and free cash flow has **declined two consecutive
fiscal years** — $74.07B (FY24) → $71.61B (FY25) → $66.99B (FY26) — while
revenue grew 35% over the same span. At $499.99 the stock trades at **55.4x**
that shrinking free cash flow. The existing ADBE position is at 9.9x.

**Recommendation: do not add at $499.99.** Framework price **$306**; maximum
defensible entry as a documented exception **$340**; hard ceiling **$380**.

## 1. What changed since the July memo

| Input | July memo (07-24/29 vintage) | This memo (2026-08-10) |
|---|---|---|
| Price | $381.70 (07-24 close) | **$499.99** (08-07 close) |
| Latest facts | through FY26 Q3 (2026-03-31) | through **FY26 Q4 (2026-06-30)** |
| TTM EBIT | $148.95B | $155.24B |
| EV/EBIT | 18.8x | **24.3x** (lease-adjusted) |
| FCF yield on EV | 2.6% | **1.78%** (lease-adjusted) |

The price move is real and was verified against exchange history: MSFT rose
~31% between 07-24 and 08-06 on the **FY26 Q4 print (2026-07-29)** — revenue
$90.0B (+18%), Azure +43% past a $100B annual run-rate with FY27 Azure guidance
*raised* to 45%, RPO $678B (+84%), 30M+ paid Copilot seats. It was the largest
single-day market-cap gain in the company's history. The 52-week range is
$349.20–$553.72; the all-time closing high is $538.66 (2025-10-28).

Worth recording: **even at July's $381.70, on today's EBIT, the price gate still
failed** (EBIT yield 5.53% vs the 6.67% arm). The July "wrong price only" verdict
was correct and has become more so, not less.

## 2. Defect found — net debt excludes finance leases

`metrics` for MSFT report **net cash of $26.87B** (`net_debt_to_ebitda`
−0.1387). The lease-inclusive figure is **net debt of $51.97B**. The two differ
by $78.8B.

Cause: `resolve_total_debt` (`src/pyvalue/metrics/balance_sheet.py:124-168`),
which feeds both `enterprise_value` and `net_debt_to_ebitda`, is
**components-preferred by explicit design** — the provider's
`TotalDebtFromBalanceSheet` rollup is consulted *only* when neither
`ShortTermDebt` nor `LongTermDebt` has a fresh row. At 2026-06-30 both
components are present, so the resolved figure is $18.905B + $31.067B =
**$49.97B**, and the $128.808B rollup is never read.

This is not an oversight. The function's own docstring names the rationale:
"a measured component sum must never be displaced by the rollup (the two can
disagree through **lease contamination** or provider unit errors)". The
finding below is therefore a challenge to a deliberate design choice, not a
bug report.

Both vendor fields are correct — they measure different scopes. Externally
confirmed: Microsoft's long-term debt excluding the current portion *is*
$31,067M, and the gap is **finance lease liabilities of $62,304M**, up from
$11,750M in 2021. For an issuer building datacenter capacity under
non-cancellable leases, excluding finance leases from net debt is precisely the
wrong choice — those are contractual obligations for the productive assets the
thesis rests on.

Impact:

| | pyvalue | Lease-inclusive | Delta |
|---|---|---|---|
| Net debt | −$26.87B (net cash) | **+$51.97B** | $78.8B |
| Enterprise value | $3,685.8B | **$3,764.7B** | +2.1% |
| `ebit_yield_ev` | 4.21% | **4.12%** | −9bp |
| `fcf_yield_ev` | 1.82% | **1.78%** | −4bp |
| `oey_ev_norm` | 2.37% | **2.32%** | −5bp |
| `net_debt_to_ebitda` | −0.14x | **+0.27x** | — |

**No QARP gate flips.** Leverage still clears both the 2.5x cap and the 1.5x
coverage-rescue arm with wide room, and the price gate fails either way. But
every price arm was ~3.4% too generous, and the framework entry price moves from
$317 to **$306**. All figures in this memo use the lease-inclusive basis.

### Status: fixed 2026-08-10 (commits `d5d8b34`, `d2e600c`, `c24aad2`)

The investigation below was written before the fix and is kept as the record of
what was found. What actually landed went further than proposed, because
reading the code turned up a second defect pushing the opposite way:

- **`LongTermDebt` was fabricated on 46% of the universe.** When the vendor's
  debt fields were null, normalization wrote
  `totalLiabilities − totalCurrentLiabilities` — all non-current liabilities —
  under a concept name asserting borrowed money. That derivation now populates
  a new `LiabilitiesNoncurrent` concept; `LongTermDebt` carries reported
  borrowings or nothing.
- **`resolve_total_debt` now keeps the broader of the two representations**,
  rather than the rollup-preferred rule proposed below. The components exceed
  the rollup on 42% of listings, so "always trust the rollup" would have traded
  one understatement for another. Fixing the fabricated input first is what
  made the comparison safe.
- **`resolve_debt_evidence` was deleted** — under the new rule it was identical
  to `resolve_total_debt`, which ends the split described above.
- **All five private debt chains were removed**, including
  `invested_capital`'s, so the ROIC gates and the price gates no longer read
  different balance sheets.

**Recompute completed 2026-08-11**, and the stored metrics now match this
memo's predicted figures exactly: `net_debt_to_ebitda` **+0.268x** (was
−0.1387), `ev_to_ebit` **24.25**, `fcf_yield_ev` **1.78%**, `oey_ev_norm`
**2.32%**. Market cap $3,712.7B against net debt ≈ $52.0B gives EV ≈ $3,765B.
No QARP gate flipped; MSFT remains 13 of 14 on the price gate alone.

Two follow-on defects surfaced during that recompute and were fixed before it
was accepted:

- **`71b259d`** — removing the fabricated `LongTermDebt` left invested capital
  with no debt leg for any period where the provider nulls all three debt
  fields, and one such gap voids the strict 10-year ROIC chain. That cost
  ~6,000 listings their ROIC persistence gates. The gap now takes that period's
  `LiabilitiesNoncurrent` as an explicit bound, which is one-way safe: it can
  only inflate invested capital, hence only depress a return.
- **`ce92a3f`** — **the claim above that all five private debt chains were
  removed was wrong.** There were six. `roic_fy_series` kept its own
  components-preferred copy, so the entire ROIC persistence family — four of
  QARP's fourteen gates — was still reading lease-blind debt. It now routes
  through the shared rule. MSFT's own `roic_10y_median` moved 30.1% → 28.9% and
  `iroic_5y` 23.5% → 18.3% as a result; neither is gated for MSFT, and both
  gates it does face on ROIC still pass.

### Fixes as originally proposed

1. **Revisit "lease contamination" in `resolve_total_debt`.** The argument
   against the current design: finance lease liabilities are on-balance-sheet
   debt under ASC 842 — non-cancellable obligations against productive assets.
   For a hyperscaler renting datacenter capacity they are not contamination of
   the debt figure, they *are* the debt figure, and an EV that omits them
   understates the claim on the enterprise by 2.1%. The "provider unit errors"
   half of the rationale remains sound; the lease half does not survive this
   case.

   **This is the second independent instance of the same defect.** Proposed
   fix #4 of the [2026-07-26 passer verification](qarp-passers-independent-verification-2026-07.md)
   already flagged `resolve_total_debt` for Westports (sukuk present in the
   rollup, absent from the components, net debt 15x understated) and proposed
   preferring the rollup when it exceeds the component sum by >1.5x. MSFT's
   ratio is **2.58x** ($128.808B / $49.972B), so that heuristic would catch it.
   The two cases together argue the heuristic should be adopted — but the
   trigger is now attested on the largest listing in the watchlist, not only on
   a Malaysian mid-cap.

   Note the codebase is already internally inconsistent here:
   `resolve_debt_evidence` (used by `interest_coverage`) takes the **`max()`**
   of the component sum and the rollup, so it sees $128.808B for MSFT while
   `enterprise_value` sees $49.97B. Two chains, two answers, same listing,
   same day.

   Blast radius: `net_debt_to_ebitda`, `fcf_to_debt`, `debt_paydown_years`,
   `short_term_debt_share`, and every EV-denominated metric — so every price
   gate. Expect universe-wide movement in lease-heavy sectors (retail,
   airlines, REITs), not just MSFT. The change needs a before/after diff at
   universe scale, and a re-run of the QARP passer list, before it lands.
2. **`LongTermDebt` series discontinuity.** The field drops from $91.838B
   (2025-12-31) to $31.423B (2026-03-31) while `TotalDebtFromBalanceSheet` rises
   $123.278B → $125.432B. Short-term debt moves only +$4.3B over the same step,
   so this is not a current-portion reclassification — it looks like EODHD
   changing what it maps into the field mid-series. Worth a universe-wide scan
   for the same break date.
3. **FY-vs-quarter disagreement at the same `end_date`.** For 2025-06-30 the
   `FY` rows read LTD $57.589B / STD $2.999B / total $60.588B while the `Q2`
   rows for the identical date read $83.152B / $11.595B / $112.184B. Whichever
   period label a metric happens to select changes the answer. This is a
   consistency check the normalizer does not currently make.

## 3. QARP gate matrix

`run-screen --config screeners/quality_reasonable_price_primary.yml`, confirmed
by `report-screen-failures`. Zero NAs — every criterion resolved to a number.

| Gate | Threshold | MSFT | Result |
|---|---|---|---|
| ROIC years >12% (10Y) | ≥ 7 | 9 | pass |
| ROIC minimum (10Y) | ≥ 7% | 10.50% | pass |
| Gross margin (TTM) | ≥ 35% | 67.94% | pass |
| GM stability (10Y std) | ≤ 7pp | 1.68pp | pass |
| Operating margin min (7Y) | ≥ 5% | 37.03% | pass |
| Cash conversion CFO/NI (TTM) | ≥ 0.90 | 1.368 | pass |
| Cash conversion median (10Y) | ≥ 0.90 | 1.341 | pass |
| Net debt / EBITDA | ≤ 2.5x | +0.27x | pass |
| Debt service (6x cov or ≤1.5x) | either | 50.9x | pass |
| No NI loss years (10Y) | == 0 | 0 | pass |
| Share count CAGR (5Y) | ≤ +1% | −0.34% | pass |
| **Price gate (OEY / EBIT / FCF)** | **any one** | **all three fail** | **FAIL** |
| Incremental ROIC (5Y) | ≥ 12% | 23.52% | pass |
| Owner-earnings CAGR (10Y) | ≥ 7% | 21.78% | pass |

Price gate detail, and the price at which each arm would clear:

| Arm | MSFT | Bar | % of bar | Clears at |
|---|---|---|---|---|
| Owner-earnings yield on EV (normalised, 5Y median) | 2.32% | ≥ 5% | 46% | $228 |
| **EBIT yield on EV** | **4.12%** | **≥ 6.67%** | **62%** | **$306** |
| FCF yield on EV | 1.78% | ≥ 5% | 36% | $173 |

A fourth, deliberately generous cross-check: `oey_ev` on **TTM** owner earnings
(maintenance capex $42.39B rather than the full $115.95B, 5Y-median basis
discarded) is 3.16% and clears at **$313**. Three independent generous readings
converge on **$306–313**, which is strong evidence the verdict is not an
artifact of one arm's methodology.

## 4. The pro case

**Quality is genuinely elite, and beats Adobe on several gates.**

| Metric | MSFT | ADBE | Better |
|---|---|---|---|
| ROIC 10Y median | 30.12% | 30.36% | tie |
| ROIC years >12% (10Y) | 9 | 10 | ADBE |
| ROIC minimum (10Y) | 10.50% | 14.78% | ADBE |
| Operating margin min (7Y) | **37.03%** | 29.25% | **MSFT** |
| Owner-earnings CAGR (10Y) | **21.78%** | 18.64% | **MSFT** |
| Interest coverage | **50.9x** | 34.8x | **MSFT** |
| SBC / revenue | **3.74%** | 8.05% | **MSFT** |
| GM stability (10Y std) | 1.68pp | 1.22pp | ADBE |
| Cash conversion (TTM) | 1.368 | 1.498 | ADBE |
| Gross margin (TTM) | 67.94% | 89.12% | ADBE |
| Altman Z | 8.83 | 7.37 | MSFT |
| Current ratio | 1.23 | 0.75 | MSFT |
| Piotroski F | 6 | 6 | tie |

**Owner earnings compounding at 21.78%/yr over a decade** is the best figure in
the anchor watchlist — faster than Adobe's 18.64%, and Microsoft is roughly 36x
Adobe's size. Stock-based compensation is less than half Adobe's as a share of
revenue.

**AI monetisation is in the reported numbers, not the pitch.** Azure +43% past a
$100B annual run-rate; RPO $678B (+84%); 30M+ paid Copilot seats. This is
qualitatively different from Adobe's position, where the AI question is
defensive.

**It hedges the existing position's specific thesis risk.** The ADBE holding is
a bet that generative AI does *not* disintermediate an incumbent. MSFT is a bet
that AI *does* transform enterprise software and the incumbent captures it.
These are not the same bet, and MSFT covers ADBE's left tail — a real
consideration when ADBE is the only position.

## 5. The con case

**5.1 The price gate fails by a wide margin, not a hair.** SK Hynix missed by
50bp in July. Microsoft misses the most generous arm by 38%. There is no reading
under which this is a near-miss.

**5.2 Free cash flow is shrinking while the multiple is not.**

| FY | Revenue | Capex | Capex/Rev | D&A | FCF | FCF YoY |
|---|---|---|---|---|---|---|
| 2016 | $91.15B | $8.34B | 9.1% | $6.62B | $24.99B | — |
| 2019 | $125.84B | $13.93B | 11.1% | $11.68B | $38.26B | — |
| 2022 | $198.27B | $23.89B | 12.0% | $14.46B | $65.14B | — |
| 2023 | $211.91B | $28.11B | 13.3% | $13.86B | $59.47B | −8.7% |
| 2024 | $245.12B | $44.48B | 18.1% | $22.29B | **$74.07B** | +24.6% |
| 2025 | $281.72B | $64.55B | 22.9% | $34.15B | $71.61B | **−3.3%** |
| 2026 | $331.84B | $115.95B | **34.9%** | $38.53B | $66.99B | **−6.5%** |

The June quarter alone ran capex at **39.8% of revenue** ($35.8B on $90.0B).
FY27 capex is guided to $255–260B on Microsoft's total-spend basis, +35%.

**5.3 The quality metrics are measured in the most flattering possible window.**
Net PP&E went from $109.99B to **$337.25B in three years (+207%)**. Capex is
$115.95B; depreciation in the P&L is **$38.53B** — a ratio of **3.01x**. The
spend is capitalised on the balance sheet; the charge has not yet reached
earnings. When capex flattens, D&A converges upward and the difference comes
straight out of EBIT — the input to the price gate's *best* arm. Today's 46.78%
operating margin and 28.46% ROIC are pre-depreciation-catch-up numbers.

**5.4 Incremental returns are decaying.** The stored `iroic_5y` is 23.52% and
clears the 12% gate. Recomputed over the trailing **three** years — the
hyperscale build itself — invested capital (equity + debt − cash) went $154.93B
→ $494.36B while NOPAT (at the realised effective tax rates, 18.98% and 19.40%)
went $71.72B → $125.12B. That is an incremental ROIC of **~15.7%**, against
23.52% on the five-year window. Approximate, and still above the gate — but the
direction says new capital earns materially less than old capital, which is
exactly what this leg of QARP exists to test.

**5.5 The investor stops being paid to wait.** `shareholder_yield_ttm` is
**1.31%** (0.71% dividend + 0.60% net buyback) against Adobe's **11.28%**. Share
count CAGR is −0.34%/yr versus Adobe's −3.52%. Net stock repurchase was $22.27B
against a $3.71T market cap.

**5.6 The balance sheet is levering quickly.** Total debt went $60.59B →
$128.81B in one fiscal year (**+113%**), and cash and short-term investments
fell $94.56B → $76.84B. Net position flipped from **$33.97B net cash** (FY25) to
**$51.97B net debt** (FY26). Still trivial against $193.77B of EBITDA, but the
rate of change matters more than the level.

**5.7 Entry timing is poor.** Adding here means buying after a +31% two-week
move, in the top quintile of the 52-week range, into the same AI-capex theme the
existing position is already exposed to from the other side.

## 6. Maximum entry price

Price ladder on the lease-inclusive EV basis, with modelled five-year returns:

| Price | EV/EBIT | EBIT yld | OE yld | FCF yld | P/FCF | P/E | Wall 9%@18x | Normal 15%@20x | Structural 4%@15x |
|---|---|---|---|---|---|---|---|---|---|
| $500 | 24.25x | 4.12% | 3.16% | 1.78% | 55.4x | 27.8x | **3.6%** | 11.7% | −4.7% |
| $450 | 21.86x | 4.57% | 3.51% | 1.97% | 49.9x | 25.0x | 5.8% | 14.0% | −2.7% |
| $400 | 19.47x | 5.14% | 3.94% | 2.22% | 44.3x | 22.2x | 8.3% | 16.7% | −0.4% |
| **$380** | 18.51x | 5.40% | 4.14% | 2.33% | 42.1x | 21.1x | 9.4% | 17.9% | 0.6% |
| $360 | 17.55x | 5.70% | 4.37% | 2.46% | 39.9x | 20.0x | 10.6% | 19.2% | 1.7% |
| **$340** | **16.60x** | **6.02%** | 4.62% | 2.60% | 37.7x | 18.9x | **11.9%** | 20.6% | 2.9% |
| **$306** | 14.97x | 6.68% | 5.12% | 2.88% | 33.9x | 17.0x | 14.2% | 23.1% | 5.0% |

Return scenarios, all five-year and all including ~0.9%/yr dividend, net debt
held flat:

- **Wall** — the depreciation catch-up binds. EBITDA margin holds at 58.4%,
  revenue compounds 14%/yr, D&A rises to ~$134B, so EBIT grows only 9%/yr; exit
  at 18x EV/EBIT.
- **Normal** — capex intensity mean-reverts toward 22% of revenue by FY31, EBIT
  grows 15%/yr; exit at 20x.
- **Structural** — capital intensity is permanent, EBIT grows 4%/yr; exit 15x.

### The three numbers

**Framework price — $306.** Where the EBIT arm clears 6.67%. No override
required; QARP simply passes.

**Maximum defensible entry — $340.** Justification:

1. It is a **statable rule**, not a judgement call: EV/EBIT ≤ 16.6x in place of
   ≤ 15x. One bounded notch, writable into a position note as a documented
   exception. Buying at $500 would be a 62% override of the gate; $340 is 11%.
2. It is the entry at which the **realistic case still returns ~12%/yr**. That
   is the correct hurdle because ADBE is already owned at a 10.20% FCF yield on
   cost — MSFT must beat the existing opportunity, not merely beat cash.
3. It is 11% below the 2026-07-24 close of $381.70. A level that traded three
   weeks ago is not a fantasy.

**Hard ceiling — $380.** Above this the realistic case falls below 10%/yr and
the structural case rounds to zero. **Above $400, no entry regardless**: that
requires underwriting the bull case as the base case, which is the specific
error the price gate exists to prevent.

### The non-price path

At a **flat $499.99**, EBIT must reach $251B (**+62%**) to clear the full 6.67%
gate, or $226B (**+46%**) to clear a relaxed 6.0% bar:

| Bar | EBIT needed | @10%/yr | @15%/yr | @20%/yr |
|---|---|---|---|---|
| 6.67% (full gate) | $251B | 5.0 yrs | 3.4 yrs | 2.6 yrs |
| 6.0% (relaxed) | $226B | 3.9 yrs | 2.7 yrs | 2.1 yrs |

So a legitimate way to own this is **time rather than price**: MSFT trades
sideways for roughly 2.5–3 years while EBIT compounds into the multiple. This
should be watched for, not bought into.

## 7. What to watch in future statements

FY27 Q1 ends 2026-09-30 and reports in late October 2026. Ranked by how
decisive each item is.

**1. Capex/revenue rolling over — the one number that matters.** 34.9% TTM,
39.8% in the June quarter, against 9% a decade ago. Want a *sequential peak*
followed by decline, and TTM below **30%** within four quarters. If it is still
above 35% at FY27 Q4, the intensity is structural rather than cyclical, and the
multiple must compress — that outcome is disqualifying, not merely
disappointing.

**2. FCF breaking its downtrend.** Want TTM FCF back above **$74.07B**, the
FY24 peak. Until it clears its own two-year-old high, the 55x P/FCF has no
growing denominator beneath it and per-share compounding is negative.

**3. The useful-life footnote — the sleeper risk.** Microsoft extended server
useful life to six years in FY2023, which added materially to reported EBIT. AI
accelerators may not last six years. Any *shortening* back toward four or five
years is an immediate, large, mechanical hit landing directly on EBIT — the
input to the best price arm. Read the property-and-equipment accounting policy
each quarter, and watch D&A/revenue rise from today's **11.6%**.

**4. Microsoft Cloud gross margin — is the new capital earning?** Azure can grow
45% while incremental returns fall. The ~15.7% three-year incremental ROIC needs
to stabilise rather than keep sliding toward the 12% gate. A 2–3pp/yr decline in
cloud gross margin is the tell that AI infrastructure is dilutive to returns.

**5. Finance-lease growth decelerating.** $62.3B of finance leases; total debt
+113% in one year. At this rate the leverage gate stops being a formality within
about two years, and interest expense starts eroding the 50.9x coverage.

**6. Buybacks resuming.** 1.31% shareholder yield. When management believes the
build is funded, repurchases restart — a more honest signal of when the capex
cycle tops than guidance is.

**7. OpenAI exposure.** Equity-method losses and the RPO concentration, both
currently aggregated rather than quantified.

**No single quarter makes this a buy at $500.** Even if capex rolls over and FCF
jumps to $90B, the FCF yield on EV is 2.4%. Only time or a drawdown fixes the
price.

## Method appendix

Trailing figures are `data/pyvalue.db` `metrics` and `financial_facts` rows for
`listing_id` 65235, read-only; the QARP verdict is from `run-screen` and
`report-screen-failures` against
`screeners/quality_reasonable_price_primary.yml`, not hand-evaluated. No EODHD
ingestion was performed — the author had refreshed MSFT facts and prices on
2026-08-07 before the review.

Derived quantities (EV on a lease-inclusive basis, clearing prices, the
five-year return scenarios, and the three-year incremental ROIC) were computed
outside the DB from the stored facts and are labelled as approximations where
the methodology differs from the metric engine's.

Web research (2026-08-10) covered price verification against exchange history,
FY26 Q4 actuals, FY27 capex guidance, and the balance-sheet debt composition
that surfaced the finance-lease finding. Sources:
[stockanalysis.com](https://stockanalysis.com/stocks/msft/history/),
[stock-analysis-on.net debt analysis](https://www.stock-analysis-on.net/NASDAQ/Company/Microsoft-Corp/Analysis/Debt),
[financecharts](https://www.financecharts.com/stocks/MSFT/balance-sheet/total-debt),
[GuruFocus forward P/E](https://www.gurufocus.com/term/forward-pe-ratio/MSFT),
TradingKey (FY26 Q4 results and FY27 capex guidance). Consensus and price
figures are point-in-time.
