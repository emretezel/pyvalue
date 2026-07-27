# Independent verification of the QARP screen's 154 passing listings

**Date:** 2026-07-26
**Input:** `data/output/screen_results_qarp_primary.csv` — 154 listings, 120 distinct companies
**Output:** `data/output/qarp_passers_verification_2026-07.csv` — one verdict row per passing listing
**Status:** audit only. No source code, schema or stored data was changed.

## Why this was done

The QARP screen (`screeners/quality_reasonable_price_primary.yml`) admitted 154 of 57,001 supported
listings. The question was whether any of those admissions are **false positives** — passes caused by bad
vendor data or by a bug in normalization/metrics rather than by the business genuinely clearing the gates.
The tolerance was explicit: same-ballpark differences are fine; the targets are order-of-magnitude errors
and gate flips.

## Headline result

| verdict | companies | listings |
|---|---|---|
| CONFIRMED — every gate reproduces and the valuation gate still passes at the real price | 99 | 128 |
| SUSPECT — the pass survives but a load-bearing input is materially wrong or definition-dependent | 10 | 14 |
| FALSE POSITIVE — the screen admitted it because of wrong data | 11 | 12 |

**The fundamentals data is broadly excellent.** Dozens of gates reproduced to three or four decimals
against independent sources across Taiwan, Thailand, China, Japan, Mexico, the Nordics and the Baltics.
Almost every error found is in the **price and share basis that builds enterprise value**, not in the
financial statements — with the notable exceptions of ITE Tech, Apranga and Ulta below.

## Method

1. **Local forensics.** The 154 listings were grouped into 120 companies (26 pass on 2-4 venues).
   Cross-venue price parity was computed in USD, cross-venue divergence measured on every price-free gate
   metric, and all sentinel/cap values catalogued so verifiers knew which numbers are conventions.
2. **Independent web verification.** 20 agents, batched by market so each reused one source family,
   rebuilt each company's gates from stockanalysis.com, macrotrends, company IR and audited filings, and
   exchange-grade sources (MOPS/FinMind for Taiwan, 10jqka/EastMoney/sohu for China, irbank.net for Japan,
   carisaham for Indonesia, biznesradar for Poland, Nasdaq Baltic PDFs, PSX, JSE). Every listing's
   valuation gate was recomputed at the **real** price and share basis.
3. **Root-cause attribution.** Every major finding was reproduced locally with write-free
   `pyvalue explain-metric` runs and direct `financial_facts` queries, attributing each defect to **EODHD
   data**, **pyvalue ingestion**, or **pyvalue EV construction** with the exact rows shown.
4. **Self-checks.** Four verdicts were re-verified by hand against primary sources — two false positives
   (ITE Tech, Medistim), one suspect (Ulta) and one confirmed pass (Moutai, which matched to 0.14pp). All
   four held. One agent hypothesis (a suspected metric cross-wiring on 300360.SHE) was **disproved**
   locally and is recorded as such.

## The false positives

| listing(s) | company | cause | evidence |
|---|---|---|---|
| 0HS2.LSE | Cadence Design Systems | pence-collapse price bug | real EV/EBIT ~59; every arm fails 3-4x |
| 0K36.LSE | Moody's | pence-collapse price bug | real EV/EBIT ~26; all arms fail ~1.6-2x |
| 0IZI.LSE | W.W. Grainger | pence-collapse price bug | real yields 2.0-3.9% vs 5/6.67% |
| 0J8P.LSE | IDEXX Laboratories | pence-collapse price bug | real EV/EBIT ~31 |
| 0JPO.LSE | KLA Corporation | pence-collapse price bug | real EV/EBIT ~51 |
| 0K10.LSE | Mettler-Toledo | pence-collapse price bug | real EV/EBIT ~26 |
| 0KFZ.LSE | Parker-Hannifin | pence-collapse price bug | real EV/EBIT ~31 |
| TSMC34.SA | TSMC (Brazilian BDR) | depositary ratio not modelled | real EV/EBIT 23.4, EV/FCF 51 |
| 3014.TW | ITE Tech | vendor cash-flow error | real TTM CFO/NI 0.115 vs a 0.90 gate |
| MEDI.OL | Medistim | cumulative-YTD quarters summed | real FCF yield ~4.4% vs 5%; all arms fail |
| WHX.MU, WHX.STU | APB Apranga | vendor operating-income error | audited FY2020 operating margin 4.14% vs a 5% gate |

For the seven London lines the 13 price-free quality gates are all genuine — these are real quality
businesses. Only the valuation gate is corrupted, and only because the price is wrong.

## Defect taxonomy — eight mechanisms, with evidence

### 1. LSE international-order-book prices stored 100x too low (pyvalue ingestion) — the big one

> **Fixed 2026-07-27** — see proposed fix 1 below for what shipped and why the fix in this section's
> original write-up would not have been enough. Prices already stored remain wrong until re-fetched.

`src/pyvalue/marketdata/eodhd.py:132-134` applies the exchange subunit hint whenever the bulk feed omits
a currency and the raw price exceeds 100:

```python
subunit_hint = EXCHANGE_SUBUNIT_HINTS.get(suffix)   # "GBX" for LSE
if subunit_hint and currency is None and price and price > 100:
    currency = subunit_hint                          # -> later divided by 100
```

Correct for LSE's pence-quoted domestic lines; **wrong for the USD/EUR-quoted international order book**,
whose listing currency is not GBP. Twelve of the 154 passers are such lines, each stored at ~1/100 of
reality (0K10.LSE 13.09 vs MTD.US 1,310.02; 0JPO.LSE 2.125 vs KLAC.US 212.75; factors 96.6x-100.7x).
Seven are outright false positives; five (Adobe, Paychex, Paycom, Lululemon, Zoetis) survive because
those companies genuinely clear the gate — corrupted input, right answer.

**Universe scale.** 924 non-GBP LSE listings have EV metrics computed, and 15.4% carry an EBIT yield
above 25% versus 4.0% of US listings — a ~4x excess implying on the order of 100 further listings with
~100x-understated enterprise values. They stayed out of the screen only because the quality gates
excluded them.

### 2. Depositary/BDR ratios are not modelled (pyvalue EV construction)

Market cap is built as *company-total share count x listing price* (`metrics/utils.py:463`), which is
wrong by the ratio for any non-1:1 depositary line. **TSMC34.SA** is the clear case: 8 BDRs represent 5
ordinary shares, so EV lands at ~0.62x reality and the true EV/EBIT is 23.4x — failing every arm.

The risk does not always materialise. Where the vendor's share count is already ADS-equivalent the EV
comes out right, which the verification confirmed for NetEase (5:1), OMA (8:1), Inditex (4:1),
Kimberly-Clark de México (5:1), Christian Dior (4:1), GTT (5:1), Nemetschek (1:4) and ResMed's CDI line
(10:1). The pipeline has no way to distinguish these cases — it is relying on vendor luck.

### 3. Dual-listed A+H companies priced entirely at one line's price (pyvalue EV construction)

**Tsingtao Brewery** consolidates 1.36bn A+H shares but the pipeline prices all of them at the discounted
H quote, understating market cap ~15-20% and inflating stored yields ~30-45%. Both arms still clear at
the real A+H EV, so no gate flips — but the mispricing is systematic for dual-venue issuers.

### 4. Minority interest omitted from enterprise value (pyvalue EV construction)

**Christian Dior** consolidates LVMH in full while owning ~41.7% of it. The pipeline's EV ignores the
EUR 42.0bn book (~EUR 134bn market) non-controlling interest, so all three yields are overstated 1.5-2.5x.
Locally: stored `ev_to_ebit` is **5.95 for CDI.PA against 15.12 for MC.PA** — the same underlying asset
priced two and a half times cheaper. The concept `NoncontrollingInterestInConsolidatedEntity` **is**
ingested but never enters the EV build. Dior still passes with minorities marked to market (~7.2% EBIT
yield), so it is not a false positive, but its rank 25/42 versus LVMH's 139/154 is an artifact.

### 5. Stale provider share counts outvoting the filings (EODHD data + arbitration design)

For **0JX9.LSE** (Marimekko's LSE line) the provider snapshot says 8.070m shares while the filings — also
in our database — say 40.539m; the gap is the April-2022 four-for-one bonus issue. `arbitrate_share_count`
normally prefers the filing when the gap exceeds 1.25x, but EODHD's market-cap anchor (EUR 76.9m, implying
~7.4m shares) is stale in the *same* direction, so the two corroborate each other and win. That line's EV
is ~5x too low; gate 12 still passes at the true EV but its rank 9 / score 75.4 are spurious.

A universe check found 11 of the 154 passers with snapshot-versus-filing divergence above 1.25x. Most are
benign unit/class mismatches (ADS versus ordinary, CDIs, A+B classes, A+H) — which is exactly why the
arbitration cannot be tightened without unit metadata.

### 6. Cumulative year-to-date cash flows summed as if discrete quarters (EODHD data shape)

**Medistim (MEDI.OL)** — a false positive. EODHD's quarterly cash-flow rows are cumulative YTD:

| period | stored CFO (NOK) |
|---|---|
| Q1 2025 | 18.8m |
| Q2 2025 | 56.5m |
| Q3 2025 | 125.6m |
| Q4 2025 | 191.6m |
| FY 2025 | 191.6m — identical to Q4 |

The TTM window sums four of these (56.5 + 125.6 + 191.6 + 7.2 = 380.9m) against a real trailing figure of
**180.0m** — independently confirmed at 179.97m. That inflated the FCF yield to 9.56% when the truth is
~4.4%; with the EBIT arm at 5.0% and owner earnings at 2.5%, every arm fails. The FY row is correct, which
is why the annual-basis gates verified — a useful diagnostic signature. The same pattern appears on
TSMC34.SA (2.48x).

### 7. Cash position built from mismatched period ends (pyvalue EV construction)

When EODHD supplies no combined `CashAndShortTermInvestments`, `resolve_cash_position` adds
`ShortTermInvestments` to `CashAndCashEquivalents` **without requiring a common as-of date**. For
**PINFRA** a Q1-2026 cash row of MXN 39.6bn is added to an FY-2025 investments row of MXN 28.3bn,
producing MXN 67.9bn of "cash" against a real ~33.5bn and halving EV to ~MXN 48bn — exactly the figure the
independent reconstruction inferred. Eight passers combine rows from different period ends, with staleness
up to six years (ZOE.MU pairs a 2026 cash row with a 2019 investments row).

### 8. Plain vendor errors in the statements (EODHD data)

- **ITE Tech (3014.TW)** — false positive. Stored FY2025 operating cash flow is 1,859.8m TWD against an
  audited **902.0m**, and the stored Q1-2026 row is +563.4m where the real quarter was negative. Stored
  `cfo_to_ni_ttm` is 1.2224; the real ratio is **0.115**, so the 0.90 gate fails on any recent window.
- **Apranga (WHX.MU/WHX.STU)** — false positive, and the smallest-magnitude error to cause one. The
  company's own audited 2020 report states operating profit of EUR 7,038k on revenue of EUR 169,958k —
  **4.14%** — and FY2018 was 4.91%. Both fall inside the 7-year window, so the true `opm_7y_min` is ~0.041
  against a 0.05 gate. Stored is 0.0522/0.0540, only ~1.1-1.3pp high, but that alone carries the gate.
- **Ulta Beauty** — suspect. For the COVID fiscal year our database stores operating income of 313.7m on
  revenue of 6,152.0m (5.10%, just clearing the 5% floor); Ulta reported **236.8m** GAAP (3.85%, failing
  it). Revenue matches to the decimal, so the whole gap is in the EBIT construction.
- **Mr D.I.Y. (5296.KLSE)** — suspect. Stored operating income runs ~1.5x reported, lifting the EBIT yield
  from a real 5.9% (fail) to a stored 8.9% (pass); the pass survives only via the verified FCF arm.
- **Westports (5246.KLSE)** — confirmed, with a fixable defect. The component debt fields (134m + 930m MYR)
  omit the 2025 sukuk that **is** present in EODHD's own `TotalDebtFromBalanceSheet` rollup (3,496m).
  Because `resolve_total_debt` prefers components whenever either side is fresh, net debt/EBITDA stored at
  0.08 against a real ~1.2 — a 15x understatement that happens not to flip a gate here.
- **Trade-Van (6183.TW)** — confirmed. Vendor capex rows are ~4-6x too small, so the stored FCF yield of
  7.5% is effectively CFO/EV; the real 5.9% still clears.
- **Stale EBITDA in the debt ratio** — 300514.SHE stores net debt/EBITDA of -2.16 against a real ~-13.9 and
  300360.SHE -3.57 against ~-6.8, both implying an FY2024-level EBITDA rather than TTM. No gate flips
  (both are deep net cash).

## Notable suspects that are not data errors

- **XTB (xtb-war)** — the data is clean (EV and both yields reproduce to three decimals, and client money
  is correctly excluded from cash), but XTB is a retail CFD broker whose ROIC, gross-margin and
  cash-conversion gates are structurally meaningless — invested capital collapses toward zero because a
  broker's equity is held as cash. **The screen's financial-sector exclusion failed to catch it.**
- **3SBio (trsbf-us)** — every metric is accurate, but FY2025 embeds a one-time out-licensing windfall
  (revenue +94%, EBIT x4) that drives the valuation and reinvestment gates. The screen is reading a
  windfall as run-rate earnings; gate 12 survives normalisation at ~15%.
- **Amdocs** — thirteen gates verify and the stock is genuinely cheap, but `owner_earnings_cagr_10y` is
  stored at 0.0707 against a 0.0700 gate while every external proxy says 2-5%, and its two listings
  disagree by 50% on that same metric.
- **Inditex** — the EBIT arm fails on real data (EV/EBIT ~19-20x); the pass rests on a ~5.2% FCF yield that
  holds only before deducting ~EUR 3.4bn of annual IFRS-16 lease principal.

## Follow-up: primary-listing classification (fixed 2026-07-26)

A separate investigation into why the screen returned ADRs and cross-listings
beside their real lines found the cause upstream of everything below:
classification read `General.PrimaryTicker` alone and treated its **absence** as
proof of primacy, so 22,452 of the 57,001 screened listings were "primary" only
because nothing said otherwise. EODHD also points an ADR's `PrimaryTicker` at
the ADR itself, so every ADR self-certified.

Replacing it with an ordered rule set over `HomeCategory`, `PrimaryTicker`, ISIN
peer groups and venue structure takes this screen's passer list from **154 to
94** and removes all seven pence-collapse false positives below — they are all
in the null-`PrimaryTicker` bucket, so they leave the universe regardless of
whether defect 1 is fixed. See `docs/providers/eodhd.md` for the rule table.

## Proposed fixes (none applied)

1. ~~**Fix the subunit heuristic (highest value).**~~ **DONE 2026-07-27, but not as proposed.**

   The original proposal here was to apply `EXCHANGE_SUBUNIT_HINTS` only when the listing's own currency
   is the subunit's *parent* (GBP for GBX, ZAR for ZAC, ILS for ILA). **That would not have worked.**
   Checking it against the data first: all 43 ISIN-matched LSE listings the catalog labels `GBP` are
   Swiss/European home lines (Phoenix, Schindler, Basler Kantonalbank, Georg Fischer, Sandoz), and not
   one is pence-quoted. Parent-gating would have kept dividing exactly the 15 of them that are already
   corrupted.

   The shipped fix deletes the derivation outright and makes `listing.currency` the sole source. EODHD
   states the quote unit per listing in `exchange-symbol-list` and pyvalue already stored it faithfully;
   the price parser was overriding it. The provider's `PriceQuote` type now has no currency field at all
   and `prepare_price_data` requires the listing currency, so the guess is unrepresentable rather than
   merely removed.

   Two findings drove that stronger form. First, the rule was wrong for two-thirds of London — the live
   catalog holds 2,467 `GBX` against 2,253 `USD`, 1,135 `EUR`, 912 `GBP` and a long tail — so it was
   never a narrow international-order-book problem. Second, being keyed on `price > 100` it was
   *unstable*: a single listing's stored series jumps 100x whenever its quote crosses that line
   (`0A7O.LSE` 1.3064 -> 90.69, `0A3T.LSE` 86.00 -> 1.0250), which corrupts history as well as levels.
   Measured blast radius: 180 of 779 ISIN-matched non-`GBX` LSE listings understated at ratios of
   0.0099-0.0102, plus 146 intra-listing 100x jumps on LSE, 13 on JSE and 21 on TASE.

   **The stored prices are not repaired by the code fix** — neither `market_data` nor
   `provider_market_data` retains the raw feed value, so `LSE`, `JSE` and `TA` must be re-fetched with
   `update-market-data` and their metrics recomputed before this screen's results change.
2. **Model depositary ratios, or refuse to price DR lines.** Add `depositary_ratio` to `listing`
   (NOT NULL DEFAULT 1.0, `CHECK (depositary_ratio > 0)`), populate from provider security metadata, and
   multiply the resolved share count by it. Where a line is a depositary receipt with an unknown ratio,
   return NA for market-cap-dependent metrics rather than a confidently wrong number.
3. **Detect cumulative-YTD cash-flow rows.** Before building a TTM window, check whether quarterly values
   increase monotonically within the fiscal year and whether Q4 equals FY; if so, difference them into
   discrete quarters or fall back to the FY row. This kills the Medistim false positive.
4. **Guard the component-debt undercount.** In `metrics/balance_sheet.py:resolve_total_debt`, when the
   provider rollup exceeds the component sum by more than ~1.5x, prefer the rollup (or emit NA).
5. **Require cash components to share a period end.** In `resolve_cash_position`, only add
   `ShortTermInvestments` to `CashAndCashEquivalents` when both rows carry the same `end_date`.
6. **Include minority interest in EV.** The concept is already ingested; adding it fixes Christian Dior's
   ranking distortion and any other partially-owned consolidator.
7. **Cross-listing consistency report.** The same issuer produced materially different values for the same
   metric across venues in ~25 cases (Apranga's owner-earnings CAGR differs 57% between its two German
   lines; Dunelm's `roic_10y_min` 0.26 vs 0.53; Inditex's interest coverage 22.5/55.7/100/184.6). A report
   command flagging same-issuer divergence would also have caught the Marimekko share-count staleness.
8. **Tighten share arbitration on implied corporate actions.** When the filing count and the provider
   snapshot differ by a near-integer ratio (2, 4, 5, 10) and the filing is more recent, prefer the filing.
9. **Extend the financial-sector exclusion** to brokers/exchanges (XTB), which the owner-earnings NWC
   chain does not currently exclude.

## Reading the CSV

`data/output/qarp_passers_verification_2026-07.csv` carries one row per passing listing, ordered by the
screen's own rank: symbol, entity, company key, rank, score, verdict, the gates flagged as materially
wrong, a one-line conclusion and the sources used. Verdicts are per company (fundamentals are shared
across venues) with per-listing overrides where a venue differs — currently only 0JX9.LSE.
