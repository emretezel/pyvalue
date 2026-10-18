# Watchlist Buy Ranking

**Living snapshot — maintained in place.** This document is the current,
complete picture of the [anchor watchlist](../reference/watchlist.md) ranked
for buy-attractiveness. On each refresh it is rewritten top to bottom — the
tables, the ranking, and the prose all move to the new data together, and
nothing is kept as an addendum. Prior versions live in git history: this
file's own log holds past snapshots (the thirteen-name 2026-08-28 snapshot
with Apple inserted 2026-09-26 ends at commit `5ab9831`), and the original
2026-07-29 nine-name memo with its three addendums sits under the old path
`docs/research/watchlist-ai-buy-ranking-2026-07.md`, ending at commit
`52a5699`.

**Snapshot date: 2026-10-18.**

The standing question, first posed by the author in July 2026: across the
watchlist, which names are the better buys on **quality**, **price**, and
**future growth prospects** — anchored by what the primary QARP screen
(`screeners/quality_reasonable_price_primary.yml`) says about each?

Scope: **all twenty-two watchlist names** — the thirteen of the previous
snapshot plus the nine that joined on 2026-10-18 (Lululemon, LVMH, Novo
Nordisk, PayPal, Paycom, Cirrus Logic, Realtek, Grupo México, Paylocity),
every one of which passes QARP 14 of 14 on the stored data. Citigroup is a
financial; QARP structurally cannot screen banks (the owner-earnings gates
need current assets/liabilities lines financial issuers do not report), so
it is ranked on bank-appropriate lenses and its corporate-shaped table rows
carry caveats. Cloudflare has never posted a GAAP-profitable fiscal year,
which makes the quality leg inapplicable rather than weak; it is ranked with
that stated. Four of the new names are tracked on their home-market listing
(Paris, Copenhagen, Taipei, Mexico City) and quoted in local currency.

Two data vintages, kept explicit throughout — and, for the first time, a
single forward vintage for every name:

- **Trailing/QARP data**: the stored `data/pyvalue.db` metrics from the
  **2026-08-11 full-universe run** (facts refreshed 2026-08-10). Nothing has
  been ingested since, so the thirteen original columns are unchanged from
  the previous snapshot and the nine new columns come from the same run.
  Prices are the **2026-08-07 close** for the US listings, **2026-07-27** for
  SK Hynix, LVMH, Novo Nordisk and Realtek, and **2026-07-24** for Grupo
  México — the 08-10 market-data refresh reached the US venues only. Known
  gaps, all recorded in the data notes: SK Hynix's 07-29 Q2 print and
  subsequent crash are not in the DB; NVDA's 08-26 Q2 FY27 print post-dates
  the fact refresh; Cloudflare's and Lululemon's latest quarters are
  fragments; LVMH's facts stop at FY2025 (semi-annual reporter); Novo
  Nordisk's Q2 is missing; Grupo México's TTM window is broken by a fragment
  quarter; and Paycom's Q2 cash-flow rows are six-month cumulative values
  stored as a quarter, which inflates its cash-flow metrics.
- **Forward/context data**: web research run **2026-10-18** — six parallel
  sweeps covering all twenty-two names under one protocol (consensus mostly
  stockanalysis.com / S&P Global via Yahoo, MarketScreener for the European
  and Taiwanese names, unless noted). Prices are the **2026-10-16 close on
  each exchange** (a normal session everywhere: New York, Seoul, Taipei,
  Paris, Copenhagen, Mexico City). The thirteen original names' forward
  layers were refreshed along with the nine new ones; the 2026-08-28 and
  2026-09-26 layers of the previous snapshot are superseded, not kept.

A third element is new in this snapshot: for the ten passers, the price
gate is **restated at the 10-16 close on the DB's own trailing basis** —
enterprise value moved by the change in market cap, debt and cash held at
the stored quarter — so that "passes QARP" is read against today's price
rather than a July/August one. Eight still clear the gate outright, Realtek
by three basis points; Paycom clears only on its inflated cash-flow row;
Paylocity misses by five basis points.

## QARP gate matrix (stored metrics, 2026-08-11 run)

Gate-by-gate evaluation of the screen's 14 criteria against stored metrics
(`pass`/`FAIL`/`NA`; an NA is a fail for the screen but shown distinctly).
Columns are in ranking order; the twenty-two names are split across two
tables for width.

**Ranks 1–11**

| Gate | ADBE | GOOGL | MSFT | NVDA | AVGO | NOVO-B | PYPL | MC | CRUS | HYNIX | AAPL |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ROIC years >12% ≥ 7/10 | pass | pass | pass | pass | FAIL | pass | pass | pass | pass | FAIL | pass |
| ROIC 10Y min ≥ 7% | pass | pass | pass | pass | FAIL | pass | pass | pass | pass | FAIL | pass |
| Gross margin ≥ 35% | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| GM 10Y std ≤ 7pp | pass | pass | pass | pass | FAIL | pass | pass | pass | pass | FAIL | pass |
| OPM 7Y min ≥ 5% | pass | pass | pass | pass | pass | pass | pass | pass | pass | FAIL | pass |
| CFO/NI TTM ≥ 0.90 | pass | FAIL | pass | FAIL | pass | pass | pass | pass | pass | pass | pass |
| CFO/NI 10Y median ≥ 0.90 | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| Net debt/EBITDA ≤ 2.5x | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| Debt service (6x cov or ≤1.5x) | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| No NI loss years (10Y) | pass | pass | pass | pass | FAIL | pass | pass | pass | pass | FAIL | pass |
| Share CAGR 5Y ≤ +1% | pass | pass | pass | pass | FAIL | pass | pass | pass | pass | pass | pass |
| **Price gate** (OEY/EBIT/FCF yield) | pass | FAIL | FAIL | FAIL | FAIL | pass | pass | pass | pass | FAIL | FAIL |
| iROIC 5Y ≥ 12% | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| Owner-earnings CAGR ≥ 7% | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| **Gates passed** | **14/14** | 12/14 | 13/14 | 12/14 | 8/14 | **14/14** | **14/14** | **14/14** | **14/14** | 8/14 | 13/14 |

**Ranks 12–22**

| Gate | C | GMEXICOB | PCTY | PAYC | LULU | 2379 | PLTR | AMD | INTC | NET | TSLA |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ROIC years >12% ≥ 7/10 | FAIL | pass | pass | pass | pass | pass | NA | FAIL | FAIL | NA | FAIL |
| ROIC 10Y min ≥ 7% | FAIL | pass | pass | pass | pass | pass | NA | FAIL | FAIL | NA | FAIL |
| Gross margin ≥ 35% | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | FAIL |
| GM 10Y std ≤ 7pp | FAIL | pass | pass | pass | pass | pass | NA | FAIL | FAIL | pass | pass |
| OPM 7Y min ≥ 5% | pass | pass | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | FAIL |
| CFO/NI TTM ≥ 0.90 | FAIL | pass | pass | pass | pass | pass | pass | pass | NA | NA | pass |
| CFO/NI 10Y median ≥ 0.90 | FAIL | pass | pass | pass | pass | pass | NA | pass | pass | NA | pass |
| Net debt/EBITDA ≤ 2.5x | pass | pass | pass | pass | pass | pass | pass | pass | pass | NA | pass |
| Debt service (6x cov or ≤1.5x) | pass | pass | pass | pass | pass | pass | pass | pass | FAIL | FAIL | pass |
| No NI loss years (10Y) | FAIL | pass | pass | pass | pass | pass | NA | FAIL | FAIL | FAIL | FAIL |
| Share CAGR 5Y ≤ +1% | pass | pass | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | FAIL |
| **Price gate** (OEY/EBIT/FCF yield) | pass | pass | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | FAIL |
| iROIC 5Y ≥ 12% | FAIL | pass | pass | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL |
| Owner-earnings CAGR ≥ 7% | NA | pass | pass | pass | pass | pass | NA | pass | NA | NA | NA |
| **Gates passed** | 6/14 | **14/14** | **14/14** | **14/14** | **14/14** | **14/14** | 5/14 | 6/14 | 3/14 | 2/14 | 5/14 |

**Ten of twenty-two pass QARP in full** — Adobe and the nine names added on
2026-10-18, every one of them with all fourteen criteria computable. The
other name clearing the price gate is Citigroup, on a 9.4% trailing EBIT
yield whose EV construction means little for a bank (see its entry). Among
the non-passers, MSFT and AAPL are 13/14 on the price gate alone; NVDA's TTM
cash-conversion miss (0.79) is the documented Blackwell-ramp working-capital
artifact (10Y median 0.97 passes) and GOOGL's (0.76) is the one-time
SpaceX/Anthropic investment gains inflating net income (10Y median 1.54
passes). Matrix verified against `pyvalue report-screen-failures` on the
same twenty-two symbols: 10/22 full passers, and all fourteen per-criterion
fallout counts reconcile (price 11, ROIC years 8, ROIC minimum 8, loss years
8, GM stability 6, OPM minimum 6, share CAGR 6, cash conversion 5, iROIC 5,
owner-earnings CAGR 5, cash-conversion median 3, debt service 2, gross
margin 1, net debt/EBITDA 1).

### Pass and failure patterns

1. **The passers split three ways on how they clear the price gate** (values
   and the re-pricing in the [watchlist profiles](../reference/watchlist.md#the-nine-2026-10-18-additions-qarp-profiles)):
   **all three arms** — Lululemon, PayPal, LVMH (and Adobe); **two arms** —
   Novo Nordisk and Grupo México on owner earnings and EBIT (FCF yields of
   2.0% and 4.8%: capex-heavy years), Cirrus Logic on EBIT and FCF; **the FCF
   arm alone** — Paycom, Paylocity, Realtek, the last by fifteen basis
   points. Restated at the 10-16 close, Adobe, Lululemon, LVMH, Novo
   Nordisk, PayPal and Cirrus Logic all pass by *more* (their shares fell
   while trailing earnings did not), Cirrus now on all three arms; Grupo
   México still passes on its EBIT arm after an 18% rise; Realtek passes by
   3bp; Paycom's FCF arm reads 6.2% on the stored row but ~4.5–4.7% once the
   Q2 cumulation artifact is corrected — a fail; Paylocity's reads 4.95% — a
   fail by 5bp. The screen's cheapness, in other words, is intact for the
   derated franchises and has already closed for the two HCM software names.
2. **Price-only — MSFT and AAPL, and effectively GOOGL and NVDA.** MSFT and
   AAPL fail the price gate alone. GOOGL and NVDA each add one TTM
   cash-conversion artifact (above) that their 10-year medians contradict;
   quality is not in question.
3. **Cyclicality-shaped — SK Hynix.** Every failure is a memory-cycle gate
   (one loss year, GM std 17.9pp, ROIC 10Y min −8.4%) plus a price gate
   measured at a pre-crash price. QARP punishing cyclicality by design.
4. **The serial-acquirer decade — Broadcom.** All fourteen criteria are
   computable and the cash gates pass emphatically (CFO/NI 10Y median 2.04,
   owner-earnings CAGR 23.9%); every failure traces to purchase accounting —
   the FY2016 merger-charge loss year, ROIC measured against goodwill-swollen
   invested capital, a GM "instability" that is a monotonic climb from 44.9%
   to 67.8%. Scar tissue, not weakness; QARP declines to look past it by
   design. Detail in the
   [Broadcom profile](../reference/watchlist.md#broadcom-avgous-qarp-profile-2026-08-26).
5. **Broad failures — AMD, INTC, TSLA, PLTR.** Weak or short quality history
   *and* an extreme price.
6. **Structurally outside the framework — C, NET.** Citigroup: the
   owner-earnings chain is NA for financials by design, and its computed
   corporate rows (CFO/NI negative on bank cash-flow noise, "gross margin",
   EV multiples) are not economically meaningful for a balance-sheet
   business. Cloudflare: ten GAAP loss years in ten make the
   profitability-based guards refuse to produce numbers — correctly (detail
   in its [profile](../reference/watchlist.md#cloudflare-netus-qarp-profile-refreshed-2026-08-11)).

## Selected trailing metrics (stored metrics, 2026-08-11 run)

Prices are the 2026-08-07 close for US listings; 2026-07-27 for SK Hynix
(pre-crash), LVMH, Novo Nordisk and Realtek; 2026-07-24 for Grupo México.
Non-US names are in their listing currency.

**Ranks 1–11**

| Metric | ADBE | GOOGL | MSFT | NVDA | AVGO | NOVO-B | PYPL | MC | CRUS | HYNIX | AAPL |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Price (DB) | $265.21 | $354.30 | $499.99 | $223.96 | $427.76 | DKK 326.50 | $59.07 | €466.80 | $124.92 | ₩1,816,000 | $313.33 |
| Market cap (DB) | $105.4B | $4.36T | $3.71T | $5.42T | $2.04T | DKK 1,453B | $50.5B | €230.2B | $6.3B | ₩1,289T | $4.57T |
| ROIC TTM | 53.9% | 22.4% | 25.6% | 99.8% | 24.3% | 42.6% | 21.2% | 12.2% | 26.3% | 49.2% | 90.3% |
| ROIC 7Y median | 33.7% | 26.4% | 32.2% | 37.0% | 12.4% | 65.6% | 16.9% | 16.7% | 19.4% | 6.6% | 64.6% |
| ROIC 10Y median | 29.6% | 20.2% | 28.9% | 38.5% | 9.4% | 77.9% | 15.0% | 16.9% | 18.9% | 12.4% | 51.8% |
| Gross margin TTM | 89.1% | 60.9% | 67.9% | 74.1% | 67.0% | 81.9% | 45.8% | 66.2% | 52.8% | 68.3% | 48.7% |
| Operating margin TTM | 36.1% | 33.1% | 46.8% | 64.0% | 43.7% | 45.3% | 18.8% | 21.9% | 23.1% | 58.6% | 33.2% |
| FCF margin TTM | 42.2% | 11.9% | 20.2% | 47.0% | 43.4% | 9.5% | 19.3% | 18.6% | 27.9% | 30.8% | 29.3% |
| Revenue CAGR 10Y | 17.4% | 18.3% | 13.8% | 45.7% | 25.1% | 11.1% | 13.6% | 8.5% | 5.5% | 17.9% | 5.9% |
| Net debt / EBITDA | 0.15x | −0.75x | 0.27x | −0.41x | 1.09x | 0.75x | 0.40x | 0.96x | −1.45x | −0.38x | 0.13x⁴ |
| EV/EBIT | 11.8 | 28.6 | 24.3 | 33.0 | 63.1 | 10.6 | 8.3 | 14.3 | 11.6 | 16.2 | 29.7 |
| Trailing P/E¹ | 11.6 | 17.8² | 27.9 | 38.3 | 52.6 | 14.1 | 11.2 | 21.4 | 13.5 | 17.2 | 36.4 |
| FCF yield on EV | 9.9% | 1.3% | 1.8% | 2.2% | 1.6% | 2.0% | 12.3% | 5.9% | 10.4% | 3.2% | 3.0% |
| Owner-earnings yield on EV | 5.4% | 1.7% | 2.3% | 0.5% | 0.9% | 5.1% | 7.1% | 5.5% | 4.4% | 0.5% | 2.1% |
| iROIC 5Y | 1.00 (cap) | 33.0% | 18.3% | 79.0% | 18.3% | 24.1% | 1.00 (cap) | 16.1% | 46.8% | 53.7% | 533%³ |
| Share count CAGR 5Y | −3.5% | −2.0% | −0.3% | −0.7% | +2.5% | −0.8% | −5.4% | −0.2% | −2.5% | +0.7% | −2.5% |
| Piotroski F | 6 | 8 | 6 | 4 | 7 | 5 | 8 | 6 | 9 | 8 | 8 |

**Ranks 12–22**

| Metric | C | GMEXICOB | PCTY | PAYC | LULU | 2379 | PLTR | AMD | INTC | NET | TSLA |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Price (DB) | $135.00 | MXN 207.28 | $150.42 | $214.94 | $128.58 | TWD 762.00 | $172.01 | $483.36 | $101.65 | $300.27 | $328.58 |
| Market cap (DB) | $226.4B | MXN 1,614B | $8.3B | $9.7B | $14.8B | TWD 391B | $395.7B | $789.1B | $512.7B | $96.4B | $1.30T |
| ROIC TTM | 4.3% | NA⁵ | 25.3% | 34.2% | 28.4% | 27.7% | 39.4% | 8.8% | 0.0% | NA | 4.2% |
| ROIC 7Y median | 2.9% | 20.5% | 25.3% | 34.1% | 42.7% | 25.9% | NA | 5.1% | 0.0% | −7.1% | 9.6% |
| ROIC 10Y median | 3.6% | 16.3% | 24.4% | 39.6% | 44.5% | 24.7% | NA | 11.6% | 11.3% | NA | 6.7% |
| Gross margin TTM | 54.6% | 49.6%⁵ | 69.2% | 80.1% | 55.7% | 48.6% | 84.8% | 53.2% | 38.9% | 73.3% | 18.9% |
| Operating margin TTM | 16.1% | NA⁵ | 21.8% | 30.3% | 18.2% | 11.1% | 42.8% | 15.7% | 0.1% | −9.3% | 4.2% |
| FCF margin TTM | −23.1% | NA⁵ | 25.3% | 35.3%⁶ | 11.4% | 14.0% | 54.6% | 20.3% | 5.0% | 15.3% | 5.6% |
| Revenue CAGR 10Y | 6.7% | 8.8% | 22.6% | 24.8% | 18.3% | 14.5% | NA | 24.1% | −0.5% | NA | 37.1% |
| Net debt / EBITDA | 1.29x | −0.10x | −0.30x | 0.93x | 0.24x | −3.03x | −3.45x | −0.93x | 1.67x | NA | −3.15x |
| EV/EBIT | 10.6 | 10.3⁵ | 21.2 | 16.1 | 7.6 | 24.5 | 147 | 120 | 6840 | NA | 289 |
| Trailing P/E¹ | 13.9 | 15.6 | 19.3 | 23.0 | 10.4 | 27.2 | 143 | 87.2 | 105 | 448 | 189 |
| FCF yield on EV | −13.5% | 4.8% | 5.5% | 7.2%⁶ | 8.3% | 5.2% | 0.9% | 1.1% | 0.5% | 0.4% | 0.5% |
| Owner-earnings yield on EV | 3.4% | 5.3% | 2.9% | 3.0% | 9.2% | 4.1% | 0.0% | 0.3% | −0.2% | −0.1% | 0.4% |
| iROIC 5Y | 1.5% | 117% | 31.6% | 27.8% | 35.3% | 21.4% | 41.0% | 3.0% | −56.3% | −2.6% | 2.9% |
| Share count CAGR 5Y | −3.5% | 0.0% | −0.6% | −4.6% | −2.5% | −0.2% | +6.3% | +6.1% | +4.6% | +2.9% | +1.1% |
| Piotroski F | 6 | 7 | 8 | 5 | 6 | 4 | 8 | 7 | 6 | 4 | 5 |

¹ 1 / earnings yield on EODHD's *adjusted* EPS series — non-GAAP for
heavy-SBC/amortisation issuers, so NET (448x on +$0.67 EPS against a GAAP
loss), AVGO (52.6x vs ≈$6.0 GAAP diluted), and INTC are flattered; the
vendor caveat is recorded in the watchlist profiles. AAPL's runs the other
way (vendor TTM EPS $8.61 vs a filed $8.71; 36.0x on the filing).
² Deflated, not flattered: the denominator carries GOOGL's one-time
investment gains; the core trailing multiple is materially higher.
³ Not the 1.00 cap: EODHD's lease-inclusive FY2025 debt rollup measures a
small positive five-year change in invested capital, so the ratio is a
literal $55.6B of new NOPAT on $10.4B of new capital. On a lease-consistent
basis the capital *shrank* and the metric would read the cap. Pass either way
(profile).
⁴ pyvalue's cash scope: cash plus *current* marketable securities. Apple's
$84B of non-current marketable securities sit outside the chain; on the
company's own basis it is $62B net cash.
⁵ Grupo México's Q3 2025 is a three-concept fragment, so no trailing
twelve-month window forms: the TTM margins and ROIC are NA, while
`gross_margin_ttm`, `cfo_to_ni_ttm` and the EBIT behind `ev_to_ebit` and the
EBIT yield fell back to the FY2025 annual figures. Verdicts are safe (FY2025
is a complete year); the fallback policy is inconsistent across the TTM
family (data notes).
⁶ Inflated: Paycom's Q2 2026 cash-flow rows are six-month cumulative values
stored as a quarter, so every TTM cash-flow metric double-counts Q1. CFO/NI
TTM reads 2.08 where ~1.64 is right; the FCF margin and yield correct to
roughly 27% and 5.2–5.5%; `shareholder_yield_ttm` 29.6% corrects to ~18.5%
(data notes).

MSFT is net debt +0.27x EBITDA (finance leases in, since the 2026-08-10 debt
rebuild), not net cash, and AAPL reads +0.13x on the cash scope in note ⁴;
Novo Nordisk (0.75x), LVMH (0.96x), Paycom (0.93x, all of it drawn in H1
2026 to fund buybacks) and PayPal (0.40x) carry modest net debt; every other
name except INTC (1.67x), AVGO (1.09x) and C is net cash. C's FCF rows and
EV multiples are bank-noise — read its entry, not this table.

## The ranking

Prices below are 2026-10-16 closes on each exchange. Consensus multiples are
computed at those prices on non-GAAP EPS unless stated; fiscal-year labels
are per company. Where a passer's price gate is quoted "re-priced", it is the
stored trailing yield moved to the 10-16 price as described in the preamble.
The confidence ordering and the expected-return ordering still diverge at
the top: GOOGL/MSFT are the higher-*certainty* businesses; Adobe ranks first
because at a third of their multiple its return does not depend on multiple
persistence, only on the disruption bear case being wrong. Among the nine
new names, the ordering follows the framework's own three legs: the derated
franchises that still pass at today's price (Novo Nordisk, PayPal, LVMH,
Cirrus) sit above the names whose cheapness has closed (Paylocity, Paycom),
the peak-cycle miner, the shrinking retailer and the thinnest pass
(Realtek).

### 1. Adobe — the price alone still carries the thesis, and the price is lower

$245.20 (−15.2% since 08-27; −6.4% versus the author's $262 entry of
2026-08-07; +4.1% since 09-25; the 52-week range is $190–364). Market cap
$95.4B. **Held position** ([portfolio notes](../portfolio/adbe-us.md)).

- **Quality**: 14/14 QARP gates, the best economics in the set at its size —
  89% gross margin, 34% ROIC 7Y median, 42% FCF margin, shares shrinking
  3.5%/yr net. The Q3 FY26 print (09-10) was a beat-and-raise: revenue
  $6.76B +13%, non-GAAP EPS $6.13, total ARR $27.50B +11.2%, AI-first ARR
  >$650M (+150%), total MAU past 1 billion, FY26 non-GAAP EPS guided
  $24.45–24.50 and revenue to $26.58–26.63B; $24.55B left on the buyback.
- **Price**: **~10.0x FY26 / 8.9x FY27** consensus EPS ($24.48 / $27.51) —
  a terminal-decline multiple on a business still compounding earnings at
  mid-teens. Re-priced to $245.20 the DB's own arms read EBIT yield 9.2%,
  FCF yield 10.7%, normalised owner-earnings yield 5.9% — every arm clears
  with more room than in August.
- **Growth**: the weak leg and the crux. FY26 revenue +11.9%, FY27 +9.1%.
  Inside the Q3 print: RPO only +8%, cRPO +9%, net-new ARR −36–37% YoY as
  management traded near-term ARR for the freemium funnel and deferred
  pricing actions indefinitely; the Q4 revenue guide midpoint ($6.825B) sat
  below consensus.
- **Overhangs**: the succession resolved the wrong way for the market —
  **Anil Chakravarthy** (ex-Informatica, head of Customer Experience) named
  CEO effective 2026-12-01 on 09-03, and **David Wadhwani**, the creative
  business head and presumed heir, resigned the same week; the CFO seat is
  still interim. The stock fell ~7% on the news. Then the AI-substitution
  thesis found a face: **Wells Fargo double-downgraded to Underweight ($225)
  on 10-12** and **HSBC cut to Hold ($267) on 10-13** after a small developer
  (ArtCraft) released free open-source clones of the core apps. Street:
  Hold, average target $272 (above the price for the first time since
  spring). **Adobe MAX runs 11-10 to 11-12**; the Q4 print is mid-December,
  date not yet set.

The math needs even less benevolence than in August: at ~8.9x FY27 with EPS
compounding low-double-digits buyback-assisted, a merely *held* multiple
implies ~20% annual returns; a re-rating to 12x adds a third on top. The bear
case has to be right — a genuine substitution of Creative Cloud by generative
tools and clones — not just loud. Everything rides on whether the new CEO
converts a billion MAU into paid ARR before the narrative becomes
self-fulfilling. Highest expected return per unit of price risk on the list;
emphatically not the highest-certainty holding, and the only passer whose
risk is to the *quality* leg rather than the price.

### 2. Alphabet — the most growth per multiple point, flat for seven weeks while the numbers improved

$340.59 (−0.02% since 08-27; −17% off the May high), $4.17T. **~25.9x FY26
adjusted / 22.3x FY27** consensus EPS ($13.14 / $15.25; the GAAP-inclusive
FY26 figure of $20.59 carries the $99B Q2 equity gain and is not a valuation
basis). Nothing has printed since Q2 (07-22: revenue $119.8B +24%, Cloud
+82% at a 35.6% margin, backlog $514B, Search +17%) — **Q3 prints 10-28**,
consensus revenue ~$127.7B. What changed is the tail: the **ad-tech remedies
came in behavioural, not structural (09-02)** — no AdX divestiture — so the
break-up risk is gone, leaving the search-case cross-appeal and the ad-tech
final-judgment haggling. What did not change is the bill: 2026 capex
$195–205B, 2027 guided to "increase significantly", Q2 FCF −$5.9B, long-term
debt roughly doubled to ~$98B and a reported ~$85B equity raise in June —
the 10-16 "who pays for AI" selloff hit it harder than Microsoft for exactly
that reason. The 12/14 gate count is the gains artifact (10Y median CFO/NI
1.54). Street: Strong Buy, average target $430. Ranked above MSFT on
price-for-growth — four multiple points cheaper on next year, growing a
third faster, Cloud's profit now real — against the fattest funding question
and legal tail of the platforms.

### 3. Microsoft — the certainty compounder, at a full price that keeps getting fuller

$529.05 (+4.75% since 08-27; its best quarter since 1991 in calendar Q3),
$3.93T. **~26.7x FY27 / 22.3x FY28** consensus EPS ($19.78 / $23.72). The
July print (Azure +43%, FY26 Azure >$100B, revenue $90.0B +18%, commercial
RPO $678B — +25% ex-OpenAI) and the 09-25 Copilot overhaul (persistent
agent, 30M+ paid seats moving to seat-plus-consumption pricing) carried the
stock to within 5% of its high. **Q1 FY27 prints 10-28**: Azure guided ~45%
cc, capex guided ">$50B" for the quarter, the first quarter on a 25-year
datacenter depreciation life. The OpenAI relationship has become two-sided:
the FT's ~$50B-not-$70B ARR story (10-08) cost a point, and a court filing
unsealed 10-16 ("Project Mango", an OpenAI-funded crawler Microsoft told
staff to keep quiet) adds a litigation thread; the FTC's cloud/AI bundling
probe is unresolved. 13/14 gates, price only; at $529 the arms are roughly
half the bar. Street: Strong Buy, average target $599. The highest-certainty
compounder here; 26.7x is a fair price for it and not a bargain — the June
2026 trough (~$350) was the QARP-shaped entry and it is long gone.

### 4. NVIDIA — the numbers keep going up and the stock keeps going down

$214.13 (−6.1% since 08-27, −12% off the high; the SOX fell eight straight
sessions into 10-16, its longest streak since 2008), $5.17T. **~23.0x FY27 /
13.5x FY28** consensus EPS ($9.31 / $15.91; FY27 ≈ calendar 2026). Q2 FY27
(08-26) was a ~4% beat ($96.2B, Data Center $89.0B) with a **$108B Q3
guide** and China carried at zero; Vera Rubin is in production and its rack
design was open-sourced at OCP (10-12); Australia signed a 2 GW buildout
(09-09). The 12/14 count is the documented working-capital artifact. What
the market is now pricing is the 10-Q: **$279B of supply commitments** (from
$119B), **up to $108.5B of guarantees** behind customers' datacenter
buildouts, one customer at 16% of revenue, five at 70% of receivables —
NVIDIA has become a financier of its own demand, and the 10-16 CoreWeave
credit scare was the read-through. Street: Strong Buy, average target $329,
with no downgrades in the window. **Q3 FY27 prints 11-17**; GTC Washington
keynote 12-01. Cheaper than in August on every forward number while
estimates rose; the doubt is the cycle and now the customers' balance
sheets, not the company. Kept one notch below MSFT on certainty; it is the
cheaper of the two on every forward number.

### 5. Broadcom — the cleanest print in the group, down 11% anyway

$328.75 (−11.5% since 08-27, −34% off the June high), $1.57T. **~28.2x FY26
/ 16.9x FY27** consensus EPS ($11.66 / $19.40) — the cheapest next-year
multiple among the AI semis, with consensus underwriting the guide in full.
Q3 FY26 (09-02): revenue $29.6B +86%, **AI revenue $16.7B +221%** (56% of
revenue), non-GAAP EPS $3.32, record 68% operating margin, **Q4 guided $34.8B
with AI $21.7B**, FY26 AI raised to $58B, and the headline: supply "secured"
for **~$115B of AI revenue in FY27 and ~$230B in FY28**. The customer story
is the Google long-term TPU agreement ("multi-tens of billions annually"),
Anthropic's 1→5→10 GW ramp to largest XPU customer in 2027 and OpenAI's
"Jalapeño" from 2027 — six XPU customers in all. The stock fell because the
market now reads how that growth is financed: Broadcom set up an AI "XPV"
vehicle with Apollo and Blackstone whose first **$35B tranche funds
Anthropic's deployment**, two of the six customers rely on Broadcom-enabled
financing, and Hock Tan said on 10-12 that capital, not chips, is the
binding constraint. 8/14 gates on the serial-acquirer pattern; the price
arms fail by ~4x on trailing numbers. Street: Strong Buy, average target
$528; Macquarie upgraded 09-03. **Q4 FY26 prints 12-09.** Slotting stays
beside NVDA, one notch back: the same bet in backlog form, now priced
tighter than NVDA on next year, with concentration and customer-credit risk
that the company itself is warehousing.

### 6. Novo Nordisk — elite economics at a patent-cliff multiple, after the price reset

DKK 259.60 (−20.5% since the DB's 07-27 price; −35% off the 52-week high;
−7.7% on the 09-21 capital markets day alone), DKK 1.15T ≈ $172B.
**~11.9x FY26 / 11.7x FY27** consensus EPS (DKK 21.84 / 22.17) — the Street
models **no EPS growth** between the two years. Dividend yield 4.5%; DKK 15B
buyback (half done by August).

- **Quality**: the best trailing economics of the nine — ROIC 10Y median
  78%, worst year 38%, gross margin 82% inside a 1.0pp band, operating
  margin 45%, ten of ten ROIC years above 12%. 14/14 gates.
- **Price**: passes on the EBIT (9.4%) and owner-earnings (5.1%) arms at
  the DB price; re-priced to DKK 259.60 those read **11.6% and 6.3%**. The
  FCF arm fails (2.0% → 2.4%) because capex absorbs most of operating cash
  flow — FCF margin 9.5% against a 45% operating margin — the capacity
  build-out and the acquired Catalent sites. EV/EBIT ~8.6x trailing at
  today's price for a 45%-margin pharma franchise.
- **Growth**: the reset. 2026 guided to **0% to −6% at constant currency**
  for both adjusted sales and operating profit (raised in August from −4% to
  −12%); US list prices fall to **$675 for Wegovy (~−50%) and Ozempic
  (~−35%) from 2027-01-01** on top of 340B and Medicare negotiation; the
  09-21 CMD anchored 2026–30 growth at "industry peer" levels (~mid-single
  digits) with the **2032 US semaglutide cliff** named by the CEO as the
  elephant in the room. Q2 (08-04): adjusted sales +3% CER, Wegovy pill DKK
  3.2B (>5M US scripts since January), DKK 6.3B of pipeline impairments; a
  further ziltivekimab write-down is flagged for Q3.
- **Overhangs**: Lilly's oral orforglipron approved 04-01 and on the market
  first and cheaper; Zepbound gaining share; ~9,000 job cuts plus ~4,000
  further exits; generics already approved in Canada and Brazil. Street:
  Hold, average target DKK 303; Morgan Stanley to Underweight (09-11), Citi
  and BMO cut targets in October. **Q3 prints 11-04**; the **CagriSema US
  decision is expected in Q4** (21% weight loss in REDEFINE 9).

The screen's read is correct and the derating is rational at the same time:
a 12x multiple with flat earnings is what the market applies to a pharma
company that has told it growth to 2030 will be ordinary and the cliff is
2032. The bet is that volume (oral Wegovy, 60M patients by 2030, CagriSema)
outruns the price reset the 2026–27 numbers already absorb — none of that
is in a 12x multiple, and a 4.5% yield plus buybacks pays for waiting. The
highest-quality cheap name on the list after Adobe, with a structural, not
cyclical, bear case on price and competition.

### 7. PayPal — cheap, clean, shrinking in share; the buyback is most of the return

$56.70 (−4.0% since the DB's 08-07 price; −28% off the 52-week high; the
$60.50 Stripe/Advent bid collapsed 08-28 and the stock fell 12.7% that day),
$48.5B. **~10.5x FY26 / 9.8x FY27** consensus EPS ($5.39 / $5.78).

- **Quality**: 14/14, all arms with room (OEY 7.1% / EBIT 12.0% / FCF 12.3%
  at the DB price; 7.4% / 12.4% / 12.8% re-priced); ROIC 10Y median 15%,
  cash conversion 1.5–1.6x, share count −5.4%/yr; iROIC at the 1.00 cap
  because invested capital shrank on buybacks. $15.3B cash against $13.4B
  debt.
- **Price**: a ~12% adjusted-FCF yield, ~$6B of FY26 buybacks (≈12% of
  market cap) plus a new $0.56 dividend; the stock trades at the bottom of
  its three-year P/E range (NTM ~9.5x vs a 12.7x mean).
- **Growth**: the problem. Q2 (07-28): revenue +5%, **transaction-margin
  dollars +1%** (+3% ex-float), active accounts flat (−0.2M q/q), non-GAAP
  operating margin −248bp; online branded checkout +2%, guided to **1–2%
  for Q3** after a September trim; FY26 non-GAAP EPS ≈ $5.38, GAAP down
  mid-single digits. Bernstein puts PayPal's US digital-wallet share at ~40%
  from 90% in 2017 as Apple Pay (~20%) and Shop Pay compound. **New CEO
  Enrique Lores** (ex-HP, since 03-01) is running a ≥$1.5B cost take-out,
  ~20% headcount reduction and a three-business reorganisation; the growth
  payoff is framed as 2027–28 and the agentic/stablecoin optionality
  (PYUSD, ChatGPT Instant Checkout, the Glance native-checkout deal of
  10-15) as "2028 and beyond".
- **Overhangs**: BofA resumed at **Underperform ($53) on 10-15** arguing the
  Braintree under-pricing unwinds as the businesses run independently;
  Goldman Sell $49. Street: Hold, average target ~$57 — at the price. **Q3
  prints 10-27** pre-market, guided to a low-single-digit EPS decline.

PayPal is Adobe's shape with the growth leg removed: a clean balance sheet,
a ~10% earnings yield and a buyback that retires 5–6% of the shares a year,
on a franchise whose core button is losing share. At ~10x the stock already
discounts ex-growth, and a financial buyer put a $60.50 floor under it in
July; the bear case is structural and the price knows it. Ranked below Novo
because the quality is lower and the decline is in the core product, above
LVMH because every arm passes with room and the near-term print is a low
bar.

### 8. LVMH — the decade-low multiple on a franchise whose engine is shrinking

€397.00 (−15.0% since the DB's 07-27 price; −34% off the 52-week high;
"more than 40% so far this year"), €195.8B ≈ $220B. **~18.0x 2026 / 16.6x
2027** consensus EPS (€22.02 / €23.96) — the bottom of LVMH's ten-year
range. Dividend €13.00 (3.3%), €5.50 interim paid 12-03.

- **Quality**: 14/14 on FY2025 facts (the DB holds no 2026 interim —
  semi-annual reporter): nine of ten ROIC years above 12%, gross margin 66%
  inside a 1.4pp band, cash conversion 1.5–1.7x, net debt 0.96x EBITDA
  (€8.2B, −19% in H1).
- **Price**: the thinnest all-arms pass at the DB price (5.5% / 7.0% /
  5.9% against 5 / 6.67 / 5 bars); **re-priced to €397 it reads 6.3% /
  8.1% / 6.9%** — comfortable, and the FCF arm now clears the Graham screen's
  6% bar too. EV/EBIT ~12.4x trailing at today's price.
- **Growth**: negative at the core. **Q3 2026 revenue (printed 10-12):
  group +1% organic** against +1.3% consensus; **Fashion & Leather Goods
  −5%** (consensus −1.8%), Asia ex-Japan −5%, US +5%, Europe +4%; H1
  recurring operating profit −4%, F&LG profit −7%. Consensus has 2026
  revenue −0.4% and 2027 +4.7%, with FY27 EPS cut from €25.56 to €24.14 in
  ninety days. The CFO called the Chinese recovery "delayed rather than
  structurally impaired"; a North American freight-partner problem cost
  ~50bp of group growth; Louis Vuitton faces a reported consumer backlash
  in China.
- **Overhangs**: Bernard Arnault, 77, deferred the succession question
  "seven to eight years" at the April AGM; Perfumes & Cosmetics flat;
  JPMorgan, UBS and Jefferies cut targets 10-13/14. Street: Outperform,
  average target €503 (+27%). **FY2026 results ~2027-01-25** — the H2 F&LG
  margin is the number.

The asymmetry here is in the multiple, not the estimates: 16.6x next year
prices LVMH as a cyclical luxury house, and if F&LG merely stabilises in
2027 the de-rating is over-done. The bear case is mostly cyclical (China
sentiment, luxury normalisation) with a structural tail (Vuitton's
desirability in China, succession opacity at a controlling shareholder).
Ranked behind PayPal because the engine is still decelerating into the next
print and the DB's view of it ends at FY2025.

### 9. Cirrus Logic — net cash a fifth of the market cap, and 80% of revenue from one customer

$107.87 (−13.6% since the DB's 08-07 price; −2.7% since 08-27 after a round
trip through $122; a new 52-week low on 10-12), $5.4B. **~12.9x FY27**
consensus non-GAAP EPS ($8.38, −9.5% YoY; the FY28 non-GAAP consensus was
not retrievable; ~15x on the lower GAAP-style series).

- **Quality**: the cleanest of the nine — Piotroski 9, net cash −1.45x
  EBITDA (~$1.17B of cash and investments, no borrowings), coverage 471x,
  cash conversion 1.4–1.6x, nine of ten ROIC years above 12%, buybacks
  retiring ~2–3% of shares a year. 14/14.
- **Price**: passes on EBIT (8.6%) and FCF (10.4%) at the DB price;
  **re-priced to $107.87 all three arms clear — 5.2% / 10.2% / 12.3%**.
  EV/EBIT ~9.8x trailing.
- **Growth**: flat to down. Q1 FY27 (08-05): record Q1 revenue $459.7M
  +13%, non-GAAP EPS $1.84; Q2 guided $510–570M at 52–54% gross margin.
  Consensus FY27 revenue +1%, EPS −9.5%: smartphone units are expected to
  fall in 2H 2026 and 2027 (Apple's split iPhone 18 launch pushes base-model
  volume to spring 2027), while the PC/SoundWire diversification — "10% of
  revenue over the next few years" — has been deferred by the memory
  shortage. Content gains (Face ID chips under Apple's American
  Manufacturing Program, ~$2 per iPhone per Stifel; a fourth-generation
  camera controller) are the bull case.
- **Overhangs**: **KeyBanc downgraded to Sector Weight on 10-15** on Apple
  cutting TSMC wafer starts by 15–20M iPhones; Stifel/KeyBanc/Barclays cut
  targets after the August print; a rejected bid for Synaptics raised
  strategic questions. Street: Hold, average target $151–163 (+40–51%).
  **Q2 FY27 prints 11-04.**

A net-cash company at ~10x EBIT buying back stock at a 52-week low is what
the screen is built to find; the price pays for a down year on a single
customer's unit cycle, and the content story has to offset units before the
multiple moves. Ranked ninth: cheaper than LVMH on every arm, but a
single-customer business with no growth in the forward numbers.

### 10. SK Hynix — the cheapest stock on the list is a quarter cheaper than in August

₩1,272,000 (10-16 Seoul close; **−23.1% since 08-28**, −57% off the June
peak, −24% in the five sessions to 10-16), ₩927T ≈ $696B. **~3.6x FY26 /
2.7x FY27** consensus KRW EPS (₩354,198 / ₩466,736; ~4.8x FY26 stripping
the Q2 one-off investment gain), with **₩69.4T net cash**. Nothing printed
in the window: **Q3 prints 10-28** (6-K confirmed; FnGuide consensus
operating profit ~₩74T, Meritz ₩80T), and the **₩40T buyback-and-cancel**
(~3.3% of shares, 08-20 to 11-19, returns >50% of 2025–27 FCF) is executing
into the weakness with no interim disclosure. What moved the stock was a
**CXMT "denser DRAM" announcement (10-13)** — read by analysts as a preview
with no production timeline and meaningful volume no earlier than late 2027
— colliding with a Korean rates/AI-funding scare (Kospi −8% on 10-15) and a
soft HBM4 equipment-order headline. The cycle itself has not turned:
TrendForce has 4Q26 DRAM contract prices +10–15% and NAND +15–20% with 2027
supply tight; Samsung's 3Q26 preliminary operating profit was ~₩107T.
Goldman called the risk/reward attractive on 10-16. 8/14 gates — every
failure a memory-cycle gate plus a price arm measured at a pre-crash price;
re-priced, the trailing EBIT yield on the DB's through-Q1 basis is well
above the bar. Street: Strong Buy, average target ₩3.14M (+147%). Still the
statistical outlier, the highest torque in both directions, and a sizing
decision rather than a conviction decision; it slips from sixth to tenth
only because four quality names that pass the screen outright now sit
above it.

### 11. Apple — a record high on a re-rating, not a re-earning

$343.46 (an all-time closing high; +9.2% since 08-27, +0.7% since 09-25,
+41% off the January low), $5.01T. **~38.9x FY26 / 35.9x FY27** consensus
EPS ($8.83 / $9.58) — ten multiple points above MSFT, thirteen above GOOGL,
for a fraction of their growth.

- **Quality**: 13/14, tied with MSFT as the best non-passer, every criterion
  computable and every input reconciled to SEC filings. Ten of ten ROIC
  years above 12%, a decade of gross margin inside a 3.3pp band, cash flow
  never below 0.99x net income, shares −2.5%/yr. The mega-cap that is *not*
  spending $50B a quarter.
- **Price**: the sole failing gate, all three arms by roughly half — and
  worse at $343 than at the DB's $313.
- **Growth**: FY27 consensus **revenue +10.4%, EPS +8.5%**; the September
  quarter was guided +9–11% with gross margin 47–48% under memory-cost
  pressure. The new product cycle is real: the **$1,999 iPhone Duo foldable
  sold out to late January within half an hour of pre-orders on 10-16**,
  iPhone 18 Pro at $1,199/$1,299, Gemini-based Siri AI shipped 09-14 —
  against Nikkei-reported 15–20% cuts to October 18 Pro component orders.
  CEO John Ternus since 09-01, first call on **11-02** when Q4 FY26 prints.
- **Overhangs**: the Street's average target ($329–340) is *below* the
  price; UBS Neutral $296, KeyBanc Sell $250; Section 301 tariff actions
  unresolved; the Google default-payment appeal continues.

The mirror image of Adobe: at ~36x with EPS compounding high-single-digits
buyback-assisted, a held multiple returns ~9% a year and any drift toward
MSFT's 27x erases three years of it. Apple drops from seventh to eleventh
not because anything went wrong — it has been the defensive bid of the
AI-capex selloff — but because six passers now stand between it and the
top. It stays above Citigroup on the framework anchor alone; on expected
return the bank edges it, and the next entry says so.

### 12. Citigroup — the first return target hit early, and the Street cut targets anyway

$124.26 (−8.0% since the DB's 08-07 price; −6.3% since 08-27; −3.9% on
print day), $205.7B. The framework caveat first: QARP cannot screen banks,
its corporate rows are noise, and the price-gate pass (9.4% trailing EBIT
yield) rests on an EV construction that means little for a balance-sheet
business. On bank lenses: **Q3 2026 (printed 10-13)** was the best
third-quarter revenue in a decade — **$23.8B +8%**, EPS **$2.85** vs $2.65,
**ROTCE 11.5%** (YTD 12.6%), CET1 12.8%, TBVPS $101.59 +6%, **$3.0B of
buybacks** ($13.3B YTD) and a 12% dividend raise; FY26 ROTCE guided **above
11%** from the 10–11% target. Price: **1.22x tangible book, ~10.9x FY26 /
9.5x FY27** consensus EPS ($11.39 / $13.05), ~8% of market cap returned a
year. Growth is modest and mechanical: 2027–28 ROTCE 11–13% unchanged,
14–15% long-term with no date; Banamex is 49% sold with **deconsolidation
early 2027** (a ~$9B CTA loss through the P&L, capital-neutral) and an IPO
to follow; Q4 Markets guided −20% QoQ. Street: Buy, average target $151,
but RBC and Evercore cut to $135 after the print. The trade-off is unchanged
and still unique here: a low-teens annual return (earnings yield plus payout
plus book compounding) with the lowest narrative risk on the list, now with
the easy multiple expansion behind it. Next print 2027-01-14.

### 13. Grupo México — a modest multiple on peak-cycle copper, within 1% of its all-time high

MXN 244.40 (+18.5% since the DB's 07-24 price; +65% in a year; the 52-week
high is MXN 246.54), MXN 1.90T ≈ $103B. **Forward P/E: not computable** —
coverage is thin and no FY EPS consensus in MXN was found; trailing ≈17.3x
on reported quarterly USD EPS, ~15x on H1 2026 annualised.

- **Quality**: genuine. ROIC 10Y median 16%, worst year 9.0%, eight of ten
  years above 12%, operating margin never below 37% in seven years, cash
  conversion 1.8x, net cash. 14/14 — on a broken TTM window (the Q3 2025
  fragment leaves the TTM margins NA and the surviving criteria on FY2025
  figures; data notes).
- **Price**: passes on owner earnings (5.3%) and EBIT (9.8%) at the DB
  price; **re-priced to MXN 244.40 the EBIT arm still clears at 8.3%**, the
  other two fall to 4.5% and 4.1%. Dividend ~3%.
- **Growth** = the copper price. Q2 (07-21): sales **$5.7B +35%**, EBITDA
  **$3.5B +50%** at a 62% margin, cash cost $0.40/lb, copper output **−3.7%**
  (Peru, Asarco) against a maintained 1.034Mt target; Southern Copper (89%
  owned) raised its own guidance and has Tía María 42% built for 2H27. LME
  copper ~$14,400/t and COMEX $6.53/lb (+32% y/y) on Chilean strikes, a
  weaker dollar and AI/electrification demand.
- **Overhangs**: the analyst average target (MXN 236) is *below* the price;
  a 20% copper retracement takes earnings down disproportionately at a 62%
  margin; Peru under a 50,000-vote Fujimori mandate; El Arco hostage to the
  CFE grid; an August dividend cut to MXN 0.80 is reported but unverified.
  **Q3 prints ~10-27.**

Not a derating story — the opposite. The screen's "cheap EBIT yield" is
peak earnings at a modest multiple, which is how cyclical miners are meant
to trade at the top; the structural copper story is why the Street still
leans Buy with targets below the price. Ranked above the two HCM names
because the cash yield is real and the balance sheet is clean, below
Citigroup because the entire thesis is one commodity price.

### 14. Paylocity — a fairly-valued compounder; the screen's discount closed on the way here

$166.32 (+10.6% since the DB's 08-07 price; a 52-week high set on 10-16;
+79% off the April low), $8.8B. **~19.0x FY27 / 17.1x FY28** consensus
non-GAAP EPS ($8.74 / $9.72; **~34x GAAP** — ~$3.4/share of SBC and
amortisation add-backs, the widest gap of the three HCM/payments names).

- **Quality**: 14/14 with ROIC 10Y median 24%, ten of ten years above 12%,
  cash conversion ~1.9–2.0x, net cash, revenue retention >92% for a third
  year, clients +7%, ARPU +5.4%; FY26 (June) revenue $1.771B +11%, recurring
  +12.2%, adjusted EBITDA margin 37%.
- **Price**: passed on the FCF arm alone at the DB price (5.5% against
  5%); **re-priced to $166.32 it reads 4.95% — a fail by five basis
  points**, with EBIT at 4.3% and owner earnings at 2.6%. On today's price
  the framework reads 13/14, the MSFT/AAPL shape at a quarter of their
  multiple.
- **Growth**: stepping down. FY27 guided to **~+7% revenue / ~+8%
  recurring** (from 12%), ~2% light of consensus, with 120–140bp of the
  margin expansion an accounting-life change and two Fed cuts embedded;
  Airbase's contribution is still not quantified. $1.3B of buyback capacity
  (≈15% of market cap) barely used.
- **Overhangs**: AI is now a 10-K risk factor ("new AI-based solutions
  could offer lower prices"); the executive chairman and founder-director
  sold into the high on 10-15; BofA reinstated at Buy (09-17), UBS resumed
  at Buy $175 (09-29), Barclays Equal Weight $162. Street: Buy-leaning,
  average target $164–173 — at the price. **Q1 FY27 prints 11-05**, the
  first test of the promised "beat and raise".

The derating from 50x to 17x was a sector event (the "AI eats per-seat
software" trade) on top of a genuine growth step-down; the evidence so far
is cyclical (retention and client adds intact) while the risk is structural
in form. The screen caught it at $150; at $166 it is a fairly-priced quality
compounder with insiders selling, not a bargain.

### 15. Paycom — EPS growth manufactured by a levered buyback, at a 52-week high above every target

$251.64 (+17.1% since the DB's 08-07 price; a 52-week high on 10-16; +140%
off the February low), $11.1B. **~20.8x FY26 / 18.0x FY27** consensus
non-GAAP EPS ($12.10 / $13.96).

- **Quality**: 14/14 on stored data — ROIC 10Y median 40%, gross margin 80%,
  record 91% revenue retention, adjusted EBITDA margin 44%. Two caveats:
  the Q2 2026 cash-flow rows are **six-month cumulative values stored as a
  quarter**, so CFO/NI TTM 2.08 is really ~1.64 and the 29.6% shareholder
  yield ~18.5%; and the company that had no debt at 2025-12-31 now carries
  **$900M drawn on its revolver** (net debt ~$785M) to fund H1 buybacks of
  ~$1.4B that retired ~20% of the shares.
- **Price**: passed on the FCF arm alone at the DB price (7.2%, inflated);
  **re-priced to $251.64 the stored row reads 6.2% and the corrected one
  ~4.5–4.7% — a fail**; EBIT 5.4%, owner earnings 2.6%. On honest data at
  today's price the framework reads 13/14.
- **Growth**: Q2 (08-05) revenue $531M +9.8%, recurring +11%, FY26 guided
  **+7–8%** (raised); FY26 non-GAAP EPS +31% is mostly the 20% share-count
  cut. Float income −9% y/y with Fed cuts ahead (~$7.5M per 25bp). AI is a
  cost lever here (self-hosted models saving ~$100M of R&D) rather than,
  so far, a pricing lever.
- **Overhangs**: Street average target ($220–226) is **12% below the
  price**; BofA initiated at Underperform ($200) on 09-29; founder-CEO with
  ~12.6% and a failed co-CEO episode in 2024; short interest 6.9%. **Q3
  prints 11-04.**

The screen caught Paycom at $215 after the post-Q2 jump and on an inflated
cash-flow row; the stock then ran 17% to a level the sell side does not
underwrite. The bear case is structural in narrative (AI shrinking the
seats PEPM vendors bill against) but has not shown up in retention or
growth; the honest risk is the move from "cheap on fear" to "fairly priced
on buyback-boosted EPS" with a balance sheet it did not have a year ago.
Below Paylocity on balance-sheet honesty and the target gap.

### 16. Lululemon — ten times a cut year, with the Street modelling the next one lower still

$97.10 (−24.5% since the DB's 08-07 price; −18% on the 09-03 print; −42% in
a year, −54% YTD; a 52-week low on 10-08), $10.75B. **~10.1x FY-Jan27 /
11.2x FY-Jan28** consensus EPS ($9.66 / $8.65 — **falling**); trailing P/E
~8x.

- **Quality (trailing)**: the screen's dream — ROIC 10Y median 44%, worst
  year 31%, gross margin 56% inside a 2.3pp band, shares −2.5%/yr, $1.39B
  of cash. 14/14, every arm with room, and **re-priced to $97.10 the arms
  read 12.0% / 17.2% / 10.8%** — the widest pass on the list. The one NA
  (interest coverage) is a provider gap on lease debt; the latest stored
  quarter is a one-concept fragment, so the DB's view ends at the quarter to
  2026-04-30 — *before* the collapse.
- **Price**: cheap on every trailing measure; the question is what the
  trailing measures are worth.
- **Growth**: negative and accelerating down. **Q2 FY26 (printed 09-03):
  revenue −4%, comparable sales −9%, Americas comps −12%, women's leggings
  ~−20%**; GAAP EPS $2.92 flattered by $0.86 of one-off IEEPA tariff
  refunds (~$2.06 ex-refund); **FY26 guidance cut** to revenue $10.35–10.50B
  (−5% to −7%) and EPS $9.48–9.73 including the refund; **Q3 operating
  margin guided ~6.5% against 17% a year earlier**, EPS $0.93–0.98 vs $2.59.
  Consensus FY-Jan28 EPS has had 20 downward revisions in thirty days.
- **Overhangs**: new CEO **Heidi O'Neill** (started 09-08) replaced the
  terminated Calvin McDonald and overhauled the C-suite on 10-07 (new
  product and operations chiefs join 10-26); Elliott took a >$1B stake in
  December and was unhappy with the appointment; Chip Wilson settled his
  proxy fight in May; tariffs assumed at 20% for the rest of the year;
  China comps −8% cc on "brand sentiment, not macro". Street: Hold/Reduce,
  average target $105–107. **Q3 prints early-to-mid December** (unconfirmed).

Everything the screen reads is history that ended in April; everything that
matters now is a margin structure resetting from 17% to single digits and a
US franchise the market treats as ex-growth. A product-led recovery under
the new team cannot show in the numbers before the spring-2027 assortment —
the March or June 2027 prints. The classic value-trap shape: the cheapest
name on trailing arms, the only passer whose quality leg is visibly
breaking, and a show-me story rather than a mispriced compounder. Ranked
below the HCM names because their numbers are intact and this one's are
not.

### 17. Realtek — the thinnest pass, priced above its own Street

TWD 778 (+2.1% since the DB's 07-27 price; +6.6% since 08-27, ~+10%
including the TWD 25 dividend; −20% off the high), TWD 401B ≈ $12.4B.
**~24.3x FY26 / 20.2x FY27** consensus EPS (TWD 31.96 / 38.47 — the FY27
figure rests on one aggregator). Dividend yield 3.2%.

- **Quality**: the weakest of the ten passers — Piotroski 4, OPM 7Y
  minimum 7.0% against a 5% bar, gross margin down 320bp YoY to 47.0% in Q2
  as DRAM and packaging costs rose, EPS TWD 7.42 vs 8.65 expected. The
  balance sheet is the strength: net cash −3.0x EBITDA, coverage 88x, nine
  of ten ROIC years above 12%.
- **Price**: passes on the FCF arm by 15bp at the DB price and **by 3bp
  re-priced** (5.03%); the EBIT and owner-earnings arms (4.0%) never did.
  Fails the Graham screen on cheapness (P/B 8.6x, EV/EBIT 24.5x) and
  Piotroski.
- **Growth**: the top line is strong — **September revenue +23% YoY, Q3
  +28% YoY** (TWD 37.7B), YTD +16% — as across-the-board price increases to
  customers flow through; consensus needs ~TWD 16.1 of H2 EPS to hit FY26,
  i.e. the hikes must restore margin on flat-to-up revenue. PCs (33% of
  revenue) are guided down sequentially into a tougher Q4; Wi-Fi 7/8 and
  automotive Ethernet are the medium-term content story.
- **Overhangs**: the average target (TWD 730) is 6% *below* the price;
  Fubon (Neutral, TWD 610) and Morgan Stanley (Equal-weight, TWD 550) sit
  on the sidelines; Taiwan risk; 107 days of inventory. **Q3 investor
  conference 10-29** (MOPS confirmed).

A 24x multiple for an 11% EPS year is not what the screen means by
reasonable; it passes on a trailing FCF arm by a rounding error while the
Street's own targets say the stock is ahead of itself. Good balance sheet,
real top-line momentum, and the least margin of safety among the passers.

### 18. Palantir — the business keeps compounding; the price still asks for perfection

$186.03 (flat since 08-27 after a round trip to a $209 record close on
10-09; −7.2% on 10-16, the fifth straight loss), $447B. Q2 (08-03): revenue
$1.94B **+93%**, US commercial +149%, adjusted operating margin 62%, FY26
guided to $8.15B (+82%), GAAP-profitable every quarter — though at a ~1%
effective tax rate. Since then: Army TITAN production ($127M, 09-01) and
ammunition-management ($48M, 09-17) awards, a **Goldman upgrade to Buy
($230, 10-08)** and a **Barclays Street-high initiation ($265, 10-09)**,
then a Phillip Securities downgrade on the re-rating (10-12). The screen
still cannot reach it (5/14 — no ten-year history, OPM floor broken by the
SBC years, +6.3%/yr trailing dilution) and the price leg is unchanged in
kind: **~115x FY26 / 79x FY27** adjusted EPS ($1.62 / $2.35), ~54x and ~36x
sales, for +84% then +50% growth. Insiders sold ~$553M in twelve months
(Karp ~$86M on 08-20, Thiel's 10b5-1 in March). Street: Buy, average target
$206, just above the price. **Q3 prints 11-02** (guide $2.16B). Best-run
hypergrowth business on the list; still the worst corporate price; the name
to accumulate only when the AI tape turns indiscriminate — the five-session
−11% into 10-16 is the first such window since July.

### 19. AMD — the challenger's multiple doubled the incumbent's, then gave back a third of it

$528.75 (+10.9% since 08-27 but **−16.2% since 09-25**, a 32% rally to $658
unwound by a third with no company news), $863B. **~69.7x FY26 / 33.5x FY27**
consensus EPS ($7.59 / $15.76) — the group's highest multiple, for a
doubling of EPS next year. The story thickened: **OpenAI 6 GW, Meta 6 GW,
Anthropic up to 2 GW** (with AMD investing up to $5B in Anthropic — its own
circular financing), Oracle 50k MI450s, Helios racks "at the very beginning
of production shipment" in Q3 with volume "much more substantial" in Q4; Q2
(08-04) revenue $11.5B +50%, Data Center +107%, non-GAAP GM 56% against
NVIDIA's 75%. Targets were marked up to $700–960 in October (Citi $800, BNP
$960). The two overhangs are specific: a single-source report that the
Helios mass-production ramp slips to Q2 2027, and **customer warrants for up
to 320M shares (~20% of the count)** that vest precisely if the deals
succeed. 6/14 gates, 6.1%/yr trailing dilution. Street: Strong Buy, average
target $636. **Q3 prints 11-03** — a binary on Helios timing. You still pay
roughly 2.4x NVIDIA's FY27 multiple for share the incumbent says it cannot
supply anyway.

### 20. Intel — the 38% September melt-up has been entirely unwound

$89.85 (−2.4% since 08-27 after a round trip through $127 on 09-24; **−27%
since 09-25**, eleven down sessions in fourteen), ~$472B. **~58.7x FY26 /
42.8x FY27** consensus EPS ($1.53 / $2.10) on a street that is a **Hold**
(32 of 49 at Hold; average target $109–118). Nothing changed in the
fundamentals or the strategic cap table (US government 9.9% plus warrants,
NVIDIA $5B, SoftBank $2B all pre-date the window): Q2 (07-23) was a genuine
beat ($16.1B +25%, non-GAAP EPS $0.42 vs $0.21) with a $15.8–16.8B Q3
guide; Fortinet is the first named foundry customer under Lip-Bu Tan; 18A
yields are reportedly above target. The reversal had a specific trigger —
**Musk confirming Terafab is in talks with TSMC (from 10-05)**, undercutting
Intel's one named leading-edge foundry anchor — then the Kiryat Gat fire
(10-15), hot inflation and the sector rout. 3/14 gates, the worst capital
record in the set (iROIC −56%), a diluted share count +17% YoY and a
foundry losing $2.1B a quarter. **Q3 prints 10-29**; a named 18A/14A
customer or Terafab clarity would matter more than the quarter.

### 21. Cloudflare — the agentic-web trade in its purest form, and its sharpest reversal

$296.30 (−3.9% since 08-27 but **−15.1% since 09-25**: a $360.93 record
close on 10-09 then −17.9% in five sessions with no company news), $105.5B.
Q2 (08-06) was its best in years — revenue $696M **+36%, accelerating**,
large customers +27%, DBNR 120%, FY26 guided to ~$2.87B (+32%) — and the
product cadence since has been relentless: Pay Per Use and default
AI-crawler blocking (09-15), Agents Week (August), **46 Birthday Week
launches** (09-28 to 10-02) including Cloudflare OS and an x402 agent
payments gateway. None of it reaches the framework: still no GAAP-profitable
year (Q2 GAAP loss $170M on $151M of restructuring; SBC 20% of revenue),
2/14 gates, and a price of **~235x FY26 / 176x FY27 non-GAAP EPS** ($1.26 /
$1.68), ~37x sales, that prices the toll-booth option as won. Insiders sell
~$0.5B a year on autopilot. Street: Buy, average target $343; Oppenheimer
$430 and BTIG $413 raised into the print. **Q3 prints 11-05**, guided to
~+31% and a 17.6% non-GAAP operating margin. The growth is the point; the
price is the problem — a QARP framework has nothing to buy here at any gate.

### 22. Tesla — still failing all three legs, with estimates still falling

$354.72 (flat since 08-27; −4.7% since 09-25; −29% off the high), $1.40T.
**~204x FY26 / 163x FY27** consensus non-GAAP EPS ($1.74 / $2.17) — and the
FY26 figure has been cut from $2.13 to $1.74 in ninety days (4 up / 9 down
in the last thirty). Q3 deliveries (10-02): **486,532**, a 5% beat of the
company-compiled consensus but **−2% YoY** against the tax-credit
pull-forward quarter; **energy storage missed** (13.7 vs 15.9 GWh). US EV
sales are −39% YoY post-credit with Tesla's own US volume estimated −20%.
Q2's quality read stands: operating margin 1.4%, automotive GM ex-credits
16.3%, **FCF −$1.1B with capex >$25B and negative FCF guided for the rest of
2026**. The narrative stack keeps growing — seven robotaxi metros, public
Cybercab rides in Austin since 09-04, >1M unsupervised miles, 1.48M FSD
subscriptions, a 2028 Roadster shown 10-15 to a −2% reaction, and Barclays'
call that a **Tesla–SpaceX merger could be announced before the 12-06 AGM
deadline**, with the SEC now permitting retail auto-voting with the board.
5/14 gates, weakest margins in the set, the DB's +1.1%/yr share CAGR
failing even the loose bar after the April award. Street split: Buy on one
tally ($398), Hold on another ($412). **Q3 prints 10-21** — the nearest
dated event on the whole list. The ranking is arithmetic, not a verdict on
the robotaxi dream.

## Scorecard

| Rank | Name | QARP (2026-08-11 data; re-priced 10-16) | Price (fwd P/E at 10-16) | Fwd growth (consensus) | Verdict |
|---|---|---|---|---|---|
| 1 | ADBE | **14/14**, wider at $245 | ~10.0x FY26 / 8.9x FY27 | Rev +11.9% → +9.1% | Paid to hold while the AI-disruption debate resolves; new CEO 12-01; held position |
| 2 | GOOGL | 12/14 (price + gains artifact) | ~25.9x FY26 adj / 22.3x FY27 | Rev +24%; Cloud +82% | Most growth per multiple point; funding/FCF the bear case; 10-28 |
| 3 | MSFT | 13/14 (price gate only) | ~26.7x FY27 / 22.3x FY28 | Rev +18%; Azure ~45% | Highest certainty at a full price; 10-28 |
| 4 | NVDA | 12/14 (price + WC artifact) | ~23.0x FY27 / 13.5x FY28 | Rev +91% → +68% | Cheapest hypergrowth math; now a financier of its customers; 11-17 |
| 5 | AVGO | 8/14 (serial-acquirer decade) | ~28.2x FY26 / 16.9x FY27 | AI rev $58B → ~$115B secured | Cleanest print, −11%; customer-credit risk warehoused; 12-09 |
| 6 | NOVO-B | **14/14** (EBIT + OEY arms, wider) | ~11.9x FY26 / 11.7x FY27 | Sales 0% to −6% CER; EPS flat | Elite economics at a cliff multiple; CagriSema Q4; 11-04 |
| 7 | PYPL | **14/14**, all arms | ~10.5x FY26 / 9.8x FY27 | TM$ +1%; EPS +1% → +7% | Ex-growth already in the price; 12% buyback; share loss structural; 10-27 |
| 8 | MC | **14/14**, all arms (wider at €397) | ~18.0x 2026 / 16.6x 2027 | Rev −0.4% → +4.7%; F&LG −5% | Decade-low multiple; cyclical with a Vuitton/succession tail; 2027-01-25 |
| 9 | CRUS | **14/14**, all three arms at $108 | ~12.9x FY27 (EPS −9.5%) | Rev +1%; EPS −9.5% | Net cash a fifth of the cap; 80% Apple; 11-04 |
| 10 | 000660 | 8/14 (cyclicality gates) | **~3.6x FY26 / 2.7x FY27** | Rev +254% → +54% | Cheapest by far, −23%; CXMT scare vs intact cycle; 10-28 |
| 11 | AAPL | 13/14 (price gate only) | ~38.9x FY26 / 35.9x FY27 | Rev +10.4%; EPS +8.5% | Record high on a re-rating; targets below price; 11-02 |
| 12 | C | Structurally outside (bank) | ~10.9x FY26 / 9.5x FY27; 1.22x TB | EPS +15%; ROTCE >11% | Low-teens return, lowest narrative risk; targets cut post-print |
| 13 | GMEXICOB | **14/14** (EBIT arm still clears) | n/a (trailing ~17x) | Copper +32% y/y; output −3.7% | Peak-cycle earnings at a modest multiple; targets below price; ~10-27 |
| 14 | PCTY | 14/14 at DB; **13/14 re-priced** (−5bp) | ~19.0x FY27 / 17.1x FY28 (~34x GAAP) | Rev ~+7%; recurring ~+8% | Fairly-valued compounder; insiders selling into a high; 11-05 |
| 15 | PAYC | 14/14 at DB; **13/14 re-priced** (corrected row) | ~20.8x FY26 / 18.0x FY27 | Rev +7–8%; EPS +31% (buyback) | Levered buyback EPS at a high above every target; 11-04 |
| 16 | LULU | **14/14**, widest arms — on pre-collapse data | ~10.1x FY-Jan27 / 11.2x FY-Jan28 | Rev −5% to −7%; EPS falling | Value-trap shape: quality leg visibly breaking; show-me; Dec |
| 17 | 2379 | **14/14** by 3bp (FCF arm) | ~24.3x FY26 / 20.2x FY27 | Rev +19%; EPS +11% | Thinnest pass, priced above its Street; 10-29 |
| 18 | PLTR | 5/14 (no 10Y history) | ~115x FY26 / 79x FY27 | Rev +84% → +50% | Best-run hypergrowth; worst corporate price; 11-02 |
| 19 | AMD | 6/14 (weak trailing record) | ~70x FY26 / 33.5x FY27 | Rev +47% → +75% | Helios binary; ~20% warrant dilution if it works; 11-03 |
| 20 | INTC | 3/14 (worst capital record) | ~59x FY26 / 43x FY27 | Rev +19% → +15% | September rally fully unwound on Terafab–TSMC; 10-29 |
| 21 | NET | 2/14 (pre-profit) | ~235x FY26 / 176x FY27 (non-GAAP) | Rev +32% → +29% | Agentic-web option priced as won; −18% in five sessions; 11-05 |
| 22 | TSLA | 5/14 (weakest margins) | ~204x FY26 / 163x FY27, estimates falling | Rev +13%; EPS +5% | Fails quality, price and growth simultaneously; 10-21 |

## What the snapshot says overall

**Quality and price now coexist in eight names at today's prices — but
quality, price and growth still coexist in one.** The nine additions were
chosen from the screen's own passers, and the forward layer sorts them
quickly. Six are derated franchises whose cheapness survived the two months
since the DB's price date and in most cases widened: Novo Nordisk, PayPal,
LVMH, Cirrus Logic, Lululemon and (on one arm) Grupo México. Two are
fair-priced quality compounders whose cheapness closed before the names
reached the list — Paylocity and Paycom re-rated 79% and 140% off their
spring lows and now trade at or above every sell-side target, and neither
clears the price gate at Friday's close on honest numbers. One, Realtek,
passes by three basis points at 24x earnings. What the six cheap passers
have in common is that each has a *specific* reason to be cheap: a US
price reset and a 2032 cliff (Novo), wallet-share loss (PayPal), a
shrinking core division and a succession question (LVMH), one customer's
unit cycle (Cirrus), a collapsing margin structure (Lululemon), a
commodity at a record (Grupo México). The screen does not distinguish a
derated compounder from a value trap; the forward layer has to, and it
ranks Novo, PayPal, LVMH and Cirrus as the former and Lululemon as the
likeliest latter.

Adobe remains the only name where all three legs hold — 14/14, under 9x
next year, and still growing earnings at mid-teens — and the two months
made it cheaper and more controversial at once: the succession went to the
non-creative candidate, two brokers downgraded on the substitution thesis,
and the stock sits 6% below the author's entry. The platform pair are
exactly where they were in August: GOOGL flat on better numbers and a
removed break-up tail, MSFT 5% dearer on Azure re-acceleration, both at
22–27x next year — fair for the quality, not mispriced. The AI semis are
the group that moved: NVDA and AVGO are cheaper on every forward number
while their estimates rose, because the market now prices the customers'
balance sheets (and the vendors' guarantees of them) rather than the
demand. Hynix is a quarter cheaper than in August at 2.7x next year with
the cycle, on the evidence, not yet turned. Apple is the one name that
re-rated without re-earning, to a record high the sell side's targets sit
below. Everything in the bottom five still pays upfront for success not yet
in the numbers.

An allocator wanting one name still takes MSFT; one paid for tolerating
controversy holds ADBE (as the author does); the barbell — ADBE plus one
platform — still covers both failure modes. The new material offers a
second barbell the August list could not: a cheap-quality sleeve of Novo,
PayPal, LVMH and Cirrus, each passing the screen at today's price with a
named, bounded bear case and a 3–5% cash return while it plays out, as the
off-thesis counterweight to the AI names. Citigroup remains the bank-shaped
version of the same idea.

## Data notes for the repo (observations only, no action taken)

- **SK Hynix rows are doubly stale**: the DB price (₩1,816,000, 07-27)
  predates the post-Q2 crash — 43% above the 10-16 Seoul close — and the
  07-29 Q2 print (revenue ₩79.3T, OP ₩60.5T) is not in `financial_facts`
  (latest stored quarter Q1 2026). Its trailing table column overstates the
  price and understates TTM earnings simultaneously; re-priced, its EBIT arm
  passes comfortably while the cyclicality gates fail by design.
- **Non-US prices are a fortnight older than US ones**: the 2026-08-10
  market-data refresh reached the US venues only, so LVMH, Novo Nordisk and
  Realtek carry 07-27 prices and Grupo México 07-24 — and the USD
  conversions behind the Graham size gate used FX rates eight days stale on
  those dates (`report-screen-failures` warns on USD/EUR, USD/DKK and
  USD/TWD). None of this reaches a QARP gate; all four columns' price-linked
  rows are restated at 10-16 in the ranking.
- **NVDA's 08-26 Q2 FY27 print and AVGO's 09-02 Q3 FY26 print post-date the
  2026-08-10 fact refresh**; their TTM metrics lag one reported quarter.
- **Fragment quarters**: Cloudflare's Q2 2026 is stored with 2 concepts (vs
  ~39 for a full quarter), so its TTM metrics run through Q1 2026 despite
  the 08-06 print falling before the refresh; Lululemon's latest quarter row
  (end_date 2026-06-30) holds one concept (`CommonStockSharesOutstanding`),
  so its TTM metrics run through the quarter ending 2026-04-30 — which is
  also why its stored column shows none of the September collapse.
- **Grupo México's TTM window is broken by a three-concept Q3 2025**
  (`CapitalExpenditures` = 0 plus two EPS rows against 38–39 concepts in
  every neighbouring quarter), leaving twenty TTM metrics NA. Three QARP
  inputs survived because the TTM-window resolver falls back to the latest
  fiscal year for `gross_margin_ttm`, `cfo_to_ni_ttm` and the EBIT behind
  the EV ratios, while `operating_margin_ttm`, `roic_ttm`, `fcf_margin_ttm`,
  `interest_coverage` and the rest emit NA — an inconsistent fallback policy
  across the TTM family. Proposed (not applied): one documented policy, and
  a normalizer check that flags a quarter stored with a small fraction of
  its neighbours' concept count. Detail in the
  [watchlist profiles](../reference/watchlist.md#data-caveats-recorded-on-addition-observations-only-nothing-changed).
- **Paycom's Q2 2026 cash-flow rows are six-month cumulative values stored
  as a quarter**: dividends $17.7M → $35.5M (every other quarter ~$21M),
  operating cash flow $212.5M → $427.6M, buybacks −$1,054M → −$1,390M
  (Q1 plus the ~$335M the 10-Q reports for the quarter), while the same
  quarter's income-statement rows are properly quarterly. Every TTM
  cash-flow metric double-counts Q1: CFO/NI TTM 2.08 (~1.64 corrected), FCF
  yield 7.20% (~5.2–5.5%), shareholder yield 29.6% (~18.5%). No stored gate
  flips; the Graham cheapness pass does not survive the correction, and
  neither does the re-priced QARP FCF arm. A universe scan finds 167
  listings whose Q2 2026 dividend is within 3% of exactly twice Q1 and 17
  where operating cash flow doubles with it — the failure is vendor-wide,
  not Paycom's. Proposed (not applied): a normalizer de-cumulation check on
  quarterly cash-flow rows.
- **Phantom mid-year "FY" rows**: LVMH and Grupo México each carry a
  `fiscal_period = 'FY'` row dated 2026-06-30 holding only the two EPS
  concepts — an interim EPS the vendor tagged as yearly. No gate reads them.
- **LVMH's fundamentals end at FY2025** (semi-annual reporter; the 2026
  half-year is not stored as a quarter), so its trailing column is a
  December-2025 business at a July-2026 price. **Novo Nordisk's Q2 2026 is
  missing** although it was published in the first week of August, before
  the 08-10 refresh — vendor lag.
- **Stale metric rows exist and must be filtered**: a `metrics` row whose
  latest `metric_compute_status` attempt is `failure` is a leftover from an
  earlier run — the screening path ignores it, and ad-hoc SQL must join
  `metric_compute_status` on `status = 'success'` to agree with the screen.
  Among watchlist rows: NET's debt ratios (a 2025-12-31 vintage, including
  the `net_debt_to_ebitda` −6.49 its watchlist profile still quotes in
  prose) and PLTR's `roic_7y_median` 9.6%. Both are shown as NA above.
- **Share-count vintages**: implied DB share counts are within ~1% of
  reported counts for most names; PLTR runs ~4% low and NET ~10% low
  (heavy-SBC issuance outrunning the vendor's vintage); Paycom's stored
  45.9M against 44.3M at quarter-end and ~45.1M on the 10-Q cover reflects
  the buyback's pace, not a calc bug.
- **Correction to the superseded July memo**: MSFT's "FY27 capex guided
  ~$255–260B" was a street preview circulating as guidance; the company
  gave only a qualitative "grows year-over-year" plus Q1 FY27 capex >$50B.
  The figure is still circulating in October and is still not guidance.
- **Apple's net debt (+0.13x EBITDA) is a cash-scope reading, not a
  balance-sheet fact**: pyvalue's cash chain stops at current marketable
  securities ($62.4B) and no `LongTermInvestments` concept is stored, so the
  $84.1B of non-current securities are invisible to EV and net debt. On the
  company's own basis Apple is $62.2B net cash; EV is 1.8% overstated and
  every price arm ~2% too low. No gate moves.
- **Apple's iROIC 5Y (533%) is a vendor-consistency artifact**: EODHD's
  FY2025 `TotalDebtFromBalanceSheet` folds ~$10.9B of lease liabilities into
  the rollup while its FY2020 rollup carries none, so the broader-reading
  rule sees +$10.4B of invested capital over five years where a
  lease-consistent basis sees −$3.3B (which would emit the 1.0 cap). Same
  root as `roic_7y_median` reading 64.6% against 66.7% on SEC data.
- **Apple's vendor EPS and interest rows**: EODHD's Q3 FY26 EPS is $1.91
  against a filed diluted $2.02 (`eps_ttm` $8.61 vs $8.71); `InterestExpense`
  carries placeholder rows because Apple stopped disclosing the line after
  FY2023, and `interest_coverage` correctly emits the 100x cap on the debt
  ≤ 1x EBIT evidence. Neither reaches a gate. Apple's QARP inputs reconcile
  to SEC filings to the decimal (table in its watchlist profile).
- **Interest-coverage NAs on the new names are guards, not defects**:
  Lululemon's ("material debt without measurable interest expense" — lease
  debt with no vendor interest line) and Grupo México's (the broken TTM
  window) both leave the debt-service gate to pass on its net-debt arm;
  Paylocity's and Apple's 100x readings are the documented cap on the
  debt ≤ 1x EBIT evidence.
- **PayPal's iROIC 5Y is the 1.00 cap** (invested capital shrank on
  buybacks), as is Adobe's; `price_to_ncav` is structurally NA for LVMH,
  Novo Nordisk, PayPal, Paycom and Apple (negative NCAV by construction).
- Citigroup's corporate-shaped metrics (gross margin, CFO/NI, FCF, EV
  multiples) compute but are not economically meaningful for a bank; its
  QARP price-gate pass should not be read as a screen endorsement.

## Method appendix

**DB layer**: stored metrics only — the 2026-08-11 full-universe run (the
first on the rebuilt debt basis, commits `d5d8b34`…`053dcda`), read with the
`metric_compute_status` success filter; no recompute and no API calls were
needed because nothing has been ingested since. Gates evaluated exactly as
`screeners/quality_reasonable_price_primary.yml` defines them (missing
metric → NA; an `any_of` with no passing arm fails). Cross-checked against
`pyvalue report-screen-failures` on the same twenty-two symbols: the 10/22
full-pass count and all fourteen per-criterion fallout counts reconcile.
Apple's inputs were additionally recomputed from its SEC XBRL company facts
at the DB cut-off and reconcile to the decimal (2026-09-26). The nine new
names' inputs were read with `pyvalue explain-metric` (write-free live
recompute agreeing with the persisted values) to obtain the debt and cash
legs behind each EV.

**Re-pricing**: for the ten passers the stored price-gate arms were restated
at the 10-16 close as yield × EV₀ / EV₁, where EV₁ = EV₀ + market cap₀ ×
(price₁/price₀ − 1) — i.e. enterprise value moved only by the change in
market cap, with the stored quarter's debt and cash legs (the metrics
layer's own `TotalDebtFromBalanceSheet` and `CashAndShortTermInvestments`
reads) held constant. It is an approximation that ignores any balance-sheet
movement since the stored quarter; it is exact in the quantity the question
turns on, which is the price.

**Web layer**: six parallel sweeps run 2026-10-18 — platforms
(MSFT/GOOGL/ADBE/AAPL), AI semis (NVDA/AMD/AVGO/INTC), Asia semis and the
Apple supplier (000660/2379/CRUS), high-multiple growth (PLTR/NET/TSLA),
payments and HCM software (PYPL/PAYC/PCTY), and consumer/pharma/mining/bank
(LULU/MC/NOVO-B/GMEXICOB/C) — each cross-checking the 10-16 close against at
least two independent sources, pulling latest-quarter actuals, consensus
forward estimates (stockanalysis.com / S&P Global via Yahoo Finance,
MarketScreener, TipRanks, MarketBeat, Finviz), catalysts, risks and street
views with per-fact source dates, and stating whether the next report had
printed. Notable primary sources: company releases and filings (Adobe Q3
FY26 release, 10-Q and the succession 8-Ks; Broadcom Q3 FY26 8-K and call
transcript; NVIDIA Q2 FY27 release and 10-Q; Microsoft and Apple IR event
pages; LVMH Q3 revenue and H1 releases; Novo Nordisk Q2 6-K and CMD deck;
PayPal, Paycom, Paylocity and Cirrus Logic 8-Ks and 10-Qs/10-K; Lululemon
Q2 8-K and call; Southern Copper Q2 8-K; Citi Q3 8-K and transcript; SK
hynix 6-Ks and newsroom; Realtek's MOPS notice and monthly revenue table;
Tesla's Q3 deliveries 8-K), Bloomberg, Reuters, CNBC, Seoul Economic Daily
and TrendForce (memory), Digitimes (Taiwan), El Financiero/Expansión
(Mexico). Every sweep's web-search budget ran out before it finished; the
remaining gaps were filled by direct fetches of primary pages, and anything
unverifiable is marked "not found" in the sweep archives rather than
estimated. Facts resting on a single source are flagged there and were
either corroborated or hedged in the prose above (Grupo México's FY
consensus, Cirrus's FY28 consensus, LVMH's H1 EPS and the Hynix buyback's
interim progress were not found). Consensus figures move intraday; treat
every multiple in this document as ±5% within its own day.
