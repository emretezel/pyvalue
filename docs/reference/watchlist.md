# Anchor Watchlist

The anchor watchlist is the fixed set of stocks the author cares about when tuning
screeners: every criteria metric of `screeners/deep_value_graham.yml` and
`screeners/quality_reasonable_price_primary.yml` should be computable for these
listings, and any NA here is investigated rather than accepted (see
[Screener NA Investigation](../research/screener-na-investigation.md)).

An NA is a **defect** when the inputs exist and pyvalue fails to use them. It is
**structural** when the guard is right and the number is genuinely undefined — as
with Cloudflare, whose unbroken loss history makes six criteria metrics
undefinable by construction (see its profile below). Root-cause every NA; fix
only the first kind.

This file is the canonical record of the watchlist; update it here (and only here)
when the set changes.

## Tracked stocks (22)

| Canonical symbol | listing_id | Issuer | Exchange | Currency |
|---|---|---|---|---|
| MSFT.US | 65235 | Microsoft Corporation | US | USD |
| GOOGL.US | 61081 | Alphabet Inc | US | USD |
| ADBE.US | 53407 | Adobe Systems Incorporated | US | USD |
| NVDA.US | 66246 | NVIDIA Corporation | US | USD |
| AMD.US | 54072 | Advanced Micro Devices Inc | US | USD |
| C.US | 56282 | Citigroup Inc. | US | USD |
| PLTR.US | 67393 | Palantir Technologies Inc. | US | USD |
| TSLA.US | 71610 | Tesla Inc | US | USD |
| INTC.US | 62587 | Intel Corporation | US | USD |
| 000660.KO | 24625 | SK Hynix Inc | KO (KOSPI) | KRW |
| NET.US | 65665 | Cloudflare Inc | US | USD |
| AVGO.US | 54870 | Broadcom Inc | US | USD |
| AAPL.US | 53177 | Apple Inc. | US | USD |
| LULU.US | 64219 | Lululemon Athletica Inc. | US | USD |
| MC.PA | 36007 | LVMH Moët Hennessy - Louis Vuitton | PA (Euronext Paris) | EUR |
| NOVO-B.CO | 6522 | Novo Nordisk A/S | CO (Nasdaq Copenhagen) | DKK |
| PYPL.US | 67995 | PayPal Holdings Inc | US | USD |
| PAYC.US | 66943 | Paycom Software, Inc. | US | USD |
| CRUS.US | 57696 | Cirrus Logic Inc | US | USD |
| 2379.TW | 51259 | Realtek Semiconductor Corp | TW (TWSE) | TWD |
| GMEXICOB.MX | 34879 | Grupo México S.A.B. de C.V | MX (BMV) | MXN |
| PCTY.US | 67065 | Paylocity Holding Corporation | US | USD |

`listing_id` values are recorded so ad-hoc read-only SQL can target these rows
without symbol joins.

### Recomputed 2026-08-11

All eleven names then on the watchlist were recomputed in the full-universe
run of 2026-08-11, the first on the rebuilt debt basis (`d5d8b34`, `d2e600c`,
`c24aad2`, then `71b259d`, `ce92a3f`); Broadcom, Apple and the nine names
added on 2026-10-18 were all in the same run's universe and carry rows of the
same vintage. Every ROIC
figure on the watchlist moved, because the ROIC FY series had kept a private
copy of the retired components-preferred debt rule until `ce92a3f`.
**Microsoft is now net debt +0.27x EBITDA rather than net cash** — the old
figure omitted $78.8B of finance leases.

Of the thirteen names on the watchlist before 2026-10-18, `ADBE.US` is the
**only** QARP pass, at 14 of 14 gates. `MSFT.US` and `AAPL.US` are 13/14 on
the price gate alone; `GOOGL.US` slipped to 12/14 when Q2 2026's one-time
investment gains pushed CFO/NI TTM to 0.76. The nine names added on
2026-10-18 **all pass 14/14 on the same run**, so the watchlist now holds ten
passers in twenty-two (see
[Nine names joined 2026-10-18](#nine-names-joined-2026-10-18)). Gate-by-gate
detail for all twenty-two names is in the
[Watchlist Buy Ranking](../research/watchlist-buy-ranking.md), which is
maintained as a latest snapshot.

Cloudflare (`NET.US`) joined the watchlist on 2026-08-07. It was already in the
catalog as a `primary` US listing, so the addition was documentation only — no
catalog row was created. It is the only watchlist name that has **never** posted
a profitable GAAP fiscal year; see
[Cloudflare's QARP profile](#cloudflare-netus-qarp-profile-refreshed-2026-08-11) below.

### Broadcom (AVGO.US) joined 2026-08-26

Broadcom (`AVGO.US`) is the twelfth name — the custom-AI-silicon and
AI-networking player the watchlist previously covered only through merchant
GPUs (NVDA, AMD) and memory (SK Hynix). Like Cloudflare's, the addition was
documentation only: the catalog already carried it as a `primary` US listing,
and its metrics were already current from the 2026-08-11 full-universe run, so
no compute was needed. It **fails QARP, 6 gates of 14, with all fourteen
criteria computable — zero NAs**; the failures are purchase-accounting scar
tissue from a decade of serial acquisition rather than operating weakness. See
[Broadcom's QARP profile](#broadcom-avgous-qarp-profile-2026-08-26) below, and
the [Watchlist Buy Ranking](../research/watchlist-buy-ranking.md) for the
forward story and where it slots among the watchlist names.

### Apple (AAPL.US) joined 2026-09-26

Apple (`AAPL.US`, listing 53177) is the thirteenth name. As with Cloudflare
and Broadcom the addition was documentation only: the catalog already carried
it as a `primary` US listing and its metrics were current from the 2026-08-11
full-universe run, so nothing was ingested or recomputed. It **fails QARP, 13
gates of 14, on the price gate alone, with all fourteen criteria computable —
zero NAs** — the same shape as Microsoft, and every input was independently
recomputed from Apple's SEC filings at the DB's own cut-off and reconciled.
See [Apple's QARP profile](#apple-aaplus-qarp-profile-2026-09-26) below, and
the [Watchlist Buy Ranking](../research/watchlist-buy-ranking.md) for the
forward story and its rank.

### Nine names joined 2026-10-18

Lululemon (`LULU.US`), LVMH (`MC.PA`), Novo Nordisk (`NOVO-B.CO`), PayPal
(`PYPL.US`), Paycom (`PAYC.US`), Cirrus Logic (`CRUS.US`), Realtek
(`2379.TW`), Grupo México (`GMEXICOB.MX`) and Paylocity (`PCTY.US`) are names
fourteen to twenty-two. All nine were among the recognizable passers of the
[2026-08-11 QARP re-run](../research/qarp-recognizable-names-2026-08-11.md),
and the addition was, once more, documentation only: each was already a
`primary` listing in the catalog with metrics of the 2026-08-11 vintage, so
nothing was ingested, normalized or recomputed. For the four non-US companies
the **home-market listing** is tracked — Paris, Copenhagen, Taipei and Mexico
City — on the SK Hynix precedent, not the US ADR/OTC lines, which are
recorded as known-not-tracked
[cross-listings](#cross-listings-known-not-tracked).

All nine **pass QARP 14 of 14 with every criterion computable — zero NAs on
the QARP set** — which turns the watchlist from one passer in thirteen into
ten in twenty-two. Six of the nine also pass the deep-value Graham screen;
LVMH, Paylocity and Realtek fail its cheapness gate (Realtek the Piotroski
floor too). Gate values, the Graham detail and the per-name data caveats are
in [their QARP profiles](#the-nine-2026-10-18-additions-qarp-profiles) below;
the forward story and ranks are in the
[Watchlist Buy Ranking](../research/watchlist-buy-ranking.md).

## Paste-ready scope line

All pyvalue commands share one scope resolver, so the watchlist can be passed
verbatim to any of them:

```
--symbols MSFT.US GOOGL.US ADBE.US NVDA.US AMD.US C.US PLTR.US TSLA.US INTC.US 000660.KO NET.US AVGO.US AAPL.US LULU.US MC.PA NOVO-B.CO PYPL.US PAYC.US CRUS.US 2379.TW GMEXICOB.MX PCTY.US
```

## Cloudflare (NET.US) QARP profile, refreshed 2026-08-11

First recorded on addition (2026-08-07) from a full pipeline run; **refreshed
here against the 2026-08-11 universe recompute**, which followed the debt-basis
rebuild of 2026-08-10/11 (see
[Recomputed 2026-08-11](#recomputed-2026-08-11) below). Price $300.27 as of
2026-08-07; market cap $96.4B.

Cloudflare **fails QARP**, 2 gates of 14:

| Gate | Result | Value |
|---|---|---|
| Gross margin (TTM) ≥ 35% | **pass** | 73.3% |
| GM stability (10Y std) ≤ 7pp | **pass** | 1.9pp |
| ROIC years >12% ≥ 7/10 | NA | non-positive average invested capital |
| ROIC 10Y min ≥ 7% | NA | same guard |
| OPM 7Y min ≥ 5% | FAIL | −37.6% |
| CFO/NI TTM ≥ 0.90 | NA | non-positive TTM net income |
| CFO/NI 10Y median ≥ 0.90 | NA | too few positive-NI FY years |
| Net debt/EBITDA ≤ 2.5x | NA | non-positive EBITDA |
| Debt service (6x cov or ≤1.5x) | FAIL | both arms NA (non-positive EBIT/EBITDA) |
| No NI loss years (10Y) | FAIL | 10 of 10 |
| Share CAGR 5Y ≤ +1% | FAIL | +2.9% (10Y: +16.6%) |
| Price gate (OEY/EBIT/FCF yield) | FAIL | −0.14% / −0.23% / +0.37% |
| iROIC 5Y ≥ 12% | FAIL | −2.6% |
| Owner-earnings CAGR 10Y ≥ 7% | NA | too few consecutive FY owner-earnings points |

The NAs are **not defects**. Cloudflare has posted a GAAP loss in all ten stored
fiscal years (FY2016 −$17.3M through FY2025 −$102.3M) and negative operating
income throughout, so the profitability-based guards correctly refuse to produce
a number. QARP is built on 10-year ROIC persistence, margin floors, no-loss-years
and owner-earnings compounding; a company that has never earned a GAAP profit is
structurally outside what the screen selects for. The two passing gates are real
and notable — 73% gross margin with 1.9pp of ten-year variation is elite pricing
power. The balance sheet is also sound (`altman_z` 12.9, `current_ratio` 1.96,
net cash: `net_debt_to_ebitda` −6.49).

### Known vendor caveat: non-GAAP EPS on a GAAP net income

EODHD's `EarningsPerShare` series for this listing is **non-GAAP** while
`NetIncomeLoss` is **GAAP**. FY2025 shows EPS +0.92 against net income
−$102.3M; the gap is essentially stock-based compensation (`sbc_to_revenue`
20.2%, `sbc_to_fcf` 132%). Consequences:

- `eps_ttm` (+0.67) and `earnings_yield` (0.24%) are non-GAAP-derived and
  disagree in sign with every NI-derived metric (`roe_ttm` −5.88%,
  `roa_ttm` −1.75%)
- **no QARP criterion reads any of the three**, so the verdict above is
  unaffected

Not fixed at the time of addition — recorded here because the contradiction is
visible in the stored metrics and will otherwise be re-discovered. It is likely
universe-wide for heavy-SBC issuers rather than specific to Cloudflare.

## Broadcom (AVGO.US) QARP profile (2026-08-26)

Recorded on addition. Source data is the stored **2026-08-11 full-universe
metric run** (see [Recomputed 2026-08-11](#recomputed-2026-08-11)) — `AVGO.US`
was in that run's universe before it joined the watchlist, so the rows below
required no fresh compute. Fundamentals run through Q2 FY2026 (Broadcom's
quarter ended 2026-05-03, stored under end_date 2026-04-30); price $427.76 and
market cap $2.035T as of 2026-08-07. By the 2026-08-26 addition date the
shares had fallen ~17% to $356.74 (08-25 close) in the August AI repricing, so
the price-linked rows below read correspondingly dearer than a same-day
compute would.

Broadcom **fails QARP, 6 gates of 14**:

| Gate | Result | Value |
|---|---|---|
| ROIC years >12% ≥ 7/10 | FAIL | 4 of 10 |
| ROIC 10Y min ≥ 7% | FAIL | −1.3% (FY2016) |
| Gross margin (TTM) ≥ 35% | **pass** | 67.0% |
| GM stability (10Y std) ≤ 7pp | FAIL | 8.0pp — a secular climb, not cyclicality |
| OPM 7Y min ≥ 5% | **pass** | 15.2% |
| CFO/NI TTM ≥ 0.90 | **pass** | 1.15 |
| CFO/NI 10Y median ≥ 0.90 | **pass** | 2.04 |
| Net debt/EBITDA ≤ 2.5x | **pass** | 1.09x |
| Debt service (6x cov or ≤1.5x) | **pass** | both arms: coverage 11.0x, net debt ≤ 1.5x |
| No NI loss years (10Y) | FAIL | 1 — FY2016, −$1.74B of Avago–Broadcom merger charges |
| Share CAGR 5Y ≤ +1% | FAIL | +2.5%/yr (VMware part-stock consideration + SBC; 10Y +1.6%/yr) |
| Price gate (OEY/EBIT/FCF yield) | FAIL | 0.86% / 1.58% / 1.57% — all three arms, each ~4x short |
| iROIC 5Y ≥ 12% | **pass** | 18.3% |
| Owner-earnings CAGR 10Y ≥ 7% | **pass** | 23.9% |

Unlike Cloudflare's profile above, **every criterion is computable — zero NAs
on the QARP set.** The six failures share one mechanism: a decade of serial
acquisition (Broadcom Corp 2016 ~$37B, CA 2018 ~$19B, Symantec Enterprise 2019
~$10.7B, VMware 2023 $69B) puts purchase accounting, not operating weakness,
in front of the gates. The FY2016 loss is the Avago–Broadcom merger-charge
year; the sub-bar ROIC years measure NOPAT against a goodwill-swollen ~$130B
average invested capital; the 8.0pp GM "instability" is a monotonic climb from
44.9% (FY2016) to 67.8% (FY2025) as the mix shifted toward software and deal
amortisation rolled off — a failure mode the stability gate cannot distinguish
from cyclicality. The cash-economics gates pass, several emphatically:
10Y-median cash conversion of 2.04 (amortisation makes GAAP net income
understate cash roughly two-to-one), owner-earnings CAGR 23.9%, and net debt
already down to 1.09x EBITDA thirty months after the $69B half-cash VMware
close. ROIC TTM is 24.3% — the acquired capital is now earning through — but
QARP's decade-persistence design reads the integration years as history, which
is what it is built to do. The price gate fails on current economics, not
history: EV/EBIT 63x trailing, FCF yield on EV 1.6%.

Structural NAs exist only outside the QARP criteria, on the tangible-book
family the deep-value screen reads: `graham_multiplier`,
`price_to_tangible_book`, `price_to_ncav`, and `roetce_ttm` all refuse to
compute because goodwill plus acquired intangibles ($97.8B + $28.3B at Q2
FY2026) exceed the $87.7B book equity — tangible book is genuinely ≈ −$38B and
NCAV −$49.3B. Correct guards, not defects: a serial acquirer sits structurally
outside the Graham screen's asset-backed lens, the same way Cloudflare's loss
history sits structurally outside QARP.

Vendor caveat, same pattern as
[Cloudflare's](#known-vendor-caveat-non-gaap-eps-on-a-gaap-net-income):
`eps_ttm` (8.13) is EODHD's adjusted series, not GAAP — the stored GAAP
quarterly `NetIncomeLoss` sums to $29.3B TTM, ≈ $6.0 diluted — so
`earnings_yield` (1.9%) is flattered by amortisation and SBC add-backs. No
QARP criterion reads either; recorded to pre-empt rediscovery.

## Apple (AAPL.US) QARP profile (2026-09-26)

Recorded on addition. Source data is the stored **2026-08-11 full-universe
metric run** (see [Recomputed 2026-08-11](#recomputed-2026-08-11)) — `AAPL.US`
(listing 53177) was in that run's universe before it joined the watchlist, so
the rows below required no fresh compute. Fundamentals run through Q3 FY2026
(Apple's quarter ended 2026-06-27, stored under end_date 2026-06-30; facts
refreshed 2026-08-10); price $313.33 and market cap $4.573T as of 2026-08-07.
By the 2026-09-26 addition date the shares had risen ~9% to a record $341.07
(09-25 close), so the price-linked rows below read correspondingly *cheaper*
than a same-day compute would.

Apple **fails QARP, 13 gates of 14** — the price gate alone:

| Gate | Result | Value |
|---|---|---|
| ROIC years >12% ≥ 7/10 | **pass** | 10 of 10 |
| ROIC 10Y min ≥ 7% | **pass** | 21.8% (FY2017) |
| Gross margin (TTM) ≥ 35% | **pass** | 48.7% |
| GM stability (10Y std) ≤ 7pp | **pass** | 3.3pp — a monotonic climb from 39% to 47% |
| OPM 7Y min ≥ 5% | **pass** | 24.1% (FY2020) |
| CFO/NI TTM ≥ 0.90 | **pass** | 1.14 |
| CFO/NI 10Y median ≥ 0.90 | **pass** | 1.26 (never below 0.99 in the ten years; FY2025 the tightest at 0.995) |
| Net debt/EBITDA ≤ 2.5x | **pass** | 0.13x (see the cash-scope note below) |
| Debt service (6x cov or ≤1.5x) | **pass** | both arms: coverage 100x (documented cap), net debt 0.13x |
| No NI loss years (10Y) | **pass** | 0 |
| Share CAGR 5Y ≤ +1% | **pass** | −2.5%/yr (10Y −3.9%/yr) |
| Price gate (OEY/EBIT/FCF yield) | FAIL | 2.13% / 3.37% / 2.97% — all three arms, each roughly half the bar |
| iROIC 5Y ≥ 12% | **pass** | 5.33 (533%; see the rollup note below) |
| Owner-earnings CAGR 10Y ≥ 7% | **pass** | 11.1% |

**Every criterion is computable — zero NAs on the QARP set**, the same clean
record as Broadcom's. The single failure is price, on current economics:
EV/EBIT 29.7x trailing, FCF yield on EV 3.0%, and a normalised owner-earnings
yield of 2.1% — the Microsoft-shaped verdict (see the
[MSFT QARP Review](../research/msft-qarp-review-2026-08.md)). The
owner-earnings arm is structurally the lowest for Apple because the five-year
*median* FY owner earnings ($98B) trail the current run-rate; the plain EBIT
and FCF arms fail on their own anyway. Everything else is emphatic: ten of ten
ROIC years above 12% with the worst year (FY2017) at 21.8%, gross margin held
inside a 3.3pp band across a decade in which it climbed nine points, and
operating cash flow never below 0.99x net income in any of the last ten
fiscal years (median 1.26x).

Structural NA outside the QARP criteria: `price_to_ncav` refuses to compute
because NCAV is negative — Apple runs current assets ($149.8B at Q3 FY2026)
almost exactly against current liabilities ($149.3B) by design, so there is no
net-current-asset value to price. Correct guard, not a defect: the Graham
screen's net-net lens does not apply to a franchise business.

### Independent verification (SEC filings, 2026-09-26)

Every QARP input was recomputed from Apple's XBRL company facts on sec.gov
(10-K FY2013–FY2025, 10-Qs through the quarter ended 2026-06-27) at the DB's
own cut-off — the 2026-08-07 close of $313.33, confirmed against Nasdaq's
price history — using pyvalue's definitions: NOPAT at the period's effective
tax rate, invested capital = debt + equity − cash, maintenance capex =
min(capex, 1.1 × D&A), maintenance NWC = the three-year average delta floored
at zero, and three-year-average CAGR endpoints.

| Metric | Stored | SEC recompute |
|---|---|---|
| ROIC years >12% / 10Y min / 10Y median | 10 / 21.8% / 51.8% | 10 / 21.8% / 51.9% |
| ROIC 7Y median | 64.6% | 66.7% |
| GM 10Y std / OPM 7Y min | 3.33pp / 24.1% | 3.33pp / 24.1% |
| CFO/NI TTM / 10Y median | 1.138 / 1.259 | 1.138 / 1.259 |
| Net debt/EBITDA | 0.130x | 0.131x |
| Share count CAGR 5Y | −2.55% | −2.59% |
| EBIT / FCF / OE yield on EV | 3.370% / 2.975% / 2.134% | 3.370% / 2.975% / 2.133% |
| Owner-earnings CAGR 10Y | 11.12% | 11.12% |
| ROIC TTM / OPM TTM / FCF margin TTM | 90.3% / 33.2% / 29.3% | 90.3% / 33.2% / 29.3% |
| Revenue CAGR 10Y / EV/EBIT | 5.94% / 29.67 | 5.94% / 29.67 |

The gate verdicts are identical on either basis. Three conventions surfaced,
recorded so they are not re-discovered; none is a defect and none moves a
gate:

- **Net cash is a scope choice.** pyvalue's cash leg is cash plus *current*
  marketable securities ($62.4B at 2026-06-27); Apple's $84.1B of
  *non-current* marketable securities sit outside the chain (no
  `LongTermInvestments` concept is stored for the listing). pyvalue therefore
  reads net debt of +$21.9B where Apple's own net-cash figure is +$62.2B. EV
  is 1.8% higher than on the company basis; the EBIT yield would be 3.43%
  there — still a fail.
- **The 533% incremental ROIC rests on a vendor rollup inconsistency.**
  EODHD's FY2025 `TotalDebtFromBalanceSheet` ($112.4B) folds ~$10.9B of lease
  liabilities into the rollup, while its FY2020 rollup ($112.4B) equals the
  borrowed components with no lease component. The broader-reading debt rule
  therefore measures a +$10.4B five-year change in invested capital where a
  lease-consistent basis shows −$3.3B; on that basis NOPAT grew $55.6B on
  shrinking capital and the metric would emit the documented 1.0 cap. Pass
  either way; the same effect puts `roic_7y_median` two points below the SEC
  recompute.
- **Vendor EPS drifts from the filing.** EODHD's Q3 FY2026 EPS is $1.91
  against a filed diluted $2.02, so `eps_ttm` reads $8.61 versus $8.71 (P/E
  36.4x versus 36.0x at the DB price). No QARP criterion reads it.
  Relatedly, `InterestExpense` carries placeholder rows (FY2024 = 0,
  Q2-2025 = $1M): Apple stopped disclosing interest expense separately after
  FY2023, and `interest_coverage` correctly falls to the documented 100x cap
  on the no-material-debt evidence (debt 0.54x TTM EBIT). The coverage arm is
  redundant at 0.13x net debt/EBITDA in any case.

## The nine 2026-10-18 additions: QARP profiles

Recorded on addition. Source data is the stored **2026-08-11 full-universe
metric run** (see [Recomputed 2026-08-11](#recomputed-2026-08-11)); all nine
were in that run's universe, so no row below required fresh compute. Prices
are the 2026-08-07 close for the five US listings, 2026-07-27 for Paris,
Copenhagen and Taipei and 2026-07-24 for Mexico City — the DB's last market
data refresh (2026-08-10) reached the US venues only. Every value is read
with the `metric_compute_status` success filter. Market caps at those prices:
Lululemon $14.8B, LVMH €230.2B, Novo Nordisk DKK 1,452.5B, PayPal $50.5B,
Paycom $9.7B, Cirrus Logic $6.3B, Realtek TWD 390.8B, Grupo México
MXN 1,613.7B, Paylocity $8.3B.

The `any_of` gates (debt service, price) are shown arm by arm; a **bold**
price arm is one that clears its bar.

| Gate | LULU | MC | NOVO-B | PYPL | PAYC | CRUS | 2379 | GMEXICOB | PCTY |
|---|---|---|---|---|---|---|---|---|---|
| ROIC years >12% ≥ 7/10 | 10 | 9 | 10 | 8 | 10 | 9 | 9 | 8 | 10 |
| ROIC 10Y min ≥ 7% | 31.4% | 9.8% | 37.9% | 9.6% | 29.9% | 9.9% | 10.8% | 9.0% | 12.0% |
| Gross margin (TTM) ≥ 35% | 55.7% | 66.2% | 81.9% | 45.8% | 80.1% | 52.8% | 48.6% | 49.6%¹ | 69.2% |
| GM stability (10Y std) ≤ 7pp | 2.3pp | 1.4pp | 1.0pp | 4.8pp | 1.9pp | 1.2pp | 3.1pp | 6.0pp | 3.5pp |
| OPM 7Y min ≥ 5% | 16.4% | 18.6% | 41.3% | 13.9% | 22.1% | 13.1% | 7.0% | 36.7% | 9.1% |
| CFO/NI TTM ≥ 0.90 | 1.33 | 1.74 | 0.97 | 1.52 | 2.08⁴ | 1.39 | 1.46 | 1.18¹ | 1.98 |
| CFO/NI 10Y median ≥ 0.90 | 1.37 | 1.51 | 1.20 | 1.64 | 1.46 | 1.59 | 1.64 | 1.76 | 1.92 |
| Net debt/EBITDA ≤ 2.5x | 0.24x | 0.96x | 0.75x | 0.40x | 0.93x | −1.45x | −3.03x | −0.10x | −0.30x |
| Debt service — coverage ≥ 6x | NA² | 15.4x | 6.80x | 13.9x | 39.5x | 471x | 87.9x | NA³ | 100x (cap) |
| Debt service — net debt ≤ 1.5x | 0.24x | 0.96x | 0.75x | 0.40x | 0.93x | −1.45x | −3.03x | −0.10x | −0.30x |
| No NI loss years (10Y) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Share CAGR 5Y ≤ +1% | −2.5% | −0.2% | −0.8% | −5.4% | −4.6% | −2.5% | −0.2% | 0.0% | −0.6% |
| Price — owner-earnings yield on EV ≥ 5% | **9.19%** | **5.49%** | **5.14%** | **7.14%** | 2.98% | 4.38% | 4.12% | **5.28%** | 2.87% |
| Price — EBIT yield on EV ≥ 6.67% | **13.19%** | **6.98%** | **9.41%** | **11.98%** | 6.19% | **8.59%** | 4.09% | **9.75%**¹ | 4.72% |
| Price — FCF yield on EV ≥ 5% | **8.27%** | **5.94%** | 1.97% | **12.31%** | **7.20%**⁴ | **10.40%** | **5.15%** | 4.83% | **5.48%** |
| iROIC 5Y ≥ 12% | 35.3% | 16.1% | 24.1% | 1.00 (cap) | 27.8% | 46.8% | 21.4% | 117% | 31.6% |
| Owner-earnings CAGR 10Y ≥ 7% | 25.3% | 11.8% | 13.7% | 17.4% | 19.0% | 7.6% | 17.8% | 13.6% | 42.3% |
| **Gates passed** | **14/14** | **14/14** | **14/14** | **14/14** | **14/14** | **14/14** | **14/14** | **14/14** | **14/14** |

¹ FY2025 figure through the TTM resolver's fiscal-year fallback (Grupo México
caveat below). ² NA on a provider interest-expense gap (Lululemon caveat).
³ NA on the broken TTM window (Grupo México caveat). Both debt-service gates
pass on the net-debt arm. ⁴ Inflated by a vendor cumulation error in Paycom's
Q2 2026 cash-flow rows (Paycom caveat); both gates still pass on corrected
figures, the FCF arm only just.

All 126 criteria cells compute. The two `any_of` gates are where the nine
differ: Lululemon and Grupo México pass debt service on the net-debt arm
alone, and the price gate splits the group three ways — **all three arms**
(Lululemon, PayPal, and LVMH by 0.5, 0.3 and 0.9 points), **two arms**
(Novo Nordisk and Grupo México on owner earnings and EBIT; Cirrus Logic on
EBIT and FCF) and the **FCF arm alone** (Paycom, Paylocity, and Realtek by
fifteen basis points; Paycom's 7.20% is a vendor-inflated reading that
corrects to roughly 5.2–5.5% — see its caveat). Trailing P/E on the vendor's
adjusted EPS runs from
10.4x (Lululemon) and 11.2x (PayPal) to 27.2x (Realtek); EV/EBIT from 7.6x
(Lululemon) to 24.5x (Realtek).

**Graham screen** (`screeners/deep_value_graham.yml`, seven criteria): six of
nine pass. The three failures are all on the cheapness gate (P/B ≤ 3.0x, or
EV/EBIT ≤ 12x, or FCF yield on EV ≥ 6%): LVMH 3.41x / 14.3x / 5.94%,
Paylocity 6.82x / 21.2x / 5.48%, Realtek 8.65x / 24.5x / 5.15% — and
Realtek also misses the Piotroski floor at 4 of 9. Every other Graham
criterion passes for all nine, with no NAs. One pass is hollow: Paycom
clears the cheapness gate on its 7.20% FCF yield alone (P/B 16.95x, EV/EBIT
16.1x), and that yield is the cumulation-inflated reading described below —
corrected to ~5.2–5.5% it would fail the 6% bar, making the honest Graham
count five of nine.

### Data caveats recorded on addition (observations only; nothing changed)

- **Grupo México's TTM window is broken by a fragment quarter.** Q3 2025
  (end_date 2025-09-30) is stored with three concepts — `CapitalExpenditures`
  = 0, `EarningsPerShare`, `EarningsPerShareDiluted` — against 38–39 for every
  neighbouring quarter, so twenty TTM metrics are NA with "quarterly records
  do not form a trailing-twelve-month window": `roic_ttm`,
  `operating_margin_ttm`, `fcf_margin_ttm`, `interest_coverage`, `roe_ttm`,
  `roa_ttm`, `accruals_ratio`, `oe_ev_ttm`, `oe_equity_ttm` and their
  dependants. Three QARP inputs still computed because the shared TTM-window
  resolver falls back to the latest fiscal year for them — `gross_margin_ttm`,
  `cfo_to_ni_ttm` and the EBIT numerator of `ebit_yield_ev` all carry as_of
  2025-12-31 (¹ above) — while the metrics listed before do not take that
  fallback and emit NA instead. The gate verdicts are safe (FY2025 is a
  complete year) but two things are worth fixing: the **fallback policy is
  inconsistent across the TTM family**, and a single vendor fragment quarter
  silently costs twenty metrics. Proposed: (a) one documented TTM-window
  policy — fall back to the latest FY for every TTM metric or for none; (b) a
  normalizer check that flags a quarter stored with a small fraction of its
  neighbours' concept count, so the fragment is visible at ingest rather than
  discovered as NAs. The listing's price is also three days staler than the
  other non-US names (07-24).
- **Phantom mid-year "FY" rows for LVMH and Grupo México.** Each carries a
  `fiscal_period = 'FY'` row dated 2026-06-30 holding only `EarningsPerShare`
  and `EarningsPerShareDiluted` (two concepts against 39–41 for a real fiscal
  year) — an interim EPS the vendor tagged as yearly. No gate reads them, but
  any FY-series metric that keys on "latest FY" could. Proposed: the
  normalizer rejects an FY row whose end_date does not fall on the issuer's
  fiscal-year end, or whose concept count is a small fraction of the prior
  FY's.
- **LVMH's fundamentals end at FY2025.** It reports semi-annually; the 2026
  half-year is not stored as a quarter, so every TTM/latest metric is the
  FY2025 figure priced at 2026-07-27 — the semi-annual pattern already
  recorded in the
  [metric verification audit](../research/qarp-dvg-metric-verification-2026-07.md).
  `graham_eps_10y_cagr_3y_avg`, `peg_ratio` and `peg_ratio_div_adj` are NA
  ("could not derive CAGR value"); none is a criterion. The price gate passes
  on all three arms, but by the thinnest margins of any all-arms passer.
- **Lululemon's coverage NA is a provider gap, correctly refused.**
  `interest_coverage` is NA with "material debt without measurable interest
  expense — debt evidence $2.14B > 1.0x TTM EBIT": the debt is essentially
  lease liabilities and the vendor carries no interest-expense line, so the
  guard declines to emit the 100x cap (² above). The gate passes on the 0.24x
  net-debt arm. Its latest stored quarter row (end_date 2026-06-30) is a
  one-concept fragment (`CommonStockSharesOutstanding` only), so TTM metrics
  run through the full quarter ending 2026-04-30 — the fragment pattern
  Cloudflare's Q2 showed. `long_term_debt` is NA ("no recent long-term debt
  fact"), by design: a missing debt row is never read as zero.
- **Novo Nordisk's Q2 2026 is absent** although it was published in the first
  week of August, before the 2026-08-10 fact refresh — vendor lag, not a
  pyvalue gap; facts run through Q1 2026. Two near-bar passes: interest
  coverage 6.80x against the 6x arm (the 0.75x net-debt arm covers it
  regardless) and CFO/NI TTM 0.97. The FCF arm fails at 1.97% because capex
  absorbs most of operating cash flow (FCF margin 9.5% against a 45.3%
  operating margin); the EBIT (9.41%) and owner-earnings (5.14%) arms carry
  the gate.
- **Realtek is the thinnest pass.** FCF yield 5.15% against a 5% bar, OPM 7Y
  minimum 6.99% against 5%, Piotroski 4. The USD market cap (read by the
  Graham size gate only) uses a USD/TWD rate eight days stale at the 07-27
  price; the same staleness applies to the EUR and DKK conversions
  (`report-screen-failures` warns on all three). `sbc_to_fcf` and
  `sbc_to_revenue` are NA on too few quarterly SBC records; not criteria.
- **Paycom's Q2 2026 cash-flow rows are six-month cumulative figures stored
  as a quarter.** `CommonStockDividendsPaid` reads $17.7M for Q1 2026 and
  $35.5M for Q2 against ~$21M in every other quarter;
  `NetCashProvidedByUsedInOperatingActivities` reads $212.5M then $427.6M;
  `SalePurchaseOfStock` −$1,054M then −$1,390M, which is Q1 plus the ~$335M
  the 10-Q reports for the quarter itself. The same quarter's income-statement
  rows (revenue $531M, net income $107M) are properly quarterly, so the
  vendor's de-cumulation failed for the cash-flow statement alone. Every TTM
  cash-flow metric therefore double-counts Q1: CFO/NI TTM reads 2.08 where
  ~1.64 is right (still a pass); the FCF arm reads 7.20% where the corrected
  yield is roughly 5.2–5.5% (still the one passing arm, now by a thin
  margin); `shareholder_yield_ttm` reads 29.6% where ~18.5% is right — the
  repurchases are real and very large (5Y share CAGR −4.6%), just not that
  large. No QARP gate flips, but the pass is far thinner than the stored rows
  say, and the Graham cheapness pass does not survive the correction (above).
  Proposed: a normalizer de-cumulation check on quarterly cash-flow rows — a
  flow concept landing within a few percent of the prior quarter's value
  times the quarter's ordinal (2x in Q2, 3x in Q3) while the same filing's
  income-statement rows do not is year-to-date, and should be differenced or
  quarantined rather than stored. The fault is not Paycom-specific: of the
  5,048 listings with both a Q1 and a Q2 2026 row stored, 167 show a Q2
  dividend within 3% of exactly twice Q1 and 17 show operating cash flow
  doubling with it (the strong signature). Paycom is the only watchlist name
  affected, and its 2025 quarters reconcile to the FY2025 total ($677.0M
  summed against $678.9M), so the failure is confined to the latest filing.
  The owner-earnings arm (2.98%) is unaffected and sits well short because
  the five-year *median* owner earnings trail the current run-rate.
- **PayPal's iROIC 5Y is the 1.00 cap**: invested capital shrank over five
  years as buybacks outran retained earnings (5Y share CAGR −5.4%), so the
  ratio is undefined upward and the documented cap applies. Passes all three
  price arms with room (7.14% / 11.98% / 12.31%).
- **Paylocity's coverage is the 100x cap** on the debt ≤ 1x EBIT evidence
  (net cash, −0.30x EBITDA), as with Apple. Its fiscal year ends in June, so
  its ten-year series (as_of 2026-06-30) are a quarter fresher than the
  others'. Price gate on the FCF arm only (5.48%).
- **Cirrus Logic** is the cleanest of the nine — Piotroski 9, net cash
  −1.45x EBITDA, coverage 471x. `long_term_debt` is NA ("no recent long-term
  debt fact"), by design: the company has no borrowings and a missing debt
  row is never read as zero.
- **Structural NAs outside the QARP set**: `price_to_ncav` refuses to compute
  for LVMH, Novo Nordisk, PayPal and Paycom — negative NCAV by construction,
  the Graham net-net lens does not apply to franchise businesses — the same
  class as Apple's.

## Cross-listings (known, NOT tracked)

Several watchlist companies have more than one listing in the catalog. Only
the tracked listing in the [table above](#tracked-stocks-22) is part of the
watchlist; the others are recorded here because they share the issuer's name
— ad-hoc SQL that joins on `issuer.name` will pick them up, which is why the
watchlist keys on `listing_id`. Their NA analysis is out of scope.

### SK Hynix

Only the Korean home listing `000660.KO` is tracked — it has by far the deepest
fact history (FY facts back to 2000). Two depositary-receipt listings for the same
company exist in the catalog but are deliberately not part of the watchlist; their
fact histories are short and their NA analysis is out of scope:

| Symbol | listing_id | Note |
|---|---|---|
| HY9H.F | 15490 | Sponsored GDR, Frankfurt, EUR |
| HXSCL.US | 62125 | ADR, US OTC, USD |

Catalog caveat: each of the three SK Hynix listings sits under its **own** issuer
row, and all three carry `primary_listing_status = 'primary'`. The flag is
per-issuer, so it does not identify the home listing among cross-listings — the
catalog currently has no issuer-level link between them. Treat `000660.KO` as the
canonical SK Hynix listing by convention (this file), not by schema.

### The 2026-10-18 additions

Home-market listings are tracked for LVMH, Novo Nordisk, Realtek and Grupo
México (the Hynix precedent). The alternates below are those that either
carry computed metrics of the 2026-08-11 run or are flagged `primary`;
further German regional and LSE lines exist with no metrics and are omitted.
Realtek and Paylocity have no alternates in the catalog.

| Company | Symbol | listing_id | Note |
|---|---|---|---|
| Lululemon | 33L.F | 9668 | Frankfurt line, EUR — flagged `primary`; 86 computed metrics |
| Lululemon | LULU.NEO | 35167 | CAD-hedged CDR, Cboe Canada — flagged `primary`; 58 |
| Lululemon | L1UL34.SA | 37555 | BDR, São Paulo, BRL; 76 |
| Lululemon | 0JVT.LSE | 29076 | LSE international line, USD; 74 |
| Lululemon | LULU.MX | 34933 | SIC line, Mexico, MXN; 59 |
| Lululemon | 33L.XETRA | 80800 | Xetra line, EUR; 59 |
| LVMH | LVMHF.US | 64243 | US OTC ordinary line, USD; 90 |
| LVMH | LVMUY.US | 64244 | ADR, US OTC — `secondary`, no metrics |
| LVMH | LVMH01.BK | 77768 | Thai DR, Bangkok, THB; 41 |
| Novo Nordisk | NONOF.US | 65979 | US OTC ordinary line, USD — flagged `primary`; 87 |
| Novo Nordisk | NVO.US | 66261 | NYSE ADR, USD — `secondary`, no metrics |
| Novo Nordisk | NVON.MX | 34974 | SIC line, Mexico, MXN; 63 |
| Novo Nordisk | N1VO34.SA | 37695 | BDR, São Paulo, BRL; 73 |
| PayPal | PYPL.NEO | 35196 | CAD-hedged CDR, Cboe Canada — flagged `primary`; 74 |
| PayPal | 2PP0.F | 9505 | Frankfurt CDR line, EUR — flagged `primary`; 58 |
| PayPal | PYPL34.SA | 37840 | BDR, São Paulo, BRL; 86 |
| PayPal | 0R9U.LSE | 30267 | LSE international line, USD; 61 |
| PayPal | PYPL.MX | 35003 | SIC line, Mexico, MXN; 64 |
| PayPal | ZPYP.NEO | 35248 | BMO CAD-hedged CDR, Cboe Canada; 59 |
| Paycom | 0KGH.LSE | 29220 | LSE international line, USD; 92 |
| Paycom | P1YC34.SA | 37773 | BDR, São Paulo, BRL; 75 |
| Cirrus Logic | CRU.DU, CRU.MU | 6945, 33050 | Düsseldorf and Munich lines, EUR; 81 each |
| Cirrus Logic | 0HYI.LSE | 28790 | LSE international line, USD; 71 |
| Grupo México | GMBXF.US | 60949 | US OTC line, USD; 91 — its vendor payload is USD-denominated with quarters through 2026-06-30 and an intact TTM window, unlike the home listing's |

The `primary` flags on 33L.F, LULU.NEO, NONOF.US, PYPL.NEO and 2PP0.F are the
same catalog caveat as SK Hynix's: the flag is per issuer row and does not
pick the home listing among cross-listings.
