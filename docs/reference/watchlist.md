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

## Tracked stocks (12)

| Canonical symbol | listing_id | Issuer | Exchange | Currency |
|---|---|---|---|---|
| MSFT.US | 65235 | Microsoft Corporation | US | USD |
| GOOGL.US | 61081 | Alphabet Inc Class A | US | USD |
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

`listing_id` values are recorded so ad-hoc read-only SQL can target these rows
without symbol joins.

### Recomputed 2026-08-11

All eleven were recomputed in the full-universe run of 2026-08-11, the first on
the rebuilt debt basis (`d5d8b34`, `d2e600c`, `c24aad2`, then `71b259d`,
`ce92a3f`). Every ROIC figure on the watchlist moved, because the ROIC FY series
had kept a private copy of the retired components-preferred debt rule until
`ce92a3f`. **Microsoft is now net debt +0.27x EBITDA rather than net cash** —
the old figure omitted $78.8B of finance leases.

`ADBE.US` remains the **only** watchlist name that passes QARP, at 14 of 14
gates. `MSFT.US` is 13/14 on the price gate alone; `GOOGL.US` slipped to 12/14
when Q2 2026's one-time investment gains pushed CFO/NI TTM to 0.76. Gate-by-gate
detail for all twelve names is in the
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

## Paste-ready scope line

All pyvalue commands share one scope resolver, so the watchlist can be passed
verbatim to any of them:

```
--symbols MSFT.US GOOGL.US ADBE.US NVDA.US AMD.US C.US PLTR.US TSLA.US INTC.US 000660.KO NET.US AVGO.US
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

## SK Hynix cross-listings (known, NOT tracked)

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
