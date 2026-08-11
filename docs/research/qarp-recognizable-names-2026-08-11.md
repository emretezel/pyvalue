# Recognizable Names in the QARP Passer List (2026-08-11)

Author: Emre Tezel

The 2026-08-11 run of `screeners/quality_reasonable_price_primary.yml` over the
full supported universe (52,842 symbols) returned **129 passing listings**.
Collapsing every company to its single web-verified home listing leaves
**90 companies — one per company**.

This document picks out the subset a generalist investor would recognize by
name: **39 of the 90**. It is a readability aid for triaging the list, not a
screening result — recognizability carries no analytical weight, and the
company notes below are descriptive only.

Provenance: `data/output/screen_results_qarp_primary_2026-08-11_raw.csv` (all
129 passing listings, raw gate values) and
`data/output/screen_results_qarp_primary_2026-08-11.csv` (the 90 deduped
companies). Ranks are the original screener ranks, so they are non-contiguous
where rows were removed; `qarp_score` is the composite of the ranking block at
the foot of the screen definition.

This run is the **first on a corrected debt basis**. See
[What changed since 2026-07-28](#what-changed-since-2026-07-28) — the passer
list is not comparable to earlier runs without reading that section.

## Global household names (12)

| Rank | Symbol | Company | Business | Score | Verdict |
|---|---|---|---|---|---|
| 14 | `KIMBERA.MX` | Kimberly-Clark de México | Tissue and personal care | 70.7 |  |
| 15 | `LULU.US` | Lululemon Athletica | Athletic apparel | 70.5 |  |
| 17 | `ZTS.US` | Zoetis | Animal health | 70.2 |  |
| 18 | `NOVO-B.CO` | Novo Nordisk | Diabetes/obesity pharma (Ozempic, Wegovy) | 69.6 |  |
| 21 | `CDI.PA` | Christian Dior SE | Holding company controlling LVMH | 67.5 | **EV defect** |
| 22 | `PYPL.US` | PayPal | Digital payments | 67.4 |  |
| 28 | `ADBE.US` | Adobe | Creative and document software | 65.9 |  |
| 60 | `600519.SHG` | Kweichow Moutai | Baijiu; China's premier liquor brand | 49.5 |  |
| 68 | `RMD.US` | ResMed | Sleep apnoea devices | 47.1 |  |
| 87 | `NXT.LSE` | Next plc | UK clothing and homeware retail | 41.5 |  |
| 118 | `CPRT.US` | Copart | Online salvage-vehicle auctions | 28.6 |  |
| 127 | `MC.PA` | LVMH | Luxury goods conglomerate | 21.1 |  |

## Well known within a sector or region (27)

| Rank | Symbol | Company | Business | Score | Verdict |
|---|---|---|---|---|---|
| 1 | `DNLM.LSE` | Dunelm Group | UK homewares, FTSE 250 | 83.0 |  |
| 12 | `030000.KO` | Cheil Worldwide | Samsung's advertising arm | 73.1 |  |
| 19 | `TXT.WAR` | Text S.A. | Warsaw-listed SaaS, formerly LiveChat | 68.9 |  |
| 26 | `DOX.US` | Amdocs | Telecom BSS/OSS software | 66.1 | **SUSPECT** |
| 29 | `NESTLE.KAR` | Nestlé Pakistan | Nestlé's listed Pakistani affiliate | 65.8 |  |
| 30 | `G.US` | Genpact | Business process outsourcing | 64.9 |  |
| 39 | `3014.TW` | ITE Tech | Taiwanese IC design | 58.5 | **FALSE POSITIVE** |
| 42 | `MRP.JSE` | Mr Price Group | South African value apparel retail | 56.3 |  |
| 44 | `OMAB.MX` | Grupo Aeroportuario del Centro Norte | Mexican airport concessions | 55.4 |  |
| 45 | `MEDI.OL` | Medistim | Norwegian surgical ultrasound | 55.1 | **FALSE POSITIVE** |
| 52 | `PAYC.US` | Paycom Software | US payroll software | 52.6 |  |
| 53 | `600809.SHG` | Shanxi Fenjiu | Baijiu (Fen wine) | 52.5 |  |
| 58 | `JD.LSE` | JD Sports Fashion | UK sports fashion retail | 49.8 |  |
| 63 | `MEKKO.HE` | Marimekko | Finnish design house | 48.8 |  |
| 73 | `XTB.WAR` | XTB (X-Trade Brokers) | European retail CFD/FX broker | 45.5 | **broker — see caveats** |
| 75 | `EVR.US` | Evercore | Independent M&A advisory | 44.9 |  |
| 80 | `EVO.ST` | Evolution AB | Live casino B2B software | 44.1 |  |
| 84 | `035900.KQ` | JYP Entertainment | K-pop agency (Twice, Stray Kids) | 42.5 |  |
| 93 | `CRUS.US` | Cirrus Logic | Audio chips, major Apple supplier | 40.3 |  |
| 95 | `2379.TW` | Realtek Semiconductor | Connectivity and networking chips | 38.6 |  |
| 97 | `QLYS.US` | Qualys | Cloud security and compliance | 37.4 |  |
| 101 | `300760.SHE` | Mindray Bio-Medical | Medical devices | 35.6 |  |
| 105 | `GMEXICOB.MX` | Grupo México | Copper mining, rail, infrastructure | 33.5 |  |
| 110 | `600660.SHG` | Fuyao Glass | Automotive glass | 31.8 |  |
| 113 | `5296.KLSE` | Mr D.I.Y. Group | Malaysian home-improvement retail | 30.7 |  |
| 114 | `NEM.XETRA` | Nemetschek | Construction/AEC software | 30.5 |  |
| 122 | `PCTY.US` | Paylocity | US payroll and HCM software | 26.7 |  |

## What changed since 2026-07-28

Against the previous run's 92 companies: **84 held, 8 dropped,
6 added.** Three separate causes moved at once and the write-up should not
credit any single one:

1. **The debt fix** (`d5d8b34`, `d2e600c`, `c24aad2`, 2026-08-10 —
   see [MSFT QARP Review](msft-qarp-review-2026-08.md)). Debt is now the
   broadest measured reading and includes finance leases, and normalization no
   longer files non-current liabilities under `LongTermDebt`.
2. **Two follow-on fixes** on 2026-08-11 (`71b259d`, `ce92a3f`) — see
   [The debt-less-period problem](#the-debt-less-period-problem) below.
3. **The first full-universe recompute since 2026-07-06.** The 2026-07-28 run
   read metrics of mixed vintage; only 4,451 listings had been recomputed on
   07-24. Prices also moved.

### The 8 that dropped

| Company | Why |
|---|---|
| `300015.SHE` Aier Eye Hospital | price gate, all three arms |
| `5246.KLSE` Westports | price gate, all three arms |
| `ADP.US` Automatic Data Processing | price gate, all three arms |
| `ITX.MC` Inditex | price gate, all three arms |
| `PAYX.US` Paychex | price gate, all three arms |
| `ULTA.US` Ulta Beauty | price gate, all three arms |
| `300908.SHE` Zhongjing Food | `iroic_5y` 0.1171 vs the 0.12 gate |
| `WTN.WAR` Wittchen | `roic_10y_min` 0.0681 vs the 0.07 gate |

Every one is a genuine gate failure — no company left this list because a metric
became unmeasurable. `ADP.US` was already recorded as failing every arm at the
current price in the 2026-07-28 caveats, so its exit confirms that note rather
than contradicting it. `5246.KLSE` (Westports) is the case that motivated the
debt fix in the first place: its sukuk sits in the provider rollup and not in
the components, so the old lease-blind reading understated its net debt roughly
15-fold.

### The 6 that were added

`GAPB.MX` (Grupo Aeroportuario del Pacífico), `OCL.AU` (Objective
Corporation), `PCTY.US` (Paylocity) and `603658.SHG` (Autobio Diagnostics) are
genuine new passers. `3014.TW` and `MEDI.OL` are **not** new — they passed the
screen on 2026-07-28 too and were removed by hand as proven false positives.
They are back in the raw list because nothing in the code changed to exclude
them, and they are flagged again above.

### The debt-less-period problem

Removing the fabricated `LongTermDebt` exposed a coverage gap that had been
papered over. Providers null all three debt fields for a period rather than
reporting a zero, and invested capital needs a debt leg for **every** period in
its window, so one gap voided the whole strict 10-year ROIC chain. That cost
`roic_years_above_12pct` / `roic_10y_min` on ~6,000 listings — including
PayPal, Kweichow Moutai, Evolution AB and Cirrus Logic.

The fix substitutes that period's `LiabilitiesNoncurrent` as an explicit
**bound** on the debt leg. Reading the gap as zero was measured and rejected:
EODHD does emit explicit zeroes for genuinely debt-free periods (141k
`LongTermDebt` and 88k `ShortTermDebt` rows are exactly `0`), so an absent row
means unreported, and on a 1,457-period sample the nearest reported debt
exceeded 20% of that period's total liabilities in 48.8% of cases.

The bound is allowed wherever the error direction is one-way. Inside invested
capital it can only inflate the denominator, so it can only depress a return.
`enterprise_value` and `net_debt_to_ebitda` were added on the same reasoning
(`053dcda`): an overstated debt figure raises EV, lowers every yield built on
it, and raises the leverage ratio, so it can block a gate but never clear one.

| | before | after |
|---|---|---|
| `roic_10y` NA on missing debt input | 6,054 listings | **492** |
| `roic_7y` NA on missing debt input | 6,190 listings | **582** |
| `ebit_yield_ev` NA on missing EV debt/cash | 5,255 listings | **1,117** |
| `net_debt_to_ebitda` NA on missing net debt | 1,546 listings | **221** |

**`short_term_debt_share` is the exclusion that proves the rule is not
arbitrary.** Total debt is its *denominator*, so a bound understates the share —
the flattering direction — and it still resolves NA. `long_term_debt`,
`debt_paydown_years` and `fcf_to_debt` are excluded for a different reason:
there the debt figure is not an input to a conclusion, it *is* the conclusion.

An intermediate run of this screen, before the bound reached EV, returned 121
listings and 84 companies. Six companies — Bioventix, JYP Entertainment, Asseco
Business Solutions, Thessaloniki Port, Thaitheparos and XTB — were absent from
it not because they had become expensive but because their EV was unmeasurable.
All six are in the list above.

## Three observations

**Familiar names still cluster at both ends of the ranking.** Lululemon,
Zoetis, Novo Nordisk, PayPal and Adobe rank high because each has taken a real
derating; LVMH, Copart and Paylocity sit near the bottom, clearing the price
gate on thin margins. Since `qarp_score` composites quality *and* cheapness, a
familiar mega-cap ranking high signals that the market has marked it down hard
— not that it is quietly cheap. Treat a high rank on a well-covered name as a
prompt to ask what the market knows, not as an edge.

**Recognizability is inversely correlated with verifiability.** The other 51
rows are mostly small- and mid-cap Asia — Chinese A-shares, Taiwanese,
Thai, Indonesian and Malaysian lines. That is where the screen finds most of
its candidates and where external corroboration is thinnest, so stored-metric
defects there are the least likely to be caught by the kind of cross-check
[the July verification round](qarp-passers-independent-verification-2026-07.md)
applied to the better-covered names.

**Twenty companies passed on a foreign line while their own home listing
failed** — AutoZone, Snap-on, GSK, Fuchs, Inditex, Paychex, Ulta, Pentair,
Kainos, Straumann, Temenos, Recordati, Wolters Kluwer, NetEase, ANTA, 3SBio,
Jumbo, Shoei, Apranga and Telia Lietuva. Every one is excluded above, because the
company did not pass on the listing that represents it. But the divergence
itself is a defect signal: the same fundamentals evaluated at two venues should
not disagree on a binary gate. This is the cross-listing consistency problem
[proposed fix 7](qarp-passers-independent-verification-2026-07.md#proposed-fixes-none-applied)
described, now attested on twenty companies in a single run.

## How the 121 were collapsed to 84

Secondary status was established per listing from primary sources — exchange
and company investor-relations pages, SEC filings, Nasdaq Baltic, and exchange
quote pages — not inferred from the venue. Venue was used only to decide which
names to look up. Twenty-three listings were verified in this round; the rest
reuse the web-verified `company_key` groups from the
[2026-07-26 verification](qarp-passers-independent-verification-2026-07.md),
whose verdicts stand.

Thirty-nine listings were removed as secondary. The full mapping, with the
home listing and source for each, is in
`data/output/qarp_passers_2026-08-11_with_verification.csv`.

One verdict is worth recording because it looks like a cross-listing and is
not: **`ITRN.US` (Ituran) is primary.** It was dual-listed on TASE and Nasdaq
until the TASE delisting took effect on 2016-05-25, after which
Nasdaq is its only listing.

Mainland China A-shares (`.SHE` / `.SHG`), Taiwan (`.TW`, `.TWO`), Korea
(`.KO`, `.KQ`), Indonesia (`.JK`), Thailand (`.BK`), Malaysia (`.KLSE`),
Pakistan (`.KAR`), Johannesburg (`.JSE`) and Warsaw (`.WAR`) lines are
domestic listings of domestically incorporated issuers; a depositary receipt
cannot be the A-share or KOSDAQ line, so these are home listings by
construction and were not searched individually.

### Why the duplicates are there at all

`run-screen` already excludes listings classified `secondary`, so the manual
pass should not be necessary. It is, because **32 of the 129 passers carry
`primary_listing_status = 'unknown'`**, which stays screen-eligible by design.
EODHD returns `PrimaryTicker`, `HomeCategory` and `ISIN` all null for these
lines, so the ordered classification rule set cannot settle them. There are
19,935 such listings in the catalog. Until that bucket is resolved, every
QARP run needs this manual collapse.

## Caveats carried forward

- **`DOX.US` (Amdocs) is SUSPECT**, not confirmed: its pass hangs on
  `owner_earnings_cagr_10y` of 0.0707 against a 0.0700 gate, where every
  external proxy indicates 2–5%.
- **`3014.TW` (ITE Tech) and `MEDI.OL` (Medistim) are proven FALSE POSITIVES**
  and are listed only because the screen still returns them. ITE Tech's EODHD
  FY2025 CFO of 1,859.8M TWD is against an audited 902.0M (real CFO/NI 0.115
  vs a 0.90 gate); Medistim's quarterly cash-flow rows are cumulative YTD and
  are summed as if discrete, inflating TTM FCF to 381m NOK against a real 180m.
- **`CDI.PA` (Christian Dior)** carries a known EV distortion: minority
  interest is omitted, so its `ev_to_ebit` reads far below LVMH's for
  substantially the same asset. Its rank is an artifact.
- **`XTB.WAR` is a broker**, and QARP's financial-sector exclusion does not
  reach brokers. The owner-earnings gates run on a maintenance-NWC chain that is
  not meaningful for a balance-sheet business, so its pass should not be read
  the way the others are. This is
  [proposed fix 9](qarp-passers-independent-verification-2026-07.md#proposed-fixes-none-applied),
  still open.
- **Three of the seven pence-collapse listings still hold 100x-low prices.**
  The ingestion fix shipped 2026-07-27 but does not repair stored values.
  Cadence, IDEXX, KLA and Parker-Hannifin have been re-fetched and are correct
  (which is why they are absent here); **Grainger, Mettler-Toledo and Moody's
  have not been re-fetched since 2026-07-17** and still carry the corrupted
  price. They do not pass this run, but they are not safe until re-fetched.
- Ranks and scores are a snapshot of the 2026-08-07 price file (2026-07-27 for
  some non-US venues). Gate 12 is price-sensitive by construction, and several
  names in both tables clear it by under one percentage point.
