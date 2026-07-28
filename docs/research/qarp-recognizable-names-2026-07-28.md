# Recognizable Names in the QARP Passer List (2026-07-28)

Author: Emre Tezel

The 2026-07-28 run of `screeners/quality_reasonable_price_primary.yml` over the
full supported universe (52,852 symbols) returned 133 passers. After removing
the four listings the [2026-07 verification round](qarp-passers-independent-verification-2026-07.md)
proved to be false positives, and after collapsing every company to its single
web-verified primary listing, **92 rows remain — one per company**.

This document picks out the subset a generalist investor would recognize by
name: **31 of the 92**. It is a readability aid for triaging the list, not a
screening result — recognizability carries no analytical weight, and the
company notes below are descriptive only.

Provenance: `data/output/qarp_passers_2026-07-28_with_verification.csv`
(annotated) and `data/output/screen_results_qarp_primary_2026-07-28.csv` (raw
gate values). Ranks are the original screener ranks, so they are non-contiguous
where rows were removed; `qarp_score` is the composite of the ranking block at
the foot of the screen definition.

## Global household names (13)

| Rank | Symbol | Company | Business | Score | Verdict |
|---|---|---|---|---|---|
| 7 | `NOVO-B.CO` | Novo Nordisk | Diabetes/obesity pharma (Ozempic, Wegovy) | 73.8 | |
| 12 | `LULU.US` | Lululemon Athletica | Athletic apparel | 71.6 | |
| 17 | `PYPL.US` | PayPal | Digital payments | 67.3 | |
| 19 | `ADBE.US` | Adobe | Creative and document software | 66.5 | |
| 27 | `CDI.PA` | Christian Dior SE | Holding company controlling LVMH | 63.7 | |
| 30 | `ZTS.US` | Zoetis | Animal health | 60.7 | |
| 56 | `ULTA.US` | Ulta Beauty | US beauty retail | 51.4 | **SUSPECT** |
| 69 | `NXT.LSE` | Next plc | UK clothing and homeware retail | 46.2 | |
| 86 | `ADP.US` | Automatic Data Processing | Payroll and HCM services | 43.0 | |
| 108 | `RMD.US` | ResMed | Sleep apnoea devices | 35.5 | |
| 116 | `CPRT.US` | Copart | Online salvage-vehicle auctions | 33.0 | |
| 121 | `ITX.MC` | Inditex | Zara owner | 30.4 | |
| 126 | `MC.PA` | LVMH | Luxury goods conglomerate | 27.1 | |

## Well known within a sector or region (18)

| Rank | Symbol | Company | Business | Score | Verdict |
|---|---|---|---|---|---|
| 1 | `DNLM.LSE` | Dunelm Group | UK homewares, FTSE 250 | 86.8 | |
| 15 | `KIMBERA.MX` | Kimberly-Clark de México | Tissue and personal care | 69.1 | |
| 20 | `NESTLE.KAR` | Nestlé Pakistan | Nestlé's listed Pakistani affiliate | 66.5 | |
| 22 | `DOX.US` | Amdocs | Telecom BSS/OSS software | 66.3 | **SUSPECT** |
| 25 | `G.US` | Genpact | Business process outsourcing | 65.0 | |
| 29 | `EVR.US` | Evercore | Independent M&A advisory | 61.0 | |
| 35 | `JD.LSE` | JD Sports Fashion | UK sports fashion retail | 59.6 | |
| 46 | `PAYC.US` | Paycom Software | US payroll software | 55.8 | |
| 55 | `PAYX.US` | Paychex | US payroll and HR services | 51.7 | |
| 63 | `600519.SHG` | Kweichow Moutai | Baijiu; China's premier liquor brand | 48.7 | |
| 73 | `QLYS.US` | Qualys | Cloud security and compliance | 45.4 | |
| 88 | `CRUS.US` | Cirrus Logic | Audio chips, major Apple supplier | 42.5 | |
| 91 | `EVO.ST` | Evolution AB | Live casino B2B software | 41.6 | |
| 92 | `035900.KQ` | JYP Entertainment | K-pop agency (Twice, Stray Kids) | 41.5 | |
| 102 | `2379.TW` | Realtek Semiconductor | Connectivity and networking chips | 38.3 | |
| 107 | `300760.SHE` | Mindray Bio-Medical | Medical devices | 36.7 | |
| 114 | `GMEXICOB.MX` | Grupo México | Copper mining, rail, infrastructure | 33.2 | |
| 124 | `600660.SHG` | Fuyao Glass | Automotive glass | 29.3 | |

## Two observations

**Familiar names cluster at both ends of the ranking, not the middle.** Novo
Nordisk, Lululemon, PayPal and Adobe rank high because each has taken a real
derating; LVMH, Inditex and Copart sit near the bottom, clearing gate 12 on thin
margins. Since `qarp_score` composites quality *and* cheapness, a familiar
mega-cap ranking high here signals that the market has marked it down hard —
not that it is quietly cheap. Treat a high rank on a well-covered name as a
prompt to ask what the market knows, not as an edge.

**Recognizability is inversely correlated with verifiability.** The other 61
rows are mostly small- and mid-cap Asia — 19 Chinese A-shares, 10 Taiwanese, 6
Thai, 4 Indonesian, 3 Malaysian. That is where the screen finds most of its
candidates and where external corroboration is thinnest, so stored-metric
defects there are the least likely to be caught by the kind of cross-check that
[the verification round](qarp-passers-independent-verification-2026-07.md)
applied to the better-covered names.

## Caveats carried from the verification round

- **`ULTA.US` and `DOX.US` are SUSPECT**, not confirmed. Ulta's gate 5 pass
  rests on a component-built EBIT (stored `opm_7y_min` 5.10%/5.95% against a
  GAAP FY2020 operating margin of 3.85%); Amdocs's pass hangs on
  `owner_earnings_cagr_10y` of 0.0707 against a 0.0700 gate, where every
  external proxy indicates 2–5%.
- **`ADP.US` is recorded CONFIRMED by author direction.** Independent
  verification on 2026-07-28 found 13 of 14 gates pass with wide margins, but
  gate 12 cleared only via the FCF arm (5.10% vs the 5.00% bar) at the
  2026-07-24 close of $250.09. The break-even price is $255.08 and the stock
  closed at $266.52 on 2026-07-28, so all three valuation arms fail at the
  current price. Separately, stored EBIT of $4,147m understates reported TTM
  operating income of $5,728m by 27.6% (19.2% vs 26.5% margin); the EBIT arm
  fails on either figure.
- Ranks and scores are a snapshot of the 2026-07-24 price file. Gate 12 is
  price-sensitive by construction, and several names in both tables clear it by
  under one percentage point.
