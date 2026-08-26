# Watchlist AI Buy Ranking (2026-07)

Research memo, written 2026-07-29. Question posed by the author: the [anchor
watchlist](../reference/watchlist.md) is a set of key AI-sector players; most
look too expensive, and only Adobe passes the primary QARP screen
(`screeners/quality_reasonable_price_primary.yml`). Among the names that do
**not** pass QARP, which are the better buys on **quality**, **price**, and
**future growth prospects**? A same-day follow-up added Adobe itself to the
ranking on the same framework, so the table below now orders all nine AI
names, passer included.

Scope: the nine AI names — MSFT, GOOGL, ADBE (benchmark), NVDA, AMD, PLTR,
TSLA, INTC, SK Hynix (000660.KO). **Citigroup (C.US) is deliberately
excluded**: it is a financial, not an AI player, and QARP structurally
excludes banks (the owner-earnings gates need current assets/liabilities
lines financial issuers do not report).

This is a point-in-time opinion memo, not a data-defect audit. It mixes two
data vintages, kept explicit throughout:

- **Trailing metrics**: `data/pyvalue.db` `metrics` rows, computed
  2026-07-24 (2026-07-27 for SK Hynix), prices as of those dates.
- **Forward/context data**: web research run 2026-07-29 (four parallel
  sweeps; consensus figures mostly stockanalysis.com / S&P Global unless
  noted). Every DB price was verified against exchange history — all were
  date-correct; several were 8-23% stale within days because a violent
  AI-chip correction was in progress that week (SOX >20% off its June
  peak; NVDA hit by circular-financing headlines 07-27/29; SK Hynix -23%
  in the two sessions after its 07-28 Q2 print).

## How each name fails QARP (DB gate matrix, 2026-07-24/27)

Gate-by-gate evaluation of the screen's criteria against stored metrics
(`pass`/`FAIL`/`NA`):

| Gate | ADBE | MSFT | GOOGL | NVDA | AMD | HYNIX | INTC | TSLA | PLTR |
|---|---|---|---|---|---|---|---|---|---|
| ROIC years >12% ≥ 7/10 | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | NA |
| ROIC 10Y min ≥ 7% | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | NA |
| Gross margin ≥ 35% | pass | pass | pass | pass | pass | pass | pass | FAIL | pass |
| GM 10Y std ≤ 7pp | pass | pass | pass | pass | FAIL | FAIL | FAIL | pass | NA |
| OPM 7Y min ≥ 5% | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | FAIL |
| CFO/NI TTM ≥ 0.90 | pass | pass | pass | FAIL | pass | pass | NA | pass | pass |
| CFO/NI 10Y median ≥ 0.90 | pass | pass | pass | pass | pass | pass | pass | pass | NA |
| Net debt/EBITDA ≤ 2.5x | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| Debt service (6x cov or ≤1.5x) | pass | pass | pass | pass | pass | pass | FAIL | pass | pass |
| No NI loss years (10Y) | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | NA |
| Share CAGR 5Y ≤ +1% | pass | pass | pass | pass | FAIL | pass | FAIL | pass | FAIL |
| **Price gate** (OEY/EBIT/FCF yield) | pass | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL |
| iROIC 5Y ≥ 12% | pass | pass | pass | pass | FAIL | pass | FAIL | FAIL | pass |
| Owner-earnings CAGR ≥ 7% | pass | pass | pass | pass | pass | pass | NA | NA | NA |

Three distinct failure patterns:

1. **Price-only failures — MSFT, GOOGL.** All thirteen quality gates pass;
   only the price gate fails (MSFT EV/EBIT ~18.8 vs the 15x arm; GOOGL ~28
   with FCF crushed by AI capex). "QARP quality, wrong price."
2. **Near-quality failures — NVDA, SK Hynix.** NVDA misses only TTM cash
   conversion (0.79 vs 0.90 — a Blackwell-ramp working-capital artifact;
   the 10Y median passes at 0.97) plus price. Hynix fails only
   cyclicality-shaped gates (one loss year, GM std 17.9pp, ROIC 10Y min
   -8.4%) — QARP punishing memory cyclicality by design — and missed the
   price gate at the 07-27 price by a hair (EBIT yield 6.17% vs 6.67%).
3. **Broad failures — AMD, INTC, TSLA, PLTR.** Weak/short quality history
   *and* extreme price (EV/EBIT at the 07-24 snapshot: AMD ~193, PLTR
   ~138, TSLA ~233; INTC negative EBIT).

## Selected trailing metrics (DB, 2026-07-24/27)

| Metric | ADBE | MSFT | GOOGL | NVDA | AMD | HYNIX | INTC | TSLA | PLTR |
|---|---|---|---|---|---|---|---|---|---|
| ROIC 7Y median | 34.8% | 32.2% | 27.6% | 38.2% | 5.2% | 6.6% | 0.0% | 10.3% | 9.6% |
| ROIC 10Y median | 30.4% | 28.1% | 21.2% | 39.1% | 11.6% | 12.4% | 11.3% | 7.0% | NA |
| Gross margin TTM | 89.1% | 68.3% | 60.4% | 74.2% | 50.3% | 68.3% | 35.4% | 19.1% | 84.1% |
| Operating margin TTM | 36.1% | 46.8% | 32.7% | 64.0% | 11.7% | 58.6% | -9.5% | 5.0% | 38.1% |
| FCF margin TTM | 42.2% | 22.9% | 15.3% | 47.0% | 22.9% | 30.8% | -5.8% | 7.2% | 51.4% |
| EV/EBIT | 10.0 | 18.8 | 28.0 | 30.4 | 192.9 | 16.2 | neg | 232.8 | 137.8 |
| FCF yield on EV | 11.7% | 2.6% | 1.7% | 2.4% | 1.0% | 3.2% | -0.7% | 0.6% | 1.0% |
| Trailing P/E (1/earnings yield) | 9.9 | 22.7 | 24.4 | 35.5 | 114 | 17.2 | 161 | 172 | 130 |
| Revenue CAGR 10Y | 17.4% | 11.7% | 18.3% | 45.7% | 24.1% | 17.9% | -0.5% | 37.1% | NA |
| Piotroski F | 6 | 6 | 8 | 4 | 7 | 8 | 6 | 5 | 8 |

All nine carry low/negative net debt except Intel (1.7x EBITDA); Hynix,
PLTR, TSLA, AMD, NVDA are meaningfully net cash.

## The ranking

Prices below are 2026-07-29 closes (or as dated). Consensus multiples are
next-fiscal-year unless noted.

### 1. Adobe — the screen's verdict holds: the only name where the price alone carries the thesis

$263.43 (07-29 close, ~$99-105B) after ripping **+17% over 07-27/29** on an
enterprise-software rebound and bullish calls (the DB's $225.11 was
date-correct on 07-24, near the 52-week low of $190.12). Still -29% over
twelve months, ~30% below the 52-week high ($376.16), and roughly half its
early-2024 level (~$580) — a derating driven by the gen-AI disruption
narrative, six straight quarters of organic ARR deceleration, a freemium
pivot read as defensive, and a leadership vacuum (CEO Narayen stepping down
once a successor is named, announced 03-12; CFO Durn exited abruptly 06-15).

- **Quality**: the only name passing all fourteen QARP gates, with the best
  economics in the DB set — 89% gross margin, 35% ROIC 7Y median, CROIC
  0.81, 5Y avg Greenblatt ROC ~221%, 42% FCF margin. Net share count is
  shrinking ~5.8%/yr (the DB's 13.3% shareholder yield is gross buybacks;
  SBC claws back roughly half).
- **Price**: cheapest quality asset here by a wide margin even after the
  bounce — **~10.8x FY26 / ~9.6x FY27 consensus EPS, ~11.7x EV/EBIT, ~10x
  P/FCF (~9.8% FCF yield)**. At the 07-24 snapshot it was 9.9x trailing
  earnings with an 11.7% FCF yield on EV.
- **Growth**: the weakest forward growth of the quality names, and the
  crux of the debate. Consensus: FY26 revenue +11.6% / EPS +16.6%; FY27
  +9.0% / +12.6%. Q2 FY26 (06-11) was a record with guidance *raised*
  (fifth straight beat), but organic Digital-Media ARR growth has
  decelerated six consecutive quarters to ~10.5%, and management's own
  freemium framing lowers individual-subscriber ARR expectations. Against
  that: list prices were *raised* on AI features (CC individual
  $59.99→$69.99), AI-first ARR tripled YoY to >$500M, AI-influenced ARR
  passed $5B, MAU >850M (+17%), and no seat/pricing erosion is yet
  measurable in reported numbers.

The street is split to an unusual degree — BofA Underperform $190 (07-07,
gen-AI TAM compression) and Morgan Stanley Underweight $240 (07-21) against
HSBC Buy $308 (07-02, "yet to see material impact from AI competitors") and
CLSA Outperform $300 (07-20); consensus Hold with the average target
(~$271) at the price. Ranked #1 on the composite because the math needs no
benevolence: at ~9.6x FY27 with EPS compounding low-double-digits
(buyback-assisted), the multiple merely *holding* implies high-teens annual
returns — the disruption bear case has to be **right**, not just loud, for
this to lose to the 20-24x names. It is emphatically not the
highest-certainty holding in the set (MSFT/GOOGL are); it is the highest
expected return per unit of price risk, with a real left tail if creative
pricing power structurally decays. A battleground stock — but the QARP pass
stands on the numbers, and you are paid to hold while the debate resolves.

### 2-3. Alphabet and Microsoft — "wrong price only", and the price objection has largely fixed itself

Both pass every quality gate and fail only on price. After 2026's derating
(MSFT -19% YTD, the worst mega-cap; GOOGL +8% after its 2025 run) they
trade at ~20x (MSFT $390.54, FY27 EPS cons. $19.53) and ~23-24x (GOOGL
$336.71, 2027E core EPS ~$14.68) forward earnings — unexceptional for this
quality tier.

- **GOOGL if you weight growth**: Q2 2026 (07-22) revenue +24%; Cloud
  +82% at a 36% margin with a **$514B backlog**; Gemini app ~950M MAU.
  Headline Q2 EPS $9.11 was mostly one-time investment gains (SpaceX IPO
  June 2026 — Alphabet's stake ~$94B — plus Anthropic's markup to a
  ~$965B valuation); core EPS ~$2.85 was merely in line. Hidden assets
  ~$100B+. Tail risks: capex $205B (2026) → ~$257B (2027E, FactSet), and
  the search/ad-tech antitrust remedies still under appeal (Chrome
  divestiture still sought).
- **MSFT if you weight certainty**: FY26 Q4 (reported 07-29) beat with
  **Azure +43% cc** past a $100B annual run-rate, commercial RPO **$678B
  (+84%)**, 30M+ paid Copilot seats. Risks: FY27 capex guided ~$255-260B,
  FCF already falling, OpenAI concentration (large RPO share; its losses
  flow through equity method).

Shared caveat: their thin DB FCF yields (1.7-2.6%) are the price gate
reading **peak investment**, not weak economics — the gate is doing its
job; the judgment call is whether the capex earns its keep.

### 4. NVIDIA — statistically the cheapest growth in the set; the doubt is the cycle, not the company

$190.01 (07-29), ~$4.6-4.7T, ~19% below its May ATH after circular-financing
headlines (reported ~$250B financing guarantee for a 10-GW Ohio DC
project). Q1 FY27 (05-20): revenue $81.6B +85%, data center $75.2B +92%,
Q2 guide $91B (~+95% YoY). Consensus FY27 (ends Jan 2027 — effectively
calendar 2026): revenue +82%, EPS $8.99 → **~21x current-year earnings**
(TIKR cited ~19x NTM). Claimed >$1T of combined Blackwell+Rubin orders
through 2027; Rubin ships Q3 2026. Trailing economics are the best in the
DB (ROIC TTM ~102%, CROIC 0.89). The entire bet is estimate integrity:
China DC revenue ~zero, GPU rental prices deflating (-31% in three June
weeks per TIKR), customers building TPUs, and NVIDIA increasingly financing
its own demand. Street: Strong Buy, avg PT ~$303.

### 5. SK Hynix — the other genuinely cheap stock (nominally the cheapest, but on peak-cycle earnings)

₩1,401,000 (07-29), ~$735B — **-53% from the late-June peak
(₩2,987,000), -23% in the two sessions** after Q2 (07-28): record revenue
₩79.3T (+257% YoY), operating profit ₩60.5T at a **76% margin**, but ~5-6%
below consensus with DRAM ASP momentum halving QoQ (net income further
inflated by a one-off Kioxia stake-sale gain). HBM4 mass shipments began
in Q2 with full H2 ramp, LTAs with ~10 customers, an estimated 60-70% of
Nvidia Vera Rubin HBM4 volume, and ~₩69T net cash (₩88.0T cash vs ₩18.6T
debt). At mid-July prices it screened ~5.8x forward earnings
(Investing.com); at ₩1.401M that is **~4.3x forward, ~13x trailing** — and
the QARP price gate would now *pass* (TTM EBIT yield on the shrunken EV
≈ 8% vs the 6.67% arm). Only the cyclicality gates still block it, which
is QARP working as designed. Bear case: Samsung qualified on HBM4
(~25-30% of Rubin volume; brokers see Hynix HBM share ~71% → ~55% by
end-2026), ~₩50T 2026 capex, and every prior memory cycle has punished
"cheap on peak earnings". Highest torque in both directions — a sizing
decision, not a conviction decision.

### 6. Palantir — the best business at still the worst price

$123.53 (07-28), ~$295B, -49% peak-to-trough from November 2025 on pure
multiple compression while fundamentals *accelerated*: Q1 2026 revenue
$1.63B **+85%**, US commercial +133%, adjusted operating margin 60%,
adjusted FCF margin ~57% (Rule of 40 ≈ 145), FY26 guided +71%. Defense
positioning deepening (NGC2 data layer, Maven ceiling >$1B). But even
after halving it costs **~83x FY26 / ~59x FY27 earnings, ~50x forward
FCF**, with +7%/yr dilution and sustained CEO selling. The name to
accumulate only if the AI correction turns indiscriminate. Q2 due 08-03
(a partner-spend wobble was flagged by Cleveland Research 07-28).

### 7. AMD — you can own the incumbent cheaper than the challenger

$429.56 (07-29), ~$700B, -22% in five sessions, still ~+100% YTD. The
forward story is real: OpenAI 6GW multi-generation deal (first 1GW of
MI450 in H2 2026; warrants for up to 160M shares ≈ 10% dilution vest on
milestones), Oracle 50k-GPU supercluster from Q3 2026, Meta up-to-6GW;
consensus FY26 revenue +43.5%, EPS +79%, then +58% → **~58x FY26 / ~36x
FY27**. But AI-GPU revenue today is ~$4B/quarter vs NVIDIA's ~$75B, so at
36x FY27 you pay more per unit of *hoped-for* share than NVDA charges for
*actual* share at ~21x — with execution risk against Vera Rubin and
dilution flowing to its own anchor customer. Relatively unattractive
inside this specific set. Street: Strong Buy, avg PT ~$575.

### 8. Intel — the turnaround is real; the price already pays for it twice

$81.92 (07-29), ~$406B — **-42% from the June 22 ATH ($140.94)** yet
still ~+156% YTD after the foundry-mania melt-up (Nvidia $5B stake + x86
co-development, SoftBank $2B, US government 10%, Tesla committed to 14A
for its ~$20B Austin Terafab). Q2 (07-23) was genuinely good: revenue
$16.1B **+25%** (fastest since 2011), EPS $0.42 vs $0.21 est, foundry
+31% — but **external customers are only ~5% of foundry revenue**, 18A-P
profitable yields reportedly slip toward late 2026/2027, and 2026 capex
was raised to $20B+. The DB shows the decade that preceded the story:
5Y avg Greenblatt ROC ~2.7%, iROIC -56%, shares +4.4%/yr. At **~54x FY26
/ ~42x FY27** on a street-consensus *Hold* (avg PT ~$115, dispersion
$74-200), the melt-up front-ran the proof.

### 9. Tesla — fails all three legs at once

$307.44 (07-28 close; traded below $300 intraday 07-29), ~$1.15T, -30%
YTD after a bad Q2 (07-22): revenue $28.2B +26% but adj. EPS $0.33 (-18%,
big miss), operating margin **1.4%**, auto GM 16.3%, **first FCF burn
(-$1.1B) since early 2024**, regulatory credits -67% post-EV-credit
expiry. Robotaxi paid miles *fell* ~36% QoQ during the Cybercab pivot
while Waymo runs ~500k paid rides/week toward 1M; NHTSA has 22 reported
robotaxi crashes. Energy (+40% GWh) is good but small; Optimus is far
out; brand damage is measurable (Yale/NBER: >1M lost US sales). Weakest
quality in the set (four loss years in ten, 19% gross margin), the most
expensive price (**~166x FY26 / ~138x FY27**, with estimates being cut
post-print), and the softest near-term growth (consensus FY26 revenue
+11.5%). Worst risk/reward on this framework.

## Scorecard

| Rank | Name | Quality (gates + economics) | Price (fwd P/E, 07-29) | Fwd growth (consensus) | Verdict |
|---|---|---|---|---|---|
| 1 | ADBE | **14/14 — the only full pass** | ~9.6x FY27 | Rev +11.6% → +9.0%; EPS +16.6% → +12.6% | Paid to hold while the AI-disruption debate resolves |
| 2-3 | GOOGL | 13/14 (price gate only) | ~23-24x | Rev +24%; ~20%/yr multi-yr | Most growth per multiple point; antitrust tail |
| 2-3 | MSFT | 13/14 (price gate only) | ~20x | EPS +16% | Highest-certainty compounder, decade-low relative multiple |
| 4 | NVDA | 12/14 (price + WC artifact) | ~21x FY27 (≈ CY26) | Rev +82% | Cheapest growth math — if the capex cycle holds |
| 5 | 000660 | Fails cyclicality gates only | ~4-6x | Supercycle-dependent | Genuinely cheap after -53%; classic memory-cycle risk |
| 6 | PLTR | Superb now; no 10Y history | ~59x FY27 | Rev +73% → +45% | Proven hypergrowth, unproven price |
| 7 | AMD | Weak trailing record | ~36x FY27 | EPS +79% → +58% | Challenger priced above the incumbent |
| 8 | INTC | Worst capital record in set | ~42x FY27 | EPS +261% off ~zero base | Story improved; price pays for success upfront |
| 9 | TSLA | Weakest margins; 4 loss yrs | ~138x FY27 | Rev +11.5%, estimates falling | Weak quality × extreme price × fading growth |

Confidence ordering and expected-return ordering diverge at the top:
MSFT/GOOGL are the higher-*certainty* businesses; Adobe ranks first because
at a third of their multiple its return does not depend on multiple
persistence, only on the disruption bear case being wrong. An allocator
wanting one name takes MSFT; one paid for tolerating controversy takes
ADBE; the barbell (ADBE + one platform) covers both failure modes.

## Framing correction to the premise

**"Most valuations are too high" is now mostly a trailing-data statement.**
The DB multiples were computed at the 07-24/27 closes; the AI-chip
correction then took 8-23% off NVDA, AMD, INTC, and Hynix within days. On
*forward* numbers, MSFT/GOOGL/NVDA sit at 20-24x — expensive only against
QARP's absolute-yield bar, not against their own histories — and Hynix is
outright statistically cheap. The genuinely still-too-high names are TSLA,
PLTR, AMD, and INTC. (The premise's other half also moved: Adobe, the one
QARP passer, bounced +17% off its late-June low during the research window
and still ranks first — see its entry above.)

## Data notes for the repo (observations only, no action taken)

- All DB watchlist prices were verified date-correct against exchange
  history (07-24 closes; 07-27 for 000660) — no vendor price defects this
  round. They were simply stale within days because of the correction.
- `market_cap` runs on stale share counts for PLTR (~4% low: implies
  ~2.29B shares vs ~2.4B) and ran ~6% high for C before it was dropped
  from scope (~1.78B implied vs 1.68B after buybacks) — vendor share-count
  vintage, not a calc bug. Hynix's implied ~710M vs 728.9M full count is
  within tolerance (treasury shares).
- MSFT FY26 Q4 (reported 07-29) and GOOGL Q2 2026 (07-22) were not yet in
  `financial_facts` at metric compute time — TTM metrics lag one
  just-reported quarter, as expected.
- NVDA's TTM cash-conversion gate failure (0.79) is a documented
  working-capital artifact of the Blackwell ramp, not an earnings-quality
  red flag (10Y median 0.97 passes). No screen change proposed — the 10Y
  median gate already provides the through-cycle check.

## Addendum 2026-08-07 — Cloudflare (NET.US) joined the watchlist, not ranked here

`NET.US` was added to the [anchor watchlist](../reference/watchlist.md) on
2026-08-07, **after** this memo's 2026-07-29 research vintage. It is deliberately
**not** ranked above, and the nine-name tables have not been reopened — the
consensus figures, prices, and forward estimates in this memo are point-in-time
and cannot be extended to a tenth name without re-running the whole sweep.

For the record, Cloudflare would not have placed on this memo's framework in any
case. It fails QARP on 12 of 14 gates, and unlike every name ranked here it has
**never** posted a profitable GAAP fiscal year (FY2016 −$17.3M through FY2025
−$102.3M), which makes the quality leg of the ranking — built on ROIC
persistence, margin floors, and owner-earnings compounding — inapplicable rather
than merely weak. Its two genuine strengths (73.3% gross margin, 1.9pp of
ten-year GM variation) are pricing-power evidence, not evidence of returns on
capital. See the
[QARP profile](../reference/watchlist.md#cloudflare-netus-qarp-profile-refreshed-2026-08-11)
for the gate-by-gate detail.

A fresh ranking that includes Cloudflare would need its own memo and its own
web-research sweep.

## Addendum 2026-08-11 — every DB table above is superseded

**All stored-metric figures in this memo — the gate matrix and the trailing
metrics table — are stale.** They were computed on mixed 2026-07-06/07-24
vintages against a debt basis that has since been rebuilt three times:

- **2026-08-10** (`d5d8b34`, `d2e600c`, `c24aad2`): debt is now the *broadest
  measured* reading of the two the provider supplies, so finance leases are in;
  and normalization no longer files non-current liabilities under
  `LongTermDebt`.
- **2026-08-11** (`71b259d`, `ce92a3f`): a period carrying no debt row no
  longer voids invested capital, and the ROIC FY series — which had kept a
  sixth, private copy of the retired components-preferred debt rule — was
  routed through the shared one. **Every ROIC figure below moved.**

The nine names were recomputed in the full-universe run of 2026-08-11 against
the 2026-08-07 close (2026-07-27 for SK Hynix). The **narrative ranking and all
forward/consensus figures stay at their 2026-07-29 research vintage** — they
were sourced from the web that day and cannot be half-updated without
contradicting themselves. Read the tables below as the current data and the
prose above as a point-in-time opinion.

### Gate matrix, recomputed 2026-08-11

| Gate | ADBE | MSFT | GOOGL | NVDA | AMD | HYNIX | INTC | TSLA | PLTR |
|---|---|---|---|---|---|---|---|---|---|
| ROIC years >12% ≥ 7/10 | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | NA |
| ROIC 10Y min ≥ 7% | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | NA |
| Gross margin ≥ 35% | pass | pass | pass | pass | pass | pass | pass | FAIL | pass |
| GM 10Y std ≤ 7pp | pass | pass | pass | pass | FAIL | FAIL | FAIL | pass | NA |
| OPM 7Y min ≥ 5% | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | FAIL |
| CFO/NI TTM ≥ 0.90 | pass | pass | **FAIL** | FAIL | pass | pass | NA | pass | pass |
| CFO/NI 10Y median ≥ 0.90 | pass | pass | pass | pass | pass | pass | pass | pass | NA |
| Net debt/EBITDA ≤ 2.5x | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| Debt service (6x cov or ≤1.5x) | pass | pass | pass | pass | pass | pass | FAIL | pass | pass |
| No NI loss years (10Y) | pass | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | NA |
| Share CAGR 5Y ≤ +1% | pass | pass | pass | pass | FAIL | pass | FAIL | FAIL | FAIL |
| **Price gate** (OEY/EBIT/FCF yield) | **pass** | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL |
| iROIC 5Y ≥ 12% | pass | pass | pass | pass | FAIL | pass | FAIL | FAIL | pass |
| Owner earnings CAGR ≥ 7% | pass | pass | pass | pass | pass | pass | NA | NA | NA |

**Adobe still passes 14 of 14** and remains the only watchlist name that does.
MSFT is still 13/14 on the price gate alone. Two gates changed sign against the
July matrix:

- **GOOGL now fails CFO/NI TTM (0.76 vs 0.90).** Not a debt effect — Q2 2026's
  headline net income was inflated by one-time investment gains (the SpaceX IPO
  markup and Anthropic's revaluation, both discussed above), so cash conversion
  falls mechanically. The 10Y median still passes at 1.54. GOOGL is now 12/14.
- **TSLA now fails the share-count gate** at +1.07%/yr against a +1.00% bar,
  where the July matrix had it passing.

### Selected trailing metrics, recomputed 2026-08-11

Prices are the 2026-08-07 close; SK Hynix is 2026-07-27.

| Metric | ADBE | MSFT | GOOGL | NVDA | AMD | HYNIX | INTC | TSLA | PLTR |
|---|---|---|---|---|---|---|---|---|---|
| Price | $265.21 | $499.99 | $354.30 | $223.96 | $483.36 | ₩1,816,000 | $101.65 | $328.58 | $172.01 |
| ROIC 7Y median | 33.7% | 32.2% | 26.4% | 37.0% | 5.1% | 6.6% | 0.0% | 9.6% | 9.6% |
| ROIC 10Y median | 29.6% | 28.9% | 20.2% | 38.5% | 11.6% | 12.4% | 11.3% | 6.7% | NA |
| Gross margin TTM | 89.1% | 67.9% | 60.9% | 74.2% | 53.2% | 68.3% | 38.9% | 18.9% | 84.8% |
| Net debt / EBITDA | 0.15x | **+0.27x** | −0.75x | −0.41x | −0.93x | −0.38x | 1.67x | −3.15x | −3.45x |
| EV/EBIT | 11.8 | 24.3 | 28.7 | 33.0 | 120.3 | 16.2 | 6840 | 289 | 147 |
| FCF yield on EV | 9.9% | 1.8% | 1.3% | 2.2% | 1.1% | 3.2% | 0.5% | 0.5% | 0.9% |
| Owner-earnings yield on EV | 5.5% | 2.3% | 1.7% | 0.5% | 0.3% | 0.5% | −0.2% | 0.5% | 0.0% |
| iROIC 5Y | 1.00 (cap) | 18.3% | 33.0% | 79.0% | 3.0% | 53.7% | −56.3% | 2.9% | 41.0% |
| Piotroski F | 6 | 6 | 8 | 4 | 7 | 8 | 6 | 5 | 8 |

**The sentence "all nine carry low/negative net debt except Intel" above is no
longer true.** Microsoft is now **net debt +0.27x EBITDA**, not net cash: the
July figure omitted $78.8B of finance-lease obligations. The other eight remain
net cash, Intel excepted.

Two other movements worth naming: **Intel's EBIT has crossed from negative to
marginally positive**, so `ev_to_ebit` is now a finite but meaningless 6,840x
rather than NA; and **AMD's EV/EBIT fell from 193 to 120** on earnings growth
rather than on price (the stock rose from $429.56 to $483.36 over the window).

MSFT's own review is separate and current: see
[MSFT QARP Review (2026-08)](msft-qarp-review-2026-08.md) for the
maximum-entry-price analysis and the finance-lease defect this memo's original
EV figures carried.

## Addendum 2026-08-26 — Broadcom (AVGO.US) joins the watchlist, assessed on this framework

`AVGO.US` (listing 54870) was added to the
[anchor watchlist](../reference/watchlist.md) on 2026-08-26 — the twelfth name,
and the piece of the AI trade the original nine covered only by proxy: custom
AI accelerators (XPUs) and the Ethernet networking that stitches the clusters
together, plus a ~$28B-run-rate infrastructure-software annuity (VMware).
Unlike the Cloudflare addendum above, this one **does** assess the newcomer on
the memo's quality/price/growth framework. The July tables are still not
reopened; two data vintages are mixed below, kept explicit:

- **Trailing/gate data**: the stored 2026-08-11 full-universe metric run — the
  same vintage as the recomputed tables in the 2026-08-11 addendum, so those
  columns are directly comparable. Fundamentals through Q2 FY2026 (quarter
  ended 2026-05-03); price the 2026-08-07 close ($427.76, market cap $2.035T).
- **Forward/context data**: web research run 2026-08-26. Broadcom closed
  2026-08-25 at **$356.74 (~$1.70T)** — down ~17% from the DB snapshot in the
  August AI repricing and ~28% below its 52-week high ($495.00) — so the DB's
  price-linked multiples overstate today's price by roughly that margin. The
  other nine names' forward figures sit at their 2026-07-29 vintage and are
  **not** directly comparable to Broadcom's.

### The QARP verdict: fails, 6 of 14 — a fourth failure pattern

Gate-by-gate detail with values is in the watchlist's
[Broadcom QARP profile](../reference/watchlist.md#broadcom-avgous-qarp-profile-2026-08-26).
The memo's taxonomy above has three failure patterns — price-only (MSFT,
GOOGL), near-quality (NVDA, Hynix), broad (AMD, INTC, TSLA, PLTR). Broadcom is
none of them. Call the fourth **the serial-acquirer decade**: every quality
failure traces to purchase accounting rather than operating economics. The one
loss year (FY2016, −$1.74B) is the Avago–Broadcom merger-charge year; ROIC
clears 12% in only 4 of 10 years (10Y min −1.3%) because NOPAT is measured
against a goodwill-swollen ~$130B average invested capital; the 8.0pp of GM
"instability" is a monotonic *climb* from 44.9% (FY2016) to 67.8% (FY2025) as
the mix shifted to software and deal amortisation rolled off; the +2.5%/yr
share drift is VMware's part-stock consideration plus SBC. Meanwhile the cash
gates pass emphatically — CFO/NI 10Y median 2.04, owner-earnings CAGR 23.9%,
iROIC 18.3%, net debt 1.09x EBITDA thirty months after the $69B half-cash
VMware close — all fourteen criteria are computable (no other non-passer here
manages that), and ROIC TTM is 24.3%: the acquired capital is earning through.
The record is scar tissue, not weakness, and QARP declines to look past it by
design. The price gate, though, fails on current economics, not history:
EV/EBIT 63x and a 1.6% FCF yield on EV at the 08-07 price (≈53x / ≈1.9% at
08-25).

### Selected trailing metrics (2026-08-11 vintage, comparable to the table above)

| Metric | AVGO |
|---|---|
| Price | $427.76 |
| ROIC 7Y median | 12.4% |
| ROIC 10Y median | 9.4% |
| Gross margin TTM | 67.0% |
| Net debt / EBITDA | 1.09x |
| EV/EBIT | 63.1 |
| FCF yield on EV | 1.6% |
| Owner-earnings yield on EV | 0.9% |
| iROIC 5Y | 18.3% |
| Piotroski F | 7 |

Rows the gate matrix hides: operating margin TTM **43.7%**, FCF margin TTM
**43.4%**, revenue TTM ~$75.5B (+32%), revenue CAGR 10Y 25.1%, Altman Z 14.6.
On TTM margins this sits with NVIDIA at the top of the watchlist — run on a
balance sheet whose tangible book is ≈ −$38B, which is Hock Tan's acquisition
model working as intended.

### The forward story (researched 2026-08-26): the largest committed AI backlog outside Nvidia

Q2 FY2026 (ended 05-03, reported 06-03): revenue **$22.2B +48%**, adjusted
EBITDA $15.2B (**69% of revenue**), FCF $10.3B (46%), GAAP net income $9.3B;
semiconductors $15.0B / infrastructure software $7.2B. **AI semiconductor
revenue $10.8B, +143% YoY, against >$30B of AI bookings taken in the
quarter.** Guidance: Q3 revenue ~$29.4B (+84%) with AI at ~$16.0B (+200%+);
FY2026 AI revenue ~$56B (+~180% on FY2025's ~$20B); FY2027 AI revenue **"in
excess of $100B" reiterated** — six XPU customers, ~10GW of visibility: Google
(Ironwood TPU), Meta (MTIA), OpenAI, Anthropic (1GW ramping 2026, 3+GW
expected 2027), and two unnamed. Consensus (stockanalysis.com / S&P Global,
Aug 2026) underwrites the guide essentially in full — FY2026 revenue $106.1B
(+66%) / EPS $11.63, FY2027 revenue $173.4B (+63%) / EPS $19.50 — so at
$356.74 the stock stands at **~30.7x FY26E and ~18.3x FY27E**. Street: Strong
Buy (37 of 49 ratings), average target $526 (median $527.50, range $216–675);
TD Cowen reiterated Buy/$500 in late August.

The cracks the August tape traded on: the **OpenAI ramp hit an $18B financing
standoff** (reported 05-07: Broadcom will finance initial production only if
Microsoft commits to ~40% of output, and Microsoft and OpenAI are deadlocked
over data-center design; Tan says the other five ramps cover nine of the ten
gigawatts). BofA's $370B AI-debt note knocked the stock −6% on 08-14 — the
customers debt-financing the capex **are** Broadcom's backlog, so the
circular-financing worry that hit NVDA in July attaches here too. AI must
supply ~72% of Q3's sequential revenue growth; custom XPUs carry lower gross
margin than networking, so adjusted-EBITDA margin is guided down a point
(69% → ~68%) and the street frets about mix. The franchises funding the
patience carry their own risks: VMware's perpetual-to-subscription conversion
(reported 5–10x price increases, a CISPE antitrust complaint in Brussels,
churn risk as multi-year renewals come due) and China/BIS export exposure.
Above all, **six customers**, two of which (Google, Meta) could cancel a
roadmap with a single in-housing decision. Q3 FY2026 prints **2026-09-02**,
days after this addendum.

### Where it slots: 4–5, beside NVIDIA — the same bet in backlog form

At ~18.3x FY2027 consensus Broadcom optically undercuts even NVDA's ~21x from
July, but the periods differ: NVDA's multiple was on its current fiscal year
(≈ calendar 2026), where Broadcom stands at ~30.7x FY26E — it is the *extra*
year of +64% consensus growth that closes the gap, and that consensus **is**
the guide. Granting it in full, today's ~$1.75T EV is ≈15x FY27E adjusted
EBITDA: the price gate cleared only by prepaying the entire >$100B ramp before
it ships. QARP's trailing lens (63x EV/EBIT) and the forward lens disagree by
4x — the widest such gap on the watchlist. So the placement mirrors NVIDIA's,
one notch back on certainty-per-gate and one notch forward on contract cover:
**alongside NVDA at 4–5**. Prefer NVDA for the cleaner decade (12/14 gates,
ROIC 10Y median 38.5%) and merchant-market upside; prefer AVGO for multi-year
design-win lock-in, six buyers instead of a market, and the software annuity —
accepting concentration risk NVDA does not carry and a GAAP record QARP will
keep failing for years. Decisively ahead of AMD (a challenger at ~36x FY27
hoping for share Broadcom already holds under contract) and PLTR (~59x FY27);
behind ADBE and the platform pair on certainty per dollar. Against Hynix at #5
it is the opposite wager — Hynix cheap on earnings that may halve, AVGO dear
on earnings contracted to double — and which mis-prices worse is the cycle
call this memo does not make. Like every name here except Adobe, it is a
watchlist stock, not a QARP pass: the screen's verdict stands.

## Method appendix

Web research was run 2026-07-29 as four parallel sweeps (mega-cap
platforms; semis; TSLA/PLTR; ADBE), each cross-checking price/market cap
against at least two sources and pulling latest-quarter actuals, consensus
forward estimates (stockanalysis.com / S&P Global, TipRanks, MarketBeat),
catalysts, risks, and street views, with per-fact source dates. Notable
primary sources: company 8-Ks/press releases (MSFT FY26 Q4, GOOGL Q2,
TSLA Q2, INTC Q2, Citi Q2, ADBE Q2 FY26, SK Hynix Q2 newsroom), CNBC,
Electrek/TechCrunch (robotaxi), Investing.com (Hynix history/forward
P/E), TradingKey, Motley Fool, Futurum. Consensus figures are
point-in-time and were moving intraday during a violent correction week —
treat every multiple in this memo as ±5% within its own day.
