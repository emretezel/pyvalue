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
