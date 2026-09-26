# Watchlist Buy Ranking

**Living snapshot — maintained in place.** This document is the current,
complete picture of the [anchor watchlist](../reference/watchlist.md) ranked
for buy-attractiveness. On each refresh it is rewritten top to bottom — the
tables, the ranking, and the prose all move to the new data together, and
nothing is kept as an addendum. Prior versions live in git history: this
file's own log holds past snapshots, and the original 2026-07-29 nine-name
memo with its three addendums sits under the old path
`docs/research/watchlist-ai-buy-ranking-2026-07.md`, ending at commit
`52a5699`.

**Snapshot date: 2026-08-28 (Apple inserted 2026-09-26).**

The standing question, first posed by the author in July 2026: across the
watchlist, which names are the better buys on **quality**, **price**, and
**future growth prospects** — anchored by what the primary QARP screen
(`screeners/quality_reasonable_price_primary.yml`) says about each?

Scope: **all thirteen watchlist names**, including the two the July memo
excluded or deferred and Apple, which joined the watchlist on 2026-09-26.
Citigroup is a financial, not an AI name; QARP
structurally cannot screen banks (the owner-earnings gates need current
assets/liabilities lines financial issuers do not report), so it is ranked on
bank-appropriate lenses and its corporate-shaped table rows carry caveats.
Cloudflare has never posted a GAAP-profitable fiscal year, which makes the
quality leg inapplicable rather than weak; it is ranked with that stated.

Two data vintages, kept explicit throughout:

- **Trailing/QARP data**: the stored `data/pyvalue.db` metrics from the
  **2026-08-11 full-universe run** (facts refreshed 2026-08-10, so every name
  carries its July/early-August print except the three noted below). Prices
  are the **2026-08-07 close** — **2026-07-27 for SK Hynix**, whose 07-28 Q2
  print and subsequent crash are *not* in the DB. NVDA's 08-26 Q2 FY27 print
  post-dates the fact refresh (stored facts end at Q1 FY27), and Cloudflare's
  08-06 Q2 is only fragmentarily stored (2 concepts), so its TTM metrics are
  effectively through Q1 2026.
- **Forward/context data**: web research run **2026-08-28** (four parallel
  sweeps; consensus figures mostly stockanalysis.com / S&P Global unless
  noted). Prices are the **2026-08-27 US close**; SK Hynix is the
  **2026-08-28 Seoul close** (₩1,653,000).
- **Apple (AAPL.US)**, inserted 2026-09-26: its trailing rows come from the
  same 2026-08-11 run (facts through Q3 FY2026, price the 2026-08-07 close
  of $313.33), but its forward layer is a **2026-09-26 sweep at the
  2026-09-25 close ($341.07)**. The other twelve forward layers were left at
  2026-08-28 rather than refreshed; where the month's drift matters for the
  comparison it is said so.

## QARP gate matrix (stored metrics, 2026-08-11 run)

Gate-by-gate evaluation of the screen's 14 criteria against stored metrics
(`pass`/`FAIL`/`NA`; an NA is a fail for the screen but shown distinctly).
Columns are in ranking order.

| Gate | ADBE | GOOGL | MSFT | NVDA | AVGO | HYNIX | AAPL | C | PLTR | AMD | INTC | NET | TSLA |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ROIC years >12% ≥ 7/10 | pass | pass | pass | pass | FAIL | FAIL | pass | FAIL | NA | FAIL | FAIL | NA | FAIL |
| ROIC 10Y min ≥ 7% | pass | pass | pass | pass | FAIL | FAIL | pass | FAIL | NA | FAIL | FAIL | NA | FAIL |
| Gross margin ≥ 35% | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | FAIL |
| GM 10Y std ≤ 7pp | pass | pass | pass | pass | FAIL | FAIL | pass | FAIL | NA | FAIL | FAIL | pass | pass |
| OPM 7Y min ≥ 5% | pass | pass | pass | pass | pass | FAIL | pass | pass | FAIL | FAIL | FAIL | FAIL | FAIL |
| CFO/NI TTM ≥ 0.90 | pass | FAIL | pass | FAIL | pass | pass | pass | FAIL | pass | pass | NA | NA | pass |
| CFO/NI 10Y median ≥ 0.90 | pass | pass | pass | pass | pass | pass | pass | FAIL | NA | pass | pass | NA | pass |
| Net debt/EBITDA ≤ 2.5x | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | NA | pass |
| Debt service (6x cov or ≤1.5x) | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | FAIL | FAIL | pass |
| No NI loss years (10Y) | pass | pass | pass | pass | FAIL | FAIL | pass | FAIL | NA | FAIL | FAIL | FAIL | FAIL |
| Share CAGR 5Y ≤ +1% | pass | pass | pass | pass | FAIL | pass | pass | pass | FAIL | FAIL | FAIL | FAIL | FAIL |
| **Price gate** (OEY/EBIT/FCF yield) | pass | FAIL | FAIL | FAIL | FAIL | FAIL | FAIL | pass | FAIL | FAIL | FAIL | FAIL | FAIL |
| iROIC 5Y ≥ 12% | pass | pass | pass | pass | pass | pass | pass | FAIL | pass | FAIL | FAIL | FAIL | FAIL |
| Owner-earnings CAGR ≥ 7% | pass | pass | pass | pass | pass | pass | pass | NA | NA | pass | NA | NA | NA |
| **Gates passed** | **14/14** | 12/14 | 13/14 | 12/14 | 8/14 | 8/14 | 13/14 | 6/14 | 5/14 | 6/14 | 3/14 | 2/14 | 5/14 |

**Adobe passes 14 of 14 and remains the only QARP pass on the watchlist.**
The only other name clearing the price gate is Citigroup, on a 9.4% trailing
EBIT yield — a number whose EV construction means little for a bank (see its
entry). Apple, inserted from the same run, is 13/14 on the price gate alone —
the MSFT shape, every arm roughly half the bar, with all fourteen criteria
computable and every input reconciled to its SEC filings (see its
[profile](../reference/watchlist.md#apple-aaplus-qarp-profile-2026-09-26)).
NVDA's TTM cash-conversion miss (0.79) is the documented Blackwell-ramp
working-capital artifact (10Y median 0.97 passes); GOOGL's (0.76) is the
one-time SpaceX/Anthropic investment gains inflating net income (10Y median
1.54 passes). Matrix verified against `pyvalue report-screen-failures` on the
same data: 1/13 full passers, and all fourteen per-criterion fallout counts
reconcile.

### Five failure patterns

1. **Price-only — MSFT and AAPL, and effectively GOOGL and NVDA.** MSFT and
   AAPL fail the price gate alone. GOOGL and NVDA each add one TTM
   cash-conversion artifact (above) that their 10-year medians contradict;
   quality is not in question.
2. **Cyclicality-shaped — SK Hynix.** Every failure is a memory-cycle gate
   (one loss year, GM std 17.9pp, ROIC 10Y min −8.4%) plus a price gate
   measured at a pre-crash price. QARP punishing cyclicality by design.
3. **The serial-acquirer decade — Broadcom.** All fourteen criteria are
   computable (no other non-passer manages that) and the cash gates pass
   emphatically (CFO/NI 10Y median 2.04, owner-earnings CAGR 23.9%); every
   failure traces to purchase accounting — the FY2016 merger-charge loss
   year, ROIC measured against goodwill-swollen invested capital, a GM
   "instability" that is a monotonic climb from 44.9% to 67.8%. Scar tissue,
   not weakness; QARP declines to look past it by design. Detail in the
   [Broadcom profile](../reference/watchlist.md#broadcom-avgous-qarp-profile-2026-08-26).
4. **Broad failures — AMD, INTC, TSLA, PLTR.** Weak or short quality history
   *and* an extreme price.
5. **Structurally outside the framework — C, NET.** Citigroup: the
   owner-earnings chain is NA for financials by design, and its computed
   corporate rows (CFO/NI negative on bank cash-flow noise, "gross margin",
   EV multiples) are not economically meaningful for a balance-sheet
   business. Cloudflare: ten GAAP loss years in ten make the
   profitability-based guards refuse to produce numbers — correctly (detail
   in its [profile](../reference/watchlist.md#cloudflare-netus-qarp-profile-refreshed-2026-08-11)).

## Selected trailing metrics (stored metrics, 2026-08-11 run)

Prices are the 2026-08-07 close (SK Hynix 2026-07-27 — pre-crash).

| Metric | ADBE | GOOGL | MSFT | NVDA | AVGO | HYNIX | AAPL | C | PLTR | AMD | INTC | NET | TSLA |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Price (DB) | $265.21 | $354.30 | $499.99 | $223.96 | $427.76 | ₩1,816,000 | $313.33 | $135.00 | $172.01 | $483.36 | $101.65 | $300.27 | $328.58 |
| Market cap (DB) | $105.4B | $4.36T | $3.71T | $5.42T | $2.04T | ₩1,289T | $4.57T | $226.4B | $395.7B | $789.1B | $512.7B | $96.4B | $1.30T |
| ROIC TTM | 53.9% | 22.4% | 25.6% | 99.8% | 24.3% | 49.2% | 90.3% | 4.3% | 39.4% | 8.8% | 0.0% | NA | 4.2% |
| ROIC 7Y median | 33.7% | 26.4% | 32.2% | 37.0% | 12.4% | 6.6% | 64.6% | 2.9% | NA | 5.1% | 0.0% | −7.1% | 9.6% |
| ROIC 10Y median | 29.6% | 20.2% | 28.9% | 38.5% | 9.4% | 12.4% | 51.8% | 3.6% | NA | 11.6% | 11.3% | NA | 6.7% |
| Gross margin TTM | 89.1% | 60.9% | 67.9% | 74.1% | 67.0% | 68.3% | 48.7% | 54.6% | 84.8% | 53.2% | 38.9% | 73.3% | 18.9% |
| Operating margin TTM | 36.1% | 33.1% | 46.8% | 64.0% | 43.7% | 58.6% | 33.2% | 16.1% | 42.8% | 15.7% | 0.1% | −9.3% | 4.2% |
| FCF margin TTM | 42.2% | 11.9% | 20.2% | 47.0% | 43.4% | 30.8% | 29.3% | −23.1% | 54.6% | 20.3% | 5.0% | 15.3% | 5.6% |
| Revenue CAGR 10Y | 17.4% | 18.3% | 13.8% | 45.7% | 25.1% | 17.9% | 5.9% | 6.7% | NA | 24.1% | −0.5% | NA | 37.1% |
| Net debt / EBITDA | 0.15x | −0.75x | 0.27x | −0.41x | 1.09x | −0.38x | 0.13x⁴ | 1.29x | −3.45x | −0.93x | 1.67x | NA | −3.15x |
| EV/EBIT | 11.8 | 28.6 | 24.3 | 33.0 | 63.1 | 16.2 | 29.7 | 10.6 | 147 | 120 | 6840 | NA | 289 |
| Trailing P/E¹ | 11.6 | 17.8² | 27.9 | 38.3 | 52.6 | 17.2 | 36.4 | 13.9 | 143 | 87.2 | 105 | 448 | 189 |
| FCF yield on EV | 9.9% | 1.3% | 1.8% | 2.2% | 1.6% | 3.2% | 3.0% | −13.5% | 0.9% | 1.1% | 0.5% | 0.4% | 0.5% |
| Owner-earnings yield on EV | 5.4% | 1.7% | 2.3% | 0.5% | 0.9% | 0.5% | 2.1% | 3.4% | 0.0% | 0.3% | −0.2% | −0.1% | 0.4% |
| iROIC 5Y | 1.00 (cap) | 33.0% | 18.3% | 79.0% | 18.3% | 53.7% | 533%³ | 1.5% | 41.0% | 3.0% | −56.3% | −2.6% | 2.9% |
| Share count CAGR 5Y | −3.5% | −2.0% | −0.3% | −0.7% | +2.5% | +0.7% | −2.5% | −3.5% | +6.3% | +6.1% | +4.6% | +2.9% | +1.1% |
| Piotroski F | 6 | 8 | 6 | 4 | 7 | 8 | 8 | 6 | 8 | 7 | 6 | 4 | 5 |

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

MSFT is net debt +0.27x EBITDA (finance leases in, since the 2026-08-10 debt
rebuild), not net cash, and AAPL reads +0.13x on the cash scope in note ⁴;
every other name except INTC (1.67x), AVGO (1.09x) and C is net cash. C's FCF
rows and EV multiples are bank-noise — read its entry, not this table.

## The ranking

Prices below are 2026-08-27 closes (SK Hynix 2026-08-28 Seoul close; Apple
the 2026-09-25 close of its own sweep date). Consensus multiples are computed
at those prices; fiscal-year labels are per company. The confidence ordering and the expected-return ordering still
diverge at the top: GOOGL/MSFT are the higher-*certainty* businesses; Adobe
ranks first because at less than half their multiple its return does not
depend on multiple persistence, only on the disruption bear case being wrong.

### 1. Adobe — still the only name where the price alone carries the thesis

$289.15 (+5.7% on 08-27 in the Salesforce-led software relief rally; +9.8%
since late July, +28% off the late-June $190 low, still −22% off the 52-week
high). Market cap $114.9B. **Now a held position** — bought at $262 on
2026-08-07 ([portfolio notes](../portfolio/adbe-us.md)).

- **Quality**: 14/14 QARP gates, the best economics in the set at its size —
  89% gross margin, 34% ROIC 7Y median, 42% FCF margin, shares shrinking
  3.5%/yr net.
- **Price**: ~**11.8x FY26 / 10.5x FY27** consensus EPS ($24.41 / $27.49) —
  the cheapest large-cap software multiple anywhere in the group. At the
  08-27 price the price gate still passes on the DB's own basis: trailing
  EBIT yield ≈7.8% and FCF yield ≈9.1% against the 6.67%/5% arms (the
  normalised owner-earnings arm sits right at ~5.0%); the EBIT arm alone
  holds to roughly $340.
- **Growth**: the weakest forward growth of the quality names and the crux:
  FY26 revenue +11.6% / FY27 +9.0%. Q2 FY26 (06-11) was a record
  beat-and-raise (FY26 EPS guide $24.35–24.45), AI-first ARR tripled YoY to
  >$500M, but management deliberately traded near-term ARR for the freemium
  funnel and deferred H2 pricing actions — Q3 net-new ARR is the
  battleground.
- **Overhangs**: no CEO successor named as of 08-28 (Narayen leaves once one
  is appointed); the CFO seat is interim (Durn left 06-15 for Marvell). The
  street is a **Hold with the average target ($271) below the price** —
  ratings drifted down all year while insiders and BlackRock (>10% 13G,
  08-27) bought. **Q3 FY26 prints 2026-09-10** — the near catalyst for the
  ARR, AI, and succession debates.

The math still needs no benevolence: at ~10.5x FY27 with EPS compounding
low-double-digits buyback-assisted, a merely *held* multiple implies
high-teens annual returns. The bear case has to be right — not just loud —
for this to lose to the 23–26x platforms. Highest expected return per unit
of price risk on the list; emphatically not the highest-certainty holding.

### 2. Alphabet — the most growth per multiple point, now with a capex bill on display

$340.65, ~$4.16T, −16.6% off the May high. **~23.0x FY27** consensus EPS
($14.81; the headline FY26 consensus of $20.59 is inflated by ~$99B of
pre-tax SpaceX/Anthropic marks and is not a valuation basis). Q2 2026
(07-22): revenue $119.8B **+24%**, Cloud **+82% at a 35.6% operating
margin**, backlog **$514B**, Gemini app 950M MAU, Search +17%. The cost:
2026 capex raised to $195–205B (2027 street ~$257B), the **first negative
FCF quarter since the 2004 IPO (−$5.9B)**, and **zero buybacks for two
straight quarters**. The 12/14 gate count is an artifact (one-time gains
crush CFO/NI TTM; the 10Y median is 1.54). Antitrust tail: search remedies
are behavioral (Chrome divestiture rejected) but DOJ's cross-appeal keeps
divestiture alive into 2027, and the ad-tech remedies ruling is still
pending. Berkshire raised its stake 83% in Q2 to ~$37B — now its #3 holding.
Ranked above MSFT on price-for-growth: a turn cheaper on FY27, growing
faster, with ~$100B+ of hidden assets — against a fatter legal tail and the
ugliest near-term cash statement of the platforms.

### 3. Microsoft — the certainty compounder, no longer at a gift price

$505.06, $3.75T — the July window closed violently: **+15.5% on 07-30**
after the FY26 Q4 print (reported as the largest single-day value gain in
market history), now −8.8% off the 52-week high. **~25.6x FY27** consensus
EPS ($19.75). The print: revenue $90.0B +18%, **Azure +43% cc** (FY26 Azure
crossed $100B), commercial RPO **$678B +84%** (swollen by OpenAI's $250B
Azure commitment), 30M+ paid Copilot seats, Q1 FY27 Azure guided +45% cc.
The costs: FY26 FCF fell ~6.5%, Q1 FY27 capex guided >$50B and rising — and
a correction to the July memo: **no FY27 capex dollar guide was given**; the
"$255–260B" figure was a street preview, not company guidance. OpenAI is
now disclosed at $24.1B of FY26 revenue — concentration is a live debate.
13/14 gates, price only. Avg PT $569. The highest-certainty compounder
here; at 25.6x it is a fair price for it, not a bargain — the June trough
(~$350) was the QARP-shaped entry and it is gone.

### 4. NVIDIA — the estimates keep proving themselves; the multiple stays reasonable

$227.98 (+8.7% on 08-27, the first post-earnings pop in a year of day-after
dips), $5.51T, −3.6% off the high. Q2 FY2027 (08-26): revenue **$96.2B
+106%** (DC $89.0B, 92.5% of revenue), GM 75%, **Q3 guide $108B** (+4%
above street), and the headline — **FY2028 pre-guided at ~70% revenue
growth, described as supply-constrained**, with Rubin ramping into full
production. **~24.6x FY27** consensus EPS ($9.27; FY ends Jan-2027 ≈
calendar 2026) and **~19x implied FY28** — for a name growing ~80% with 12/14
gates (the TTM cash-conversion miss is the documented working-capital
artifact) and a $99B buyback authorization. The bear case is unchanged in
kind: circular financing (Ohio credit support, now capped at $105B for
~4.25GW, down from the ~$250B explored), zero China in guidance (a $400M
H20/H200 write-down shows the stranded cost; any resumption is upside), Q3
GM guided down ~100bp on memory-cost inflation, and hyperscaler capex
dependence. Statistically still the cheapest hypergrowth in the set; the
doubt remains the cycle, not the company. Avg PT ~$323 post a wave of hikes
(street high $515).

### 5. Broadcom — the same bet as NVIDIA in backlog form, priced tighter than July

$371.54, $1.77T — a −17% August drawdown from the DB snapshot (BofA's
$370B AI-SPV-debt note on 08-14, then Marvell's expanded Google deal on
08-19 read as second-sourcing in the decade-old TPU franchise), then +4.5%
on NVDA's print. **~31.9x FY26 / ~19.1x FY27** consensus ($11.63 / $19.50 —
consensus essentially underwrites the guide in full). **Q3 FY26 prints
2026-09-02**: guided $29.4B (+84%) with AI revenue ~$16.0B; FY26 AI ~$56B
and **FY27 AI "in excess of $100B" reiterated**. The OpenAI financing
standoff resolved in late June (the "Jalapeño" inference ASIC unveiled
~06-24, first deployments by end-2026, Microsoft the largest taker of the
first run). QARP failure pattern is the serial-acquirer decade (8/14, all
computable, cash gates emphatic); the price gate fails on current economics
(EV/EBIT 63x trailing at the DB price). Slotting stays beside NVDA, one
notch back: prefer NVDA for the cleaner decade and merchant upside, AVGO
for six contracted buyers and the software annuity — accepting customer
concentration (two of six could in-house away), the SPV financing question
(AVGO's own backstop ≤ ~$29B), VMware's pending EC complaint, and a GAAP
record QARP will keep failing for years. The 09-02 print, days after this
snapshot, is both the near catalyst and the near risk.

### 6. SK Hynix — the cheap outlier, and the value-trap objection just lost its main leg

₩1,653,000 (08-28 Seoul close; five ≥9% daily moves since late July, still
−45% off the June peak), ~$870–915B. **~4.7x FY26 / ~3.6x FY27** consensus
KRW EPS (₩350,682 / ₩462,810, 44 analysts) with **₩69.4T net cash**. Q2
(07-28): revenue ₩79.3T +257%, operating profit ₩60.5T at a **76% margin**
— and a ~5.6% revenue miss with DRAM ASP momentum halving QoQ, which
crashed the stock 23% in two sessions. What changed since: **a ₩40T (~$29B)
buyback-and-cancellation — the largest treasury cancellation by any Korean
listed company — plus a policy of returning >50% of cumulative 2025–27 FCF**
(announced 08-19; Goldman sizes potential returns at ~₩140T). "Cheap on
peak earnings with no capital return" was the classic memory-cycle bear
case; half of it just died. The other half sharpened: Nvidia's Rubin Ultra
HBM spec cut (~08-06 reports) showed a single customer decision can vaporize
HBM content, Samsung *and* Micron are now HBM4-qualified (SKH holds an
estimated 60–70% of Rubin volume), and the board approved ~$38B of new
fabs. At the 08-28 price the trailing EBIT yield on the DB's own (through
Q1-2026) basis is ≈6.8% — the price arm would clear again — while the
cyclicality gates keep failing by design. Highest torque in both
directions; a sizing decision, not a conviction decision. Q3 prints 10-29.

### 7. Apple — the other 13-of-14, at the dearest multiple per growth point on the list

$341.07 (2026-09-25, a record close; +8.9% since the DB's 08-07 price, +8.4%
since this snapshot's 08-27 price date, +40% off the January low of $243),
~$4.98T. **Now run by John Ternus** — CEO since 09-01, Tim Cook Executive
Chair — with the first foldable iPhone shipping in October.

- **Quality**: 13/14 QARP gates, tied with MSFT as the best non-passer, with
  every criterion computable and every input reconciled to SEC filings. Ten
  of ten ROIC years above 12% (worst year 21.8%), a decade of gross margin
  inside a 3.3pp band while climbing from 39% to 47%, operating cash flow
  never below 0.99x net income in any of the last ten years (median 1.26x),
  and a share count shrinking 2.5%/yr on $82B of trailing buybacks. The highest-certainty cash
  machine on the list alongside MSFT — and, unlike MSFT, one that is not
  converting itself into a capital-heavy business: capex is ~2% of revenue.
- **Price**: the sole failing gate, and it fails all three arms by roughly
  half — trailing EBIT yield 3.4%, FCF yield 3.0%, normalised owner-earnings
  yield 2.1% at the DB price ($313), each ~8% lower again at $341
  (3.1% / 2.7% / 2.0%). **~38.6x FY26 / ~35.6x FY27** consensus EPS ($8.83 /
  $9.59) — ten multiple points above MSFT, twelve above GOOGL, for a fraction
  of their growth. The street average target ($328–340) sits at or below the
  price; the rating is a "Moderate Buy" with sells from Jefferies ($264) and
  Neutrals from UBS ($296) and Moffett ($304) against $380–400 from Evercore,
  Loop, TD Cowen and Wedbush.
- **Growth**: the crux and the weak leg. FY27 consensus is **revenue
  +10.5%, EPS +8.6%**, after an FY26 (+14.8% / +18.4%) flattered by tariff
  refunds and a strong iPhone 17 cycle. The Q3 FY26 print (07-30) was a June
  record on every line — revenue $109.4B **+16.4%**, iPhone +21.7%, Greater
  China +22.4%, Services +12.1%, EPS $2.02 +29% (≈$0.11 of it IEEPA-refund)
  — and the stock still fell **7.4% the next day** on an FQ4 guide of +9–11%
  (street ~12%), gross margin guided down to 47–48%, "very significant"
  advanced-node supply constraints and rising memory costs. The 09-09 event
  brought the $1,199 iPhone 18 Pro (+$100) and the **first foldable, iPhone
  Duo at $1,999–3,199** (ships 10-23); the early read-through is mixed —
  shorter Pro lead times and one sell-side build cut in week one, lengthening
  lead times in week two. iOS 27 with the Gemini-based Siri AI shipped 09-14:
  Apple's AI story is distribution and pricing power, not a capex race.
- **Overhangs**: Section 301 forced-labor tariffs since 07-24 (10% India /
  12.5% China-Vietnam) with the smartphone exemption unresolved; the DOJ
  antitrust case in early settlement talks; the Google search remedies in
  force but under cross-appeal, with the ~$20B+/yr default payment continuing
  for now; Epic at the Supreme Court (argument early 2027); the EU DMA front
  calmed on 08-18. Berkshire held its 227.9M shares through Q2. **FQ4 FY26
  prints ~10-29** (date not yet confirmed by Apple).

The math is the mirror image of Adobe's: at ~35.6x with EPS compounding
high-single-digits buyback-assisted, a merely *held* multiple returns ~9% a
year, and any drift toward MSFT's 25x erases three years of it. Nothing about
the business has to go wrong for this to be a mediocre buy; the multiple has
to stay generous. It ranks below Hynix, whose price arm passes, and above
Citigroup on the framework anchor alone — on expected return the bank probably
edges it, and the prose says so. The QARP-shaped price is far away: the
loosest arm (5% FCF yield) clears at ~$186 and the EBIT arm at ~$158 on
trailing numbers, below even the January low.

### 8. Citigroup — nothing to do with AI, everything to do with getting paid

$132.68, $222.6B, −10% off the high — the calm one: no August day moved
>3.5%. The framework caveat first: QARP cannot screen banks (owner-earnings
chain NA by design), its computed corporate rows are noise (CFO/NI negative
on trading/loan flows, "FCF yield" −13.5%), and its price-gate pass (9.4%
trailing EBIT yield) rests on an EV construction that means little for a
balance-sheet business. On bank lenses the picture is genuinely good: Q2
(07-14) was a **record revenue quarter ($24.8B, +14%)**, EPS $3.15 beat by
15%, **ROTCE 13.0%** (H1 13.1%), efficiency 57.4% from 62.7%, CET1 12.8%.
Price: **1.32x tangible book** (TBVPS $100.89), **~11.9x FY26 / ~10.1x
FY27** consensus EPS ($11.15 / ~$13.13), a 2.0% dividend just raised 12%,
and $26B left of the $30B buyback — the share count shrinks 3.5%/yr, the
best trend on the watchlist alongside Adobe's. Growth is the modest leg:
management held the FY26 ROTCE guide at 10–11% despite the 13% H1 (flagging
~$5B of accelerated 2H investment — the print-day −5.3%), targets 11–13%
for 2027-28 and 14–15% only by 2029-31. The trade-off it offers is unique
here: a low-teens expected annual return (earnings yield + payout + book
compounding) with the lowest narrative risk on the list — and the least
upside if the AI trade keeps working. Banamex is 49% sold with the IPO
still open. Avg PT $154.50. Next print 10-13.

### 9. Palantir — the wobble thesis died on contact; the price question didn't

$185.93 (+51% off the July low, back within 10% of the November ATH),
$446.8B. Q2 2026 (08-03) was, in CNBC's word, "otherworldly": revenue
$1.94B **+93%** (accelerating from +85%), US commercial **+149%**, adjusted
operating margin 62%, adjusted FCF margin 63%, **Rule of 40 ≈ 155**, FY26
guided to **+82%** ($8.15B), GAAP profitable every quarter — the
Cleveland-Research partner-spend wobble simply did not materialize, and the
stock rose 29.5% the next day. Defense positioning deepened (NGC2 next
phase, up to $244M directed through March 2027). The screen still can't
reach it (5/14 — no ten-year history, OPM floor broken by the SBC-heavy
years, +6.3%/yr trailing dilution though the pace has slowed to +1.8% YoY)
and the price leg is worse than July, not better: **~116x FY26 / ~80x FY27**
consensus EPS, ~53x FY26E EV/revenue, with the **street's average target
($192) sitting at the price**. Karp's programmatic selling continues
(~$86M on 08-20). Best-run hypergrowth business on the list; still the
worst corporate price. The name to accumulate only when the AI tape turns
indiscriminate — July 30 at $122 was exactly that moment, and it lasted
four sessions.

### 10. AMD — the challenger still costs about double the incumbent

$476.67, $778B, −18% off the July intraday high, +123% YTD. The forward
story keeps thickening: **~14GW of stage commitments** (OpenAI 6GW — first
1GW of MI450 lands H2 2026, warrants ≈10% dilution; Meta 6GW; Anthropic up
to 2GW alongside AMD investing up to $5B in Anthropic — its own turn at
circular financing), Oracle deploying 50k MI450s, Helios racks shipping
September, Q2 (08-04) revenue +50% with Data Center +107%. But gross margin
sat at 56% (vs NVIDIA's 75%) and the stock fell 7% on the print; NVDA's
"supply-constrained ~70% FY28" guide then reframed the share-shift story —
AMD dipped on NVIDIA's blowout day. At **~63x FY26 / ~43x FY27** consensus
EPS ($7.57 / ~$11) you still pay roughly twice NVIDIA's current-year
multiple for share the incumbent says it cannot supply anyway — with
execution risk against Rubin, 6.1%/yr trailing dilution, and 5/14
quality-history gates. Street span is the widest here: avg PT ~$554–614,
high $1,250 (Baird), low $235. Q3 prints ~11-03.

### 11. Intel — the turnaround is more real; the price still pays for it twice

$92.09, ~$485B, −35% off the June high yet +150% YTD, beta 2.24. The proof
advanced: Q2 (07-23) revenue $16.1B **+25%** (fastest since 2011), non-GAAP
EPS $0.42 vs $0.21, non-GAAP GM 41.8%, DCAI +59%; 18A yields reportedly
jumped ~65%→>85% in a quarter; Hot Chips (08-25) showed Diamond Rapids
built *and packaged* entirely in-house on 18A-P. (The −$11B GAAP quarter is
the mark-to-market on the government's escrowed shares — an artifact of the
stock's own rally.) What did not advance: **external foundry revenue is
still ~$293M/quarter (~5% of foundry)** with no marquee external customer
signed (14A decisions expected 2H26–1H27), and the equity keeps paying
upfront — a **$20B share sale at $95** (210.5M shares) pushed the count to
5.25B, **+21% YoY**, on top of the government/SoftBank/Nvidia stakes. 3/14
gates — the worst capital record in the set (iROIC −56%) — at **~61x FY26 /
~45x FY27** consensus EPS ($1.51 / $2.04) on a street that is a **Hold**
(avg PT $114.88, range $75–200). The story needs the 14A/18A external wins
to arrive before the next $20B of dilution does.

### 12. Cloudflare — the business accelerates; the framework still can't reach the price

$308.23 (+8.2% on NVDA day; an all-time high $332.22 on 08-13, −16% AI-tape
drawdown after it), $109.8B. Q2 (08-06) was its best in years: revenue
$696M **+36% — accelerating**, the largest beat in 18 quarters, DBNR 120%
(+6pts YoY), >$100k customers +27%, FCF +69% YoY (8% margin), FY26 guided
up to ~$2.87B. The AI-tollbooth option is unique: publishers now send >1B
HTTP-402 pay-per-crawl responses daily and the x402 wallet/pay marketplace
went live in August — unquantified revenue today, real strategic
positioning. None of that reaches the framework: still no GAAP-profitable
fiscal year (FY26 is locked lossy by the ~$165M restructuring of the May
"agentic AI-first" 20% workforce cut; the ex-charge Q2 loss was only ~$19M,
so underlying breakeven is near, but no timeline is guided), 2/14 gates,
and a price of **~245x FY26 / ~185x FY27 non-GAAP EPS** (~38x FY26E
EV/revenue) that already prices the option as won. Avg PT $334. The growth
is the point; the price is the problem — a QARP framework has nothing to
buy here at any gate.

### 13. Tesla — still failing all three legs at once

$354.81 (+19% off the July trough on Nevada robotaxi permits 08-21 and the
09-03 Cybercab event; −29% off the high), $1.40T. The Q2 print (07-22)
deepened every quality concern: operating margin **1.4%**, adjusted EPS
$0.33 vs $0.53 (−39% miss), **first FCF burn since early 2024 (−$1.1B)**,
regulatory credits −67%, gross margin 16.8% — while robotaxi paid miles
*fell* ~36% QoQ during the Cybercab pivot (cumulative driverless miles
~380k vs Waymo's 220M+ and 500k paid trips/week). Energy is genuinely good
(13.5 GWh, +41%) and small. The price leg got worse two ways: estimates
keep falling (FY26 consensus EPS $1.77 → **~200x**) and the share count
absorbed the 304M-share Musk award (~8% dilution, April), with the ~$1T
2025 package still accruing — the DB's +1.1%/yr 5Y share CAGR now fails
even QARP's loose bar. Q3 deliveries face the credit-expiry-inflated 497k
comp. Weakest quality in the set (4 loss years, 18.9% GM), the most
extreme price, the softest near-term growth: the ranking is arithmetic,
not a verdict on the robotaxi dream. Avg PT $390, span $125–600. Next
print ~10-28.

## Scorecard

| Rank | Name | QARP (2026-08-11 data) | Price (fwd P/E at 08-27/28) | Fwd growth (consensus) | Verdict |
|---|---|---|---|---|---|
| 1 | ADBE | **14/14 — the only pass** | ~11.8x FY26 / 10.5x FY27 | Rev +11.6% → +9.0% | Paid to hold while the AI-disruption debate resolves; held position |
| 2 | GOOGL | 12/14 (price + gains artifact) | ~23.0x FY27 core | Rev +24%; Cloud +82% | Most growth per multiple point; capex/FCF and antitrust tails |
| 3 | MSFT | 13/14 (price gate only) | ~25.6x FY27 | Rev +18%; EPS +14% | Highest certainty; the June gift price is gone |
| 4 | NVDA | 12/14 (price + WC artifact) | ~24.6x FY27 (≈CY26), ~19x FY28 | Rev ~+80%; FY28 guided ~+70% | Cheapest hypergrowth math — if the capex cycle holds |
| 5 | AVGO | 8/14 (serial-acquirer decade) | ~31.9x FY26 / 19.1x FY27 | AI rev $56B → >$100B guided | NVDA's bet in backlog form; 09-02 print is the test |
| 6 | 000660 | 8/14 (cyclicality gates) | **~4.7x FY26 / 3.6x FY27** | Rev +260% → +51% | Cheapest by far; ₩40T buyback kills half the bear case |
| 7 | AAPL | 13/14 (price gate only) | ~38.6x FY26 / 35.6x FY27 (at 09-25) | Rev +10.5%; EPS +8.6% (FY27) | Highest-certainty franchise at the dearest multiple per growth point; nothing for the framework to buy at $341 |
| 8 | C | Structurally outside (bank) | ~11.9x FY26 / 10.1x FY27, 1.32x TB | EPS +48% → +18%; ROTCE 13% | Low-teens return, lowest narrative risk; the anti-AI ballast |
| 9 | PLTR | 5/14 (no 10Y history) | ~116x FY26 / 80x FY27 | Rev +82% → +49% | Best-run hypergrowth; worst corporate price; buy panics only |
| 10 | AMD | 6/14 (weak trailing record) | ~63x FY26 / 43x FY27 | Rev +47%; EPS +81% | Challenger at ~2x the incumbent's multiple |
| 11 | INTC | 3/14 (worst capital record) | ~61x FY26 / 45x FY27 | Rev +19% → +13% | Turnaround real, unsigned; dilution machine meanwhile |
| 12 | NET | 2/14 (pre-profit) | ~245x FY26 / 185x FY27 (non-GAAP) | Rev +32% → +29% | Accelerating tollbooth option, priced as already won |
| 13 | TSLA | 5/14 (weakest margins) | ~200x FY26, estimates falling | Rev ~+11%; EPS shrinking | Fails quality, price, and growth simultaneously |

## What the snapshot says overall

**Quality and price coexist in exactly one name (plus one bank).** Adobe
still passes everything at the 08-27 price with headroom; Citigroup is
cheap on bank lenses but is a different asset class in this list. The
platform pair re-rated hard off the June-July trough — MSFT's +15.5% day
closed the window QARP had been pointing at — and now trade at 23–26x
next-year earnings: fair for the quality, no longer mispriced. Apple joins
MSFT in the quality-without-price camp — 13/14, the price gate the only
failure — but at ~36x next-year earnings for high-single-digit growth it is
the dearest multiple per growth point in the group, and the framework has
nothing to buy at a record high. NVDA (and
AVGO in backlog form) keep converting the AI-capex thesis into printed
numbers at ~19–25x forward, which is the group's honest growth-at-a-price
zone. Hynix is the statistical outlier — ~3.6x next-year consensus with
net cash and, newly, the largest capital-return program in Korean history —
if you can carry memory-cycle risk. Everything else pays upfront for
success not yet in the numbers: PLTR and NET for flawless compounding, AMD
for share gains against a supply-constrained incumbent, INTC for foundry
customers not yet signed, TSLA for a robotaxi future its current P&L
actively contradicts.

An allocator wanting one name still takes MSFT; one paid for tolerating
controversy holds ADBE (as the author now does); the barbell — ADBE plus
one platform — still covers both failure modes, and C is the off-thesis
ballast for whoever wants the AI exposure diluted.

## Data notes for the repo (observations only, no action taken)

- **SK Hynix rows are doubly stale**: the DB price (₩1,816,000, 07-27)
  predates the post-Q2 crash — 9.9% above the 08-28 Seoul close — and the
  07-28 Q2 print (revenue ₩79.3T, OP ₩60.5T) is not yet in
  `financial_facts` (latest stored quarter Q1 2026). Its trailing table
  column overstates the price and understates TTM earnings simultaneously.
- **NVDA's 08-26 Q2 FY27 print post-dates the 2026-08-10 fact refresh**
  (stored facts end Q1 FY27); AVGO's Q3 FY26 prints 09-02. Their TTM
  metrics lag one just-reported/imminent quarter, as expected.
- **Cloudflare's Q2 2026 is only fragmentarily stored** (2 concepts vs ~39
  for a full quarter), so its TTM metrics effectively run through Q1 2026
  despite the 08-06 print falling before the fact refresh.
- **Stale metric rows exist and must be filtered**: a `metrics` row whose
  latest `metric_compute_status` attempt is `failure` is a leftover from an
  earlier run — the screening path ignores it, and ad-hoc SQL must join
  `metric_compute_status` on `status = 'success'` to agree with the screen.
  Among watchlist rows: NET's debt ratios (a 2025-12-31 vintage, including
  the `net_debt_to_ebitda` −6.49 its watchlist profile still quotes in
  prose) and PLTR's `roic_7y_median` 9.6%, which prior versions of this
  memo's tables carried as live. Both are shown as NA above.
- **Share-count vintages**: implied DB share counts are within ~1% of
  reported counts for nine names; PLTR runs ~4% low (~2.30B implied vs
  ~2.40B) and NET ~10% low (~321M implied vs ~356M) — heavy-SBC issuance
  outrunning the vendor's vintage, not a calc bug.
- **Correction to the superseded July memo**: MSFT's "FY27 capex guided
  ~$255–260B" was a street preview circulating as guidance; the company
  gave only a qualitative "grows year-over-year" plus Q1 FY27 capex >$50B.
- **Apple's column mixes two vintages by design**: trailing rows from the
  2026-08-11 run at the 08-07 close ($313.33), forward layer from a
  2026-09-26 sweep at $341.07. The other twelve forward layers were not
  refreshed on insertion.
- **Apple's net debt (+0.13x EBITDA) is a cash-scope reading, not a
  balance-sheet fact**: pyvalue's cash chain stops at current marketable
  securities ($62.4B) and no `LongTermInvestments` concept is stored, so the
  $84.1B of non-current securities are invisible to EV and net debt. On the
  company's own basis Apple is $62.2B net cash; EV is 1.8% overstated and
  every price arm ~2% too low. No gate moves. Whether the cash chain should
  read long-term marketable securities is a design question for the
  metrics layer, not fixed here.
- **Apple's iROIC 5Y (533%) is a vendor-consistency artifact**: EODHD's
  FY2025 `TotalDebtFromBalanceSheet` folds ~$10.9B of lease liabilities into
  the rollup while its FY2020 rollup carries none, so the broader-reading
  rule sees +$10.4B of invested capital over five years where a
  lease-consistent basis sees −$3.3B (which would emit the 1.0 cap). Same
  root as `roic_7y_median` reading 64.6% against 66.7% on SEC data.
- **Apple's vendor EPS and interest rows**: EODHD's Q3 FY26 EPS is $1.91
  against a filed diluted $2.02 (`eps_ttm` $8.61 vs $8.71); `InterestExpense`
  carries placeholder rows (FY2024 = 0, Q2-2025 = $1M) because Apple stopped
  disclosing the line after FY2023, and `interest_coverage` correctly emits
  the 100x cap on the debt ≤ 1x EBIT evidence (0.54x). Neither reaches a gate.
- **Apple's QARP inputs reconcile to SEC filings**: every criterion was
  recomputed from XBRL company facts at the DB cut-off and matches to the
  decimal (table in its watchlist profile); the one non-QARP NA,
  `price_to_ncav`, is structural (negative NCAV by design).
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
`pyvalue report-screen-failures` on the same thirteen symbols: the 1/13
full-pass count and all fourteen per-criterion fallout counts reconcile.
Apple's inputs were additionally recomputed from its SEC XBRL company facts
(10-K FY2013–FY2025, 10-Qs through the June 2026 quarter) at the DB
cut-off, using pyvalue's own definitions, and reconcile to the decimal.

**Web layer**: four parallel sweeps run 2026-08-28 (platforms
MSFT/GOOGL/ADBE; AI semis NVDA/AMD/AVGO; cyclical/turnaround/financial
INTC/000660/C; high-multiple growth TSLA/PLTR/NET), plus a fifth on Apple
run 2026-09-26 under the same protocol, each cross-checking prices against
at least two independent sources and pulling latest-quarter actuals,
consensus forward estimates (stockanalysis.com / S&P Global, TipRanks,
MarketBeat, Zacks), catalysts, risks, and street views with per-fact source
dates. Notable primary sources: company releases/filings (NVDA Q2 FY27, PLTR
Q2, NET Q2, SK Hynix Q2 newsroom + ₩40T buyback release, Citi Q2, MSFT FY26
Q4 + 10-K OpenAI disclosures, Broadcom Q2 FY26, Apple Q3 FY26 release +
10-Q + the Ternus 8-K), CNBC, Bloomberg/KED (Korea), Electrek/TechCrunch
(robotaxi), TrendForce (memory pricing). Facts resting on a single source
are flagged in the sweeps' archives and were either corroborated or hedged
in the prose above. Consensus figures move intraday; treat every multiple in
this document as ±5% within its own day.
