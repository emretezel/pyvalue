# SYMBOL — Company Name

Position note. Framework, metric rationale, and decision rules live in
[README.md](README.md); this file holds only what is specific to this holding.

Copy this file to `<symbol-lowercased-with-dashes>.md`, fill it in on the day the
position is opened, then never edit the entry baseline again.

| | |
|---|---|
| Canonical symbol | `SYMBOL.XX` |
| `listing_id` | |
| Exchange / currency | |
| Opened | YYYY-MM-DD |
| Cost basis | **$0.00** |
| Screen at entry | `screeners/<screen>.yml` — PASS n/n |
| Fiscal year end | |
| Next update due | after Q_ FY__ reports (~month year) |

## Thesis

Three or four sentences. What the business is, why it is high quality in numbers, what
was paid, and what the market appears to be worried about that you think is wrong.

**What has to be true:** the two or three business facts the thesis depends on, and
where they would show up first if they stopped being true.

## Entry baseline — frozen, never edited

Price data as of YYYY-MM-DD ($0.00 close); fundamentals as of Q_ FY__ (YYYY-MM-DD).

**Business size:** market cap $0.00B · EV $0.00B · net debt $0.00B · revenue $0.00B ·
EBITDA $0.00B · EBIT $0.00B · FCF $0.00B · shares outstanding 0.0M

| Per share (TTM) | Value | Yield on cost ($0.00) | Yield on price ($0.00) |
|---|---|---|---|
| Free cash flow | $0.00 | **0.00%** | 0.00% |
| Owner earnings | $0.00 | **0.00%** | 0.00% |
| Earnings (EPS) | $0.00 | **0.00%** | 0.00% |
| Dividend | $0.00 | **0.00%** | 0.00% |
| Shareholder yield (div + net buyback) | — | **0.00%** | 0.00% |

Per-share figures derive from the latest price and one stored ratio — see
[README.md](README.md#deriving-the-per-share-figures).

**Valuation at entry:** FCF yield on EV 0.00% · owner-earnings yield on EV 0.00%
(normalised 0.00%) · EBIT yield on EV 0.00% · EV/EBIT 0.00x · EV/EBITDA 0.00x ·
P/E 0.00x on cost · P/FCF 0.00x on cost · P/B 0.00x · PEG 0.00

## Quality tripwires

Set the breach column **tighter than the screen threshold** — the position was bought
for quality, not for barely clearing a bar. Mark the one or two tripwires that carry
the thesis.

| Tripwire | At entry | Breach below/above | Screen threshold |
|---|---|---|---|
| ROIC (TTM) | | | — |
| ROIC minimum (10Y) | | | ≥ 7% |
| ROIC years above 12% | | | ≥ 7 |
| Gross margin (TTM) | | | ≥ 35% |
| Gross margin 10Y std | | | ≤ 7pp |
| Operating margin min (7Y) | | | ≥ 5% |
| Cash conversion CFO/NI (TTM) | | | ≥ 0.90 |
| Cash conversion median (10Y) | | | ≥ 0.90 |
| Net debt / EBITDA | | | ≤ 2.5x |
| Interest coverage | | | ≥ 6x |
| Share count CAGR (5Y) | | | ≤ +1% |
| Owner earnings CAGR (10Y) | | | ≥ 7% |
| Incremental ROIC (5Y) | | | ≥ 0.12 |

## Accounting-quality watch list

| Metric | At entry | Reading |
|---|---|---|
| Accruals ratio | | |
| SBC / FCF | | |
| SBC / revenue | | |
| Piotroski F-score | | / 9 |
| Altman Z | | |
| Current ratio | | |
| Debt paydown years | | |

**Buyback efficiency.** Net buyback yield __% against a share count moving from __M to
__M (__%). Efficiency = __%. Below ~50% the buyback is offsetting dilution rather than
shrinking the base.

Share count trend (`CommonStockSharesOutstanding`, quarter-end): __M → __M → __M.

## Manual inputs to collect each quarter

Tailor to the business model. Defaults worth keeping:

- Backlog / deferred revenue / RPO growth, or the sector equivalent
- Net revenue retention or churn
- Segment growth
- Guidance: raised / held / cut, and the reason
- Insider open-market purchases
- Debt maturities inside 24 months
- Named competitive threats and what changed this quarter

## Sell triggers

**Sell on valuation** (thesis intact, price extreme)
- `fcf_yield_ev` below __%, or `ev_to_ebit` above __x.
- Trim rather than exit while the compounding rate still exceeds __%.

**Sell on thesis** (price irrelevant)
- [thesis-carrying tripwire] breached for two consecutive quarters.
- Cash conversion below __ for two consecutive quarters.
- Share count rising year over year, or buyback efficiency below 50%.
- FCF per share below $__ (the entry level) for four consecutive quarters.
- Net debt / EBITDA above __x other than for a disclosed, value-creating acquisition.

**Add**
- `fcf_yield_ev` at or above __% with every tripwire intact.

## Quarterly log

| Date | Fiscal Q | Price | FCF/sh | FCF yld on cost | FCF yld on price | OE/sh | OE yld on cost | Shareholder yld | Shares (M) | EV/EBIT | Screen | Tripwires | Action |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| YYYY-MM-DD | Q_ FY__ (entry) | | | | | | | | | | | 0 breached | **Bought at $0.00** |

### Notes by quarter

**YYYY-MM-DD — Position opened.** What was bought, at what price relative to the market,
which screen legs cleared and by how much, and the one thing that would change the mind.
