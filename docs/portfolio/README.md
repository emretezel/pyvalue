# Portfolio Position Notes

One note per holding, updated once a quarter after the company reports. Each note
exists to answer three questions, in this order:

1. **Is the thesis intact?** Is the business still the quality compounder that was
   bought, judged by the QARP criteria that justified the purchase?
2. **Is it still worth owning at today's price?** Cheap, fairly priced, or expensive
   enough to sell?
3. **Is the investment actually working?** Independent of the share price — is cash
   flow per share growing?

A note is not a research report. It is a scoreboard with pre-committed thresholds,
written when judgement was calm so it can be applied when it is not.

## Layout

```
docs/portfolio/
  README.md                   this file -- the shared framework
  position-note-template.md   blank note, copy for each new holding
  adbe-us.md                  one file per position, named after the canonical symbol
```

Name each file after the canonical symbol, lowercased, with `.` replaced by `-`
(`ADBE.US` → `adbe-us.md`). That keeps the file discoverable from any screen output
and sorts sensibly as positions accumulate.

## Open positions

| Position | Symbol | listing_id | Opened | Cost basis | Note |
|---|---|---|---|---|---|
| Adobe Inc. | ADBE.US | 53407 | 2026-08-07 | $262.00 | [adbe-us.md](adbe-us.md) |

## The central idea: two yields, one denominator each

Every cash-flow yield in these notes is computed twice, on two different denominators.
The difference between them is the most informative number in the note.

**Yield on cost** freezes the denominator at the purchase price forever. It therefore
moves *only when the business moves*. FCF per share up 12% → FCF yield on cost up 12%.
It is a scoreboard for the **business**, immune to market mood, and it is what
compounding actually feels like from the inside.

**Yield on latest price** floats with the market. It is what a new buyer would be
offered today, so it is the **buy / hold / sell** signal — including for the marginal
dollar of your own money.

Read them together:

| | Yield on cost **rising** (business compounding) | Yield on cost **falling** (business shrinking) |
|---|---|---|
| **Yield on price ≥ entry level** | Compounding *and* still cheap — **add** | Cheap for a reason — **investigate, lean sell** |
| **Yield on price well below entry** | Compounding but re-rated upward — **hold**, trim only at an extreme | Expensive *and* deteriorating — **sell** |

The bottom-left cell is the value trap; the top-right is the one most investors sell
too early. The gap between the two yields is pure multiple re-rating: paper gains that
the business has not yet earned. Track it explicitly.

### Deriving the per-share figures

All three per-share numbers fall out of the latest price and one stored ratio, so no
share count is needed:

| Figure | Formula | Yield on cost |
|---|---|---|
| FCF per share | `latest_price / price_to_fcf` | `FCF_per_share / cost_basis` |
| Owner earnings per share | `latest_price * oey_equity` | `OE_per_share / cost_basis` |
| EPS (TTM) | `eps_ttm` (stored directly) | `eps_ttm / cost_basis` |
| Dividend per share | `latest_price * dividend_yield_ttm` | `DPS / cost_basis` |

`dividend_yield_ttm` is a measured `0` for evidenced non-payers, not missing data —
so income yield on cost is a real zero for a company that has never paid, and the row
stays in the table so the day it initiates a dividend is visible.

## What each tracked metric is doing there

### A. Compounding — is the investment working?

The business scoreboard. These are the numbers that should grind upward regardless of
what the share price does.

| Metric | Why it earns a row |
|---|---|
| FCF per share, and its yield on cost | The single best per-share compounding measure; cash, not accrual |
| Owner earnings per share, yield on cost | Buffett's figure: earnings after *maintenance* capex only, so growth spending is not penalised |
| EPS (TTM), yield on cost | Accounting anchor; the gap vs FCF/share is the accrual story |
| Dividend per share, income yield on cost | Cash actually received; the only fully unambiguous return |
| `net_buyback_yield` | For a non-payer this *is* the capital return channel |
| `shareholder_yield_ttm` | Dividend + net buyback: total capital returned |
| Share count (absolute) | The claim you own; buybacks only count if shares actually fall |
| **Buyback efficiency** (derived) | `share count reduction % ÷ net_buyback_yield %`. Below ~60% means much of the buyback is plugging stock-comp dilution rather than shrinking the share base — a leak that a headline buyback yield hides |
| `owner_earnings_cagr_10y`, `fcf_per_share_cagr_10y` | Long-run compounding rate, the thing being bought |

### B. Valuation — what is the market offering now?

| Metric | Why it earns a row |
|---|---|
| `fcf_yield_ev` | Primary valuation gauge; EV-based so leverage cannot flatter it |
| `oey_ev_norm` | Owner-earnings yield on a 5Y-median numerator — guards against buying peak earnings |
| `ebit_yield_ev` / `ev_to_ebit` | Greenblatt's lens; comparable across capital structures |
| `earnings_yield`, `price_to_fcf`, `price_to_book` | Conventional cross-checks |
| `peg_ratio` | Growth-adjusted; the sanity check on paying up for a compounder |
| **Yield spread vs entry** (derived) | Yield on cost minus yield on price: how much re-rating has happened since purchase |

### C. Quality tripwires — is the thesis intact?

These are the QARP criteria that justified the buy. Each has a **breach level** set at
purchase. A breach is not an automatic sell; two consecutive quarters of breach is a
mandatory re-underwrite.

`roic_ttm`, `roic_10y_min`, `gross_margin_ttm`, `gm_10y_std`, `opm_7y_min`,
`cfo_to_ni_ttm`, `cfo_to_ni_10y_median`, `net_debt_to_ebitda`, `interest_coverage`,
`share_count_cagr_5y`, `owner_earnings_cagr_10y`, `iroic_5y`.

Running the screen itself each quarter is the cheapest version of this check — it
prints every criterion with its value.

### D. Accounting quality — the quiet warnings

Not in the screen, but this is where deterioration shows up first.

| Metric | What a bad reading means |
|---|---|
| `accruals_ratio` | Rising / positive = earnings drifting ahead of cash (Sloan). Negative is good |
| `sbc_to_fcf`, `sbc_to_revenue` | Cash generation partly funded by paying staff in stock; large values make "FCF" flattering |
| `piotroski_f_score` | Nine-point fundamental health; a multi-point fall is a warning even from a high base |
| `altman_z` | Distress score; irrelevant until it is not |
| `current_ratio`, `working_capital` | Structurally negative for subscription businesses (deferred revenue) — read the *trend*, not the level |
| `debt_paydown_years` | Years of FCF to clear debt; the honest leverage measure |
| `mcapex_ttm` vs total capex | Maintenance vs growth split; rising maintenance share means the moat is getting expensive to hold |

### E. Manual inputs the database cannot supply

A short list, but these often lead the reported numbers by a quarter or more. Fill them
in from the earnings release.

- **Remaining performance obligations / deferred revenue growth** — for a subscription
  business, the best forward indicator there is
- **Net revenue retention** — expansion within the existing base, the moat in one number
- **Segment revenue growth** — where growth is actually coming from
- **Guidance change** — raised, held, or cut, and management's stated reason
- **Insider buying / selling** — open-market purchases only; sales are noise
- **Debt maturity wall** — what refinances in the next 24 months, and at what rate
- **One-line narrative** — what changed in the competitive position this quarter

## Decision rules

Written in advance, applied mechanically. Position-specific numbers live in each note;
these are the defaults.

**Sell on valuation** — thesis intact, price extreme:
- `fcf_yield_ev` below 4% (EV/FCF above 25x), **or** `ev_to_ebit` above 25x,
- and the compounding rate no longer justifies the multiple (`owner_earnings_cagr_10y`
  below the earnings yield you are giving up).

**Sell on thesis** — price irrelevant:
- Two or more tripwires breached for two consecutive quarters, **or**
- FCF per share lower than at entry for four consecutive quarters, **or**
- Any single tripwire breached by a wide margin (e.g. cash conversion below 0.7,
  net debt/EBITDA above 3.5x).

**Add**:
- `fcf_yield_ev` at or above the entry level, all tripwires intact, position below its
  target portfolio weight.

**Hold** — everything else. The default, and the correct answer most quarters.

## Quarterly refresh

Run after the company reports, then update the note's log table.

```bash
# 1. Latest price (0-day window forces a refresh even if today's snapshot exists)
pyvalue update-market-data --symbols ADBE.US --max-age-days 0

# 2. New filing -> raw payload -> facts -> metrics
pyvalue ingest-fundamentals --symbols ADBE.US --max-age-days 0
pyvalue normalize-fundamentals --symbols ADBE.US
pyvalue compute-metrics --symbols ADBE.US

# 3. Re-run the screen that justified the purchase
pyvalue run-screen --config screeners/quality_reasonable_price_primary.yml --symbols ADBE.US
```

Then pull the tracked metrics in one read (substitute the position's `listing_id`):

```sql
SELECT metric_id, ROUND(value, 4) AS value, as_of
FROM metrics
WHERE listing_id = 53407
  AND metric_id IN (
    'fcf_yield_ev', 'oey_ev', 'oey_ev_norm', 'ebit_yield_ev', 'earnings_yield',
    'ev_to_ebit', 'ev_to_ebitda', 'price_to_fcf', 'price_to_book', 'peg_ratio',
    'eps_ttm', 'market_cap',
    'roic_ttm', 'roic_10y_min', 'gross_margin_ttm', 'gm_10y_std', 'opm_7y_min',
    'cfo_to_ni_ttm', 'cfo_to_ni_10y_median', 'net_debt_to_ebitda',
    'interest_coverage', 'share_count_cagr_5y', 'owner_earnings_cagr_10y', 'iroic_5y',
    'dividend_yield_ttm', 'net_buyback_yield', 'shareholder_yield_ttm',
    'accruals_ratio', 'sbc_to_fcf', 'sbc_to_revenue', 'piotroski_f_score', 'altman_z',
    'current_ratio', 'debt_paydown_years', 'mcapex_ttm', 'fcf_per_share_cagr_10y'
  )
ORDER BY metric_id;
```

Share count actually outstanding, for the buyback-efficiency check:

```sql
SELECT fiscal_period, end_date, value / 1e6 AS shares_millions
FROM financial_facts
WHERE listing_id = 53407 AND concept = 'CommonStockSharesOutstanding'
ORDER BY end_date DESC LIMIT 8;
```

Metric definitions are in [Metrics Catalog](../reference/metrics.md); read the
definition before acting on a number, because several have documented caps and
fallbacks (`iroic_5y` caps at `1.0`, `dividend_yield_ttm` measures a true `0` for
evidenced non-payers).

## Caveats worth re-reading each quarter

- **Fiscal lag.** Metric `as_of` dates trail the price. A note dated today mixes a
  fresh price with a quarter-old balance sheet; that is correct, not a bug, but the
  yield on latest price is always slightly stale on the numerator.
- **Screen pass ≠ buy.** The screen is a filter on the past. Nothing in it sees
  competitive disruption coming.
- **Cost basis is not a price target.** The market does not know or care what was paid.
  Yield on cost measures the business; it never justifies holding a broken one.
- **One quarter is noise.** Every rule here requires two consecutive quarters before it
  compels an action, deliberately.
