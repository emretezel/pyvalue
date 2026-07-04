# Screener NA Investigation

Living research document for the screener fine-tuning effort: which criteria
metrics of `screeners/deep_value_graham.yml` (DVG) and
`screeners/quality_reasonable_price_primary.yml` (QARP) come out **NA** (not
computable), for the [anchor watchlist](../reference/watchlist.md) and for the
supported universe, and what to do about each cause.

A criterion whose metric is NA **fails** — the symbol is excluded from the screen
— so systematic NAs silently shrink the screenable universe. NA means "no row in
`metrics`"; the latest attempt's outcome lives in `metric_compute_status`
(`status`, `reason_code`, `reason_detail`).

## Operating constraints

- **No EODHD API calls.** All analysis works with the facts, FX rates, and price
  snapshots already in `data/pyvalue.db`. Stale or missing source data is
  *documented* as a data gap, never fixed by re-ingesting.
- **Universe numbers are persisted-state numbers.** They are as fresh as the last
  `compute-metrics` / report backfill that touched each (listing, metric) pair,
  and `reason_code` is the *first* templated warning of the last failed attempt.
- Scratch outputs (CSVs) go to `data/output/na_investigation/` (gitignored);
  durable conclusions are distilled into the findings tables below.

## Runbook

`<WL9>` is the watchlist scope line from
[docs/reference/watchlist.md](../reference/watchlist.md).

### (a) Recompute watchlist metrics from existing data

```
pyvalue compute-metrics --symbols <WL9>
```

Local computation only (reads facts + stored market data, writes `metrics` and
`metric_compute_status`). Run this before any per-stock analysis so screen
results reflect the current code, not a stale snapshot.

### (b) Enumerate NAs per stock and screen

```
pyvalue run-screen --config screeners/deep_value_graham.yml --symbols MSFT.US   # per-criterion PASS/FAIL
pyvalue report-screen-failures --config screeners/deep_value_graham.yml \
    --symbols <WL9> --output-csv data/output/na_investigation/dvg_watchlist.csv
pyvalue report-screen-failures --config screeners/quality_reasonable_price_primary.yml \
    --symbols <WL9> --output-csv data/output/na_investigation/qarp_watchlist.csv
pyvalue report-fact-freshness --symbols <WL9> --metrics <na metric ids>
```

Note `report-screen-failures` recomputes NA screen metrics and backfills
`metrics`/`metric_compute_status` — intended here.

Per-(symbol, metric) root cause — persisted state incl. `reason_detail`,
per-concept input depth, market seam, and a write-free live recompute with
untemplated warnings:

```
pyvalue explain-metric --symbols 000660.KO --metrics oey_ev_norm sbc_to_fcf
pyvalue explain-metric --symbols <symbol> --screen screeners/quality_reasonable_price_primary.yml
```

### (c) Universe-scale NA ranking (persisted state, read-only)

`pyvalue report-metric-status --config <screen.yml> --all-supported --reasons`
(added by this investigation) ranks a screen's metrics by persisted failure rate
and lists each metric's unique failure reasons — seconds at full-universe scale,
no recompute, no writes. A fresh *recompute* of the universe (needed for the four
DVG metrics added 2026-07 that were never batch-computed) is a separate,
explicitly-scheduled follow-up.

### (d) Recording findings

One row per (metric, cause) in the findings tables below: screens affected,
watchlist stocks hit, universe failure share, classification — **bug** /
**calc-modification** / **fallback** / **data-gap** / **leave-NA** — evidence,
and the proposed follow-up. Metric-formula changes land as separate commits, one
per metric, after author sign-off.

## Classification rubric (structural NA causes)

1. **Strict consecutive FY chains** (roic_7y/10y variants, cfo_to_ni_10y_median,
   ni_loss_years_10y, gm_10y_std, opm_7y_min/10y_std, owner_earnings_cagr_10y):
   one missing fiscal year voids the whole metric. Candidate: tolerate gaps /
   "N of M years".
2. **Non-positive guards that NA instead of failing the criterion**:
   interest_coverage NAs on zero interest (a debt-free firm can never pass);
   cfo_to_ni_* NA on NI<=0; sbc_to_fcf NAs on FCF<=0; net_debt_to_ebitda and
   fcf_to_ebitda NA on EBITDA<=0. Per metric: NA vs computed-but-failing vs
   documented cap.
3. **All-or-nothing composites**: piotroski_f_score (9 signals x 3 FY years),
   altman_z (NAs when RetainedEarnings is absent), owner-earnings chain
   (delta_nwc_maint needs 4 consecutive FY of 4 balance-sheet concepts — the most
   fragile dependency in QARP).
4. **Market-data seam**: market_cap / EV metrics need a positive shares fact and
   a positive stored price.
5. **TTM strictness**: 4 consecutive quarters; interest_coverage needs 4 *aligned*
   EBIT+interest quarters; sbc_to_fcf needs 4 SBC quarters even for zero-SBC
   filers.
6. **Freshness gate**: 400-day fact age everywhere.
7. **Currency seams**: missing listing currency / missing FX rate.
8. **Sector fit**: financials may lack industrial-style concepts — to be
   *verified*, not assumed (C.US currently passes interest_coverage and
   net_debt_to_ebitda).

## Findings

### Snapshot 2026-07-04 — persisted state before recompute

**Headline: four DVG criteria metrics were never batch-computed.**
`piotroski_f_score`, `altman_z`, `price_to_book`, `fcf_to_ebitda` had ~10 status
rows in the whole DB (a small test scope) vs 61,091 (the full primary universe)
for the older metrics. Until they are computed, DVG excludes essentially every
symbol on those criteria. Operational fix, not a code fix.

**Universe failure rates, criteria metrics (persisted `metric_compute_status`):**

| Metric | Failure share | Screens |
|---|---|---|
| cfo_to_ni_10y_median | 87.6% | DVG, QARP |
| sbc_to_fcf | 85.5% | QARP |
| owner_earnings_cagr_10y | 85.3% | QARP |
| roic_years_above_12pct / roic_10y_min | 75.2% | QARP |
| iroic_5y | 72.9% | QARP |
| oey_ev_norm | 72.2% | QARP |
| gm_10y_std | 71.7% | QARP |
| ni_loss_years_10y | 66.7% | DVG, QARP |
| roic_7y_median | 66.2% | DVG, QARP |
| opm_7y_min | 65.3% | QARP |
| net_debt_to_ebitda | 59.4% | DVG, QARP |
| interest_coverage | 59.0% | DVG, QARP |
| cfo_to_ni_ttm | 57.5% | QARP |
| gross_margin_ttm | 40.0% | QARP |
| share_count_cagr_5y | 34.5% | QARP |
| accruals_ratio | 31.8% | QARP |
| eps_streak | 17.0% | QARP |
| market_cap | 10.1% | DVG |
| piotroski_f_score / altman_z / price_to_book / fcf_to_ebitda | n/a (never batch-computed) | DVG |

Caveats: whole 61k global primary universe including micro-caps; persisted-state
freshness as described above. A market-cap-floored view is future work.

**Watchlist criteria-NA matrix (beyond the four never-computed metrics, which hit
all nine):**

| Stock | NA criteria metrics (persisted reason) |
|---|---|
| MSFT.US, GOOGL.US | none |
| NVDA.US | owner_earnings_cagr_10y (non-positive endpoint averages) |
| ADBE.US | iroic_5y (non-positive delta invested capital) |
| AMD.US, TSLA.US | cfo_to_ni_10y_median (non-positive FY net income in one year), owner_earnings_cagr_10y |
| C.US | cfo_to_ni_10y_median, owner_earnings_cagr_10y, sbc_to_fcf (SBC facts stale) |
| PLTR.US | interest_coverage (latest quarter too old), ni_loss_years_10y, gm_10y_std, roic_10y_min, roic_years_above_12pct, cfo_to_ni_10y_median, owner_earnings_cagr_10y (short history — 2020 listing) |
| 000660.KO | cfo_to_ni_10y_median (loss year), oey_ev_norm (missing EV debt/cash facts), owner_earnings_cagr_10y, sbc_to_fcf (no quarterly SBC in Korean filings) |

Code-reading note on `oey_ev_norm`: `resolve_enterprise_value_denominator`
(`src/pyvalue/metrics/enterprise_value.py`) requires `ShortTermDebt`,
`LongTermDebt` and `CashAndShortTermInvestments` each individually, with no
fallbacks — while `invested_capital.py` has fallbacks for all three. Concrete
fallback candidate.

Ranking-metric note: `dividend_yield_ttm` NAs for non-payers (ADBE, AMD, TSLA,
PLTR), which NAs `shareholder_yield_ttm` (0.20 QARP ranking weight, silently
renormalized away). A non-payer arguably has dividend yield 0, not NA.

### Per-metric verdicts (to be completed after root-cause pass)

| Metric | Screens | Watchlist stocks hit | Universe share | Classification | Evidence | Proposed follow-up |
|---|---|---|---|---|---|---|
| _pending_ | | | | | | |
