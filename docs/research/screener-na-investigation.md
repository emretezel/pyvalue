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
- **Universe numbers are persisted-state numbers.** They are as fresh as the
  last `compute-metrics` run that touched each (listing, metric) pair (since
  2026-07-05 the diagnostics are read-only — nothing backfills), and
  `reason_code` is the *first* templated warning of the last failed attempt.
- Scratch outputs (CSVs) go to `data/output/na_investigation/` (gitignored);
  durable conclusions are distilled into the findings tables below.

## Runbook

`<WL9>` is the watchlist scope line from
[docs/reference/watchlist.md](../reference/watchlist.md).

### (a) Recompute watchlist metrics from existing data

```
pyvalue normalize-fundamentals --provider EODHD --symbols <WL9> --force   # only when concept mappings changed
pyvalue compute-metrics --symbols <WL9>
```

Local computation only (reads stored raw payloads / facts / market data,
writes `financial_facts`, `metrics` and `metric_compute_status`). Run this
before any per-stock analysis so screen results reflect the current code, not
a stale snapshot. The `--force` normalization matters whenever a concept
mapping was added or changed since the listing was last normalized: the
normalizer is payload-hash-gated and will otherwise silently skip re-extraction
(see findings below — this starved `RetainedEarnings` for 18,574 listings).

### (b) Enumerate NAs per stock and screen

```
pyvalue run-screen --config screeners/deep_value_graham.yml --symbols MSFT.US   # per-criterion PASS/FAIL
pyvalue report-screen-failures --config screeners/deep_value_graham.yml \
    --symbols <WL9> --output-csv data/output/na_investigation/dvg_watchlist.csv
pyvalue report-screen-failures --config screeners/quality_reasonable_price_primary.yml \
    --symbols <WL9> --output-csv data/output/na_investigation/qarp_watchlist.csv
pyvalue report-fact-freshness --symbols <WL9> --metrics <na metric ids>
```

Note (2026-07-05): `report-screen-failures` is now a pure read of persisted
state — criterion fallout plus metric NA impact counts only. It no longer
recomputes or backfills; run `compute-metrics` first and use
`report-metric-status --config <screen> --reasons` for per-reason root causes.

Per-(symbol, metric) root cause — persisted state incl. `reason_detail`,
per-concept input depth, market seam, and a write-free live recompute with
untemplated warnings:

```
pyvalue explain-metric --symbols 000660.KO --metrics oey_ev_norm sbc_to_fcf
pyvalue explain-metric --symbols <symbol> --config screeners/quality_reasonable_price_primary.yml
```

### (c) Universe-scale NA ranking (persisted state, read-only)

`pyvalue report-metric-status --config <screen.yml> --all-supported --reasons`
(added by this investigation) ranks a screen's metrics by persisted failure rate
and lists each metric's unique failure reasons — seconds at full-universe scale,
no recompute, no writes. The full-universe *recompute* (needed for the four DVG
metrics added 2026-07 that were never batch-computed) ran on 2026-07-04; the
2026-07-05 snapshot below reflects it.

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
   fragile dependency in QARP; since the per-year maintenance-NWC change, the FY
   owner-earnings series subtracts each year's own trailing 3-delta value, so
   the 5y metrics need up to 8 and the 10y metrics up to 13 consecutive FY of
   those concepts).
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

### Snapshot 2026-07-05 — persisted state after the full universe recompute

The universe re-normalization and full metric recompute have run: every
registered metric now has a persisted outcome for all 61,091 supported
listings, stamped 2026-07-04 in `metric_compute_status.attempted_at`
(`never_attempted` = 0 throughout). The four DVG metrics that were never
batch-computed get their first universe-scale numbers below; the 19
pre-existing criteria metrics reproduce their pre-recompute shares exactly
(same-day recompute over effectively unchanged inputs — cfo_to_ni_10y_median
again fails 53,514 listings with an identical 35,043-listing loss-year
bucket). DVG is no longer zeroed by never-computed criteria; its binding
constraints are now the generic ones (cfo_to_ni_10y_median 87.6%,
ni_loss_years_10y 66.7%, roic_7y_median 66.2%).

Notables from the `--reasons` breakdown: post-sweep, `altman_z` fails on
missing/stale `RetainedEarnings` for only 1,582 listings (vs the
18,574-listing starved cohort — the hash-gate fix held at universe scale);
its dominant gap, like `price_to_book`'s (missing equity, 12,244), is the
~12k-listing missing-balance-sheet-history cluster (example BLTN.AS).
`piotroski_f_score` fails mostly on the 400-day FY freshness gate (25,899 of
39,004 failures — rubric cause 6).

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
| piotroski_f_score | 63.8% | DVG |
| fcf_to_ebitda | 59.9% | DVG |
| net_debt_to_ebitda | 59.4% | DVG, QARP |
| interest_coverage | 59.0% | DVG, QARP |
| cfo_to_ni_ttm | 57.5% | QARP |
| gross_margin_ttm | 40.0% | QARP |
| share_count_cagr_5y | 34.5% | QARP |
| accruals_ratio | 31.8% | QARP |
| price_to_book | 30.6% | DVG |
| altman_z | 29.9% | DVG |
| eps_streak | 17.0% | QARP |
| market_cap | 10.1% | DVG |

Caveats: whole 61k global primary universe including micro-caps; persisted-state
freshness as described above. A market-cap-floored view is future work.

### Snapshot 2026-07-04 — persisted state before recompute

**Headline: four DVG criteria metrics were never batch-computed.**
`piotroski_f_score`, `altman_z`, `price_to_book`, `fcf_to_ebitda` had ~10 status
rows in the whole DB (a small test scope) vs 61,091 (the full primary universe)
for the older metrics. Until they are computed, DVG excludes essentially every
symbol on those criteria. Operational fix, not a code fix. **Resolved** by the
2026-07-04 full recompute — current numbers in the snapshot above.

**Watchlist criteria-NA matrix (beyond the four then-never-computed metrics,
which hit all nine):**

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

### 2026-07-04 root-cause pass — findings

**Operational finding 1 (fixed for the watchlist): the normalizer's hash gate
starves new concept mappings.** `normalize-fundamentals` skips any listing
whose raw payload hash is unchanged, and concept-mapping additions do not
invalidate that state. Consequence found live: the 18,574 listings normalized
on 2026-06-17 had **zero** `RetainedEarnings` facts even where the raw payload
carries `retainedEarnings` (verified for NVDA 146.97B / PLTR −3.56B / TSLA
39.0B USD), because the mapping only entered the tree with the altman_z work
on 2026-07-03; the 42,517 listings re-normalized that day have the facts
(37,726 of them). Fix applied (local, no API): `normalize-fundamentals
--provider EODHD --symbols NVDA.US PLTR.US TSLA.US --force` + recompute —
altman_z now computes for all nine watchlist stocks (NVDA 60.5, TSLA 15.0,
PLTR 141.0). **Follow-ups:** (a) a one-off `--force` normalization sweep of the
2026-06-17 cohort before any universe altman_z rollout; (b) design change:
include a concept-map version/hash in `fundamentals_normalization_state` so
mapping changes auto-invalidate.

**Operational finding 2: never-computed metrics.** The four DVG metrics added
2026-07 had ~10 status rows DB-wide until this investigation's watchlist
recompute; the full universe compute ran 2026-07-04 (see the 2026-07-05
snapshot). `report-metric-status` now makes this failure mode visible as
`never_attempted`.

**Data-quality finding: KRW price cap.** `market_data` holds 45 rows with
`price >= 999,999`; 000660.KO's latest close is stored as exactly
`999999.9999` KRW (prior closes 886k/876k), i.e. a provider-side numeric cap,
not a real price. market_cap for high-priced KRW listings is understated until
a corrected feed arrives (refresh out of scope).

**Watchlist criteria-NA matrix after fixes** (23 criteria metrics x 9 stocks =
207 cells; 19 NA cells remain, all root-caused):

| Stock | Remaining NA criteria metrics |
|---|---|
| MSFT.US, GOOGL.US | none |
| NVDA.US | owner_earnings_cagr_10y |
| ADBE.US | iroic_5y |
| AMD.US, TSLA.US | cfo_to_ni_10y_median, owner_earnings_cagr_10y |
| C.US | cfo_to_ni_10y_median, owner_earnings_cagr_10y, sbc_to_fcf |
| 000660.KO | cfo_to_ni_10y_median, owner_earnings_cagr_10y, sbc_to_fcf, oey_ev_norm |
| PLTR.US | those + interest_coverage, ni_loss_years_10y, gm_10y_std, roic_10y_min, roic_years_above_12pct (short history: 8 FY rows, 2020 listing) |

The financials hypothesis did **not** hold: C.US passes interest_coverage
(EODHD maps bank income statements onto OperatingIncomeLoss) and
net_debt_to_ebitda; its NAs are the generic ones.

### Per-metric verdicts

| Metric | Screens | Watchlist hit | Universe share | Classification | Evidence | Proposed follow-up |
|---|---|---|---|---|---|---|
| altman_z | DVG | ~~NVDA, PLTR, TSLA~~ fixed | 29.9% (post-recompute) | **data-gap (operational)** — hash-gated normalizer starved RetainedEarnings | raw payloads carry retainedEarnings; 2026-06-17 cohort has 0 facts | ~~--force normalization sweep + universe compute~~ done 2026-07-04; concept-map version in normalization state |
| cfo_to_ni_10y_median | DVG, QARP | AMD, C, PLTR, TSLA, 000660.KO | 87.6% (35,043 of 53,514 failures = loss-year guard) | **calc-modification** | TSLA: single 2019 loss year voids a 20-FY history; contradicted DVG's own loss tolerance (DVG now gates on `ni_loss_year_share <= 0.40` instead of `ni_loss_years_10y <= 4`, 2026-07-05) | ~~median over positive-NI years with a minimum count (e.g. >=6 of 10); loss years excluded, not fatal~~ done 2026-07-05: adaptive consecutive joint chain capped at 10y, loss years skipped, >=6 positive-NI points, freshness/as_of on the chain anchor |
| owner_earnings_cagr_10y | QARP | NVDA, AMD, C, TSLA, 000660.KO (+PLTR short history) | 85.3% | **calc-modification / possibly replace** | "non-positive endpoint averages": CAGR from a <=0 base is undefined; capex-heavy early years push owner earnings negative | consider regression-slope owner-earnings growth (defined for any sign pattern) or 3y-avg windows that skip non-positive bases |
| sbc_to_fcf | QARP | C.US (SBC line ended FY2023), 000660.KO (annual-only SBC: FY=5, Q=1) | 85.5% | **fallback** | Korean filings report SBC annually; C's provider line stopped | fall back to latest fresh FY SBC as the TTM numerator when 4 quarters are unavailable; document the approximation |
| oey_ev_norm | QARP | 000660.KO | 72.2% | **fallback** | `CashAndShortTermInvestments` MISSING while `CashAndCashEquivalents` (14.9T KRW) + `ShortTermInvestments` (20.2T KRW) both present; EV resolver has NO fallbacks, `invested_capital.py` does | mirror invested-capital fallbacks in `resolve_enterprise_value_denominator` (cash = CCE+STI; debt = TotalDebtFromBalanceSheet) |
| interest_coverage | DVG, QARP | PLTR.US | 59.0% | **calc-modification** | InterestExpense line ended FY2023 ($3.5M) while EBIT fresh ($1.4B) — effectively debt-free firms can never pass a `>=` bar | when EBIT>0 and interest is absent-or-<=0 with fresh EBIT, emit a documented cap value (e.g. 100x) instead of None |
| iroic_5y | QARP | ADBE.US | 72.9% | **calc-modification (decide semantics)** | buybacks shrink invested capital -> delta IC <= 0 while NOPAT grows — arguably excellent, not uncomputable | options: treat deltaIC<=0 with deltaNOPAT>0 as pass/cap, or keep NA with a documented caveat — needs author decision |
| dividend_yield_ttm (-> shareholder_yield_ttm 0.20 QARP ranking weight) | QARP ranking | ADBE, AMD, TSLA, PLTR | n/a (ranking) | **bug-class** | ADBE's last dividend row is 2016 (value 0) — a non-payer has yield 0, not NA; the NA silently renormalizes 20% of the ranking | dividend_yield_ttm = 0 when no recent dividend facts exist but the CF statement is otherwise fresh |
| PLTR history cluster (roic_10y_min, roic_years_above_12pct, gm_10y_std, ni_loss_years_10y, opm_7y_min...) | QARP | PLTR.US | 65-75% | **leave-NA** | 8 FY rows (2020 listing); the screen deliberately demands 10y maturity | none — document that young listings are excluded by design |
| Strict-chain family (roic_7y, gm_10y_std, opm_*, ni_loss_years_10y) universe-wide | both | (watchlist mostly OK) | 65-72%, with 12-22k "missing strict consecutive FY chain" buckets | **calc-modification candidate (universe)** | one missing mid-history year voids the metric; also 12-16k "latest FY point too old" (400d gate vs non-annual filers) | consider n-of-m tolerance after a fresh universe recompute quantifies the true chain-gap share |

### Diagnostic CLI verdict (the user's question 4)

**2026-07-04 verdict (superseded, kept for the record):** all five existing
commands stay — none is a strict subset: coverage is the only write-free joint
"all metrics computable" gate; fact-freshness the only recompute-free concept
view (now with the market-data seam line); metric-failures the reason survey
over arbitrary metric sets; screen-failures adds criterion attribution;
run-screen is the product (single-symbol mode now prints NA reasons + an
explain-metric hint). The redundancy that did exist — twin
recompute/bucket/example/CSV cores inside the two failure reports — was
consolidated into `cli/_failure_analysis.py` rather than deleting a command.
Two genuine gaps were closed with new commands: **report-metric-status**
(persisted NA-share ranking + unique reason listing, seconds at 61k scale,
zero writes — the "sort a screen's metrics by universe NA%" ask) and
**explain-metric** (per-symbol root cause with untemplated warnings and
`reason_detail`, write-free).

**2026-07-05 verdict (author decision, current):** the diagnostics family was
consolidated around a strict read-only contract — only `normalize-fundamentals`,
`compute-metrics`, market-data refresh, and `clear-*` mutate the database:

- **report-metric-coverage deleted** (never used; its success counts are the
  complement of the status survey's failure counts).
- **report-metric-failures merged into `report-metric-status --reasons`**,
  which now classifies persisted state against current input watermarks:
  fresh-failure buckets by `reason_code` with biggest-cap examples and
  `reason_detail`, plus explicit `stale_inputs` / `never_attempted`
  run-compute-metrics buckets. No recompute, no backfill.
- **report-screen-failures slimmed to criterion fallout** (pass/fail,
  threshold-vs-NA split, metric→criteria NA impact counts) and prints a
  `report-metric-status --config <screen> --reasons` hint instead of inlining
  root causes.
- **explain-metric keeps its live recompute** — it was already write-free and
  its untemplated warnings are the microscope's core value; its screen flag
  was renamed `--screen` → `--config` for CLI-wide consistency.

### Follow-up queue (each its own commit, author sign-off per item)

1. **Done 2026-07-04/05.** `normalize-fundamentals --force` sweep of the
   2026-06-17 cohort (18,574 listings; local, no API) + universe
   compute-metrics for piotroski_f_score/altman_z/price_to_book/fcf_to_ebitda;
   universe NA ranking refreshed via `report-metric-status` (2026-07-05
   snapshot).
2. metrics: EV denominator fallbacks (oey_ev_norm, ev_to_* family).
3. **Done 2026-07-05.** metrics: cfo_to_ni_10y_median loss-year tolerance —
   adaptive consecutive joint chain (cap 10y, first-gap stop), loss years
   skipped, >=6 positive-NI points, `statistics.median`; freshness/`as_of` on
   the chain anchor. Companion `ni_loss_year_share` + DVG gate swap follow as
   their own commits.
4. metrics: dividend_yield_ttm = 0 for non-payers.
5. metrics: interest_coverage documented cap for zero-interest EBIT-positive
   firms.
6. metrics: sbc_to_fcf FY fallback for annual-only SBC filers.
7. metrics: owner-earnings growth redesign (regression slope vs endpoint CAGR).
8. persistence: concept-map version in `fundamentals_normalization_state`.
9. tests: migrate the 20 flat `tests/*.py` files into `tests/unit|regression/`.
10. catalog: cross-listing issuer linkage (all three SK Hynix listings are
    `primary` under separate issuers).
