# pyvalue

Fundamental data ingestion and screening toolkit focusing on value-oriented strategies. Two data providers are supported: SEC (US only) and EODHD (global).

## Contents
- [Quick start](#quick-start)
- [Data providers](#data-providers)
- [Symbol format](#symbol-format)
- [CLI persistence](#cli-persistence)
- [EODHD universes and fundamentals](#eodhd-universes-and-fundamentals)
- [SEC company facts (US only)](#sec-company-facts-us-only)
- [Market data (EODHD)](#market-data-eodhd)
- [Global fundamentals (EODHD)](#global-fundamentals-eodhd)
- [Metrics and screening](#metrics-and-screening)
- [Metric reference](#metric-reference)
- [Private configuration](#private-configuration)
- [End-to-end workflow](#end-to-end-workflow)
- [Example: LSE (non-US exchange)](#example-lse-non-us-exchange)

## Quick start

```bash
python -m pip install -e .[dev]
pytest
```

## Disclaimer

This project is provided for educational and informational purposes only and does not constitute investment, financial, legal, or tax advice. Outputs may be inaccurate, incomplete, or delayed and are provided “as is” without warranties of any kind. You are solely responsible for any investment decisions and outcomes based on this software; use at your own risk. Nothing here is an offer or solicitation to buy or sell any security, and past performance is not indicative of future results. Consult a licensed professional before making investment decisions.

## Data providers

- **SEC**: US-only fundamentals. SEC digital company facts do not enforce reporting standards, so some metrics may not be computable.
- **EODHD**: fundamentals for global exchanges, plus **all** market data.

Recommendation: use **EODHD** for all purposes (including US fundamentals) for better coverage. You need active EODHD subscriptions for both market data and fundamentals.

## Symbol format

All tickers are stored and referenced with an exchange suffix using EODHD codes
(e.g., `AAPL.US`, `SHEL.LSE`). US symbols always use `.US`.

## CLI persistence

Persist the US universe into a local SQLite database via the CLI:

```bash
pyvalue load-universe --provider SEC
```

Unless noted otherwise, every CLI command accepts `--database` to point at a specific SQLite file;
if omitted, it defaults to `data/pyvalue.db`.

ETFs are excluded by default; pass `--include-etfs` to store them as well.

> Nasdaq serves the symbol directories via FTP (`ftp://ftp.nasdaqtrader.com/symboldirectory/...`).
> You can verify availability manually with
> `curl "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt"`.

## EODHD universes and fundamentals

**EODHD fundamentals subscription required.** Market data is always fetched from EODHD, so an EODHD market data subscription is also required.

### Load an exchange (example: London Stock Exchange, LSE)

```bash
pyvalue load-universe --provider EODHD --exchange-code LSE --database data/pyvalue.db
```

This pulls the London Stock Exchange symbol list from EODHD (requires `[eodhd].api_key` in `private/config.toml`), keeps equities by default (ETFs excluded unless `--include-etfs`), and stores ISINs when available.

### SEC company facts (US only)

SEC requires a descriptive `User-Agent` header that includes contact details. Set
`[sec].user_agent` in `private/config.toml` or an environment variable such as:

```bash
export PYVALUE_SEC_USER_AGENT="pyvalue/0.1 (contact: you@example.com)"
```

Then ingest the latest company facts for a ticker (AAPL shown below). Note that SEC data is less standardized; some metrics may be missing compared to EODHD:

```bash
pyvalue ingest-fundamentals --provider SEC AAPL.US
```

This downloads the JSON payload from `https://data.sec.gov/api/xbrl/companyfacts/…` and
stores it in the `fundamentals_raw` table. Pass `--cik` if you already know the exact CIK.

Normalize the previously ingested payload into structured rows for downstream metrics:

```bash
pyvalue normalize-fundamentals --provider SEC AAPL.US
```

To normalize every stored SEC payload after a bulk ingest, run:

```bash
pyvalue normalize-fundamentals-bulk --provider SEC
```

This iterates over the `fundamentals_raw` table, converts each JSON payload into
`financial_facts`, reports progress, and can be cancelled with Ctrl+C.

This populates the `financial_facts` table with the concepts required to compute the
initial metric set (debt, current assets/liabilities, EPS, dividends, cash flow, etc.).

Provider-specific commands target a source:

```bash
pyvalue ingest-fundamentals --provider SEC AAPL.US
pyvalue normalize-fundamentals --provider SEC AAPL.US
```

To ingest and normalize US fundamentals from EODHD as well:

```bash
pyvalue ingest-fundamentals --provider EODHD AAPL.US
pyvalue normalize-fundamentals --provider EODHD AAPL.US
```

Normalized facts are provider-agnostic. Re-normalizing a symbol replaces any
previous facts for that symbol regardless of provider.

## Market data (EODHD)

Market data is always fetched from EODHD and requires an active EODHD market data subscription.

Store your EODHD API token in `private/config.toml` (quotes optional; they are stripped automatically):

```toml
[eodhd]
api_key = "YOUR_EOD_TOKEN"
```

Fetch the latest quote and persist it in `market_data`:

```bash
pyvalue update-market-data AAPL.US
```

If the symbol does not include an exchange suffix, pass `--exchange-code`:

```bash
pyvalue update-market-data AAPL --exchange-code US
```

To refresh every stored ticker on an exchange (using the latest universe in SQLite) with throttling
that respects EODHD limits, run:

```bash
pyvalue update-market-data-bulk --exchange-code US --rate 950
```

The bulk command enforces the requested symbols-per-minute rate (950 by default) and can
be interrupted with Ctrl+C.

If you ingest raw prices before share counts were available, recompute stored market caps later via:

```bash
pyvalue recalc-market-cap --exchange-code US
```

This multiplies each stored price by the latest share count from normalized SEC data.

By default the CLI uses EODHD’s `/api/eod/{symbol}.EXCH` endpoint and multiplies the returned
close price by the latest share count (from SEC or EODHD fundamentals) to derive market cap.
You can still inject a custom provider when instantiating `MarketDataService` in Python if you
need a different feed.

## Global fundamentals (EODHD)

Store your EODHD API token in `private/config.toml` (see Market data section above). Pull
fundamentals for a ticker and normalize them into `financial_facts` using the EODHD ruleset
(region is inferred from the exchange’s country code):

```bash
pyvalue ingest-fundamentals --provider EODHD SHEL.LSE
pyvalue normalize-fundamentals --provider EODHD SHEL.LSE
```

To ingest and normalize every listing for an exchange directly from EODHD (example: London
Stock Exchange, code LSE):

```bash
pyvalue ingest-fundamentals-bulk --provider EODHD --exchange-code LSE
pyvalue normalize-fundamentals-bulk --provider EODHD --exchange-code LSE
```

Metrics and screening read only `financial_facts`. They do not track provider.

## Metrics and screening

Compute metrics (e.g., working capital, long-term debt) for a ticker:

```bash
pyvalue compute-metrics AAPL.US --metrics working_capital long_term_debt
```

To compute the full metric set for every stored ticker, run:

```bash
pyvalue compute-metrics-bulk --exchange-code US
```

The bulk command iterates over the stored exchange listings, evaluates each metric,
prints progress, and can be cancelled with Ctrl+C.

### Fact coverage report

List missing or stale (older than one year by default) financial facts required by the metrics for an exchange:

```bash
pyvalue report-fact-freshness --exchange-code US --metrics working_capital eps_ttm
```

Add `--output-csv fact_report.csv` for concept-level details that can be inspected in a spreadsheet.

Define screening criteria in YAML (see `screeners/value.yml`) and evaluate them:

```bash
pyvalue run-screen AAPL.US screeners/value.yml
```

The sample screen checks nine value-focused criteria (leverage, profitability, liquidity,
valuation). Metrics are cached in the `metrics` table for reuse.

To evaluate every stored symbol and print a pass-only table, run:

```bash
pyvalue run-screen-bulk screeners/value.yml --exchange-code US
```

Rows represent criteria and columns show the symbols that satisfied every rule along with the
left-hand metric values used for comparison.

By default the CSV is written to `data/screen_results.csv`. Pass `--output-csv results.csv` to override.

Additional metrics include:

- `eps_streak`: Counts consecutive positive EPS (diluted) FY values.
- `current_ratio`: Current assets divided by current liabilities.
- `graham_eps_10y_cagr_3y_avg`: Graham EPS 10-year-period CAGR using 3-year average EPS at
  the start and end of the period (using full-year GAAP EPS data).
- `mcapex_fy`: Maintenance capex proxy for the latest FY, computed as
  `min(Capex_FY, 1.1 × D&A_FY)` with single-input fallback and absolute-value handling.
- `mcapex_5y`: Average of the latest five available FY `mcapex_fy` values
  (requires exactly five FY points; gaps are allowed).
- `mcapex_ttm`: Maintenance capex proxy for TTM, computed as
  `min(Capex_TTM, 1.1 × D&A_TTM)` with single-input fallback and absolute-value handling.
- `nwc_mqr`: Net working capital for the most recent quarter using
  `(AssetsCurrent - Cash) - (LiabilitiesCurrent - ShortTermDebt)` with EODHD-specific fallbacks.
- `nwc_fy`: Net working capital for latest FY end using the same adjusted-NWC formula.
- `delta_nwc_ttm`: Quarter-over-quarter-year change in NWC
  (`NWC(MQR) - NWC(same quarter last year)`).
- `delta_nwc_fy`: Fiscal-year NWC change (`NWC(latest FY) - NWC(prior FY)`).
- `delta_nwc_maint`: `max(average(last 3 FY deltas of NWC), 0)`.
- `oe_equity_ttm`: Owner earnings equity (TTM), computed as
  `NI_TTM + D&A_TTM - MCapex_TTM - delta_nwc_maint` (EODHD-oriented).
- `oe_equity_5y_avg`: Average of the latest five available FY owner earnings equity
  values using `OE_FY = NI_FY + D&A_FY - MCapex_FY - latest_delta_nwc_maint`
  (requires exactly five points; gaps allowed; EODHD-oriented).

## Metric reference

The built-in value screen relies on the following metrics. Each row outlines how the metric
is derived from normalized SEC or market data plus the value-investing intuition behind it.

| Metric | How it is calculated | Why value investors care |
| --- | --- | --- |
| `working_capital` | Latest `AssetsCurrent - LiabilitiesCurrent`. | Healthy working capital protects downside by ensuring near-term obligations are covered without diluting shareholders. |
| `long_term_debt` | US SEC: `LongTermDebtNoncurrent + LongTermDebtCurrent`; else sum noncurrent components + current, falling back to notes payable or debt+lease rollups. | Excessive leverage magnifies downside, so keeping long-term debt manageable relative to liquidity (e.g., working capital) preserves margin of safety. |
| `debt_paydown_years` | Total debt (`ShortTermDebt + LongTermDebt`) divided by trailing 12-month free cash flow (quarterly OCF minus capex, EODHD-only). | Estimates how many years of current free cash flow would be needed to repay debt; lower is healthier. |
| `short_term_debt_share` | Short-term debt divided by total debt (`ShortTermDebt / (ShortTermDebt + LongTermDebt)`, EODHD-only). | Shows how much debt matures soon; higher shares imply more refinancing risk. |
| `return_on_invested_capital` | TTM EBIT × (1 − tax rate) divided by average invested capital, where invested capital is `ShortTermDebt + LongTermDebt + StockholdersEquity − CashAndShortTermInvestments` (EODHD-only, tax rate uses a fallback when needed). | Measures how efficiently the business earns after-tax operating profits on the capital invested. |
| `net_debt_to_ebitda` | Net debt (`ShortTermDebt + LongTermDebt - CashAndShortTermInvestments`) divided by trailing 12-month EBITDA (quarterly sum, EODHD-only). | Highlights leverage relative to operating cash earnings; lower or negative suggests balance-sheet strength. |
| `interest_coverage` | Trailing 12-month `OperatingIncomeLoss` divided by trailing 12-month `InterestExpense` (quarterly sums, EODHD-only). | Indicates how comfortably operating profits cover financing costs; higher is safer. |
| `current_ratio` | Latest `AssetsCurrent / LiabilitiesCurrent`. | A current ratio above ~1 indicates the business can stomach short-term shocks without forced asset sales or equity issuance. |
| `earnings_yield` | Trailing 12-month EPS (sum of latest four quarterly EPS values) divided by the latest price. | The inverse of P/E highlights how much earnings power you receive per dollar invested; higher yields can indicate cheaper valuations. |
| `eps_streak` | Number of consecutive fiscal years with positive EPS. | Consistent profitability signals durable business quality and lowers the odds that current earnings are a cyclical mirage. |
| `graham_eps_10y_cagr_3y_avg` | Computes the 10-year-period EPS CAGR using 3-year average EPS at the start and end of the period. | Requires sustained compounding, favoring firms that can steadily grow earnings instead of relying on one-off rebounds. |
| `graham_multiplier` | `(Price / TTM EPS) × (Price / TBVPS)`, where TBVPS is tangible book value per share (`(Equity - Goodwill - Intangibles) / Shares`). | Benjamin Graham’s combined PE×PB test guards against paying too much for either earnings or assets, enforcing a strict valuation ceiling. |
| `roc_greenblatt_5y_avg` | Average over up to five fiscal years of `EBIT / Tangible Capital`, where tangible capital is `Net PPE + AssetsCurrent - LiabilitiesCurrent`. | Joel Greenblatt’s ROC stresses whether management can reinvest incremental capital at high rates—a key quality signal for value investors who want cheap *and* good businesses. |
| `roe_greenblatt_5y_avg` | Average over up to five fiscal years of net income available to common shareholders divided by the two-year average of common equity (after subtracting preferred equity). | Sustained high ROE shows that the firm generates attractive returns on shareholders’ capital without leverage-driven distortion. |
| `price_to_fcf` | Latest market cap divided by trailing 12-month free cash flow, with FCF computed as (operating cash flow – capex) across the latest four quarters. | Cash flow–based multiples focus on hard cash instead of accounting earnings, helping avoid value traps with low-quality accrual profits. |
| `mcapex_fy` | Latest fiscal-year maintenance capex proxy: `min(CapitalExpenditures_FY, 1.1 × D&A_FY)`, where D&A uses `DepreciationDepletionAndAmortization` and falls back to cash-flow depreciation when needed (EODHD-only). If only one side exists, uses that side; absolute values are used for sign consistency. | Approximates recurring reinvestment needs without letting unusually high growth capex dominate the estimate. |
| `mcapex_5y` | Average of the latest 5 available fiscal-year `mcapex_fy` values (requires exactly five points; year gaps allowed; EODHD-only). | Smooths one-off investment cycles and produces a steadier maintenance reinvestment baseline. |
| `mcapex_ttm` | Trailing 12-month maintenance capex proxy: `min(CapitalExpenditures_TTM, 1.1 × D&A_TTM)` using quarterly sums and the same D&A fallback/sign rules (EODHD-only). | Gives a near-term maintenance reinvestment estimate for valuation and cash-flow quality checks. |
| `nwc_mqr` | Most recent-quarter net working capital: `(AssetsCurrent - Cash) - (LiabilitiesCurrent - ShortTermDebt)` where `Cash` prefers `CashAndShortTermInvestments` and falls back to `CashAndCashEquivalents + ShortTermInvestments`; if short-term debt is missing, liabilities are used as-is (EODHD-oriented). | Isolates operating working capital by excluding cash and debt components, helping assess short-term capital lock-up. |
| `nwc_fy` | Latest FY-end net working capital using the same adjusted formula and fallback rules as `nwc_mqr` (EODHD-oriented). | Provides an annual baseline for working-capital intensity and trend analysis. |
| `delta_nwc_ttm` | `NWC(MQR) - NWC(same fiscal quarter previous year)` with strict quarter matching (EODHD-oriented). | Captures year-over-year working-capital drift without quarter-seasonality distortion. |
| `delta_nwc_fy` | `NWC(latest FY) - NWC(strict prior FY)` (EODHD-oriented). | Highlights annual changes in operating working-capital requirements. |
| `delta_nwc_maint` | `max(average(last 3 consecutive FY deltas of NWC), 0)` (EODHD-oriented). | Converts multi-year NWC drift into a conservative maintenance adjustment that does not go negative. |
| `oe_equity_ttm` | Owner earnings equity TTM: `NI_TTM + D&A_TTM - MCapex_TTM - delta_nwc_maint` (EODHD-oriented), where NI prefers `NetIncomeLoss` and falls back to net income available to common, D&A prefers income-statement D&A and falls back to cash-flow depreciation, and missing D&A defaults to 0. | Approximates owner earnings after maintenance reinvestment and sustained working-capital drag using near-term (TTM) fundamentals. |
| `oe_equity_5y_avg` | Average of latest 5 FY owner earnings equity values, each computed as `NI_FY + D&A_FY - MCapex_FY - latest_delta_nwc_maint` (EODHD-oriented; strict 5 values; year gaps allowed; same NI/D&A fallback rules as TTM). | Smooths cyclical noise and yields a multi-year owner-earnings baseline for intrinsic-value comparisons. |
| `market_cap` | Latest stored market capitalization snapshot. | Screening for a minimum size filters out illiquid micro-caps where information quality and trading costs can erode returns. |
| `eps_ttm` | Sum of the most recent four quarterly EPS values. | Used to verify that current earnings have not collapsed relative to history, preventing “cheap” valuations caused by deteriorating fundamentals. |
| `eps_6y_avg` | Average of the latest six fiscal-year EPS values. | Provides a normalized earnings power baseline for comparisons against current EPS streaks or TTM values, smoothing out cyclical peaks and troughs. |

## Private configuration

Place API keys or region-specific credentials inside the `private/` directory (ignored by git).

## End-to-end workflow

1. Load the latest US universe into SQLite:
   ```bash
   pyvalue load-universe --provider SEC
   ```
2. Ingest SEC facts for every stored ticker on an exchange (honors API-rate throttling):
   ```bash
   pyvalue ingest-fundamentals-bulk --provider SEC --exchange-code US --user-agent "pyvalue/0.1 (your@email)"
   ```
3. Normalize the ingested payloads so metrics can consume them:
   ```bash
   pyvalue normalize-fundamentals-bulk --provider SEC
   ```
4. Fetch market data for every ticker (default EODHD, throttled to 950/min):
   ```bash
   pyvalue update-market-data-bulk --exchange-code US
   ```
5. Compute the entire metric set for all tickers:
   ```bash
   pyvalue compute-metrics-bulk --exchange-code US
   ```
6. Run the value screen across the universe (CSV export defaults to `data/screen_results.csv`):
   ```bash
   pyvalue run-screen-bulk screeners/value.yml --exchange-code US
   ```

### Example: LSE (non-US exchange)

1. Load the LSE universe:
   ```bash
   pyvalue load-universe --provider EODHD --exchange-code LSE
   ```
2. Ingest EODHD fundamentals for all LSE tickers:
   ```bash
   pyvalue ingest-fundamentals-bulk --provider EODHD --exchange-code LSE
   ```
3. Normalize the ingested payloads:
   ```bash
   pyvalue normalize-fundamentals-bulk --provider EODHD --exchange-code LSE
   ```
4. Fetch market data for all LSE tickers:
   ```bash
   pyvalue update-market-data-bulk --exchange-code LSE
   ```
5. Compute metrics for all LSE tickers:
   ```bash
   pyvalue compute-metrics-bulk --exchange-code LSE
   ```
