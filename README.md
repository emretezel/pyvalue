# pyvalue

Fundamental data ingestion and screening toolkit focusing on value-oriented strategies. Currently supports fetching the US equity universe.

## Quick start

```bash
python -m pip install -e .[dev]
pytest
```

## Symbol format

All tickers are stored and referenced with an exchange suffix using EODHD codes
(e.g., `AAPL.US`, `SHEL.LSE`). US symbols always use `.US`.

## US universe loader

```python
from pyvalue.universe import USUniverseLoader

loader = USUniverseLoader()
universe = loader.load()
for item in universe:
    print(item.symbol, item.exchange)
```

The loader downloads Nasdaq Trader symbol directories, filters out test issues, and normalizes exchange names across NASDAQ, NYSE, NYSE Arca, NYSE MKT, and Cboe BZX.

## CLI persistence

Persist the US universe into a local SQLite database via the CLI:

```bash
pyvalue load-us-universe
```

Unless noted otherwise, every CLI command accepts `--database` to point at a specific SQLite file;
if omitted, it defaults to `data/pyvalue.db`.

ETFs are excluded by default; pass `--include-etfs` to store them as well.

> Nasdaq serves the symbol directories via FTP (`ftp://ftp.nasdaqtrader.com/symboldirectory/...`).
> You can verify availability manually with
> `curl "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt"`.

## UK universe and company facts

### Load the UK universe (EODHD)

```bash
pyvalue load-eodhd-universe --exchange-code LSE --database data/pyvalue.db
```

This pulls the London Stock Exchange symbol list from EODHD (requires `[eodhd].api_key` in `private/config.toml`), keeps equities by default (ETFs excluded unless `--include-etfs`), and stores ISINs when available.

### Map UK symbols to Companies House

GLEIF provides LEI/company-number data and ISIN→LEI mappings. Refresh the symbol map after loading the UK universe:

```bash
pyvalue refresh-uk-symbol-map --database data/pyvalue.db --isin-date YYYY-MM-DD
```

Use a recent `--isin-date` (e.g., yesterday) if today’s ISIN file is not yet published. This joins stored ISINs to Companies House numbers and stores them in `uk_symbol_map`.

### Ingest Companies House profiles

Configure your Companies House API key:

```toml
[companies_house]
api_key = "YOUR_COMPANIES_HOUSE_KEY"
```

Then ingest by symbol (uses the mapping) or by explicit company number:

```bash
pyvalue ingest-uk-facts --symbol SHEL --database data/pyvalue.db
# or
pyvalue ingest-uk-facts 04366849 --database data/pyvalue.db
```

Bulk ingest all mapped UK symbols:

```bash
pyvalue ingest-uk-facts-bulk --database data/pyvalue.db
```

### US company facts

SEC requires a descriptive `User-Agent` header that includes contact details. Set
`[sec].user_agent` in `private/config.toml` or an environment variable such as:

```bash
export PYVALUE_SEC_USER_AGENT="pyvalue/0.1 (contact: you@example.com)"
```

Then ingest the latest company facts for a ticker (AAPL shown below):

```bash
pyvalue ingest-us-facts AAPL
```

This downloads the JSON payload from `https://data.sec.gov/api/xbrl/companyfacts/…` and
stores it in the `company_facts` table. Pass `--cik` if you already know the exact CIK.

Normalize the previously ingested payload into structured rows for downstream metrics:

```bash
pyvalue normalize-us-facts AAPL
```

To normalize every stored SEC payload after a bulk ingest, run:

```bash
pyvalue normalize-us-facts-bulk
```

This iterates over the `company_facts` table, converts each JSON payload into
`financial_facts`, reports progress, and can be cancelled with Ctrl+C.

This populates the `financial_facts` table with the concepts required to compute the
initial metric set (debt, current assets/liabilities, EPS, dividends, cash flow, etc.).

## UK company facts (Companies House)

Store your Companies House API key in `private/config.toml`:

```toml
[companies_house]
api_key = "YOUR_COMPANIES_HOUSE_KEY"
```

Then ingest the company profile JSON by company number (optionally associate a ticker):

```bash
pyvalue ingest-uk-facts 00000000 --symbol VOD
```

## Market data (EODHD default, Alpha Vantage fallback)

Store your EODHD API token in `private/config.toml` (quotes optional; they are stripped automatically):

```toml
[eodhd]
api_key = "YOUR_EOD_TOKEN"

[alpha_vantage]
api_key = "YOUR_KEY"
```

Fetch the latest quote and persist it in `market_data`:

```bash
pyvalue update-market-data AAPL.US
```

To refresh every stored ticker (using the latest universe in SQLite) with throttling that
respects EODHD limits, run:

```bash
pyvalue update-market-data-bulk --rate 950
```

The bulk command enforces the requested symbols-per-minute rate (950 by default) and can
be interrupted with Ctrl+C.

If you ingest raw prices before share counts were available, recompute stored market caps later via:

```bash
pyvalue recalc-market-cap
```

This multiplies each stored price by the latest share count from normalized SEC data.

By default the CLI uses EODHD’s `/api/eod/{symbol}.US` endpoint and multiplies the returned
close price by the latest SEC share count (from `EntityCommonStockSharesOutstanding` or
`CommonStockSharesOutstanding`) to derive market cap. If no EODHD key is present it falls
back to Alpha Vantage’s `GLOBAL_QUOTE` + `OVERVIEW`. You can still inject a custom provider
when instantiating `MarketDataService` in Python if you need a different feed.

## Global fundamentals (EODHD)

Store your EODHD API token in `private/config.toml` (see Market data section above). Pull
fundamentals for a ticker and normalize them into `financial_facts` with provider `EODHD`
(region is inferred from the exchange’s country code):

```bash
pyvalue ingest-eodhd-fundamentals SHEL --exchange-code LSE
pyvalue normalize-eodhd-fundamentals SHEL
```

To ingest and normalize every listing for an exchange directly from EODHD (example: London
Stock Exchange, code LSE):

```bash
pyvalue ingest-eodhd-fundamentals-bulk --exchange-code LSE
pyvalue normalize-eodhd-fundamentals-bulk
```

Metrics and screening look up normalized facts by provider priority (SEC first, then
EODHD), so US tickers still rely on SEC data while non‑US symbols use EODHD.

## Metrics and screening

Compute metrics (e.g., working capital, long-term debt) for a ticker:

```bash
pyvalue compute-metrics AAPL.US --metrics working_capital long_term_debt
```

To compute the full metric set for every stored ticker, run:

```bash
pyvalue compute-metrics-bulk
```

The bulk command iterates over the stored universe (default US), evaluates each metric,
prints progress, and can be cancelled with Ctrl+C.

Define screening criteria in YAML (see `screeners/value.yml`) and evaluate them:

```bash
pyvalue run-screen AAPL.US screeners/value.yml
```

The sample screen checks nine value-focused criteria (leverage, profitability, liquidity,
valuation). Metrics are cached in the `metrics` table for reuse.

To evaluate every stored symbol and print a pass-only table, run:

```bash
pyvalue run-screen-bulk screeners/value.yml --output-csv results.csv
```

Rows represent criteria and columns show the symbols that satisfied every rule along with the
left-hand metric values used for comparison.

Pass `--output-csv results.csv` to write the same table to disk in CSV form.

Additional metrics include:

- `eps_streak`: Counts consecutive positive EPS (diluted) FY values.
- `current_ratio`: Current assets divided by current liabilities.
- `graham_eps_10y_cagr_3y_avg`: Graham EPS 10-year CAGR averaged over the latest three
  periods (using full-year GAAP EPS data).

## Metric reference

The built-in value screen relies on the following metrics. Each row outlines how the metric
is derived from normalized SEC or market data plus the value-investing intuition behind it.

| Metric | How it is calculated | Why value investors care |
| --- | --- | --- |
| `working_capital` | Latest `AssetsCurrent - LiabilitiesCurrent`. | Healthy working capital protects downside by ensuring near-term obligations are covered without diluting shareholders. |
| `long_term_debt` | Latest `LongTermDebtNoncurrent`, falling back to `LongTermDebt`. | Excessive leverage magnifies downside, so keeping long-term debt manageable relative to liquidity (e.g., working capital) preserves margin of safety. |
| `current_ratio` | Latest `AssetsCurrent / LiabilitiesCurrent`. | A current ratio above ~1 indicates the business can stomach short-term shocks without forced asset sales or equity issuance. |
| `earnings_yield` | Trailing 12-month EPS (sum of latest four quarterly EPS values) divided by the latest price. | The inverse of P/E highlights how much earnings power you receive per dollar invested; higher yields can indicate cheaper valuations. |
| `eps_streak` | Number of consecutive fiscal years with positive EPS. | Consistent profitability signals durable business quality and lowers the odds that current earnings are a cyclical mirage. |
| `graham_eps_10y_cagr_3y_avg` | Computes the 10-year EPS CAGR for up to the last three years and averages the values. | Requires decade-long compounding, favoring firms that can steadily grow earnings instead of relying on one-off rebounds. |
| `graham_multiplier` | `(Price / TTM EPS) × (Price / TBVPS)`, where TBVPS is tangible book value per share (`(Equity - Goodwill - Intangibles) / Shares`). | Benjamin Graham’s combined PE×PB test guards against paying too much for either earnings or assets, enforcing a strict valuation ceiling. |
| `roc_greenblatt_5y_avg` | Average over up to five fiscal years of `EBIT / Tangible Capital`, where tangible capital is `Net PPE + AssetsCurrent - LiabilitiesCurrent`. | Joel Greenblatt’s ROC stresses whether management can reinvest incremental capital at high rates—a key quality signal for value investors who want cheap *and* good businesses. |
| `roe_greenblatt_5y_avg` | Average over up to five fiscal years of net income available to common shareholders divided by the two-year average of common equity (after subtracting preferred equity). | Sustained high ROE shows that the firm generates attractive returns on shareholders’ capital without leverage-driven distortion. |
| `price_to_fcf` | Latest market cap divided by trailing 12-month free cash flow, with FCF computed as (operating cash flow – capex) across the latest four quarters. | Cash flow–based multiples focus on hard cash instead of accounting earnings, helping avoid value traps with low-quality accrual profits. |
| `market_cap` | Latest stored market capitalization snapshot. | Screening for a minimum size filters out illiquid micro-caps where information quality and trading costs can erode returns. |
| `eps_ttm` | Sum of the most recent four quarterly EPS values. | Used to verify that current earnings have not collapsed relative to history, preventing “cheap” valuations caused by deteriorating fundamentals. |
| `eps_6y_avg` | Average of the latest six fiscal-year EPS values. | Provides a normalized earnings power baseline for comparisons against current EPS streaks or TTM values, smoothing out cyclical peaks and troughs. |

## Private configuration

Place API keys or region-specific credentials inside the `private/` directory (ignored by git).

## End-to-end workflow

1. Load the latest US universe into SQLite:
   ```bash
   pyvalue load-us-universe
   ```
2. Ingest SEC facts for every stored ticker (honors API-rate throttling):
   ```bash
   pyvalue ingest-us-facts-bulk --user-agent "pyvalue/0.1 (your@email)"
   ```
3. Normalize the ingested payloads so metrics can consume them:
   ```bash
   pyvalue normalize-us-facts-bulk
   ```
4. Fetch market data for every ticker (default EODHD, throttled to 950/min):
   ```bash
   pyvalue update-market-data-bulk
   ```
5. Compute the entire metric set for all tickers:
   ```bash
   pyvalue compute-metrics-bulk
   ```
6. Run the value screen across the universe (optional CSV export):
   ```bash
   pyvalue run-screen-bulk screeners/value.yml --output-csv results.csv
   ```
