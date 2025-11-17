# pyvalue

Fundamental data ingestion and screening toolkit focusing on value-oriented strategies. Currently supports fetching the US equity universe.

## Quick start

```bash
python -m pip install -e .[dev]
pytest
```

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
pyvalue load-us-universe --database data/pyvalue.db
```

ETFs are excluded by default; pass `--include-etfs` to store them as well.

> Nasdaq serves the symbol directories via FTP (`ftp://ftp.nasdaqtrader.com/symboldirectory/...`).
> You can verify availability manually with
> `curl "ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt"`.

## SEC company facts

SEC requires a descriptive `User-Agent` header that includes contact details. Set an
environment variable such as:

```bash
export PYVALUE_SEC_USER_AGENT="pyvalue/0.1 (contact: you@example.com)"
```

Then ingest the latest company facts for a ticker (AAPL shown below):

```bash
pyvalue ingest-us-facts AAPL --database data/pyvalue.db
```

This downloads the JSON payload from `https://data.sec.gov/api/xbrl/companyfacts/…` and
stores it in the `company_facts` table. Pass `--cik` if you already know the exact CIK.

Normalize the previously ingested payload into structured rows for downstream metrics:

```bash
pyvalue normalize-us-facts AAPL --database data/pyvalue.db
```

This populates the `financial_facts` table with the concepts required to compute the
initial metric set (debt, current assets/liabilities, EPS, dividends, cash flow, etc.).

## Market data (Alpha Vantage)

Store your Alpha Vantage API key in `private/config.toml`:

```toml
[alpha_vantage]
api_key = "YOUR_KEY"
```

Fetch the latest quote and persist it in `market_data`:

```bash
pyvalue update-market-data AAPL
```

The market data service currently uses Alpha Vantage's `GLOBAL_QUOTE`, but the design
allows swapping providers by injecting a different implementation later.

## Metrics and screening

Compute metrics (e.g., working capital, long-term debt) for a ticker:

```bash
pyvalue compute-metrics AAPL --metrics working_capital long_term_debt
```

Define screening criteria in YAML (see `screeners/low_debt.yml`) and evaluate them:

```bash
pyvalue run-screen AAPL screeners/low_debt.yml
```

The sample screen checks whether long-term debt is less than or equal to 1.75× working
capital. Metrics are cached in the `metrics` table for reuse.

Additional metrics include:

- `eps_streak`: Counts consecutive positive EPS (diluted) FY values.
- `graham_eps_10y_cagr_3y_avg`: Graham EPS 10-year CAGR averaged over the latest three
  periods (using full-year GAAP EPS data).

## Private configuration

Place API keys or region-specific credentials inside the `private/` directory (ignored by git).
