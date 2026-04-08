# Configuration

## Local Config File

The project expects local credentials in:

```text
private/config.toml
```

This file is gitignored and should never be committed.

## EODHD Credentials

```toml
[eodhd]
api_key = "YOUR_EOD_TOKEN"
fundamentals_requests_per_minute = 950
fundamentals_daily_buffer_calls = 5000
market_data_requests_per_minute = 950
market_data_daily_buffer_calls = 5000
```

Use this for:
- `refresh-supported-exchanges --provider EODHD`
- `refresh-supported-tickers --provider EODHD`
- `ingest-fundamentals --provider EODHD`
- `report-fundamentals-progress --provider EODHD`
- `update-market-data --provider EODHD`
- `report-market-data-progress --provider EODHD`
- EODHD exchange-scoped and all-supported workflows

Optional EODHD throttling and quota settings:
- `fundamentals_requests_per_minute`: default `950`, capped at the EODHD limit of `1000`; used by `ingest-fundamentals --provider EODHD` when the scope spans many symbols
- `fundamentals_daily_buffer_calls`: default `5000`, reserved from the shared daily call budget so fundamentals ingestion stops early instead of consuming the full allowance
- `market_data_requests_per_minute`: default `950`, capped at the EODHD limit of `1000`; used by `update-market-data --provider EODHD` across both exchange-bulk and per-symbol market-data requests
- `market_data_daily_buffer_calls`: default `5000`, reserved from the shared daily call budget so market-data refresh stops early instead of consuming the full allowance

## SEC User-Agent

SEC requires a descriptive `User-Agent` with contact details.

Preferred config:

```toml
[sec]
user_agent = "pyvalue/0.1 (contact: you@example.com)"
```

You can also use an environment variable:

```bash
export PYVALUE_SEC_USER_AGENT="pyvalue/0.1 (contact: you@example.com)"
```

## FX Configuration

FX behavior is configured under an optional `[fx]` section:

```toml
[fx]
provider = "EODHD"
pivot_currency = "USD"
secondary_pivot_currency = "EUR"
stale_warning_days = 7
```

Settings:

- `provider`: default `EODHD`; `FRANKFURTER` remains available for explicit
  refreshes
- `pivot_currency`: primary triangulation pivot, default `USD`
- `secondary_pivot_currency`: optional secondary pivot after the primary
  direct/inverse lookup path, default `EUR`
- `stale_warning_days`: warn when the selected on-or-before rate is older than
  this many days

FX semantics:

- `refresh-fx-rates` is the only FX command that talks to a remote provider
- stored rates are always `1 base_currency = rate quote_currency`
- configured subunit currencies are normalized before lookup:
  `GBX`/`GBP0.01` -> `GBP`, `ZAC` -> `ZAR`, `ILA` -> `ILS`
- lookups use latest available rate on or before the requested date
- direct provider rows are persisted; inverse and triangulated cross-rates are
  computed at lookup time, not stored
- `normalize-fundamentals` uses only stored FX data and never performs runtime
  FX fetches

## Database Path Behavior

Most commands accept:

```text
--database <path>
```

If omitted, the default is:

```text
data/pyvalue.db
```

Use a separate database path when you want to:
- test workflows without touching your main dataset
- compare providers or exchanges independently
- keep one database per region or strategy

## Configuration Rules

- Keep secrets in `private/config.toml` or environment variables only.
- Do not commit tokens or personal `User-Agent` strings.
- Prefer EODHD for most workflows because market data always comes from EODHD anyway.

## Related Docs

- [EODHD Provider Guide](providers/eodhd.md)
- [SEC Provider Guide](providers/sec.md)
- [CLI Reference](reference/cli.md)
- [Getting Started](getting-started.md)
