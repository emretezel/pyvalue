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
fundamentals_requests_per_minute = 600
fundamentals_daily_buffer_calls = 5000
```

Use this for:
- `load-universe --provider EODHD`
- `refresh-supported-exchanges --provider EODHD`
- `refresh-supported-tickers --provider EODHD`
- `ingest-fundamentals --provider EODHD`
- `ingest-fundamentals-global --provider EODHD`
- `update-market-data`
- exchange and global EODHD workflows

Optional EODHD throttling and quota settings:
- `fundamentals_requests_per_minute`: default `600`, capped at the EODHD limit of `1000`; used by `ingest-fundamentals-global`
- `fundamentals_daily_buffer_calls`: default `5000`, reserved from the daily call budget so global ingestion stops early instead of consuming the full allowance

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
- [Getting Started](getting-started.md)
