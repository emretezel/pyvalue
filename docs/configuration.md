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
```

Use this for:
- `load-universe --provider EODHD`
- `ingest-fundamentals --provider EODHD`
- `update-market-data`
- all bulk EODHD workflows

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
