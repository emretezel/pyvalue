# SEC Provider Guide

## What SEC Covers

The SEC provider supports US company facts only.

Use it when you want:
- US issuer fundamentals from SEC filings
- an alternative to EODHD fundamentals for US names

## SEC User-Agent Requirement

SEC requests must include a descriptive `User-Agent` with contact details.

Example:

```bash
export PYVALUE_SEC_USER_AGENT="pyvalue/0.1 (contact: you@example.com)"
```

Or configure it in `private/config.toml`.

## Fundamentals Ingestion

Catalog refresh:

```bash
pyvalue refresh-supported-exchanges --provider SEC
pyvalue refresh-supported-tickers --provider SEC --exchange-codes US
```

Single symbol:

```bash
pyvalue ingest-fundamentals --provider SEC --symbols AAPL.US
```

Optional override:
- `--cik`: provide the exact SEC CIK if needed

Exchange-scoped:

```bash
pyvalue ingest-fundamentals --provider SEC --exchange-codes US
```

## Normalization

Single symbol:

```bash
pyvalue normalize-fundamentals --provider SEC --symbols AAPL.US
```

Exchange-scoped:

```bash
pyvalue normalize-fundamentals --provider SEC --exchange-codes US
```

## Important Limitations

Compared with EODHD:
- field coverage is less standardized
- concept availability is less consistent across issuers
- many EODHD-oriented metrics may not compute from SEC data

`pyvalue` stores normalized facts provider-agnostically, but the quality and breadth of those facts depends on the upstream provider.

## Practical Recommendation

Prefer EODHD for production-style screening workflows, especially if you need:
- global exchanges
- market data
- newer quality/value metrics with richer fallback logic

## Related Docs

- [Configuration](../configuration.md)
- [Ingestion and Normalization Guide](../guides/ingestion-and-normalization.md)
