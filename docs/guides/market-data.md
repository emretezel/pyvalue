# Market Data Guide

## Scope

Market data in `pyvalue` always comes from EODHD.

This covers:
- latest prices
- stored market-cap snapshots
- bulk quote refreshes
- later market-cap recalculation using updated share counts

## Update One Symbol

```bash
pyvalue update-market-data AAPL.US
```

If the symbol has no suffix:

```bash
pyvalue update-market-data AAPL --exchange-code US
```

## Bulk Update an Exchange

```bash
pyvalue update-market-data-bulk --exchange-code US --rate 950
```

Use `--rate` to throttle symbols per minute.

## Recalculate Market Cap

If prices were ingested before useful share-count facts were available, recompute stored market caps later:

```bash
pyvalue recalc-market-cap --exchange-code US
```

This uses the latest price and latest normalized share count.

## Operational Notes

- Market-data freshness is separate from fundamentals freshness.
- Some valuation metrics require both fresh market data and fresh normalized facts.
- Market cap stored in `market_data` is a snapshot, not a time series of fully modeled enterprise value.

## Related Docs

- [EODHD Provider Guide](../providers/eodhd.md)
- [CLI Reference](../reference/cli.md)
- [Data Model and Storage](../architecture/data-model-and-storage.md)
