# Market Data Guide

## Scope

Market data in `pyvalue` always comes from EODHD.

Commands that accept `--provider` already default to `EODHD`, so the examples
below only include it when the extra clarity helps.

This covers:
- latest prices
- stored market-cap snapshots
- bulk quote refreshes
- later market-cap recalculation using updated share counts

## Update One Symbol

```bash
pyvalue update-market-data --symbols AAPL.US
```

If the symbol has no suffix:

```bash
pyvalue update-market-data --symbols AAPL.US
```

## Update an Exchange

```bash
pyvalue update-market-data --exchange-codes US --rate 950
```

Use `--rate` to throttle symbols per minute.

## Global Multi-Day Update

Refresh market data across the stored EODHD `supported_tickers` catalog:

```bash
pyvalue update-market-data --all-supported
```

Recommended workflow for large runs:

```bash
pyvalue refresh-supported-exchanges
pyvalue refresh-supported-tickers --all-supported
pyvalue update-market-data --all-supported
```

Important behavior:
- default freshness is `30` days
- the command selects missing symbols first, then the oldest stale symbols
- large exchange and all-supported runs can mix exchange-bulk fetches with
  per-symbol fallbacks
- it uses the EODHD daily quota and stops cleanly when the remaining budget is exhausted
- rerun it the next day to continue from the remaining stale or missing symbols

Useful options:
- `--exchange-codes`: limit the run to selected exchanges
- `--max-symbols`: cap one run
- `--max-age-days`: change the freshness window
- retry backoff is respected by default; use `--retry-failed-now` to bypass it

## Progress Reporting

To see whether market data is complete for the current scope:

```bash
pyvalue report-market-data-progress
```

The report defaults to the same 30-day freshness window and shows:
- overall status: `COMPLETE`, `INCOMPLETE`, or `BLOCKED_BY_BACKOFF`
- supported, stored, missing, stale, fresh, and blocked counts
- per-exchange breakdown
- recent failures
- remaining usable EODHD quota when available

`Stored` means a snapshot exists in the DB. `Fresh` means the symbol currently
counts as complete for the selected freshness window.

## Recalculate Market Cap

If prices were ingested before useful share-count facts were available, recompute stored market caps later:

```bash
pyvalue recalc-market-cap --exchange-codes US
```

This uses the latest price and latest normalized share count, and updates only
the latest stored `market_data.as_of` row for each selected symbol.

## Operational Notes

- Market-data freshness is separate from fundamentals freshness.
- API-call accounting is hybrid on EODHD market-data refreshes: per-symbol calls
  cost `1`, and full exchange-bulk calls cost `100`.
- Some valuation metrics require both fresh market data and fresh normalized facts.
- Market cap stored in `market_data` is a snapshot, not a time series of fully modeled enterprise value.

## Related Docs

- [EODHD Provider Guide](../providers/eodhd.md)
- [CLI Reference](../reference/cli.md)
- [Data Model and Storage](../architecture/data-model-and-storage.md)
