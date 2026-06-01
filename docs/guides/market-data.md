# Market Data Guide

## Scope

Market data in `pyvalue` always comes from EODHD.

Commands that accept `--provider` already default to `EODHD`, so the examples
below only include it when the extra clarity helps.

This covers:
- latest prices
- bulk quote refreshes

Storage invariants:
- `listing.currency` is the authoritative listing quote unit and may be a
  subunit such as `GBX`, `ZAC`, or `ILA`
- `market_data.price` is stored in the **major** currency
  (`canonical_trading_currency(listing.currency)`, e.g. GBP for a GBX listing).
  Subunits never cross the data boundary: an incoming pence/cent/agorot quote is
  divided by its subunit divisor before it is written, and the snapshot read
  path reports the same base currency so the price and currency stay consistent.
- there is no stored `market_cap` column (removed in migration 072): market cap
  is computed on demand as a share-count fact x the price as of that fact's date
  (`metrics.utils.market_cap_money`)
- market-data rows do not persist a duplicate currency column

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

Refresh market data across the stored EODHD `provider_listing` catalog:

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
- suspicious price jumps are rejected before they are written to `market_data`;
  the symbol is recorded as a fetch failure in `market_data_fetch_state`
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

## Market Cap

Market cap is not stored. It is computed on demand as a share-count
`financial_facts` row x the `market_data` price *as of that fact's date*
(`metrics.utils.market_cap_money`), so a price and a share count are never
multiplied across mismatched dates. For this to resolve, `market_data` must hold
a price at or before each share-count date; a future enhancement to
`update-market-data` will backfill those share-count-dated prices alongside the
latest day.

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
