# EODHD Subscription Schedule

The EODHD subscription is bought four times a year, one month at a time, to
refresh fundamentals, prices and FX for the holdings. This file is the
canonical schedule: the four windows, the reporting calendars they are derived
from, and the run sheet for each window. Update it here (and only here) when
the holdings change or a window misses a filing.

The schedule is driven by the five holdings and two candidates below. The
anchor watchlist in [Anchor Watchlist](../reference/watchlist.md) is wider;
the windows are chosen for these seven and then applied to the whole watchlist
scope line.

| Symbol | Company | Role |
|---|---|---|
| INTC.US | Intel | holding |
| GOOGL.US | Alphabet | holding |
| C.US | Citigroup | holding |
| ADBE.US | Adobe | holding |
| 000660.KO | SK Hynix | holding |
| MC.PA | LVMH | candidate |
| NOVO-B.CO | Novo Nordisk | candidate |

## The four windows

| Window | Quarter captured | Last filing waited for | Picked up at the window's end |
|---|---|---|---|
| 20 Feb – 20 Mar | Q4 and full year | Citi 10-K (20-21 Feb); Novo Nordisk (3-4 Feb) | Adobe Q1 (12 Mar); SK Hynix audited annual report (mid-March) |
| 15 May – 15 Jun | Q1 | Novo Nordisk (5-7 May); Citi 10-Q (7-8 May) | Adobe Q2 (11-13 Jun) |
| 15 Aug – 15 Sep | Q2 / H1 | Novo Nordisk (5-7 Aug); Citi 10-Q (1-6 Aug) | Adobe Q3 (10-12 Sep) |
| 15 Nov – 15 Dec | Q3 | Novo Nordisk (5-6 Nov); Citi 10-Q (6-7 Nov) | Adobe Q4 (10-11 Dec) |

Prices and FX are refreshed early in the window, fundamentals in the last
three days. The split matters: the fundamentals fetch is what the window is
timed for, and running it late absorbs EODHD's own ingestion lag and catches
Adobe's next quarter for free.

## Reporting calendars

Days from quarter end to the earnings release, as EODHD records them in
`Earnings.History.reportDate` for 2024-2026 (snapshot of `fundamentals_raw`
taken 2026-10-18). The Citi filing columns come from
`Financials.Balance_Sheet.quarterly.filing_date`, which EODHD stamps with the
10-Q / 10-K date once the SEC filing lands.

| Company | Days after quarter end | Typical release dates | Cadence |
|---|---|---|---|
| Citigroup | 12-15 (10-Q about 37; 10-K about 52) | 14 Jan, 14 Apr, 14 Jul, 14 Oct | quarterly |
| Adobe | 10-13 on its own fiscal quarters (end Feb / May / Aug / Nov) | 12 Mar, 12 Jun, 11 Sep, 10 Dec; 10-K mid-January | quarterly |
| SK Hynix | 22-29 | 22-28 Jan, 22-24 Apr, 23-29 Jul, 23-28 Oct | quarterly |
| Intel | 23-37 | 23-31 Jan, 24-25 Apr, 24 Jul, 23 Oct - 6 Nov | quarterly |
| Alphabet | 23-36 | 5 Feb, 25-30 Apr, 23-24 Jul, 30 Oct | quarterly |
| LVMH | 23-28 | 27-28 Jan, 23-24 Jul | **semi-annual only** |
| Novo Nordisk | 32-38 | 3-4 Feb, 2-7 May, 5-7 Aug, 5-6 Nov | quarterly |

## Why these dates

- **Novo Nordisk is the binding constraint.** It reports five weeks after
  quarter end, later than every other name. A window that opens on the 15th
  to 20th of the second month after quarter end clears Novo plus whatever
  ingestion lag EODHD adds.
- **Citi's balance-sheet detail follows the SEC filing, not the press
  release.** EODHD stamps Citi's settled quarters with the 10-Q date (about
  37 days after quarter end) and the full year with the 10-K in late
  February. Opening the annual window on 20 February rather than the 15th
  picks that up; the other three windows open after the 10-Q is in.
- **Adobe is offset by two months.** Its quarters end in February, May,
  August and November and it reports within two weeks, so a fundamentals
  fetch in the final days of each window captures its newest quarter. The
  database never falls more than one Adobe quarter behind.
- **LVMH publishes full financials twice a year.** EODHD has no Q1 or Q3
  earnings rows for it at all. The February and August windows bring new LVMH
  statements; May and November bring nothing new for it. That is LVMH's
  design, not a timing gap.
- **Prices and FX are not timing-sensitive.** `update-market-data` stores
  only the latest close per listing (no daily history), so any day inside the
  window is as good as any other. `refresh-fx-rates` tops up the stored FX
  series from the last stored date, so the three-month gap is backfilled
  automatically.

## Run sheet

Run everything in the `pyvalue` conda environment. The scope line below
covers the seven names that drive the schedule; swap in the paste-ready
watchlist scope line from [Anchor Watchlist](../reference/watchlist.md) to
refresh the full watchlist in the same window.

Early in the window (prices and FX):

```bash
pyvalue refresh-fx-rates
pyvalue update-market-data --symbols INTC.US GOOGL.US C.US ADBE.US 000660.KO MC.PA NOVO-B.CO
```

In the last three days of the window (fundamentals, then the derived layers):

```bash
pyvalue ingest-fundamentals --symbols INTC.US GOOGL.US C.US ADBE.US 000660.KO MC.PA NOVO-B.CO --max-age-days 0
pyvalue normalize-fundamentals --symbols INTC.US GOOGL.US C.US ADBE.US 000660.KO MC.PA NOVO-B.CO
pyvalue compute-metrics --symbols INTC.US GOOGL.US C.US ADBE.US 000660.KO MC.PA NOVO-B.CO
pyvalue update-market-data --symbols INTC.US GOOGL.US C.US ADBE.US 000660.KO MC.PA NOVO-B.CO --max-age-days 0
```

`--max-age-days 0` forces the fetch even when the previous one is inside the
default 30-day freshness window, which it will be if the fundamentals are
fetched twice in one window. The second `update-market-data` call re-prices
the metrics at the window's closing price.

Then confirm that every name's newest quarter moved:

```sql
SELECT l.symbol || '.' || e.exchange_code AS symbol,
       MAX(json_extract(q.value, '$.date')) AS newest_quarter,
       substr(fr.last_fetched_at, 1, 10) AS fetched_on
FROM fundamentals_raw fr
JOIN provider_listing pl ON pl.provider_listing_id = fr.provider_listing_id
JOIN listing l ON l.listing_id = pl.listing_id
JOIN exchange e ON e.exchange_id = l.exchange_id,
     json_each(fr.data, '$.Financials.Balance_Sheet.quarterly') q
WHERE l.symbol || '.' || e.exchange_code
      IN ('INTC.US', 'GOOGL.US', 'C.US', 'ADBE.US', '000660.KO', 'MC.PA', 'NOVO-B.CO')
GROUP BY fr.provider_listing_id
ORDER BY symbol;
```

Every row's `newest_quarter` should equal the quarter the window targets
(LVMH: the half-year, and only in the February and August windows). If one
name has not moved, re-run `ingest-fundamentals` for it on the window's last
day with `--max-age-days 0`; EODHD's lag after a Copenhagen, Paris or Korea
release is less predictable than after a US filing.

## Keeping the schedule current

When a holding is added or dropped, re-derive the binding constraint. The
report dates EODHD holds for any listing come from the same raw payload:

```sql
SELECT l.symbol || '.' || e.exchange_code AS symbol,
       json_extract(h.value, '$.date') AS quarter_end,
       json_extract(h.value, '$.reportDate') AS report_date
FROM fundamentals_raw fr
JOIN provider_listing pl ON pl.provider_listing_id = fr.provider_listing_id
JOIN listing l ON l.listing_id = pl.listing_id
JOIN exchange e ON e.exchange_id = l.exchange_id,
     json_each(fr.data, '$.Earnings.History') h
WHERE l.symbol || '.' || e.exchange_code IN ('NOVO-B.CO')
  AND json_extract(h.value, '$.date') >= '2025-01-01'
ORDER BY symbol, quarter_end DESC;
```

A new name that reports later than Novo Nordisk, or one that reports only
semi-annually, shifts the window it falls in; everything else slots into the
existing four windows unchanged.

EODHD's own lag between a release and the payload update cannot be measured
from a single snapshot of `fundamentals_raw`. Record what each window
actually observed here so the opening dates can be tightened or relaxed on
evidence:

| Window | Fetched on | Names still stale at the first fetch | Resolved by |
|---|---|---|---|
| _none yet_ | | | |

## Related Docs

- [EODHD Provider Guide](eodhd.md)
- [Anchor Watchlist](../reference/watchlist.md)
- [CLI Reference](../reference/cli.md)
