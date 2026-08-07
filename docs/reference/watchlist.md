# Anchor Watchlist

The anchor watchlist is the fixed set of stocks the author cares about when tuning
screeners: every criteria metric of `screeners/deep_value_graham.yml` and
`screeners/quality_reasonable_price_primary.yml` should be computable for these
listings, and any NA here is investigated rather than accepted (see
[Screener NA Investigation](../research/screener-na-investigation.md)).

An NA is a **defect** when the inputs exist and pyvalue fails to use them. It is
**structural** when the guard is right and the number is genuinely undefined — as
with Cloudflare, whose unbroken loss history makes six criteria metrics
undefinable by construction (see its profile below). Root-cause every NA; fix
only the first kind.

This file is the canonical record of the watchlist; update it here (and only here)
when the set changes.

## Tracked stocks (11)

| Canonical symbol | listing_id | Issuer | Exchange | Currency |
|---|---|---|---|---|
| MSFT.US | 65235 | Microsoft Corporation | US | USD |
| GOOGL.US | 61081 | Alphabet Inc Class A | US | USD |
| ADBE.US | 53407 | Adobe Systems Incorporated | US | USD |
| NVDA.US | 66246 | NVIDIA Corporation | US | USD |
| AMD.US | 54072 | Advanced Micro Devices Inc | US | USD |
| C.US | 56282 | Citigroup Inc. | US | USD |
| PLTR.US | 67393 | Palantir Technologies Inc. | US | USD |
| TSLA.US | 71610 | Tesla Inc | US | USD |
| INTC.US | 62587 | Intel Corporation | US | USD |
| 000660.KO | 24625 | SK Hynix Inc | KO (KOSPI) | KRW |
| NET.US | 65665 | Cloudflare Inc | US | USD |

`listing_id` values are recorded so ad-hoc read-only SQL can target these rows
without symbol joins.

Cloudflare (`NET.US`) joined the watchlist on 2026-08-07. It was already in the
catalog as a `primary` US listing, so the addition was documentation only — no
catalog row was created. It is the only watchlist name that has **never** posted
a profitable GAAP fiscal year; see
[Cloudflare's QARP profile](#cloudflare-netus-qarp-profile-2026-08-07) below.

## Paste-ready scope line

All pyvalue commands share one scope resolver, so the watchlist can be passed
verbatim to any of them:

```
--symbols MSFT.US GOOGL.US ADBE.US NVDA.US AMD.US C.US PLTR.US TSLA.US INTC.US 000660.KO NET.US
```

## Cloudflare (NET.US) QARP profile, 2026-08-07

Recorded at the time of addition, from a full pipeline run (fundamentals
re-ingested, price refreshed, facts re-normalized with `--force`, all 93 metrics
recomputed). Price $284.43 as of 2026-08-06; market cap $100.3B.

Cloudflare **fails QARP**, 2 gates of 14:

| Gate | Result | Value |
|---|---|---|
| Gross margin (TTM) ≥ 35% | **pass** | 73.3% |
| GM stability (10Y std) ≤ 7pp | **pass** | 1.9pp |
| ROIC years >12% ≥ 7/10 | NA | non-positive average invested capital |
| ROIC 10Y min ≥ 7% | NA | same guard |
| OPM 7Y min ≥ 5% | FAIL | −37.6% |
| CFO/NI TTM ≥ 0.90 | NA | non-positive TTM net income |
| CFO/NI 10Y median ≥ 0.90 | NA | too few positive-NI FY years |
| Net debt/EBITDA ≤ 2.5x | NA | non-positive EBITDA |
| Debt service (6x cov or ≤1.5x) | FAIL | both arms NA (non-positive EBIT/EBITDA) |
| No NI loss years (10Y) | FAIL | 10 of 10 |
| Share CAGR 5Y ≤ +1% | FAIL | +2.9% (10Y: +16.6%) |
| Price gate (OEY/EBIT/FCF yield) | FAIL | −0.14% / −0.22% / +0.36% |
| iROIC 5Y ≥ 12% | FAIL | −2.9% |
| Owner-earnings CAGR 10Y ≥ 7% | NA | too few consecutive FY owner-earnings points |

The NAs are **not defects**. Cloudflare has posted a GAAP loss in all ten stored
fiscal years (FY2016 −$17.3M through FY2025 −$102.3M) and negative operating
income throughout, so the profitability-based guards correctly refuse to produce
a number. QARP is built on 10-year ROIC persistence, margin floors, no-loss-years
and owner-earnings compounding; a company that has never earned a GAAP profit is
structurally outside what the screen selects for. The two passing gates are real
and notable — 73% gross margin with 1.9pp of ten-year variation is elite pricing
power. The balance sheet is also sound (`altman_z` 13.4, `current_ratio` 1.96,
net cash: `net_debt_to_ebitda` −6.5).

### Known vendor caveat: non-GAAP EPS on a GAAP net income

EODHD's `EarningsPerShare` series for this listing is **non-GAAP** while
`NetIncomeLoss` is **GAAP**. FY2025 shows EPS +0.92 against net income
−$102.3M; the gap is essentially stock-based compensation (`sbc_to_revenue`
20.2%, `sbc_to_fcf` 132%). Consequences:

- `eps_ttm` (+0.67) and `earnings_yield` (0.24%) are non-GAAP-derived and
  disagree in sign with every NI-derived metric (`roe_ttm` −5.88%,
  `roa_ttm` −1.75%)
- **no QARP criterion reads any of the three**, so the verdict above is
  unaffected

Not fixed at the time of addition — recorded here because the contradiction is
visible in the stored metrics and will otherwise be re-discovered. It is likely
universe-wide for heavy-SBC issuers rather than specific to Cloudflare.

## SK Hynix cross-listings (known, NOT tracked)

Only the Korean home listing `000660.KO` is tracked — it has by far the deepest
fact history (FY facts back to 2000). Two depositary-receipt listings for the same
company exist in the catalog but are deliberately not part of the watchlist; their
fact histories are short and their NA analysis is out of scope:

| Symbol | listing_id | Note |
|---|---|---|
| HY9H.F | 15490 | Sponsored GDR, Frankfurt, EUR |
| HXSCL.US | 62125 | ADR, US OTC, USD |

Catalog caveat: each of the three SK Hynix listings sits under its **own** issuer
row, and all three carry `primary_listing_status = 'primary'`. The flag is
per-issuer, so it does not identify the home listing among cross-listings — the
catalog currently has no issuer-level link between them. Treat `000660.KO` as the
canonical SK Hynix listing by convention (this file), not by schema.
