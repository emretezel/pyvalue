# Anchor Watchlist

The anchor watchlist is the fixed set of stocks the author cares about when tuning
screeners: every criteria metric of `screeners/deep_value_graham.yml` and
`screeners/quality_reasonable_price_primary.yml` should be computable for these
listings, and any NA here is treated as a defect to be root-caused (see
[Screener NA Investigation](../research/screener-na-investigation.md)).

This file is the canonical record of the watchlist; update it here (and only here)
when the set changes.

## Tracked stocks (10)

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

`listing_id` values are recorded so ad-hoc read-only SQL can target these rows
without symbol joins.

## Paste-ready scope line

All pyvalue commands share one scope resolver, so the watchlist can be passed
verbatim to any of them:

```
--symbols MSFT.US GOOGL.US ADBE.US NVDA.US AMD.US C.US PLTR.US TSLA.US INTC.US 000660.KO
```

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
