# FX Coverage Gaps — 2026-07 normalize-fundamentals audit

Author: Emre Tezel

The 2026-07-19 full-universe `normalize-fundamentals` run logged **25,587**
"Missing FX rate for monetary conversion" warnings and **1,051** zero-share
skeleton warnings. This document records what the audit found, what was fixed
in code (commits `b19bae3`, `ea01d0d`, `5948977`), which gaps are **accepted
as unfixable with EODHD data**, and the operational follow-up needed to
materialize the fixes in `financial_facts`.

## Root causes and resolutions

| # | Cause | Warnings | Resolution |
|---|-------|----------|------------|
| 1 | Triangulation pivoted only through USD/EUR while PGK's deep history is GBP-crossed | 11,761 | **Fixed** — configurable pivot chain, default `USD, EUR, GBP` (`b19bae3`) |
| 2 | Euro legacy currencies have no market series anywhere | 4,151 | **Fixed** — statutory Council Regulation (EC) No 2866/98 rates served from code (`ea01d0d`) |
| 3 | One provider-corrupt WMT.MX period labeled PGK | 29 | **Quarantined** — `EODHD_QUARANTINED_PERIODS` + migration 085 (`b19bae3`) |
| 4 | GEL / ZMW / BRL genuinely lack provider history at the filing dates | 9,646 | **Accepted** — documented below, nothing to convert with |

The zero-share warning storm (1,051) was an unrelated sentinel problem —
`SharesStats.SharesOutstanding = 0` skeletons — fixed with non-positive-count
guards and migration 086 (`5948977`); see
`docs/reference/eodhd-concept-normalization.md`, "Shares".

## Fixed: PGK via the GBP bridge (11,761 warnings)

EODHD's FOREX catalog offers PGK only as `GBPPGK` (history from 1999-12-31),
`PGKGBP` (2004-09-22), and `USDPGK` (2024-09-27); no PGK/EUR or PGK/AUD pair
exists. All histories were fully backfilled (`fx_refresh_state.
full_history_backfilled = 1`), so the data to bridge PGK→AUD/USD/EUR through
GBP had been sitting in `fx_rates` all along — only the USD/EUR-limited pivot
set could not reach it. With GBP in the pivot chain, PGK statements convert
back to ~2000, covering the full metric window (TTM/5y/10y) of the affected
issuers:

`SST.AU BOC.AU KSL.AU BFL.AU BOCOF.US BOU1.F BOU1.MU BOU1.STU` (PNG issuers
on ASX, US OTC, and German venues).

## Fixed: euro legacy currencies (4,151 warnings)

Transition-era filings (1999-2002) in NLG, DEM, FRF, ESP, FIM, PTE, BEF, GRD
plus IEP→GBP conversions. EODHD has no pairs for dead currencies and never
will; the conversions are statutory, not market data. Served from
`EURO_LEGACY_FIXED_RATES` (`pyvalue.money.fx`) — exact by law at any date on
or after each currency's euro adoption, refused before it. 108 European
symbols affected (list reproducible from the audit log; includes `AALB.AS`,
`KBC.BR`, `BCP.LS`, `COL.MC`, `SANOMA.HE`, ...). These periods predate every
current metric window, so this is completeness, not screening impact.

## Quarantined: WMT.MX 2017-07-31 (29 warnings)

Walmex's quarterly statements contain a period ending 2017-07-31 — a fiscal
quarter end Walmex does not have — whose values are Walmart Inc's (US parent)
fiscal Q2-FY2018 balance sheet in USD (`totalAssets` 201,566,000,000) with
`currency_symbol = "PGK"` (verified against `fundamentals_raw`, 2026-07-20).
Until the GBP pivot landed, the missing PGK→MXN rate accidentally blocked the
monetary fields; the FX-free concepts (share counts, EPS) had already leaked
into `financial_facts` and were purged by migration 085. The period is now
dropped wholesale at the normalizer choke point (`EODHD_QUARANTINED_PERIODS`).

**Report to EODHD support:** the corrupt WMT.MX period, and the zero-share
skeleton payloads (1,051 symbols), are provider-side defects.

## Accepted gaps (nothing to convert with)

These stay as warnings by design; suppressing them would hide real signal.

- **GEL before 2024** (9,005 warnings). EODHD carries only `EURGEL` (from
  2024-01-02) and `USDGEL` (from 2024-10-02), both fully backfilled — the
  provider simply has no earlier Georgian-lari FX. Affected: the Georgian
  banks (`TBCG.LSE`-class LSE listings, `TBCCF.US`, `BDGSF.US`, `GRGCF.US`,
  `GEB.F`, `2IX.F/MU`, `LR6.F`, `BOU1`-era Frankfurt lines) for pre-2024
  statements. Long-horizon metrics for these issuers stay incomplete; recent
  periods convert fine. An external source (e.g. National Bank of Georgia
  official rates) would be the only remedy and is out of scope — pyvalue is
  EODHD-only.
- **ZMW before 2013** (639 warnings, all 2008-2012, Zambeef: `ZAM.LSE`,
  `MLZAM.PA`). The rebased kwacha (ZMW) only came into existence on
  2013-01-01 — EODHD's ZMW series starts exactly there, which is the
  currency's birth, not a coverage hole. Pre-2013 statements labeled "ZMW"
  are anachronistic old-kwacha (ZMK) figures; the rebase was 1,000:1, so even
  external pre-2013 rates would risk 1000× errors unless the underlying
  values were verifiably restated. Do not attempt a workaround.
- **BRL in 1972** (2 warnings, `VALE.US`). Brazil's currency in 1972 was the
  cruzeiro; BRL dates from 1994 and EODHD's `USDBRL` history from 1991.
  Ancient history with zero screening relevance.

Expected residual profile after a full re-normalization: **~9,646**
missing-FX warnings (GEL 9,005 + ZMW 639 + BRL 2), all in this accepted
category. Anything materially above that means a new gap appeared and is
worth a fresh look.

## Operational follow-up (user-triggered)

Normalization is hash-gated: unchanged raw payloads are skipped, so the FX
fixes do **not** reach `financial_facts` until the affected symbols are
re-normalized with `--force` (no EODHD API calls involved — normalization
reads stored `fundamentals_raw`):

```bash
# PGK cluster (+ WMT.MX to regenerate its facts post-quarantine)
pyvalue normalize-fundamentals --force --symbols \
  SST.AU BOC.AU KSL.AU BFL.AU BOCOF.US BOU1.F BOU1.MU BOU1.STU WMT.MX

# Euro legacy cluster (108 symbols; regenerate the list from the audit log)
cat data/logs/pyvalue.log.2026-07-19 data/logs/pyvalue.log \
  | grep 'Missing FX rate for monetary conversion' \
  | grep -E 'from=(ESP|FRF|FIM|PTE|BEF|DEM|GRD|NLG|IEP) ' \
  | grep -oE 'symbol=[^ ]+' | sed 's/symbol=//' | sort -u
pyvalue normalize-fundamentals --force --symbols <that list>
```

Then recompute metrics for the same scopes
(`pyvalue compute-metrics --symbols ...`). The PGK cluster is the only one
with current-metric impact; the euro legacy cluster only completes deep
history.

## Related

- `docs/providers/eodhd.md` — Caveats
- `docs/configuration.md` — FX configuration and semantics
- `docs/reference/eodhd-concept-normalization.md` — quarantine registry,
  share-count sentinels
- `docs/research/qarp-dvg-metric-verification-2026-07.md` — the metric-level
  audit this log review followed
