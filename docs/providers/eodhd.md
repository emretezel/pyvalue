# EODHD Provider Guide

## What EODHD Covers

EODHD is the only provider supported by `pyvalue`.

CLI commands that accept `--provider` already default to `EODHD`, so the flag
is optional.

It covers:
- global exchange universes
- global fundamentals
- all market data used by the project

For exactly how EODHD fundamentals payloads are mapped into `financial_facts` concepts, see
[EODHD Concept Normalization](../reference/eodhd-concept-normalization.md).

## Subscription Requirements

You need an active EODHD subscription for:
- fundamentals endpoints
- market data endpoints

## Universe Loading

`pyvalue` stores the EODHD supported-exchange catalog in SQLite and uses it for
exchange metadata lookups. Refresh it explicitly when you want the latest
exchange list from EODHD:

```bash
pyvalue refresh-supported-exchanges
```

An exchange EODHD no longer lists (e.g. after a plan change) is dropped from
the catalog together with its provider layer -- `provider_listing` mappings,
raw fundamentals, and fetch/normalization state -- while canonical rows and
data are retained. A drop of >= 5 exchanges exceeding half the catalog is
blocked as a suspected truncated payload unless `--allow-mass-drop` is passed.

`pyvalue` also stores a per-exchange `provider_listing` catalog for EODHD.
Refresh one exchange:

```bash
pyvalue refresh-supported-tickers --exchange-codes LSE
```

Refresh all stored exchanges:

```bash
pyvalue refresh-supported-tickers --all-supported
```

Ticker refresh keeps only `Common Stock`, `Preferred Stock`, and `Stock`.
ETF, fund, and other security types are excluded from the operational catalog.
When a ticker disappears from EODHD, only its provider layer goes: the
`provider_listing` mapping plus the provider-scoped raw fundamentals,
fetch/normalization state, and `provider_market_data` price observations tied
to it. Canonical rows (`listing`, `issuer`) and canonical data
(`financial_facts`, `market_data`, `metrics`, compute/refresh state) are
provider-independent and are never deleted by a refresh -- a payload
absence cannot distinguish a real delisting from a plan change, a provider
glitch, or a truncated response (2026-07-11 incident: a truncated 200-response
for a plan-dropped exchange nearly emptied it). A listing left with no provider
mapping is reported as orphaned and becomes unreachable through every
provider-joined scope until a provider maps it again.

Two guards protect the refresh against plan drift and bad payloads: an
exchange the plan no longer covers answers `exchange-symbol-list` with HTTP
404 and is warned about and skipped (stored data untouched), and a payload
that would remove >= 20 mappings *and* more than half of an exchange's
existing mappings is rolled back and skipped unless the operator passes
`--allow-mass-delisting`.

Example:

```bash
pyvalue refresh-supported-tickers --exchange-codes LSE
```

## Fundamentals Ingestion

Single symbol:

```bash
pyvalue ingest-fundamentals --symbols AAPL.US
```

Exchange-scoped:

```bash
pyvalue ingest-fundamentals --exchange-codes US
```

Quota-aware all-supported run across the stored supported-ticker catalog:

```bash
pyvalue ingest-fundamentals --all-supported
```

EODHD ingestion always reads from stored `provider_listing`, not from a live
symbol-list request. Refresh the ticker catalog before running it:

```bash
pyvalue refresh-supported-tickers --exchange-codes US
pyvalue ingest-fundamentals --exchange-codes US
```

Supported-ticker refresh stores catalog currency on `listing.currency` as the
listing quote unit. Tickers whose payload currency is absent or malformed
(anything other than three uppercase ASCII letters, e.g. the `'Unknown'`
placeholder) are skipped and reported in the refresh warning output.

The refresh also stores the symbol list's `Isin` field on `listing.isin` — the
identifier primary-listing classification uses to group cross-listings of one
security.

**It is a second source, not a better one.** `exchange-symbol-list` does publish
`Isin` (74.4% of WAR tickers, 66.0% of US), but largely for the same listings
whose fundamentals payload already carries it. Measured on the 2026-07-27
catalog refresh, coverage moved 53,500 → 53,558 — **58 listings**. `ITX.WAR`
illustrates the gap that remains: both sources return no ISIN for it, so nothing
links it to `ITX.MC` and it stays classified as a separate primary listing.

The write is still worth having: it covers listings that have no stored payload
at all, and it keeps the identifier current without waiting for an ingest. But
it does not rescue the ~22,600 listings that have no ISIN anywhere — closing
that gap needs evidence pyvalue does not ingest, such as the Search API's
`isPrimary` flag.

Unlike currency, a missing or malformed ISIN does not skip the ticker:
`listing.isin` is nullable and absence costs only classification precision. A
later refresh may correct a stored ISIN but never blanks one, since a payload
missing the field is far more likely to be a provider gap than a genuine
retraction.
Single-symbol fundamentals ingestion uses existing catalog
currency when one is already present and otherwise leaves listing currency
unset; it does not copy `General.CurrencyCode` from the raw payload into catalog
metadata.

For large multi-day runs:

```bash
pyvalue refresh-supported-exchanges
pyvalue refresh-supported-tickers --all-supported
pyvalue ingest-fundamentals --all-supported
```

If you upgrade an existing database and need to backfill the canonical
primary-vs-secondary listing classification without downloading anything
again, run:

```bash
pyvalue reconcile-listing-status --all-supported
```

Every other command (normalize, market-data, metrics, screening,
metadata-refresh, reports) only *reads* the cached classification -- it never
reconciles as a side effect. `ingest-fundamentals` keeps the cache
current (it reclassifies in the same transaction that stores each raw payload),
and migration 078 is the one-time backstop that resolves any leftover `unknown`
listing with stored fundamentals. Run `reconcile-listing-status` for an explicit
full re-derivation from stored raw fundamentals.

`ingest-fundamentals` checks the EODHD user/quota endpoint
before each multi-symbol run, subtracts the configured daily buffer, throttles
by requests per minute, and exits cleanly when the remaining daily allowance is
exhausted. Multi-symbol EODHD runs now use concurrent fetch workers with a
single batched SQLite writer, so exchange and all-supported runs can get much
closer to the configured request ceiling without relying on the Extended
Fundamentals bulk API. Rerun it the next day to continue from the remaining
eligible ticker set.

To see whether a multi-day run is actually complete for the current scope, use:

```bash
pyvalue report-fundamentals-progress
```

This report defaults to a 30-day freshness window. That means old
`fundamentals_raw` rows count as incomplete by default, and
`ingest-fundamentals --all-supported` now uses the same
30-day freshness window when `--max-age-days` is omitted. Use `--missing-only`
on the report if you only care whether each supported ticker has ever been
ingested once.
In the summary, `Stored` means a raw payload exists in the DB, while `Fresh`
means the ticker currently counts as complete for the selected mode/window.

Successful EODHD refreshes replace the stored raw payload for the same
provider-symbol in `fundamentals_raw`. Older historical periods remain
available through the newly stored payload and normalized downstream tables are
refreshed only when you run normalization again.

`pyvalue` classifies each canonical listing as primary, secondary or unknown on
`listing.primary_listing_status`, using an ordered rule set over several EODHD
fields rather than `General.PrimaryTicker` alone. The rule lives in
`pyvalue.universe.listing_classification`; in order, first match wins:

| # | Rule | Result |
| --- | --- | --- |
| R1 | `General.HomeCategory` is `ADR`/`BDR`/… | secondary |
| R2 | `General.PrimaryTicker` present | primary iff it names this listing, else secondary |
| R3 | No `PrimaryTicker`, but an ISIN peer has one | inherit that peer's answer |
| R4 | Otherwise | unknown |
| rescue | An R1 demotion on a *primary* US exchange with no surviving ISIN/LEI sibling | unknown |

Two things are worth understanding about why this is not simply "read
`PrimaryTicker`". EODHD leaves that field null on roughly 31% of payloads, and
the field points at the *receipt itself* for a depositary receipt
(`DNLMY.US` → `DNLMY.US`), so an ADR self-certifies as primary. R1 therefore
precedes R2, and a missing field now yields `unknown` rather than `primary`.

`unknown` means "no evidence either way" and **stays eligible** for primary-only
scopes — it is not a synonym for secondary. 19,389 listings land there: roughly
8,800 on domestic exchanges (Thailand, Korea, Taiwan, Pakistan, Indonesia) where
EODHD simply never populates `PrimaryTicker`, and roughly 10,600 quote-venue
lines. Excluding either group would silently delete real companies from the
universe.

### Every rule reads a field EODHD publishes

That is a deliberate boundary, set in 2026-07 when two further rules were
removed: an ISIN-group tie-break that preferred the venue matching the ISIN's
issuing country, and a demotion for listings quoted on a "secondary-quote venue"
(US OTC tiers, German regional exchanges, the LSE international order book, B3
BDR ticker shapes).

Both rested on a hand-coded map of venue quality that EODHD's data can neither
support nor check:

- The exchange catalog carries **no market-type field**. `F` (Frankfurt
  Exchange) and `XETRA` (XETRA Stock Exchange) are structurally identical rows —
  same country, same currency, both with a MIC — yet the rule demoted one and
  not the other.
- Reading the exchange *name* would be actively wrong: `TWO` is "Taiwan OTC
  Exchange", which is the Taipei Exchange, a genuine primary market carrying
  1,101 listings.
- `operating_mic` for `US` is the comma-joined string `XNAS, XNYS, OTCM, XCBO`,
  describing the umbrella exchange, so it cannot classify an individual listing.
- `General.InternationalDomestic` (19.3% populated) describes the *company's*
  listing footprint, not this listing's status — Dunelm's primary London line
  reads `International`. `General.Listings` is ticker-code matched rather than
  entity matched: `MTD.US` lists `MTD.F = MEITO TRANSPORT`, a Japanese trucking
  company, while omitting the `0K10.LSE` peer that matters.
- The map covered four venue families, leaving **59 of 68 exchanges** — 29,078
  of 76,151 listings, all of Shenzhen, KOSDAQ, Toronto, Australia, Korea —
  with no opinion at all.

Removing the two rules moved 10,621 listings from `secondary` to `unknown` and
1,029 back to `primary`. The accepted cost is weaker deduplication: one security
can now hold two primary lines where the vendor self-declares both (`LULU.US`
and `33L.F` on `US5500211090`), and quote-venue lines re-enter every scope.
Issuers holding more than one eligible listing went 504 → 1,262. Nothing leaves
the universe — `unknown` and `primary` are both eligible.

Measured on the QARP screen over the full universe: **96 → 134 rows, none
dropped**. Roughly 16 of the 38 additions are a company the result already held
(`LULU.US` now joined by `0JVT.LSE` and `33L.F`; `ZTS.US` by `0M3Q.LSE` and
`ZOE.MU`), 3 more are a second venue for a newly-added company (`PNT.F`/
`PNT.STU`, `TSI.DU`/`TSI.MU`, `WHX.MU`/`WHX.STU`), and the rest are names the
screen did not previously hold. Distinct companies went 94 → 121.

Four of those additions are known-bad rows: `0HS2` (Cadence), `0J8P` (IDEXX),
`0JPO` (KLA) and `0KFZ` (Parker-Hannifin) are LSE international-order-book lines
with **no ISIN**, so R3 cannot reach them and the venue rule was the only thing
holding them out. All four are confirmed false positives in
`docs/research/qarp-passers-independent-verification-2026-07.md`, caused by a
separate defect the venue rule was masking rather than fixing:
`src/pyvalue/marketdata/eodhd.py` applied the GBX subunit hint to the USD-quoted
international order book, storing those prices ~100x too low. That was never a
classification problem, and it is now **fixed** — see *Price currency* below.

`TSMC34.SA`, the fifth false positive from the same audit, does **not** return:
its payload carries an explicit `HomeCategory: BDR`, so R1 demotes it and always
did. The removed B3 ticker-shape test only ever mattered for BDRs EODHD leaves
unlabelled.

The rescue exists for the same reason. R1 assumes a depositary receipt is
redundant because the shares it wraps are listed somewhere else we can screen —
but EODHD's `HomeCategory` is loose, and 296 listings on NASDAQ/NYSE proper are
labelled `ADR` while being their issuer's only line here (Arm Holdings, AerCap,
Credicorp, Ascendis Pharma). Demoting those deduplicates nothing; it deletes the
company. The rescue fires only when the label is implausible (an exchange
listing rather than an OTC tier) *and* no sibling survives, and it returns
`unknown`, never `primary` — there is no evidence of primacy, only no reason to
exclude.

Its known cost: a receipt and its underlying are different securities with
different ISINs, so the sibling test can only see the pair when the receipt
publishes its issuer's LEI. About 69% of receipts publish none, and for those a
surviving underlying is invisible — the receipt is rescued even though the
company is already in the universe. Closing that gap needs evidence pyvalue does
not ingest; EODHD's Search API exposes an `isPrimary` flag that would settle it
directly.

Measured rather than assumed: on the live catalog the rescue fires for 311
listings and reintroduces no duplicate among the QARP passers. `NTES.US` and
`OMAB.US` both stay secondary — their siblings *are* linkable. The two extra
passers versus not rescuing at all, `DOX.US` (Amdocs) and `ITRN.US` (Ituran),
are not duplicates: neither has any surviving sibling in the universe, which is
exactly why the rescue fired.

"Surviving" means *not* `secondary`, so removing the venue rule made the rescue
strictly narrower: the 10,621 quote-venue lines that became `unknown` are now
survivors, and 12 receipts that used to be rescued stay demoted because their
sibling turns out to be visible after all. Firings went 323 → 311.

Once a
listing is classified as secondary, downstream normalization, market-data,
metric, screening, metadata-refresh, and FX-discovery scopes exclude it.
Classification writes only the status column: a secondary listing keeps its
raw payload and everything it accumulated while primary (normalized facts,
market data, metrics, refresh state). Exclusion is purely scope-side, so a
listing that later flips back to primary re-enters those scopes with its
history intact.

**`ingest-fundamentals` applies the whole rule set and leaves the result final.**
It also settles issuer identity in the same pass, so a bootstrap needs no
reconcile at all.

R3 needs a listing's ISIN peer group and the rescue needs its ISIN/LEI siblings,
neither of which one payload can supply. So each write batch re-evaluates its
whole *neighbourhood* — every listing sharing an ISIN or an issuer with one that
arrived — rather than only the listings in the batch. That reach-back is what
makes ingestion order irrelevant: `0K10.LSE` carries no `PrimaryTicker` and is
decided entirely by `MTD.US`, so whichever lands second has to reach back and
settle the other. It runs on the same connection as the payload write, so
payloads, statuses and the issuer partition commit or roll back together, and an
interrupted ingest is still correct as far as it got.

Both derived values are produced together because they are not independent:
grouping names a merged issuer after its *primary* listing, so classifying
without regrouping would leave issuer names stale.

The reach-back is cheap because the neighbourhood is small — measured at **0.12s
per 25-payload batch** on the live catalog, or roughly 6 minutes spread across a
full bootstrap's ~2,900 batches. Groups average 1.44 listings per ISIN and 74%
are singletons, so most batches expand barely at all.

`reconcile-listing-status` and `reconcile-issuer-identity` remain as **repair
tools** — for a database that got into a bad state some other way, or the
one-off pass after a migration. They run the same operation, differing only in
scope and which half of the result they emphasise:

```bash
pyvalue reconcile-listing-status --provider EODHD --all-supported
```

Each reports the resulting status counts, which rule decided each listing, how
many rows actually changed, what happened to issuer identity, and the
before/after distribution across the database. On a catalog that ingest has kept
current they report zero changes — that is the check that ingest is doing its
job. A narrow `--symbols` or `--exchange-codes` run is still correct: the scope
expands to whole peer groups first, so the rules see the same evidence they
would at full scope.

Important fundamentals options:

- `--symbols`, `--exchange-codes`, or `--all-supported`: choose the scope
- `--rate`: EODHD uses symbols per minute; default `950`, capped at `1000`
- `--max-symbols`: limit one run
- `--max-age-days`: refresh stale or missing data; default `30`
- retry backoff is respected by default; use `--retry-failed-now` to bypass it

## FX Refresh

EODHD is also the default FX provider.

Refresh FX coverage explicitly with:

```bash
pyvalue refresh-fx-rates
```

Behavior:

- syncs the EODHD FOREX catalog into `fx_supported_pairs`
- refreshes canonical six-letter pairs such as `EURUSD`
- treats three-letter shorthands such as `EUR` as aliases for `USDEUR`
  and does not refresh those aliases separately
- stores each observation twice in one transaction: the provider row in
  `provider_fx_rates` (with EODHD's pair symbol) and the canonical
  provider-free rate in `fx_rates`, which all conversion reads consume
- tracks pair coverage and retry state in `fx_refresh_state`
- backfills full available history on the first unbounded run, then refreshes
  only the missing older/newer outer ranges later
- euro legacy currencies (NLG, DEM, FRF, ...) are never refreshed: EODHD's
  FOREX catalog has no pairs for dead currencies, and none are needed -- the
  FX service serves their irrevocable statutory conversion rates from code
  (see `docs/configuration.md`, FX semantics)

If you need to limit the first backfill window:

```bash
pyvalue refresh-fx-rates --start-date 2000-01-01
```

A later unbounded run can still fill the older missing history.

## Fundamentals Normalization

Single symbol:

```bash
pyvalue normalize-fundamentals --symbols AAPL.US
```

Exchange-scoped:

```bash
pyvalue normalize-fundamentals --exchange-codes US
```

All-supported:

```bash
pyvalue normalize-fundamentals --all-supported
```

Force re-normalization:

```bash
pyvalue normalize-fundamentals --all-supported --force
```

Normalization converts raw EODHD payloads into provider-agnostic
`financial_facts` records keyed by canonical `listing_id`.
Exchange and all-supported normalization runs parallelize automatically.
By default, normalization skips symbols whose raw payload has not changed since
the last successful EODHD normalization.
Listings already classified as secondary from `General.PrimaryTicker` are
excluded from normalization scopes.
EODHD normalization requires `listing.currency`. Raw payload currencies are
source currencies only.
Monetary fact currency lookup checks entry-level `currency`,
`currency_symbol`, or `CurrencyCode`, then direct statement-level currency, then
payload-level `General.CurrencyCode`; facts are converted to
base(`listing.currency`) when the source currency differs.
Normalization never fetches FX from the network. When a symbol needs currency
conversion, each worker process resolves direct, inverse, and
configured-pivot triangulated rates (default pivots `USD, EUR, GBP`; see
`docs/configuration.md`) from the stored canonical `fx_rates` series, loading
each pair's history lazily and caching it in memory. A fact whose conversion
cannot be resolved is skipped with a structured warning.
Run `refresh-fx-rates` before normalization when the database does not already
contain the required history.

## Market Data

Market data is always fetched from EODHD.

Single symbol:

```bash
pyvalue update-market-data --symbols AAPL.US
```

Exchange-scoped:

```bash
pyvalue update-market-data --exchange-codes US
```

Quota-aware all-supported run across the stored supported-ticker catalog:

```bash
pyvalue update-market-data --all-supported
```

For large multi-day runs:

```bash
pyvalue refresh-supported-exchanges
pyvalue refresh-supported-tickers --all-supported
pyvalue update-market-data --all-supported
```

`update-market-data` checks the EODHD user/quota endpoint
before each multi-symbol run, subtracts the configured daily buffer, throttles
by requests per minute, and exits cleanly when the remaining daily allowance is
exhausted. Market-data refreshes use hybrid accounting: per-symbol requests
cost one EODHD API call, while exchange-bulk refreshes cost `100` API calls for
the exchange. Large exchange and all-supported runs can therefore move through
the supported universe much faster than a pure per-symbol loop while staying
quota-aware.

To see whether a multi-day market-data run is actually complete for the current
scope, use:

```bash
pyvalue report-market-data-progress
```

This report defaults to a 30-day freshness window. A symbol is incomplete when
its latest stored `market_data.as_of` is missing or older than the selected
window. In the summary, `Stored` means a market-data snapshot exists in the DB,
while `Fresh` means the symbol currently counts as complete for the selected
window.

Important market-data options:

- `--symbols`, `--exchange-codes`, or `--all-supported`: choose the scope
- `--rate`: requests per minute, capped at the EODHD limit of `1000`
- `--max-symbols`: limit one run
- `--max-age-days`: refresh stale or missing market data; default `30`
- listings already classified as secondary from raw fundamentals are excluded
  before market-data refresh planning and progress accounting
- retry backoff is respected by default; use `--retry-failed-now` to bypass it

Market cap is not stored; it is computed on demand as the latest share-count fact
x the latest `market_data` price (`metrics.utils.market_cap_money`).

### Price currency

**Neither EOD endpoint states a currency.** `/api/eod` and
`/api/eod-bulk-last-day` both return OHLCV plus a date and nothing more —
verified against the live API. The quote unit comes from
`exchange-symbol-list`, which gives it per listing (`"Currency": "USD"` for
`0HS2.LSE`, Cadence Design Systems' London line), and pyvalue stores it in
`listing.currency`.

That column is therefore the **sole** source of a price's currency. The provider
type `PriceQuote` has no currency field, and `prepare_price_data` takes the
listing currency as a required argument with no fallback, so the collapse of a
subunit quote (`GBX` -> GBP) is driven entirely by the declared code.

This replaced a rule that derived the currency from the exchange suffix and the
price magnitude (`LSE` + `price > 100` implied pence). It was wrong for
two-thirds of London — the live catalog holds 2,467 `GBX` listings against
2,253 `USD`, 1,135 `EUR`, 912 `GBP`, 212 `SEK` and a long tail — and, being
keyed on magnitude, it also made a single listing's stored series jump 100x
whenever its quote crossed 100. Prices ingested before the fix remain scaled
wrongly until those exchanges are re-fetched; re-run `update-market-data` for
`LSE`, `JSE` and `TA`, then recompute their metrics.

## EODHD-Oriented Metrics

Many newer metrics in the project are intentionally EODHD-oriented because they rely on normalized concepts or fallback logic designed around EODHD payload structure. The metrics catalog marks this in the calculation column where relevant.

## Caveats

- Exchange suffixes matter: use `AAPL.US`, `SHEL.LSE`, etc.
- Some fields are normalized through EODHD-specific fallback chains; compute metrics only after normalization.
- Market data freshness is independent from fundamentals freshness.
- FX coverage has known, accepted gaps — GEL before 2024, ZMW before 2013
  (the rebased kwacha's birth), BRL before 1991 — that keep a residual
  ~9.6k missing-FX warnings on a full-universe normalization; frontier
  currencies with GBP-only depth (PGK) and euro legacy currencies are
  handled by the pivot chain and statutory rates instead. Full audit:
  `docs/research/fx-coverage-gaps-2026-07.md`.
- One provider-corrupt statement period (WMT.MX 2017-07-31, Walmart Inc USD
  data labeled PGK) is quarantined at normalization
  (`EODHD_QUARANTINED_PERIODS`); report new instances to EODHD support and
  extend the registry.
- Dead-symbol skeleton payloads carry `SharesStats` zeros ("no data"
  sentinels); non-positive share counts are never stored.

## Related Docs

- [Configuration](../configuration.md)
- [Market Data Guide](../guides/market-data.md)
- [Ingestion and Normalization Guide](../guides/ingestion-and-normalization.md)
