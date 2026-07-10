"""Company-total share-count resolution shared by the share-basis metrics.

EODHD gives pyvalue two *current* share-count sources, and neither is right for
every issuer (2026-07 P/B investigation, ``docs/research/
qarp-dvg-metric-verification-2026-07.md``):

- the **snapshot**: ``SharesStats.SharesOutstanding``, normalized as the
  ``INSTANT`` row of ``CommonStockSharesOutstanding`` dated by EODHD's refresh
  timestamp. Fresh and correct for single-class issuers (TSLA, C), but for
  dual-class issuers it counts only the listed class (GOOGL Class A 5.82B vs
  the 12.23B company total -- P/B understated 2.1x);
- the **periodic** rows: the filing-keyed Q/FY history of
  ``EntityCommonStockSharesOutstanding`` / ``CommonStockSharesOutstanding``.
  The company total for dual-class filers, but a weighted-average (TSLA) or
  issued-incl-treasury (C) figure for others (~6% off).

The tie-breaker is the provider's own headline market capitalization
(``ProviderMarketCapitalization``, from ``Highlights.MarketCapitalization``),
which EODHD always computes as last close x the *company total*: dividing it by
the stored close nearest its own date yields an implied total share count, and
whichever stored candidate lies closer (in log space) is the real total. The
implied figure itself is discarded -- metrics always consume a *reported*
count, so anchor/price noise never enters a metric value.

Consumers: ``market_cap_money`` (and through it the EV family),
``price_to_book`` / ``price_to_tangible_book``, and ``graham_multiplier``.
FY-*history* consumers (piotroski F7, share-count CAGRs, FY diluted maps) need
period-consistent series, not the current count, and deliberately do not use
this resolver.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from typing import Literal, Optional

from pyvalue.currency import normalize_currency_code
from pyvalue.facts import RawFactSource
from pyvalue.persistence.storage import FactRecord, MarketDataRepository

LOGGER = logging.getLogger(__name__)

# Shares-outstanding concepts, in periodic-candidate priority. Entity first:
# on equal end_date it matches the pre-resolver market-cap behaviour (the two
# concepts are fed from the same statement sections and rarely disagree).
SHARE_COUNT_CONCEPTS: tuple[str, ...] = (
    "EntityCommonStockSharesOutstanding",
    "CommonStockSharesOutstanding",
)

# Only the Common concept ever carries the provider snapshot: the normalizer
# emits SharesStats.SharesOutstanding as its single INSTANT row.
SNAPSHOT_SHARE_CONCEPT: str = "CommonStockSharesOutstanding"

# The arbitration anchor emitted by the EODHD normalizer (INSTANT, monetary,
# dated by the provider refresh timestamp). Arbitration evidence only -- never
# a metric output.
PROVIDER_MARKET_CAP_CONCEPT: str = "ProviderMarketCapitalization"

# What a resolver-backed metric must declare in ``required_concepts`` so the
# compute driver's per-listing fact preload includes every row the resolver
# may read (an undeclared concept comes back empty from the cache, silently).
SHARE_RESOLVER_REQUIRED_CONCEPTS: tuple[str, ...] = (
    *SHARE_COUNT_CONCEPTS,
    PROVIDER_MARKET_CAP_CONCEPT,
)

# <=2%: post-filing buybacks/issuance drift within a quarter. A class split or
# ADS mismatch is >=10%, so agreement this tight means both rows describe the
# same total and the fresher snapshot wins without consulting the anchor.
SNAPSHOT_AGREEMENT_MAX_RATIO: float = 1.02

# >=25% cannot be buyback or weighted-average drift: the observed single-class
# periodic artifacts (TSLA weighted-average ~6%, Citi issued-incl-treasury
# ~6.5%) sit well below, true class splits (GOOGL 2.10x, ADS ratios) far above.
# With no anchor available, gaps this large default to the filing-based total.
STRUCTURAL_DIVERGENCE_MIN_RATIO: float = 1.25

# The anchor must land within 2x of the candidate it endorses, else the
# provider cap is stale or garbled (e.g. computed off a sentinel price) and is
# ignored in favour of the anchorless policy. Distances are multiplicative
# ratios (max/min, always >= 1) rather than |log| values: the ordering is
# identical, but a genuine geometric tie (e.g. candidates 80 and 125 around an
# anchor of 100) compares 1.25 == 1.25 exactly instead of two log floats that
# differ in the last ulp.
ANCHOR_MAX_DISTANCE_RATIO: float = 2.0

# The provider cap is last-close x shares at its own refresh date; a close
# more than ~2 trading weeks away prices a different market regime and is
# worse than no anchor at all.
ANCHOR_PRICE_MAX_DISTANCE_DAYS: int = 10

ShareCountChoice = Literal["snapshot", "periodic"]


def arbitrate_share_count(
    snapshot: Optional[float],
    periodic: Optional[float],
    implied: Optional[float],
) -> Optional[ShareCountChoice]:
    """Pick the candidate that is the company-total share count.

    Pure decision rule (the property-tested core): ``snapshot`` is the provider
    SharesStats count, ``periodic`` the latest filing-based count, ``implied``
    the anchor-derived estimate (provider market cap / same-date close), any of
    which may be absent. Non-positive inputs are treated as absent. Returns
    which candidate to use, or ``None`` when neither exists.

    Order of the rules matters and is deliberate:

    1. one candidate -> use it (nothing to arbitrate);
    2. agreement within ``SNAPSHOT_AGREEMENT_MAX_RATIO`` -> snapshot (fresher);
    3. anchored -> the candidate at the smaller multiplicative distance from
       the anchor, provided the winner is within
       ``ANCHOR_MAX_DISTANCE_RATIO`` of it; a geometric tie goes to the
       periodic row (the filing-based figure is the safer company-total
       claim);
    4. anchorless -> periodic for structural gaps
       (>= ``STRUCTURAL_DIVERGENCE_MIN_RATIO``), snapshot for small ones
       (the pre-resolver status quo, bounded by the band).
    """

    if snapshot is not None and snapshot <= 0:
        snapshot = None
    if periodic is not None and periodic <= 0:
        periodic = None
    if implied is not None and implied <= 0:
        implied = None

    if snapshot is None and periodic is None:
        return None
    if periodic is None:
        return "snapshot"
    if snapshot is None:
        return "periodic"

    ratio = max(snapshot, periodic) / min(snapshot, periodic)
    if ratio <= SNAPSHOT_AGREEMENT_MAX_RATIO:
        return "snapshot"

    if implied is not None:
        snapshot_distance = max(snapshot, implied) / min(snapshot, implied)
        periodic_distance = max(periodic, implied) / min(periodic, implied)
        if min(snapshot_distance, periodic_distance) <= ANCHOR_MAX_DISTANCE_RATIO:
            if snapshot_distance < periodic_distance:
                return "snapshot"
            return "periodic"

    return "periodic" if ratio >= STRUCTURAL_DIVERGENCE_MIN_RATIO else "snapshot"


def resolve_current_share_count(
    listing_id: int,
    repo: RawFactSource,
    market_repo: MarketDataRepository,
) -> Optional[FactRecord]:
    """Return the fact carrying the best current company-total share count.

    Gathers the snapshot and periodic candidates, computes the anchor-implied
    total lazily (only when the candidates actually disagree beyond the
    agreement band -- the common case never touches ``market_data``), and
    returns the *chosen fact record* so callers keep provenance and apply
    their own recency policy (`market_cap` has none today; P/B and Graham gate
    on ``is_recent_fact``).
    """

    snapshot = _snapshot_candidate(listing_id, repo)
    periodic = _periodic_candidate(listing_id, repo)

    implied: Optional[float] = None
    if snapshot is not None and periodic is not None:
        ratio = max(snapshot.value, periodic.value) / min(
            snapshot.value, periodic.value
        )
        if ratio > SNAPSHOT_AGREEMENT_MAX_RATIO:
            implied = _implied_total_shares(listing_id, repo, market_repo)

    choice = arbitrate_share_count(
        snapshot.value if snapshot is not None else None,
        periodic.value if periodic is not None else None,
        implied,
    )
    if choice is None:
        return None
    chosen = snapshot if choice == "snapshot" else periodic
    if chosen is not None and snapshot is not None and periodic is not None:
        # Only disagreements are worth a trace; single-candidate resolutions
        # are the overwhelmingly common, uninteresting case.
        LOGGER.debug(
            "share resolver: listing_id=%s chose %s "
            "(snapshot=%s periodic=%s implied=%s)",
            listing_id,
            choice,
            snapshot.value,
            periodic.value,
            implied,
        )
    return chosen


def _is_usable_count(record: FactRecord) -> bool:
    """A positive, count-kinded row -- the intent the typed scalar readers
    usually enforce (a share count is a currency-less quantity), checked here
    without raising."""

    return record.unit_kind == "count" and record.value > 0


def _snapshot_candidate(listing_id: int, repo: RawFactSource) -> Optional[FactRecord]:
    """Latest provider SharesStats snapshot, if it is a usable count."""

    records = repo.facts_for_concept(
        listing_id, SNAPSHOT_SHARE_CONCEPT, fiscal_period="INSTANT", limit=1
    )
    for record in records:
        # Re-check the period explicitly rather than trusting the filter:
        # in-memory fact sources are only required to honour the RawFactSource
        # shape, not its filtering fidelity.
        if record.fiscal_period == "INSTANT" and _is_usable_count(record):
            return record
    return None


def _periodic_candidate(listing_id: int, repo: RawFactSource) -> Optional[FactRecord]:
    """Latest filing-based (non-INSTANT) count across the share concepts.

    The freshest end_date wins across both concepts; on an exact date tie the
    Entity row wins (iteration order), matching the pre-resolver market-cap
    preference. A fresher Common row beating an older Entity row is a
    deliberate improvement over the old unconditional Entity-first read.
    """

    best: Optional[FactRecord] = None
    for concept in SHARE_COUNT_CONCEPTS:
        record = _first_usable_periodic(listing_id, repo, concept)
        if record is None:
            continue
        if best is None or record.end_date > best.end_date:
            best = record
    return best


def _first_usable_periodic(
    listing_id: int, repo: RawFactSource, concept: str
) -> Optional[FactRecord]:
    """Freshest usable non-INSTANT count for one concept.

    ``latest_fact`` first: it is the LIMIT-1 read of the same newest-first
    ordering ``facts_for_concept`` uses, so in the overwhelmingly common case
    (Entity rows are never INSTANT) one row suffices. Only when the newest row
    is the INSTANT snapshot (Common) or otherwise unusable does this fall back
    to scanning the concept's history for the first filing-based row.
    """

    latest = repo.latest_fact(listing_id, concept)
    if latest is None:
        return None
    if latest.fiscal_period != "INSTANT" and _is_usable_count(latest):
        return latest
    for record in repo.facts_for_concept(listing_id, concept):
        if record.fiscal_period != "INSTANT" and _is_usable_count(record):
            return record
    return None


def _implied_total_shares(
    listing_id: int,
    repo: RawFactSource,
    market_repo: MarketDataRepository,
) -> Optional[float]:
    """Anchor-implied company-total share count, or None when unavailable.

    provider market cap / close nearest the cap's own date. Currency policy is
    *same-currency or no anchor*: both sides are in the listing base currency
    by construction (the normalizer FX-aligns monetary facts; the price reader
    collapses subunit codes), so a mismatch means normalize-time FX was
    unavailable -- degrade to the anchorless rule instead of raising. The
    division deliberately uses bare floats, not ``Money.__truediv__``, whose
    mismatch behaviour is to raise: an *auxiliary* input must never abort a
    metric.
    """

    anchor = repo.latest_fact(listing_id, PROVIDER_MARKET_CAP_CONCEPT)
    if anchor is None or anchor.unit_kind != "monetary" or anchor.value <= 0:
        return None

    # hasattr-guard for legacy in-memory fakes that predate the nearest-date
    # reader (same defensive pattern as graham_multiplier._latest_snapshot);
    # they degrade to the anchorless policy.
    if not hasattr(market_repo, "snapshot_near_date_by_id"):
        return None
    price_data = market_repo.snapshot_near_date_by_id(
        listing_id,
        anchor.end_date,
        max_distance_days=ANCHOR_PRICE_MAX_DISTANCE_DAYS,
    )
    if price_data is None or price_data.price is None or price_data.price <= 0:
        return None

    anchor_currency = normalize_currency_code(anchor.currency)
    price_currency = normalize_currency_code(price_data.currency)
    if anchor_currency is None or anchor_currency != price_currency:
        LOGGER.debug(
            "share resolver: anchor/price currency mismatch for listing_id=%s "
            "(%s vs %s); ignoring anchor",
            listing_id,
            anchor.currency,
            price_data.currency,
        )
        return None

    return anchor.value / price_data.price


__all__ = [
    "ANCHOR_MAX_DISTANCE_RATIO",
    "ANCHOR_PRICE_MAX_DISTANCE_DAYS",
    "PROVIDER_MARKET_CAP_CONCEPT",
    "SHARE_COUNT_CONCEPTS",
    "SHARE_RESOLVER_REQUIRED_CONCEPTS",
    "SNAPSHOT_AGREEMENT_MAX_RATIO",
    "SNAPSHOT_SHARE_CONCEPT",
    "STRUCTURAL_DIVERGENCE_MIN_RATIO",
    "ShareCountChoice",
    "arbitrate_share_count",
    "resolve_current_share_count",
]
