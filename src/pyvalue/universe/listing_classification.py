"""Primary/secondary listing classification from provider evidence.

Author: Emre Tezel

A company's shares often trade on many venues: the home exchange, foreign
cross-listings, depositary receipts. Screens want one row per business, so the
universe has to know which line is *the* listing and which are echoes of it.

This module owns that judgement. It is deliberately pure -- it takes evidence
records and returns classifications, touching no database -- because the rule is
domain logic that two very different callers need: the reconcile commands and
the fundamentals ingest path.

Why a rule *set* rather than one field
--------------------------------------
EODHD publishes ``General.PrimaryTicker``, which names the primary line for a
company, and reading it was the whole of the original rule. Two things broke
that:

* It is **absent on ~31% of payloads**, and the original code read absence as
  proof of primacy. That single default admitted 22,452 of the 57,001 listings
  the QARP screen ran over -- every LSE international-order-book line, most
  German regional lines, most US OTC lines.
* For a depositary receipt EODHD sets it to the **receipt itself**
  (``DNLMY.US`` -> ``DNLMY.US``), so an ADR self-certifies as primary no matter
  how carefully the field is read.

So the rules below layer independent evidence, and -- crucially -- absence of
evidence produces ``UNKNOWN`` rather than ``PRIMARY``. Fail-open on a missing
vendor field is what created the problem.

Every rule reads a field EODHD publishes
----------------------------------------
That is a deliberate boundary, set in 2026-07 after two earlier rules were
removed. They inferred a verdict from *where* a listing traded -- an ISIN-group
tie-break preferring the venue in the ISIN's issuing country, and a demotion for
listings quoted on a "secondary-quote venue" (US OTC tiers, German regional
exchanges, the LSE international order book, B3 BDR ticker shapes).

Both rested on a hand-coded map of venue quality that EODHD's data cannot
support and cannot check:

* The exchange catalog has **no market-type field**. ``F`` (Frankfurt Exchange)
  and ``XETRA`` (XETRA Stock Exchange) are structurally identical rows -- same
  country, same currency, both with a MIC -- yet the removed rule demoted one
  and not the other.
* Reading the exchange *name* would be actively wrong: ``TWO`` is "Taiwan OTC
  Exchange", which is the Taipei Exchange, a genuine primary market carrying
  1,101 listings.
* The map covered four venue families, so 59 of 68 exchanges -- 29,078 of 76,151
  listings, all of Shenzhen, KOSDAQ, Toronto, Australia, Korea -- got no opinion
  at all. It was a confident judgement about the US and Germany and silence
  everywhere else.

Removing them moved 10,621 listings from ``secondary`` to ``unknown`` and 1,029
back to ``primary``. The accepted cost is that one security can now hold two
primary lines when the vendor self-declares both (``LULU.US`` and ``33L.F`` on
``US5500211090``), and that quote-venue lines re-enter every scope. ``unknown``
and ``primary`` are both eligible downstream, so nothing is deleted from the
universe -- deduplication is simply weaker than it was.

The rules, in order (first match wins)
--------------------------------------
R1  ``HomeCategory`` says depositary receipt -> SECONDARY. EODHD labels ADRs and
    BDRs explicitly; this must precede R2 because an ADR's ``PrimaryTicker``
    points at itself.
R2  ``PrimaryTicker`` present -> PRIMARY iff it names this listing, else
    SECONDARY. The vendor's own answer, trusted when given.
R3  No ``PrimaryTicker``, but an ISIN peer has one -> inherit that answer. Six
    of ASML's seven lines name ``ASML.AS``; the seventh (``ASME.DU``, whose
    field is null) should not therefore be primary.
R4  Otherwise UNKNOWN -- genuinely unknown, and treated as eligible downstream.
    19,389 listings land here: roughly 8,800 on domestic exchanges (Thailand,
    Korea, Taiwan, Pakistan, Indonesia) where EODHD simply never populates
    ``PrimaryTicker``, and roughly 10,600 quote-venue lines that the removed
    venue rule used to demote. Excluding either group would silently delete real
    companies from the universe.

Finally a rescue pass undoes an R1 demotion that would erase a company outright:
a receipt is only redundant if the shares it wraps are listed somewhere we can
screen instead. See :func:`_rescue_sole_listings`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Final, FrozenSet, Iterable, List, Optional


class ListingStatus(str, Enum):
    """Resolved primary-listing classification for one canonical listing.

    Mirrors the ``listing.primary_listing_status`` vocabulary (CHECK-enforced
    since migration 088). ``UNKNOWN`` means "no evidence either way" and stays
    eligible for primary-only scopes -- it is not a synonym for secondary.
    """

    PRIMARY = "primary"
    SECONDARY = "secondary"
    UNKNOWN = "unknown"


class ClassificationRule(str, Enum):
    """Which rule decided a listing, for reporting and audit.

    Persisted nowhere: the stored column keeps only the verdict. This travels
    with the in-memory result so ``reconcile-listing-status`` can show the
    operator *why* the universe moved, which matters when a rule change shifts
    tens of thousands of rows.
    """

    DEPOSITARY_RECEIPT = "depositary_receipt"
    SELF_DECLARED_PRIMARY = "self_declared_primary"
    PRIMARY_TICKER_ELSEWHERE = "primary_ticker_elsewhere"
    ISIN_PEER_NAMES_THIS = "isin_peer_names_this"
    ISIN_PEER_NAMES_OTHER = "isin_peer_names_other"
    SOLE_LISTING_RESCUE = "sole_listing_rescue"
    NO_EVIDENCE = "no_evidence"


# EODHD's ``General.HomeCategory`` values that denote a depositary receipt -- a
# certificate representing shares held abroad. A receipt is legally a distinct
# security from the shares it wraps (it has its own ISIN), so it is never the
# issuer's primary listing regardless of what ``PrimaryTicker`` claims.
# ``'Domestic'``, ``'Domestic Primary'`` and friends are deliberately absent:
# they say nothing about primacy that R2 does not say better.
DEPOSITARY_CATEGORIES: Final[FrozenSet[str]] = frozenset(
    {
        "ADR",
        "ADR PRIMARY",
        "ADR SECONDARY",
        "ADR PREFERRED",
        "BDR",
    }
)


# US venues that are primary markets rather than over-the-counter quote tiers.
# Used only by the sole-listing rescue, as the signal that an ``ADR`` label is
# implausible: a real depositary receipt overwhelmingly trades over the counter,
# so one listed on an exchange proper is more likely a foreign issuer's ordinary
# shares that EODHD has labelled loosely.
#
# This is the last venue knowledge the module holds, and it survives the 2026-07
# removal because it is not a judgement about venue *quality* -- it never decides
# a listing on its own, only qualifies how much to trust a vendor label.
PRIMARY_US_EXCHANGES: Final[FrozenSet[str]] = frozenset(
    {"NASDAQ", "NYSE", "AMEX", "NYSE ARCA", "BATS"}
)


@dataclass(frozen=True)
class ListingEvidence:
    """Everything the rules need about one listing, already normalized.

    Attributes:
        listing_id: Canonical surrogate key; the result is keyed by it.
        provider_symbol: Qualified provider symbol (``0K10.LSE``), uppercase.
            Compared against ``primary_ticker``, which EODHD publishes in the
            same form.
        primary_ticker: ``General.PrimaryTicker``, qualified and uppercased, or
            None when the payload omits it.
        home_category: ``General.HomeCategory``, uppercased, or None.
        isin: ``listing.isin`` -- the stored column, not the payload, because
            the catalog refresh covers listings whose payload omits it.
        lei: ``General.LEI`` from the stored payload -- the legal-entity
            identifier, used only by the sole-listing rescue to recognise
            siblings that ISIN cannot (two securities of one issuer have
            different ISINs). Not a stored column: caching it on ``listing``
            duplicated ``issuer.lei``.
        venue_tier: ``General.Exchange`` -- the real venue for US listings
            (``PINK``, ``NASDAQ``), uppercased. Read only by the rescue.
    """

    listing_id: int
    provider_symbol: str
    primary_ticker: Optional[str] = None
    home_category: Optional[str] = None
    isin: Optional[str] = None
    lei: Optional[str] = None
    venue_tier: Optional[str] = None


@dataclass(frozen=True)
class ListingClassification:
    """One listing's verdict plus the rule that produced it."""

    listing_id: int
    status: ListingStatus
    rule: ClassificationRule


def _classify_without_peers(
    evidence: ListingEvidence,
) -> Optional[ListingClassification]:
    """Apply the rules that need only this listing: R1, R2.

    Returns None when the listing needs peer evidence (R3) to be decided, so the
    peer passes can tell "resolved" from "deferred".
    """

    if (evidence.home_category or "") in DEPOSITARY_CATEGORIES:
        return ListingClassification(
            evidence.listing_id,
            ListingStatus.SECONDARY,
            ClassificationRule.DEPOSITARY_RECEIPT,
        )
    if evidence.primary_ticker:
        if evidence.primary_ticker == evidence.provider_symbol:
            return ListingClassification(
                evidence.listing_id,
                ListingStatus.PRIMARY,
                ClassificationRule.SELF_DECLARED_PRIMARY,
            )
        return ListingClassification(
            evidence.listing_id,
            ListingStatus.SECONDARY,
            ClassificationRule.PRIMARY_TICKER_ELSEWHERE,
        )
    return None


def classify_listings(
    records: Iterable[ListingEvidence],
) -> Dict[int, ListingClassification]:
    """Classify a set of listings, using ISIN peer groups within it.

    Args:
        records: Every listing to classify **plus every ISIN peer of those
            listings**. Peers need not be classified themselves, but they must
            be present or R3 will silently under-fire -- a narrow scope must
            expand to whole ISIN groups before calling this.

    Returns:
        One :class:`ListingClassification` per input record, keyed by
        ``listing_id``.

    R3 runs as its own pass because it is set-level, not row-level: it needs
    every peer's R2 answer before it can lend one out.
    """

    evidence_list: List[ListingEvidence] = list(records)
    resolved: Dict[int, ListingClassification] = {}
    by_isin: Dict[str, List[ListingEvidence]] = {}
    for evidence in evidence_list:
        outcome = _classify_without_peers(evidence)
        if outcome is not None:
            resolved[evidence.listing_id] = outcome
        if evidence.isin:
            by_isin.setdefault(evidence.isin, []).append(evidence)

    # --- R3: a listing with no PrimaryTicker inherits its peers' answer. ---
    for evidence in evidence_list:
        if evidence.listing_id in resolved or not evidence.isin:
            continue
        peers_with_answer = [
            peer
            for peer in by_isin[evidence.isin]
            if peer.listing_id != evidence.listing_id and peer.primary_ticker
        ]
        if not peers_with_answer:
            continue
        if any(
            peer.primary_ticker == evidence.provider_symbol
            for peer in peers_with_answer
        ):
            resolved[evidence.listing_id] = ListingClassification(
                evidence.listing_id,
                ListingStatus.PRIMARY,
                ClassificationRule.ISIN_PEER_NAMES_THIS,
            )
        else:
            resolved[evidence.listing_id] = ListingClassification(
                evidence.listing_id,
                ListingStatus.SECONDARY,
                ClassificationRule.ISIN_PEER_NAMES_OTHER,
            )

    # --- R4: no evidence at all. ---
    for evidence in evidence_list:
        if evidence.listing_id in resolved:
            continue
        resolved[evidence.listing_id] = ListingClassification(
            evidence.listing_id,
            ListingStatus.UNKNOWN,
            ClassificationRule.NO_EVIDENCE,
        )

    _rescue_sole_listings(evidence_list, resolved)
    return resolved


def _rescue_sole_listings(
    evidence_list: List[ListingEvidence],
    resolved: Dict[int, ListingClassification],
) -> None:
    """Undo a depositary-receipt demotion when it would erase the company.

    R1 assumes a receipt is redundant because the shares it wraps are listed
    somewhere we can screen instead. When they are *not*, the demotion does not
    deduplicate anything -- it deletes the only line the universe has.

    That case is common and it is largely EODHD's ``HomeCategory`` being loose:
    296 listings on NASDAQ/NYSE proper are labelled ``ADR`` while being their
    issuer's sole line here, among them Arm Holdings, AerCap, Credicorp and
    Ascendis Pharma, whose US listing genuinely is the primary trading line.
    Alibaba is the honest version of the same shape -- 9988.HK really is primary,
    but it is outside our plan, so ``BABA.US`` is still the only line we can
    screen.

    Rescued listings become ``UNKNOWN``, never ``PRIMARY``: there is no positive
    evidence they are a primary listing, only an absence of any reason to
    exclude them. ``UNKNOWN`` is eligible, so the company stays screenable while
    the classification stays honest about what is known.

    Two conditions must both hold, and the pairing matters:

    * **The label must be implausible.** The listing trades on a primary
      exchange (NASDAQ, NYSE, AMEX) rather than an OTC tier. A real depositary
      receipt overwhelmingly trades over the counter; ``DNLMY.US`` and
      ``LVMHF.US`` sit on PINK and stay demoted, which is what keeps Dunelm and
      LVMH from appearing twice.
    * **The company must have nowhere else to go.** No sibling listing survives
      classification.

    Siblings are matched on ISIN *or* LEI, and only the LEI arm can ever see the
    pair: a receipt and its underlying are different securities with different
    ISINs. Whether it fires therefore depends on the vendor -- of 2,516 receipts
    in the catalog, 769 publish an LEI and 669 of those are linked by it. The
    ~1,750 that publish none are invisible to this test, and such a receipt is
    rescued even when its underlying survives.

    Note that "survives" means *not* ``SECONDARY``, so the rescue got strictly
    narrower when the venue rule was removed in 2026-07: the 10,621 quote-venue
    lines that became ``UNKNOWN`` are now survivors, and 12 receipts that used to
    be rescued correctly stay demoted because their sibling is visible after all.
    Firings went 323 -> 311.

    Closing the remaining gap needs evidence pyvalue does not ingest -- EODHD's
    Search API publishes an ``isPrimary`` flag that would settle it outright.
    """

    survivors_by_isin: set[str] = set()
    survivors_by_lei: set[str] = set()
    for evidence in evidence_list:
        if resolved[evidence.listing_id].status is ListingStatus.SECONDARY:
            continue
        if evidence.isin:
            survivors_by_isin.add(evidence.isin)
        if evidence.lei:
            survivors_by_lei.add(evidence.lei)

    for evidence in evidence_list:
        outcome = resolved[evidence.listing_id]
        if outcome.rule is not ClassificationRule.DEPOSITARY_RECEIPT:
            continue
        if (evidence.venue_tier or "") not in PRIMARY_US_EXCHANGES:
            continue
        if (evidence.isin and evidence.isin in survivors_by_isin) or (
            evidence.lei and evidence.lei in survivors_by_lei
        ):
            continue
        resolved[evidence.listing_id] = ListingClassification(
            evidence.listing_id,
            ListingStatus.UNKNOWN,
            ClassificationRule.SOLE_LISTING_RESCUE,
        )


def summarize(rules: Iterable[ClassificationRule]) -> Dict[str, int]:
    """Count deciding rules, for the reconcile command's report.

    Takes the rules themselves rather than whole results so both the resolver's
    own output and the repository's persisted records -- which carry the rule
    under a different attribute name -- can feed it without an adapter.
    """

    counts: Dict[str, int] = {}
    for rule in rules:
        counts[rule.value] = counts.get(rule.value, 0) + 1
    return counts


__all__ = [
    "ClassificationRule",
    "DEPOSITARY_CATEGORIES",
    "PRIMARY_US_EXCHANGES",
    "ListingClassification",
    "ListingEvidence",
    "ListingStatus",
    "classify_listings",
    "summarize",
]
