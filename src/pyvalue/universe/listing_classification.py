"""Primary/secondary listing classification from provider evidence.

Author: Emre Tezel

A company's shares often trade on many venues: the home exchange, foreign
cross-listings, depositary receipts. Screens want one row per business, so the
universe has to know which line is *the* listing and which are echoes of it.

This module owns that judgement. It is deliberately pure -- it takes evidence
records and returns classifications, touching no database -- because the rule is
domain logic that two very different callers need: the reconcile command, which
holds the whole universe, and the fundamentals ingest path, which holds one
payload at a time.

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
evidence now produces ``UNKNOWN`` rather than ``PRIMARY``. Fail-open on a
missing vendor field is what created the problem.

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
R4  An ISIN group still holding more than one candidate primary -> keep the line
    whose venue country matches the ISIN's country prefix. This is where the
    vendor is caught being wrong: ``LULU.US`` and ``33L.F`` share
    ``US5500211090`` and *both* self-declare.
R5  No evidence at all, but the venue is a secondary-quote market -> SECONDARY,
    unless the issuer is headquartered in that venue's own country.
R6  Otherwise UNKNOWN -- genuinely unknown, and treated as eligible downstream.
    Roughly 8,900 listings on domestic exchanges (Thailand, Korea, Taiwan,
    Pakistan, Indonesia) land here because EODHD simply never populates
    ``PrimaryTicker`` for them; excluding them would silently delete real
    companies from the universe.

Finally a rescue pass undoes an R1 demotion that would erase a company outright:
a receipt is only redundant if the shares it wraps are listed somewhere we can
screen instead. See :func:`_rescue_sole_listings`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Dict, Final, FrozenSet, Iterable, List, Mapping, Optional


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
    ISIN_GROUP_TIE_BREAK = "isin_group_tie_break"
    SECONDARY_QUOTE_VENUE = "secondary_quote_venue"
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


# For a US listing EODHD's ``General.Exchange`` names the real venue tier rather
# than the ``US`` umbrella the symbol lives under. The over-the-counter tiers
# are quote venues: an F-share or Y-share line here is a foreign company's
# shares changing hands off-exchange, not its primary market. NASDAQ/NYSE/AMEX
# are deliberately absent -- they are primary markets.
US_OTC_TIERS: Final[FrozenSet[str]] = frozenset(
    {
        "PINK",
        "OTCGREY",
        "OTCQB",
        "OTCQX",
        "OTCCE",
        "OTCMKTS",
        "OTCBB",
    }
)


# US venues that are primary markets rather than quote venues. Used only by the
# sole-listing rescue, as the signal that an ``ADR`` label is implausible: a real
# depositary receipt overwhelmingly trades over the counter, so one listed on an
# exchange proper is more likely a foreign issuer's ordinary shares that EODHD
# has labelled loosely.
PRIMARY_US_EXCHANGES: Final[FrozenSet[str]] = frozenset(
    {"NASDAQ", "NYSE", "AMEX", "NYSE ARCA", "BATS"}
)


# Germany's regional exchanges. They carry thousands of foreign issuers'
# duplicate quotes: of 4,597 evidence-free German-regional listings, just 83
# have a German head office. XETRA is deliberately excluded -- it is the primary
# venue for German issuers, and demoting it would delete real companies.
GERMAN_REGIONAL_EXCHANGES: Final[FrozenSet[str]] = frozenset(
    {"F", "MU", "DU", "STU", "HM", "BE"}
)


# The LSE's international order book quotes non-UK companies in their own
# currency, under a distinctive 4-character code beginning with zero
# (``0K10`` = Mettler-Toledo). These are the lines whose stored prices the
# 2026-07 verification found corrupted 100x by the pence heuristic.
LSE_INTERNATIONAL_ORDER_BOOK: Final[re.Pattern[str]] = re.compile(r"0[A-Z0-9]{3}")


# B3 (Brazil) Brazilian Depositary Receipts carry a numeric suffix in the 31-39
# band on a 4-character root (``TSMC34``). EODHD labels only some of them via
# HomeCategory, so the ticker shape catches the rest.
B3_DEPOSITARY_RECEIPT: Final[re.Pattern[str]] = re.compile(r"[A-Z0-9]{4}3[0-9]")


# Head-office country, as EODHD spells it in ``General.AddressData.Country``,
# for each venue in the secondary-quote set. A listing whose issuer *is* based
# in the venue's country is exempt from R5: a German small cap quoted only in
# Frankfurt, or a US company trading only on OTCQX, is a genuine primary
# listing. The exemption rescues 821 US OTC, 83 German regional, 7 LSE and 1 B3
# listing that would otherwise be demoted on venue alone.
#
# This cannot be sourced from the exchange catalog, which is otherwise the right
# home for venue geography (and *is* the source of ``venue_country_iso2``
# below): EODHD spells the same country differently across its two endpoints --
# ``exchanges-list`` says ``USA`` and ``UK`` where a fundamentals payload says
# ``United States`` and ``United Kingdom``. Comparing those directly would make
# the exemption silently never fire for the two venues that account for most of
# it. The table therefore holds payload-side spellings on purpose.
VENUE_HOME_COUNTRY: Final[Mapping[str, str]] = {
    "F": "Germany",
    "MU": "Germany",
    "DU": "Germany",
    "STU": "Germany",
    "HM": "Germany",
    "BE": "Germany",
    "US": "United States",
    "LSE": "United Kingdom",
    "SA": "Brazil",
}


@dataclass(frozen=True)
class ListingEvidence:
    """Everything the rules need about one listing, already normalized.

    Attributes:
        listing_id: Canonical surrogate key; the result is keyed by it.
        provider_symbol: Qualified provider symbol (``0K10.LSE``), uppercase.
            Compared against ``primary_ticker``, which EODHD publishes in the
            same form.
        bare_symbol: Symbol without the exchange suffix, for the ticker-shape
            venue rules.
        exchange_code: Canonical exchange code (``LSE``, ``MU``).
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
            (``PINK``, ``NASDAQ``), uppercased.
        hq_country: ``General.AddressData.Country``, verbatim, for the R5
            exemption.
        venue_country_iso2: The exchange's ISO 3166 alpha-2 country from the
            provider exchange catalog, for the R4 tie-break. Sourced from the
            catalog rather than hardcoded here so venue geography keeps one
            owner.
    """

    listing_id: int
    provider_symbol: str
    bare_symbol: str
    exchange_code: str
    primary_ticker: Optional[str] = None
    home_category: Optional[str] = None
    isin: Optional[str] = None
    lei: Optional[str] = None
    venue_tier: Optional[str] = None
    hq_country: Optional[str] = None
    venue_country_iso2: Optional[str] = None


@dataclass(frozen=True)
class ListingClassification:
    """One listing's verdict plus the rule that produced it."""

    listing_id: int
    status: ListingStatus
    rule: ClassificationRule


def is_secondary_quote_venue(evidence: ListingEvidence) -> bool:
    """Return True when the listing trades on a venue that is never a home market.

    Structural market knowledge, not tunable policy: these venues exist to quote
    securities whose primary market is elsewhere. Used only as R5, after every
    positive evidence path has been exhausted, so a listing with real evidence
    is never judged on its venue.
    """

    exchange = evidence.exchange_code
    if exchange == "US":
        return (evidence.venue_tier or "") in US_OTC_TIERS
    if exchange == "LSE":
        return LSE_INTERNATIONAL_ORDER_BOOK.fullmatch(evidence.bare_symbol) is not None
    if exchange in GERMAN_REGIONAL_EXCHANGES:
        return True
    if exchange == "SA":
        return B3_DEPOSITARY_RECEIPT.fullmatch(evidence.bare_symbol) is not None
    return False


def needs_sibling_evidence(evidence: ListingEvidence) -> bool:
    """True when the sole-listing rescue could apply to this listing.

    The rescue is the only rule that asks whether a *sibling* survives, and it
    only ever fires for a depositary receipt on a primary exchange. Loading
    sibling evidence is expensive -- LEI lives in the raw payloads, so finding
    listings that share one means scanning them -- and this predicate lets a
    caller pay that cost only when it could change an answer.

    Lives here rather than in the repository so the rule stays in one place: if
    the rescue's preconditions change, the evidence a caller must fetch changes
    with them, automatically.
    """

    return (evidence.home_category or "") in DEPOSITARY_CATEGORIES and (
        evidence.venue_tier or ""
    ) in PRIMARY_US_EXCHANGES


def _is_home_market_issuer(evidence: ListingEvidence) -> bool:
    """Return True when the issuer is headquartered in this venue's own country.

    The R5 exemption. A head office in the venue's country is strong evidence
    that a quote there is the company's real listing rather than a foreign echo,
    which is what keeps German small caps on regional exchanges and US-only OTC
    companies in the universe.
    """

    if evidence.hq_country is None:
        return False
    return VENUE_HOME_COUNTRY.get(evidence.exchange_code) == evidence.hq_country


def _isin_country_prefix(isin: str) -> str:
    """Return an ISIN's 2-letter issuing-country prefix."""

    return isin[:2]


def _classify_without_peers(
    evidence: ListingEvidence,
) -> Optional[ListingClassification]:
    """Apply the rules that need only this listing: R1, R2.

    Returns None when the listing needs peer evidence (R3/R4) or the venue
    fallback (R5) to be decided, so callers can tell "resolved" from "deferred".
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


def classify_listing_without_peers(
    evidence: ListingEvidence,
) -> ListingClassification:
    """Classify one listing using only the evidence it carries itself.

    For the fundamentals ingest path, which holds a single payload and cannot
    see the ISIN peer group without re-reading other listings' blobs. Anything
    R1/R2 cannot settle is returned as ``UNKNOWN``, never guessed: an
    ingest-time verdict is provisional and ``reconcile-listing-status`` resolves
    it properly against the whole graph.

    R5 is deliberately *not* applied here even though it needs no peers. A
    venue-based demotion can contradict R3 (a peer naming this very listing as
    primary), and writing a wrong ``secondary`` is worse than writing an honest
    ``unknown`` -- ``unknown`` stays eligible, so the listing is merely
    unfiltered until reconcile runs, not silently deleted from the universe.
    """

    resolved = _classify_without_peers(evidence)
    if resolved is not None:
        return resolved
    return ListingClassification(
        evidence.listing_id,
        ListingStatus.UNKNOWN,
        ClassificationRule.NO_EVIDENCE,
    )


def classify_listings(
    records: Iterable[ListingEvidence],
) -> Dict[int, ListingClassification]:
    """Classify a set of listings, using ISIN peer groups within it.

    Args:
        records: Every listing to classify **plus every ISIN peer of those
            listings**. Peers need not be classified themselves, but they must
            be present or R3/R4 will silently under-fire -- a narrow scope must
            expand to whole ISIN groups before calling this.

    Returns:
        One :class:`ListingClassification` per input record, keyed by
        ``listing_id``.

    The peer rules run as two passes because they are set-level, not row-level:
    R3 needs every peer's R2 answer before it can lend one out, and R4 needs
    R3's output before it can tell whether a group still has more than one
    claimant.
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

    # --- R4: one security cannot have two primary listings. ---
    # Where a group still holds several, prefer the venue in the ISIN's own
    # issuing country; failing that, demote whichever claimants sit on a
    # secondary-quote venue. Both branches are needed: `LULU.US` vs `33L.F`
    # resolves by country, while Pentair's `PNT.F`/`PNT.STU` share an Irish ISIN
    # with no Irish line present and are demoted by venue instead.
    for isin, group in by_isin.items():
        claimants = [
            evidence
            for evidence in group
            if resolved.get(evidence.listing_id) is not None
            and resolved[evidence.listing_id].status is ListingStatus.PRIMARY
        ]
        if len(claimants) < 2:
            continue
        country = _isin_country_prefix(isin)
        home_venue_ids = {
            evidence.listing_id
            for evidence in claimants
            if evidence.venue_country_iso2 == country
        }
        for evidence in claimants:
            survives = (
                evidence.listing_id in home_venue_ids
                if home_venue_ids
                else not is_secondary_quote_venue(evidence)
            )
            if survives:
                continue
            resolved[evidence.listing_id] = ListingClassification(
                evidence.listing_id,
                ListingStatus.SECONDARY,
                ClassificationRule.ISIN_GROUP_TIE_BREAK,
            )

    # --- R5/R6: no evidence at all. ---
    for evidence in evidence_list:
        if evidence.listing_id in resolved:
            continue
        if is_secondary_quote_venue(evidence) and not _is_home_market_issuer(evidence):
            resolved[evidence.listing_id] = ListingClassification(
                evidence.listing_id,
                ListingStatus.SECONDARY,
                ClassificationRule.SECONDARY_QUOTE_VENUE,
            )
        else:
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

    Siblings are matched on ISIN *or* LEI. Both are imperfect here: a receipt and
    its underlying are different securities with different ISINs, and receipts
    usually carry no LEI, so a genuine pair such as ``OMAB.US`` / ``OMAB.MX`` is
    invisible to this test and the US line is rescued even though the Mexican
    one survives. That is a known cost -- linking a receipt to its underlying
    needs issuer-level identity, which is what re-keying ``issuer`` on LEI will
    provide. Until then the rescue trades a handful of reintroduced duplicates
    for 296 companies that would otherwise vanish, and the venue gate keeps that
    handful small.
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
    "B3_DEPOSITARY_RECEIPT",
    "ClassificationRule",
    "DEPOSITARY_CATEGORIES",
    "GERMAN_REGIONAL_EXCHANGES",
    "LSE_INTERNATIONAL_ORDER_BOOK",
    "ListingClassification",
    "ListingEvidence",
    "ListingStatus",
    "US_OTC_TIERS",
    "VENUE_HOME_COUNTRY",
    "classify_listing_without_peers",
    "classify_listings",
    "is_secondary_quote_venue",
    "needs_sibling_evidence",
    "summarize",
]
