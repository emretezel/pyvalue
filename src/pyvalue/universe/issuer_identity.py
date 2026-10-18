"""Grouping listings into legal entities (issuer identity).

Author: Emre Tezel

``issuer`` is meant to model a company, but nothing ever made it do so. Its
identity is ``(name, country)``, and ``country`` is NULL on 65,752 of 70,564
rows -- and SQLite treats NULLs as distinct in a UNIQUE index, so in practice
almost every listing got its own issuer row. The 154 QARP passers mapped to 151
distinct ``issuer_id``s: Dunelm's London line and its ADR were separate
companies as far as the database was concerned, as were LVMH's Paris line and
its OTC line.

This module derives the grouping the table always intended, from the two
identifiers the catalog now stores on each listing:

* **LEI** identifies the legal entity directly. Two listings sharing one are the
  same company -- including different share classes, which is why Alphabet's
  ``ABEA``/``ABEC`` and Bank of America's common plus its preferred series
  collapse into single issuers.
* **ISIN** identifies a security, and a security has exactly one issuer, so a
  shared ISIN implies a shared entity too. It reaches pairs LEI cannot, because
  EODHD publishes an LEI for only about a quarter of listings.

How far this reaches, measured
-----------------------------
Of the 71,543 stored payloads, 53,500 publish an ISIN and 18,271 publish an LEI
-- and the LEI set is a strict *subset* of the ISIN set. Not one listing carries
an LEI without also carrying an ISIN. So the two identifiers are not
interchangeable sources of reach:

* **ISIN is the reach.** It is the only thing that can group the 35,229 listings
  that have an ISIN and no LEI.
* **LEI is a bridge, not extra reach.** It never links a listing ISIN could not
  touch; what it does is join *different ISIN groups* that belong to one entity.
  Alphabet's two share classes each have an ISIN, and only the shared LEI merges
  those two groups into one company.

18,043 listings publish neither and can only ever be singleton issuers.

Depositary receipts
-------------------
A receipt is a distinct security with its own ISIN (``DNLMY.US`` is
``US26543P1030`` where ``DNLM.LSE`` is ``GB00B1CKQ739``), so ISIN never bridges
a receipt to the shares it wraps. Whether the *entity* link exists depends
entirely on whether the receipt publishes its issuer's LEI, and that splits the
population: of 2,516 receipts, 769 carry an LEI and 669 of those (87%) are
grouped with a non-receipt listing through it. The remaining ~1,750 publish no
LEI and stay separate -- ``DNLMY.US`` among them.

So grouping links receipts to their underlying *when EODHD gives it the
evidence*, and not otherwise. Both halves are pinned by tests, because an
earlier version of this docstring claimed the link never happened and was wrong
about roughly a quarter of all receipts.

Every link rests on an identifier, never on one name resembling another -- which
is what makes the grouping safe to apply wholesale.

Naming the entity
-----------------
Grouping also decides what the merged ``issuer`` row is *called*, and that is
the one place names enter this module -- as output, never as evidence. EODHD's
``General.Name`` is a *security* name: it carries the share class ("Alphabet Inc
Class A" / "Alphabet Inc Class C"), so no listing's name is the company's name
as such. The issuer row models the legal entity, so the group's name is derived
in two steps, both pure and both pinned by tests:

1. Choose the listing whose name describes the company best. The vendor says
   which line that is: ``HomeCategory`` reads ``Domestic Primary`` on the main
   share line and ``Domestic Secondary`` / ``Domestic Preferred`` on the others
   (GOOGL over GOOG, BRK-B over BRK-A, BAC over its seventeen preferred series).
   Classification cannot make that call, because both share classes self-declare
   ``PrimaryTicker`` and are *both* primary. Where the vendor is silent the
   classification-primary line wins, then the lowest listing id.
2. Strip the trailing share-class designator from that name
   (:func:`entity_name_from_security_name`), so Alphabet is "Alphabet Inc" and not
   the name of whichever class happened to sort first.

The reconciler is the only writer of ``issuer.name`` after the row is created.
It used to be one of four -- ``normalize-fundamentals``,
``refresh-security-metadata`` and ``refresh-supported-tickers`` each rewrote the
name with the security name of whichever listing they processed last, which is
how Bank of America's row came to be called after a Merrill Lynch preferred.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Final, Iterable, List, Optional, Sequence, Set, Tuple

# EODHD's ``General.HomeCategory`` value marking the line the vendor itself
# treats as the company's primary share class. Stored uppercased, matching how
# ``ListingEvidence.home_category`` normalizes the field. Only this one value is
# read here: ``Domestic Secondary`` and ``Domestic Preferred`` say "not this one"
# by not being it, and the depositary values belong to the classifier.
VENDOR_PRIMARY_LINE: Final[str] = "DOMESTIC PRIMARY"

# A trailing share-class designator on a vendor security name. Anchored at the
# end and matched case-insensitively: an optional separator, one of the
# designator keywords EODHD actually uses, a single class letter, and optionally
# the security type the US feeds append ("Class A Common Stock", "Class A
# Ordinary Shares"). Surveyed over the 71,543 stored payloads: ~1,090 names end
# this way. Bare letters ("Under Armour Inc A"), mid-name designators (Liberty's
# tracking stocks) and receipt markers ("... Class A CEDEAR") are deliberately
# left alone -- stripping them would need guessing, and a slightly longer name
# is harmless where a wrong one is not.
_SHARE_CLASS_SUFFIX: Final[re.Pattern[str]] = re.compile(
    r"""
    [\s,-]*                                        # separator before the designator
    \b(?:class|cl\.?|series|ser\.?|srs\.?)         # designator keyword
    \s+[A-Za-z]                                    # the class letter
    (?:\s+(?:common\s+stock|ordinary\s+shares?|shares?|stock))?  # security type
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)


def entity_name_from_security_name(security_name: str) -> str:
    """Return the legal entity's name implied by a vendor security name.

    Strips one trailing share-class designator (see ``_SHARE_CLASS_SUFFIX``) from
    the trimmed name: ``"Alphabet Inc Class A"`` -> ``"Alphabet Inc"``,
    ``"DoorDash, Inc. Class A Common Stock"`` -> ``"DoorDash, Inc."``. Names
    without one are returned trimmed and otherwise unchanged. A name that *is*
    only a designator is returned as given rather than emptied, because
    ``issuer.name`` is NOT NULL and a bad name beats no name.

    Idempotent, and the result is always a prefix of the trimmed input -- the
    function only ever removes a suffix. Both properties are test-pinned. The
    strip runs to a fixpoint rather than once, which is what makes idempotence
    hold for the (unobserved) case of two stacked designators.
    """

    trimmed = security_name.strip()
    current = trimmed
    while True:
        stripped = _SHARE_CLASS_SUFFIX.sub("", current, count=1).rstrip()
        if stripped == current or not stripped:
            break
        current = stripped
    return current or trimmed


@dataclass(frozen=True)
class ListingIdentity:
    """One listing's entity evidence, as stored on the canonical row.

    Attributes:
        listing_id: Canonical surrogate key.
        issuer_id: The issuer row the listing currently points at.
        isin: ``listing.isin``, already shape-normalized, or None.
        lei: ``General.LEI`` from the listing's stored payload, already
            shape-normalized, or None. Read from the payload rather than a
            column: caching it on ``listing`` duplicated ``issuer.lei``.
        entity_name: This listing's security name (``General.Name`` when the
            payload has one, else the issuer name currently stored), one of the
            candidates for naming the merged group.
        is_primary: Whether the listing is classified ``primary``. Only a
            tie-break for which name the merged issuer keeps -- a company is
            best described by the name on its primary line.
        home_category: ``General.HomeCategory``, uppercased, or None. Read for
            one value only, :data:`VENDOR_PRIMARY_LINE`, which outranks
            ``is_primary`` when choosing the name: two share classes are both
            primary, and only the vendor says which one is the company.
    """

    listing_id: int
    issuer_id: int
    isin: Optional[str] = None
    lei: Optional[str] = None
    entity_name: Optional[str] = None
    is_primary: bool = False
    home_category: Optional[str] = None


@dataclass(frozen=True)
class IssuerGroup:
    """One legal entity and the listings that belong to it.

    ``representative_issuer_id`` is the existing issuer row the group should
    collapse onto, or ``None`` when every row it currently touches has been
    claimed by another group and the caller must allocate a fresh one. See
    :func:`group_listings` for why that case exists.
    """

    representative_issuer_id: Optional[int]
    lei: Optional[str]
    name: Optional[str]
    listing_ids: Tuple[int, ...]
    merged_issuer_ids: Tuple[int, ...]

    @property
    def merges(self) -> bool:
        """True when this group spans more than one existing issuer row."""

        return len(self.merged_issuer_ids) > 1


class _UnionFind:
    """Minimal union-find over listing ids.

    Grouping is transitive -- A shares an LEI with B, B shares an ISIN with C,
    so all three are one company -- and union-find is the honest expression of
    that. Path compression keeps ``find`` near-constant across the ~76k listings
    this runs over.
    """

    def __init__(self) -> None:
        self._parent: Dict[int, int] = {}

    def find(self, item: int) -> int:
        parent = self._parent.setdefault(item, item)
        while parent != item:
            item, parent = parent, self._parent.setdefault(parent, parent)
            self._parent[item] = parent
        return item

    def union(self, left: int, right: int) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root != right_root:
            # Lower id wins, so the representative is stable across runs.
            if right_root < left_root:
                left_root, right_root = right_root, left_root
            self._parent[right_root] = left_root


def _index_by(
    records: Sequence[ListingIdentity], attribute: str
) -> Dict[str, List[ListingIdentity]]:
    """Bucket records by a non-null string attribute."""

    buckets: Dict[str, List[ListingIdentity]] = {}
    for record in records:
        value = getattr(record, attribute)
        if value:
            buckets.setdefault(value, []).append(record)
    return buckets


def group_listings(
    records: Iterable[ListingIdentity],
) -> List[IssuerGroup]:
    """Group listings into legal entities.

    Args:
        records: Every listing to consider. Grouping is only as complete as the
            set it is given, so callers pass the whole universe rather than a
            scope.

    Returns:
        One :class:`IssuerGroup` per derived entity, ordered by representative
        issuer id so the result is reproducible.

    LEI links are applied first and are never overridden. ISIN links are then
    applied only where they would not fuse two *different* LEIs: a shared ISIN
    across two distinct legal entities is contradictory evidence, and the safe
    reading is that one of the identifiers is wrong, so the link is skipped
    rather than guessed at. In practice this is rare, but silently merging two
    real companies would be much worse than leaving them apart.
    """

    listings: List[ListingIdentity] = list(records)
    union = _UnionFind()
    for record in listings:
        union.find(record.listing_id)

    for peers in _index_by(listings, "lei").values():
        for peer in peers[1:]:
            union.union(peers[0].listing_id, peer.listing_id)

    lei_by_root: Dict[int, Set[str]] = {}
    for record in listings:
        if record.lei:
            lei_by_root.setdefault(union.find(record.listing_id), set()).add(record.lei)

    for peers in _index_by(listings, "isin").values():
        roots = {union.find(peer.listing_id) for peer in peers}
        distinct_leis: Set[str] = set()
        for root in roots:
            distinct_leis |= lei_by_root.get(root, set())
        if len(distinct_leis) > 1:
            # Contradictory evidence -- leave the entities separate.
            continue
        for peer in peers[1:]:
            union.union(peers[0].listing_id, peer.listing_id)
        merged_root = union.find(peers[0].listing_id)
        if distinct_leis:
            lei_by_root[merged_root] = distinct_leis

    members: Dict[int, List[ListingIdentity]] = {}
    for record in listings:
        members.setdefault(union.find(record.listing_id), []).append(record)

    # Assign each group a distinct surviving issuer row.
    #
    # A pre-existing issuer can own listings that end up in *different* groups:
    # the runtime name-collision merge has created 3,991 multi-listing issuers,
    # and nothing ever required their listings to share an identifier. Taking the
    # lowest issuer id per group independently would then hand two unrelated
    # entities the same representative and silently fuse them -- the opposite of
    # the bug this module exists to fix.
    #
    # So representatives are claimed greedily, in a deterministic order. A group
    # with no unclaimed candidate left gets ``None`` and the caller allocates a
    # fresh row for it.
    ordered = sorted(
        members.values(),
        key=lambda group_members: min(member.listing_id for member in group_members),
    )
    claimed: Set[int] = set()
    groups: List[IssuerGroup] = []
    for group_members in ordered:
        issuer_ids = sorted({member.issuer_id for member in group_members})
        representative = next(
            (issuer_id for issuer_id in issuer_ids if issuer_id not in claimed),
            None,
        )
        if representative is not None:
            claimed.add(representative)
        leis = sorted({member.lei for member in group_members if member.lei})
        groups.append(
            IssuerGroup(
                representative_issuer_id=representative,
                # A group can only carry one LEI: the skip above guarantees no
                # group ever fuses two, so this is a single value or nothing.
                lei=leis[0] if leis else None,
                name=_group_name(group_members),
                listing_ids=tuple(
                    sorted(member.listing_id for member in group_members)
                ),
                merged_issuer_ids=tuple(issuer_ids),
            )
        )
    return groups


def _group_name(members: Sequence[ListingIdentity]) -> Optional[str]:
    """Choose the name a merged issuer keeps.

    Three tiers, each a tie-break for the one above:

    1. The line the vendor marks ``Domestic Primary``. Share classes are all
       classified primary (each self-declares ``PrimaryTicker``), so without this
       Alphabet was named after whichever of GOOG/GOOGL had the lower listing
       id -- "Alphabet Inc Class C" -- and Bank of America after a preferred.
    2. A classification-primary line: a company is best described by the line
       that actually is the company, not by an ADR's ``... PLC ADR`` or a German
       regional line's abbreviated ``NEMETSCHEK SE O.N.``.
    3. The lowest listing id, so the choice is deterministic across runs.

    The chosen *security* name is then reduced to the entity's name by
    :func:`entity_name_from_security_name`, which is also why two classes that
    tie on every tier still agree: "Alphabet Inc Class A" and "Alphabet Inc
    Class C" both become "Alphabet Inc".
    """

    named = [member for member in members if member.entity_name]
    if not named:
        return None
    best = min(
        named,
        key=lambda member: (
            member.home_category != VENDOR_PRIMARY_LINE,
            not member.is_primary,
            member.listing_id,
        ),
    )
    # ``named`` is filtered on entity_name, so this branch cannot be taken; it
    # narrows the Optional honestly instead of asserting it away.
    if best.entity_name is None:
        return None
    return entity_name_from_security_name(best.entity_name)


__all__ = [
    "VENDOR_PRIMARY_LINE",
    "IssuerGroup",
    "ListingIdentity",
    "entity_name_from_security_name",
    "group_listings",
]
