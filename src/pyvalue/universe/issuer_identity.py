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

What this deliberately does **not** do
--------------------------------------
It does not link a depositary receipt to the shares it wraps. A receipt is a
distinct security with its own ISIN (``DNLMY.US`` is ``US26543P1030`` where
``DNLM.LSE`` is ``GB00B1CKQ739``) and receipts rarely carry an LEI, so neither
identifier bridges the pair. No evidence pyvalue currently ingests does. Grouping
is therefore *cross-listings of one security and share classes of one entity* --
which is what makes it safe, since every link rests on an identifier rather than
on a name resembling another name.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


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
        entity_name: The issuer name currently recorded for this listing, used
            to name a merged group.
        is_primary: Whether the listing is classified ``primary``. Only a
            tie-break for which name the merged issuer keeps -- a company is
            best described by the name on its primary line.
    """

    listing_id: int
    issuer_id: int
    isin: Optional[str] = None
    lei: Optional[str] = None
    entity_name: Optional[str] = None
    is_primary: bool = False


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

    The primary listing's name wins: a company is best described by the name on
    the line that actually is the company, not by an ADR's ``... PLC ADR`` or a
    German regional line's abbreviated ``NEMETSCHEK SE O.N.``. Ties fall back to
    the lowest listing id so the choice is deterministic across runs.
    """

    named = [member for member in members if member.entity_name]
    if not named:
        return None
    named.sort(key=lambda member: (not member.is_primary, member.listing_id))
    return named[0].entity_name


__all__ = [
    "IssuerGroup",
    "ListingIdentity",
    "group_listings",
]
