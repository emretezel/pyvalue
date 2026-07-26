"""Unit tests for grouping listings into legal entities.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Optional

from pyvalue.universe.issuer_identity import ListingIdentity, group_listings


def _identity(
    listing_id: int,
    issuer_id: int,
    *,
    isin: Optional[str] = None,
    lei: Optional[str] = None,
    entity_name: Optional[str] = None,
    is_primary: bool = False,
) -> ListingIdentity:
    return ListingIdentity(
        listing_id=listing_id,
        issuer_id=issuer_id,
        isin=isin,
        lei=lei,
        entity_name=entity_name,
        is_primary=is_primary,
    )


def test_shared_lei_groups_share_classes() -> None:
    """Alphabet's A and C shares are one company, two securities."""

    lei = "5493006MHB84DD0ZWV18"
    groups = group_listings(
        [
            _identity(1, 10, isin="US02079K3059", lei=lei, entity_name="Alphabet"),
            _identity(2, 20, isin="US02079K1079", lei=lei, entity_name="Alphabet C"),
        ]
    )

    assert len(groups) == 1
    assert groups[0].listing_ids == (1, 2)
    assert groups[0].merged_issuer_ids == (10, 20)
    assert groups[0].lei == lei


def test_shared_isin_groups_cross_listings_without_a_lei() -> None:
    """ISIN reaches pairs LEI cannot: EODHD publishes LEI for ~26% of listings."""

    isin = "US5926881054"
    groups = group_listings(
        [
            _identity(1, 10, isin=isin, entity_name="Mettler-Toledo"),
            _identity(2, 20, isin=isin, entity_name="Mettler-Toledo Intl"),
            _identity(3, 30, isin=isin, entity_name="METTLER-TOLEDO O.N."),
        ]
    )

    assert len(groups) == 1
    assert groups[0].listing_ids == (1, 2, 3)
    assert groups[0].lei is None


def test_grouping_is_transitive_across_both_identifiers() -> None:
    """A shares an LEI with B, B shares an ISIN with C -> all one company."""

    lei = "5493000BD5GJNUDIUG10"
    groups = group_listings(
        [
            _identity(1, 10, lei=lei),
            _identity(2, 20, lei=lei, isin="US5926881054"),
            _identity(3, 30, isin="US5926881054"),
        ]
    )

    assert len(groups) == 1
    assert groups[0].listing_ids == (1, 2, 3)


def test_depositary_receipt_is_not_grouped_with_its_underlying() -> None:
    """A receipt is a distinct security with its own ISIN and usually no LEI.

    Nothing pyvalue ingests bridges the pair, and this test pins that limit so
    it stays a known gap rather than a surprise: Dunelm's London line and its
    ADR remain separate entities.
    """

    groups = group_listings(
        [
            _identity(
                1,
                10,
                isin="GB00B1CKQ739",
                lei="213800WCOWEI3T5DUV19",
                entity_name="Dunelm Group PLC",
            ),
            _identity(2, 20, isin="US26543P1030", entity_name="Dunelm Group PLC ADR"),
        ]
    )

    assert len(groups) == 2
    assert {group.listing_ids for group in groups} == {(1,), (2,)}


def test_conflicting_leis_block_an_isin_link() -> None:
    """A shared ISIN across two distinct LEIs is contradictory evidence.

    One identifier must be wrong; merging two real companies would be far worse
    than leaving them apart, so the link is skipped rather than guessed at.
    """

    groups = group_listings(
        [
            _identity(1, 10, isin="US0000000001", lei="A" * 20),
            _identity(2, 20, isin="US0000000001", lei="B" * 20),
        ]
    )

    assert len(groups) == 2


def test_representative_is_the_lowest_issuer_id() -> None:
    """Stable across runs, so a re-run is a no-op rather than a reshuffle."""

    lei = "5493000BD5GJNUDIUG10"
    groups = group_listings(
        [
            _identity(9, 90, lei=lei),
            _identity(3, 30, lei=lei),
            _identity(5, 50, lei=lei),
        ]
    )

    assert groups[0].representative_issuer_id == 30
    assert groups[0].merged_issuer_ids == (30, 50, 90)


def test_group_takes_the_primary_listings_name() -> None:
    """A company is best described by the name on the line that is the company.

    Not by a German regional line's abbreviation or an ADR's '... ADR' suffix.
    """

    lei = "5493000BD5GJNUDIUG10"
    groups = group_listings(
        [
            _identity(1, 10, lei=lei, entity_name="METTLER-TOLEDO O.N."),
            _identity(
                2,
                20,
                lei=lei,
                entity_name="Mettler-Toledo International Inc.",
                is_primary=True,
            ),
        ]
    )

    assert groups[0].name == "Mettler-Toledo International Inc."


def test_group_name_falls_back_deterministically() -> None:
    """With no primary line the lowest listing id wins, so runs agree."""

    lei = "5493000BD5GJNUDIUG10"
    groups = group_listings(
        [
            _identity(7, 70, lei=lei, entity_name="Second"),
            _identity(2, 20, lei=lei, entity_name="First"),
        ]
    )

    assert groups[0].name == "First"


def test_listings_without_identifiers_stay_separate() -> None:
    """No identifier, no link -- grouping never guesses from names."""

    groups = group_listings(
        [
            _identity(1, 10, entity_name="Acme Corp"),
            _identity(2, 20, entity_name="Acme Corp"),
        ]
    )

    assert len(groups) == 2
    assert all(not group.merges for group in groups)


def test_single_listing_group_does_not_report_a_merge() -> None:
    groups = group_listings([_identity(1, 10, isin="US5926881054")])

    assert len(groups) == 1
    assert not groups[0].merges


def test_groups_never_share_a_representative() -> None:
    """One pre-existing issuer owning two unrelated entities must split.

    The runtime name-collision merge has produced 3,991 multi-listing issuers and
    never required their listings to share an identifier. Taking each group's
    lowest issuer id independently would hand both groups the same row and fuse
    two companies -- the opposite of what this module is for.
    """

    groups = group_listings(
        [
            # Both currently parented by issuer 1, but unrelated securities.
            _identity(1, 1, isin="US0000000017"),
            _identity(2, 1, isin="US0000000025"),
            _identity(3, 3, isin="US0000000017"),
            _identity(4, 4, isin="US0000000025"),
        ]
    )

    assert len(groups) == 2
    representatives = [group.representative_issuer_id for group in groups]
    assert len(set(representatives)) == 2, representatives


def test_group_with_no_free_representative_defers_allocation() -> None:
    """When every candidate is claimed the grouper asks for a fresh row.

    ``None`` means "allocate one" -- the pure module cannot mint ids, and reusing
    a claimed row would merge two entities.
    """

    groups = group_listings(
        [
            _identity(1, 1, isin="US0000000017"),
            _identity(2, 1, isin="US0000000025"),
        ]
    )

    assert len(groups) == 2
    assert groups[0].representative_issuer_id == 1
    assert groups[1].representative_issuer_id is None
