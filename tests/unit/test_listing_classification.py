"""Unit tests for the primary/secondary listing classification rules.

Author: Emre Tezel

Every fixture below is a real case from the 2026-07 universe audit, so a failure
here names an actual listing that would be misclassified.
"""

from __future__ import annotations

from typing import Optional

import pytest

from pyvalue.universe.listing_classification import (
    ClassificationRule,
    ListingEvidence,
    ListingStatus,
    classify_listings,
    summarize,
)


def _evidence(
    listing_id: int,
    provider_symbol: str,
    *,
    primary_ticker: Optional[str] = None,
    home_category: Optional[str] = None,
    isin: Optional[str] = None,
    lei: Optional[str] = None,
    venue_tier: Optional[str] = None,
) -> ListingEvidence:
    """Build evidence the way the repository's normalizer would."""

    return ListingEvidence(
        listing_id=listing_id,
        provider_symbol=provider_symbol,
        primary_ticker=primary_ticker,
        home_category=home_category,
        isin=isin,
        lei=lei,
        venue_tier=venue_tier,
    )


# ---------------------------------------------------------------------------
# R1: depositary receipts.
# ---------------------------------------------------------------------------


def test_depositary_receipt_beats_its_own_primary_ticker_claim() -> None:
    """An ADR self-declares as primary; HomeCategory must override that.

    ``DNLMY.US`` is Dunelm's ADR and EODHD sets its PrimaryTicker to itself. If
    R2 ran first the receipt would be classified primary and appear in the
    screen beside ``DNLM.LSE`` -- which is exactly what happened.
    """

    result = classify_listings(
        [
            _evidence(
                1,
                "DNLMY.US",
                primary_ticker="DNLMY.US",
                home_category="ADR",
                venue_tier="PINK",
            )
        ]
    )

    assert result[1].status is ListingStatus.SECONDARY
    assert result[1].rule is ClassificationRule.DEPOSITARY_RECEIPT


@pytest.mark.parametrize(
    "category", ["ADR", "BDR", "ADR PRIMARY", "ADR SECONDARY", "ADR PREFERRED"]
)
def test_every_depositary_category_is_secondary(category: str) -> None:
    result = classify_listings([_evidence(1, "XXXX.US", home_category=category)])
    assert result[1].status is ListingStatus.SECONDARY


def test_domestic_home_category_is_not_a_depositary_signal() -> None:
    """``Domestic`` says nothing about primacy -- R2 must still decide."""

    result = classify_listings(
        [_evidence(1, "ADBE.US", primary_ticker="ADBE.US", home_category="DOMESTIC")]
    )
    assert result[1].rule is ClassificationRule.SELF_DECLARED_PRIMARY


# ---------------------------------------------------------------------------
# R2: the vendor's own answer.
# ---------------------------------------------------------------------------


def test_primary_ticker_naming_this_listing_is_primary() -> None:
    result = classify_listings([_evidence(1, "ASML.AS", primary_ticker="ASML.AS")])
    assert result[1].status is ListingStatus.PRIMARY
    assert result[1].rule is ClassificationRule.SELF_DECLARED_PRIMARY


def test_primary_ticker_naming_another_listing_is_secondary() -> None:
    result = classify_listings([_evidence(1, "ASME.F", primary_ticker="ASML.AS")])
    assert result[1].status is ListingStatus.SECONDARY
    assert result[1].rule is ClassificationRule.PRIMARY_TICKER_ELSEWHERE


# ---------------------------------------------------------------------------
# R3: inherit the answer from an ISIN peer.
# ---------------------------------------------------------------------------


def test_missing_primary_ticker_inherits_peer_answer() -> None:
    """The core fix: ASML's Dusseldorf line has no PrimaryTicker of its own.

    Six of its seven peers name ``ASML.AS``. Before the peer rule, the missing
    field alone made ``ASME.DU`` primary.
    """

    isin = "NL0010273215"
    result = classify_listings(
        [
            _evidence(1, "ASML.AS", primary_ticker="ASML.AS", isin=isin),
            _evidence(2, "ASME.F", primary_ticker="ASML.AS", isin=isin),
            _evidence(3, "ASME.DU", isin=isin),  # no PrimaryTicker
        ]
    )

    assert result[1].status is ListingStatus.PRIMARY
    assert result[2].status is ListingStatus.SECONDARY
    assert result[3].status is ListingStatus.SECONDARY
    assert result[3].rule is ClassificationRule.ISIN_PEER_NAMES_OTHER


def test_peer_naming_this_listing_promotes_it() -> None:
    """A listing its peers point *at* is primary even with no field of its own."""

    isin = "GB00B1CKQ739"
    result = classify_listings(
        [
            _evidence(1, "AAA.LSE", isin=isin),
            _evidence(2, "AAA.F", primary_ticker="AAA.LSE", isin=isin),
        ]
    )

    assert result[1].status is ListingStatus.PRIMARY
    assert result[1].rule is ClassificationRule.ISIN_PEER_NAMES_THIS


def test_peer_rules_need_a_shared_isin() -> None:
    """Without an ISIN there is no peer group, so no answer can be inherited.

    This is why a depositary receipt is never grouped with its underlying: it
    carries its own ISIN. ``DNLMY.US`` (US26543P1030) and ``DNLM.LSE``
    (GB00B1CKQ739) are different securities and R3 must not link them.
    """

    result = classify_listings(
        [
            _evidence(1, "DNLM.LSE", primary_ticker="DNLM.LSE", isin="GB00B1CKQ739"),
            _evidence(2, "DNLMY.US", isin="US26543P1030", venue_tier="PINK"),
        ]
    )

    assert result[2].rule is not ClassificationRule.ISIN_PEER_NAMES_OTHER


# ---------------------------------------------------------------------------
# R4: no evidence at all.
#
# The two tests below pin behaviour that is a *deliberate trade*, not an
# accident. Two rules used to sit between R3 and R4 -- an ISIN-group tie-break
# and a demotion for listings on hand-picked "secondary-quote" venues -- and
# both were removed in 2026-07 because EODHD publishes nothing that supports or
# checks a venue-quality judgement. If either test starts failing, someone has
# reintroduced structural inference into a rule set that is meant to read only
# vendor fields.
# ---------------------------------------------------------------------------


def test_two_self_declared_primaries_on_one_isin_both_stand() -> None:
    """``LULU.US`` and ``33L.F`` share US5500211090 and both self-declare.

    The vendor is simply wrong about one of them, and nothing it publishes says
    which. The removed tie-break picked the venue matching the ISIN's country
    prefix; without it both stay primary and the screen shows the company twice.

    Accepted: 1,029 listings across the catalog are in this position. The
    alternative was a hand-coded venue ranking covering 9 of 68 exchanges.
    """

    isin = "US5500211090"
    result = classify_listings(
        [
            _evidence(
                1, "LULU.US", primary_ticker="LULU.US", isin=isin, venue_tier="NASDAQ"
            ),
            _evidence(2, "33L.F", primary_ticker="33L.F", isin=isin),
        ]
    )

    assert result[1].status is ListingStatus.PRIMARY
    assert result[2].status is ListingStatus.PRIMARY
    assert result[2].rule is ClassificationRule.SELF_DECLARED_PRIMARY


@pytest.mark.parametrize(
    ("provider_symbol", "venue_tier"),
    [
        ("0K10.LSE", None),  # LSE international order book
        ("LVMHF.US", "PINK"),  # US OTC
        ("WOLTF.US", "OTCGREY"),
        ("ZOE.MU", None),  # German regional
        ("TSMC34.SA", None),  # B3 BDR shape
    ],
)
def test_evidence_free_listing_on_a_quote_venue_is_unknown(
    provider_symbol: str, venue_tier: Optional[str]
) -> None:
    """Where a listing trades no longer decides anything.

    These five shapes were all demoted to ``secondary`` by the removed venue
    rule. They now resolve ``unknown``, which stays eligible for primary-only
    scopes -- so they re-enter every screen rather than leaving the universe.
    """

    result = classify_listings([_evidence(1, provider_symbol, venue_tier=venue_tier)])
    assert result[1].status is ListingStatus.UNKNOWN
    assert result[1].rule is ClassificationRule.NO_EVIDENCE


def test_evidence_free_listing_on_a_domestic_venue_stays_unknown() -> None:
    """Thailand, Korea, Taiwan and friends simply lack PrimaryTicker coverage.

    ~8,800 listings land here for this reason alone. They must stay eligible:
    demoting them would silently remove real companies the screen should see.
    """

    result = classify_listings([_evidence(1, "NETBAY.BK")])
    assert result[1].status is ListingStatus.UNKNOWN
    assert result[1].rule is ClassificationRule.NO_EVIDENCE


# ---------------------------------------------------------------------------
# Reporting helper.
# ---------------------------------------------------------------------------


def test_summarize_counts_rules() -> None:
    counts = summarize(
        [
            ClassificationRule.SELF_DECLARED_PRIMARY,
            ClassificationRule.SELF_DECLARED_PRIMARY,
            ClassificationRule.NO_EVIDENCE,
        ]
    )
    assert counts == {"self_declared_primary": 2, "no_evidence": 1}


# ---------------------------------------------------------------------------
# The sole-listing rescue.
# ---------------------------------------------------------------------------


def test_sole_exchange_listed_adr_is_rescued() -> None:
    """A NASDAQ 'ADR' with nowhere else to go must not vanish.

    EODHD labels Arm, AerCap, Credicorp and ~290 others ``ADR`` on an exchange
    proper while we hold no other line for them. Demoting those deduplicates
    nothing -- it deletes the company.
    """

    result = classify_listings(
        [
            _evidence(
                1,
                "ARM.US",
                primary_ticker="ARM.US",
                home_category="ADR",
                venue_tier="NASDAQ",
                isin="US0420682058",
            )
        ]
    )

    assert result[1].status is ListingStatus.UNKNOWN
    assert result[1].rule is ClassificationRule.SOLE_LISTING_RESCUE


def test_rescue_does_not_fire_for_otc_receipts() -> None:
    """``DNLMY.US`` sits on PINK, so the ADR label is plausible and stands.

    This is the gate that keeps Dunelm from appearing twice: without it the
    rescue would readmit every OTC receipt whose underlying we cannot link.
    """

    result = classify_listings(
        [
            _evidence(
                1,
                "DNLMY.US",
                primary_ticker="DNLMY.US",
                home_category="ADR",
                venue_tier="PINK",
                isin="US26543P1030",
            )
        ]
    )

    assert result[1].status is ListingStatus.SECONDARY
    assert result[1].rule is ClassificationRule.DEPOSITARY_RECEIPT


def test_rescue_does_not_fire_when_an_isin_sibling_survives() -> None:
    """With the underlying present, the receipt really is redundant."""

    isin = "US0378331005"
    result = classify_listings(
        [
            _evidence(1, "AAPL.US", primary_ticker="AAPL.US", isin=isin),
            _evidence(
                2,
                "AAPLX.US",
                home_category="ADR",
                venue_tier="NASDAQ",
                isin=isin,
            ),
        ]
    )

    assert result[2].status is ListingStatus.SECONDARY
    assert result[2].rule is ClassificationRule.DEPOSITARY_RECEIPT


def test_rescue_does_not_fire_when_an_lei_sibling_survives() -> None:
    """LEI catches siblings ISIN cannot: a receipt and its underlying differ.

    Same issuer, two securities, two ISINs -- only the LEI links them, which is
    why the evidence carries it.
    """

    lei = "213800WCOWEI3T5DUV19"
    result = classify_listings(
        [
            _evidence(
                1, "AAA.LSE", primary_ticker="AAA.LSE", isin="GB00B1CKQ739", lei=lei
            ),
            _evidence(
                2,
                "AAAY.US",
                home_category="ADR",
                venue_tier="NYSE",
                isin="US26543P1030",
                lei=lei,
            ),
        ]
    )

    assert result[2].status is ListingStatus.SECONDARY
    assert result[2].rule is ClassificationRule.DEPOSITARY_RECEIPT


def test_rescue_ignores_a_secondary_sibling() -> None:
    """A sibling that is itself demoted is no refuge.

    If every line of an issuer is secondary the company is gone either way, so
    the rescue must look for a *surviving* sibling, not merely any sibling.

    The sibling here is demoted by R2 -- its ``PrimaryTicker`` names a third
    listing outside our universe. It used to be a Frankfurt line demoted on
    venue alone, which stopped working when the venue rule was removed: such a
    line is now ``unknown``, i.e. a survivor. That change is why the rescue fires
    323 -> 311 times on the live catalog.
    """

    lei = "213800WCOWEI3T5DUV19"
    result = classify_listings(
        [
            _evidence(
                1, "AAA.F", primary_ticker="AAA.XETRA", isin="GB00B1CKQ739", lei=lei
            ),
            _evidence(
                2,
                "AAAY.US",
                home_category="ADR",
                venue_tier="NYSE",
                isin="US26543P1030",
                lei=lei,
            ),
        ]
    )

    assert result[1].status is ListingStatus.SECONDARY
    assert result[2].status is ListingStatus.UNKNOWN
    assert result[2].rule is ClassificationRule.SOLE_LISTING_RESCUE


def test_rescue_never_promotes_to_primary() -> None:
    """Rescue means "no reason to exclude", not "evidence of primacy"."""

    result = classify_listings(
        [
            _evidence(
                1,
                "BABA.US",
                home_category="ADR",
                venue_tier="NYSE",
                isin="US01609W1027",
            )
        ]
    )
    assert result[1].status is not ListingStatus.PRIMARY
    assert result[1].status is ListingStatus.UNKNOWN
