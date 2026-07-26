"""Unit tests for ISIN/LEI shape normalization.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Optional

import pytest

from pyvalue.identifiers import shaped_isin, shaped_lei


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Canonical values taken from live EODHD payloads.
        ("US5926881054", "US5926881054"),  # Mettler-Toledo
        ("GB00B1CKQ739", "GB00B1CKQ739"),  # Dunelm, digits in the NSIN
        ("NL0010273215", "NL0010273215"),  # ASML
        ("KYG040111059", "KYG040111059"),  # ANTA Sports, letter in the NSIN
        # Providers publish inconsistent casing and padding.
        ("us5926881054", "US5926881054"),
        ("  US5926881054  ", "US5926881054"),
    ],
)
def test_shaped_isin_accepts_and_normalizes_valid_codes(
    raw: str, expected: str
) -> None:
    assert shaped_isin(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "",
        "   ",
        "US592688105",  # 11 characters
        "US59268810544",  # 13 characters
        "US59268810-4",  # punctuation
        "1S5926881054",  # country prefix must be alphabetic
        "U15926881054",  # ...both characters of it
        "US592688105X",  # check digit must be numeric
    ],
)
def test_shaped_isin_rejects_structurally_impossible_values(raw: Optional[str]) -> None:
    """Absence is a valid state, so rejection means NULL rather than an error."""

    assert shaped_isin(raw) is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("5493000BD5GJNUDIUG10", "5493000BD5GJNUDIUG10"),  # Mettler-Toledo
        ("213800WCOWEI3T5DUV19", "213800WCOWEI3T5DUV19"),  # Dunelm
        ("iog4e947oatn0kjysd45", "IOG4E947OATN0KJYSD45"),  # LVMH, lowercased
        ("  IOG4E947OATN0KJYSD45  ", "IOG4E947OATN0KJYSD45"),
    ],
)
def test_shaped_lei_accepts_and_normalizes_valid_codes(raw: str, expected: str) -> None:
    assert shaped_lei(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "",
        "5493000BD5GJNUDIUG1",  # 19 characters
        "5493000BD5GJNUDIUG100",  # 21 characters
        "5493000BD5GJNUDIUG-0",  # punctuation
    ],
)
def test_shaped_lei_rejects_structurally_impossible_values(raw: Optional[str]) -> None:
    assert shaped_lei(raw) is None


def test_shaped_helpers_accept_non_string_input() -> None:
    """``json_extract``-style callers can hand through non-string JSON values.

    The normalizers coerce rather than raise, so a numeric or boolean payload
    value degrades to "no identifier" like any other unusable input.
    """

    assert shaped_isin(12345) is None
    assert shaped_lei(True) is None
