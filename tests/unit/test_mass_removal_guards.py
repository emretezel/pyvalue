"""Property tests for the mass-removal guard predicates.

The guards protect the provider layer from anomalous payloads (the 2026-07-11
BE incident: a truncated 200-response nearly emptied an exchange). The
predicates are pure, so their invariants are checked property-based per the
project testing rules: monotonicity in the removal count, the absolute floor,
and the majority-fraction requirement.

Author: Emre Tezel
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from pyvalue.persistence.storage.supported_tickers import (
    MASS_DELISTING_MIN_REMOVED,
    mass_delisting_suspicious,
)

_COUNTS = st.integers(min_value=0, max_value=100_000)


@given(existing=_COUNTS, removed=_COUNTS)
def test_below_absolute_floor_never_trips(existing: int, removed: int) -> None:
    if removed < MASS_DELISTING_MIN_REMOVED:
        assert mass_delisting_suspicious(existing, removed) is False


@given(existing=_COUNTS, removed=_COUNTS)
def test_minority_removals_never_trip(existing: int, removed: int) -> None:
    # Removing at most half of the existing mappings is normal churn,
    # regardless of absolute size.
    if removed * 2 <= existing:
        assert mass_delisting_suspicious(existing, removed) is False


@given(existing=_COUNTS)
def test_full_wipe_at_or_above_floor_always_trips(existing: int) -> None:
    # A payload that empties an exchange of floor-size or larger is exactly
    # the BE failure shape and must always be caught.
    if existing >= MASS_DELISTING_MIN_REMOVED:
        assert mass_delisting_suspicious(existing, existing) is True


@given(existing=_COUNTS, removed=_COUNTS, extra=_COUNTS)
def test_monotonic_in_removed(existing: int, removed: int, extra: int) -> None:
    # Removing even more from the same exchange can never look less suspicious.
    if mass_delisting_suspicious(existing, removed):
        assert mass_delisting_suspicious(existing, removed + extra) is True


def test_incident_anchor_cases() -> None:
    # The BE incident (2,835 removed of 2,865 mappings) must trip; the same
    # day's legitimate churn on F (295 removed of ~12,085) must not.
    assert mass_delisting_suspicious(2865, 2835) is True
    assert mass_delisting_suspicious(12085, 295) is False
