"""Property tests for the mass-removal guard predicates.

The guards protect the provider layer from anomalous payloads (the 2026-07-11
BE incident: a truncated 200-response nearly emptied an exchange; a truncated
exchanges-list would do the same to the whole catalog via the drop cascade).
Both predicates are pure and share one shape -- an absolute floor AND a
majority fraction -- so their invariants are checked property-based per the
project testing rules: monotonicity in the removal count, the floor, and the
fraction requirement.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Callable, Tuple

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pyvalue.persistence.storage.entities import (
    MASS_EXCHANGE_DROP_MIN,
    mass_exchange_drop_suspicious,
)
from pyvalue.persistence.storage.supported_tickers import (
    MASS_DELISTING_MIN_REMOVED,
    mass_delisting_suspicious,
)

_COUNTS = st.integers(min_value=0, max_value=100_000)

# (predicate, absolute floor) -- the two guards share the floor+fraction shape.
_GUARDS: Tuple[Tuple[Callable[[int, int], bool], int], ...] = (
    (mass_delisting_suspicious, MASS_DELISTING_MIN_REMOVED),
    (mass_exchange_drop_suspicious, MASS_EXCHANGE_DROP_MIN),
)
_GUARD_IDS = ("delisting", "exchange-drop")


@pytest.mark.parametrize(("suspicious", "floor"), _GUARDS, ids=_GUARD_IDS)
@given(existing=_COUNTS, removed=_COUNTS)
def test_below_absolute_floor_never_trips(
    suspicious: Callable[[int, int], bool],
    floor: int,
    existing: int,
    removed: int,
) -> None:
    if removed < floor:
        assert suspicious(existing, removed) is False


@pytest.mark.parametrize(("suspicious", "floor"), _GUARDS, ids=_GUARD_IDS)
@given(existing=_COUNTS, removed=_COUNTS)
def test_minority_removals_never_trip(
    suspicious: Callable[[int, int], bool],
    floor: int,
    existing: int,
    removed: int,
) -> None:
    # Removing at most half of the existing rows is normal churn, regardless
    # of absolute size.
    if removed * 2 <= existing:
        assert suspicious(existing, removed) is False


@pytest.mark.parametrize(("suspicious", "floor"), _GUARDS, ids=_GUARD_IDS)
@given(existing=_COUNTS)
def test_full_wipe_at_or_above_floor_always_trips(
    suspicious: Callable[[int, int], bool],
    floor: int,
    existing: int,
) -> None:
    # A payload that empties a floor-sized-or-larger slice is exactly the BE
    # failure shape and must always be caught.
    if existing >= floor:
        assert suspicious(existing, existing) is True


@pytest.mark.parametrize(("suspicious", "floor"), _GUARDS, ids=_GUARD_IDS)
@given(existing=_COUNTS, removed=_COUNTS, extra=_COUNTS)
def test_monotonic_in_removed(
    suspicious: Callable[[int, int], bool],
    floor: int,
    existing: int,
    removed: int,
    extra: int,
) -> None:
    # Removing even more from the same slice can never look less suspicious.
    if suspicious(existing, removed):
        assert suspicious(existing, removed + extra) is True


def test_incident_anchor_cases() -> None:
    # Ticker guard: the BE incident (2,835 removed of 2,865 mappings) must
    # trip; the same day's legitimate churn on F (295 of ~12,085) must not.
    assert mass_delisting_suspicious(2865, 2835) is True
    assert mass_delisting_suspicious(12085, 295) is False
    # Exchange guard: the real BE/IC/TA plan change (3 dropped of 73) passes
    # without a flag; a truncated exchanges-list (68 dropped of 73) is caught.
    assert mass_exchange_drop_suspicious(73, 3) is False
    assert mass_exchange_drop_suspicious(73, 68) is True
