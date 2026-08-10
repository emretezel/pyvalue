"""Property tests for the single debt rule.

``broader_debt_reading`` is the whole codebase's decision between the two ways
a provider states debt -- the component sum and the total-debt rollup. Five
private copies of that decision used to disagree with each other; these
properties pin the contract every caller now shares.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Optional

from hypothesis import given
from hypothesis import strategies as st

from pyvalue.metrics.balance_sheet import BalanceSheetPosition, broader_debt_reading
from pyvalue.money import Money

AS_OF = "2026-06-30"

_amounts = st.floats(
    min_value=-1e12, max_value=1e12, allow_nan=False, allow_infinity=False
)


def _position(amount: Optional[float]) -> Optional[BalanceSheetPosition]:
    if amount is None:
        return None
    return BalanceSheetPosition(money=Money(amount, "USD"), as_of=AS_OF)


@given(components=_amounts, rollup=_amounts)
def test_result_is_never_below_either_input(components: float, rollup: float) -> None:
    """The core invariant: resolved debt never understates a reported figure.

    This is the property the old components-preferred rule violated -- it could
    return the component sum while a larger rollup sat in the same payload,
    which is how $78.8B of Microsoft's debt disappeared from enterprise value.
    """

    resolved = broader_debt_reading(_position(components), _position(rollup))

    assert resolved is not None
    assert resolved.money.amount >= components
    assert resolved.money.amount >= rollup


@given(components=_amounts, rollup=_amounts)
def test_result_is_always_one_of_the_inputs(components: float, rollup: float) -> None:
    """No synthesis: the answer is a reported figure, never a blend of them."""

    resolved = broader_debt_reading(_position(components), _position(rollup))

    assert resolved is not None
    assert resolved.money.amount in (components, rollup)


@given(components=_amounts, rollup=_amounts)
def test_order_does_not_matter(components: float, rollup: float) -> None:
    """Commutative -- which representation a caller happens to fetch first
    cannot change the answer, or callers would diverge again."""

    forward = broader_debt_reading(_position(components), _position(rollup))
    reverse = broader_debt_reading(_position(rollup), _position(components))

    assert forward is not None and reverse is not None
    assert forward.money.amount == reverse.money.amount


@given(amount=_amounts)
def test_a_single_side_passes_through(amount: float) -> None:
    """One representation present is a real balance-sheet shape, not a gap."""

    assert broader_debt_reading(_position(amount), None) == _position(amount)
    assert broader_debt_reading(None, _position(amount)) == _position(amount)


def test_neither_side_is_unknown_not_zero() -> None:
    """Absent debt facts must not be read as a debt-free balance sheet."""

    assert broader_debt_reading(None, None) is None
