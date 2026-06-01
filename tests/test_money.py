"""Tests for the ``Money`` value type and ``CurrencyMismatchError``.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

from hypothesis import given, strategies as st
import pytest

from pyvalue.currency import normalize_currency_code
from pyvalue.money.fx import FXService, MissingFXRateError
from pyvalue.money import CurrencyMismatchError, Money


class _StubFXService(FXService):
    """Minimal :class:`FXService` double: one fixed rate, no DB or network.

    Subclassing the concrete service (rather than a Protocol) keeps the test
    type-clean against ``Money.convert``'s ``fx_service: FXService`` parameter
    while never opening a database connection.
    """

    def __init__(self, rate: Optional[Decimal]) -> None:
        # Intentionally do not call super().__init__: we only need a rate and a
        # provider name, not the live FX repository/config.
        self._rate = rate
        self.provider_name = "STUB"

    def convert_amount(
        self,
        amount: float | Decimal,
        from_currency: str,
        to_currency: str,
        as_of_date: str | date,
    ) -> Optional[Decimal]:
        if normalize_currency_code(from_currency) == normalize_currency_code(
            to_currency
        ):
            return Decimal(str(amount))
        if self._rate is None:
            return None
        return Decimal(str(amount)) * self._rate


# Currencies used by the property tests.
MAJOR_CODES = ("USD", "EUR", "GBP", "JPY", "ZAR", "ILS")
SUBUNIT_TO_BASE = {"GBX": "GBP", "ZAC": "ZAR", "ILA": "ILS", "GBP0.01": "GBP"}
FINITE_FLOATS = st.floats(
    allow_nan=False, allow_infinity=False, min_value=-1e9, max_value=1e9
)


# --------------------------------------------------------------------------
# Construction & normalization
# --------------------------------------------------------------------------


def test_constructor_normalizes_case_and_keeps_major_amount() -> None:
    money = Money(123.45, "usd")
    assert money.currency == "USD"
    assert money.amount == 123.45


@pytest.mark.parametrize(
    "subunit,base",
    [("GBX", "GBP"), ("ZAC", "ZAR"), ("ILA", "ILS"), ("GBP0.01", "GBP")],
)
def test_constructor_collapses_subunits_to_major(subunit: str, base: str) -> None:
    # A subunit input is divided by 100 and rebased: 2500 pence -> 25 pounds.
    money = Money(2500.0, subunit)
    assert money.currency == base
    assert money.amount == pytest.approx(25.0)


def test_money_can_never_hold_a_subunit_currency() -> None:
    assert Money(2500.0, "GBX").currency == "GBP"


@pytest.mark.parametrize("bad_currency", ["", "   "])
def test_constructor_rejects_blank_currency(bad_currency: str) -> None:
    # A blank/whitespace currency normalizes to None, so Money refuses it.
    # (Passing ``None`` itself is a static type error mypy already forbids.)
    with pytest.raises(ValueError):
        Money(10.0, bad_currency)


def test_from_value_soft_paths() -> None:
    assert Money.from_value(None, "USD") is None
    assert Money.from_value(10.0, None) is None
    assert Money.from_value(10.0, "  ") is None
    assert Money.from_value(10.0, "gbx") == Money(10.0, "GBX")


def test_of_factory_matches_constructor() -> None:
    assert Money.of(5.0, "EUR") == Money(5.0, "EUR")


# --------------------------------------------------------------------------
# Equality & hashing
# --------------------------------------------------------------------------


def test_equality_uses_normalized_values() -> None:
    # 2500 GBX and 25 GBP are the same money after normalization.
    assert Money(2500.0, "GBX") == Money(25.0, "GBP")
    assert Money(1.0, "USD") != Money(1.0, "EUR")


def test_money_is_hashable() -> None:
    pool = {Money(1.0, "USD"), Money(1.0, "USD"), Money(1.0, "EUR")}
    assert len(pool) == 2


# --------------------------------------------------------------------------
# Arithmetic
# --------------------------------------------------------------------------


def test_add_and_sub_same_currency() -> None:
    assert Money(2.0, "USD") + Money(3.0, "USD") == Money(5.0, "USD")
    assert Money(5.0, "USD") - Money(3.0, "USD") == Money(2.0, "USD")


def test_add_across_currencies_raises() -> None:
    with pytest.raises(CurrencyMismatchError) as exc:
        _ = Money(2.0, "USD") + Money(3.0, "EUR")
    assert exc.value.left == "USD"
    assert exc.value.right == "EUR"
    assert exc.value.operation == "add"


def test_sub_across_currencies_raises() -> None:
    with pytest.raises(CurrencyMismatchError):
        _ = Money(2.0, "USD") - Money(3.0, "GBP")


def test_sum_of_money_iterable() -> None:
    # sum() starts from int 0, which __radd__ treats as the identity.
    assert sum([Money(1.0, "USD"), Money(2.0, "USD"), Money(3.0, "USD")]) == Money(
        6.0, "USD"
    )


def test_scalar_multiplication_both_sides() -> None:
    assert Money(4.0, "USD") * 2 == Money(8.0, "USD")
    assert 2 * Money(4.0, "USD") == Money(8.0, "USD")


def test_division_by_scalar_yields_money() -> None:
    assert Money(10.0, "USD") / 4 == Money(2.5, "USD")


def test_division_money_by_money_yields_float_ratio() -> None:
    ratio = Money(10.0, "USD") / Money(4.0, "USD")
    assert isinstance(ratio, float)
    assert ratio == pytest.approx(2.5)


def test_division_money_by_money_across_currencies_raises() -> None:
    with pytest.raises(CurrencyMismatchError):
        _ = Money(10.0, "USD") / Money(4.0, "EUR")


def test_division_by_zero_money_raises() -> None:
    with pytest.raises(ZeroDivisionError):
        _ = Money(10.0, "USD") / Money(0.0, "USD")


def test_neg_and_abs() -> None:
    assert -Money(5.0, "USD") == Money(-5.0, "USD")
    assert abs(Money(-5.0, "USD")) == Money(5.0, "USD")


# --------------------------------------------------------------------------
# Ordering
# --------------------------------------------------------------------------


def test_ordering_same_currency() -> None:
    assert Money(1.0, "USD") < Money(2.0, "USD")
    assert Money(2.0, "USD") >= Money(2.0, "USD")
    assert sorted([Money(3.0, "USD"), Money(1.0, "USD"), Money(2.0, "USD")]) == [
        Money(1.0, "USD"),
        Money(2.0, "USD"),
        Money(3.0, "USD"),
    ]


def test_ordering_across_currencies_raises() -> None:
    with pytest.raises(CurrencyMismatchError):
        _ = Money(1.0, "USD") < Money(2.0, "EUR")


# --------------------------------------------------------------------------
# Conversion
# --------------------------------------------------------------------------


def test_convert_same_currency_is_noop() -> None:
    money = Money(10.0, "USD")
    assert money.convert(
        "USD", fx_service=_StubFXService(None), as_of="2024-01-01"
    ) is (money)


def test_convert_applies_rate() -> None:
    money = Money(10.0, "USD")
    converted = money.convert(
        "EUR", fx_service=_StubFXService(Decimal("0.9")), as_of="2024-01-01"
    )
    assert converted == Money(9.0, "EUR")


def test_convert_missing_rate_returns_none() -> None:
    money = Money(10.0, "USD")
    assert (
        money.convert("EUR", fx_service=_StubFXService(None), as_of="2024-01-01")
        is None
    )


def test_convert_or_raise_raises_on_missing_rate() -> None:
    money = Money(10.0, "USD")
    with pytest.raises(MissingFXRateError) as exc:
        money.convert_or_raise(
            "EUR", fx_service=_StubFXService(None), as_of="2024-01-01"
        )
    assert exc.value.base_currency == "USD"
    assert exc.value.quote_currency == "EUR"


def test_convert_subunit_target_normalizes_before_conversion() -> None:
    # Target "GBX" must be treated as GBP; a USD->GBP rate is applied.
    money = Money(10.0, "USD")
    converted = money.convert(
        "GBX", fx_service=_StubFXService(Decimal("0.8")), as_of="2024-01-01"
    )
    assert converted == Money(8.0, "GBP")


# --------------------------------------------------------------------------
# Property-based invariants
# --------------------------------------------------------------------------


@given(amount=FINITE_FLOATS, currency=st.sampled_from(MAJOR_CODES))
def test_major_currency_amount_is_unchanged(amount: float, currency: str) -> None:
    money = Money(amount, currency)
    assert money.currency == currency
    assert money.amount == amount


@given(amount=FINITE_FLOATS, subunit=st.sampled_from(sorted(SUBUNIT_TO_BASE)))
def test_subunit_amount_is_divided_by_100(amount: float, subunit: str) -> None:
    money = Money(amount, subunit)
    assert money.currency == SUBUNIT_TO_BASE[subunit]
    assert money.amount == pytest.approx(amount / 100.0)


@given(a=FINITE_FLOATS, b=FINITE_FLOATS, currency=st.sampled_from(MAJOR_CODES))
def test_addition_is_commutative_within_a_currency(
    a: float, b: float, currency: str
) -> None:
    assert Money(a, currency) + Money(b, currency) == Money(b, currency) + Money(
        a, currency
    )


@given(
    a=FINITE_FLOATS,
    b=FINITE_FLOATS,
    pair=st.sampled_from([(x, y) for x in MAJOR_CODES for y in MAJOR_CODES if x != y]),
)
def test_cross_currency_addition_always_raises(
    a: float, b: float, pair: tuple[str, str]
) -> None:
    left, right = pair
    with pytest.raises(CurrencyMismatchError):
        _ = Money(a, left) + Money(b, right)
