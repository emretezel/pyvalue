"""Shared monetary normalization and FX conversion helpers.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import tempfile
from typing import Callable, Iterable, Optional, overload

from pyvalue.config import Config
from pyvalue.currency import (
    is_monetary_unit_kind,
    merge_currency_codes,
    normalize_currency_code,
    normalize_monetary_amount,
)
from pyvalue.fx import FXService, MissingFXRateError
from pyvalue.storage import FactRecord


_EPHEMERAL_FX_DATABASE = Path(tempfile.gettempdir()) / "pyvalue_ephemeral_fx.db"


class CurrencyMismatchError(ValueError):
    """Raised when an arithmetic or comparison op mixes two different currencies.

    ``Money`` deliberately refuses to combine amounts in different currencies so
    that a metric can never *silently* add, subtract or compare values that have
    not first been converted to a common currency. The motivation is the whole
    point of the type: bare-float math let a USD fundamental and a EUR price
    combine without anyone noticing. Callers must convert (see
    :meth:`Money.convert`) before operating across currencies.
    """

    def __init__(self, left: str, right: str, operation: str) -> None:
        self.left = left
        self.right = right
        self.operation = operation
        super().__init__(f"Currency mismatch in {operation}: {left} vs {right}")


@dataclass(frozen=True)
class Money:
    """An amount of money tied to a single, normalized *major* currency.

    Why this type exists: monetary facts and market prices can be denominated in
    different currencies, and some quote currencies are subunits (GBX/ZAC/ILA =
    base/100). Operating on bare floats lets mismatched currencies combine
    silently. ``Money`` keeps the currency travelling with the amount and turns
    any cross-currency arithmetic into a hard :class:`CurrencyMismatchError`, so
    each metric is forced to convert every input to one target currency first.

    Invariants (enforced in :meth:`__post_init__`):

    * ``currency`` is always a normalized *major* ISO code. Subunit inputs are
      collapsed to their base currency and the amount divided by the subunit
      divisor (reusing :func:`pyvalue.currency.normalize_monetary_amount`), so a
      ``Money`` can never hold pence/agorot/cents -- the same guarantee the data
      boundary enforces for ``market_data.price`` and ``financial_facts.value``.
    * ``amount`` is a ``float`` (project policy permits REAL/float everywhere).

    A ``Money`` is never currency-less: constructing one with a missing or
    unparseable currency raises ``ValueError`` (use :meth:`from_value` for the
    soft, ``Optional``-returning path).

    Equality and hashing come from the frozen dataclass and compare
    ``(amount, currency)`` *after* normalization, so ``Money(2500, "GBX")``
    equals ``Money(25, "GBP")`` and never raises on a currency difference.
    Ordering (``<``, ``<=`` ...) is currency-safe and raises on a mismatch.
    """

    amount: float
    currency: str

    def __post_init__(self) -> None:
        # Normalize subunit -> major exactly once, at construction, so every
        # downstream operation can assume a major currency and equal-currency
        # comparisons are meaningful. Re-running this on a derived result (e.g.
        # the output of ``__add__``) is idempotent for major currencies.
        normalized_amount, normalized_currency = normalize_monetary_amount(
            self.amount, self.currency
        )
        if normalized_currency is None:
            raise ValueError(f"Money requires a currency code; got {self.currency!r}")
        if normalized_amount is None:
            raise ValueError(f"Money requires a numeric amount; got {self.amount!r}")
        object.__setattr__(self, "amount", float(normalized_amount))
        object.__setattr__(self, "currency", normalized_currency)

    # -- factories --------------------------------------------------------

    @classmethod
    def of(cls, amount: float, currency: str) -> "Money":
        """Build a ``Money`` from an amount and a (possibly subunit) currency."""

        return cls(amount, currency)

    @classmethod
    def from_value(cls, amount: float | None, currency: str | None) -> "Money | None":
        """Build a ``Money`` or return ``None`` when it cannot be formed.

        Bridges the many tuple-returning helpers (e.g.
        :func:`normalize_money_value`) whose amount/currency may be ``None``.
        Returns ``None`` when ``amount`` is ``None`` or ``currency`` is
        missing/unparseable; otherwise behaves like the constructor.
        """

        if amount is None:
            return None
        # Reject a missing/unparseable currency, but pass the *original* code to
        # the constructor so a raw subunit (e.g. "GBX") still gets its amount
        # divided -- pre-normalizing here would collapse the code without
        # dividing the amount, silently inflating subunit values 100x.
        if currency is None or normalize_currency_code(currency) is None:
            return None
        return cls(amount, currency)

    # -- currency-safe arithmetic -----------------------------------------

    def _require_same_currency(self, other: "Money", operation: str) -> None:
        if self.currency != other.currency:
            raise CurrencyMismatchError(self.currency, other.currency, operation)

    def __add__(self, other: "Money") -> "Money":
        if not isinstance(other, Money):
            return NotImplemented
        self._require_same_currency(other, "add")
        return Money(self.amount + other.amount, self.currency)

    def __radd__(self, other: object) -> "Money":
        # Support ``sum(iterable_of_money)``, which starts from the int ``0``.
        if other == 0:
            return self
        return NotImplemented

    def __sub__(self, other: "Money") -> "Money":
        if not isinstance(other, Money):
            return NotImplemented
        self._require_same_currency(other, "subtract")
        return Money(self.amount - other.amount, self.currency)

    def __mul__(self, factor: float) -> "Money":
        # Money * scalar -> Money. Money * Money is intentionally undefined (the
        # product of two currency amounts is not itself a currency amount).
        if not isinstance(factor, (int, float)) or isinstance(factor, bool):
            return NotImplemented
        return Money(self.amount * factor, self.currency)

    __rmul__ = __mul__

    @overload
    def __truediv__(self, divisor: "Money") -> float: ...

    @overload
    def __truediv__(self, divisor: float) -> "Money": ...

    def __truediv__(self, divisor: "float | Money") -> "Money | float":
        # Money / Money -> dimensionless ratio (float), same currency only.
        # Money / scalar -> Money. Cross-currency division raises.
        if isinstance(divisor, Money):
            self._require_same_currency(divisor, "divide")
            if divisor.amount == 0:
                raise ZeroDivisionError("division by a zero-amount Money")
            return self.amount / divisor.amount
        if not isinstance(divisor, (int, float)) or isinstance(divisor, bool):
            return NotImplemented
        return Money(self.amount / divisor, self.currency)

    def __neg__(self) -> "Money":
        return Money(-self.amount, self.currency)

    def __abs__(self) -> "Money":
        return Money(abs(self.amount), self.currency)

    # -- currency-safe ordering (eq/hash provided by the frozen dataclass) --

    def __lt__(self, other: "Money") -> bool:
        if not isinstance(other, Money):
            return NotImplemented
        self._require_same_currency(other, "compare")
        return self.amount < other.amount

    def __le__(self, other: "Money") -> bool:
        if not isinstance(other, Money):
            return NotImplemented
        self._require_same_currency(other, "compare")
        return self.amount <= other.amount

    def __gt__(self, other: "Money") -> bool:
        if not isinstance(other, Money):
            return NotImplemented
        self._require_same_currency(other, "compare")
        return self.amount > other.amount

    def __ge__(self, other: "Money") -> bool:
        if not isinstance(other, Money):
            return NotImplemented
        self._require_same_currency(other, "compare")
        return self.amount >= other.amount

    # -- conversion -------------------------------------------------------

    def convert(
        self, target: str, *, fx_service: FXService, as_of: str
    ) -> "Money | None":
        """Convert into ``target`` currency, or ``None`` if no rate exists.

        Delegates to :meth:`FXService.convert_amount`. A same-currency convert
        is a no-op returning ``self``. Returns ``None`` (soft fail) when the
        rate is unavailable; callers that must hard-fail use
        :meth:`convert_or_raise`.
        """

        normalized_target = normalize_currency_code(target)
        if normalized_target is None:
            raise ValueError(f"convert requires a target currency; got {target!r}")
        if normalized_target == self.currency:
            return self
        converted = fx_service.convert_amount(
            self.amount, self.currency, normalized_target, as_of
        )
        if converted is None:
            return None
        return Money(float(converted), normalized_target)

    def convert_or_raise(
        self, target: str, *, fx_service: FXService, as_of: str
    ) -> "Money":
        """Like :meth:`convert` but raise ``MissingFXRateError`` on no rate."""

        result = self.convert(target, fx_service=fx_service, as_of=as_of)
        if result is None:
            raise MissingFXRateError(
                provider=fx_service.provider_name,
                base_currency=self.currency,
                quote_currency=normalize_currency_code(target) or str(target),
                as_of=str(as_of),
            )
        return result


class _NoFetchFXConfig(Config):
    def __init__(self) -> None:
        pass

    @property
    def fx_provider(self) -> str:
        return "EODHD"

    @property
    def fx_pivot_currency(self) -> str:
        return "USD"

    @property
    def fx_secondary_pivot_currency(self) -> Optional[str]:
        return "EUR"

    @property
    def fx_stale_warning_days(self) -> int:
        return 7


def normalize_money_value(
    amount: float | None,
    currency: Optional[str],
) -> tuple[Optional[float], Optional[str]]:
    """Return a normalized monetary amount and currency.

    Configured subunit currencies are converted into their base currencies before
    any downstream arithmetic.
    """

    normalized_amount, normalized_currency = normalize_monetary_amount(amount, currency)
    if normalized_amount is None:
        return None, normalized_currency
    return float(normalized_amount), normalized_currency


def normalize_fact_value(record: FactRecord) -> tuple[Optional[float], Optional[str]]:
    """Return a normalized numeric value and currency for one stored fact.

    ``unit_kind`` is authoritative: only monetary / per_share facts carry a
    currency (the schema couples the two), so a non-monetary fact returns its
    raw value with no currency. Monetary facts already hold a *major* currency
    and a major amount, so ``normalize_money_value`` is effectively a no-op kept
    for defensiveness against any subunit code that predates migration 071.
    """

    if not is_monetary_unit_kind(record.unit_kind):
        return record.value, None
    return normalize_money_value(record.value, record.currency)


def fx_service_for_context(
    *objects: object,
    default_database: str | Path = _EPHEMERAL_FX_DATABASE,
) -> FXService:
    """Return an FX service using the first available ``db_path``."""

    for obj in objects:
        database = getattr(obj, "db_path", None)
        if database is not None:
            return FXService(database)
    return FXService(default_database, config=_NoFetchFXConfig())


def ephemeral_fx_database_path() -> Path:
    """Return the local no-fetch FX database path used for test doubles."""

    return _EPHEMERAL_FX_DATABASE


def fx_converter_for_context(
    *objects: object,
    default_database: str | Path = _EPHEMERAL_FX_DATABASE,
) -> Callable[[float, str, str, str], Optional[float]]:
    """Return a float-based FX converter compatible with legacy helper APIs."""

    service = fx_service_for_context(*objects, default_database=default_database)

    def convert(
        amount: float,
        from_currency: str,
        to_currency: str,
        as_of: str,
    ) -> Optional[float]:
        converted = service.convert_amount(amount, from_currency, to_currency, as_of)
        if converted is None:
            return None
        return float(converted)

    return convert


def choose_target_currency(currencies: Iterable[Optional[str]]) -> Optional[str]:
    """Return the first normalized non-null currency code."""

    for currency in currencies:
        normalized = normalize_currency_code(currency)
        if normalized is not None:
            return normalized
    return None


def currencies_match(currencies: Iterable[Optional[str]]) -> bool:
    """Return True when all non-null currencies agree after normalization."""

    return merge_currency_codes(list(currencies)) is not None


def convert_money_value(
    *,
    amount: float,
    source_currency: Optional[str],
    target_currency: Optional[str],
    as_of: str,
    fx_service: Optional[FXService],
    logger: logging.Logger,
    operation: str,
    symbol: str,
    field_name: str,
    raise_on_missing_fx: bool = False,
) -> Optional[float]:
    """Convert one monetary amount into ``target_currency`` with structured warnings."""

    normalized_amount, normalized_source = normalize_money_value(
        amount, source_currency
    )
    normalized_target = normalize_currency_code(target_currency)
    if normalized_amount is None:
        return None
    if normalized_source is None:
        logger.warning(
            "Missing currency for monetary value | operation=%s symbol=%s field=%s as_of=%s",
            operation,
            symbol,
            field_name,
            as_of,
        )
        return None
    if normalized_target is None:
        logger.warning(
            "Missing target currency for monetary conversion | operation=%s symbol=%s field=%s source_currency=%s as_of=%s",
            operation,
            symbol,
            field_name,
            normalized_source,
            as_of,
        )
        return None
    if normalized_source == normalized_target:
        return normalized_amount
    if fx_service is None:
        logger.warning(
            "FX service unavailable for monetary conversion | operation=%s symbol=%s field=%s from=%s to=%s as_of=%s",
            operation,
            symbol,
            field_name,
            normalized_source,
            normalized_target,
            as_of,
        )
        return None
    converted = fx_service.convert_amount(
        normalized_amount,
        normalized_source,
        normalized_target,
        as_of,
    )
    if converted is None:
        logger.warning(
            "Missing FX rate for monetary conversion | operation=%s symbol=%s field=%s from=%s to=%s as_of=%s",
            operation,
            symbol,
            field_name,
            normalized_source,
            normalized_target,
            as_of,
        )
        if raise_on_missing_fx:
            raise MissingFXRateError(
                provider=fx_service.provider_name,
                base_currency=normalized_source,
                quote_currency=normalized_target,
                as_of=as_of,
            )
        return None
    return float(converted)


def align_money_values(
    *,
    values: Iterable[tuple[float, Optional[str], str, str]],
    fx_service: Optional[FXService],
    logger: logging.Logger,
    operation: str,
    symbol: str,
    target_currency: Optional[str] = None,
    raise_on_missing_fx: bool = False,
) -> tuple[Optional[list[float]], Optional[str]]:
    """Convert a sequence of monetary values into one target currency.

    ``values`` items must be ``(amount, currency, as_of, field_name)`` tuples.
    """

    collected = list(values)
    resolved_target = normalize_currency_code(
        target_currency
    ) or choose_target_currency(currency for _, currency, _, _ in collected)
    if resolved_target is None:
        logger.warning(
            "Missing target currency for monetary alignment | operation=%s symbol=%s",
            operation,
            symbol,
        )
        return None, None

    aligned: list[float] = []
    for amount, currency, as_of, field_name in collected:
        converted = convert_money_value(
            amount=amount,
            source_currency=currency,
            target_currency=resolved_target,
            as_of=as_of,
            fx_service=fx_service,
            logger=logger,
            operation=operation,
            symbol=symbol,
            field_name=field_name,
            raise_on_missing_fx=raise_on_missing_fx,
        )
        if converted is None:
            return None, None
        aligned.append(converted)
    return aligned, resolved_target


__all__ = [
    "CurrencyMismatchError",
    "Money",
    "align_money_values",
    "choose_target_currency",
    "convert_money_value",
    "currencies_match",
    "ephemeral_fx_database_path",
    "fx_converter_for_context",
    "fx_service_for_context",
    "normalize_fact_value",
    "normalize_money_value",
]
