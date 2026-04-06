"""Shared currency and unit helpers.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Literal, Mapping, Optional, Sequence
import logging


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CurrencySubunit:
    """Metadata describing a provider subunit currency code."""

    base_currency: str
    divisor: Decimal


SUBUNIT_CURRENCY_REGISTRY = MappingProxyType(
    {
        "GBX": CurrencySubunit(base_currency="GBP", divisor=Decimal("100")),
        "GBP0.01": CurrencySubunit(base_currency="GBP", divisor=Decimal("100")),
        "ZAC": CurrencySubunit(base_currency="ZAR", divisor=Decimal("100")),
        "ILA": CurrencySubunit(base_currency="ILS", divisor=Decimal("100")),
    }
)
SUBUNIT_BASE_CURRENCIES = frozenset(
    info.base_currency for info in SUBUNIT_CURRENCY_REGISTRY.values()
)
GBX_SUBUNIT_CODES = frozenset({"GBX", "GBP0.01"})
GBX_TO_GBP_RATIO = Decimal("100")
MetricUnitKind = Literal[
    "monetary",
    "per_share",
    "ratio",
    "percent",
    "multiple",
    "count",
    "other",
]
MONETARY_UNIT_KINDS = frozenset({"monetary", "per_share"})
SHARES_UNIT = "shares"


@dataclass(frozen=True)
class CurrencyResolution:
    """Resolved currency for one raw monetary payload field."""

    currency_code: Optional[str]
    source: Optional[str]


def to_decimal(value: Any) -> Optional[Decimal]:
    """Return ``value`` as a ``Decimal`` when possible."""

    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def normalize_currency_code(value: object) -> Optional[str]:
    """Normalize a raw currency code and collapse configured subunits."""

    if value is None:
        return None
    try:
        code = str(value).strip().upper()
    except Exception:
        return None
    if not code:
        return None
    subunit = currency_subunit(value)
    if subunit is not None:
        return subunit.base_currency
    return code


def legacy_currency_from_unit(unit: object) -> Optional[str]:
    """Return a documented legacy currency fallback inferred from ``unit``."""

    raw_unit = raw_currency_code(unit)
    if raw_unit is None:
        return None
    if raw_unit == SHARES_UNIT.upper():
        return None
    if "/" in raw_unit:
        return None
    if is_subunit_currency(raw_unit):
        return normalize_currency_code(raw_unit)
    if len(raw_unit) == 3 and raw_unit.isalpha():
        return normalize_currency_code(raw_unit)
    return None


def fact_currency_or_none(
    currency_code: object,
    unit: object,
) -> Optional[str]:
    """Return a normalized fact currency using the documented legacy fallback."""

    explicit = raw_currency_code(currency_code)
    if explicit is not None:
        return explicit
    return legacy_currency_from_unit(unit)


def raw_currency_code(value: object) -> Optional[str]:
    """Normalize a raw currency code without applying subunit collapse."""

    if value is None:
        return None
    try:
        code = str(value).strip().upper()
    except Exception:
        return None
    return code or None


def currency_subunit(value: object) -> Optional[CurrencySubunit]:
    """Return subunit metadata for ``value`` when configured."""

    code = raw_currency_code(value)
    if code is None:
        return None
    return SUBUNIT_CURRENCY_REGISTRY.get(code)


def is_subunit_currency(value: object) -> bool:
    """Return True when ``value`` denotes a configured subunit code."""

    return currency_subunit(value) is not None


def subunit_base_currency(value: object) -> Optional[str]:
    """Return the normalized base currency for a configured subunit code."""

    subunit = currency_subunit(value)
    if subunit is None:
        return None
    return subunit.base_currency


def subunit_divisor(value: object) -> Optional[Decimal]:
    """Return the amount divisor for a configured subunit code."""

    subunit = currency_subunit(value)
    if subunit is None:
        return None
    return subunit.divisor


def is_subunit_base_currency(value: object) -> bool:
    """Return True when ``value`` belongs to a configured subunit family."""

    normalized = normalize_currency_code(value)
    if normalized is None:
        return False
    return normalized in SUBUNIT_BASE_CURRENCIES


def is_gbx_subunit_currency(value: object) -> bool:
    """Return True when ``value`` denotes pence rather than pounds."""

    code = raw_currency_code(value)
    return code in GBX_SUBUNIT_CODES


def normalize_monetary_amount(
    amount: Any,
    currency_code: object,
) -> tuple[Optional[Decimal], Optional[str]]:
    """Normalize a monetary amount and its currency code.

    Configured subunit currencies are always converted into their base currencies
    before the amount is returned.
    """

    normalized_currency = normalize_currency_code(currency_code)
    decimal_value = to_decimal(amount)
    if decimal_value is None:
        return None, normalized_currency
    divisor = subunit_divisor(currency_code)
    if divisor is not None:
        decimal_value = decimal_value / divisor
    return decimal_value, normalized_currency


def is_monetary_unit_kind(unit_kind: Optional[str]) -> bool:
    """Return True when ``unit_kind`` represents a currency-bearing value."""

    if unit_kind is None:
        return False
    return unit_kind in MONETARY_UNIT_KINDS


def metric_currency_or_none(
    unit_kind: Optional[str],
    currency_code: Optional[str],
) -> Optional[str]:
    """Normalize metric currency only for monetary metric kinds."""

    if not is_monetary_unit_kind(unit_kind):
        return None
    return normalize_currency_code(currency_code)


def resolve_eodhd_currency(
    entry: Optional[Mapping[str, Any]],
    *,
    statement_currency: Optional[object] = None,
    payload_currency: Optional[object] = None,
    fallback_currency: Optional[object] = None,
) -> CurrencyResolution:
    """Resolve one EODHD monetary currency with explicit precedence.

    Precedence is:
    1. Entry-level currency on the specific statement or earnings row.
    2. Statement-level currency.
    3. Payload-level default currency.
    4. Optional documented fallback supplied by the caller.
    """

    for key in ("currency", "currency_symbol", "CurrencyCode"):
        if entry is None:
            break
        code = normalize_currency_code(entry.get(key))
        if code is not None:
            return CurrencyResolution(currency_code=code, source=f"entry:{key}")

    statement_code = normalize_currency_code(statement_currency)
    if statement_code is not None:
        return CurrencyResolution(currency_code=statement_code, source="statement")

    payload_code = normalize_currency_code(payload_currency)
    if payload_code is not None:
        return CurrencyResolution(currency_code=payload_code, source="payload")

    fallback_code = normalize_currency_code(fallback_currency)
    if fallback_code is not None:
        return CurrencyResolution(currency_code=fallback_code, source="fallback")

    return CurrencyResolution(currency_code=None, source=None)


def warn_missing_monetary_currency(
    *,
    symbol: str,
    field_name: str,
    statement_name: Optional[str] = None,
    end_date: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Emit a structured warning for unresolved raw monetary currency."""

    active_logger = logger or LOGGER
    active_logger.warning(
        "Missing currency for monetary field | symbol=%s statement=%s field=%s end_date=%s",
        symbol,
        statement_name or "unknown",
        field_name,
        end_date or "unknown",
    )


def merge_currency_codes(codes: Sequence[Optional[str]]) -> Optional[str]:
    """Return a shared normalized currency code when all non-null inputs agree."""

    merged: Optional[str] = None
    for code in codes:
        normalized = normalize_currency_code(code)
        if normalized is None:
            continue
        if merged is None:
            merged = normalized
            continue
        if merged != normalized:
            return None
    return merged


__all__ = [
    "CurrencyResolution",
    "CurrencySubunit",
    "GBX_SUBUNIT_CODES",
    "GBX_TO_GBP_RATIO",
    "MONETARY_UNIT_KINDS",
    "MetricUnitKind",
    "SHARES_UNIT",
    "SUBUNIT_BASE_CURRENCIES",
    "SUBUNIT_CURRENCY_REGISTRY",
    "currency_subunit",
    "fact_currency_or_none",
    "is_gbx_subunit_currency",
    "is_subunit_base_currency",
    "is_subunit_currency",
    "is_monetary_unit_kind",
    "legacy_currency_from_unit",
    "merge_currency_codes",
    "metric_currency_or_none",
    "normalize_currency_code",
    "normalize_monetary_amount",
    "raw_currency_code",
    "resolve_eodhd_currency",
    "subunit_base_currency",
    "subunit_divisor",
    "to_decimal",
    "warn_missing_monetary_currency",
]
