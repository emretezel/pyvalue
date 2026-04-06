"""Shared monetary normalization and FX conversion helpers.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from pathlib import Path
import tempfile
from typing import Callable, Iterable, Optional

from pyvalue.config import Config
from pyvalue.currency import (
    fact_currency_or_none,
    merge_currency_codes,
    normalize_currency_code,
    normalize_monetary_amount,
)
from pyvalue.fx import FXService
from pyvalue.storage import FactRecord


_EPHEMERAL_FX_DATABASE = Path(tempfile.gettempdir()) / "pyvalue_ephemeral_fx.db"


class _NoFetchFXConfig(Config):
    def __init__(self) -> None:
        pass

    @property
    def fx_pivot_currency(self) -> str:
        return "USD"

    @property
    def fx_secondary_pivot_currency(self) -> Optional[str]:
        return "EUR"

    @property
    def fx_lazy_fetch(self) -> bool:
        return False

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
    """Return a normalized numeric value and currency for one stored fact."""

    return normalize_money_value(
        record.value,
        fact_currency_or_none(
            getattr(record, "currency", None),
            getattr(record, "unit", None),
        ),
    )


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
        )
        if converted is None:
            return None, None
        aligned.append(converted)
    return aligned, resolved_target


__all__ = [
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
