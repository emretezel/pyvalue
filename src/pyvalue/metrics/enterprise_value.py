"""Shared enterprise-value helpers for EV-based metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Callable, Optional, Sequence

import logging

from pyvalue.storage import FactRecord, FinancialFactsRepository, MarketDataRepository

LOGGER = logging.getLogger(__name__)

EV_FALLBACK_REQUIRED_CONCEPTS = (
    "EnterpriseValue",
    "ShortTermDebt",
    "LongTermDebt",
    "CashAndShortTermInvestments",
)

FXConverter = Callable[[float, str, str, str], Optional[float]]


def normalize_fact_value(record: FactRecord) -> tuple[float, Optional[str]]:
    """Normalize subunit FX codes so EV helpers can compare currencies safely."""

    value = record.value
    currency = record.currency
    if currency in {"GBX", "GBP0.01"}:
        return value / 100.0, "GBP"
    return value, currency


def merge_currency_codes(codes: Sequence[Optional[str]]) -> Optional[str]:
    merged: Optional[str] = None
    for code in codes:
        if not code:
            continue
        if merged is None:
            merged = code
        elif merged != code:
            return None
    return merged


def convert_denominator_amount(
    *,
    symbol: str,
    amount: float,
    source_currency: Optional[str],
    target_currency: Optional[str],
    as_of: str,
    context: str,
    converter: FXConverter,
) -> Optional[float]:
    if target_currency and source_currency and source_currency != target_currency:
        converted = converter(amount, source_currency, target_currency, as_of)
        if converted is None:
            LOGGER.warning(
                "%s: FX conversion failed %s -> %s for %s",
                context,
                source_currency,
                target_currency,
                symbol,
            )
            return None
        return converted
    return amount


def resolve_enterprise_value_denominator(
    *,
    symbol: str,
    repo: FinancialFactsRepository,
    market_repo: MarketDataRepository,
    target_currency: Optional[str],
    context: str,
    converter: FXConverter,
) -> Optional[float]:
    """Resolve EV using normalized EV first, then the existing debt/cash fallback."""

    ev_fact = repo.latest_fact(symbol, "EnterpriseValue")
    if ev_fact is not None:
        ev_value, ev_currency = normalize_fact_value(ev_fact)
        if ev_value > 0:
            converted = convert_denominator_amount(
                symbol=symbol,
                amount=ev_value,
                source_currency=ev_currency,
                target_currency=target_currency,
                as_of=ev_fact.end_date,
                context=context,
                converter=converter,
            )
            if converted is not None and converted > 0:
                return converted
            if converted is not None and converted <= 0:
                LOGGER.warning(
                    "%s: non-positive enterprise value after FX for %s",
                    context,
                    symbol,
                )
            return None
        LOGGER.warning(
            "%s: non-positive normalized enterprise value for %s; trying fallback",
            context,
            symbol,
        )

    snapshot = market_repo.latest_snapshot(symbol)
    if snapshot is None or snapshot.market_cap is None:
        LOGGER.warning("%s: missing market cap snapshot for %s", context, symbol)
        return None
    if snapshot.market_cap <= 0:
        LOGGER.warning("%s: non-positive market cap snapshot for %s", context, symbol)
        return None

    short_debt = repo.latest_fact(symbol, "ShortTermDebt")
    long_debt = repo.latest_fact(symbol, "LongTermDebt")
    cash = repo.latest_fact(symbol, "CashAndShortTermInvestments")
    if short_debt is None or long_debt is None or cash is None:
        LOGGER.warning(
            "%s: missing EV fallback debt/cash facts for %s", context, symbol
        )
        return None

    short_value, short_currency = normalize_fact_value(short_debt)
    long_value, long_currency = normalize_fact_value(long_debt)
    cash_value, cash_currency = normalize_fact_value(cash)
    currency = merge_currency_codes(
        [
            getattr(snapshot, "currency", None),
            short_currency,
            long_currency,
            cash_currency,
        ]
    )
    if currency is None and any(
        code is not None
        for code in (
            getattr(snapshot, "currency", None),
            short_currency,
            long_currency,
            cash_currency,
        )
    ):
        LOGGER.warning("%s: EV fallback currency mismatch for %s", context, symbol)
        return None

    ev_value = snapshot.market_cap + short_value + long_value - cash_value
    if ev_value <= 0:
        LOGGER.warning("%s: non-positive derived EV for %s", context, symbol)
        return None

    converted = convert_denominator_amount(
        symbol=symbol,
        amount=ev_value,
        source_currency=currency,
        target_currency=target_currency,
        as_of=snapshot.as_of,
        context=context,
        converter=converter,
    )
    if converted is None or converted <= 0:
        if converted is not None:
            LOGGER.warning("%s: non-positive EV after FX for %s", context, symbol)
        return None
    return converted


__all__ = [
    "EV_FALLBACK_REQUIRED_CONCEPTS",
    "convert_denominator_amount",
    "merge_currency_codes",
    "normalize_fact_value",
    "resolve_enterprise_value_denominator",
]
