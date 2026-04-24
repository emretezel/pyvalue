"""Shared enterprise-value helpers for EV-based metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Optional

import logging

from pyvalue.metrics.base import MetricCurrencyInvariantError
from pyvalue.metrics.utils import normalize_metric_amount, normalize_metric_record
from pyvalue.storage import FactRecord, FinancialFactsRepository, MarketDataRepository

LOGGER = logging.getLogger(__name__)

EV_FALLBACK_REQUIRED_CONCEPTS = (
    "EnterpriseValue",
    "ShortTermDebt",
    "LongTermDebt",
    "CashAndShortTermInvestments",
)


def normalize_fact_value(
    record: FactRecord,
    *,
    metric_id: str,
    symbol: str,
    expected_currency: Optional[str],
    contexts: tuple[object, ...],
) -> tuple[float, str]:
    """Normalize one EV fact and enforce the listing-currency invariant."""

    normalized_value, normalized_currency = normalize_metric_record(
        record,
        metric_id=metric_id,
        symbol=symbol,
        expected_currency=expected_currency,
        contexts=contexts,
    )
    return normalized_value, normalized_currency


def validate_denominator_amount(
    *,
    symbol: str,
    amount: float,
    source_currency: Optional[str],
    target_currency: Optional[str],
    as_of: str,
    context: str,
    contexts: tuple[object, ...],
) -> float:
    return normalize_metric_amount(
        amount,
        source_currency,
        metric_id=context,
        symbol=symbol,
        input_name="denominator",
        as_of=as_of,
        expected_currency=target_currency,
        contexts=contexts,
    )[0]


def resolve_enterprise_value_denominator(
    *,
    symbol: str,
    repo: FinancialFactsRepository,
    market_repo: MarketDataRepository,
    target_currency: Optional[str],
    context: str,
) -> Optional[float]:
    """Resolve EV using normalized EV first, then the existing debt/cash fallback."""

    ev_fact = repo.latest_fact(symbol, "EnterpriseValue")
    if ev_fact is not None:
        try:
            ev_value, _ = normalize_fact_value(
                ev_fact,
                metric_id=context,
                symbol=symbol,
                expected_currency=target_currency,
                contexts=(repo, market_repo),
            )
        except MetricCurrencyInvariantError as exc:
            LOGGER.warning(
                "%s: unusable enterprise value fact for %s (%s); trying fallback",
                context,
                symbol,
                exc.summary_reason,
            )
        else:
            if ev_value > 0:
                return ev_value
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
    market_cap = validate_denominator_amount(
        symbol=symbol,
        amount=snapshot.market_cap,
        source_currency=getattr(snapshot, "currency", None),
        target_currency=target_currency,
        as_of=snapshot.as_of,
        context=context,
        contexts=(market_repo, repo),
    )

    short_debt = repo.latest_fact(symbol, "ShortTermDebt")
    long_debt = repo.latest_fact(symbol, "LongTermDebt")
    cash = repo.latest_fact(symbol, "CashAndShortTermInvestments")
    if short_debt is None or long_debt is None or cash is None:
        LOGGER.warning(
            "%s: missing EV fallback debt/cash facts for %s", context, symbol
        )
        return None

    short_value, _ = normalize_fact_value(
        short_debt,
        metric_id=context,
        symbol=symbol,
        expected_currency=target_currency,
        contexts=(repo, market_repo),
    )
    long_value, _ = normalize_fact_value(
        long_debt,
        metric_id=context,
        symbol=symbol,
        expected_currency=target_currency,
        contexts=(repo, market_repo),
    )
    cash_value, _ = normalize_fact_value(
        cash,
        metric_id=context,
        symbol=symbol,
        expected_currency=target_currency,
        contexts=(repo, market_repo),
    )

    ev_value = market_cap + short_value + long_value - cash_value
    if ev_value <= 0:
        LOGGER.warning("%s: non-positive derived EV for %s", context, symbol)
        return None

    return ev_value


__all__ = [
    "EV_FALLBACK_REQUIRED_CONCEPTS",
    "normalize_fact_value",
    "resolve_enterprise_value_denominator",
    "validate_denominator_amount",
]
