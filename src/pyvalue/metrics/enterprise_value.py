"""Shared enterprise-value helpers for EV-based metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricCurrencyInvariantError
from pyvalue.metrics.utils import (
    SHARE_COUNT_CONCEPTS,
    market_cap_money,
    require_metric_money,
)
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

LOGGER = logging.getLogger(__name__)

EV_FALLBACK_REQUIRED_CONCEPTS = (
    "EnterpriseValue",
    "ShortTermDebt",
    "LongTermDebt",
    "CashAndShortTermInvestments",
    # The market-cap fallback (market cap = shares x price) reads a share-count
    # fact, so every EV metric that can fall back must preload it. See
    # metrics.utils.market_cap_money.
    *SHARE_COUNT_CONCEPTS,
)


def _money(
    fact: MonetaryFact, *, target_currency: str, symbol: str, context: str
) -> Money:
    """Align one EV fact to the target (listing) currency via the Money seam."""

    return require_metric_money(
        fact.money,
        target_currency=target_currency,
        metric_id=context,
        symbol=symbol,
        input_name=fact.concept,
        as_of=fact.end_date,
    )


def resolve_enterprise_value_denominator(
    *,
    symbol: str,
    repo: RegionFactsRepository,
    market_repo: MarketDataRepository,
    target_currency: str,
    context: str,
) -> Optional[Money]:
    """Resolve EV (as ``Money``) using the EV fact first, then a debt/cash build.

    The reported ``EnterpriseValue`` fact wins when present and positive; failing
    that, EV is rebuilt as market cap + total debt - cash. Every input is aligned
    to ``target_currency`` through the shared Money seam, so the result is a
    single-currency ``Money`` and a mismatched EV fact degrades to the fallback
    rather than mixing currencies.
    """

    ev_fact = repo.latest_monetary_fact(symbol, "EnterpriseValue")
    if ev_fact is not None:
        try:
            ev_money = _money(
                ev_fact,
                target_currency=target_currency,
                symbol=symbol,
                context=context,
            )
        except MetricCurrencyInvariantError as exc:
            LOGGER.warning(
                "%s: unusable enterprise value fact for %s (%s); trying fallback",
                context,
                symbol,
                exc.summary_reason,
            )
        else:
            if ev_money.amount > 0:
                return ev_money
            LOGGER.warning(
                "%s: non-positive normalized enterprise value for %s; trying fallback",
                context,
                symbol,
            )

    cap = market_cap_money(
        symbol,
        repo=repo,
        market_repo=market_repo,
        metric_id=context,
        target_currency=target_currency,
        contexts=(market_repo, repo),
    )
    if cap is None:
        LOGGER.warning("%s: missing market cap for %s", context, symbol)
        return None

    short_debt = repo.latest_monetary_fact(symbol, "ShortTermDebt")
    long_debt = repo.latest_monetary_fact(symbol, "LongTermDebt")
    cash = repo.latest_monetary_fact(symbol, "CashAndShortTermInvestments")
    if short_debt is None or long_debt is None or cash is None:
        LOGGER.warning(
            "%s: missing EV fallback debt/cash facts for %s", context, symbol
        )
        return None

    enterprise_value = (
        cap.money
        + _money(
            short_debt, target_currency=target_currency, symbol=symbol, context=context
        )
        + _money(
            long_debt, target_currency=target_currency, symbol=symbol, context=context
        )
        - _money(cash, target_currency=target_currency, symbol=symbol, context=context)
    )
    if enterprise_value.amount <= 0:
        LOGGER.warning("%s: non-positive derived EV for %s", context, symbol)
        return None

    return enterprise_value


__all__ = [
    "EV_FALLBACK_REQUIRED_CONCEPTS",
    "resolve_enterprise_value_denominator",
]
