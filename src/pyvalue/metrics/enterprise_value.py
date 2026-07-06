"""Shared enterprise-value helpers for EV-based metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Optional

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.balance_sheet import (
    CASH_CONCEPTS,
    DEBT_CONCEPTS,
    resolve_cash_position,
    resolve_total_debt,
)
from pyvalue.metrics.utils import SHARE_COUNT_CONCEPTS, market_cap_money
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

LOGGER = logging.getLogger(__name__)

EV_REQUIRED_CONCEPTS = (
    *DEBT_CONCEPTS,
    *CASH_CONCEPTS,
    # EV = market cap + total debt - cash. Market cap (= shares x price) reads a
    # share-count fact, so every EV metric must preload it too. See
    # metrics.utils.market_cap_money.
    *SHARE_COUNT_CONCEPTS,
)


def resolve_enterprise_value_denominator(
    *,
    listing_id: int,
    repo: RegionFactsRepository,
    market_repo: MarketDataRepository,
    target_currency: str,
    context: str,
) -> Optional[Money]:
    """Compute EV (as ``Money``) as market cap + total debt - cash.

    EV is always built from components -- market cap (latest shares x latest
    price) plus total debt minus cash -- rather than read from a stored
    ``EnterpriseValue`` fact, so it floats with every price refresh and uses
    one consistent definition across the universe. Debt and cash resolve
    through the same shared chains as ``net_debt_to_ebitda``
    (:mod:`pyvalue.metrics.balance_sheet`): at least one fresh debt side, and
    the cash rollup with its equivalents-plus-short-term-investments
    fallback -- so a listing carries an EV exactly when it carries a net-debt
    position. Every input is aligned to ``target_currency`` through the
    shared Money seam, so the result is a single-currency ``Money``. Returns
    ``None`` when market cap, debt, or cash cannot be resolved fresh, or when
    the computed EV is non-positive.
    """

    cap = market_cap_money(
        listing_id,
        repo=repo,
        market_repo=market_repo,
        metric_id=context,
        target_currency=target_currency,
        contexts=(market_repo, repo),
    )
    if cap is None:
        LOGGER.warning("%s: missing market cap for listing_id=%s", context, listing_id)
        return None

    debt = resolve_total_debt(
        listing_id, repo, target_currency=target_currency, metric_id=context
    )
    cash = resolve_cash_position(
        listing_id, repo, target_currency=target_currency, metric_id=context
    )
    if debt is None or cash is None:
        LOGGER.warning(
            "%s: missing EV debt/cash facts for listing_id=%s", context, listing_id
        )
        return None

    enterprise_value = cap.money + debt.money - cash.money
    if enterprise_value.amount <= 0:
        LOGGER.warning(
            "%s: non-positive derived EV for listing_id=%s", context, listing_id
        )
        return None

    return enterprise_value


__all__ = [
    "EV_REQUIRED_CONCEPTS",
    "resolve_enterprise_value_denominator",
]
