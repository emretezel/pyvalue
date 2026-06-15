"""Shared enterprise-value helpers for EV-based metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.utils import (
    SHARE_COUNT_CONCEPTS,
    market_cap_money,
    require_metric_money,
)
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

LOGGER = logging.getLogger(__name__)

EV_REQUIRED_CONCEPTS = (
    "ShortTermDebt",
    "LongTermDebt",
    "CashAndShortTermInvestments",
    # EV = market cap + total debt - cash. Market cap (= shares x price) reads a
    # share-count fact, so every EV metric must preload it too. See
    # metrics.utils.market_cap_money.
    *SHARE_COUNT_CONCEPTS,
)


def _money(
    fact: MonetaryFact, *, target_currency: str, listing_id: int, context: str
) -> Money:
    """Align one EV component fact to the target (listing) currency via the seam."""

    return require_metric_money(
        fact.money,
        target_currency=target_currency,
        metric_id=context,
        listing_id=listing_id,
        input_name=fact.concept,
        as_of=fact.end_date,
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
    price) plus short- and long-term debt minus cash -- rather than read from a
    stored ``EnterpriseValue`` fact, so it floats with every price refresh and
    uses one consistent definition across the universe. Every input is aligned to
    ``target_currency`` through the shared Money seam, so the result is a
    single-currency ``Money``. Returns ``None`` when market cap or any debt/cash
    component is missing, or when the computed EV is non-positive.
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

    short_debt = repo.latest_monetary_fact(listing_id, "ShortTermDebt")
    long_debt = repo.latest_monetary_fact(listing_id, "LongTermDebt")
    cash = repo.latest_monetary_fact(listing_id, "CashAndShortTermInvestments")
    if short_debt is None or long_debt is None or cash is None:
        LOGGER.warning(
            "%s: missing EV debt/cash facts for listing_id=%s", context, listing_id
        )
        return None

    enterprise_value = (
        cap.money
        + _money(
            short_debt,
            target_currency=target_currency,
            listing_id=listing_id,
            context=context,
        )
        + _money(
            long_debt,
            target_currency=target_currency,
            listing_id=listing_id,
            context=context,
        )
        - _money(
            cash,
            target_currency=target_currency,
            listing_id=listing_id,
            context=context,
        )
    )
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
