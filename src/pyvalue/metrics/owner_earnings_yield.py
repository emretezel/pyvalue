"""Owner earnings yield metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.enterprise_value import (
    EV_REQUIRED_CONCEPTS,
    resolve_enterprise_value_denominator,
)
from pyvalue.metrics.owner_earnings_enterprise import (
    REQUIRED_CONCEPTS as OE_EV_REQUIRED_CONCEPTS,
    OwnerEarningsEnterpriseCalculator,
)
from pyvalue.metrics.owner_earnings_equity import (
    REQUIRED_CONCEPTS as OE_EQUITY_REQUIRED_CONCEPTS,
    OwnerEarningsEquityCalculator,
)
from pyvalue.metrics.utils import SHARE_RESOLVER_REQUIRED_CONCEPTS, market_cap_money
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

LOGGER = logging.getLogger(__name__)

# The equity-yield denominator is market cap (shares x price), so preload the
# share-count concepts market_cap_money resolves alongside the owner-earnings
# equity concepts.
REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(OE_EQUITY_REQUIRED_CONCEPTS + SHARE_RESOLVER_REQUIRED_CONCEPTS)
)
REQUIRED_EV_CONCEPTS = tuple(
    dict.fromkeys(OE_EV_REQUIRED_CONCEPTS + EV_REQUIRED_CONCEPTS)
)


def _denominator_market_cap(
    *,
    listing_id: int,
    repo: RegionFactsRepository,
    market_repo: MarketDataRepository,
    target_currency: str,
    context: str,
) -> Optional[Money]:
    cap = market_cap_money(
        listing_id,
        repo=repo,
        market_repo=market_repo,
        metric_id=context,
        target_currency=target_currency,
        contexts=(repo, market_repo),
    )
    if cap is None:
        return None
    return cap.money


def _denominator_enterprise_value(
    *,
    listing_id: int,
    repo: RegionFactsRepository,
    market_repo: MarketDataRepository,
    target_currency: str,
    context: str,
) -> Optional[Money]:
    return resolve_enterprise_value_denominator(
        listing_id=listing_id,
        repo=repo,
        market_repo=market_repo,
        target_currency=target_currency,
        context=context,
    )


@dataclass
class OwnerEarningsYieldEquityMetric:
    """Compute owner earnings yield using TTM owner earnings equity."""

    id: str = "oey_equity"
    required_concepts = REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = OwnerEarningsEquityCalculator().compute_ttm(listing_id, repo)
        if numerator is None:
            LOGGER.warning(
                "oey_equity: missing numerator for listing_id=%s", listing_id
            )
            return None

        market_cap = _denominator_market_cap(
            listing_id=listing_id,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.money.currency,
            context=self.id,
        )
        if market_cap is None:
            return None

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=numerator.money / market_cap,
            as_of=numerator.as_of,
        )


@dataclass
class OwnerEarningsYieldEquityFiveYearMetric:
    """Compute owner earnings yield using 5-year average owner earnings equity."""

    id: str = "oey_equity_5y"
    required_concepts = REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = OwnerEarningsEquityCalculator().compute_5y_average(listing_id, repo)
        if numerator is None:
            LOGGER.warning(
                "oey_equity_5y: missing numerator for listing_id=%s", listing_id
            )
            return None

        market_cap = _denominator_market_cap(
            listing_id=listing_id,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.money.currency,
            context=self.id,
        )
        if market_cap is None:
            return None

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=numerator.money / market_cap,
            as_of=numerator.as_of,
        )


@dataclass
class OwnerEarningsYieldEVMetric:
    """Compute owner earnings yield using TTM owner earnings enterprise."""

    id: str = "oey_ev"
    required_concepts = REQUIRED_EV_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = OwnerEarningsEnterpriseCalculator().compute_ttm(listing_id, repo)
        if numerator is None:
            LOGGER.warning("oey_ev: missing numerator for listing_id=%s", listing_id)
            return None

        enterprise_value = _denominator_enterprise_value(
            listing_id=listing_id,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.money.currency,
            context=self.id,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=numerator.money / enterprise_value,
            as_of=numerator.as_of,
        )


@dataclass
class OwnerEarningsYieldEVNormalizedMetric:
    """Compute normalized owner earnings yield using FY median owner earnings enterprise."""

    id: str = "oey_ev_norm"
    required_concepts = REQUIRED_EV_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = OwnerEarningsEnterpriseCalculator().compute_5y_median(
            listing_id, repo
        )
        if numerator is None:
            LOGGER.warning(
                "oey_ev_norm: missing numerator for listing_id=%s", listing_id
            )
            return None

        enterprise_value = _denominator_enterprise_value(
            listing_id=listing_id,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.money.currency,
            context=self.id,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=numerator.money / enterprise_value,
            as_of=numerator.as_of,
        )


__all__ = [
    "OwnerEarningsYieldEquityMetric",
    "OwnerEarningsYieldEquityFiveYearMetric",
    "OwnerEarningsYieldEVMetric",
    "OwnerEarningsYieldEVNormalizedMetric",
]
