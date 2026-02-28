"""Owner earnings yield (equity) metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.fx import FXRateStore
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.owner_earnings_equity import (
    REQUIRED_CONCEPTS,
    OwnerEarningsEquityCalculator,
)
from pyvalue.storage import FinancialFactsRepository, MarketDataRepository

LOGGER = logging.getLogger(__name__)


def _denominator_market_cap(
    *,
    symbol: str,
    market_repo: MarketDataRepository,
    target_currency: Optional[str],
    context: str,
) -> Optional[float]:
    snapshot = market_repo.latest_snapshot(symbol)
    if snapshot is None or snapshot.market_cap is None:
        LOGGER.warning("%s: missing market cap snapshot for %s", context, symbol)
        return None
    if snapshot.market_cap <= 0:
        LOGGER.warning("%s: non-positive market cap snapshot for %s", context, symbol)
        return None

    market_cap = snapshot.market_cap
    snapshot_currency = getattr(snapshot, "currency", None)
    if target_currency and snapshot_currency and snapshot_currency != target_currency:
        converted = FXRateStore().convert(
            market_cap,
            snapshot_currency,
            target_currency,
            snapshot.as_of,
        )
        if converted is None:
            LOGGER.warning(
                "%s: FX conversion failed %s -> %s for %s",
                context,
                snapshot_currency,
                target_currency,
                symbol,
            )
            return None
        market_cap = converted
    return market_cap


@dataclass
class OwnerEarningsYieldEquityMetric:
    """Compute owner earnings yield using TTM owner earnings equity."""

    id: str = "oey_equity"
    required_concepts = REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = OwnerEarningsEquityCalculator().compute_ttm(symbol, repo)
        if numerator is None:
            LOGGER.warning("oey_equity: missing numerator for %s", symbol)
            return None

        market_cap = _denominator_market_cap(
            symbol=symbol,
            market_repo=market_repo,
            target_currency=numerator.currency,
            context=self.id,
        )
        if market_cap is None:
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=numerator.value / market_cap,
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
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = OwnerEarningsEquityCalculator().compute_5y_average(symbol, repo)
        if numerator is None:
            LOGGER.warning("oey_equity_5y: missing numerator for %s", symbol)
            return None

        market_cap = _denominator_market_cap(
            symbol=symbol,
            market_repo=market_repo,
            target_currency=numerator.currency,
            context=self.id,
        )
        if market_cap is None:
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=numerator.value / market_cap,
            as_of=numerator.as_of,
        )


__all__ = ["OwnerEarningsYieldEquityMetric", "OwnerEarningsYieldEquityFiveYearMetric"]
