"""Market capitalization metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Optional

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import normalize_market_cap_amount
from pyvalue.storage import FinancialFactsRepository, MarketDataRepository

LOGGER = logging.getLogger(__name__)


@dataclass
class MarketCapitalizationMetric:
    id: str = "market_cap"
    required_concepts: tuple[str, ...] = ()
    uses_market_data = True
    uses_financial_facts = False

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        snapshot = market_repo.latest_snapshot(symbol)
        if snapshot is None or snapshot.market_cap is None or snapshot.as_of is None:
            LOGGER.warning("market_cap: missing market cap snapshot for %s", symbol)
            return None
        if snapshot.market_cap <= 0:
            LOGGER.warning("market_cap: non-positive market cap for %s", symbol)
            return None
        value, currency = normalize_market_cap_amount(
            snapshot.market_cap,
            metric_id=self.id,
            symbol=symbol,
            as_of=snapshot.as_of,
            contexts=(market_repo, repo),
        )
        return MetricResult.monetary(
            symbol=symbol,
            metric_id=self.id,
            value=value,
            as_of=snapshot.as_of,
            currency=currency,
        )


__all__ = ["MarketCapitalizationMetric"]
