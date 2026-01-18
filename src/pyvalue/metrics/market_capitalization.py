"""Market capitalization metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyvalue.metrics.base import MetricResult
from pyvalue.storage import FinancialFactsRepository, MarketDataRepository


@dataclass
class MarketCapitalizationMetric:
    id: str = "market_cap"
    required_concepts: tuple[str, ...] = ()
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        snapshot = market_repo.latest_snapshot(symbol)
        if snapshot is None or snapshot.market_cap is None or snapshot.as_of is None:
            return None
        if snapshot.market_cap <= 0:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.market_cap,
            as_of=snapshot.as_of,
        )


__all__ = ["MarketCapitalizationMetric"]
