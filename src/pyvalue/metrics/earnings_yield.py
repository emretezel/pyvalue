"""Earnings yield metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import latest_quarterly_records
from pyvalue.storage import FinancialFactsRepository, MarketDataRepository

EPS_CONCEPTS = ["EarningsPerShareDiluted", "EarningsPerShareBasic"]


@dataclass
class EarningsYieldMetric:
    id: str = "earnings_yield"
    required_concepts = tuple(EPS_CONCEPTS)
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        quarterly_records = self._latest_quarters(symbol, repo)
        if len(quarterly_records) < 4:
            return None
        ttm_eps = sum(record.value for record in quarterly_records[:4])
        as_of = quarterly_records[0].end_date
        price_record = market_repo.latest_price(symbol)
        if price_record is None:
            return None
        _, price = price_record
        if price is None or price <= 0:
            return None
        yield_value = ttm_eps / price
        return MetricResult(symbol=symbol, metric_id=self.id, value=yield_value, as_of=as_of)

    def _latest_quarters(self, symbol: str, repo: FinancialFactsRepository) -> List:
        return latest_quarterly_records(repo.facts_for_concept, symbol, EPS_CONCEPTS, periods=4)
