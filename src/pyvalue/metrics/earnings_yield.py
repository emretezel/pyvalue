"""Earnings yield metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import filter_unique_fy
from pyvalue.storage import FactRecord, FinancialFactsRepository, MarketDataRepository

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
        eps_fact = self._latest_eps(symbol, repo)
        if eps_fact is None or eps_fact.value is None or eps_fact.value <= 0:
            return None
        price_record = market_repo.latest_price(symbol)
        if price_record is None:
            return None
        _, price = price_record
        if price <= 0:
            return None
        yield_value = eps_fact.value / price
        return MetricResult(symbol=symbol, metric_id=self.id, value=yield_value, as_of=eps_fact.end_date)

    def _latest_eps(self, symbol: str, repo: FinancialFactsRepository) -> Optional[FactRecord]:
        records = []
        for concept in EPS_CONCEPTS:
            records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
            if records:
                break
        if not records:
            return None
        unique = filter_unique_fy(records)
        if not unique:
            return None
        latest = max(unique.keys())
        return unique[latest]
