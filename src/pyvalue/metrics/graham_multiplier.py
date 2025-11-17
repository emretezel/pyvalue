# Author: Emre Tezel
"""Graham multiplier metric implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import filter_unique_fy
from pyvalue.storage import FactRecord, FinancialFactsRepository, MarketDataRepository


EPS_CONCEPTS = ["EarningsPerShareDiluted", "EarningsPerShareBasic"]
EQUITY_CONCEPTS = [
    "StockholdersEquity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
]
SHARE_CONCEPTS = ["CommonStockSharesOutstanding", "EntityCommonStockSharesOutstanding"]
GOODWILL_CONCEPTS = ["Goodwill"]
INTANGIBLE_CONCEPTS = ["IntangibleAssetsNetExcludingGoodwill", "IntangibleAssetsNet"]


@dataclass
class GrahamMultiplierMetric:
    id: str = "graham_multiplier"
    required_concepts = tuple(EPS_CONCEPTS + EQUITY_CONCEPTS + SHARE_CONCEPTS + GOODWILL_CONCEPTS + INTANGIBLE_CONCEPTS)
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

        equity = self._latest_value(symbol, repo, EQUITY_CONCEPTS)
        shares = self._latest_value(symbol, repo, SHARE_CONCEPTS)
        if equity is None or shares is None or shares <= 0:
            return None

        goodwill = self._latest_value(symbol, repo, GOODWILL_CONCEPTS) or 0.0
        intangibles = self._latest_value(symbol, repo, INTANGIBLE_CONCEPTS) or 0.0

        price_data = market_repo.latest_price(symbol)
        if price_data is None:
            return None
        _, price = price_data
        if price <= 0:
            return None

        tbvps = (equity - goodwill - intangibles) / shares
        if tbvps <= 0:
            return None

        multiplier = (price / eps_fact.value) * (price / tbvps)
        return MetricResult(symbol=symbol, metric_id=self.id, value=multiplier, as_of=eps_fact.end_date)

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
        latest_date = max(unique.keys())
        return unique[latest_date]

    def _latest_value(self, symbol: str, repo: FinancialFactsRepository, concepts: list[str]) -> Optional[float]:
        for concept in concepts:
            fact = repo.latest_fact(symbol, concept)
            if fact is not None and fact.value is not None:
                return fact.value
        return None
