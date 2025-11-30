"""Graham multiplier metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import is_recent_fact, latest_quarterly_records
from pyvalue.storage import FinancialFactsRepository, MarketDataRepository


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
        eps_records = self._latest_quarters(symbol, repo)
        if len(eps_records) < 4:
            return None
        ttm_eps = sum(record.value for record in eps_records[:4])
        if ttm_eps <= 0:
            return None
        eps_as_of = eps_records[0].end_date

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

        multiplier = (price / ttm_eps) * (price / tbvps)
        return MetricResult(symbol=symbol, metric_id=self.id, value=multiplier, as_of=eps_as_of)

    def _latest_quarters(self, symbol: str, repo: FinancialFactsRepository):
        return latest_quarterly_records(repo.facts_for_concept, symbol, EPS_CONCEPTS, periods=4)

    def _latest_value(self, symbol: str, repo: FinancialFactsRepository, concepts: list[str]) -> Optional[float]:
        for concept in concepts:
            fact = repo.latest_fact(symbol, concept)
            if fact is None or not is_recent_fact(fact):
                continue
            if fact.value is not None:
                try:
                    return float(fact.value)
                except (TypeError, ValueError):
                    continue
        return None
