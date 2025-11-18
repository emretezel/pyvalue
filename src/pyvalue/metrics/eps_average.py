"""Six-year average EPS metric using fiscal year data.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import filter_unique_fy
from pyvalue.storage import FinancialFactsRepository

EPS_CONCEPTS = ["EarningsPerShareDiluted", "EarningsPerShareBasic"]


@dataclass
class EPSAverageSixYearMetric:
    id: str = "eps_6y_avg"
    required_concepts = tuple(EPS_CONCEPTS)
    uses_market_data = False

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
    ) -> Optional[MetricResult]:
        history = self._fetch_history(symbol, repo)
        if len(history) < 6:
            return None
        latest_records = history[:6]
        avg = sum(record.value for record in latest_records) / 6
        as_of = latest_records[0].end_date
        return MetricResult(symbol=symbol, metric_id=self.id, value=avg, as_of=as_of)

    def _fetch_history(self, symbol: str, repo: FinancialFactsRepository):
        for concept in EPS_CONCEPTS:
            records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
            unique = filter_unique_fy(records)
            if unique:
                ordered = sorted(unique.values(), key=lambda rec: rec.end_date, reverse=True)
                if len(ordered) >= 6:
                    return ordered
        return []


__all__ = ["EPSAverageSixYearMetric"]
