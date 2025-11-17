"""Long-term debt metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.storage import FinancialFactsRepository


@dataclass
class LongTermDebtMetric:
    id: str = "long_term_debt"
    required_concepts = ("LongTermDebtNoncurrent", "LongTermDebt")

    def compute(self, symbol: str, repo: FinancialFactsRepository) -> Optional[MetricResult]:
        for concept in self.required_concepts:
            fact = repo.latest_fact(symbol, concept)
            if fact is not None:
                return MetricResult(
                    symbol=symbol,
                    metric_id=self.id,
                    value=fact.value,
                    as_of=fact.end_date,
                )
        return None
