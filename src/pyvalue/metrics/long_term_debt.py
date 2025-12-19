"""Long-term debt metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import is_recent_fact, resolve_long_term_debt
from pyvalue.storage import FinancialFactsRepository

LOGGER = logging.getLogger(__name__)


@dataclass
class LongTermDebtMetric:
    id: str = "long_term_debt"
    required_concepts = ("LongTermDebt",)

    def compute(self, symbol: str, repo: FinancialFactsRepository) -> Optional[MetricResult]:
        fact = resolve_long_term_debt(repo, symbol)
        if fact is not None and is_recent_fact(fact):
            return MetricResult(
                symbol=symbol,
                metric_id=self.id,
                value=fact.value,
                as_of=fact.end_date,
            )
        LOGGER.warning("long_term_debt: no recent long-term debt fact for %s", symbol)
        return None
