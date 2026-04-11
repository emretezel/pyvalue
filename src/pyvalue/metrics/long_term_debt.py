"""Long-term debt metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import is_recent_fact, normalize_metric_record
from pyvalue.storage import FinancialFactsRepository

LOGGER = logging.getLogger(__name__)


@dataclass
class LongTermDebtMetric:
    id: str = "long_term_debt"
    required_concepts = ("LongTermDebt",)

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        fact = repo.latest_fact(symbol, "LongTermDebt")
        if fact is not None and is_recent_fact(fact):
            value, currency = normalize_metric_record(
                fact,
                metric_id=self.id,
                symbol=symbol,
                contexts=(repo,),
            )
            return MetricResult.monetary(
                symbol=symbol,
                metric_id=self.id,
                value=value,
                as_of=fact.end_date,
                currency=currency,
            )
        LOGGER.warning("long_term_debt: no recent long-term debt fact for %s", symbol)
        return None
