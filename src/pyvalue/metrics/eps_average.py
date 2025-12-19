"""Six-year average EPS metric using fiscal year data.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import MAX_FY_FACT_AGE_DAYS, filter_unique_fy, has_recent_fact
from pyvalue.storage import FinancialFactsRepository

EPS_CONCEPTS = ["EarningsPerShare"]

LOGGER = logging.getLogger(__name__)


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
            LOGGER.warning("eps_6y_avg: need >=6 FY EPS records for %s, found %s", symbol, len(history))
            return None
        if not has_recent_fact(repo, symbol, EPS_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning("eps_6y_avg: no recent FY EPS fact for %s", symbol)
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
