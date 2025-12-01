"""Earnings per share streak metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import logging

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import MAX_FY_FACT_AGE_DAYS, filter_unique_fy, has_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository

FALLBACK_CONCEPTS = [
    "EarningsPerShareDiluted",
    "EarningsPerShareBasicAndDiluted",
    "EarningsPerShareBasic",
]

LOGGER = logging.getLogger(__name__)


@dataclass
class EPSStreakMetric:
    id: str = "eps_streak"
    required_concepts = tuple(FALLBACK_CONCEPTS)

    def compute(self, symbol: str, repo: FinancialFactsRepository) -> Optional[MetricResult]:
        records: List[FactRecord] = []
        for concept in FALLBACK_CONCEPTS:
            records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
            if records:
                break
        if not records:
            LOGGER.warning("eps_streak: no FY EPS records for %s", symbol)
            return None
        if not has_recent_fact(repo, symbol, FALLBACK_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning("eps_streak: no recent FY EPS fact for %s", symbol)
            return None

        unique = filter_unique_fy(records)

        streak = 0
        latest_as_of = records[0].end_date
        for end_date in sorted(unique.keys(), reverse=True):
            record = unique[end_date]
            if record.value is None or record.value <= 0:
                break
            streak += 1
        return MetricResult(symbol=symbol, metric_id=self.id, value=float(streak), as_of=latest_as_of)
