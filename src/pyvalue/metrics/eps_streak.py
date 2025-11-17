# Author: Emre Tezel
"""Earnings per share streak metric."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import filter_unique_fy
from pyvalue.storage import FactRecord, FinancialFactsRepository

FALLBACK_CONCEPTS = [
    "EarningsPerShareDiluted",
    "EarningsPerShareBasicAndDiluted",
    "EarningsPerShareBasic",
]


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
