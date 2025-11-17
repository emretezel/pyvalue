# Author: Emre Tezel
"""Earnings per share streak metric."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pyvalue.metrics.base import Metric, MetricResult
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

        unique: Dict[str, FactRecord] = {}
        for record in records:
            if not self._is_valid_frame(record.frame):
                continue
            if record.end_date not in unique:
                unique[record.end_date] = record

        streak = 0
        latest_as_of = records[0].end_date
        for end_date in sorted(unique.keys(), reverse=True):
            record = unique[end_date]
            if record.value is None or record.value <= 0:
                break
            streak += 1
        return MetricResult(symbol=symbol, metric_id=self.id, value=float(streak), as_of=latest_as_of)

    def _is_valid_frame(self, frame: Optional[str]) -> bool:
        if not frame:
            return False
        if not frame.startswith("CY"):
            return False
        if len(frame) != 6:
            return False
        year = frame[2:]
        if not year.isdigit():
            return False
        # Filter out quarter frames like CY2023Q1
        if frame.endswith(("Q1", "Q2", "Q3", "Q4")):
            return False
        return True
