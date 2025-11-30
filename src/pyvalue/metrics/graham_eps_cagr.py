"""Graham EPS 10y CAGR% (3y average) metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import MAX_FY_FACT_AGE_DAYS, filter_unique_fy, has_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository

EPS_CONCEPTS = ["EarningsPerShareDiluted", "EarningsPerShareBasic"]


def _sorted_records(records: Dict[str, FactRecord]) -> List[FactRecord]:
    return [records[end_date] for end_date in sorted(records.keys())]


@dataclass
class GrahamEPSCAGRMetric:
    id: str = "graham_eps_10y_cagr_3y_avg"
    required_concepts = tuple(EPS_CONCEPTS)

    def compute(self, symbol: str, repo: FinancialFactsRepository) -> Optional[MetricResult]:
        records: List[FactRecord] = []
        for concept in EPS_CONCEPTS:
            records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
            if records:
                break
        if len(records) < 12:
            return None
        if not has_recent_fact(repo, symbol, EPS_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS):
            return None
        latest_date = records[0].end_date
        filtered = filter_unique_fy(records)
        ordered = sorted(filtered.values(), key=lambda r: r.end_date)
        if len(ordered) < 12:
            return None
        cagr_values = self._compute_cagrs(ordered)
        if not cagr_values:
            return None
        avg_cagr = sum(cagr_values) / len(cagr_values)
        return MetricResult(symbol=symbol, metric_id=self.id, value=avg_cagr, as_of=latest_date)

    def _compute_cagrs(self, ordered: List[FactRecord]) -> List[float]:
        cagr_values: List[float] = []
        eps_history = ordered
        for idx in range(len(eps_history) - 1, 9, -1):
            current = eps_history[idx]
            past = eps_history[idx - 10]
            if past.value is None or past.value <= 0:
                continue
            if current.value is None or current.value <= 0:
                continue
            years = 10
            cagr = (current.value / past.value) ** (1 / years) - 1
            cagr_values.append(cagr)
            if len(cagr_values) == 3:
                break
        return cagr_values
