"""Graham EPS 10y period CAGR% (3y average) metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    filter_unique_fy,
    has_recent_fact,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository

EPS_CONCEPTS = ["EarningsPerShare"]
WINDOW_YEARS = 10
AVG_WINDOW = 3
CAGR_YEARS = 7
MIN_REQUIRED = WINDOW_YEARS


def _sorted_records(records: Dict[str, FactRecord]) -> List[FactRecord]:
    return [records[end_date] for end_date in sorted(records.keys())]


LOGGER = logging.getLogger(__name__)


@dataclass
class GrahamEPSCAGRMetric:
    id: str = "graham_eps_10y_cagr_3y_avg"
    required_concepts = tuple(EPS_CONCEPTS)

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        records: List[FactRecord] = []
        for concept in EPS_CONCEPTS:
            records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
            if records:
                break
        if len(records) < MIN_REQUIRED:
            LOGGER.warning(
                "graham_eps_cagr: need >=%s FY EPS records for %s, found %s",
                MIN_REQUIRED,
                symbol,
                len(records),
            )
            return None
        if not has_recent_fact(
            repo, symbol, EPS_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("graham_eps_cagr: no recent FY EPS fact for %s", symbol)
            return None
        latest_date = records[0].end_date
        filtered = filter_unique_fy(records)
        ordered = sorted(filtered.values(), key=lambda r: r.end_date)
        if len(ordered) < MIN_REQUIRED:
            LOGGER.warning(
                "graham_eps_cagr: need >=%s unique FY EPS records for %s after filtering, found %s",
                MIN_REQUIRED,
                symbol,
                len(ordered),
            )
            return None
        cagr_value = self._compute_cagr(ordered)
        if cagr_value is None:
            LOGGER.warning(
                "graham_eps_cagr: could not derive CAGR value for %s", symbol
            )
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=cagr_value, as_of=latest_date
        )

    def _compute_cagr(self, ordered: List[FactRecord]) -> Optional[float]:
        eps_history = ordered[-WINDOW_YEARS:]
        start_values = [record.value for record in eps_history[:AVG_WINDOW]]
        end_values = [record.value for record in eps_history[-AVG_WINDOW:]]
        if any(value is None or value <= 0 for value in start_values + end_values):
            return None
        start_avg = sum(start_values) / AVG_WINDOW
        end_avg = sum(end_values) / AVG_WINDOW
        if start_avg <= 0 or end_avg <= 0:
            return None
        return (end_avg / start_avg) ** (1 / CAGR_YEARS) - 1
