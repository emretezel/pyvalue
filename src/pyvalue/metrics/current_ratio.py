# Author: Emre Tezel
"""Current ratio metric implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.storage import FinancialFactsRepository


@dataclass
class CurrentRatioMetric:
    id: str = "current_ratio"
    required_concepts = ("AssetsCurrent", "LiabilitiesCurrent")

    def compute(self, symbol: str, repo: FinancialFactsRepository) -> Optional[MetricResult]:
        assets = repo.latest_fact(symbol, "AssetsCurrent")
        liabilities = repo.latest_fact(symbol, "LiabilitiesCurrent")
        if assets is None or liabilities is None:
            return None
        if liabilities.value is None or liabilities.value == 0:
            return None
        ratio = assets.value / liabilities.value
        as_of = max(assets.end_date, liabilities.end_date)
        return MetricResult(symbol=symbol, metric_id=self.id, value=ratio, as_of=as_of)
