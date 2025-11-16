# Author: Emre Tezel
"""Working capital metric implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.storage import FinancialFactsRepository


@dataclass
class WorkingCapitalMetric:
    id: str = "working_capital"
    required_concepts = ("AssetsCurrent", "LiabilitiesCurrent")

    def compute(self, symbol: str, repo: FinancialFactsRepository) -> Optional[MetricResult]:
        assets = repo.latest_fact(symbol, "AssetsCurrent")
        liabilities = repo.latest_fact(symbol, "LiabilitiesCurrent")
        if assets is None or liabilities is None:
            return None
        as_of = max(assets.end_date, liabilities.end_date)
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=assets.value - liabilities.value,
            as_of=as_of,
        )
