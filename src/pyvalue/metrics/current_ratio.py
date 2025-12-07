"""Current ratio metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import is_recent_fact, resolve_assets_current, resolve_liabilities_current
from pyvalue.storage import FinancialFactsRepository

LOGGER = logging.getLogger(__name__)


@dataclass
class CurrentRatioMetric:
    id: str = "current_ratio"
    required_concepts = ("AssetsCurrent", "LiabilitiesCurrent")

    def compute(self, symbol: str, repo: FinancialFactsRepository) -> Optional[MetricResult]:
        assets = resolve_assets_current(repo, symbol)
        liabilities = resolve_liabilities_current(repo, symbol)
        if assets is None or liabilities is None:
            LOGGER.warning("current_ratio: missing assets/liabilities for %s", symbol)
            return None
        if liabilities.value is None or liabilities.value == 0:
            LOGGER.warning("current_ratio: liabilities missing/zero for %s", symbol)
            return None
        as_of_record = assets if assets.end_date >= liabilities.end_date else liabilities
        if not is_recent_fact(as_of_record):
            LOGGER.warning("current_ratio: latest assets/liabilities too old for %s (%s)", symbol, as_of_record.end_date)
            return None
        ratio = assets.value / liabilities.value
        as_of = as_of_record.end_date
        return MetricResult(symbol=symbol, metric_id=self.id, value=ratio, as_of=as_of)
