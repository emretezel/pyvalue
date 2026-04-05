"""Current ratio metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import is_recent_fact
from pyvalue.money import fx_service_for_context
from pyvalue.storage import FinancialFactsRepository

LOGGER = logging.getLogger(__name__)


@dataclass
class CurrentRatioMetric:
    id: str = "current_ratio"
    required_concepts = ("AssetsCurrent", "LiabilitiesCurrent")

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        assets = repo.latest_fact(symbol, "AssetsCurrent")
        liabilities = repo.latest_fact(symbol, "LiabilitiesCurrent")
        if assets is None or liabilities is None:
            LOGGER.warning("current_ratio: missing assets/liabilities for %s", symbol)
            return None
        if liabilities.value is None or liabilities.value == 0:
            LOGGER.warning("current_ratio: liabilities missing/zero for %s", symbol)
            return None
        as_of_record = (
            assets if assets.end_date >= liabilities.end_date else liabilities
        )
        if not is_recent_fact(as_of_record):
            LOGGER.warning(
                "current_ratio: latest assets/liabilities too old for %s (%s)",
                symbol,
                as_of_record.end_date,
            )
            return None
        if assets.currency is None or liabilities.currency is None:
            LOGGER.warning(
                "current_ratio: missing currency for %s (assets=%s liabilities=%s)",
                symbol,
                assets.currency,
                liabilities.currency,
            )
            return None
        liabilities_value = liabilities.value
        if liabilities.currency != assets.currency:
            converted = fx_service_for_context(repo).convert_amount(
                liabilities.value,
                liabilities.currency,
                assets.currency,
                liabilities.end_date,
            )
            if converted is None:
                LOGGER.warning(
                    "current_ratio: FX conversion failed for %s (%s -> %s)",
                    symbol,
                    liabilities.currency,
                    assets.currency,
                )
                return None
            liabilities_value = float(converted)
        ratio = assets.value / liabilities_value
        as_of = as_of_record.end_date
        return MetricResult.ratio(
            symbol=symbol,
            metric_id=self.id,
            value=ratio,
            as_of=as_of,
        )
