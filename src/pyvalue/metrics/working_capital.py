"""Working capital metric implementation.

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
class WorkingCapitalMetric:
    id: str = "working_capital"
    required_concepts = ("AssetsCurrent", "LiabilitiesCurrent")

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        assets = repo.latest_fact(symbol, "AssetsCurrent")
        liabilities = repo.latest_fact(symbol, "LiabilitiesCurrent")
        if assets is None or liabilities is None:
            LOGGER.warning("working_capital: missing assets/liabilities for %s", symbol)
            return None
        as_of_record = (
            assets if assets.end_date >= liabilities.end_date else liabilities
        )
        if not is_recent_fact(as_of_record):
            LOGGER.warning(
                "working_capital: latest assets/liabilities too old for %s (%s)",
                symbol,
                as_of_record.end_date,
            )
            return None
        if assets.currency is None or liabilities.currency is None:
            LOGGER.warning(
                "working_capital: missing currency for %s (assets=%s liabilities=%s)",
                symbol,
                assets.currency,
                liabilities.currency,
            )
            return None
        as_of = as_of_record.end_date
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
                    "working_capital: FX conversion failed for %s (%s -> %s)",
                    symbol,
                    liabilities.currency,
                    assets.currency,
                )
                return None
            liabilities_value = float(converted)
        return MetricResult.monetary(
            symbol=symbol,
            metric_id=self.id,
            value=assets.value - liabilities_value,
            as_of=as_of,
            currency=assets.currency,
        )
