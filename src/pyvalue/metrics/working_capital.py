"""Working capital metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class WorkingCapitalMetric:
    id: str = "working_capital"
    required_concepts = ("AssetsCurrent", "LiabilitiesCurrent")

    def compute(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        assets = repo.latest_monetary_fact(symbol, "AssetsCurrent")
        liabilities = repo.latest_monetary_fact(symbol, "LiabilitiesCurrent")
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
        as_of = as_of_record.end_date
        target_currency = require_metric_ticker_currency(
            symbol,
            repo,
            metric_id=self.id,
            as_of=as_of,
        )
        assets_money = require_metric_money(
            assets.money,
            target_currency=target_currency,
            metric_id=self.id,
            symbol=symbol,
            input_name="AssetsCurrent",
            as_of=assets.end_date,
        )
        liabilities_money = require_metric_money(
            liabilities.money,
            target_currency=target_currency,
            metric_id=self.id,
            symbol=symbol,
            input_name="LiabilitiesCurrent",
            as_of=liabilities.end_date,
        )
        working_capital = assets_money - liabilities_money
        return MetricResult.monetary(
            symbol=symbol,
            metric_id=self.id,
            value=working_capital.amount,
            as_of=as_of,
            currency=working_capital.currency,
        )
