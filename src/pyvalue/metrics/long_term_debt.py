"""Long-term debt metric.

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
class LongTermDebtMetric:
    id: str = "long_term_debt"
    required_concepts = ("LongTermDebt",)

    def compute(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        fact = repo.latest_monetary_fact(symbol, "LongTermDebt")
        if fact is not None and is_recent_fact(fact):
            target_currency = require_metric_ticker_currency(
                symbol,
                repo,
                metric_id=self.id,
                input_name="LongTermDebt",
                as_of=fact.end_date,
            )
            money = require_metric_money(
                fact.money,
                target_currency=target_currency,
                metric_id=self.id,
                symbol=symbol,
                input_name="LongTermDebt",
                as_of=fact.end_date,
            )
            return MetricResult.monetary(
                symbol=symbol,
                metric_id=self.id,
                value=money.amount,
                as_of=fact.end_date,
                currency=money.currency,
            )
        LOGGER.warning("long_term_debt: no recent long-term debt fact for %s", symbol)
        return None
