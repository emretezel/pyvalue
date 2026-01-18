"""Short-term debt share metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import is_recent_fact
from pyvalue.storage import FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

DEBT_CONCEPTS = ("ShortTermDebt", "LongTermDebt")


@dataclass
class ShortTermDebtShareMetric:
    """Compute short-term debt as a share of total debt (EODHD-only)."""

    id: str = "short_term_debt_share"
    required_concepts = tuple(DEBT_CONCEPTS)

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        short_debt = repo.latest_fact(symbol, "ShortTermDebt")
        long_debt = repo.latest_fact(symbol, "LongTermDebt")
        if short_debt is None or long_debt is None:
            LOGGER.warning("short_term_debt_share: missing debt inputs for %s", symbol)
            return None
        if not is_recent_fact(short_debt) or not is_recent_fact(long_debt):
            LOGGER.warning("short_term_debt_share: debt facts too old for %s", symbol)
            return None

        total_debt = short_debt.value + long_debt.value
        if total_debt <= 0:
            LOGGER.warning(
                "short_term_debt_share: non-positive total debt for %s", symbol
            )
            return None
        ratio = short_debt.value / total_debt
        as_of = max(short_debt.end_date, long_debt.end_date)
        return MetricResult(symbol=symbol, metric_id=self.id, value=ratio, as_of=as_of)


__all__ = ["ShortTermDebtShareMetric"]
