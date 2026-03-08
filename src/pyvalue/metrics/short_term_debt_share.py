"""Short-term debt share metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import is_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

DEBT_COMPONENT_CONCEPTS = ("ShortTermDebt", "LongTermDebt")
TOTAL_DEBT_FALLBACK_CONCEPTS = ("TotalDebtFromBalanceSheet",)


@dataclass
class _DebtAmount:
    value: float
    as_of: str
    currency: Optional[str]


@dataclass
class ShortTermDebtShareMetric:
    """Compute short-term debt as a share of total debt (EODHD-only)."""

    id: str = "short_term_debt_share"
    required_concepts = tuple(DEBT_COMPONENT_CONCEPTS + TOTAL_DEBT_FALLBACK_CONCEPTS)

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        short_record = self._latest_recent_fact(repo, symbol, "ShortTermDebt")
        if short_record is None:
            LOGGER.warning(
                "short_term_debt_share: missing short-term debt for %s", symbol
            )
            return None

        short_value, short_currency = self._normalize_currency(short_record)
        short_debt = _DebtAmount(
            value=short_value,
            as_of=short_record.end_date,
            currency=short_currency,
        )

        total_debt = self._compute_total_debt(
            symbol=symbol, repo=repo, short_debt=short_debt
        )
        if total_debt is None:
            LOGGER.warning(
                "short_term_debt_share: missing usable total debt for %s", symbol
            )
            return None

        if total_debt.value <= 0:
            LOGGER.warning(
                "short_term_debt_share: non-positive total debt for %s", symbol
            )
            return None

        ratio = short_debt.value / total_debt.value
        if ratio < 0 or ratio > 1:
            LOGGER.warning("short_term_debt_share: ratio out of bounds for %s", symbol)
            return None

        as_of = max(short_debt.as_of, total_debt.as_of)
        return MetricResult(symbol=symbol, metric_id=self.id, value=ratio, as_of=as_of)

    def _compute_total_debt(
        self,
        *,
        symbol: str,
        repo: FinancialFactsRepository,
        short_debt: _DebtAmount,
    ) -> Optional[_DebtAmount]:
        long_record = self._latest_recent_fact(repo, symbol, "LongTermDebt")
        if long_record is not None:
            long_value, long_currency = self._normalize_currency(long_record)
            if self._currencies_match(short_debt.currency, long_currency):
                return _DebtAmount(
                    value=short_debt.value + long_value,
                    as_of=max(short_debt.as_of, long_record.end_date),
                    currency=short_debt.currency or long_currency,
                )

        total_record = self._latest_recent_fact(
            repo, symbol, "TotalDebtFromBalanceSheet"
        )
        if total_record is None:
            return None
        total_value, total_currency = self._normalize_currency(total_record)
        if not self._currencies_match(short_debt.currency, total_currency):
            return None
        return _DebtAmount(
            value=total_value,
            as_of=total_record.end_date,
            currency=short_debt.currency or total_currency,
        )

    def _latest_recent_fact(
        self, repo: FinancialFactsRepository, symbol: str, concept: str
    ) -> Optional[FactRecord]:
        record = repo.latest_fact(symbol, concept)
        if record is None:
            return None
        if not is_recent_fact(record):
            return None
        return record

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        value = record.value
        code = record.currency
        if code in {"GBX", "GBP0.01"}:
            return value / 100.0, "GBP"
        return value, code

    def _currencies_match(self, left: Optional[str], right: Optional[str]) -> bool:
        if left and right:
            return left == right
        return True


__all__ = ["ShortTermDebtShareMetric"]
