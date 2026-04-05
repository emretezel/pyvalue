"""Short-term debt share metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.fx import FXService
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import is_recent_fact
from pyvalue.money import align_money_values, fx_service_for_context
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
        fx_service = fx_service_for_context(repo)
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
            symbol=symbol,
            repo=repo,
            short_debt=short_debt,
            fx_service=fx_service,
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

        aligned, _ = align_money_values(
            values=[
                (
                    short_debt.value,
                    short_debt.currency,
                    short_debt.as_of,
                    "ShortTermDebt",
                ),
                (total_debt.value, total_debt.currency, total_debt.as_of, "TotalDebt"),
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation="metric:short_term_debt_share",
            symbol=symbol,
            target_currency=short_debt.currency or total_debt.currency,
        )
        if aligned is None:
            LOGGER.warning("short_term_debt_share: currency mismatch for %s", symbol)
            return None

        ratio = aligned[0] / aligned[1]
        if ratio < 0 or ratio > 1:
            LOGGER.warning("short_term_debt_share: ratio out of bounds for %s", symbol)
            return None

        as_of = max(short_debt.as_of, total_debt.as_of)
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=ratio,
            as_of=as_of,
            unit_kind="percent",
        )

    def _compute_total_debt(
        self,
        *,
        symbol: str,
        repo: FinancialFactsRepository,
        short_debt: _DebtAmount,
        fx_service: FXService,
    ) -> Optional[_DebtAmount]:
        long_record = self._latest_recent_fact(repo, symbol, "LongTermDebt")
        if long_record is not None:
            long_value, long_currency = self._normalize_currency(long_record)
            aligned, currency = align_money_values(
                values=[
                    (
                        short_debt.value,
                        short_debt.currency,
                        short_debt.as_of,
                        "ShortTermDebt",
                    ),
                    (long_value, long_currency, long_record.end_date, "LongTermDebt"),
                ],
                fx_service=fx_service,
                logger=LOGGER,
                operation="metric:short_term_debt_share:debt_components",
                symbol=symbol,
                target_currency=short_debt.currency or long_currency,
            )
            if aligned is not None and currency is not None:
                return _DebtAmount(
                    value=aligned[0] + aligned[1],
                    as_of=max(short_debt.as_of, long_record.end_date),
                    currency=currency,
                )

        total_record = self._latest_recent_fact(
            repo, symbol, "TotalDebtFromBalanceSheet"
        )
        if total_record is None:
            return None
        total_value, total_currency = self._normalize_currency(total_record)
        return _DebtAmount(
            value=total_value,
            as_of=total_record.end_date,
            currency=total_currency,
        )

    def _latest_recent_fact(
        self, repo: FinancialFactsRepository, symbol: str, concept: str
    ) -> Optional[FactRecord]:
        record = repo.latest_fact(symbol, concept)
        if record is None or not is_recent_fact(record):
            return None
        return record

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        value = record.value
        code = record.currency
        if code in {"GBX", "GBP0.01"}:
            return value / 100.0, "GBP"
        return value, code


__all__ = ["ShortTermDebtShareMetric"]
