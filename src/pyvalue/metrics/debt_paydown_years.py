"""Debt paydown and FCF-to-debt metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.fx import FXService
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import is_recent_fact
from pyvalue.money import (
    align_money_values,
    fx_service_for_context,
    normalize_money_value,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

OPERATING_CASH_FLOW_CONCEPTS = ("NetCashProvidedByUsedInOperatingActivities",)
CAPEX_CONCEPTS = ("CapitalExpenditures",)
DEBT_COMPONENT_CONCEPTS = ("ShortTermDebt", "LongTermDebt")
TOTAL_DEBT_FALLBACK_CONCEPTS = ("TotalDebtFromBalanceSheet",)
REQUIRED_CONCEPTS = tuple(
    OPERATING_CASH_FLOW_CONCEPTS
    + CAPEX_CONCEPTS
    + DEBT_COMPONENT_CONCEPTS
    + TOTAL_DEBT_FALLBACK_CONCEPTS
)
QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}


@dataclass
class _TTMResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class _DebtResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class _FCFDebtInputs:
    fcf: _TTMResult
    debt: _DebtResult
    as_of: str


class _FCFDebtCalculator:
    def compute_inputs(
        self, symbol: str, repo: FinancialFactsRepository, *, metric_id: str
    ) -> Optional[_FCFDebtInputs]:
        fx_service = fx_service_for_context(repo)
        debt = self._compute_total_debt(
            symbol,
            repo,
            metric_id=metric_id,
            fx_service=fx_service,
        )
        if debt is None:
            LOGGER.warning("%s: missing debt inputs for %s", metric_id, symbol)
            return None
        if debt.total <= 0:
            LOGGER.warning("%s: non-positive debt for %s", metric_id, symbol)
            return None

        fcf = self._compute_ttm_fcf(
            symbol,
            repo,
            metric_id=metric_id,
            fx_service=fx_service,
        )
        if fcf is None:
            LOGGER.warning("%s: missing TTM FCF for %s", metric_id, symbol)
            return None
        if fcf.total <= 0:
            LOGGER.warning("%s: non-positive FCF for %s", metric_id, symbol)
            return None

        aligned_pair, target_currency = align_money_values(
            values=[
                (debt.total, debt.currency, debt.as_of, "TotalDebt"),
                (fcf.total, fcf.currency, fcf.as_of, "FreeCashFlow"),
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation=f"metric:{metric_id}:inputs",
            symbol=symbol,
            target_currency=debt.currency or fcf.currency,
        )
        if aligned_pair is None or target_currency is None:
            LOGGER.warning("%s: currency mismatch for %s", metric_id, symbol)
            return None

        return _FCFDebtInputs(
            fcf=_TTMResult(
                total=aligned_pair[1], as_of=fcf.as_of, currency=target_currency
            ),
            debt=_DebtResult(
                total=aligned_pair[0], as_of=debt.as_of, currency=target_currency
            ),
            as_of=max(debt.as_of, fcf.as_of),
        )

    def _compute_total_debt(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        metric_id: str,
        fx_service: FXService,
    ) -> Optional[_DebtResult]:
        short_debt = self._latest_recent_fact(repo, symbol, "ShortTermDebt")
        long_debt = self._latest_recent_fact(repo, symbol, "LongTermDebt")
        total_debt = self._latest_recent_fact(repo, symbol, "TotalDebtFromBalanceSheet")

        if short_debt is not None and long_debt is not None:
            short_value, short_currency = self._normalize_currency(short_debt)
            long_value, long_currency = self._normalize_currency(long_debt)
            aligned, currency = align_money_values(
                values=[
                    (short_value, short_currency, short_debt.end_date, "ShortTermDebt"),
                    (long_value, long_currency, long_debt.end_date, "LongTermDebt"),
                ],
                fx_service=fx_service,
                logger=LOGGER,
                operation=f"metric:{metric_id}:debt",
                symbol=symbol,
                target_currency=short_currency or long_currency,
            )
            if aligned is None or currency is None:
                LOGGER.warning("%s: debt currency conflict for %s", metric_id, symbol)
                return None
            return _DebtResult(
                total=aligned[0] + aligned[1],
                as_of=max(short_debt.end_date, long_debt.end_date),
                currency=currency,
            )

        if total_debt is not None:
            value, currency = self._normalize_currency(total_debt)
            return _DebtResult(
                total=value, as_of=total_debt.end_date, currency=currency
            )

        one_side = short_debt or long_debt
        if one_side is None:
            return None
        value, currency = self._normalize_currency(one_side)
        return _DebtResult(total=value, as_of=one_side.end_date, currency=currency)

    def _compute_ttm_fcf(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        *,
        metric_id: str,
        fx_service: FXService,
    ) -> Optional[_TTMResult]:
        operating = self._ttm_sum(
            symbol,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            metric_id=metric_id,
            fx_service=fx_service,
        )
        if operating is None:
            return None

        capex = self._ttm_sum(
            symbol,
            repo,
            CAPEX_CONCEPTS,
            metric_id=metric_id,
            fx_service=fx_service,
        )
        if capex is None:
            LOGGER.warning(
                "%s: missing/stale capex for %s; assuming zero", metric_id, symbol
            )
            return _TTMResult(
                total=operating.total,
                as_of=operating.as_of,
                currency=operating.currency,
            )

        aligned, currency = align_money_values(
            values=[
                (
                    operating.total,
                    operating.currency,
                    operating.as_of,
                    OPERATING_CASH_FLOW_CONCEPTS[0],
                ),
                (capex.total, capex.currency, capex.as_of, CAPEX_CONCEPTS[0]),
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation=f"metric:{metric_id}:fcf",
            symbol=symbol,
            target_currency=operating.currency or capex.currency,
        )
        if aligned is None or currency is None:
            LOGGER.warning(
                "%s: currency mismatch between OCF and capex for %s", metric_id, symbol
            )
            return None
        return _TTMResult(
            total=aligned[0] - aligned[1],
            as_of=max(operating.as_of, capex.as_of),
            currency=currency,
        )

    def _ttm_sum(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        metric_id: str,
        fx_service: FXService,
    ) -> Optional[_TTMResult]:
        for concept in concepts:
            records = repo.facts_for_concept(symbol, concept)
            quarterly = self._filter_quarterly(records)
            if len(quarterly) < 4:
                LOGGER.warning(
                    "%s: need 4 quarterly %s records for %s, found %s",
                    metric_id,
                    concept,
                    symbol,
                    len(quarterly),
                )
                continue
            if not is_recent_fact(quarterly[0]):
                LOGGER.warning(
                    "%s: latest %s (%s) too old for %s",
                    metric_id,
                    concept,
                    quarterly[0].end_date,
                    symbol,
                )
                continue

            normalized_values, currency = align_money_values(
                values=[
                    (record.value, record.currency, record.end_date, concept)
                    for record in quarterly[:4]
                    if record.value is not None
                ],
                fx_service=fx_service,
                logger=LOGGER,
                operation=f"metric:{metric_id}:{concept}",
                symbol=symbol,
                target_currency=quarterly[0].currency,
            )
            if normalized_values is None:
                LOGGER.warning(
                    "%s: currency conflict in %s quarterly values for %s",
                    metric_id,
                    concept,
                    symbol,
                )
                continue
            return _TTMResult(
                total=sum(normalized_values),
                as_of=quarterly[0].end_date,
                currency=currency,
            )
        return None

    def _filter_quarterly(self, records: Sequence[FactRecord]) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in QUARTERLY_PERIODS:
                continue
            if record.end_date in seen_end_dates:
                continue
            if record.value is None:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        filtered.sort(key=lambda record: record.end_date, reverse=True)
        return filtered

    def _latest_recent_fact(
        self, repo: FinancialFactsRepository, symbol: str, concept: str
    ) -> Optional[FactRecord]:
        record = repo.latest_fact(symbol, concept)
        if record is None or not is_recent_fact(record):
            return None
        return record

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        normalized_value, normalized_currency = normalize_money_value(
            record.value,
            record.currency,
        )
        return (
            record.value if normalized_value is None else normalized_value,
            normalized_currency,
        )


@dataclass
class DebtPaydownYearsMetric:
    """Compute total debt divided by TTM free cash flow (EODHD-only)."""

    id: str = "debt_paydown_years"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        inputs = _FCFDebtCalculator().compute_inputs(symbol, repo, metric_id=self.id)
        if inputs is None:
            return None
        ratio = inputs.debt.total / inputs.fcf.total
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=ratio,
            as_of=inputs.as_of,
            unit_kind="multiple",
        )


@dataclass
class FCFToDebtMetric:
    """Compute TTM free cash flow divided by total debt (EODHD-only)."""

    id: str = "fcf_to_debt"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        inputs = _FCFDebtCalculator().compute_inputs(symbol, repo, metric_id=self.id)
        if inputs is None:
            return None
        ratio = inputs.fcf.total / inputs.debt.total
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=ratio,
            as_of=inputs.as_of,
            unit_kind="ratio",
        )


__all__ = ["DebtPaydownYearsMetric", "FCFToDebtMetric"]
