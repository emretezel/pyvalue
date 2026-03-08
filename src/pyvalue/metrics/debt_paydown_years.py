"""Debt paydown and FCF-to-debt metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import is_recent_fact
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
        debt = self._compute_total_debt(symbol, repo, metric_id=metric_id)
        if debt is None:
            LOGGER.warning("%s: missing debt inputs for %s", metric_id, symbol)
            return None
        if debt.total <= 0:
            LOGGER.warning("%s: non-positive debt for %s", metric_id, symbol)
            return None

        fcf = self._compute_ttm_fcf(symbol, repo, metric_id=metric_id)
        if fcf is None:
            LOGGER.warning("%s: missing TTM FCF for %s", metric_id, symbol)
            return None
        if fcf.total <= 0:
            LOGGER.warning("%s: non-positive FCF for %s", metric_id, symbol)
            return None

        if not self._currencies_match(debt.currency, fcf.currency):
            LOGGER.warning("%s: currency mismatch for %s", metric_id, symbol)
            return None

        return _FCFDebtInputs(fcf=fcf, debt=debt, as_of=max(debt.as_of, fcf.as_of))

    def _compute_total_debt(
        self, symbol: str, repo: FinancialFactsRepository, *, metric_id: str
    ) -> Optional[_DebtResult]:
        short_debt = self._latest_recent_fact(repo, symbol, "ShortTermDebt")
        long_debt = self._latest_recent_fact(repo, symbol, "LongTermDebt")
        total_debt = self._latest_recent_fact(repo, symbol, "TotalDebtFromBalanceSheet")

        if short_debt is not None and long_debt is not None:
            short_value, short_currency = self._normalize_currency(short_debt)
            long_value, long_currency = self._normalize_currency(long_debt)
            currency = self._merge_currency([short_currency, long_currency])
            if currency is None and any(
                code is not None for code in (short_currency, long_currency)
            ):
                LOGGER.warning("%s: debt currency conflict for %s", metric_id, symbol)
                return None
            return _DebtResult(
                total=short_value + long_value,
                as_of=max(short_debt.end_date, long_debt.end_date),
                currency=currency,
            )

        if total_debt is not None:
            value, currency = self._normalize_currency(total_debt)
            return _DebtResult(
                total=value,
                as_of=total_debt.end_date,
                currency=currency,
            )

        one_side = short_debt or long_debt
        if one_side is None:
            return None
        value, currency = self._normalize_currency(one_side)
        return _DebtResult(total=value, as_of=one_side.end_date, currency=currency)

    def _compute_ttm_fcf(
        self, symbol: str, repo: FinancialFactsRepository, *, metric_id: str
    ) -> Optional[_TTMResult]:
        operating = self._ttm_sum(
            symbol, repo, OPERATING_CASH_FLOW_CONCEPTS, metric_id=metric_id
        )
        if operating is None:
            return None

        capex = self._ttm_sum(symbol, repo, CAPEX_CONCEPTS, metric_id=metric_id)
        if capex is None:
            LOGGER.warning(
                "%s: missing/stale capex for %s; assuming zero", metric_id, symbol
            )
            capex_total = 0.0
            capex_as_of = operating.as_of
            capex_currency = None
        else:
            capex_total = capex.total
            capex_as_of = capex.as_of
            capex_currency = capex.currency

        if (
            capex_currency
            and operating.currency
            and capex_currency != operating.currency
        ):
            LOGGER.warning(
                "%s: currency mismatch between OCF and capex for %s", metric_id, symbol
            )
            return None

        return _TTMResult(
            total=operating.total - capex_total,
            as_of=max(operating.as_of, capex_as_of),
            currency=operating.currency or capex_currency,
        )

    def _ttm_sum(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        metric_id: str,
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

            normalized_values, currency = self._normalize_quarterly_values(
                quarterly[:4]
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

    def _normalize_quarterly_values(
        self, records: Sequence[FactRecord]
    ) -> tuple[Optional[list[float]], Optional[str]]:
        currency = None
        normalized: list[float] = []
        for record in records:
            value, code = self._normalize_currency(record)
            if currency is None and code:
                currency = code
            elif code and currency and code != currency:
                return None, None
            normalized.append(value)
        return normalized, currency

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

    def _merge_currency(self, codes: Sequence[Optional[str]]) -> Optional[str]:
        currency = None
        for code in codes:
            if not code:
                continue
            if currency is None:
                currency = code
            elif code != currency:
                return None
        return currency

    def _currencies_match(self, left: Optional[str], right: Optional[str]) -> bool:
        if left and right:
            return left == right
        return True


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
        )


__all__ = ["DebtPaydownYearsMetric", "FCFToDebtMetric"]
