"""Debt paydown years metric implementation.

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
DEBT_CONCEPTS = ("ShortTermDebt", "LongTermDebt")
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
class DebtPaydownYearsMetric:
    """Compute total debt divided by TTM free cash flow (EODHD-only)."""

    id: str = "debt_paydown_years"
    required_concepts = tuple(
        OPERATING_CASH_FLOW_CONCEPTS + CAPEX_CONCEPTS + DEBT_CONCEPTS
    )

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        debt = self._compute_total_debt(symbol, repo)
        if debt is None:
            LOGGER.warning("debt_paydown_years: missing debt inputs for %s", symbol)
            return None

        fcf = self._compute_ttm_fcf(symbol, repo)
        if fcf is None:
            LOGGER.warning("debt_paydown_years: missing TTM FCF for %s", symbol)
            return None
        if fcf.total <= 0:
            LOGGER.warning("debt_paydown_years: non-positive FCF for %s", symbol)
            return None

        if not self._currencies_match(debt.currency, fcf.currency):
            LOGGER.warning("debt_paydown_years: currency mismatch for %s", symbol)
            return None

        ratio = debt.total / fcf.total
        as_of = max(debt.as_of, fcf.as_of)
        return MetricResult(symbol=symbol, metric_id=self.id, value=ratio, as_of=as_of)

    def _compute_total_debt(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_DebtResult]:
        short_debt = repo.latest_fact(symbol, "ShortTermDebt")
        long_debt = repo.latest_fact(symbol, "LongTermDebt")
        if short_debt is None or long_debt is None:
            return None
        if not is_recent_fact(short_debt) or not is_recent_fact(long_debt):
            return None

        short_value, short_currency = self._normalize_currency(short_debt)
        long_value, long_currency = self._normalize_currency(long_debt)
        currency = self._merge_currency([short_currency, long_currency])
        if currency is None and any(
            code is not None for code in (short_currency, long_currency)
        ):
            return None

        total_debt = short_value + long_value
        as_of = max(short_debt.end_date, long_debt.end_date)
        return _DebtResult(total=total_debt, as_of=as_of, currency=currency)

    def _compute_ttm_fcf(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_TTMResult]:
        operating = self._ttm_sum(symbol, repo, OPERATING_CASH_FLOW_CONCEPTS)
        capex = self._ttm_sum(symbol, repo, CAPEX_CONCEPTS)
        if operating is None:
            return None
        if capex is None:
            LOGGER.warning(
                "debt_paydown_years: missing/stale capex for %s; assuming zero",
                symbol,
            )
            capex_total = 0.0
            capex_as_of = operating.as_of
            capex_currency = None
        else:
            capex_total = capex.total
            capex_as_of = capex.as_of
            capex_currency = capex.currency
        fcf_total = operating.total - capex_total
        as_of = operating.as_of if operating.as_of >= capex_as_of else capex_as_of
        currency = operating.currency or capex_currency
        return _TTMResult(total=fcf_total, as_of=as_of, currency=currency)

    def _ttm_sum(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
    ) -> Optional[_TTMResult]:
        for concept in concepts:
            records = repo.facts_for_concept(symbol, concept)
            quarterly = self._filter_quarterly(records)
            if len(quarterly) < 4:
                LOGGER.warning(
                    "debt_paydown_years: need 4 quarterly %s records for %s, found %s",
                    concept,
                    symbol,
                    len(quarterly),
                )
                continue
            if not is_recent_fact(quarterly[0]):
                LOGGER.warning(
                    "debt_paydown_years: latest %s (%s) too old for %s",
                    concept,
                    quarterly[0].end_date,
                    symbol,
                )
                continue
            normalized, currency = self._normalize_quarterly(quarterly[:4])
            if normalized is None:
                LOGGER.warning(
                    "debt_paydown_years: currency conflict in %s quarterly values for %s",
                    concept,
                    symbol,
                )
                continue
            total = sum(record.value for record in normalized)
            return _TTMResult(
                total=total, as_of=normalized[0].end_date, currency=currency or None
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
        return filtered

    def _normalize_quarterly(
        self, records: Sequence[FactRecord]
    ) -> tuple[Optional[list[FactRecord]], Optional[str]]:
        currency = None
        normalized: list[FactRecord] = []
        for record in records:
            value, code = self._normalize_currency(record)
            if currency is None and code:
                currency = code
            elif code and currency and code != currency:
                return None, None
            normalized.append(
                FactRecord(
                    symbol=record.symbol,
                    cik=record.cik,
                    concept=record.concept,
                    fiscal_period=record.fiscal_period,
                    end_date=record.end_date,
                    unit=record.unit,
                    value=value,
                    accn=record.accn,
                    filed=record.filed,
                    frame=record.frame,
                    start_date=getattr(record, "start_date", None),
                    accounting_standard=getattr(record, "accounting_standard", None),
                    currency=code,
                )
            )
        return normalized, currency

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


__all__ = ["DebtPaydownYearsMetric"]
