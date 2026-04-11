"""Stock-based compensation load metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    is_recent_fact,
    normalize_metric_record,
    resolve_metric_ticker_currency,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

STOCK_BASED_COMPENSATION_CONCEPT = "StockBasedCompensation"
REVENUE_CONCEPT = "Revenues"
OPERATING_CASH_FLOW_CONCEPT = "NetCashProvidedByUsedInOperatingActivities"
CAPEX_CONCEPT = "CapitalExpenditures"

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}

REQUIRED_CONCEPTS = (
    STOCK_BASED_COMPENSATION_CONCEPT,
    REVENUE_CONCEPT,
    OPERATING_CASH_FLOW_CONCEPT,
    CAPEX_CONCEPT,
)


@dataclass(frozen=True)
class _AmountResult:
    total: float
    as_of: str
    currency: Optional[str]


class SBCLoadCalculator:
    """Shared calculator for SBC load TTM inputs."""

    def compute_ttm_sbc(
        self, symbol: str, repo: FinancialFactsRepository, *, context: str
    ) -> Optional[_AmountResult]:
        return self._compute_ttm_amount(
            symbol,
            repo,
            STOCK_BASED_COMPENSATION_CONCEPT,
            context=context,
        )

    def compute_ttm_revenue(
        self, symbol: str, repo: FinancialFactsRepository, *, context: str
    ) -> Optional[_AmountResult]:
        return self._compute_ttm_amount(
            symbol,
            repo,
            REVENUE_CONCEPT,
            context=context,
        )

    def compute_ttm_fcf(
        self, symbol: str, repo: FinancialFactsRepository, *, context: str
    ) -> Optional[_AmountResult]:
        operating = self._compute_ttm_amount(
            symbol,
            repo,
            OPERATING_CASH_FLOW_CONCEPT,
            context=context,
        )
        if operating is None:
            LOGGER.warning(
                "%s: missing TTM operating cash flow for %s", context, symbol
            )
            return None

        capex = self._compute_ttm_amount(
            symbol,
            repo,
            CAPEX_CONCEPT,
            context=context,
        )
        if capex is None:
            LOGGER.warning(
                "%s: missing/stale capex for %s; assuming zero", context, symbol
            )
            return _AmountResult(
                total=operating.total,
                as_of=operating.as_of,
                currency=operating.currency,
            )

        return _AmountResult(
            total=operating.total - capex.total,
            as_of=max(operating.as_of, capex.as_of),
            currency=operating.currency or capex.currency,
        )

    def _compute_ttm_amount(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
        *,
        context: str,
    ) -> Optional[_AmountResult]:
        records = repo.facts_for_concept(symbol, concept)
        quarterly = self._filter_quarterly(records)
        if len(quarterly) < 4:
            LOGGER.warning(
                "%s: need 4 quarterly %s records for %s, found %s",
                context,
                concept,
                symbol,
                len(quarterly),
            )
            return None
        if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest %s (%s) too old for %s",
                context,
                concept,
                quarterly[0].end_date,
                symbol,
            )
            return None

        target_currency = resolve_metric_ticker_currency(
            symbol,
            repo,
            candidate_currencies=[record.currency for record in quarterly[:4]],
        )
        normalized: list[float] = []
        for record in quarterly[:4]:
            value, _ = normalize_metric_record(
                record,
                metric_id=context,
                symbol=symbol,
                input_name=concept,
                expected_currency=target_currency,
                contexts=(repo,),
            )
            normalized.append(value)

        return _AmountResult(
            total=sum(normalized),
            as_of=quarterly[0].end_date,
            currency=target_currency,
        )

    def _filter_quarterly(self, records: Iterable[FactRecord]) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if (
                period not in QUARTERLY_PERIODS
                or record.end_date in seen_end_dates
                or record.value is None
            ):
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered


@dataclass
class SBCToRevenueMetric:
    """Compute SBC as a share of trailing revenue."""

    id: str = "sbc_to_revenue"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        calculator = SBCLoadCalculator()
        sbc = calculator.compute_ttm_sbc(symbol, repo, context=self.id)
        if sbc is None:
            LOGGER.warning("%s: missing TTM SBC for %s", self.id, symbol)
            return None

        revenue = calculator.compute_ttm_revenue(symbol, repo, context=self.id)
        if revenue is None:
            LOGGER.warning("%s: missing TTM revenue for %s", self.id, symbol)
            return None
        if revenue.total <= 0:
            LOGGER.warning("%s: non-positive TTM revenue for %s", self.id, symbol)
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=sbc.total / revenue.total,
            as_of=max(sbc.as_of, revenue.as_of),
            unit_kind="percent",
        )


@dataclass
class SBCToFCFMetric:
    """Compute SBC as a share of trailing free cash flow."""

    id: str = "sbc_to_fcf"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        calculator = SBCLoadCalculator()
        sbc = calculator.compute_ttm_sbc(symbol, repo, context=self.id)
        if sbc is None:
            LOGGER.warning("%s: missing TTM SBC for %s", self.id, symbol)
            return None

        fcf = calculator.compute_ttm_fcf(symbol, repo, context=self.id)
        if fcf is None:
            LOGGER.warning("%s: missing TTM FCF for %s", self.id, symbol)
            return None
        if fcf.total <= 0:
            LOGGER.warning("%s: non-positive TTM FCF for %s", self.id, symbol)
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=sbc.total / fcf.total,
            as_of=max(sbc.as_of, fcf.as_of),
            unit_kind="percent",
        )


__all__ = [
    "SBCLoadCalculator",
    "SBCToRevenueMetric",
    "SBCToFCFMetric",
]
