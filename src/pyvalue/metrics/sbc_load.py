"""Stock-based compensation load metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, is_recent_fact
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
            # Keep existing repo semantics for FCF: missing capex means treat it as 0.
            LOGGER.warning(
                "%s: missing/stale capex for %s; assuming zero", context, symbol
            )
            return _AmountResult(
                total=operating.total,
                as_of=operating.as_of,
                currency=operating.currency,
            )

        currency = self._combine_currency([operating.currency, capex.currency])
        if currency is None and any(
            code is not None for code in (operating.currency, capex.currency)
        ):
            LOGGER.warning(
                "%s: currency mismatch in TTM FCF inputs for %s", context, symbol
            )
            return None

        return _AmountResult(
            total=operating.total - capex.total,
            as_of=max(operating.as_of, capex.as_of),
            currency=currency,
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

        normalized, currency = self._normalize_quarterly(quarterly[:4])
        if normalized is None:
            LOGGER.warning(
                "%s: currency conflict in %s quarterly values for %s",
                context,
                concept,
                symbol,
            )
            return None

        return _AmountResult(
            total=sum(normalized),
            as_of=quarterly[0].end_date,
            currency=currency,
        )

    def currencies_match(self, left: Optional[str], right: Optional[str]) -> bool:
        if left is None and right is None:
            return True
        return self._combine_currency([left, right]) is not None

    def _combine_currency(self, currencies: Iterable[Optional[str]]) -> Optional[str]:
        selected: Optional[str] = None
        for currency in currencies:
            if currency is None:
                continue
            if selected is None:
                selected = currency
                continue
            if currency != selected:
                return None
        return selected

    def _filter_quarterly(self, records: Iterable[FactRecord]) -> list[FactRecord]:
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
        self, records: list[FactRecord]
    ) -> tuple[Optional[list[float]], Optional[str]]:
        currency: Optional[str] = None
        normalized: list[float] = []
        for record in records:
            code = getattr(record, "currency", None)
            value = record.value
            if code in {"GBX", "GBP0.01"}:
                code = "GBP"
                value = value / 100.0 if value is not None else None
            if value is None:
                continue
            if currency is None and code:
                currency = code
            elif code and currency and code != currency:
                return None, None
            normalized.append(value)
        return normalized, currency


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
        if not calculator.currencies_match(sbc.currency, revenue.currency):
            LOGGER.warning("%s: currency mismatch for %s", self.id, symbol)
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=sbc.total / revenue.total,
            as_of=max(sbc.as_of, revenue.as_of),
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
        if not calculator.currencies_match(sbc.currency, fcf.currency):
            LOGGER.warning("%s: currency mismatch for %s", self.id, symbol)
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=sbc.total / fcf.total,
            as_of=max(sbc.as_of, fcf.as_of),
        )


__all__ = [
    "SBCLoadCalculator",
    "SBCToRevenueMetric",
    "SBCToFCFMetric",
]
