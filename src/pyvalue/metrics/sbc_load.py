"""Stock-based compensation load metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import logging

from pyvalue.fx import FXService
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, is_recent_fact
from pyvalue.money import align_money_values, fx_service_for_context
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
            fx_service=fx_service_for_context(repo),
        )

    def compute_ttm_revenue(
        self, symbol: str, repo: FinancialFactsRepository, *, context: str
    ) -> Optional[_AmountResult]:
        return self._compute_ttm_amount(
            symbol,
            repo,
            REVENUE_CONCEPT,
            context=context,
            fx_service=fx_service_for_context(repo),
        )

    def compute_ttm_fcf(
        self, symbol: str, repo: FinancialFactsRepository, *, context: str
    ) -> Optional[_AmountResult]:
        fx_service = fx_service_for_context(repo)
        operating = self._compute_ttm_amount(
            symbol,
            repo,
            OPERATING_CASH_FLOW_CONCEPT,
            context=context,
            fx_service=fx_service,
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
            fx_service=fx_service,
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

        aligned, currency = align_money_values(
            values=[
                (
                    operating.total,
                    operating.currency,
                    operating.as_of,
                    OPERATING_CASH_FLOW_CONCEPT,
                ),
                (capex.total, capex.currency, capex.as_of, CAPEX_CONCEPT),
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation=f"metric:{context}:fcf",
            symbol=symbol,
            target_currency=operating.currency or capex.currency,
        )
        if aligned is None or currency is None:
            LOGGER.warning(
                "%s: currency mismatch in TTM FCF inputs for %s", context, symbol
            )
            return None

        return _AmountResult(
            total=aligned[0] - aligned[1],
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
        fx_service: FXService,
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

        normalized, currency = align_money_values(
            values=[
                (record.value, record.currency, record.end_date, concept)
                for record in quarterly[:4]
                if record.value is not None
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation=f"metric:{context}:{concept}",
            symbol=symbol,
            target_currency=quarterly[0].currency,
        )
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
        fx_service = fx_service_for_context(repo)
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
        aligned, _ = align_money_values(
            values=[
                (sbc.total, sbc.currency, sbc.as_of, STOCK_BASED_COMPENSATION_CONCEPT),
                (revenue.total, revenue.currency, revenue.as_of, REVENUE_CONCEPT),
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation=f"metric:{self.id}",
            symbol=symbol,
            target_currency=sbc.currency or revenue.currency,
        )
        if aligned is None:
            LOGGER.warning("%s: currency mismatch for %s", self.id, symbol)
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=aligned[0] / aligned[1],
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
        fx_service = fx_service_for_context(repo)
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
        aligned, _ = align_money_values(
            values=[
                (sbc.total, sbc.currency, sbc.as_of, STOCK_BASED_COMPENSATION_CONCEPT),
                (fcf.total, fcf.currency, fcf.as_of, "FreeCashFlow"),
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation=f"metric:{self.id}",
            symbol=symbol,
            target_currency=sbc.currency or fcf.currency,
        )
        if aligned is None:
            LOGGER.warning("%s: currency mismatch for %s", self.id, symbol)
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=aligned[0] / aligned[1],
            as_of=max(sbc.as_of, fcf.as_of),
            unit_kind="percent",
        )


__all__ = [
    "SBCLoadCalculator",
    "SBCToRevenueMetric",
    "SBCToFCFMetric",
]
