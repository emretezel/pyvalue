"""Stock-based compensation load metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money

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
class _MoneyResult:
    money: Money
    as_of: str


class SBCLoadCalculator:
    """Shared calculator for SBC load TTM inputs.

    Each TTM amount is aligned to the listing currency, so the ratios computed
    by the metrics below (SBC / revenue, SBC / FCF) are currency-safe.
    """

    def compute_ttm_sbc(
        self, listing_id: int, repo: RegionFactsRepository, *, context: str
    ) -> Optional[_MoneyResult]:
        return self._compute_ttm_amount(
            listing_id,
            repo,
            STOCK_BASED_COMPENSATION_CONCEPT,
            context=context,
        )

    def compute_ttm_revenue(
        self, listing_id: int, repo: RegionFactsRepository, *, context: str
    ) -> Optional[_MoneyResult]:
        return self._compute_ttm_amount(
            listing_id,
            repo,
            REVENUE_CONCEPT,
            context=context,
        )

    def compute_ttm_fcf(
        self, listing_id: int, repo: RegionFactsRepository, *, context: str
    ) -> Optional[_MoneyResult]:
        operating = self._compute_ttm_amount(
            listing_id,
            repo,
            OPERATING_CASH_FLOW_CONCEPT,
            context=context,
        )
        if operating is None:
            LOGGER.warning(
                "%s: missing TTM operating cash flow for listing_id=%s",
                context,
                listing_id,
            )
            return None

        capex = self._compute_ttm_amount(
            listing_id,
            repo,
            CAPEX_CONCEPT,
            context=context,
        )
        if capex is None:
            LOGGER.warning(
                "%s: missing/stale capex for listing_id=%s; assuming zero",
                context,
                listing_id,
            )
            return operating

        return _MoneyResult(
            money=operating.money - capex.money,
            as_of=max(operating.as_of, capex.as_of),
        )

    def _compute_ttm_amount(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
        *,
        context: str,
    ) -> Optional[_MoneyResult]:
        records = repo.monetary_facts_for_concept(listing_id, concept)
        quarterly = self._filter_quarterly(records)
        if len(quarterly) < 4:
            LOGGER.warning(
                "%s: need 4 quarterly %s records for listing_id=%s, found %s",
                context,
                concept,
                listing_id,
                len(quarterly),
            )
            return None
        if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest %s (%s) too old for listing_id=%s",
                context,
                concept,
                quarterly[0].end_date,
                listing_id,
            )
            return None

        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            metric_id=context,
            input_name=concept,
            as_of=quarterly[0].end_date,
        )
        monies = [
            require_metric_money(
                record.money,
                target_currency=target_currency,
                metric_id=context,
                listing_id=listing_id,
                input_name=concept,
                as_of=record.end_date,
            )
            for record in quarterly[:4]
        ]
        return _MoneyResult(money=sum_money(monies), as_of=quarterly[0].end_date)

    def _filter_quarterly(self, records: Iterable[MonetaryFact]) -> list[MonetaryFact]:
        filtered: list[MonetaryFact] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in QUARTERLY_PERIODS or record.end_date in seen_end_dates:
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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        calculator = SBCLoadCalculator()
        sbc = calculator.compute_ttm_sbc(listing_id, repo, context=self.id)
        if sbc is None:
            LOGGER.warning("%s: missing TTM SBC for listing_id=%s", self.id, listing_id)
            return None

        revenue = calculator.compute_ttm_revenue(listing_id, repo, context=self.id)
        if revenue is None:
            LOGGER.warning(
                "%s: missing TTM revenue for listing_id=%s", self.id, listing_id
            )
            return None
        if revenue.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive TTM revenue for listing_id=%s", self.id, listing_id
            )
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=sbc.money / revenue.money,
            as_of=max(sbc.as_of, revenue.as_of),
            unit_kind="percent",
        )


@dataclass
class SBCToFCFMetric:
    """Compute SBC as a share of trailing free cash flow."""

    id: str = "sbc_to_fcf"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        calculator = SBCLoadCalculator()
        sbc = calculator.compute_ttm_sbc(listing_id, repo, context=self.id)
        if sbc is None:
            LOGGER.warning("%s: missing TTM SBC for listing_id=%s", self.id, listing_id)
            return None

        fcf = calculator.compute_ttm_fcf(listing_id, repo, context=self.id)
        if fcf is None:
            LOGGER.warning("%s: missing TTM FCF for listing_id=%s", self.id, listing_id)
            return None
        if fcf.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive TTM FCF for listing_id=%s", self.id, listing_id
            )
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=sbc.money / fcf.money,
            as_of=max(sbc.as_of, fcf.as_of),
            unit_kind="percent",
        )


__all__ = [
    "SBCLoadCalculator",
    "SBCToRevenueMetric",
    "SBCToFCFMetric",
]
