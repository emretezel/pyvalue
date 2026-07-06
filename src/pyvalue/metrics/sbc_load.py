"""Stock-based compensation load metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.ttm import resolve_ttm_window
from pyvalue.metrics.utils import (
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
        resolution = resolve_ttm_window(
            repo.monetary_facts_for_concept(listing_id, concept)
        )
        window = resolution.window
        if window is None:
            LOGGER.warning(
                "%s: %s (concept=%s, listing_id=%s)",
                context,
                resolution.failure,
                concept,
                listing_id,
            )
            return None

        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            metric_id=context,
            input_name=concept,
            as_of=window.as_of,
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
            for record in window.records
        ]
        return _MoneyResult(money=sum_money(monies), as_of=window.as_of)


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
