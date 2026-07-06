"""Enterprise-value based valuation metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.enterprise_value import (
    EV_REQUIRED_CONCEPTS,
    resolve_enterprise_value_denominator,
)
from pyvalue.metrics.ttm import paired_records, resolve_ttm_window
from pyvalue.metrics.utils import (
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

LOGGER = logging.getLogger(__name__)

EBIT_CONCEPT = "OperatingIncomeLoss"
OPERATING_CASH_FLOW_CONCEPT = "NetCashProvidedByUsedInOperatingActivities"
CAPEX_CONCEPT = "CapitalExpenditures"
DA_PRIMARY_CONCEPT = "DepreciationDepletionAndAmortization"
DA_FALLBACK_CONCEPT = "DepreciationFromCashFlow"
REVENUE_CONCEPT = "Revenues"

EBIT_REQUIRED_CONCEPTS = tuple(dict.fromkeys((EBIT_CONCEPT,) + EV_REQUIRED_CONCEPTS))
FCF_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        (
            OPERATING_CASH_FLOW_CONCEPT,
            CAPEX_CONCEPT,
        )
        + EV_REQUIRED_CONCEPTS
    )
)
EBITDA_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        (
            EBIT_CONCEPT,
            DA_PRIMARY_CONCEPT,
            DA_FALLBACK_CONCEPT,
        )
        + EV_REQUIRED_CONCEPTS
    )
)
SALES_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys((REVENUE_CONCEPT,) + EV_REQUIRED_CONCEPTS)
)


@dataclass(frozen=True)
class TTMResult:
    money: Money
    as_of: str


class EnterpriseValueRatioCalculator:
    """Shared numerator calculators for EV-based valuation metrics."""

    def compute_ttm_ebit(
        self, listing_id: int, repo: RegionFactsRepository, *, context: str
    ) -> Optional[TTMResult]:
        return self._compute_ttm_amount(listing_id, repo, EBIT_CONCEPT, context=context)

    def compute_ttm_fcf(
        self, listing_id: int, repo: RegionFactsRepository, *, context: str
    ) -> Optional[TTMResult]:
        operating = self._compute_ttm_amount(
            listing_id,
            repo,
            OPERATING_CASH_FLOW_CONCEPT,
            context=context,
        )
        if operating is None:
            LOGGER.warning("%s: missing TTM FCF for listing_id=%s", context, listing_id)
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

        return TTMResult(
            money=operating.money - capex.money,
            as_of=max(operating.as_of, capex.as_of),
        )

    def compute_ttm_ebitda(
        self, listing_id: int, repo: RegionFactsRepository, *, context: str
    ) -> Optional[TTMResult]:
        resolution = resolve_ttm_window(
            repo.monetary_facts_for_concept(listing_id, EBIT_CONCEPT)
        )
        window = resolution.window
        if window is None:
            LOGGER.warning(
                "%s: %s (concept=%s, listing_id=%s)",
                context,
                resolution.failure,
                EBIT_CONCEPT,
                listing_id,
            )
            return None

        # Primary D&A rows are listed before the fallback rows: paired_records
        # keeps the first candidate per end_date, so the primary concept wins
        # a quarter and the fallback only fills its holes -- the same
        # per-quarter primary-else-fallback rule as before the window refactor.
        pairs = paired_records(
            window,
            [
                *repo.monetary_facts_for_concept(listing_id, DA_PRIMARY_CONCEPT),
                *repo.monetary_facts_for_concept(listing_id, DA_FALLBACK_CONCEPT),
            ],
        )
        if pairs is None:
            LOGGER.warning(
                "%s: missing D&A for a TTM window quarter (listing_id=%s)",
                context,
                listing_id,
            )
            return None

        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=context
        )
        quarter_totals = [
            self._money(ebit_record, target_currency, listing_id, context)
            + self._money(da_record, target_currency, listing_id, context)
            for ebit_record, da_record in pairs
        ]
        return TTMResult(
            money=sum_money(quarter_totals),
            as_of=window.as_of,
        )

    def compute_ttm_revenue(
        self, listing_id: int, repo: RegionFactsRepository, *, context: str
    ) -> Optional[TTMResult]:
        return self._compute_ttm_amount(
            listing_id, repo, REVENUE_CONCEPT, context=context
        )

    def _compute_ttm_amount(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
        *,
        context: str,
    ) -> Optional[TTMResult]:
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
            listing_id, repo, metric_id=context
        )
        monies = [
            self._money(record, target_currency, listing_id, context)
            for record in window.records
        ]
        return TTMResult(
            money=sum_money(monies),
            as_of=window.as_of,
        )

    def _money(
        self, fact: MonetaryFact, target_currency: str, listing_id: int, context: str
    ) -> Money:
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=context,
            listing_id=listing_id,
            input_name=fact.concept,
            as_of=fact.end_date,
        )


@dataclass
class EBITYieldEVMetric:
    """Compute trailing EBIT yield on enterprise value."""

    id: str = "ebit_yield_ev"
    required_concepts = EBIT_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = EnterpriseValueRatioCalculator().compute_ttm_ebit(
            listing_id, repo, context=self.id
        )
        if numerator is None:
            LOGGER.warning(
                "%s: missing numerator for listing_id=%s", self.id, listing_id
            )
            return None

        enterprise_value = resolve_enterprise_value_denominator(
            listing_id=listing_id,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.money.currency,
            context=self.id,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=numerator.money / enterprise_value,
            as_of=numerator.as_of,
        )


@dataclass
class FCFYieldEVMetric:
    """Compute trailing FCF yield on enterprise value."""

    id: str = "fcf_yield_ev"
    required_concepts = FCF_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = EnterpriseValueRatioCalculator().compute_ttm_fcf(
            listing_id, repo, context=self.id
        )
        if numerator is None:
            LOGGER.warning(
                "%s: missing numerator for listing_id=%s", self.id, listing_id
            )
            return None

        enterprise_value = resolve_enterprise_value_denominator(
            listing_id=listing_id,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.money.currency,
            context=self.id,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=numerator.money / enterprise_value,
            as_of=numerator.as_of,
        )


@dataclass
class EVToEBITMetric:
    """Compute enterprise value divided by trailing EBIT."""

    id: str = "ev_to_ebit"
    required_concepts = EBIT_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = EnterpriseValueRatioCalculator().compute_ttm_ebit(
            listing_id, repo, context=self.id
        )
        if numerator is None:
            LOGGER.warning(
                "%s: missing denominator EBIT for listing_id=%s", self.id, listing_id
            )
            return None
        if numerator.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive EBIT for listing_id=%s", self.id, listing_id
            )
            return None

        enterprise_value = resolve_enterprise_value_denominator(
            listing_id=listing_id,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.money.currency,
            context=self.id,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=enterprise_value / numerator.money,
            as_of=numerator.as_of,
        )


@dataclass
class EVToEBITDAMetric:
    """Compute enterprise value divided by trailing component EBITDA."""

    id: str = "ev_to_ebitda"
    required_concepts = EBITDA_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = EnterpriseValueRatioCalculator().compute_ttm_ebitda(
            listing_id, repo, context=self.id
        )
        if numerator is None:
            LOGGER.warning(
                "%s: missing denominator EBITDA for listing_id=%s", self.id, listing_id
            )
            return None
        if numerator.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive EBITDA for listing_id=%s", self.id, listing_id
            )
            return None

        enterprise_value = resolve_enterprise_value_denominator(
            listing_id=listing_id,
            repo=repo,
            market_repo=market_repo,
            target_currency=numerator.money.currency,
            context=self.id,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=enterprise_value / numerator.money,
            as_of=numerator.as_of,
        )


@dataclass
class EVToSalesMetric:
    """Compute enterprise value divided by trailing revenue.

    Stays usable for margin-trough cyclicals and temporarily unprofitable
    businesses where EV/EBIT and EV/EBITDA are undefined.
    """

    id: str = "ev_to_sales"
    required_concepts = SALES_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        revenue = EnterpriseValueRatioCalculator().compute_ttm_revenue(
            listing_id, repo, context=self.id
        )
        if revenue is None:
            LOGGER.warning(
                "%s: missing denominator revenue for listing_id=%s",
                self.id,
                listing_id,
            )
            return None
        if revenue.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive revenue for listing_id=%s", self.id, listing_id
            )
            return None

        enterprise_value = resolve_enterprise_value_denominator(
            listing_id=listing_id,
            repo=repo,
            market_repo=market_repo,
            target_currency=revenue.money.currency,
            context=self.id,
        )
        if enterprise_value is None:
            return None

        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=enterprise_value / revenue.money,
            as_of=revenue.as_of,
        )


__all__ = [
    "TTMResult",
    "EBITYieldEVMetric",
    "FCFYieldEVMetric",
    "EVToEBITMetric",
    "EVToEBITDAMetric",
    "EVToSalesMetric",
]
