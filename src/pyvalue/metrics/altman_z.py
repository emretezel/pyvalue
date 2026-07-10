"""Altman Z-Score bankruptcy-risk metric.

The classic 1968 five-factor discriminant score for public manufacturers,
used here as a value-trap / distress filter: Z < 1.81 signals distress,
Z > 2.99 the safe zone. Stocks (working capital, retained earnings, total
assets/liabilities) come from the latest balance sheet; flows (EBIT, sales)
are trailing twelve months with an FY fallback; market value of equity is
the on-demand market cap.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.enterprise_value_ratios import (
    EBIT_CONCEPT,
    REVENUE_CONCEPT,
    EnterpriseValueRatioCalculator,
    TTMResult,
)
from pyvalue.metrics.utils import (
    SHARE_RESOLVER_REQUIRED_CONCEPTS,
    is_recent_fact,
    market_cap_money,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

LOGGER = logging.getLogger(__name__)

ASSETS_CONCEPT = "Assets"
LIABILITIES_CONCEPT = "Liabilities"
CURRENT_ASSETS_CONCEPT = "AssetsCurrent"
CURRENT_LIABILITIES_CONCEPT = "LiabilitiesCurrent"
RETAINED_EARNINGS_CONCEPT = "RetainedEarnings"

REQUIRED_CONCEPTS = (
    ASSETS_CONCEPT,
    LIABILITIES_CONCEPT,
    CURRENT_ASSETS_CONCEPT,
    CURRENT_LIABILITIES_CONCEPT,
    RETAINED_EARNINGS_CONCEPT,
    EBIT_CONCEPT,
    REVENUE_CONCEPT,
    # X4 divides market cap (shares x price) by liabilities, so the
    # share-count concepts must be preloaded too.
    *SHARE_RESOLVER_REQUIRED_CONCEPTS,
)

# Altman's original public-manufacturer discriminant weights (1968).
X1_WEIGHT = 1.2  # working capital / total assets
X2_WEIGHT = 1.4  # retained earnings / total assets
X3_WEIGHT = 3.3  # EBIT / total assets
X4_WEIGHT = 0.6  # market value of equity / total liabilities
X5_WEIGHT = 1.0  # sales / total assets


@dataclass(frozen=True)
class _Component:
    money: Money
    as_of: str


@dataclass
class AltmanZMetric:
    """Compute the classic five-factor Altman Z-Score."""

    id: str = "altman_z"
    required_concepts = REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        # Resolve the listing currency once; every monetary component is
        # aligned to it, so each X term is a currency-safe Money/Money ratio.
        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            market_repo,
            metric_id=self.id,
            input_name=ASSETS_CONCEPT,
        )

        assets = self._latest(listing_id, repo, ASSETS_CONCEPT, target_currency)
        if assets is None or assets.money.amount <= 0:
            LOGGER.warning(
                "%s: missing/non-positive total assets for listing_id=%s",
                self.id,
                listing_id,
            )
            return None
        liabilities = self._latest(
            listing_id, repo, LIABILITIES_CONCEPT, target_currency
        )
        if liabilities is None or liabilities.money.amount <= 0:
            LOGGER.warning(
                "%s: missing/non-positive total liabilities for listing_id=%s",
                self.id,
                listing_id,
            )
            return None

        # The score is only comparable when every factor is present, so any
        # missing component (including retained earnings, which may simply not
        # be normalized yet) suppresses the metric rather than skewing it.
        current_assets = self._latest(
            listing_id, repo, CURRENT_ASSETS_CONCEPT, target_currency
        )
        current_liabilities = self._latest(
            listing_id, repo, CURRENT_LIABILITIES_CONCEPT, target_currency
        )
        # Negative retained earnings (an accumulated deficit) is meaningful --
        # it is precisely what drags young or chronically lossmaking issuers
        # toward the distress zone -- so only absence suppresses the score.
        retained = self._latest(
            listing_id, repo, RETAINED_EARNINGS_CONCEPT, target_currency
        )
        if current_assets is None or current_liabilities is None or retained is None:
            return None

        calculator = EnterpriseValueRatioCalculator()
        ebit = self._flow_with_fy_fallback(
            listing_id,
            repo,
            concept=EBIT_CONCEPT,
            ttm=calculator.compute_ttm_ebit(listing_id, repo, context=self.id),
            target_currency=target_currency,
        )
        revenue = self._flow_with_fy_fallback(
            listing_id,
            repo,
            concept=REVENUE_CONCEPT,
            ttm=calculator.compute_ttm_revenue(listing_id, repo, context=self.id),
            target_currency=target_currency,
        )
        if ebit is None or revenue is None:
            return None

        cap = market_cap_money(
            listing_id,
            repo=repo,
            market_repo=market_repo,
            metric_id=self.id,
            target_currency=target_currency,
            contexts=(market_repo, repo),
        )
        if cap is None:
            return None

        working_capital = current_assets.money - current_liabilities.money
        x1 = working_capital / assets.money
        x2 = retained.money / assets.money
        x3 = ebit.money / assets.money
        x4 = cap.money / liabilities.money
        x5 = revenue.money / assets.money
        z_score = (
            X1_WEIGHT * x1
            + X2_WEIGHT * x2
            + X3_WEIGHT * x3
            + X4_WEIGHT * x4
            + X5_WEIGHT * x5
        )

        as_of = max(
            assets.as_of,
            liabilities.as_of,
            current_assets.as_of,
            current_liabilities.as_of,
            retained.as_of,
            ebit.as_of,
            revenue.as_of,
        )
        return MetricResult(
            listing_id=listing_id, metric_id=self.id, value=z_score, as_of=as_of
        )

    def _latest(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
        target_currency: str,
    ) -> Optional[_Component]:
        record = repo.latest_monetary_fact(listing_id, concept)
        if record is None or not is_recent_fact(record):
            LOGGER.warning(
                "%s: missing/stale %s for listing_id=%s", self.id, concept, listing_id
            )
            return None
        money = require_metric_money(
            record.money,
            target_currency=target_currency,
            metric_id=self.id,
            listing_id=listing_id,
            input_name=concept,
            as_of=record.end_date,
        )
        return _Component(money=money, as_of=record.end_date)

    def _flow_with_fy_fallback(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        concept: str,
        ttm: Optional[TTMResult],
        target_currency: str,
    ) -> Optional[_Component]:
        # Prefer the shared TTM policy (strict 4 quarters); the classic Z uses
        # annual statements, so a recent FY figure is an acceptable stand-in
        # when the quarterly history is too thin.
        if ttm is not None:
            return _Component(money=ttm.money, as_of=ttm.as_of)

        records = repo.monetary_facts_for_concept(
            listing_id, concept, fiscal_period="FY", limit=1
        )
        if not records or not is_recent_fact(records[0]):
            LOGGER.warning(
                "%s: missing TTM and recent FY %s for listing_id=%s",
                self.id,
                concept,
                listing_id,
            )
            return None
        record = records[0]
        money = require_metric_money(
            record.money,
            target_currency=target_currency,
            metric_id=self.id,
            listing_id=listing_id,
            input_name=concept,
            as_of=record.end_date,
        )
        return _Component(money=money, as_of=record.end_date)


__all__ = ["AltmanZMetric"]
