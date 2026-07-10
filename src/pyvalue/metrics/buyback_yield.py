"""Net buyback yield metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.share_count_change import ShareCountChangeCalculator
from pyvalue.metrics.ttm import resolve_ttm_window
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    SHARE_RESOLVER_REQUIRED_CONCEPTS,
    market_cap_money,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

LOGGER = logging.getLogger(__name__)

SALE_PURCHASE_CONCEPT = "SalePurchaseOfStock"
ISSUANCE_CAPITAL_STOCK_CONCEPT = "IssuanceOfCapitalStock"
SHARE_COUNT_CONCEPT = "CommonStockSharesOutstanding"

FALLBACK_YEARS = 1

REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        (
            SALE_PURCHASE_CONCEPT,
            ISSUANCE_CAPITAL_STOCK_CONCEPT,
            SHARE_COUNT_CONCEPT,
            # Denominator market cap = shares x price; preload the share-count
            # concepts market_cap_money resolves.
            *SHARE_RESOLVER_REQUIRED_CONCEPTS,
        )
    )
)


@dataclass(frozen=True)
class _MoneyResult:
    money: Money
    as_of: str


@dataclass
class NetBuybackYieldMetric:
    """Compute net buyback yield using EODHD financing cash flow or share fallback."""

    id: str = "net_buyback_yield"
    required_concepts = REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        numerator = self._compute_net_buybacks_ttm(listing_id, repo)
        if numerator is not None:
            # Market cap is resolved in the numerator's (listing) currency, so the
            # yield (Money / Money) is currency-safe.
            cap = market_cap_money(
                listing_id,
                repo=repo,
                market_repo=market_repo,
                metric_id=self.id,
                target_currency=numerator.money.currency,
                contexts=(repo, market_repo),
            )
            if cap is not None:
                return MetricResult(
                    listing_id=listing_id,
                    metric_id=self.id,
                    value=numerator.money / cap.money,
                    as_of=numerator.as_of,
                )

        # Fallback: net share-count change over one year (a dimensionless rate).
        snapshot = ShareCountChangeCalculator().compute_pair_for_years(
            listing_id,
            repo,
            exact_years=FALLBACK_YEARS,
            context=self.id,
        )
        if snapshot is None:
            return None
        value = -((snapshot.latest.shares / snapshot.prior.shares) - 1.0)
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=value,
            as_of=snapshot.as_of,
        )

    def _compute_net_buybacks_ttm(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[_MoneyResult]:
        primary = self._ttm_sum(listing_id, repo, SALE_PURCHASE_CONCEPT)
        if primary is not None:
            # Net buybacks = -(cash from sale/purchase of stock).
            return _MoneyResult(money=-primary.money, as_of=primary.as_of)

        fallback = self._ttm_sum(listing_id, repo, ISSUANCE_CAPITAL_STOCK_CONCEPT)
        if fallback is not None:
            return _MoneyResult(money=-fallback.money, as_of=fallback.as_of)
        return None

    def _ttm_sum(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
    ) -> Optional[_MoneyResult]:
        # Opt into the annual cadence: an annual-only filer's financing cash
        # flow (sale/purchase of stock) is a single FY figure. The compute()
        # fallback -- year-over-year share-count change -- is already FY-based,
        # so annual support here just prefers the direct cash figure when present.
        resolution = resolve_ttm_window(
            repo.monetary_facts_for_concept(listing_id, concept),
            annual_max_age_days=MAX_FY_FACT_AGE_DAYS,
        )
        window = resolution.window
        if window is None:
            LOGGER.warning(
                "%s: %s (concept=%s, listing_id=%s)",
                self.id,
                resolution.failure,
                concept,
                listing_id,
            )
            return None

        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            metric_id=self.id,
            input_name="ShareRepurchases",
            as_of=window.as_of,
        )
        total = sum_money(
            [
                require_metric_money(
                    record.money,
                    target_currency=target_currency,
                    metric_id=self.id,
                    listing_id=listing_id,
                    input_name="ShareRepurchases",
                    as_of=record.end_date,
                )
                for record in window.records
            ]
        )
        return _MoneyResult(money=total, as_of=window.as_of)


__all__ = ["NetBuybackYieldMetric"]
