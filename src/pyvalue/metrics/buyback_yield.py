"""Net buyback yield metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.share_count_change import ShareCountChangeCalculator
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    SHARE_COUNT_CONCEPTS,
    is_recent_fact,
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

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FALLBACK_YEARS = 1

REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        (
            SALE_PURCHASE_CONCEPT,
            ISSUANCE_CAPITAL_STOCK_CONCEPT,
            SHARE_COUNT_CONCEPT,
            # Denominator market cap = shares x price; preload the share-count
            # concepts market_cap_money resolves.
            *SHARE_COUNT_CONCEPTS,
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
        records = repo.monetary_facts_for_concept(listing_id, concept)
        quarterly = self._filter_quarterly(records)
        if len(quarterly) < 4:
            LOGGER.warning(
                "%s: need 4 quarterly %s records for listing_id=%s, found %s",
                self.id,
                concept,
                listing_id,
                len(quarterly),
            )
            return None
        if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
            LOGGER.warning(
                "%s: latest %s (%s) too old for listing_id=%s",
                self.id,
                concept,
                quarterly[0].end_date,
                listing_id,
            )
            return None

        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            metric_id=self.id,
            input_name="ShareRepurchases",
            as_of=quarterly[0].end_date,
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
                for record in quarterly[:4]
            ]
        )
        return _MoneyResult(money=total, as_of=quarterly[0].end_date)

    def _filter_quarterly(self, records: Iterable[MonetaryFact]) -> list[MonetaryFact]:
        filtered: list[MonetaryFact] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in QUARTERLY_PERIODS:
                continue
            if record.end_date in seen_end_dates:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered


__all__ = ["NetBuybackYieldMetric"]
