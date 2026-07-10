"""Price to Free Cash Flow metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    SHARE_RESOLVER_REQUIRED_CONCEPTS,
    is_recent_fact,
    market_cap_money,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

OPERATING_CASH_FLOW_CONCEPTS = ["NetCashProvidedByUsedInOperatingActivities"]
CAPEX_CONCEPTS = ["CapitalExpenditures"]
QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}

LOGGER = logging.getLogger(__name__)


@dataclass
class _MoneyResult:
    money: Money
    as_of: str


@dataclass
class PriceToFCFMetric:
    id: str = "price_to_fcf"
    # Numerator is market cap (shares x price); preload the share-count concepts
    # market_cap_money resolves alongside the FCF concepts.
    required_concepts = tuple(
        OPERATING_CASH_FLOW_CONCEPTS
        + CAPEX_CONCEPTS
        + list(SHARE_RESOLVER_REQUIRED_CONCEPTS)
    )
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        target_currency = require_metric_ticker_currency(
            listing_id, repo, metric_id=self.id, input_name="FreeCashFlow"
        )
        fcf_result = self._compute_ttm_fcf(listing_id, repo, target_currency)
        if fcf_result is None:
            LOGGER.warning(
                "price_to_fcf: missing TTM FCF for listing_id=%s", listing_id
            )
            return None
        if fcf_result.money.amount <= 0:
            LOGGER.warning(
                "price_to_fcf: non-positive TTM FCF for listing_id=%s", listing_id
            )
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
            LOGGER.warning(
                "price_to_fcf: missing market cap for listing_id=%s", listing_id
            )
            return None

        # Market cap and FCF are both in the listing currency, so the multiple
        # (Money / Money) is currency-safe.
        ratio = cap.money / fcf_result.money
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=ratio,
            as_of=fcf_result.as_of,
        )

    def _compute_ttm_fcf(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        operating = self._ttm_sum(
            listing_id, repo, OPERATING_CASH_FLOW_CONCEPTS, target_currency
        )
        if operating is None:
            return None
        capex = self._ttm_sum(listing_id, repo, CAPEX_CONCEPTS, target_currency)
        if capex is None:
            LOGGER.warning(
                "price_to_fcf: missing/stale capex for listing_id=%s; assuming zero",
                listing_id,
            )
            return operating
        return _MoneyResult(
            money=operating.money - capex.money,
            as_of=max(operating.as_of, capex.as_of),
        )

    def _ttm_sum(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        target_currency: str,
    ) -> Optional[_MoneyResult]:
        for concept in concepts:
            records = repo.monetary_facts_for_concept(listing_id, concept)
            quarterly = self._filter_quarterly(records)
            if len(quarterly) < 4:
                LOGGER.warning(
                    "price_to_fcf: need 4 quarterly %s records for listing_id=%s,"
                    " found %s",
                    concept,
                    listing_id,
                    len(quarterly),
                )
                continue
            values = quarterly[:4]
            if not is_recent_fact(values[0]):
                LOGGER.warning(
                    "price_to_fcf: latest %s (%s) too old for listing_id=%s",
                    concept,
                    values[0].end_date,
                    listing_id,
                )
                continue
            monies = [
                require_metric_money(
                    record.money,
                    target_currency=target_currency,
                    metric_id=self.id,
                    listing_id=listing_id,
                    input_name="FreeCashFlow",
                    as_of=record.end_date,
                )
                for record in values
            ]
            return _MoneyResult(money=sum_money(monies), as_of=values[0].end_date)
        return None

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


__all__ = ["PriceToFCFMetric"]
