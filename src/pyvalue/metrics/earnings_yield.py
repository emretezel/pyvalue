"""Earnings yield metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.marketdata.base import PriceData
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.ttm import resolve_ttm_window
from pyvalue.metrics.utils import (
    is_recent_fact,
    require_metric_amount_money,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

EPS_CONCEPTS = ["EarningsPerShare"]

LOGGER = logging.getLogger(__name__)


@dataclass
class EarningsYieldMetric:
    id: str = "earnings_yield"
    required_concepts = tuple(EPS_CONCEPTS)
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        eps = self._ttm_eps(listing_id, repo)
        if eps is None:
            return None
        eps_money, as_of = eps

        price_data = self._latest_snapshot(market_repo, listing_id)
        if price_data is None or price_data.price is None:
            LOGGER.warning(
                "earnings_yield: missing price for listing_id=%s", listing_id
            )
            return None
        # Price is aligned to the EPS (listing) currency, so the yield
        # (EPS Money / price Money) is currency-safe.
        price_money = require_metric_amount_money(
            price_data.price,
            price_data.currency,
            target_currency=eps_money.currency,
            metric_id=self.id,
            listing_id=listing_id,
            input_name="price",
            as_of=price_data.as_of,
        )
        if price_money.amount <= 0:
            LOGGER.warning(
                "earnings_yield: non-positive price for listing_id=%s", listing_id
            )
            return None
        yield_value = eps_money / price_money
        return MetricResult(
            listing_id=listing_id, metric_id=self.id, value=yield_value, as_of=as_of
        )

    def _ttm_eps(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[tuple[Money, str]]:
        # Quarterly-path failures are not logged here: every miss (short,
        # stale, or cadence-broken history) funnels into the FY fallback
        # below, and the FY branch already logs the metric-level miss.
        resolution = resolve_ttm_window(
            repo.monetary_facts_for_concept(listing_id, EPS_CONCEPTS[0])
        )
        window = resolution.window
        if window is not None:
            target_currency = require_metric_ticker_currency(
                listing_id,
                repo,
                metric_id=self.id,
                input_name="EarningsPerShare",
                as_of=window.as_of,
            )
            ttm = sum_money(
                [
                    require_metric_money(
                        record.money,
                        target_currency=target_currency,
                        metric_id=self.id,
                        listing_id=listing_id,
                        input_name="EarningsPerShare",
                        as_of=record.end_date,
                    )
                    for record in window.records
                ]
            )
            return ttm, window.as_of

        fy_record = self._latest_fy_eps(listing_id, repo)
        if fy_record is None:
            LOGGER.warning(
                "earnings_yield: missing EPS quarters for listing_id=%s", listing_id
            )
            return None
        if not is_recent_fact(fy_record):
            LOGGER.warning(
                "earnings_yield: latest FY EPS too old for listing_id=%s (%s)",
                listing_id,
                fy_record.end_date,
            )
            return None
        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            metric_id=self.id,
            input_name="EarningsPerShare",
            as_of=fy_record.end_date,
        )
        money = require_metric_money(
            fy_record.money,
            target_currency=target_currency,
            metric_id=self.id,
            listing_id=listing_id,
            input_name="EarningsPerShare",
            as_of=fy_record.end_date,
        )
        return money, fy_record.end_date

    def _latest_fy_eps(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MonetaryFact]:
        records = repo.monetary_facts_for_concept(
            listing_id, "EarningsPerShare", fiscal_period="FY", limit=1
        )
        if records:
            return records[0]
        return None

    def _latest_snapshot(
        self, market_repo: MarketDataRepository, listing_id: int
    ) -> Optional[PriceData]:
        if hasattr(market_repo, "latest_snapshot_by_id"):
            snapshot = market_repo.latest_snapshot_by_id(listing_id)
            if snapshot:
                return snapshot
        if hasattr(market_repo, "latest_price_by_id"):
            price_entry = market_repo.latest_price_by_id(listing_id)
            if isinstance(price_entry, tuple) and len(price_entry) >= 2:
                as_of, price = price_entry[0], price_entry[1]
                return PriceData(symbol="", price=price, as_of=as_of)
        return None
