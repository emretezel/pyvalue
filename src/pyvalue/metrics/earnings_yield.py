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
from pyvalue.metrics.utils import (
    is_recent_fact,
    latest_quarterly_records,
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
        symbol: str,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        eps = self._ttm_eps(symbol, repo)
        if eps is None:
            return None
        eps_money, as_of = eps

        price_data = self._latest_snapshot(market_repo, symbol)
        if price_data is None or price_data.price is None:
            LOGGER.warning("earnings_yield: missing price for %s", symbol)
            return None
        # Price is aligned to the EPS (listing) currency, so the yield
        # (EPS Money / price Money) is currency-safe.
        price_money = require_metric_amount_money(
            price_data.price,
            price_data.currency,
            target_currency=eps_money.currency,
            metric_id=self.id,
            symbol=symbol,
            input_name="price",
            as_of=price_data.as_of,
        )
        if price_money.amount <= 0:
            LOGGER.warning("earnings_yield: non-positive price for %s", symbol)
            return None
        yield_value = eps_money / price_money
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=yield_value, as_of=as_of
        )

    def _ttm_eps(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[tuple[Money, str]]:
        quarterly_records = self._latest_quarters(symbol, repo)
        if len(quarterly_records) >= 4:
            target_currency = require_metric_ticker_currency(
                symbol,
                repo,
                metric_id=self.id,
                input_name="EarningsPerShare",
                as_of=quarterly_records[0].end_date,
            )
            ttm = sum_money(
                [
                    require_metric_money(
                        record.money,
                        target_currency=target_currency,
                        metric_id=self.id,
                        symbol=symbol,
                        input_name="EarningsPerShare",
                        as_of=record.end_date,
                    )
                    for record in quarterly_records[:4]
                ]
            )
            return ttm, quarterly_records[0].end_date

        fy_record = self._latest_fy_eps(symbol, repo)
        if fy_record is None:
            LOGGER.warning("earnings_yield: missing EPS quarters for %s", symbol)
            return None
        if not is_recent_fact(fy_record):
            LOGGER.warning(
                "earnings_yield: latest FY EPS too old for %s (%s)",
                symbol,
                fy_record.end_date,
            )
            return None
        target_currency = require_metric_ticker_currency(
            symbol,
            repo,
            metric_id=self.id,
            input_name="EarningsPerShare",
            as_of=fy_record.end_date,
        )
        money = require_metric_money(
            fy_record.money,
            target_currency=target_currency,
            metric_id=self.id,
            symbol=symbol,
            input_name="EarningsPerShare",
            as_of=fy_record.end_date,
        )
        return money, fy_record.end_date

    def _latest_quarters(
        self, symbol: str, repo: RegionFactsRepository
    ) -> list[MonetaryFact]:
        return latest_quarterly_records(
            repo.monetary_facts_for_concept, symbol, EPS_CONCEPTS, periods=4
        )

    def _latest_fy_eps(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[MonetaryFact]:
        records = repo.monetary_facts_for_concept(
            symbol, "EarningsPerShare", fiscal_period="FY", limit=1
        )
        if records:
            return records[0]
        return None

    def _latest_snapshot(
        self, market_repo: MarketDataRepository, symbol: str
    ) -> Optional[PriceData]:
        if hasattr(market_repo, "latest_snapshot"):
            snapshot = market_repo.latest_snapshot(symbol)
            if snapshot:
                return snapshot
        if hasattr(market_repo, "latest_price"):
            price_entry = market_repo.latest_price(symbol)
            if isinstance(price_entry, PriceData):
                return price_entry
            if isinstance(price_entry, tuple) and len(price_entry) >= 2:
                as_of, price = price_entry[0], price_entry[1]
                return PriceData(symbol=symbol, price=price, as_of=as_of)
        return None
