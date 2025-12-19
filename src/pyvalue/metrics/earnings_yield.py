"""Earnings yield metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import logging

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import latest_quarterly_records
from pyvalue.fx import FXRateStore
from pyvalue.marketdata.base import PriceData
from pyvalue.storage import FinancialFactsRepository, MarketDataRepository

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
        repo: FinancialFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        quarterly_records = self._latest_quarters(symbol, repo)
        if len(quarterly_records) < 4:
            LOGGER.warning("earnings_yield: missing EPS quarters for %s", symbol)
            return None
        ttm_eps = sum(record.value for record in quarterly_records[:4])
        as_of = quarterly_records[0].end_date
        price_data = self._latest_snapshot(market_repo, symbol)
        if price_data is None or price_data.price is None:
            LOGGER.warning("earnings_yield: missing price for %s", symbol)
            return None
        price = price_data.price
        target_currency = self._select_currency(quarterly_records)
        if target_currency and price_data.currency and price_data.currency != target_currency:
            converted = FXRateStore().convert(price, price_data.currency, target_currency, price_data.as_of)
            if converted is None:
                LOGGER.warning(
                    "earnings_yield: FX conversion failed %s -> %s for %s",
                    price_data.currency,
                    target_currency,
                    symbol,
                )
                return None
            price = converted
        if price is None or price <= 0:
            LOGGER.warning("earnings_yield: non-positive price after FX for %s", symbol)
            return None
        yield_value = ttm_eps / price
        return MetricResult(symbol=symbol, metric_id=self.id, value=yield_value, as_of=as_of)

    def _latest_quarters(self, symbol: str, repo: FinancialFactsRepository) -> List:
        return latest_quarterly_records(repo.facts_for_concept, symbol, EPS_CONCEPTS, periods=4)

    def _latest_snapshot(self, market_repo: MarketDataRepository, symbol: str) -> Optional[PriceData]:
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

    def _select_currency(self, records: List) -> Optional[str]:
        for record in records:
            code = getattr(record, "currency", None)
            if code:
                return code
        return None
