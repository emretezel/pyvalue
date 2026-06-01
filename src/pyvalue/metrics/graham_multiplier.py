"""Graham multiplier metric implementation.

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
from pyvalue.storage import MarketDataRepository

EPS_CONCEPTS = ["EarningsPerShare"]
EQUITY_CONCEPTS = ["StockholdersEquity"]
SHARE_CONCEPTS = ["CommonStockSharesOutstanding"]
GOODWILL_CONCEPTS = ["Goodwill"]
INTANGIBLE_CONCEPTS = ["IntangibleAssetsNetExcludingGoodwill"]

LOGGER = logging.getLogger(__name__)


@dataclass
class GrahamMultiplierMetric:
    id: str = "graham_multiplier"
    required_concepts = tuple(
        EPS_CONCEPTS
        + EQUITY_CONCEPTS
        + SHARE_CONCEPTS
        + GOODWILL_CONCEPTS
        + INTANGIBLE_CONCEPTS
    )
    uses_market_data = True

    def compute(
        self,
        symbol: str,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        # Resolve the listing currency once; every monetary input (EPS, equity,
        # goodwill, intangibles, price) is aligned to it so the price/EPS and
        # price/TBVPS ratios are currency-safe. Shares are a dimensionless count.
        target_currency = require_metric_ticker_currency(
            symbol,
            repo,
            market_repo,
            metric_id=self.id,
            input_name="EarningsPerShare",
        )

        eps = self._ttm_eps(symbol, repo, target_currency)
        if eps is None:
            LOGGER.warning("graham_multiplier: missing EPS quarters for %s", symbol)
            return None
        ttm_eps_money, eps_as_of = eps
        if ttm_eps_money.amount <= 0:
            LOGGER.warning("graham_multiplier: non-positive TTM EPS for %s", symbol)
            return None

        equity = self._latest_monetary(symbol, repo, EQUITY_CONCEPTS, target_currency)
        shares = self._latest_shares(symbol, repo)
        if equity is None or shares is None or shares <= 0:
            LOGGER.warning("graham_multiplier: equity/shares missing for %s", symbol)
            return None

        goodwill = self._latest_optional_monetary(
            symbol, repo, GOODWILL_CONCEPTS, target_currency
        )
        intangibles = self._latest_optional_monetary(
            symbol, repo, INTANGIBLE_CONCEPTS, target_currency
        )

        price_data = self._latest_snapshot(market_repo, symbol)
        if price_data is None or price_data.price is None:
            LOGGER.warning("graham_multiplier: missing price for %s", symbol)
            return None
        price_money = require_metric_amount_money(
            price_data.price,
            price_data.currency,
            target_currency=target_currency,
            metric_id=self.id,
            symbol=symbol,
            input_name="price",
            as_of=price_data.as_of,
        )
        if price_money.amount <= 0:
            LOGGER.warning("graham_multiplier: non-positive price for %s", symbol)
            return None

        # Tangible book value per share (per-share money).
        tbvps = (equity - goodwill - intangibles) / shares
        if tbvps.amount <= 0:
            LOGGER.warning("graham_multiplier: non-positive TBVPS for %s", symbol)
            return None

        multiplier = (price_money / ttm_eps_money) * (price_money / tbvps)
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=multiplier, as_of=eps_as_of
        )

    def _ttm_eps(
        self, symbol: str, repo: RegionFactsRepository, target_currency: str
    ) -> Optional[tuple[Money, str]]:
        eps_records = self._latest_quarters(symbol, repo)
        if len(eps_records) >= 4:
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
                    for record in eps_records[:4]
                ]
            )
            return ttm, eps_records[0].end_date

        fy_record = self._latest_fy_eps(symbol, repo)
        if fy_record is None or not is_recent_fact(fy_record):
            return None
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

    def _latest_monetary(
        self,
        symbol: str,
        repo: RegionFactsRepository,
        concepts: list[str],
        target_currency: str,
    ) -> Optional[Money]:
        for concept in concepts:
            fact = repo.latest_monetary_fact(symbol, concept)
            if fact is None or not is_recent_fact(fact):
                continue
            return require_metric_money(
                fact.money,
                target_currency=target_currency,
                metric_id=self.id,
                symbol=symbol,
                input_name=concept,
                as_of=fact.end_date,
            )
        return None

    def _latest_optional_monetary(
        self,
        symbol: str,
        repo: RegionFactsRepository,
        concepts: list[str],
        target_currency: str,
    ) -> Money:
        money = self._latest_monetary(symbol, repo, concepts, target_currency)
        return money if money is not None else Money.of(0.0, target_currency)

    def _latest_shares(
        self, symbol: str, repo: RegionFactsRepository
    ) -> Optional[float]:
        for concept in SHARE_CONCEPTS:
            fact = repo.latest_scalar_fact(symbol, concept)
            if fact is None or not is_recent_fact(fact):
                continue
            return fact.value
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
