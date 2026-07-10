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
from pyvalue.metrics.share_resolver import (
    SHARE_RESOLVER_REQUIRED_CONCEPTS,
    resolve_current_share_count,
)
from pyvalue.metrics.ttm import TTMWindow, resolve_ttm_window
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
EQUITY_CONCEPTS = ["StockholdersEquity"]
GOODWILL_CONCEPTS = ["Goodwill"]
INTANGIBLE_CONCEPTS = ["IntangibleAssetsNetExcludingGoodwill"]

LOGGER = logging.getLogger(__name__)


@dataclass
class GrahamMultiplierMetric:
    id: str = "graham_multiplier"
    required_concepts = tuple(
        EPS_CONCEPTS
        + EQUITY_CONCEPTS
        + list(SHARE_RESOLVER_REQUIRED_CONCEPTS)
        + GOODWILL_CONCEPTS
        + INTANGIBLE_CONCEPTS
    )
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        # Resolve the listing currency once; every monetary input (EPS, equity,
        # goodwill, intangibles, price) is aligned to it so the price/EPS and
        # price/TBVPS ratios are currency-safe. Shares are a dimensionless count.
        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            market_repo,
            metric_id=self.id,
            input_name="EarningsPerShare",
        )

        eps = self._ttm_eps(listing_id, repo, target_currency)
        if eps is None:
            LOGGER.warning(
                "graham_multiplier: missing EPS quarters for listing_id=%s", listing_id
            )
            return None
        ttm_eps_money, eps_as_of = eps
        if ttm_eps_money.amount <= 0:
            LOGGER.warning(
                "graham_multiplier: non-positive TTM EPS for listing_id=%s", listing_id
            )
            return None

        equity = self._latest_monetary(
            listing_id, repo, EQUITY_CONCEPTS, target_currency
        )
        shares = self._latest_shares(listing_id, repo, market_repo)
        if equity is None or shares is None or shares <= 0:
            LOGGER.warning(
                "graham_multiplier: equity/shares missing for listing_id=%s",
                listing_id,
            )
            return None

        goodwill = self._latest_optional_monetary(
            listing_id, repo, GOODWILL_CONCEPTS, target_currency
        )
        intangibles = self._latest_optional_monetary(
            listing_id, repo, INTANGIBLE_CONCEPTS, target_currency
        )

        price_data = self._latest_snapshot(market_repo, listing_id)
        if price_data is None or price_data.price is None:
            LOGGER.warning(
                "graham_multiplier: missing price for listing_id=%s", listing_id
            )
            return None
        price_money = require_metric_amount_money(
            price_data.price,
            price_data.currency,
            target_currency=target_currency,
            metric_id=self.id,
            listing_id=listing_id,
            input_name="price",
            as_of=price_data.as_of,
        )
        if price_money.amount <= 0:
            LOGGER.warning(
                "graham_multiplier: non-positive price for listing_id=%s", listing_id
            )
            return None

        # Tangible book value per share (per-share money).
        tbvps = (equity - goodwill - intangibles) / shares
        if tbvps.amount <= 0:
            LOGGER.warning(
                "graham_multiplier: non-positive TBVPS for listing_id=%s", listing_id
            )
            return None

        multiplier = (price_money / ttm_eps_money) * (price_money / tbvps)
        return MetricResult(
            listing_id=listing_id, metric_id=self.id, value=multiplier, as_of=eps_as_of
        )

    def _ttm_eps(
        self, listing_id: int, repo: RegionFactsRepository, target_currency: str
    ) -> Optional[tuple[Money, str]]:
        window = self._resolve_eps_window(listing_id, repo)
        if window is not None:
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
        if fy_record is None or not is_recent_fact(fy_record):
            return None
        money = require_metric_money(
            fy_record.money,
            target_currency=target_currency,
            metric_id=self.id,
            listing_id=listing_id,
            input_name="EarningsPerShare",
            as_of=fy_record.end_date,
        )
        return money, fy_record.end_date

    def _resolve_eps_window(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[TTMWindow[MonetaryFact]]:
        """Return the first EPS concept's trailing-twelve-month window, or None.

        Keeps the legacy concept-fallback iteration. The failure reason is not
        threaded further because every failure funnels into the same FY-EPS
        fallback in ``_ttm_eps``; ``compute`` logs the metric-level miss only
        when both paths fail, exactly as before the window refactor.
        """

        for concept in EPS_CONCEPTS:
            resolution = resolve_ttm_window(
                repo.monetary_facts_for_concept(listing_id, concept)
            )
            if resolution.window is not None:
                return resolution.window
        return None

    def _latest_fy_eps(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MonetaryFact]:
        records = repo.monetary_facts_for_concept(
            listing_id, "EarningsPerShare", fiscal_period="FY", limit=1
        )
        if records:
            return records[0]
        return None

    def _latest_monetary(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: list[str],
        target_currency: str,
    ) -> Optional[Money]:
        for concept in concepts:
            fact = repo.latest_monetary_fact(listing_id, concept)
            if fact is None or not is_recent_fact(fact):
                continue
            return require_metric_money(
                fact.money,
                target_currency=target_currency,
                metric_id=self.id,
                listing_id=listing_id,
                input_name=concept,
                as_of=fact.end_date,
            )
        return None

    def _latest_optional_monetary(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: list[str],
        target_currency: str,
    ) -> Money:
        money = self._latest_monetary(listing_id, repo, concepts, target_currency)
        return money if money is not None else Money.of(0.0, target_currency)

    def _latest_shares(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[float]:
        record = resolve_current_share_count(listing_id, repo, market_repo)
        if record is None or not is_recent_fact(record):
            return None
        return record.value

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
