"""Price-to-book and price-to-tangible-book valuation metrics.

Book value here means the *common* shareholders' claim: the derived
``CommonStockholdersEquity`` (= StockholdersEquity - PreferredStock - NCI)
is preferred, falling back to total ``StockholdersEquity`` when the derived
concept is absent. Graham's price-to-book criterion and the academic HML
value factor both price the common equity, not preferred capital.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    is_recent_fact,
    require_metric_amount_money,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money
from pyvalue.persistence.storage import MarketDataRepository

LOGGER = logging.getLogger(__name__)

# Fallback order matters: the derived common-equity concept is the correct
# numerator; total stockholders' equity is only a rescue when the derivation
# is unavailable. Sign guards apply *after* selection -- a present-but-negative
# common equity must not silently fall through to the total.
EQUITY_CONCEPTS = ("CommonStockholdersEquity", "StockholdersEquity")
SHARE_CONCEPTS = ("CommonStockSharesOutstanding",)
GOODWILL_CONCEPTS = ("Goodwill",)
INTANGIBLE_CONCEPTS = ("IntangibleAssetsNetExcludingGoodwill",)


@dataclass(frozen=True)
class _BookValueInputs:
    """Currency-aligned inputs shared by both price-to-book style ratios."""

    equity: Money
    goodwill: Money
    intangibles: Money
    shares: float
    price: Money
    # The equity fact's end_date: the balance-sheet date the ratio prices.
    as_of: str

    @property
    def tangible_equity(self) -> Money:
        return self.equity - self.goodwill - self.intangibles


class PriceToBookCalculator:
    """Resolve the shared inputs for price-to-book style ratios."""

    def resolve_inputs(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
        *,
        context: str,
    ) -> Optional[_BookValueInputs]:
        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            market_repo,
            metric_id=context,
            input_name=EQUITY_CONCEPTS[0],
        )

        equity = self._latest_monetary(
            listing_id, repo, EQUITY_CONCEPTS, target_currency, context
        )
        if equity is None:
            LOGGER.warning("%s: missing equity for listing_id=%s", context, listing_id)
            return None
        equity_money, equity_as_of = equity

        shares = self._latest_shares(listing_id, repo)
        if shares is None or shares <= 0:
            LOGGER.warning(
                "%s: missing/non-positive shares for listing_id=%s",
                context,
                listing_id,
            )
            return None

        # Goodwill and intangibles legitimately absent for many issuers, so a
        # missing fact means zero rather than "cannot compute".
        goodwill = self._latest_optional_monetary(
            listing_id, repo, GOODWILL_CONCEPTS, target_currency, context
        )
        intangibles = self._latest_optional_monetary(
            listing_id, repo, INTANGIBLE_CONCEPTS, target_currency, context
        )

        price_data = market_repo.latest_snapshot_by_id(listing_id)
        if price_data is None or price_data.price is None:
            LOGGER.warning("%s: missing price for listing_id=%s", context, listing_id)
            return None
        price_money = require_metric_amount_money(
            price_data.price,
            price_data.currency,
            target_currency=target_currency,
            metric_id=context,
            listing_id=listing_id,
            input_name="price",
            as_of=price_data.as_of,
        )
        if price_money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive price for listing_id=%s", context, listing_id
            )
            return None

        return _BookValueInputs(
            equity=equity_money,
            goodwill=goodwill,
            intangibles=intangibles,
            shares=shares,
            price=price_money,
            as_of=equity_as_of,
        )

    def _latest_monetary(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        target_currency: str,
        context: str,
    ) -> Optional[tuple[Money, str]]:
        for concept in concepts:
            record = repo.latest_monetary_fact(listing_id, concept)
            if record is None or not is_recent_fact(record):
                continue
            money = require_metric_money(
                record.money,
                target_currency=target_currency,
                metric_id=context,
                listing_id=listing_id,
                input_name=concept,
                as_of=record.end_date,
            )
            return money, record.end_date
        return None

    def _latest_optional_monetary(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concepts: Sequence[str],
        target_currency: str,
        context: str,
    ) -> Money:
        resolved = self._latest_monetary(
            listing_id, repo, concepts, target_currency, context
        )
        if resolved is None:
            return Money.of(0.0, target_currency)
        return resolved[0]

    def _latest_shares(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[float]:
        for concept in SHARE_CONCEPTS:
            record = repo.latest_scalar_fact(listing_id, concept)
            if record is None or not is_recent_fact(record):
                continue
            return record.value
        return None


@dataclass
class PriceToBookMetric:
    """Compute price divided by common book value per share (P/B)."""

    id: str = "price_to_book"
    required_concepts = EQUITY_CONCEPTS + SHARE_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        inputs = PriceToBookCalculator().resolve_inputs(
            listing_id, repo, market_repo, context=self.id
        )
        if inputs is None:
            return None
        if inputs.equity.amount <= 0:
            # Negative book value makes the multiple meaningless for screening
            # (a "cheap" negative P/B would rank above a genuinely cheap one).
            LOGGER.warning(
                "%s: non-positive book value for listing_id=%s", self.id, listing_id
            )
            return None

        bvps = inputs.equity / inputs.shares
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=inputs.price / bvps,
            as_of=inputs.as_of,
        )


@dataclass
class PriceToTangibleBookMetric:
    """Compute price divided by tangible common book value per share (P/TB)."""

    id: str = "price_to_tangible_book"
    required_concepts = (
        EQUITY_CONCEPTS + SHARE_CONCEPTS + GOODWILL_CONCEPTS + INTANGIBLE_CONCEPTS
    )
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        inputs = PriceToBookCalculator().resolve_inputs(
            listing_id, repo, market_repo, context=self.id
        )
        if inputs is None:
            return None
        tangible_equity = inputs.tangible_equity
        if tangible_equity.amount <= 0:
            # Common for acquisitive issuers: goodwill exceeds book equity.
            # There is no tangible cushion to price, so no multiple is emitted.
            LOGGER.warning(
                "%s: non-positive tangible book value for listing_id=%s",
                self.id,
                listing_id,
            )
            return None

        tbvps = tangible_equity / inputs.shares
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=inputs.price / tbvps,
            as_of=inputs.as_of,
        )


__all__ = ["PriceToBookMetric", "PriceToTangibleBookMetric"]
