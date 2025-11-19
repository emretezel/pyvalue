"""Facade for fetching and storing market data.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Union

from pyvalue.config import Config
from pyvalue.marketdata import (
    AlphaVantageProvider,
    EODHDProvider,
    MarketDataProvider,
    PriceData,
)
from pyvalue.storage import FinancialFactsRepository, MarketDataRepository

LOGGER = logging.getLogger(__name__)

SHARE_CONCEPTS = [
    "EntityCommonStockSharesOutstanding",
    "CommonStockSharesOutstanding",
]


def latest_share_count(symbol: str, repo: FinancialFactsRepository) -> Optional[float]:
    for concept in SHARE_CONCEPTS:
        fact = repo.latest_fact(symbol, concept)
        if fact is None or fact.value is None:
            continue
        try:
            return float(fact.value)
        except (TypeError, ValueError):
            continue
    return None


class MarketDataService:
    """Coordinates provider selection and persistence of price data."""

    def __init__(
        self,
        db_path: Union[str, Path],
        provider: Optional[MarketDataProvider] = None,
        config: Optional[Config] = None,
    ) -> None:
        self.config = config or Config()
        self.repo = MarketDataRepository(db_path)
        self.repo.initialize_schema()
        self.facts_repo = FinancialFactsRepository(db_path)
        self.facts_repo.initialize_schema()
        self.provider = provider or self._default_provider()

    def _default_provider(self) -> MarketDataProvider:
        api_key = self.config.eodhd_api_key
        if api_key:
            return EODHDProvider(api_key=api_key)
        alpha_key = self.config.alpha_vantage_api_key
        if alpha_key:
            return AlphaVantageProvider(api_key=alpha_key)
        raise RuntimeError(
            "No market data API key configured. Set eodhd.api_key or alpha_vantage.api_key in private/config.toml."
        )

    def refresh_symbol(self, symbol: str) -> PriceData:
        data = self.provider.latest_price(symbol)
        market_cap = data.market_cap
        if market_cap is None:
            shares = latest_share_count(symbol, self.facts_repo)
            if shares is not None and data.price is not None:
                market_cap = shares * data.price
        self.repo.upsert_price(
            symbol=data.symbol,
            as_of=data.as_of,
            price=data.price,
            volume=data.volume,
            currency=data.currency,
            market_cap=market_cap,
        )
        data.market_cap = market_cap
        LOGGER.info("Stored market data for %s at %s", data.symbol, data.as_of)
        return data

__all__ = ["MarketDataService", "latest_share_count"]
