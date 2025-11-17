"""Facade for fetching and storing market data.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Union

from pyvalue.config import Config
from pyvalue.marketdata import AlphaVantageProvider, MarketDataProvider, PriceData
from pyvalue.storage import MarketDataRepository

LOGGER = logging.getLogger(__name__)


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
        self.provider = provider or self._default_provider()

    def _default_provider(self) -> MarketDataProvider:
        api_key = self.config.alpha_vantage_api_key
        if not api_key:
            raise RuntimeError(
                "Alpha Vantage API key missing. Set in private/config.toml under [alpha_vantage]."
            )
        return AlphaVantageProvider(api_key=api_key)

    def refresh_symbol(self, symbol: str) -> PriceData:
        data = self.provider.latest_price(symbol)
        self.repo.upsert_price(symbol=data.symbol, as_of=data.as_of, price=data.price, volume=data.volume, currency=data.currency, market_cap=data.market_cap)
        LOGGER.info("Stored market data for %s at %s", data.symbol, data.as_of)
        return data


__all__ = ["MarketDataService"]
