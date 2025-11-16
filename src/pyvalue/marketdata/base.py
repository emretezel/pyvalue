# Author: Emre Tezel
"""Abstract interfaces for market data providers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass
class PriceData:
    symbol: str
    price: float
    as_of: str
    currency: Optional[str] = None
    volume: Optional[int] = None
    market_cap: Optional[float] = None


class MarketDataProvider(Protocol):
    """Protocol for fetching latest price/market data."""

    def latest_price(self, symbol: str) -> PriceData:
        ...
