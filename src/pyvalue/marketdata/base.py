"""Abstract interfaces for market data providers.

Author: Emre Tezel
"""

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


@dataclass(frozen=True)
class MarketDataUpdate:
    """Prepared market-data row ready for persistence."""

    security_id: int
    symbol: str
    as_of: str
    price: float
    volume: Optional[int] = None
    market_cap: Optional[float] = None
    currency: Optional[str] = None
    source_provider: str = "EODHD"


class MarketDataProvider(Protocol):
    """Protocol for fetching latest price/market data."""

    def latest_price(self, symbol: str) -> PriceData: ...
