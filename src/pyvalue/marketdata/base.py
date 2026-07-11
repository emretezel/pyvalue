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


@dataclass(frozen=True)
class MarketDataUpdate:
    """Prepared market-data row ready for persistence."""

    security_id: int
    symbol: str
    as_of: str
    price: float
    volume: Optional[int] = None
    currency: Optional[str] = None
    # Provider-layer key for the dual write into ``provider_market_data``,
    # threaded from the market-data eligibility query (which already reads
    # ``provider_listing``). ``None`` means canonical-only: the observation is
    # persisted to ``market_data`` without a provider-layer row (test seeds
    # against uncatalogued fixtures; listings whose provider layer was purged).
    provider_listing_id: Optional[int] = None


class MarketDataProvider(Protocol):
    """Protocol for fetching latest price/market data."""

    def latest_price(self, symbol: str) -> PriceData: ...
