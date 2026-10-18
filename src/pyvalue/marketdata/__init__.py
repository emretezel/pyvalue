"""Market data providers and interfaces.

Author: Emre Tezel
"""

from .base import (
    MarketDataProvider,
    MarketDataReader,
    MarketDataUpdate,
    PriceData,
    PriceQuote,
)
from .eodhd import EODHDProvider

__all__ = [
    "PriceData",
    "PriceQuote",
    "MarketDataUpdate",
    "MarketDataProvider",
    "MarketDataReader",
    "EODHDProvider",
]
