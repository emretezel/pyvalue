"""Market data providers and interfaces.

Author: Emre Tezel
"""

from .base import PriceData, MarketDataProvider
from .eodhd import EODHDProvider

__all__ = ["PriceData", "MarketDataProvider", "EODHDProvider"]
