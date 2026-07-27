"""Abstract interfaces for market data providers.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass(frozen=True)
class PriceQuote:
    """One raw price observation exactly as a provider reported it.

    Deliberately carries **no currency**. EODHD's EOD endpoints (``/api/eod``
    and ``/api/eod-bulk-last-day``) return OHLCV and a date -- no currency field
    on either, verified against the live API. The quote unit is a property of
    the *listing* (``listing.currency``, NOT NULL), not of the price row, so a
    price parser has no information with which to name one.

    The field is absent rather than ``Optional`` on purpose: a parser that
    cannot express a currency cannot invent one. Prices used to be scaled by a
    currency derived from the exchange suffix and the price magnitude, which
    stored every non-pence London listing at 1/100 of its real value; making the
    guess unrepresentable is what stops it recurring.
    """

    symbol: str
    price: float
    as_of: str
    volume: Optional[int] = None


@dataclass
class PriceData:
    """A price already resolved to its major currency.

    Produced by ``marketdata.service.prepare_price_data`` and by the snapshot
    readers in ``persistence.storage.metrics_market``. Unlike
    :class:`PriceQuote` this *does* carry a currency, because by this point one
    is known: it is always ``listing.currency`` collapsed to its major unit
    (GBX -> GBP), never a subunit and never derived from anything else.
    """

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

    def latest_price(self, symbol: str) -> PriceQuote: ...
