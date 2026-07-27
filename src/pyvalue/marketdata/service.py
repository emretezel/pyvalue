"""Facade for fetching and storing market data.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

from pyvalue.config import Config
from pyvalue.currency import normalize_monetary_amount
from pyvalue.marketdata import (
    EODHDProvider,
    MarketDataProvider,
    MarketDataUpdate,
    PriceData,
    PriceQuote,
)
from pyvalue.persistence.storage import (
    MarketDataRepository,
)


def prepare_price_data(
    symbol: str,
    quote: PriceQuote,
    listing_currency: str,
) -> PriceData:
    """Resolve a raw provider quote into a major-currency ``PriceData`` row.

    ``listing_currency`` is ``listing.currency`` -- the **only** source of a
    price's currency, and required rather than optional because there is
    nothing to fall back to. It is NOT NULL in the schema and constrained to
    three uppercase letters, and it carries the quote unit verbatim, subunits
    included (GBX pence, ZAC cents, ILA agorot). The collapse to the major unit
    is then a registry lookup in :func:`normalize_monetary_amount`, so a GBX
    listing stores pounds and a USD listing stores dollars untouched.

    The provider is deliberately not consulted. It used to supply a currency
    it derived from the exchange suffix and the price magnitude, and because
    that value was tried *first* it silently overrode this one -- storing every
    non-pence London listing at 1/100 of its real value. A price row is a
    number and a date; its unit belongs to the listing.

    No anomaly guard runs here. Market value is derived on demand as the latest
    share-count fact times the latest price (``metrics.utils.market_cap_money``),
    so the price stored here is just the latest observation and there is no
    cross-snapshot value jump to police.
    """

    major_amount, major_currency = normalize_monetary_amount(
        quote.price, listing_currency
    )
    return PriceData(
        symbol=symbol.upper(),
        price=float(major_amount) if major_amount is not None else quote.price,
        as_of=quote.as_of,
        volume=quote.volume,
        currency=major_currency,
    )


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
        api_key = self.config.eodhd_api_key
        if api_key:
            return EODHDProvider(api_key=api_key)
        raise RuntimeError(
            "No market data API key configured. Set eodhd.api_key in private/config.toml."
        )

    def persist_updates(self, updates: list[MarketDataUpdate]) -> None:
        self.repo.upsert_prices(updates)


__all__ = [
    "MarketDataService",
    "prepare_price_data",
]
