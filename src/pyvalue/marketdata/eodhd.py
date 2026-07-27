"""EODHD market data provider implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, timedelta
import logging
from typing import Dict, Optional

import requests

from pyvalue.marketdata.base import MarketDataProvider, PriceQuote

LOGGER = logging.getLogger(__name__)

API_URL = "https://eodhd.com/api/eod"
BULK_LAST_DAY_URL = "https://eodhd.com/api/eod-bulk-last-day"
SINGLE_SYMBOL_LOOKBACK_DAYS = 30


class EODHDProvider(MarketDataProvider):
    """Fetch latest EOD price data from the EODHD API.

    Reports the number and the date the feed returned, and nothing else. The
    quote currency is *not* this class's to determine: the EOD endpoints carry
    no currency field, and the quote unit belongs to the listing
    (``listing.currency``), which the caller already holds. See
    :class:`~pyvalue.marketdata.base.PriceQuote`.
    """

    def __init__(
        self, api_key: str, session: Optional[requests.Session] = None
    ) -> None:
        if not api_key:
            raise ValueError("EODHD API key is required")
        self.api_key = api_key
        self.session = session or requests.Session()

    def latest_price(self, symbol: str) -> PriceQuote:
        ticker = self._format_symbol(symbol)
        params = {
            "api_token": self.api_key,
            "fmt": "json",
            "from": (
                date.today() - timedelta(days=SINGLE_SYMBOL_LOOKBACK_DAYS)
            ).isoformat(),
        }
        url = f"{API_URL}/{ticker}"
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or not payload:
            raise ValueError(f"Unexpected EODHD response for {symbol}: {payload}")
        return self._quote_from_entry(symbol.upper(), payload[-1])

    def latest_prices_for_exchange(self, exchange_code: str) -> Dict[str, PriceQuote]:
        exchange_norm = exchange_code.strip().upper()
        params = {"api_token": self.api_key, "fmt": "json"}
        url = f"{BULK_LAST_DAY_URL}/{exchange_norm}"
        response = self.session.get(url, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError(
                f"Unexpected EODHD bulk response for {exchange_code}: {payload}"
            )
        prices: Dict[str, PriceQuote] = {}
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            code = self._extract_text(
                entry,
                "code",
                "Code",
                "symbol",
                "Symbol",
                "ticker",
                "Ticker",
            )
            if not code:
                continue
            symbol = self._format_bulk_symbol(code, exchange_norm)
            try:
                prices[symbol] = self._quote_from_entry(symbol, entry)
            except ValueError:
                LOGGER.warning(
                    "Skipping bulk market data row without usable price for %s",
                    symbol,
                )
        return prices

    def _format_symbol(self, symbol: str) -> str:
        if "." in symbol:
            return symbol.upper()
        return f"{symbol.upper()}.US"

    def _format_bulk_symbol(self, code: str, exchange_code: str) -> str:
        normalized = code.strip().upper()
        if "." in normalized:
            return normalized
        return f"{normalized}.{exchange_code}"

    def _quote_from_entry(
        self,
        symbol: str,
        entry: Mapping[str, object],
    ) -> PriceQuote:
        """Parse one EOD row into a :class:`PriceQuote`.

        Reads only the number, the date and the volume. No currency is read or
        derived: both EOD endpoints omit the field entirely, and the quote unit
        is the listing's, resolved by the caller from ``listing.currency``.
        """

        price = None
        for key in ("Close", "close", "adjusted_close", "Adjusted_Close", "price"):
            price = self._extract_float(entry, key)
            if price is not None:
                break
        if price is None:
            raise ValueError(
                f"Missing Close price in EODHD response for {symbol}: {entry}"
            )
        as_of = self._extract_text(entry, "date", "Date")
        if as_of is None:
            raise ValueError(f"Missing date in EODHD response for {symbol}: {entry}")
        volume = self._extract_int(entry, "Volume")
        if volume is None:
            volume = self._extract_int(entry, "volume")
        return PriceQuote(
            symbol=symbol.upper(),
            price=price,
            as_of=as_of,
            volume=volume,
        )

    def _extract_float(self, entry: Mapping[str, object], key: str) -> Optional[float]:
        value = entry.get(key)
        if value is None and key.lower() != key:
            value = entry.get(key.lower())
        # EODHD JSON values are arbitrary objects; only a number or a numeric
        # string can become a float -- anything else (including None) is a
        # missing value here.
        if not isinstance(value, (int, float, str)):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            LOGGER.warning("Invalid float value for %s: %s", key, value)
            return None

    def _extract_int(self, entry: Mapping[str, object], key: str) -> Optional[int]:
        value = entry.get(key)
        if value is None and key.lower() != key:
            value = entry.get(key.lower())
        # See _extract_float: narrow the arbitrary JSON value to the numeric and
        # string forms int(float(...)) can actually parse.
        if not isinstance(value, (int, float, str)):
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            LOGGER.warning("Invalid integer value for %s: %s", key, value)
            return None

    def _extract_text(self, entry: Mapping[str, object], *keys: str) -> Optional[str]:
        for key in keys:
            value = entry.get(key)
            if value is None and key.lower() != key:
                value = entry.get(key.lower())
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None


__all__ = ["EODHDProvider"]
