"""EODHD market data provider implementation.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from typing import Optional

import requests

from pyvalue.marketdata.base import MarketDataProvider, PriceData

LOGGER = logging.getLogger(__name__)

API_URL = "https://eodhd.com/api/eod"


class EODHDProvider(MarketDataProvider):
    """Fetch latest EOD price data from the EODHD API."""

    def __init__(self, api_key: str, session: Optional[requests.Session] = None) -> None:
        if not api_key:
            raise ValueError("EODHD API key is required")
        self.api_key = api_key
        self.session = session or requests.Session()

    def latest_price(self, symbol: str) -> PriceData:
        ticker = self._format_symbol(symbol)
        params = {"api_token": self.api_key, "fmt": "json"}
        url = f"{API_URL}/{ticker}"
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or not payload:
            raise ValueError(f"Unexpected EODHD response for {symbol}: {payload}")
        entry = payload[-1]
        price = self._extract_float(entry, "Close")
        if price is None:
            raise ValueError(f"Missing Close price in EODHD response for {symbol}: {entry}")
        as_of = entry.get("date") or entry.get("Date")
        volume = self._extract_int(entry, "Volume")
        return PriceData(symbol=symbol.upper(), price=price, as_of=as_of, volume=volume, currency=None)

    def _format_symbol(self, symbol: str) -> str:
        if "." in symbol:
            return symbol.upper()
        return f"{symbol.upper()}.US"

    def _extract_float(self, entry, key: str) -> Optional[float]:
        value = entry.get(key) or entry.get(key.lower())
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            LOGGER.warning("Invalid float value for %s: %s", key, value)
            return None

    def _extract_int(self, entry, key: str) -> Optional[int]:
        value = entry.get(key) or entry.get(key.lower())
        if value is None:
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            LOGGER.warning("Invalid integer value for %s: %s", key, value)
            return None


__all__ = ["EODHDProvider"]
