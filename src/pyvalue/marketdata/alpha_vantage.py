"""Alpha Vantage implementation of the MarketDataProvider.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from typing import Optional

import requests

from pyvalue.marketdata.base import MarketDataProvider, PriceData

LOGGER = logging.getLogger(__name__)

API_URL = "https://www.alphavantage.co/query"


class AlphaVantageProvider(MarketDataProvider):
    """Fetch latest price data using Alpha Vantage GLOBAL_QUOTE endpoint."""

    def __init__(self, api_key: str, session: Optional[requests.Session] = None) -> None:
        if not api_key:
            raise ValueError("Alpha Vantage API key is required")
        self.api_key = api_key
        self.session = session or requests.Session()

    def latest_price(self, symbol: str) -> PriceData:
        payload = self._perform_request({"function": "GLOBAL_QUOTE", "symbol": symbol})
        if "Global Quote" not in payload:
            raise ValueError(f"Unexpected Alpha Vantage response: {payload}")
        quote = payload["Global Quote"]
        price = float(quote["05. price"])
        as_of = quote.get("07. latest trading day") or quote.get("latestTradingDay")
        volume = int(quote.get("06. volume", 0) or 0) or None
        currency = None  # Alpha Vantage does not return currency for US stocks.
        market_cap = self._fetch_market_cap(symbol)
        return PriceData(
            symbol=symbol.upper(),
            price=price,
            as_of=as_of,
            volume=volume,
            currency=currency,
            market_cap=market_cap,
        )

    def _perform_request(self, params: dict) -> dict:
        params = {**params, "apikey": self.api_key}
        response = self.session.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def _fetch_market_cap(self, symbol: str) -> Optional[float]:
        try:
            overview = self._perform_request({"function": "OVERVIEW", "symbol": symbol})
        except requests.RequestException as exc:
            LOGGER.warning("Failed to fetch Alpha Vantage overview for %s: %s", symbol, exc)
            return None
        raw_cap = overview.get("MarketCapitalization")
        if not raw_cap:
            return None
        try:
            return float(raw_cap)
        except (TypeError, ValueError):
            LOGGER.warning("Invalid market cap value for %s: %s", symbol, raw_cap)
            return None
