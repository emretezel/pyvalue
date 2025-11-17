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
        params = {"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": self.api_key}
        response = self.session.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if "Global Quote" not in payload:
            raise ValueError(f"Unexpected Alpha Vantage response: {payload}")
        quote = payload["Global Quote"]
        price = float(quote["05. price"])
        as_of = quote.get("07. latest trading day") or quote.get("latestTradingDay")
        volume = int(quote.get("06. volume", 0) or 0) or None
        currency = None  # Alpha Vantage does not return currency for US stocks.
        return PriceData(symbol=symbol.upper(), price=price, as_of=as_of, volume=volume, currency=currency)
