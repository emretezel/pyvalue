"""EODHD fundamentals ingestion helpers.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Dict, List, Optional

import requests

LOGGER = logging.getLogger(__name__)

BASE_URL = "https://eodhd.com/api"
DEFAULT_TIMEOUT = 30


@dataclass(frozen=True)
class ExchangeSymbol:
    code: str
    name: str
    operating_mic: Optional[str] = None


class EODHDFundamentalsClient:
    """Download fundamentals and exchange metadata from EODHD."""

    def __init__(self, api_key: str, session: Optional[requests.Session] = None) -> None:
        if not api_key:
            raise ValueError("EODHD API key is required")
        self.api_key = api_key
        self.session = session or requests.Session()

    def list_exchanges(self, timeout: int = DEFAULT_TIMEOUT) -> List[Dict]:
        url = f"{BASE_URL}/exchanges-list/"
        params = {"api_token": self.api_key, "fmt": "json"}
        response = self.session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError(f"Unexpected EODHD exchange response: {payload}")
        return payload

    def exchange_metadata(self, code: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[Dict]:
        payload = self.list_exchanges(timeout=timeout)
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            if (entry.get("Code") or "").upper() == code.upper():
                return entry
        return None

    def list_symbols(self, exchange_code: str, timeout: int = DEFAULT_TIMEOUT) -> List[Dict]:
        code = exchange_code.upper()
        url = f"{BASE_URL}/exchange-symbol-list/{code}"
        params = {
            "api_token": self.api_key,
            "fmt": "json",
            "delisted": "0",
            "type": "stock",
        }
        response = self.session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError(f"Unexpected EODHD symbols response for {code}: {payload}")
        return payload

    def fetch_fundamentals(
        self,
        symbol: str,
        exchange_code: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> Dict:
        ticker = self._format_symbol(symbol, exchange_code)
        url = f"{BASE_URL}/fundamentals/{ticker}"
        params = {"api_token": self.api_key, "fmt": "json"}
        response = self.session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected EODHD fundamentals response for {ticker}: {payload}")
        return payload

    def _format_symbol(self, symbol: str, exchange_code: Optional[str]) -> str:
        cleaned = symbol.strip().upper()
        if "." in cleaned:
            return cleaned
        if exchange_code:
            return f"{cleaned}.{exchange_code.upper()}"
        return cleaned


__all__ = ["EODHDFundamentalsClient", "ExchangeSymbol"]
