"""EODHD market data provider implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta
import logging
from typing import Dict, Optional

import requests  # type: ignore[import-untyped]

from pyvalue.currency import (
    is_gbx_subunit_currency,
    normalize_currency_code,
    normalize_monetary_amount,
)
from pyvalue.marketdata.base import MarketDataProvider, PriceData

LOGGER = logging.getLogger(__name__)

API_URL = "https://eodhd.com/api/eod"
BULK_LAST_DAY_URL = "https://eodhd.com/api/eod-bulk-last-day"
SINGLE_SYMBOL_LOOKBACK_DAYS = 30


class EODHDProvider(MarketDataProvider):
    """Fetch latest EOD price data from the EODHD API."""

    def __init__(
        self, api_key: str, session: Optional[requests.Session] = None
    ) -> None:
        if not api_key:
            raise ValueError("EODHD API key is required")
        self.api_key = api_key
        self.session = session or requests.Session()

    def latest_price(self, symbol: str) -> PriceData:
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
        return self._price_data_from_entry(symbol.upper(), payload[-1], ticker)

    def latest_prices_for_exchange(self, exchange_code: str) -> Dict[str, PriceData]:
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
        prices: Dict[str, PriceData] = {}
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
                prices[symbol] = self._price_data_from_entry(
                    symbol, entry, exchange_norm
                )
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

    def _price_data_from_entry(
        self,
        symbol: str,
        entry,
        exchange_hint: Optional[str] = None,
    ) -> PriceData:
        price = None
        for key in ("Close", "close", "adjusted_close", "Adjusted_Close", "price"):
            price = self._extract_float(entry, key)
            if price is not None:
                break
        if price is None:
            raise ValueError(
                f"Missing Close price in EODHD response for {symbol}: {entry}"
            )
        currency = self._extract_text(entry, "currency", "Currency")
        currency = normalize_currency_code(currency)
        suffix = exchange_hint or (symbol.split(".")[-1] if "." in symbol else "")
        if "." in suffix:
            suffix = suffix.split(".")[-1]
        suffix = suffix.upper()
        gbx_hint = suffix in {"LSE", "LON", "XLON"}
        if is_gbx_subunit_currency(self._extract_text(entry, "currency", "Currency")):
            normalized_price, normalized_currency = normalize_monetary_amount(
                price,
                self._extract_text(entry, "currency", "Currency"),
            )
            if normalized_price is not None:
                price = float(normalized_price)
            currency = normalized_currency
        elif gbx_hint and currency is None and price and price > 100:
            price = price / 100.0
            currency = "GBP"
        as_of = self._extract_text(entry, "date", "Date")
        if as_of is None:
            raise ValueError(f"Missing date in EODHD response for {symbol}: {entry}")
        volume = self._extract_int(entry, "Volume")
        if volume is None:
            volume = self._extract_int(entry, "volume")
        return PriceData(
            symbol=symbol.upper(),
            price=price,
            as_of=as_of,
            volume=volume,
            currency=currency,
        )

    def _extract_float(self, entry, key: str) -> Optional[float]:
        value = entry.get(key)
        if value is None and key.lower() != key:
            value = entry.get(key.lower())
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            LOGGER.warning("Invalid float value for %s: %s", key, value)
            return None

    def _extract_int(self, entry, key: str) -> Optional[int]:
        value = entry.get(key)
        if value is None and key.lower() != key:
            value = entry.get(key.lower())
        if value is None:
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            LOGGER.warning("Invalid integer value for %s: %s", key, value)
            return None

    def _extract_text(self, entry, *keys: str) -> Optional[str]:
        for key in keys:
            value = entry.get(key)
            if value is None and key.lower() != key:
                value = entry.get(key.lower())
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text.upper() if key.lower() == "currency" else text
        return None


__all__ = ["EODHDProvider"]
