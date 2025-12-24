"""Universe loader for UK-listed equities using EODHD exchange symbol list.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Callable, Dict, List, Mapping, Optional, Sequence
import json
import logging

import requests

from pyvalue.universe.us import Listing

LOGGER = logging.getLogger(__name__)


class UKUniverseLoader:
    """Download and normalize an equity universe from EODHD."""

    #: Equity security types we keep by default (ETFs handled separately).
    DEFAULT_ALLOWED_TYPES = {"Common Stock"}

    def __init__(
        self,
        api_key: Optional[str],
        exchange_code: str = "LSE",
        include_etfs: bool = False,
        allowed_currencies: Optional[Sequence[str]] = None,
        include_exchanges: Optional[Sequence[str]] = None,
        allowed_types: Optional[Sequence[str]] = None,
        fetcher: Optional[Callable[[str], str]] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        if api_key is None and fetcher is None:
            raise ValueError("EODHD API key required unless a custom fetcher is provided")

        self.api_key = api_key
        self.exchange_code = exchange_code
        self.include_etfs = include_etfs
        self.allowed_types = set(allowed_types or self.DEFAULT_ALLOWED_TYPES)
        self.allowed_currencies = (
            {code.upper() for code in allowed_currencies} if allowed_currencies else None
        )
        self.include_exchanges = (
            {code.upper() for code in include_exchanges} if include_exchanges else None
        )
        self._custom_fetcher = fetcher
        self.session = session or requests.Session()

    def load(self) -> List[Listing]:
        """Return the consolidated list of UK listings."""

        rows = self._download_and_parse()
        listings: Dict[str, Listing] = {}

        for row in rows:
            listing = self._row_to_listing(row)
            if listing is None:
                continue
            listings[listing.symbol] = listing

        LOGGER.info("Loaded %s symbols from EODHD %s feed", len(listings), self.exchange_code)
        return sorted(listings.values(), key=lambda l: l.symbol)

    def _download_and_parse(self) -> List[Mapping[str, str]]:
        LOGGER.debug("Fetching EODHD exchange symbol list for %s", self.exchange_code)
        body = self._fetch()
        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Failed to decode EODHD response") from exc

        if not isinstance(data, list):
            raise RuntimeError("Unexpected EODHD response format")

        rows: List[Mapping[str, str]] = []
        for item in data:
            if not isinstance(item, Mapping):
                continue
            rows.append({k: str(v) if v is not None else "" for k, v in item.items()})
        return rows

    def _fetch(self) -> str:
        if self._custom_fetcher is not None:
            return self._custom_fetcher(self.exchange_code)

        url = f"https://eodhd.com/api/exchange-symbol-list/{self.exchange_code}"
        params = {
            "api_token": self.api_key,
            "fmt": "json",
            "delisted": "0",
            "type": "stock",
        }
        response = self.session.get(url, params=params, timeout=30)
        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"EODHD request failed: {exc}") from exc
        return response.text

    def _row_to_listing(self, row: Mapping[str, str]) -> Optional[Listing]:
        symbol = (row.get("Code") or "").strip()
        if not symbol:
            return None

        if self.include_exchanges is not None:
            row_exchange = (row.get("Exchange") or row.get("exchange") or "").strip().upper()
            if not row_exchange or row_exchange not in self.include_exchanges:
                return None

        sec_type = (row.get("Type") or "").strip()
        sec_type_upper = sec_type.upper()
        is_etf = sec_type_upper == "ETF"
        if is_etf:
            if not self.include_etfs:
                return None
        elif sec_type not in self.allowed_types:
            return None

        name = (row.get("Name") or "").strip()
        exchange = (self.exchange_code or "").strip().upper()
        if not exchange:
            exchange = (row.get("Exchange") or "").strip().upper()
        if not exchange:
            exchange = "LSE"

        isin = (row.get("ISIN") or row.get("Isin") or row.get("isin") or "").strip() or None

        qualified = symbol if "." in symbol else f"{symbol}.{self.exchange_code.upper()}"

        currency = (row.get("Currency") or row.get("currency") or "").strip() or None
        if currency:
            currency = currency.upper()
        if self.allowed_currencies is not None:
            if not currency or currency not in self.allowed_currencies:
                return None

        return Listing(
            symbol=qualified,
            security_name=name,
            exchange=exchange,
            market_category=exchange,
            is_etf=is_etf,
            is_test_issue=False,
            status=None,
            round_lot_size=None,
            source="eodhd",
            isin=isin,
            currency=currency,
        )


__all__ = ["UKUniverseLoader"]
