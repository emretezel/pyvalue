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
    """Download and normalize the UK equity universe from EODHD."""

    #: Equity security types we keep by default (ETFs handled separately).
    ALLOWED_TYPES = {"Common Stock", "Preferred Stock"}

    def __init__(
        self,
        api_key: Optional[str],
        exchange_code: str = "LSE",
        fetcher: Optional[Callable[[str], str]] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        if api_key is None and fetcher is None:
            raise ValueError("EODHD API key required unless a custom fetcher is provided")

        self.api_key = api_key
        self.exchange_code = exchange_code
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
        params = {"api_token": self.api_key, "fmt": "json"}
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

        sec_type = (row.get("Type") or "").strip()
        is_etf = sec_type.upper() == "ETF"
        if not is_etf and sec_type not in self.ALLOWED_TYPES:
            return None

        name = (row.get("Name") or "").strip()
        exchange = (row.get("Exchange") or self.exchange_code or "LSE").strip()

        isin = (row.get("ISIN") or row.get("Isin") or row.get("isin") or "").strip() or None

        return Listing(
            symbol=symbol,
            security_name=name,
            exchange=exchange,
            market_category=exchange,
            is_etf=is_etf,
            is_test_issue=False,
            status=None,
            round_lot_size=None,
            source="eodhd",
            isin=isin,
        )


__all__ = ["UKUniverseLoader"]
