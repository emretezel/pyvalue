"""SEC company-facts ingestion helpers.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
from typing import Dict, Optional

import requests

LOGGER = logging.getLogger(__name__)

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/{cik}.json"
DEFAULT_TIMEOUT = 30


def _format_cik(cik: str) -> str:
    """Return a zero-padded SEC-compliant CIK string."""

    normalized = cik.upper().replace("CIK", "").lstrip("0")
    if not normalized:
        raise ValueError("CIK must contain digits")
    value = int(normalized)
    return f"CIK{value:010d}"


@dataclass
class CompanyInfo:
    """Simple container for ticker metadata."""

    symbol: str
    cik: str
    name: str


class SECCompanyFactsClient:
    """Download SEC company facts payloads and ticker metadata."""

    def __init__(
        self,
        user_agent: Optional[str] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        ua = user_agent or os.environ.get("PYVALUE_SEC_USER_AGENT")
        if not ua:
            raise ValueError(
                "SEC user agent is required. Provide --user-agent or set PYVALUE_SEC_USER_AGENT."
            )
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": ua, "Accept": "application/json"})
        self._ticker_cache: Dict[str, CompanyInfo] = {}

    def resolve_company(self, symbol: str, timeout: int = DEFAULT_TIMEOUT) -> CompanyInfo:
        """Lookup the CIK associated with the given ticker."""

        normalized = symbol.upper().strip()
        if normalized in self._ticker_cache:
            return self._ticker_cache[normalized]

        LOGGER.info("Fetching ticker mapping from SEC")
        response = self.session.get(COMPANY_TICKERS_URL, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        for entry in payload.values():
            ticker = entry.get("ticker", "").upper()
            cik = _format_cik(str(entry.get("cik_str", "0")))
            info = CompanyInfo(symbol=ticker, cik=cik, name=entry.get("title", ""))
            self._ticker_cache[ticker] = info
        if normalized not in self._ticker_cache:
            raise ValueError(f"Ticker {symbol} not found in SEC mapping")
        return self._ticker_cache[normalized]

    def fetch_company_facts(self, cik: str, timeout: int = DEFAULT_TIMEOUT) -> Dict:
        """Retrieve the SEC company facts JSON payload."""

        formatted = _format_cik(cik)
        url = COMPANY_FACTS_URL.format(cik=formatted)
        LOGGER.info("Downloading company facts for %s", formatted)
        response = self.session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()


__all__ = ["SECCompanyFactsClient", "CompanyInfo"]
