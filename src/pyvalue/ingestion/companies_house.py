"""Companies House ingestion helpers for UK company data.

Author: Emre Tezel
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Dict, Optional

import requests

LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30


class CompaniesHouseClient:
    """Download company profiles from Companies House."""

    def __init__(
        self,
        api_key: str,
        session: Optional[requests.Session] = None,
        base_url: str = "https://api.company-information.service.gov.uk",
        fetcher: Optional[callable] = None,
    ) -> None:
        if not api_key:
            raise ValueError("Companies House API key is required")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self._custom_fetcher = fetcher

        # Companies House uses HTTP Basic; the key is the username with an empty password.
        credentials = f"{api_key}:".encode()
        token = base64.b64encode(credentials).decode()
        self.session.headers.update({"Authorization": f"Basic {token}"})

    def fetch_company_profile(self, company_number: str, timeout: int = DEFAULT_TIMEOUT) -> Dict:
        """Retrieve the company profile JSON payload."""

        normalized = company_number.strip()
        if not normalized:
            raise ValueError("Company number is required")

        if self._custom_fetcher is not None:
            return self._parse_json(self._custom_fetcher(normalized))

        url = f"{self.base_url}/company/{normalized}"
        LOGGER.info("Downloading Companies House profile for %s", normalized)
        response = self.session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def fetch_filing_history(
        self, company_number: str, category: str = "accounts", items: int = 100, timeout: int = DEFAULT_TIMEOUT
    ) -> Dict:
        """Return filing history JSON for the company."""

        normalized = company_number.strip()
        url = f"{self.base_url}/company/{normalized}/filing-history"
        params = {"category": category, "items_per_page": items}
        LOGGER.info("Downloading filing history for %s", normalized)
        resp = self.session.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def fetch_document_metadata(self, metadata_url: str, timeout: int = DEFAULT_TIMEOUT) -> Dict:
        """Fetch document metadata to locate the XHTML resource link."""

        resp = self.session.get(metadata_url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def fetch_document(self, url: str, timeout: int = DEFAULT_TIMEOUT) -> bytes:
        """Download a Companies House document, preferring XHTML/iXBRL."""

        headers = {"Accept": "application/xhtml+xml, text/html;q=0.9"}
        resp = self.session.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        ctype = resp.headers.get("Content-Type", "").lower()
        if "xhtml" not in ctype and "html" not in ctype:
            raise RuntimeError(f"Document is not XHTML/iXBRL (content-type: {ctype})")
        return resp.content

    @staticmethod
    def _parse_json(raw: str) -> Dict:
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Failed to decode Companies House response") from exc


__all__ = ["CompaniesHouseClient"]
