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

    @staticmethod
    def _parse_json(raw: str) -> Dict:
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Failed to decode Companies House response") from exc


__all__ = ["CompaniesHouseClient"]
