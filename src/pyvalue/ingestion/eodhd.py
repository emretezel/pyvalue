"""EODHD fundamentals ingestion helpers.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Dict, Final, List, Optional

import requests

LOGGER = logging.getLogger(__name__)

BASE_URL = "https://eodhd.com/api"
DEFAULT_TIMEOUT = 30

# requests embeds the full request URL -- query string included -- in every
# HTTPError message, so a bare ``raise_for_status()`` leaks the EODHD
# ``api_token`` into tracebacks and log files. Every HTTP error raised from
# this module is therefore routed through :func:`_raise_for_status_sanitized`.
_API_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(r"api_token=[^&\s]+")


def redact_api_token(text: str) -> str:
    """Replace any ``api_token=...`` query fragment in ``text`` with a marker.

    Public because callers that print provider error messages (e.g. the CLI's
    per-exchange warning lines, which also see ``ConnectionError``/``Timeout``
    messages embedding the request URL) reuse it before echoing anything.
    """

    return _API_TOKEN_PATTERN.sub("api_token=REDACTED", text)


class ExchangeNotInPlanError(RuntimeError):
    """An exchange's symbol list returned HTTP 404: not in the EODHD plan.

    EODHD serves ``exchange-symbol-list/{code}`` only for exchanges the
    account's subscription covers; a 404 therefore means the local exchange
    catalog is ahead of the plan (e.g. the subscription changed). This is an
    expected operational condition, not a bug -- callers skip the exchange
    and leave stored data untouched.
    """

    def __init__(self, exchange_code: str) -> None:
        self.exchange_code = exchange_code
        super().__init__(
            f"Exchange {exchange_code} is not covered by the current EODHD "
            "plan (HTTP 404 from exchange-symbol-list)."
        )


def _raise_for_status_sanitized(response: requests.Response) -> None:
    """``raise_for_status`` with the ``api_token`` scrubbed from the message.

    The original ``response`` stays attached so callers can still branch on
    ``exc.response.status_code``; only the human-readable message (what ends
    up in tracebacks and logs) is redacted.
    """

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise requests.HTTPError(
            redact_api_token(str(exc)), response=exc.response
        ) from None


@dataclass(frozen=True)
class ExchangeSymbol:
    code: str
    name: str
    operating_mic: Optional[str] = None


class EODHDFundamentalsClient:
    """Download fundamentals and exchange metadata from EODHD."""

    def __init__(
        self, api_key: str, session: Optional[requests.Session] = None
    ) -> None:
        if not api_key:
            raise ValueError("EODHD API key is required")
        self.api_key = api_key
        self.session = session or requests.Session()

    def list_exchanges(self, timeout: int = DEFAULT_TIMEOUT) -> List[Dict]:
        url = f"{BASE_URL}/exchanges-list/"
        params = {"api_token": self.api_key, "fmt": "json"}
        response = self.session.get(url, params=params, timeout=timeout)
        _raise_for_status_sanitized(response)
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError(f"Unexpected EODHD exchange response: {payload}")
        return payload

    def list_symbols(
        self, exchange_code: str, timeout: int = DEFAULT_TIMEOUT
    ) -> List[Dict]:
        code = exchange_code.upper()
        url = f"{BASE_URL}/exchange-symbol-list/{code}"
        params = {
            "api_token": self.api_key,
            "fmt": "json",
            "delisted": "0",
        }
        response = self.session.get(url, params=params, timeout=timeout)
        try:
            _raise_for_status_sanitized(response)
        except requests.HTTPError as exc:
            # 404 here has one meaning: the subscription does not cover this
            # exchange (the endpoint exists for every covered exchange, even
            # empty ones). Surface it as a typed condition so the refresh can
            # skip the exchange instead of dying. Other statuses stay HTTP
            # errors. (A 404 on fetch_fundamentals means "unknown symbol" --
            # different semantics, deliberately NOT mapped.)
            if exc.response is not None and exc.response.status_code == 404:
                raise ExchangeNotInPlanError(code) from exc
            raise
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError(f"Unexpected EODHD symbols response for {code}: {payload}")
        return payload

    def user_metadata(self, timeout: int = DEFAULT_TIMEOUT) -> Dict:
        """Return the current subscription usage metadata from EODHD."""

        url = f"{BASE_URL}/user"
        params = {"api_token": self.api_key, "fmt": "json"}
        response = self.session.get(url, params=params, timeout=timeout)
        _raise_for_status_sanitized(response)
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected EODHD user response: {payload}")
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
        _raise_for_status_sanitized(response)
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(
                f"Unexpected EODHD fundamentals response for {ticker}: {payload}"
            )
        return payload

    def _format_symbol(self, symbol: str, exchange_code: Optional[str]) -> str:
        cleaned = symbol.strip().upper()
        if "." in cleaned:
            return cleaned
        if exchange_code:
            return f"{cleaned}.{exchange_code.upper()}"
        return cleaned


__all__ = [
    "EODHDFundamentalsClient",
    "ExchangeNotInPlanError",
    "ExchangeSymbol",
    "redact_api_token",
]
