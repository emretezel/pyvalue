"""Unit tests for EODHD HTTP error sanitization (api_token redaction).

requests embeds the full request URL -- query string included -- in every
``HTTPError`` message, so an unsanitized ``raise_for_status()`` leaks the
account's ``api_token`` into tracebacks and log files (observed in the
2026-07-11 refresh crash). Every HTTP error raised by the ingestion client is
routed through a sanitizer that scrubs the token while keeping the original
``response`` attached.

Author: Emre Tezel
"""

from __future__ import annotations

import json as json_lib

import pytest
import requests

from pyvalue.ingestion import EODHDFundamentalsClient, redact_api_token


class _CannedSession(requests.Session):
    """Return one canned status/payload with the query merged into the URL."""

    def __init__(self, status: int, payload: object) -> None:
        # Deliberately no super().__init__(): no real network machinery needed.
        self.status = status
        self.payload = payload

    def request(
        self,
        method: str | bytes,
        url: str | bytes,
        *args: object,
        **kwargs: object,
    ) -> requests.Response:
        raw_params = kwargs.get("params")
        params = raw_params if isinstance(raw_params, dict) else {}
        query = "&".join(f"{key}={value}" for key, value in params.items())
        response = requests.Response()
        response.status_code = self.status
        response.reason = "Error"
        response.url = f"{url!s}?{query}" if query else str(url)
        response._content = json_lib.dumps(self.payload).encode("utf-8")
        return response


def test_http_error_message_is_sanitized_response_preserved() -> None:
    client = EODHDFundamentalsClient(
        api_key="SECRET", session=_CannedSession(500, {"error": "boom"})
    )

    with pytest.raises(requests.HTTPError) as excinfo:
        client.list_exchanges()

    message = str(excinfo.value)
    assert "SECRET" not in message
    assert "api_token=REDACTED" in message
    # Callers still branch on the status (e.g. the 404 -> not-in-plan mapping),
    # so the original response object must survive the re-raise.
    assert excinfo.value.response is not None
    assert excinfo.value.response.status_code == 500


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "500 Server Error for url: https://eodhd.com/api/x?api_token=abc123&fmt=json",
            "500 Server Error for url: https://eodhd.com/api/x?api_token=REDACTED&fmt=json",
        ),
        (
            "https://eodhd.com/api/x?fmt=json&api_token=abc123",
            "https://eodhd.com/api/x?fmt=json&api_token=REDACTED",
        ),
        (
            "api_token=first&x=1 api_token=second",
            "api_token=REDACTED&x=1 api_token=REDACTED",
        ),
        ("no token here", "no token here"),
    ],
)
def test_redact_api_token_cases(text: str, expected: str) -> None:
    assert redact_api_token(text) == expected
