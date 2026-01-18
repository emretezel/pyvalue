"""Tests for SEC ingestion client.

Author: Emre Tezel
"""

from pyvalue.ingestion.sec import (
    COMPANY_FACTS_URL,
    COMPANY_TICKERS_URL,
    SECCompanyFactsClient,
)


class DummyResponse:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):  # pragma: no cover - simple stub
        return None

    def json(self):
        return self._data


class DummySession:
    def __init__(self, responses):
        self.responses = responses
        self.headers = {}
        self.requests = []

    def get(self, url, timeout=30):
        self.requests.append((url, timeout))
        return DummyResponse(self.responses[url])


def test_resolve_company_fetches_mapping(monkeypatch):
    responses = {
        COMPANY_TICKERS_URL: {
            "0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."},
            "1": {"ticker": "MSFT", "cik_str": 789019, "title": "Microsoft"},
        }
    }
    session = DummySession(responses)
    client = SECCompanyFactsClient(user_agent="Tester", session=session)

    info = client.resolve_company("aapl")

    assert info.cik == "CIK0000320193"
    assert info.name == "Apple Inc."
    # Second call should reuse cache and avoid another HTTP GET.
    client.resolve_company("MSFT")
    assert len(session.requests) == 1


def test_fetch_company_facts_formats_cik(monkeypatch):
    cik = "0000320193"
    url = COMPANY_FACTS_URL.format(cik="CIK0000320193")
    responses = {
        COMPANY_TICKERS_URL: {
            "0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."},
        },
        url: {"cik": 320193},
    }
    session = DummySession(responses)
    client = SECCompanyFactsClient(user_agent="Tester", session=session)

    data = client.fetch_company_facts(cik)

    assert data == {"cik": 320193}
    assert session.requests[-1][0] == url
