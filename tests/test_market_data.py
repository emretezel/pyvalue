"""Tests for market data providers and services.

Author: Emre Tezel
"""
from pyvalue.marketdata.alpha_vantage import AlphaVantageProvider
from pyvalue.marketdata.service import MarketDataService
from pyvalue.storage import MarketDataRepository
from pyvalue.config import Config


class DummySession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, params=None, timeout=30):
        self.calls.append((url, params, timeout))

        class DummyResponse:
            def __init__(self, data):
                self.data = data

            def raise_for_status(self):
                return None

            def json(self):
                return self.data

        return DummyResponse(self.payload)


def test_alpha_vantage_provider_parses_response():
    payload = {
        "Global Quote": {
            "05. price": "177.90",
            "07. latest trading day": "2024-03-01",
            "06. volume": "1000",
        }
    }
    session = DummySession(payload)
    provider = AlphaVantageProvider(api_key="demo", session=session)  # type: ignore[arg-type]

    data = provider.latest_price("AAPL")

    assert data.price == 177.90
    assert data.as_of == "2024-03-01"
    assert data.volume == 1000
    assert data.symbol == "AAPL"


def test_market_data_service_persists_prices(monkeypatch, tmp_path):
    payload = {
        "Global Quote": {
            "05. price": "150.00",
            "07. latest trading day": "2024-03-02",
            "06. volume": "500",
        }
    }
    session = DummySession(payload)

    class DummyConfig:
        def __init__(self, key):
            self._key = key

        @property
        def alpha_vantage_api_key(self):
            return self._key

    monkeypatch.setenv("PYVALUE_SEC_USER_AGENT", "test")
    # Inject provider directly to avoid hitting disk for config.
    provider = AlphaVantageProvider(api_key="demo", session=session)  # type: ignore[arg-type]
    service = MarketDataService(db_path=tmp_path / "data.db", provider=provider, config=DummyConfig("demo"))

    result = service.refresh_symbol("AAPL")

    assert result.price == 150.0

    repo = MarketDataRepository(tmp_path / "data.db")
    latest = repo.latest_price("AAPL")
    assert latest[0] == "2024-03-02"
    assert latest[1] == 150.0
