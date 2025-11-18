"""Tests for market data providers and services.

Author: Emre Tezel
"""

from pyvalue.marketdata.alpha_vantage import AlphaVantageProvider
from pyvalue.marketdata.service import MarketDataService
from pyvalue.storage import MarketDataRepository


class DummySession:
    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    def get(self, url, params=None, timeout=30):
        self.calls.append((url, params, timeout))
        function = (params or {}).get("function")
        data = self.payloads.get(function)
        if data is None:
            raise AssertionError(f"No payload stubbed for {function}")

        class DummyResponse:
            def __init__(self, data):
                self.data = data

            def raise_for_status(self):
                return None

            def json(self):
                return self.data

        return DummyResponse(data)


def test_alpha_vantage_provider_parses_response():
    payloads = {
        "GLOBAL_QUOTE": {
            "Global Quote": {
                "05. price": "177.90",
                "07. latest trading day": "2024-03-01",
                "06. volume": "1000",
            }
        },
        "OVERVIEW": {
            "MarketCapitalization": "300000000000",
        },
    }
    session = DummySession(payloads)
    provider = AlphaVantageProvider(api_key="demo", session=session)  # type: ignore[arg-type]

    data = provider.latest_price("AAPL")

    assert data.price == 177.90
    assert data.as_of == "2024-03-01"
    assert data.volume == 1000
    assert data.symbol == "AAPL"
    assert data.market_cap == 300000000000.0


def test_market_data_service_persists_prices(monkeypatch, tmp_path):
    payloads = {
        "GLOBAL_QUOTE": {
            "Global Quote": {
                "05. price": "150.00",
                "07. latest trading day": "2024-03-02",
                "06. volume": "500",
            }
        },
        "OVERVIEW": {
            "MarketCapitalization": "2500000000",
        },
    }
    session = DummySession(payloads)

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
    assert result.market_cap == 2500000000.0

    repo = MarketDataRepository(tmp_path / "data.db")
    latest_snapshot = repo.latest_snapshot("AAPL")
    assert latest_snapshot is not None
    assert latest_snapshot.as_of == "2024-03-02"
    assert latest_snapshot.price == 150.0
    assert latest_snapshot.market_cap == 2500000000.0
    latest = repo.latest_price("AAPL")
    assert latest[0] == "2024-03-02"
    assert latest[1] == 150.0
