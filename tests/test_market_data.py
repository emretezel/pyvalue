"""Tests for market data providers and services.

Author: Emre Tezel
"""

from pyvalue.marketdata.alpha_vantage import AlphaVantageProvider
from pyvalue.marketdata.eodhd import EODHDProvider
from pyvalue.marketdata.base import PriceData
from pyvalue.marketdata.service import MarketDataService
from pyvalue.storage import FactRecord, FinancialFactsRepository, FundamentalsRepository, MarketDataRepository


class DummyAlphaSession:
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


class DummyEODSession:
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
    session = DummyAlphaSession(payloads)
    provider = AlphaVantageProvider(api_key="demo", session=session)  # type: ignore[arg-type]

    data = provider.latest_price("AAPL.US")

    assert data.price == 177.90
    assert data.as_of == "2024-03-01"
    assert data.volume == 1000
    assert data.symbol == "AAPL.US"
    assert data.market_cap == 300000000000.0


def test_eodhd_provider_parses_response():
    payload = [
        {"date": "2024-03-01", "Close": "200.50", "Volume": "12345"},
        {"date": "2024-03-04", "Close": "205.75", "Volume": "9000"},
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)  # type: ignore[arg-type]

    data = provider.latest_price("mcd.us")

    assert data.price == 205.75
    assert data.as_of == "2024-03-04"
    assert data.volume == 9000
    assert data.symbol == "MCD.US"


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
    session = DummyAlphaSession(payloads)

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

    result = service.refresh_symbol("AAPL.US")

    assert result.price == 150.0
    assert result.market_cap == 2500000000.0

    repo = MarketDataRepository(tmp_path / "data.db")
    latest_snapshot = repo.latest_snapshot("AAPL.US")
    assert latest_snapshot is not None
    assert latest_snapshot.as_of == "2024-03-02"
    assert latest_snapshot.price == 150.0
    assert latest_snapshot.market_cap == 2500000000.0
    latest = repo.latest_price("AAPL.US")
    assert latest[0] == "2024-03-02"
    assert latest[1] == 150.0


def test_market_data_service_derives_market_cap_from_shares(tmp_path):
    class DummyProvider:
        def latest_price(self, symbol):
            return PriceData(symbol=symbol, price=50.0, as_of="2024-01-02", volume=None, currency=None)

    class DummyConfig:
        eodhd_api_key = None
        alpha_vantage_api_key = None

    db_path = tmp_path / "shares.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAPL.US",
        [
            FactRecord(
                symbol="AAPL.US",
                cik="CIK0000320193",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2023-12-31",
                unit="shares",
                value=1000,
                accn=None,
                filed=None,
                frame="CY2023",
                start_date=None,
            )
        ],
    )

    service = MarketDataService(db_path=db_path, provider=DummyProvider(), config=DummyConfig())
    service.refresh_symbol("AAPL.US")

    repo = MarketDataRepository(db_path)
    snapshot = repo.latest_snapshot("AAPL.US")
    assert snapshot is not None
    assert snapshot.market_cap == 50000.0


def test_eodhd_provider_converts_gbx_to_gbp():
    payload = [
        {"date": "2024-03-01", "Close": "99.0", "Volume": "1000", "currency": "GBX"},
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)  # type: ignore[arg-type]

    data = provider.latest_price("SHEL.LSE")

    assert data.price == 0.99
    assert data.currency == "GBP"


def test_eodhd_provider_converts_gbx_by_suffix_when_currency_missing():
    payload = [
        {"date": "2024-03-01", "Close": "2783.5", "Volume": "1000"},
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)  # type: ignore[arg-type]

    data = provider.latest_price("SHEL.LSE")

    assert data.price == 27.835
    assert data.currency == "GBP"


def test_market_data_service_uses_fundamentals_shares(tmp_path):
    class DummyProvider:
        def latest_price(self, symbol):
            return PriceData(symbol=symbol, price=20.0, as_of="2024-01-02", volume=None, currency=None)

    class DummyConfig:
        eodhd_api_key = None
        alpha_vantage_api_key = None

    db_path = tmp_path / "sharesfund.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD",
        "SHEL.LSE",
        {"SharesStats": {"SharesOutstanding": 50}},
        region="UK",
        exchange="LSE",
    )

    service = MarketDataService(db_path=db_path, provider=DummyProvider(), config=DummyConfig())
    service.refresh_symbol("SHEL.LSE")

    repo = MarketDataRepository(db_path)
    snapshot = repo.latest_snapshot("SHEL.LSE")
    assert snapshot is not None
    assert snapshot.market_cap == 1000.0
