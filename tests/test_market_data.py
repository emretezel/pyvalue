"""Tests for market data providers and services.

Author: Emre Tezel
"""

import pytest

from pyvalue.marketdata.eodhd import EODHDProvider
from pyvalue.marketdata.base import PriceData
from pyvalue.marketdata.service import (
    MarketDataService,
    SuspiciousMarketPriceChangeError,
)
from pyvalue.storage import (
    FactRecord,
    FinancialFactsRepository,
    FundamentalsRepository,
    MarketDataRepository,
    SupportedTickerRepository,
)


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
    assert "from" in session.calls[0][1]


def test_market_data_service_persists_prices(tmp_path):
    class DummyProvider:
        def latest_price(self, symbol):
            return PriceData(
                symbol=symbol,
                price=150.0,
                as_of="2024-03-02",
                volume=500,
                market_cap=2500000000.0,
                currency=None,
            )

    service = MarketDataService(db_path=tmp_path / "data.db", provider=DummyProvider())

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
            return PriceData(
                symbol=symbol,
                price=50.0,
                as_of="2024-01-02",
                volume=None,
                currency=None,
            )

    class DummyConfig:
        eodhd_api_key = None

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

    service = MarketDataService(
        db_path=db_path, provider=DummyProvider(), config=DummyConfig()
    )
    service.refresh_symbol("AAPL.US")

    repo = MarketDataRepository(db_path)
    snapshot = repo.latest_snapshot("AAPL.US")
    assert snapshot is not None
    assert snapshot.market_cap == 50000.0


@pytest.mark.parametrize(
    (
        "symbol",
        "exchange",
        "quote_currency",
        "base_currency",
        "price",
        "major_price",
        "shares",
        "market_cap",
    ),
    [
        ("SHEL.LSE", "LSE", "GBX", "GBP", 2783.5, 27.835, 50, 1391.75),
        ("NPN.JSE", "JSE", "ZAC", "ZAR", 23750.0, 237.5, 20, 4750.0),
        ("BCOM.TA", "TA", "ILA", "ILS", 1234.0, 12.34, 10, 123.4),
    ],
)
def test_market_data_service_stores_major_price_and_market_cap(
    tmp_path,
    symbol,
    exchange,
    quote_currency,
    base_currency,
    price,
    major_price,
    shares,
    market_cap,
):
    # A subunit-quoted listing (GBX/ZAC/ILA) must be stored in its MAJOR
    # currency: the raw quote price is divided by 100 and the snapshot reports
    # the base currency, so subunits never cross the data boundary.
    class DummyProvider:
        def latest_price(self, requested_symbol):
            return PriceData(
                symbol=requested_symbol,
                price=price,
                as_of="2024-03-04",
                volume=100,
                currency=None,
            )

    class DummyConfig:
        eodhd_api_key = None

    db_path = tmp_path / f"{quote_currency.lower()}-market-data.db"
    catalog_repo = SupportedTickerRepository(db_path)
    catalog_repo.initialize_schema()
    catalog_repo.replace_for_exchange(
        "EODHD",
        exchange,
        [
            {
                "Code": symbol.split(".")[0],
                "Name": f"{symbol} Plc",
                "Type": "Common Stock",
                "Currency": quote_currency,
            }
        ],
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        symbol,
        [
            FactRecord(
                symbol=symbol,
                cik=None,
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2023-12-31",
                unit="shares",
                value=shares,
                accn=None,
                filed=None,
                frame="CY2023",
                start_date=None,
            )
        ],
    )

    service = MarketDataService(
        db_path=db_path,
        provider=DummyProvider(),
        config=DummyConfig(),
    )

    service.refresh_symbol(symbol)

    snapshot = MarketDataRepository(db_path).latest_snapshot(symbol)
    assert snapshot is not None
    assert snapshot.price == pytest.approx(major_price)
    assert snapshot.market_cap == pytest.approx(market_cap)
    assert snapshot.currency == base_currency


def test_market_data_service_prefers_share_unit_count_when_duplicates_exist(tmp_path):
    class DummyProvider:
        def latest_price(self, symbol):
            return PriceData(
                symbol=symbol,
                price=10.0,
                as_of="2024-01-02",
                volume=None,
                currency=None,
            )

    class DummyConfig:
        eodhd_api_key = None

    db_path = tmp_path / "duplicate-shares.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAPL.US",
        [
            FactRecord(
                symbol="AAPL.US",
                cik="CIK0000320193",
                concept="EntityCommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2023-12-31",
                unit="USD",
                value=1_000_000.0,
                accn=None,
                filed="2024-03-01",
                frame="CY2023",
                start_date=None,
            ),
            FactRecord(
                symbol="AAPL.US",
                cik="CIK0000320193",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2023-12-31",
                unit="shares",
                value=1_000.0,
                accn=None,
                filed=None,
                frame="CY2023",
                start_date=None,
            ),
        ],
    )

    service = MarketDataService(
        db_path=db_path, provider=DummyProvider(), config=DummyConfig()
    )
    service.refresh_symbol("AAPL.US")

    repo = MarketDataRepository(db_path)
    snapshot = repo.latest_snapshot("AAPL.US")
    assert snapshot is not None
    assert snapshot.market_cap == 10000.0


def test_eodhd_provider_preserves_gbx_quote_price():
    payload = [
        {"date": "2024-03-01", "Close": "99.0", "Volume": "1000", "currency": "GBX"},
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)  # type: ignore[arg-type]

    data = provider.latest_price("SHEL.LSE")

    assert data.price == 99.0
    assert data.currency == "GBX"


def test_eodhd_provider_preserves_zac_quote_price():
    payload = [
        {"date": "2024-03-01", "Close": "23750.0", "Volume": "1000", "currency": "ZAC"},
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)  # type: ignore[arg-type]

    data = provider.latest_price("ABG.JSE")

    assert data.price == 23750.0
    assert data.currency == "ZAC"


def test_eodhd_provider_infers_gbx_by_suffix_when_currency_missing():
    payload = [
        {"date": "2024-03-01", "Close": "2783.5", "Volume": "1000"},
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)  # type: ignore[arg-type]

    data = provider.latest_price("SHEL.LSE")

    assert data.price == 2783.5
    assert data.currency == "GBX"


def test_market_data_service_uses_fundamentals_shares(tmp_path):
    class DummyProvider:
        def latest_price(self, symbol):
            return PriceData(
                symbol=symbol,
                price=20.0,
                as_of="2024-01-02",
                volume=None,
                currency=None,
            )

    class DummyConfig:
        eodhd_api_key = None

    db_path = tmp_path / "sharesfund.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD",
        "SHEL.LSE",
        {"SharesStats": {"SharesOutstanding": 50}},
        exchange="LSE",
    )

    service = MarketDataService(
        db_path=db_path, provider=DummyProvider(), config=DummyConfig()
    )
    service.refresh_symbol("SHEL.LSE")

    repo = MarketDataRepository(db_path)
    snapshot = repo.latest_snapshot("SHEL.LSE")
    assert snapshot is not None
    assert snapshot.market_cap == 1000.0


def test_market_data_service_rejects_unexplained_price_jump(tmp_path):
    class DummyProvider:
        def latest_price(self, symbol):
            return PriceData(
                symbol=symbol,
                price=5298.0,
                as_of="2026-03-20",
                volume=0,
                currency="USD",
            )

    class DummyConfig:
        eodhd_api_key = None

    db_path = tmp_path / "suspicious-price.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "ATXS.US",
        [
            FactRecord(
                symbol="ATXS.US",
                cik="CIK0000000001",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2025-12-31",
                unit="shares",
                value=58_005_300.0,
                accn=None,
                filed=None,
                frame="CY2025",
                start_date=None,
            )
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price(
        "ATXS.US",
        "2025-12-22",
        12.92,
        market_cap=749_428_631.04,
        currency="USD",
    )

    service = MarketDataService(
        db_path=db_path, provider=DummyProvider(), config=DummyConfig()
    )

    with pytest.raises(
        SuspiciousMarketPriceChangeError,
        match=r"suspicious market data for ATXS\.US",
    ):
        service.refresh_symbol("ATXS.US")

    latest_snapshot = market_repo.latest_snapshot("ATXS.US")
    assert latest_snapshot is not None
    assert latest_snapshot.as_of == "2025-12-22"
    assert latest_snapshot.price == 12.92
    assert latest_snapshot.market_cap == 749_428_631.04


def test_market_data_service_allows_split_like_price_change_when_value_is_stable(
    tmp_path,
):
    class DummyProvider:
        def latest_price(self, symbol):
            return PriceData(
                symbol=symbol,
                price=100.0,
                as_of="2026-03-20",
                volume=0,
                currency="USD",
            )

    class DummyConfig:
        eodhd_api_key = None

    db_path = tmp_path / "split-like-price.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "SPLT.US",
        [
            FactRecord(
                symbol="SPLT.US",
                cik="CIK0000000002",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2025-12-31",
                unit="shares",
                value=1_000_000.0,
                accn=None,
                filed=None,
                frame="CY2025",
                start_date=None,
            )
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price(
        "SPLT.US",
        "2026-01-15",
        1.0,
        market_cap=100_000_000.0,
        currency="USD",
    )

    service = MarketDataService(
        db_path=db_path, provider=DummyProvider(), config=DummyConfig()
    )
    service.refresh_symbol("SPLT.US")

    latest_snapshot = market_repo.latest_snapshot("SPLT.US")
    assert latest_snapshot is not None
    assert latest_snapshot.as_of == "2026-03-20"
    assert latest_snapshot.price == 100.0
    assert latest_snapshot.market_cap == 100_000_000.0


def test_eodhd_provider_parses_bulk_exchange_response():
    payload = [
        {"code": "AAA", "close": "10.5", "date": "2024-03-04", "volume": "100"},
        {
            "code": "SHEL",
            "close": "2783.5",
            "date": "2024-03-04",
            "volume": "200",
        },
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)  # type: ignore[arg-type]

    data = provider.latest_prices_for_exchange("LSE")

    assert data["AAA.LSE"].price == 10.5
    assert data["AAA.LSE"].as_of == "2024-03-04"
    assert data["SHEL.LSE"].price == 2783.5
    assert data["SHEL.LSE"].currency == "GBX"
    assert session.calls[0][0].endswith("/api/eod-bulk-last-day/LSE")


def test_market_data_service_prepare_price_data_uses_currency_hint(tmp_path):
    class DummyProvider:
        def latest_price(self, symbol):
            return PriceData(
                symbol=symbol,
                price=2783.5,
                as_of="2024-03-04",
                volume=100,
                currency=None,
            )

    class DummyConfig:
        eodhd_api_key = None

    db_path = tmp_path / "hint.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "SHEL.LSE",
        [
            FactRecord(
                symbol="SHEL.LSE",
                cik="CIK",
                concept="CommonStockSharesOutstanding",
                fiscal_period="FY",
                end_date="2024-01-01",
                unit="shares",
                value=50,
            )
        ],
    )
    service = MarketDataService(
        db_path=db_path,
        provider=DummyProvider(),
        config=DummyConfig(),
    )
    service.supported_ticker_repo.fetch_currency = lambda symbol: (_ for _ in ()).throw(
        AssertionError("fetch_currency should not be used when a currency hint exists")
    )

    prepared = service.prepare_price_data(
        "SHEL.LSE",
        PriceData(
            symbol="SHEL.LSE",
            price=2783.5,
            as_of="2024-03-04",
            volume=100,
            currency=None,
        ),
        currency_hint="GBX",
    )

    # The hint says GBX (pence); the stored price is the major amount (GBP).
    assert prepared.price == pytest.approx(27.835)
    assert prepared.market_cap == pytest.approx(1391.75)
    assert prepared.currency == "GBP"


def test_market_data_service_prepare_price_data_uses_ila_currency_hint(tmp_path):
    class DummyProvider:
        def latest_price(self, symbol):
            return PriceData(
                symbol=symbol,
                price=1234.0,
                as_of="2024-03-04",
                volume=100,
                currency=None,
            )

    class DummyConfig:
        eodhd_api_key = None

    service = MarketDataService(
        db_path=tmp_path / "ila-hint.db",
        provider=DummyProvider(),
        config=DummyConfig(),
    )
    service.supported_ticker_repo.fetch_currency = lambda symbol: (_ for _ in ()).throw(
        AssertionError("fetch_currency should not be used when a currency hint exists")
    )

    prepared = service.prepare_price_data(
        "BCOM.TA",
        PriceData(
            symbol="BCOM.TA",
            price=1234.0,
            as_of="2024-03-04",
            volume=100,
            currency=None,
        ),
        currency_hint="ILA",
    )

    # ILA agorot collapse to ILS major: 1234 -> 12.34.
    assert prepared.price == pytest.approx(12.34)
    assert prepared.currency == "ILS"
