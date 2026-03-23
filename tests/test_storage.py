import sqlite3

from pyvalue.storage import (
    FundamentalsRepository,
    MarketDataFetchStateRepository,
    MarketDataRepository,
    SupportedExchangeRepository,
    SupportedTickerRepository,
)
from pyvalue.universe import Listing


def _listing(symbol: str, is_etf: bool = False) -> Listing:
    """Helper to instantiate listings in a compact way.

    Author: Emre Tezel
    """

    return Listing(
        symbol=f"{symbol}.US" if "." not in symbol else symbol,
        security_name=f"Company {symbol}",
        exchange="NYSE",
        market_category="N",
        is_etf=is_etf,
        is_test_issue=False,
        status="N",
        round_lot_size=100,
        source="test",
        isin=None,
        currency=None,
    )


def test_supported_ticker_repository_replace_from_listings_persists_rows(tmp_path):
    repo = SupportedTickerRepository(tmp_path / "universe.db")
    repo.initialize_schema()

    inserted = repo.replace_from_listings(
        "SEC",
        "US",
        [_listing("AAA"), _listing("BBB", is_etf=True)],
    )

    assert inserted == 2

    with sqlite3.connect(tmp_path / "universe.db") as conn:
        rows = conn.execute(
            """
            SELECT provider_symbol, provider_exchange_code, listing_exchange, security_type
            FROM supported_tickers
            ORDER BY provider_symbol
            """
        ).fetchall()

    assert rows == [
        ("AAA.US", "US", "NYSE", "Common Stock"),
        ("BBB.US", "US", "NYSE", "ETF"),
    ]


def test_supported_ticker_repository_replace_from_listings_overwrites_exchange_slice(
    tmp_path,
):
    repo = SupportedTickerRepository(tmp_path / "universe.db")
    repo.initialize_schema()

    repo.replace_from_listings("SEC", "US", [_listing("AAA")])
    repo.replace_from_listings("SEC", "US", [_listing("CCC")])

    with sqlite3.connect(tmp_path / "universe.db") as conn:
        rows = conn.execute(
            "SELECT provider_symbol FROM supported_tickers ORDER BY provider_symbol"
        ).fetchall()

    assert rows == [("CCC.US",)]


def test_supported_ticker_repository_list_symbols_initializes_schema(tmp_path):
    repo = SupportedTickerRepository(tmp_path / "universe.db")

    assert repo.list_symbols_by_exchange("SEC", "US") == []


def test_supported_ticker_repository_normalizes_exchange_and_fetches_currency(tmp_path):
    repo = SupportedTickerRepository(tmp_path / "universe.db")
    repo.initialize_schema()
    listing = Listing(
        symbol="FOO.LSE",
        security_name="Foo PLC",
        exchange="LSE",
        market_category="",
        is_etf=False,
        status="Active",
        round_lot_size=0,
        source="test",
        isin="GB00TEST",
        currency="GBP",
    )
    repo.replace_from_listings("EODHD", "LSE", [listing])

    assert repo.list_symbols_by_exchange("EODHD", "LSE") == ["FOO.LSE"]
    assert repo.list_symbols_by_exchange("eodhd", "lse") == ["FOO.LSE"]
    assert repo.fetch_currency("FOO.LSE", provider="EODHD") == "GBP"


def test_fundamentals_repository_normalizes_provider(tmp_path):
    repo = FundamentalsRepository(tmp_path / "funds.db")
    repo.initialize_schema()
    repo.upsert("eodhd", "FOO.LSE", payload={"bar": 1})

    assert repo.symbols("EODHD") == ["FOO.LSE"]
    assert repo.symbols("eodhd") == ["FOO.LSE"]


def test_supported_exchange_repository_replaces_rows_per_provider(tmp_path):
    repo = SupportedExchangeRepository(tmp_path / "supported-exchanges.db")
    repo.initialize_schema()
    repo.replace_for_provider(
        "EODHD",
        [
            {"Code": "LSE", "Name": "London Exchange"},
            {"Code": "US", "Name": "USA Stocks"},
        ],
    )
    repo.replace_for_provider(
        "OTHER",
        [{"Code": "TSX", "Name": "Toronto Exchange"}],
    )

    inserted = repo.replace_for_provider(
        "eodhd",
        [{"Code": "lse", "Name": "London Exchange Refreshed"}],
    )

    assert inserted == 1
    rows = repo.list_all("EODHD")
    assert [(row.provider, row.code, row.name) for row in rows] == [
        ("EODHD", "LSE", "London Exchange Refreshed"),
    ]
    other_rows = repo.list_all("OTHER")
    assert [(row.provider, row.code, row.name) for row in other_rows] == [
        ("OTHER", "TSX", "Toronto Exchange")
    ]


def test_supported_exchange_repository_fetch_normalizes_code(tmp_path):
    repo = SupportedExchangeRepository(tmp_path / "supported-exchanges.db")
    repo.initialize_schema()
    repo.replace_for_provider(
        "EODHD",
        [
            {
                "Code": " lse ",
                "Name": " London Exchange ",
                "Country": " UK ",
                "Currency": " GBP ",
                "OperatingMIC": " XLON ",
                "CountryISO2": " GB ",
                "CountryISO3": " GBR ",
            }
        ],
    )

    record = repo.fetch("eodhd", "LSe")

    assert record is not None
    assert record.provider == "EODHD"
    assert record.code == "LSE"
    assert record.name == "London Exchange"
    assert record.country == "UK"
    assert record.currency == "GBP"
    assert record.operating_mic == "XLON"
    assert record.country_iso2 == "GB"
    assert record.country_iso3 == "GBR"


def test_supported_ticker_repository_replaces_rows_per_exchange(tmp_path):
    repo = SupportedTickerRepository(tmp_path / "supported-tickers.db")
    repo.initialize_schema()
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock"},
            {"Code": "BRK.B", "Name": "Share Class", "Type": "Preferred Stock"},
        ],
    )
    repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "CCC", "Name": "CCC Inc", "Type": "Common Stock"}],
    )

    inserted = repo.replace_for_exchange(
        "eodhd",
        "lse",
        [{"Code": "AAA", "Name": "AAA plc refreshed", "Type": "Common Stock"}],
    )

    assert inserted == 1
    lse = repo.list_for_exchange("EODHD", "LSE")
    us = repo.list_for_exchange("EODHD", "US")
    assert [(row.symbol, row.security_name) for row in lse] == [
        ("AAA.LSE", "AAA plc refreshed")
    ]
    assert [(row.symbol, row.security_name) for row in us] == [("CCC.US", "CCC Inc")]

    repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "BRK.B", "Name": "Berkshire B", "Type": "Common Stock"}],
    )

    us = repo.list_for_exchange("EODHD", "US")
    assert [(row.symbol, row.code) for row in us] == [("BRK.B.US", "BRK.B")]


def test_supported_ticker_repository_lists_eligible_symbols(tmp_path):
    db_path = tmp_path / "supported-tickers.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [
            {"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB plc", "Type": "Preferred Stock"},
        ],
    )

    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert("EODHD", "AAA.LSE", {"General": {"CurrencyCode": "GBP"}})

    rows = repo.list_eligible_for_fundamentals(
        "EODHD",
        exchange_codes=["LSE"],
        max_age_days=None,
        missing_only=True,
    )

    assert [row.symbol for row in rows] == ["BBB.LSE"]


def test_fundamentals_repository_upsert_marks_fetch_state_success(tmp_path):
    db_path = tmp_path / "fundamentals-fetch-state.db"
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()

    repo.upsert("EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US")

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT last_fetched_at, last_status, attempts
            FROM fundamentals_fetch_state
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            """
        ).fetchone()

    assert row is not None
    assert row[0] is not None
    assert row[1] == "ok"
    assert row[2] == 0


def test_supported_ticker_repository_lists_market_data_symbols_missing_then_oldest(
    tmp_path,
):
    db_path = tmp_path / "supported-market-data.db"
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    repo.replace_for_exchange(
        "EODHD",
        "US",
        [
            {"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock"},
            {"Code": "BBB", "Name": "BBB Inc", "Type": "Common Stock"},
            {"Code": "CCC", "Name": "CCC Inc", "Type": "Common Stock"},
            {"Code": "DDD", "Name": "DDD Inc", "Type": "Common Stock"},
        ],
    )

    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("BBB.US", "2026-03-21", 10.0)
    market_repo.upsert_price("CCC.US", "2026-03-01", 10.0)
    market_repo.upsert_price("DDD.US", "2026-03-10", 10.0)

    rows = repo.list_eligible_for_market_data(
        "EODHD",
        exchange_codes=["US"],
        max_age_days=7,
    )

    assert [row.symbol for row in rows] == ["AAA.US", "CCC.US", "DDD.US"]


def test_market_data_fetch_state_repository_tracks_success_and_failure(tmp_path):
    repo = MarketDataFetchStateRepository(tmp_path / "market-state.db")
    repo.initialize_schema()

    repo.mark_failure("eodhd", "aaa.us", "boom", base_backoff_seconds=60)
    failed = repo.fetch("EODHD", "AAA.US")

    assert failed is not None
    assert failed["last_status"] == "error"
    assert failed["last_error"] == "boom"
    assert failed["attempts"] == 1
    assert failed["next_eligible_at"] is not None

    repo.mark_success("EODHD", "AAA.US")
    success = repo.fetch("eodhd", "aaa.us")

    assert success is not None
    assert success["last_status"] == "ok"
    assert success["last_error"] is None
    assert success["next_eligible_at"] is None
    assert success["attempts"] == 0
