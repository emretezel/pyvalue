import sqlite3

from pyvalue.storage import (
    FundamentalsRepository,
    SupportedExchangeRepository,
    SupportedTickerRepository,
    UniverseRepository,
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


def test_replace_universe_persists_rows(tmp_path):
    # Persist two listings and verify that they can be read back from SQLite.
    repo = UniverseRepository(tmp_path / "universe.db")
    repo.initialize_schema()

    inserted = repo.replace_universe([_listing("AAA"), _listing("BBB", is_etf=True)])

    assert inserted == 2

    with sqlite3.connect(tmp_path / "universe.db") as conn:
        rows = conn.execute(
            "SELECT symbol, exchange, is_etf FROM listings ORDER BY symbol"
        ).fetchall()

    assert rows == [("AAA.US", "NYSE", 0), ("BBB.US", "NYSE", 1)]


def test_replace_universe_overwrites_previous_data(tmp_path):
    # Insert a listing twice and ensure the second call replaces the first batch.
    repo = UniverseRepository(tmp_path / "universe.db")
    repo.initialize_schema()

    repo.replace_universe([_listing("AAA")])
    repo.replace_universe([_listing("CCC")])

    with sqlite3.connect(tmp_path / "universe.db") as conn:
        rows = conn.execute("SELECT symbol FROM listings ORDER BY symbol").fetchall()

    assert rows == [("CCC.US",)]


def test_universe_repository_fetch_symbols_initializes_schema(tmp_path):
    repo = UniverseRepository(tmp_path / "universe.db")

    assert repo.fetch_symbols_by_exchange("NYSE") == []


def test_universe_repository_normalizes_exchange(tmp_path):
    repo = UniverseRepository(tmp_path / "universe.db")
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
    repo.replace_universe([listing])

    assert repo.fetch_symbols_by_exchange("LSE") == ["FOO.LSE"]
    assert repo.fetch_symbols_by_exchange("lse") == ["FOO.LSE"]


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
    rows = repo.list_all()
    assert [(row.provider, row.code, row.name) for row in rows] == [
        ("EODHD", "LSE", "London Exchange Refreshed"),
        ("OTHER", "TSX", "Toronto Exchange"),
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
