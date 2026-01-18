import sqlite3

from pyvalue.storage import FundamentalsRepository, UniverseRepository
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
