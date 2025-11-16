# Author: Emre Tezel
import sqlite3

from pyvalue.storage import UniverseRepository
from pyvalue.universe import Listing


def _listing(symbol: str, is_etf: bool = False) -> Listing:
    """Helper to instantiate listings in a compact way."""

    return Listing(
        symbol=symbol,
        security_name=f"Company {symbol}",
        exchange="NYSE",
        market_category="N",
        is_etf=is_etf,
        is_test_issue=False,
        status="N",
        round_lot_size=100,
        source="test",
    )


def test_replace_universe_persists_rows(tmp_path):
    # Persist two listings and verify that they can be read back from SQLite.
    repo = UniverseRepository(tmp_path / "universe.db")
    repo.initialize_schema()

    inserted = repo.replace_universe([_listing("AAA"), _listing("BBB", is_etf=True)], region="US")

    assert inserted == 2

    with sqlite3.connect(tmp_path / "universe.db") as conn:
        rows = conn.execute(
            "SELECT symbol, region, is_etf FROM listings ORDER BY symbol"
        ).fetchall()

    assert rows == [("AAA", "US", 0), ("BBB", "US", 1)]


def test_replace_universe_overwrites_previous_data(tmp_path):
    # Insert a listing twice and ensure the second call replaces the first batch.
    repo = UniverseRepository(tmp_path / "universe.db")
    repo.initialize_schema()

    repo.replace_universe([_listing("AAA")], region="US")
    repo.replace_universe([_listing("CCC")], region="US")

    with sqlite3.connect(tmp_path / "universe.db") as conn:
        rows = conn.execute("SELECT symbol FROM listings ORDER BY symbol").fetchall()

    assert rows == [("CCC",)]
