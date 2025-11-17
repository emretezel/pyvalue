"""Tests for the US universe loader.

Author: Emre Tezel
"""

import pytest

from pyvalue.universe import USUniverseLoader, Listing
from pyvalue.universe.us import NASDAQ_LISTED_PATH, OTHER_LISTED_PATH


@pytest.fixture
def nasdaq_payload():
    return """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc.|Q|N|N|100|N|N
MSFT|Microsoft Corp.|Q|N|N|100|N|N
TEST|Test Security|Q|Y|N|100|N|N
File Creation Time: 20230901
"""


@pytest.fixture
def other_payload():
    return """ACT Symbol|Security Name|Exchange|Test Issue|ETF|Round Lot Size
BRK.A|Berkshire Hathaway Inc.|N|N|N|1
IVV|iShares Core S&P 500 ETF|P|N|Y|50
File Creation Time: 20230901
"""


def test_loader_combines_files(nasdaq_payload, other_payload, monkeypatch):
    # Compose deterministic responses for both Nasdaq Trader endpoints.
    payloads = {
        NASDAQ_LISTED_PATH: nasdaq_payload,
        OTHER_LISTED_PATH: other_payload,
    }

    def fetcher(path: str) -> str:
        return payloads[path]

    loader = USUniverseLoader(fetcher=fetcher, allowed_exchanges=["NASDAQ", "NYSE", "NYSE Arca"])

    listings = loader.load()

    assert [listing.symbol for listing in listings] == ["AAPL", "BRK.A", "IVV", "MSFT"]

    berkshire = next(item for item in listings if item.symbol == "BRK.A")
    assert berkshire.exchange == "NYSE"
    assert berkshire.round_lot_size == 1
    assert berkshire.is_etf is False

    ivv = next(item for item in listings if item.symbol == "IVV")
    assert ivv.is_etf is True


def test_loader_filters_test_issues(monkeypatch, nasdaq_payload):
    # Provide an "other listed" payload that marks the entry as a test issue.
    other_payload = """ACT Symbol|Security Name|Exchange|Test Issue|ETF|Round Lot Size
ZZZZ|Some Corp.|P|Y|N|100
File Creation Time: 20230901
"""
    payloads = {
        NASDAQ_LISTED_PATH: nasdaq_payload,
        OTHER_LISTED_PATH: other_payload,
    }

    loader = USUniverseLoader(fetcher=lambda path: payloads[path], allowed_exchanges=["NASDAQ", "NYSE", "NYSE Arca"])

    listings = loader.load()

    assert all(listing.symbol != "ZZZZ" for listing in listings)
