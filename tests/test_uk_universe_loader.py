"""Tests for the UK universe loader.

Author: Emre Tezel
"""

import json

import pytest

from pyvalue.universe import UKUniverseLoader


@pytest.fixture
def eodhd_payload():
    return json.dumps(
        [
            {"Code": "AAA", "Name": "AAA plc", "Exchange": "LSE", "Type": "Common Stock", "Isin": "GB000AAA"},
            {"Code": "BBB", "Name": "BBB plc", "Exchange": "LSE", "Type": "Preferred Stock"},
            {"Code": "ETF1", "Name": "ETF", "Exchange": "LSE", "Type": "ETF"},
            {"Code": "WRT", "Name": "Warrant", "Exchange": "LSE", "Type": "Warrant"},
        ]
    )


def test_loader_filters_by_type(eodhd_payload):
    loader = UKUniverseLoader(api_key="dummy", exchange_code="LSE", fetcher=lambda _: eodhd_payload)

    listings = loader.load()

    symbols = [l.symbol for l in listings]
    assert symbols == ["AAA", "BBB", "ETF1"]

    etf = next(item for item in listings if item.symbol == "ETF1")
    assert etf.is_etf is True

    aaa = next(item for item in listings if item.symbol == "AAA")
    assert aaa.isin == "GB000AAA"


def test_loader_requires_api_key_without_fetcher():
    with pytest.raises(ValueError):
        UKUniverseLoader(api_key=None)
