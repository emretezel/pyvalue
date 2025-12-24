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
            {
                "Code": "AAA",
                "Name": "AAA plc",
                "Exchange": "LSE",
                "Type": "Common Stock",
                "Isin": "GB000AAA",
                "Currency": "GBP",
            },
            {"Code": "BBB", "Name": "BBB plc", "Exchange": "LSE", "Type": "Preferred Stock", "Currency": "EUR"},
            {"Code": "ETF1", "Name": "ETF", "Exchange": "LSE", "Type": "ETF", "Currency": "GBP"},
            {"Code": "WRT", "Name": "Warrant", "Exchange": "LSE", "Type": "Warrant", "Currency": "USD"},
        ]
    )


def test_loader_filters_by_type(eodhd_payload):
    loader = UKUniverseLoader(api_key="dummy", exchange_code="LSE", fetcher=lambda _: eodhd_payload)

    listings = loader.load()

    symbols = [l.symbol for l in listings]
    assert symbols == ["AAA.LSE"]

    aaa = next(item for item in listings if item.symbol == "AAA.LSE")
    assert aaa.isin == "GB000AAA"
    assert aaa.currency == "GBP"


def test_loader_includes_etfs_when_enabled(eodhd_payload):
    loader = UKUniverseLoader(
        api_key="dummy",
        exchange_code="LSE",
        fetcher=lambda _: eodhd_payload,
        include_etfs=True,
    )

    listings = loader.load()

    symbols = [l.symbol for l in listings]
    assert symbols == ["AAA.LSE", "ETF1.LSE"]

    etf = next(item for item in listings if item.symbol == "ETF1.LSE")
    assert etf.is_etf is True


def test_loader_filters_by_currency(eodhd_payload):
    loader = UKUniverseLoader(
        api_key="dummy",
        exchange_code="LSE",
        fetcher=lambda _: eodhd_payload,
        allowed_currencies=["GBP"],
    )

    listings = loader.load()

    symbols = [l.symbol for l in listings]
    assert symbols == ["AAA.LSE"]


def test_loader_filters_by_exchange_field():
    payload = json.dumps(
        [
            {
                "Code": "AAA",
                "Name": "AAA Inc",
                "Exchange": "NYSE",
                "Type": "Common Stock",
                "Currency": "USD",
            },
            {
                "Code": "BBB",
                "Name": "BBB Inc",
                "Exchange": "NASDAQ",
                "Type": "Common Stock",
                "Currency": "USD",
            },
        ]
    )
    loader = UKUniverseLoader(
        api_key="dummy",
        exchange_code="US",
        fetcher=lambda _: payload,
        include_exchanges=["NYSE"],
    )

    listings = loader.load()

    symbols = [l.symbol for l in listings]
    assert symbols == ["AAA.US"]


def test_loader_uses_exchange_code_for_exchange_field():
    payload = json.dumps(
        [
            {
                "Code": "AAA",
                "Name": "AAA plc",
                "Exchange": "FOO",
                "Type": "Common Stock",
                "Currency": "GBP",
            }
        ]
    )
    loader = UKUniverseLoader(api_key="dummy", exchange_code="LSE", fetcher=lambda _: payload)

    listings = loader.load()

    assert listings[0].exchange == "LSE"


def test_loader_requires_api_key_without_fetcher():
    with pytest.raises(ValueError):
        UKUniverseLoader(api_key=None)
