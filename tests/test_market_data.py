"""Tests for market data providers and services.

Author: Emre Tezel
"""

from pathlib import Path
import json as json_lib

import pytest
import requests

from pyvalue.config import Config
from pyvalue.marketdata.eodhd import EODHDProvider
from pyvalue.marketdata.base import PriceData
from pyvalue.marketdata.service import MarketDataService
from pyvalue.persistence.storage import (
    MarketDataRepository,
    SupportedTickerRepository,
)

from conftest import seed_exchange


class DummyEODSession(requests.Session):
    """A stand-in ``requests.Session`` that returns a canned EODHD payload.

    Subclasses the real ``requests.Session`` so it satisfies the provider's
    ``Session`` parameter type. ``__init__`` skips the real HTTP machinery, and
    every outgoing request is intercepted at ``request`` -- the single funnel
    that ``get``/``post``/... delegate to -- so we return a genuine
    ``requests.Response`` carrying the canned JSON body. Returning a real
    ``Response`` keeps the override Liskov-compatible (so no ``type: ignore``)
    while ``raise_for_status``/``json`` behave exactly as in production.
    """

    def __init__(self, payload: object) -> None:
        # Deliberately do NOT call super().__init__(): we never make a real
        # network request, so building the underlying adapters/pools is waste.
        # The recorded calls keep ``(url, params)`` so assertions can inspect
        # the query the provider issued.
        self.payload = payload
        # Each recorded call keeps the request URL and the query parameters the
        # provider attached, so tests can assert on the issued query.
        self.calls: list[tuple[str, dict[str, object] | None]] = []

    def request(
        self,
        method: str | bytes,
        url: str | bytes,
        *args: object,
        **kwargs: object,
    ) -> requests.Response:
        raw_params = kwargs.get("params")
        # The provider only ever passes a ``dict`` (or nothing) for ``params``;
        # narrow explicitly so the recorded type stays precise rather than
        # widening the tuple to ``object``.
        params = raw_params if isinstance(raw_params, dict) else None
        self.calls.append((str(url), params))
        response = requests.Response()
        response.status_code = 200
        response._content = json_lib.dumps(self.payload).encode("utf-8")
        return response


class DummyConfig(Config):
    """A ``Config`` with no backing file so ``eodhd_api_key`` resolves to None.

    Subclasses the real ``Config`` to satisfy the service's ``Config``
    parameter type while overriding ``__init__`` to avoid reading any file;
    the empty parser makes every ``_get_value`` lookup (including
    ``eodhd_api_key``) return ``None``, which is what these tests rely on.
    """

    def __init__(self) -> None:
        # Skip Config.__init__ (which reads private/config.toml); set up an
        # empty parser so property accessors return their None/default values.
        import configparser

        self.path = Path("private/config.toml")
        self._parser = configparser.ConfigParser()


def _seed_listing(
    db_path: Path, symbol: str, currency: str = "USD"
) -> SupportedTickerRepository:
    """Seed a cataloged EODHD listing carrying ``currency`` for ``symbol``.

    ``listing.currency`` is NOT NULL with no fallback, so every listing must be
    created from a provider payload that carries a currency. Tests that drive
    ``refresh_symbol``/``upsert_price``/``replace_facts`` for an uncatalogued
    symbol seed the listing here first so the strict creation path is satisfied.
    """

    ticker, suffix = symbol.split(".")
    seed_exchange(db_path, suffix, currency=currency)
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    repo.replace_for_exchange(
        "EODHD",
        suffix,
        [{"Code": ticker, "Type": "Common Stock", "Currency": currency}],
    )
    return repo


def test_eodhd_provider_parses_response() -> None:
    payload = [
        {"date": "2024-03-01", "Close": "200.50", "Volume": "12345"},
        {"date": "2024-03-04", "Close": "205.75", "Volume": "9000"},
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)

    data = provider.latest_price("mcd.us")

    assert data.price == 205.75
    assert data.as_of == "2024-03-04"
    assert data.volume == 9000
    assert data.symbol == "MCD.US"
    first_params = session.calls[0][1]
    assert first_params is not None
    assert "from" in first_params


def test_market_data_service_persists_prices(tmp_path: Path) -> None:
    class DummyProvider:
        def latest_price(self, symbol: str) -> PriceData:
            return PriceData(
                symbol=symbol,
                price=150.0,
                as_of="2024-03-02",
                volume=500,
                currency=None,
            )

    _seed_listing(tmp_path / "data.db", "AAPL.US", currency="USD")
    service = MarketDataService(db_path=tmp_path / "data.db", provider=DummyProvider())

    result = service.refresh_symbol("AAPL.US")

    assert result.price == 150.0

    repo = MarketDataRepository(tmp_path / "data.db")
    latest_snapshot = repo.latest_snapshot("AAPL.US")
    assert latest_snapshot is not None
    assert latest_snapshot.as_of == "2024-03-02"
    assert latest_snapshot.price == 150.0
    latest = repo.latest_price("AAPL.US")
    assert latest is not None
    assert latest[0] == "2024-03-02"
    assert latest[1] == 150.0


@pytest.mark.parametrize(
    ("symbol", "exchange", "quote_currency", "base_currency", "price", "major_price"),
    [
        ("SHEL.LSE", "LSE", "GBX", "GBP", 2783.5, 27.835),
        ("NPN.JSE", "JSE", "ZAC", "ZAR", 23750.0, 237.5),
        ("BCOM.TA", "TA", "ILA", "ILS", 1234.0, 12.34),
    ],
)
def test_market_data_service_stores_major_price_for_subunit_quote(
    tmp_path: Path,
    symbol: str,
    exchange: str,
    quote_currency: str,
    base_currency: str,
    price: float,
    major_price: float,
) -> None:
    # A subunit-quoted listing (GBX/ZAC/ILA) must be stored in its MAJOR
    # currency: the raw quote price is divided by 100 and the snapshot reports
    # the base currency, so subunits never cross the data boundary.
    class DummyProvider:
        def latest_price(self, requested_symbol: str) -> PriceData:
            return PriceData(
                symbol=requested_symbol,
                price=price,
                as_of="2024-03-04",
                volume=100,
                currency=None,
            )

    db_path = tmp_path / f"{quote_currency.lower()}-market-data.db"
    catalog_repo = SupportedTickerRepository(db_path)
    catalog_repo.initialize_schema()
    seed_exchange(db_path, exchange, currency=quote_currency)
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

    service = MarketDataService(
        db_path=db_path,
        provider=DummyProvider(),
        config=DummyConfig(),
    )

    service.refresh_symbol(symbol)

    snapshot = MarketDataRepository(db_path).latest_snapshot(symbol)
    assert snapshot is not None
    assert snapshot.price == pytest.approx(major_price)
    assert snapshot.currency == base_currency


def test_eodhd_provider_preserves_gbx_quote_price() -> None:
    payload = [
        {"date": "2024-03-01", "Close": "99.0", "Volume": "1000", "currency": "GBX"},
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)

    data = provider.latest_price("SHEL.LSE")

    assert data.price == 99.0
    assert data.currency == "GBX"


def test_eodhd_provider_preserves_zac_quote_price() -> None:
    payload = [
        {"date": "2024-03-01", "Close": "23750.0", "Volume": "1000", "currency": "ZAC"},
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)

    data = provider.latest_price("ABG.JSE")

    assert data.price == 23750.0
    assert data.currency == "ZAC"


def test_eodhd_provider_infers_gbx_by_suffix_when_currency_missing() -> None:
    payload = [
        {"date": "2024-03-01", "Close": "2783.5", "Volume": "1000"},
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)

    data = provider.latest_price("SHEL.LSE")

    assert data.price == 2783.5
    assert data.currency == "GBX"


def test_market_data_service_stores_large_price_change_without_guard(
    tmp_path: Path,
) -> None:
    # The suspicious-price-jump guard was removed with the market_cap column:
    # market value is derived by pairing a share-count fact with the price as of
    # that fact's date, so there is no cross-snapshot value jump to police. A
    # large price move between refreshes is therefore stored without error
    # (previously this raised SuspiciousMarketPriceChangeError).
    class DummyProvider:
        def latest_price(self, symbol: str) -> PriceData:
            return PriceData(
                symbol=symbol,
                price=5298.0,
                as_of="2026-03-20",
                volume=0,
                currency="USD",
            )

    db_path = tmp_path / "no-guard.db"
    _seed_listing(db_path, "ATXS.US", currency="USD")
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("ATXS.US", "2025-12-22", 12.92, currency="USD")

    service = MarketDataService(
        db_path=db_path, provider=DummyProvider(), config=DummyConfig()
    )
    prepared = service.refresh_symbol("ATXS.US")
    assert prepared.price == 5298.0

    latest_snapshot = market_repo.latest_snapshot("ATXS.US")
    assert latest_snapshot is not None
    assert latest_snapshot.as_of == "2026-03-20"
    assert latest_snapshot.price == 5298.0


def test_eodhd_provider_parses_bulk_exchange_response() -> None:
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
    provider = EODHDProvider(api_key="demo", session=session)

    data = provider.latest_prices_for_exchange("LSE")

    assert data["AAA.LSE"].price == 10.5
    assert data["AAA.LSE"].as_of == "2024-03-04"
    assert data["SHEL.LSE"].price == 2783.5
    assert data["SHEL.LSE"].currency == "GBX"
    assert session.calls[0][0].endswith("/api/eod-bulk-last-day/LSE")


def test_market_data_service_prepare_price_data_uses_currency_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class DummyProvider:
        def latest_price(self, symbol: str) -> PriceData:
            return PriceData(
                symbol=symbol,
                price=2783.5,
                as_of="2024-03-04",
                volume=100,
                currency=None,
            )

    db_path = tmp_path / "hint.db"
    _seed_listing(db_path, "SHEL.LSE", currency="GBX")
    service = MarketDataService(
        db_path=db_path,
        provider=DummyProvider(),
        config=DummyConfig(),
    )

    def _fail_fetch_currency(symbol: str, provider: str | None = None) -> str | None:
        raise AssertionError(
            "fetch_currency should not be used when a currency hint exists"
        )

    # Replace the bound method so any accidental currency lookup fails loudly;
    # monkeypatch keeps the override typed (no method-assign) and auto-reverts.
    monkeypatch.setattr(
        service.supported_ticker_repo, "fetch_currency", _fail_fetch_currency
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
    assert prepared.currency == "GBP"


def test_market_data_service_prepare_price_data_uses_ila_currency_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class DummyProvider:
        def latest_price(self, symbol: str) -> PriceData:
            return PriceData(
                symbol=symbol,
                price=1234.0,
                as_of="2024-03-04",
                volume=100,
                currency=None,
            )

    service = MarketDataService(
        db_path=tmp_path / "ila-hint.db",
        provider=DummyProvider(),
        config=DummyConfig(),
    )

    def _fail_fetch_currency(symbol: str, provider: str | None = None) -> str | None:
        raise AssertionError(
            "fetch_currency should not be used when a currency hint exists"
        )

    # See the GBX variant: monkeypatch keeps the stubbed method properly typed.
    monkeypatch.setattr(
        service.supported_ticker_repo, "fetch_currency", _fail_fetch_currency
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
