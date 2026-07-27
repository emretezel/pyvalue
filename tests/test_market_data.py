"""Tests for market data providers and services.

Author: Emre Tezel
"""

from pathlib import Path

import pytest

from pyvalue.config import Config
from pyvalue.marketdata.eodhd import EODHDProvider
from pyvalue.marketdata.base import MarketDataUpdate, PriceQuote
from pyvalue.marketdata.service import MarketDataService, prepare_price_data
from pyvalue.persistence.storage import (
    MarketDataRepository,
    SupportedTickerRepository,
)

from conftest import (
    DummyEODSession,
    resolve_listing_id,
    seed_exchange,
    seed_price,
)


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
    ``persist_updates``/``upsert_price``/``replace_facts`` for an uncatalogued
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
        def latest_price(self, symbol: str) -> PriceQuote:
            return PriceQuote(
                symbol=symbol,
                price=150.0,
                as_of="2024-03-02",
                volume=500,
            )

    db_path = tmp_path / "data.db"
    ticker_repo = _seed_listing(db_path, "AAPL.US", currency="USD")
    aapl = next(
        row
        for row in ticker_repo.list_for_provider("EODHD", exchange_codes=["US"])
        if row.symbol == "AAPL.US"
    )
    service = MarketDataService(db_path=db_path, provider=DummyProvider())

    # Mirror the live update-market-data path: prepare the quoted price, build a
    # natural-identity MarketDataUpdate, and persist the batch.
    prepared = prepare_price_data("AAPL.US", service.provider.latest_price(""), "USD")
    assert prepared.price == 150.0
    service.persist_updates(
        [
            MarketDataUpdate(
                security_id=aapl.security_id,
                symbol="AAPL.US",
                as_of=prepared.as_of,
                price=prepared.price,
                volume=prepared.volume,
                currency=prepared.currency,
            )
        ]
    )

    repo = MarketDataRepository(db_path)
    latest_snapshot = repo.latest_snapshot_by_id(aapl.security_id)
    assert latest_snapshot is not None
    assert latest_snapshot.as_of == "2024-03-02"
    assert latest_snapshot.price == 150.0
    latest = repo.latest_price_by_id(aapl.security_id)
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
    # the base currency, so subunits never cross the data boundary. The listing
    # currency alone drives this -- the provider supplies no currency at all.
    class DummyProvider:
        def latest_price(self, requested_symbol: str) -> PriceQuote:
            return PriceQuote(
                symbol=requested_symbol,
                price=price,
                as_of="2024-03-04",
                volume=100,
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

    # prepare_price_data collapses the subunit quote to its MAJOR currency; this
    # is the normalization the live update-market-data path relies on before
    # persisting, so we assert the prepared PriceData directly.
    prepared = prepare_price_data(
        symbol,
        service.provider.latest_price(symbol),
        quote_currency,
    )
    assert prepared.price == pytest.approx(major_price)
    assert prepared.currency == base_currency


def test_eodhd_provider_quote_carries_no_currency() -> None:
    # The provider parses a number and a date. It cannot name a currency --
    # PriceQuote has no such field -- so no exchange suffix or price magnitude
    # can make it scale the value. The quote unit is the listing's.
    payload = [
        {"date": "2024-03-01", "Close": "2783.5", "Volume": "1000"},
    ]
    session = DummyEODSession(payload)
    provider = EODHDProvider(api_key="demo", session=session)

    quote = provider.latest_price("SHEL.LSE")

    assert quote.price == 2783.5
    assert not hasattr(quote, "currency")


def test_market_data_service_stores_large_price_change_without_guard(
    tmp_path: Path,
) -> None:
    # The suspicious-price-jump guard was removed with the market_cap column:
    # market value is derived by pairing a share-count fact with the price as of
    # that fact's date, so there is no cross-snapshot value jump to police. A
    # large price move between refreshes is therefore stored without error
    # (previously this raised SuspiciousMarketPriceChangeError).
    class DummyProvider:
        def latest_price(self, symbol: str) -> PriceQuote:
            return PriceQuote(
                symbol=symbol,
                price=5298.0,
                as_of="2026-03-20",
                volume=0,
            )

    db_path = tmp_path / "no-guard.db"
    ticker_repo = _seed_listing(db_path, "ATXS.US", currency="USD")
    atxs = next(
        row
        for row in ticker_repo.list_for_provider("EODHD", exchange_codes=["US"])
        if row.symbol == "ATXS.US"
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    seed_price(db_path, "ATXS.US", "2025-12-22", 12.92, currency="USD")

    service = MarketDataService(
        db_path=db_path, provider=DummyProvider(), config=DummyConfig()
    )
    # A large jump from the prior 12.92 to 5298.0 is persisted without error.
    prepared = prepare_price_data(
        "ATXS.US", service.provider.latest_price("ATXS.US"), "USD"
    )
    assert prepared.price == 5298.0
    service.persist_updates(
        [
            MarketDataUpdate(
                security_id=atxs.security_id,
                symbol="ATXS.US",
                as_of=prepared.as_of,
                price=prepared.price,
                volume=prepared.volume,
                currency=prepared.currency,
            )
        ]
    )

    latest_snapshot = market_repo.latest_snapshot_by_id(atxs.security_id)
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
    assert session.calls[0][0].endswith("/api/eod-bulk-last-day/LSE")


def test_prepare_price_data_is_decided_solely_by_the_listing_currency() -> None:
    # The listing currency is the only input that decides scale. The identical
    # quote -- same number, same venue suffix -- resolves two different ways
    # purely because the two listings declare different quote units.
    quote = PriceQuote(symbol="X.LSE", price=2783.5, as_of="2024-03-04", volume=100)

    as_pence = prepare_price_data("X.LSE", quote, "GBX")
    assert as_pence.price == pytest.approx(27.835)
    assert as_pence.currency == "GBP"

    as_dollars = prepare_price_data("X.LSE", quote, "USD")
    assert as_dollars.price == pytest.approx(2783.5)
    assert as_dollars.currency == "USD"


def _seed_price_history(db_path: Path, symbol: str, rows: dict[str, float]) -> int:
    """Seed closes for ``symbol`` and return its listing id."""

    for as_of, price in rows.items():
        seed_price(db_path, symbol, as_of, price)
    return resolve_listing_id(db_path, symbol)


def test_snapshot_near_date_prefers_the_exact_date(tmp_path: Path) -> None:
    db_path = tmp_path / "near-exact.db"
    _seed_listing(db_path, "AAA.US")
    listing_id = _seed_price_history(
        db_path,
        "AAA.US",
        {"2026-03-25": 95.0, "2026-03-29": 100.0, "2026-04-01": 105.0},
    )

    snapshot = MarketDataRepository(db_path).snapshot_near_date_by_id(
        listing_id, "2026-03-29", max_distance_days=10
    )

    assert snapshot is not None
    assert snapshot.as_of == "2026-03-29"
    assert snapshot.price == 100.0
    assert snapshot.currency == "USD"


def test_snapshot_near_date_uses_the_nearest_row_either_side(tmp_path: Path) -> None:
    db_path = tmp_path / "near-side.db"
    _seed_listing(db_path, "AAA.US")
    listing_id = _seed_price_history(
        db_path,
        "AAA.US",
        {"2026-03-20": 95.0, "2026-04-02": 105.0},
    )
    repo = MarketDataRepository(db_path)

    # 2026-03-29: before-row is 9 days away, after-row 4 -> the after row wins.
    after = repo.snapshot_near_date_by_id(
        listing_id, "2026-03-29", max_distance_days=10
    )
    assert after is not None
    assert after.as_of == "2026-04-02"
    # 2026-03-23: before-row 3 days, after-row 10 -> the before row wins.
    before = repo.snapshot_near_date_by_id(
        listing_id, "2026-03-23", max_distance_days=10
    )
    assert before is not None
    assert before.as_of == "2026-03-20"


def test_snapshot_near_date_equidistant_tie_prefers_on_or_before(
    tmp_path: Path,
) -> None:
    # The provider cap is computed from the last close *before* its refresh
    # stamp, so an equidistant tie must resolve backwards.
    db_path = tmp_path / "near-tie.db"
    _seed_listing(db_path, "AAA.US")
    listing_id = _seed_price_history(
        db_path,
        "AAA.US",
        {"2026-03-27": 95.0, "2026-03-31": 105.0},
    )

    snapshot = MarketDataRepository(db_path).snapshot_near_date_by_id(
        listing_id, "2026-03-29", max_distance_days=10
    )

    assert snapshot is not None
    assert snapshot.as_of == "2026-03-27"


def test_snapshot_near_date_misses_outside_the_window(tmp_path: Path) -> None:
    db_path = tmp_path / "near-miss.db"
    _seed_listing(db_path, "AAA.US")
    listing_id = _seed_price_history(db_path, "AAA.US", {"2026-03-01": 95.0})
    repo = MarketDataRepository(db_path)

    assert (
        repo.snapshot_near_date_by_id(listing_id, "2026-03-29", max_distance_days=10)
        is None
    )
    # Widening the window brings the row back into range.
    within = repo.snapshot_near_date_by_id(
        listing_id, "2026-03-29", max_distance_days=30
    )
    assert within is not None
    assert within.as_of == "2026-03-01"


def test_snapshot_near_date_returns_none_without_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "near-empty.db"
    _seed_listing(db_path, "AAA.US")
    listing_id = resolve_listing_id(db_path, "AAA.US")

    assert (
        MarketDataRepository(db_path).snapshot_near_date_by_id(
            listing_id, "2026-03-29", max_distance_days=10
        )
        is None
    )


def test_snapshot_near_date_reports_the_collapsed_listing_currency(
    tmp_path: Path,
) -> None:
    # Stored prices are major-unit; the reported currency must be the listing
    # currency collapsed to its base (GBX -> GBP), mirroring the latest-snapshot
    # reader so no second subunit collapse can occur downstream.
    db_path = tmp_path / "near-gbx.db"
    _seed_listing(db_path, "AAA.LSE", currency="GBX")
    listing_id = _seed_price_history(db_path, "AAA.LSE", {"2026-03-29": 3.48})

    snapshot = MarketDataRepository(db_path).snapshot_near_date_by_id(
        listing_id, "2026-03-29", max_distance_days=10
    )

    assert snapshot is not None
    assert snapshot.currency == "GBP"
    assert snapshot.price == 3.48
