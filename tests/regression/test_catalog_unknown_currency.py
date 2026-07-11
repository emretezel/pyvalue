"""Regression: catalog writers must survive non-ISO-shaped provider currencies.

EODHD's ``/exchanges-list/`` publishes ``"Currency": "Unknown"`` for its
currency-less virtual exchanges (FOREX / GBOND / MONEY). Migration 057 added
the 3-uppercase-letter shape CHECK to ``provider_exchange.currency`` and
coerced the legacy ``'UNKNOWN'`` rows to NULL, but the writer
(``ExchangeProviderRepository.replace_for_provider``) kept passing the payload
value through untouched, so the first refresh after that migration died with
``sqlite3.IntegrityError: CHECK constraint failed``. The ticker writer had the
same latent hole: a malformed payload currency (``'Unknown'`` upper-cases to
``'UNKNOWN'``) would trip ``listing.currency``'s CHECK instead of taking the
documented skip-and-report path for currency-less entries.

Both writers now funnel payload currencies through
``currency.shaped_currency_code``: the exchange catalog stores NULL (the column
is nullable -- exactly migration 057's cleanup), and the ticker catalog skips
the row and reports it via ``skipped_no_currency``. These tests fail with an
``IntegrityError`` on the unguarded writers.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path

from conftest import seed_exchange
from pyvalue.persistence.storage import (
    ExchangeProviderRepository,
    SupportedTickerRepository,
)


def test_replace_for_provider_coerces_unknown_currency_to_null(
    tmp_path: Path,
) -> None:
    repo = ExchangeProviderRepository(tmp_path / "catalog.db")
    repo.initialize_schema()

    # Mirrors the live EODHD payload shape: three virtual exchanges with the
    # 'Unknown' placeholder, one clean row, and one row whose currency needs
    # strip+uppercase to satisfy the schema shape.
    stored = repo.replace_for_provider(
        "EODHD",
        [
            {"Code": "US", "Name": "USA Stocks", "Country": "USA", "Currency": "USD"},
            {
                "Code": "PA",
                "Name": "Paris Exchange",
                "Country": "France",
                "Currency": " eur ",
            },
            {
                "Code": "FOREX",
                "Name": "FOREX",
                "Country": "Unknown",
                "Currency": "Unknown",
            },
            {
                "Code": "GBOND",
                "Name": "Government Bonds",
                "Country": "Unknown",
                "Currency": "Unknown",
            },
            {
                "Code": "MONEY",
                "Name": "Money Market Virtual Exchange",
                "Country": "Unknown",
                "Currency": "Unknown",
            },
        ],
    )

    assert stored == 5
    by_code = {row.code: row.currency for row in repo.list_all("EODHD")}
    assert by_code == {
        "US": "USD",
        "PA": "EUR",
        "FOREX": None,
        "GBOND": None,
        "MONEY": None,
    }


def test_replace_for_exchange_skips_unknown_currency_ticker(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    seed_exchange(db_path, "US")

    result = SupportedTickerRepository(db_path).replace_for_exchange(
        "EODHD",
        "US",
        [
            {"Code": "AAA", "Name": "Alpha Corp", "Currency": "USD"},
            {"Code": "BBB", "Name": "Beta Corp", "Currency": "Unknown"},
        ],
    )

    # The malformed-currency ticker takes the currency-less skip path instead
    # of crashing the whole exchange slice on listing.currency's CHECK.
    assert result.inserted == 1
    assert result.skipped_no_currency == ("BBB",)
    tickers = SupportedTickerRepository(db_path).list_for_provider("EODHD")
    assert [ticker.provider_ticker for ticker in tickers] == ["AAA"]
