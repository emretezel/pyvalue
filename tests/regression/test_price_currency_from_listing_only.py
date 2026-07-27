"""Regression: a price's currency comes from the listing, never from a guess.

The EODHD price parser used to derive a currency from two things that are not
evidence about the listing -- the exchange suffix (``EXCHANGE_SUBUNIT_HINTS``
mapped ``LSE`` to ``GBX``) and the price magnitude (``price > 100``). That
derived value was tried *before* the listing's own currency, so it silently
overrode it and every non-pence London quote above 100 was stored at 1/100 of
its real value.

The headline case is real: ``0HS2.LSE`` is Cadence Design Systems' London line,
which EODHD's supported-ticker catalog labels ``"Currency": "USD"``. The payload
in :data:`CADENCE_LSE_BULK_ENTRY` is the verbatim ``eod-bulk-last-day/LSE`` row
captured from the live API on 2026-07-27; note it carries no currency field at
all, which is true of both EOD endpoints. On the buggy code the 341.5 close was
stored as 3.415 GBP; correct is 341.5 USD (its NASDAQ line closed at 330.11 the
same week).

Each case below fails on the pre-fix code and passes on the fix.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

import pytest

from pyvalue.marketdata.eodhd import EODHDProvider
from pyvalue.marketdata.service import prepare_price_data

from conftest import DummyEODSession

# Verbatim live payload -- see the module docstring. Kept whole rather than
# trimmed so the test keeps pinning the real vendor row shape.
CADENCE_LSE_BULK_ENTRY: Dict[str, Any] = {
    "code": "0HS2",
    "exchange_short_name": "LSE",
    "date": "2026-07-27",
    "open": 333,
    "high": 342.46,
    "low": 327,
    "close": 341.5,
    "adjusted_close": 341.5,
    "volume": 738,
}


def _bulk_quote(entry: Dict[str, Any], exchange: str, symbol: str) -> Any:
    """Run one bulk-feed entry through the real provider parsing path.

    The feed keys rows by bare code, so ``code`` is set from ``symbol`` to keep
    the two consistent however a caller varies the payload.
    """

    row = {**entry, "code": symbol.split(".")[0], "exchange_short_name": exchange}
    provider = EODHDProvider(api_key="demo", session=DummyEODSession([row]))
    return provider.latest_prices_for_exchange(exchange)[symbol]


def test_usd_quoted_london_line_is_not_divided_by_100() -> None:
    # The live Cadence payload, end to end. Pre-fix: 3.415 GBP.
    quote = _bulk_quote(CADENCE_LSE_BULK_ENTRY, "LSE", "0HS2.LSE")
    assert quote.price == 341.5

    prepared = prepare_price_data("0HS2.LSE", quote, "USD")

    assert prepared.price == pytest.approx(341.5)
    assert prepared.currency == "USD"


@pytest.mark.parametrize("close", [95.0, 105.0])
def test_scale_does_not_depend_on_price_magnitude(close: float) -> None:
    # The old rule fired only above 100, so one listing's stored series jumped
    # 100x whenever its quote crossed that line (0A7O.LSE went 1.3064 -> 90.69).
    # Crossing the threshold must now change nothing but the number itself.
    entry = {**CADENCE_LSE_BULK_ENTRY, "close": close, "adjusted_close": close}

    prepared = prepare_price_data(
        "0HS2.LSE", _bulk_quote(entry, "LSE", "0HS2.LSE"), "USD"
    )

    assert prepared.price == pytest.approx(close)
    assert prepared.currency == "USD"


@pytest.mark.parametrize(
    ("exchange", "symbol", "listing_currency"),
    [
        ("LSE", "0A36.LSE", "EUR"),
        ("JSE", "CPP.JSE", "ZAR"),
        ("TA", "ARBE.TA", "ILS"),
    ],
)
def test_venue_does_not_impose_a_subunit(
    exchange: str, symbol: str, listing_currency: str
) -> None:
    # LSE, JSE and TA each carried a subunit hint keyed on the venue. Those
    # exchanges list plenty of majors-quoted lines (LSE alone: 2,253 USD and
    # 1,135 EUR against 2,467 GBX), so venue identity must not scale anything.
    entry = {**CADENCE_LSE_BULK_ENTRY, "close": 250.0, "adjusted_close": 250.0}

    prepared = prepare_price_data(
        symbol, _bulk_quote(entry, exchange, symbol), listing_currency
    )

    assert prepared.price == pytest.approx(250.0)
    assert prepared.currency == listing_currency


@pytest.mark.parametrize(
    ("exchange", "symbol", "listing_currency", "close", "major", "base"),
    [
        ("LSE", "SHEL.LSE", "GBX", 2783.5, 27.835, "GBP"),
        ("JSE", "NPN.JSE", "ZAC", 23750.0, 237.5, "ZAR"),
        ("TA", "BCOM.TA", "ILA", 1234.0, 12.34, "ILS"),
    ],
)
def test_declared_subunits_still_collapse(
    exchange: str,
    symbol: str,
    listing_currency: str,
    close: float,
    major: float,
    base: str,
) -> None:
    # The other half of the fix: removing the guess must not stop genuine
    # subunit listings collapsing. These do it purely on the declared listing
    # currency, with the provider contributing no currency at all.
    entry = {**CADENCE_LSE_BULK_ENTRY, "close": close, "adjusted_close": close}

    prepared = prepare_price_data(
        symbol, _bulk_quote(entry, exchange, symbol), listing_currency
    )

    assert prepared.price == pytest.approx(major)
    assert prepared.currency == base


def test_provider_quote_cannot_carry_a_currency() -> None:
    # The structural guarantee behind all of the above: the parser's output type
    # has no currency field, so no future rule can put one there.
    quote = _bulk_quote(CADENCE_LSE_BULK_ENTRY, "LSE", "0HS2.LSE")

    assert not hasattr(quote, "currency")
    fields: Tuple[str, ...] = tuple(quote.__dataclass_fields__)
    assert fields == ("symbol", "price", "as_of", "volume")
