"""Regression: a self-declaring depositary receipt must not name the company.

Author: Emre Tezel

The bug
-------
Making the reconciler the single writer of ``issuer.name`` exposed a tie-break
the per-listing writers had been masking: Apple's issuer became
**"APPLE INC. CDR"**. ``APC8.F`` is a Frankfurt Canadian-depositary-receipt line
whose payload says ``PrimaryTicker: APC8.F`` -- a receipt self-declares primary
exactly like the real line -- and it has a lower listing id than ``AAPL.US``.
With both classified primary and neither tagged ``Domestic Primary`` (that tag
is for multi-class companies), the lowest id won. 223 companies were renamed
after a receipt in the same pass: Amazon, Capgemini, Mercedes-Benz, Coca-Cola
HBC among them.

The evidence that settles it is already in the payloads: twelve of Apple's
fourteen lines publish ``PrimaryTicker: AAPL.US``, while nothing points at the
receipt but itself. The grouper now prefers the line the group's other listings
defer to, and this test fails on the previous rule with the CDR's name.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from conftest import seed_exchange, seed_raw_fundamentals
from pyvalue.persistence.storage import (
    FundamentalsRepository,
    SupportedTickerRepository,
    UniverseReconciler,
)

_LEI = "HWUPKR0MPOU8FGXBT394"


def _payload(
    code: str, exchange: str, name: str, primary_ticker: str, home: str = ""
) -> dict[str, object]:
    general: dict[str, object] = {
        "Code": code,
        "Name": name,
        "LEI": _LEI,
        "PrimaryTicker": primary_ticker,
    }
    if home:
        general["HomeCategory"] = home
    return {"General": general}


def _issuer_names(db_path: Path) -> dict[str, str]:
    with sqlite3.connect(db_path) as conn:
        return {
            str(symbol): str(name)
            for symbol, name in conn.execute(
                """
                SELECT l.symbol || '.' || e.exchange_code, i.name
                FROM listing l
                JOIN issuer i ON i.issuer_id = l.issuer_id
                JOIN "exchange" e ON e.exchange_id = l.exchange_id
                """
            )
        }


def test_company_is_named_after_the_line_its_peers_point_at(tmp_path: Path) -> None:
    db_path = tmp_path / "apple-cdr.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "F", "US")
    # Frankfurt is catalogued first, so the receipt gets the lower listing id --
    # the ordering the old tie-break lost on.
    ticker_repo.replace_for_exchange(
        "EODHD",
        "F",
        [
            {"Code": "APC8", "Name": "APPLE INC. CDR", "Currency": "EUR"},
            {"Code": "APC", "Name": "Apple Inc", "Currency": "EUR"},
        ],
    )
    ticker_repo.replace_for_exchange(
        "EODHD", "US", [{"Code": "AAPL", "Name": "Apple Inc.", "Currency": "USD"}]
    )
    FundamentalsRepository(db_path).initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "APC8.F",
        _payload("APC8", "F", "APPLE INC. CDR", "APC8.F"),
        exchange="F",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "APC.F",
        _payload("APC", "F", "Apple Inc", "AAPL.US"),
        exchange="F",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAPL.US",
        _payload("AAPL", "US", "Apple Inc.", "AAPL.US", home="Domestic"),
        exchange="US",
    )

    UniverseReconciler(db_path).reconcile()

    names = _issuer_names(db_path)
    assert set(names) == {"APC8.F", "APC.F", "AAPL.US"}
    assert set(names.values()) == {"Apple Inc."}
