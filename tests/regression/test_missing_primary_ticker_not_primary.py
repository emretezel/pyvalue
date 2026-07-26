"""Regression: a missing ``PrimaryTicker`` must not classify a listing primary.

Author: Emre Tezel

The bug
-------
``SecurityListingStatusRepository`` read one EODHD field, ``General.
PrimaryTicker``, and treated its *absence* as proof of primacy::

    if primary_provider_symbol is None:
        is_primary_listing = True          # <- fail-open

EODHD leaves that field null on ~31% of payloads, so 22,452 of the 57,001
listings the QARP screen ran over were "primary" only because nothing said
otherwise. The screen consequently returned ADRs and foreign cross-listings
next to the real lines -- 54 of its 154 passers were duplicates.

These tests fail on the buggy code (every listing below came back ``primary``)
and pass on the rule set that replaced it.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from conftest import seed_raw_fundamentals, seed_exchange
from pyvalue.persistence.storage import (
    FundamentalsRepository,
    SecurityListingStatusRepository,
    SupportedTickerRepository,
)


def _catalog(db_path: Path, exchange: str, rows: list[dict[str, object]]) -> None:
    """Catalog listings through the production refresh path."""

    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    seed_exchange(db_path, exchange)
    repo.replace_for_exchange("EODHD", exchange, rows)


def _statuses(db_path: Path) -> dict[str, str]:
    with sqlite3.connect(db_path) as conn:
        return {
            str(symbol): str(status)
            for symbol, status in conn.execute(
                """
                SELECT l.symbol || '.' || e.exchange_code, l.primary_listing_status
                FROM listing l
                JOIN "exchange" e ON e.exchange_id = l.exchange_id
                """
            )
        }


def test_missing_primary_ticker_no_longer_defaults_to_primary(
    tmp_path: Path,
) -> None:
    """An LSE international-order-book line inherits its ISIN peer's answer.

    ``0K10.LSE`` is Mettler-Toledo's London line: no ``PrimaryTicker`` of its
    own, but it shares ``US5926881054`` with ``MTD.US``, which names itself. The
    peer group is what makes the answer available.
    """

    db_path = tmp_path / "missing-primary-ticker.db"
    _catalog(
        db_path,
        "US",
        [
            {
                "Code": "MTD",
                "Name": "Mettler-Toledo International Inc.",
                "Type": "Common Stock",
                "Currency": "USD",
                "Isin": "US5926881054",
            }
        ],
    )
    _catalog(
        db_path,
        "LSE",
        [
            {
                "Code": "0K10",
                "Name": "Mettler-Toledo International Inc.",
                "Type": "Common Stock",
                "Currency": "USD",
                "Isin": "US5926881054",
            }
        ],
    )

    FundamentalsRepository(db_path).initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "MTD.US",
        {"General": {"Name": "Mettler-Toledo", "PrimaryTicker": "MTD.US"}},
        exchange="US",
    )
    # Faithful to the live payload: no PrimaryTicker, no HomeCategory.
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "0K10.LSE",
        {"General": {"Name": "Mettler-Toledo International Inc."}},
        exchange="LSE",
    )

    SecurityListingStatusRepository(db_path).reconcile_eodhd_fundamentals()

    statuses = _statuses(db_path)
    assert statuses["MTD.US"] == "primary"
    assert statuses["0K10.LSE"] == "secondary"


def test_evidence_free_listing_is_unknown_not_primary(tmp_path: Path) -> None:
    """With no evidence at all the answer is ``unknown``, never ``primary``.

    ``unknown`` still passes primary-only scopes, so nothing is lost -- but the
    listing is no longer *asserted* to be a primary line on no evidence.
    """

    db_path = tmp_path / "evidence-free.db"
    _catalog(
        db_path,
        "BK",
        [
            {
                "Code": "NETBAY",
                "Name": "Netbay Public Company Limited",
                "Type": "Common Stock",
                "Currency": "THB",
            }
        ],
    )
    FundamentalsRepository(db_path).initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "NETBAY.BK",
        {"General": {"Name": "Netbay Public Company Limited"}},
        exchange="BK",
    )

    SecurityListingStatusRepository(db_path).reconcile_eodhd_fundamentals()

    assert _statuses(db_path)["NETBAY.BK"] == "unknown"


def test_adr_is_secondary_despite_naming_itself_primary(tmp_path: Path) -> None:
    """EODHD points an ADR's ``PrimaryTicker`` at the ADR itself.

    Reading that field alone -- however carefully -- classifies every ADR as
    primary. ``HomeCategory`` is the only signal that catches this, and it has
    to be consulted first.
    """

    db_path = tmp_path / "adr-self-primary.db"
    _catalog(
        db_path,
        "US",
        [
            {
                "Code": "DNLMY",
                "Name": "Dunelm Group PLC ADR",
                "Type": "Common Stock",
                "Currency": "USD",
                "Isin": "US26543P1030",
            }
        ],
    )
    FundamentalsRepository(db_path).initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "DNLMY.US",
        {
            "General": {
                "Name": "Dunelm Group PLC ADR",
                "PrimaryTicker": "DNLMY.US",
                "HomeCategory": "ADR",
                "Exchange": "PINK",
            }
        },
        exchange="US",
    )

    SecurityListingStatusRepository(db_path).reconcile_eodhd_fundamentals()

    assert _statuses(db_path)["DNLMY.US"] == "secondary"


def test_narrow_scope_still_sees_isin_peers(tmp_path: Path) -> None:
    """A single-symbol reconcile must not lose the peer rules.

    ``0K10.LSE`` alone looks evidence-free; beside ``MTD.US`` it is plainly the
    secondary line. The scope expands to whole ISIN groups for context while
    writing only the requested listing.
    """

    db_path = tmp_path / "narrow-scope-peers.db"
    _catalog(
        db_path,
        "US",
        [
            {
                "Code": "MTD",
                "Name": "Mettler-Toledo",
                "Type": "Common Stock",
                "Currency": "USD",
                "Isin": "US5926881054",
            }
        ],
    )
    _catalog(
        db_path,
        "LSE",
        [
            {
                "Code": "0K10",
                "Name": "Mettler-Toledo",
                "Type": "Common Stock",
                "Currency": "USD",
                "Isin": "US5926881054",
            }
        ],
    )
    FundamentalsRepository(db_path).initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "MTD.US",
        {"General": {"Name": "Mettler-Toledo", "PrimaryTicker": "MTD.US"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "0K10.LSE",
        {"General": {"Name": "Mettler-Toledo"}},
        exchange="LSE",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE listing SET primary_listing_status = 'unknown'")

    records = SecurityListingStatusRepository(db_path).reconcile_eodhd_fundamentals(
        provider_symbols=["0K10.LSE"]
    )

    # Only the requested listing is written; the peer supplied context and kept
    # its own stored status.
    assert [record.provider_symbol for record in records] == ["0K10.LSE"]
    statuses = _statuses(db_path)
    assert statuses["0K10.LSE"] == "secondary"
    assert statuses["MTD.US"] == "unknown"
