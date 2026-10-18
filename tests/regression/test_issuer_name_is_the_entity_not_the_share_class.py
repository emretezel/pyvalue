"""Regression: a multi-class company's issuer is named after the company.

Author: Emre Tezel

The bug
-------
Alphabet's sixteen listings were correctly grouped onto one ``issuer`` row by
their shared LEI, but that row was named **"Alphabet Inc Class C"**. Two defects
produced it, and each on its own was enough to keep it there:

1. The grouper named a merged issuer after its *primary* listing and broke ties
   by lowest listing id. Both US share classes self-declare ``PrimaryTicker``
   and are therefore both primary, so GOOG (the lower id) won and the Class A
   line displayed as Class C. Bank of America was named after one of its
   preferred series the same way.
2. ``normalize-fundamentals``, ``refresh-security-metadata`` and
   ``refresh-supported-tickers`` each rewrote ``issuer.name`` with the *security*
   name of whichever listing they processed last, so even a correct reconcile
   verdict flipped back on the next run.

The fix names the group after the line EODHD itself marks ``Domestic Primary``,
strips the share-class designator from that security name, and makes the
reconciler the only writer of ``issuer.name`` after the row is created. These
tests exercise the whole path -- catalog, payloads, reconcile, then the two
writers that used to rename -- and fail on the old code at the first assertion.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from conftest import seed_exchange, seed_raw_fundamentals
from pyvalue.persistence.storage import (
    FundamentalsRepository,
    SecurityMetadataUpdate,
    SecurityRepository,
    SupportedTickerRepository,
    UniverseReconciler,
)

_LEI = "5493006MHB84DD0ZWV18"

# The vendor's own view of Alphabet's two US lines. Note the ids: GOOG is
# catalogued first so the old lowest-id tie-break would pick its Class C name.
_GOOG_ROW = {
    "Code": "GOOG",
    "Name": "Alphabet Inc Class C",
    "Type": "Common Stock",
    "Currency": "USD",
    "Isin": "US02079K1079",
}
_GOOGL_ROW = {
    "Code": "GOOGL",
    "Name": "Alphabet Inc Class A",
    "Type": "Common Stock",
    "Currency": "USD",
    "Isin": "US02079K3059",
}


def _payload(code: str, name: str, home_category: str) -> dict[str, object]:
    return {
        "General": {
            "Code": code,
            "Name": name,
            "LEI": _LEI,
            "PrimaryTicker": f"{code}.US",
            "HomeCategory": home_category,
        }
    }


def _seed_alphabet(db_path: Path) -> None:
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US")
    ticker_repo.replace_for_exchange("EODHD", "US", [_GOOG_ROW, _GOOGL_ROW])
    FundamentalsRepository(db_path).initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "GOOG.US",
        _payload("GOOG", "Alphabet Inc Class C", "Domestic Secondary"),
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "GOOGL.US",
        _payload("GOOGL", "Alphabet Inc Class A", "Domestic Primary"),
        exchange="US",
    )


def _issuer_name_by_symbol(db_path: Path) -> dict[str, str]:
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


def test_share_classes_resolve_to_the_entity_name(tmp_path: Path) -> None:
    """Both classes land on one issuer called "Alphabet Inc" -- not Class C."""

    db_path = tmp_path / "alphabet-name.db"
    _seed_alphabet(db_path)

    UniverseReconciler(db_path).reconcile()

    names = _issuer_name_by_symbol(db_path)
    assert names == {"GOOG.US": "Alphabet Inc", "GOOGL.US": "Alphabet Inc"}


def test_catalog_refresh_and_metadata_backfill_do_not_rename(tmp_path: Path) -> None:
    """The two former renamers leave the reconciler's verdict in place."""

    db_path = tmp_path / "alphabet-name-stable.db"
    _seed_alphabet(db_path)
    UniverseReconciler(db_path).reconcile()

    # A catalog refresh that restyles GOOG's security name used to rename the
    # shared issuer "Alphabet Inc. Class C Shares".
    restyled = dict(_GOOG_ROW, Name="Alphabet Inc. Class C Shares")
    SupportedTickerRepository(db_path).replace_for_exchange(
        "EODHD", "US", [restyled, _GOOGL_ROW]
    )
    assert set(_issuer_name_by_symbol(db_path).values()) == {"Alphabet Inc"}

    # A per-listing metadata backfill (normalize / refresh-security-metadata)
    # fills description gaps only; it has no name to write.
    security_repo = SecurityRepository(db_path)
    goog_id = security_repo.resolve_ids_many(["GOOG.US"])["GOOG.US"]
    security_repo.upsert_metadata_many(
        [SecurityMetadataUpdate(security_id=goog_id, description="Search giant")]
    )
    names = _issuer_name_by_symbol(db_path)
    assert set(names.values()) == {"Alphabet Inc"}

    # And the verdict is a fixpoint: reconciling again changes nothing.
    result = UniverseReconciler(db_path).reconcile()
    assert result.groups_merged == 0
    assert result.listings_repointed == 0
    assert _issuer_name_by_symbol(db_path) == names
