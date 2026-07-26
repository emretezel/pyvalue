"""Regression: ``issuer`` must group a company's cross-listings.

Author: Emre Tezel

The bug
-------
``issuer`` was meant to model a company but never did. Its identity is
``(name, country)``, ``country`` is NULL on 65,752 of 70,564 rows, and SQLite
treats NULLs as distinct in a UNIQUE index -- so in practice every listing got
its own issuer row. The 154 QARP passers mapped to 151 distinct ``issuer_id``s,
which meant nothing downstream could dedupe by company or compare the same
metric across a company's venues.

These tests exercise the whole path: catalog the listings, run
``reconcile-issuer-identity``, and assert the resulting parent rows.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from conftest import seed_exchange, seed_raw_fundamentals
from pyvalue.persistence.storage import (
    FundamentalsRepository,
    IssuerIdentityRepository,
    SupportedTickerRepository,
)


def _catalog(db_path: Path, exchange: str, rows: list[dict[str, object]]) -> None:
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    seed_exchange(db_path, exchange)
    repo.replace_for_exchange("EODHD", exchange, rows)


def _set_lei(db_path: Path, symbol: str, exchange: str, lei: str) -> None:
    """Publish an LEI for a listing the only way there is: in its payload.

    ``listing.lei`` was dropped as a duplicate of ``issuer.lei``, so the raw
    fundamentals payload is now the single source and grouping reads it back
    from there.
    """

    FundamentalsRepository(db_path).initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        f"{symbol}.{exchange}",
        {"General": {"LEI": lei}},
        exchange=exchange,
    )


def _issuers(db_path: Path) -> dict[str, int]:
    with sqlite3.connect(db_path) as conn:
        return {
            str(symbol): int(issuer_id)
            for symbol, issuer_id in conn.execute(
                """
                SELECT l.symbol || '.' || e.exchange_code, l.issuer_id
                FROM listing l
                JOIN "exchange" e ON e.exchange_id = l.exchange_id
                """
            )
        }


def test_cross_listings_of_one_security_share_an_issuer(tmp_path: Path) -> None:
    """Mettler-Toledo's four lines share ``US5926881054`` and one company."""

    db_path = tmp_path / "issuer-cross-listings.db"
    isin = "US5926881054"
    for exchange, code in (
        ("US", "MTD"),
        ("LSE", "0K10"),
        ("F", "MTO"),
        ("STU", "MTO"),
    ):
        _catalog(
            db_path,
            exchange,
            [
                {
                    "Code": code,
                    "Name": f"Mettler-Toledo ({exchange})",
                    "Type": "Common Stock",
                    "Currency": "USD",
                    "Isin": isin,
                }
            ],
        )

    before = _issuers(db_path)
    assert len({*before.values()}) == 4, "each listing starts with its own issuer"

    result = IssuerIdentityRepository(db_path).reconcile()

    after = _issuers(db_path)
    assert len({*after.values()}) == 1
    assert result.groups_merged == 1
    assert result.issuers_removed == 3


def test_share_classes_share_an_issuer_via_lei(tmp_path: Path) -> None:
    """Two securities, two ISINs, one legal entity -- only the LEI links them."""

    db_path = tmp_path / "issuer-share-classes.db"
    lei = "5493006MHB84DD0ZWV18"
    _catalog(
        db_path,
        "US",
        [
            {
                "Code": "GOOG",
                "Name": "Alphabet Inc Class C",
                "Type": "Common Stock",
                "Currency": "USD",
                "Isin": "US02079K1079",
            },
            {
                "Code": "GOOGL",
                "Name": "Alphabet Inc Class A",
                "Type": "Common Stock",
                "Currency": "USD",
                "Isin": "US02079K3059",
            },
        ],
    )
    _set_lei(db_path, "GOOG", "US", lei)
    _set_lei(db_path, "GOOGL", "US", lei)

    IssuerIdentityRepository(db_path).reconcile()

    after = _issuers(db_path)
    assert after["GOOG.US"] == after["GOOGL.US"]
    with sqlite3.connect(db_path) as conn:
        stored_lei = conn.execute(
            "SELECT lei FROM issuer WHERE issuer_id = ?", (after["GOOG.US"],)
        ).fetchone()[0]
    assert stored_lei == lei


def test_receipt_without_an_lei_keeps_its_own_issuer(tmp_path: Path) -> None:
    """With no LEI there is no evidence linking a receipt to its underlying.

    A receipt is a distinct security with its own ISIN, so ISIN cannot bridge
    the pair and ``DNLMY.US`` stays separate from ``DNLM.LSE``. Grouping rests
    on identifiers alone -- it must never start matching names that look alike,
    which is exactly how the old name-based identity fused unrelated companies.
    """

    db_path = tmp_path / "issuer-adr.db"
    _catalog(
        db_path,
        "LSE",
        [
            {
                "Code": "DNLM",
                "Name": "Dunelm Group PLC",
                "Type": "Common Stock",
                "Currency": "GBX",
                "Isin": "GB00B1CKQ739",
            }
        ],
    )
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
    _set_lei(db_path, "DNLM", "LSE", "213800WCOWEI3T5DUV19")

    IssuerIdentityRepository(db_path).reconcile()

    after = _issuers(db_path)
    assert after["DNLM.LSE"] != after["DNLMY.US"]


def test_reconcile_is_convergent(tmp_path: Path) -> None:
    """A second run on an unchanged catalog changes nothing.

    The representative is the lowest issuer id precisely so repeated runs agree
    instead of reshuffling parents.
    """

    db_path = tmp_path / "issuer-convergent.db"
    isin = "US5926881054"
    for exchange, code in (("US", "MTD"), ("F", "MTO")):
        _catalog(
            db_path,
            exchange,
            [
                {
                    "Code": code,
                    "Name": "Mettler-Toledo",
                    "Type": "Common Stock",
                    "Currency": "USD",
                    "Isin": isin,
                }
            ],
        )

    repo = IssuerIdentityRepository(db_path)
    repo.reconcile()
    first = _issuers(db_path)

    second_result = repo.reconcile()

    assert _issuers(db_path) == first
    assert second_result.listings_repointed == 0
    assert second_result.issuers_removed == 0


def test_merged_issuer_keeps_metadata_only_one_row_carried(tmp_path: Path) -> None:
    """A merge must not lose a sector that only the absorbed row had."""

    db_path = tmp_path / "issuer-metadata.db"
    isin = "US5926881054"
    for exchange, code in (("US", "MTD"), ("F", "MTO")):
        _catalog(
            db_path,
            exchange,
            [
                {
                    "Code": code,
                    "Name": f"Mettler-Toledo ({exchange})",
                    "Type": "Common Stock",
                    "Currency": "USD",
                    "Isin": isin,
                }
            ],
        )
    issuers = _issuers(db_path)
    # Put the sector on whichever row will be absorbed, not the survivor.
    absorbed = max(issuers["MTD.US"], issuers["MTO.F"])
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE issuer SET sector = 'Healthcare' WHERE issuer_id = ?", (absorbed,)
        )

    IssuerIdentityRepository(db_path).reconcile()

    survivor = _issuers(db_path)["MTD.US"]
    with sqlite3.connect(db_path) as conn:
        sector = conn.execute(
            "SELECT sector FROM issuer WHERE issuer_id = ?", (survivor,)
        ).fetchone()[0]
    assert sector == "Healthcare"


def test_issuer_shared_across_two_groups_is_not_deleted_early(
    tmp_path: Path,
) -> None:
    """An issuer whose listings land in different groups must survive.

    The catalog already contains 3,991 multi-listing issuers, created by the
    runtime name-collision merge, and nothing guaranteed their listings shared an
    identifier. Deleting an absorbed issuer per group therefore dropped a row
    another group's listings still pointed at -- ``FOREIGN KEY constraint
    failed`` on the live catalog. Deletion has to wait until every group has been
    repointed.

    Fixture: one issuer owning two listings with unrelated ISINs, each of which
    groups with a different third listing.
    """

    db_path = tmp_path / "issuer-split-parent.db"
    _catalog(
        db_path,
        "US",
        [
            {
                "Code": "AAA",
                "Name": "Shared Parent",
                "Type": "Common Stock",
                "Currency": "USD",
                "Isin": "US0000000017",
            },
            {
                "Code": "BBB",
                "Name": "Shared Parent",
                "Type": "Common Stock",
                "Currency": "USD",
                "Isin": "US0000000025",
            },
        ],
    )
    _catalog(
        db_path,
        "F",
        [
            {
                "Code": "AAAF",
                "Name": "Peer Of AAA",
                "Type": "Common Stock",
                "Currency": "EUR",
                "Isin": "US0000000017",
            },
            {
                "Code": "BBBF",
                "Name": "Peer Of BBB",
                "Type": "Common Stock",
                "Currency": "EUR",
                "Isin": "US0000000025",
            },
        ],
    )

    # Force AAA.US and BBB.US onto one issuer row, the shape the runtime
    # name-collision merge produces.
    issuers = _issuers(db_path)
    shared = issuers["AAA.US"]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE listing SET issuer_id = ? WHERE listing_id IN "
            '(SELECT listing_id FROM listing l JOIN "exchange" e '
            " ON e.exchange_id = l.exchange_id "
            " WHERE l.symbol IN ('AAA','BBB') AND e.exchange_code = 'US')",
            (shared,),
        )
        conn.execute(
            "DELETE FROM issuer WHERE NOT EXISTS "
            "(SELECT 1 FROM listing WHERE listing.issuer_id = issuer.issuer_id)"
        )

    # Must not raise.
    IssuerIdentityRepository(db_path).reconcile()

    after = _issuers(db_path)
    assert after["AAA.US"] == after["AAAF.F"]
    assert after["BBB.US"] == after["BBBF.F"]
    assert after["AAA.US"] != after["BBB.US"]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
