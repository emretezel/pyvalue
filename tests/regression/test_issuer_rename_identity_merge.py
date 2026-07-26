"""Regression: a provider rename must never abort a per-exchange refresh.

Migration 060 made ``(name, country)`` the unique issuer identity, but the
runtime catalog path kept issuing a blind ``UPDATE issuer SET name = ...``. When
a provider restyled a listing's display name to one another issuer row already
held with the same country (EODHD renamed ~2k Berlin listings this way, e.g.
``PEARL GOLD`` -> ``Pearl Gold AG``), the UPDATE violated the UNIQUE index and
the whole per-exchange refresh transaction rolled back with
``sqlite3.IntegrityError``.

The first fix converged the two rows: merge on collision, exactly as migration
060 had done wholesale. That stopped the crash but caused a subtler problem --
a provider restyling one listing's name onto another's is not evidence that they
are the same business, and merging on it fused unrelated companies into shared
parent rows (2,862 of them on the live catalog).

Migration 089 removed the premise instead. ``issuer.lei`` is the natural key,
names are descriptive metadata, and ``UNIQUE (name, country)`` is gone -- so
there is no collision to survive and no reason to merge. Entity grouping belongs
to ``reconcile-issuer-identity``, which works strictly from ISIN and LEI.

These tests keep both properties pinned: the refresh still must not abort, and
it must no longer fuse.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from conftest import seed_exchange, seed_facts
from pyvalue.persistence.storage import FactRecord, SupportedTickerRepository

# The same ticker catalogued on two venues under diverging display names.
_US_ROW = {
    "Code": "PGLD",
    "Name": "Pearl Gold AG",
    "Type": "Common Stock",
    "Currency": "USD",
}
_BE_ROW_OLD = {
    "Code": "PGLD",
    "Name": "PEARL GOLD",
    "Type": "Common Stock",
    "Currency": "EUR",
}
_BE_ROW_NEW = {
    "Code": "PGLD",
    "Name": "Pearl Gold AG",
    "Type": "Common Stock",
    "Currency": "EUR",
}


def _seed_two_venue_catalog(db_path: Path) -> tuple[int, int]:
    """Catalog PGLD on US and BE with diverging issuer names.

    Returns ``(us_listing_id, be_listing_id)``. Both issuers are given a country,
    which was the precondition for the original UNIQUE collision -- NULL
    countries are distinct under SQLite's UNIQUE semantics and could never
    collide.
    """

    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US", "BE")
    ticker_repo.replace_for_exchange("EODHD", "US", [_US_ROW])
    ticker_repo.replace_for_exchange("EODHD", "BE", [_BE_ROW_OLD])
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE issuer SET country = 'Germany'")
    by_symbol = {row.symbol: row for row in ticker_repo.list_for_provider("EODHD")}
    return by_symbol["PGLD.US"].security_id, by_symbol["PGLD.BE"].security_id


def test_rename_onto_an_existing_name_does_not_abort_the_refresh(
    tmp_path: Path,
) -> None:
    """The original incident: the whole exchange slice rolled back."""

    db_path = tmp_path / "issuer-rename-no-abort.db"
    us_id, be_id = _seed_two_venue_catalog(db_path)
    seed_facts(
        db_path,
        "PGLD.BE",
        [
            FactRecord(
                symbol="PGLD.BE",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                value=100.0,
                currency="EUR",
            )
        ],
    )

    result = SupportedTickerRepository(db_path).replace_for_exchange(
        "EODHD", "BE", [_BE_ROW_NEW]
    )

    assert result.inserted == 1
    assert result.removed == 0
    assert result.orphaned_listings == 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        fact_rows = conn.execute(
            "SELECT COUNT(*) FROM financial_facts WHERE listing_id = ?", (be_id,)
        ).fetchone()[0]
        mapping_rows = conn.execute("SELECT COUNT(*) FROM provider_listing").fetchone()[
            0
        ]
        conn.execute("PRAGMA foreign_keys=ON")
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        listing_ids = {
            row["listing_id"] for row in conn.execute("SELECT listing_id FROM listing")
        }

    assert listing_ids == {us_id, be_id}
    assert fact_rows == 1
    assert mapping_rows == 2
    assert fk_violations == []


def test_rename_no_longer_fuses_two_issuers(tmp_path: Path) -> None:
    """Matching names are not evidence of the same company.

    The merge-on-collision fix traded a crash for silent fusion; with no
    identifier linking these two listings they must stay separate entities, and
    ``reconcile-issuer-identity`` is what merges when an ISIN or LEI says so.
    """

    db_path = tmp_path / "issuer-rename-no-fuse.db"
    us_id, be_id = _seed_two_venue_catalog(db_path)

    SupportedTickerRepository(db_path).replace_for_exchange(
        "EODHD", "BE", [_BE_ROW_NEW]
    )

    with sqlite3.connect(db_path) as conn:
        issuer_by_listing = {
            listing_id: issuer_id
            for listing_id, issuer_id in conn.execute(
                "SELECT listing_id, issuer_id FROM listing"
            )
        }
        names = sorted(name for (name,) in conn.execute("SELECT name FROM issuer"))

    assert issuer_by_listing[us_id] != issuer_by_listing[be_id]
    # Both rows now legitimately carry the same name.
    assert names == ["Pearl Gold AG", "Pearl Gold AG"]


def test_rename_never_overwrites_existing_metadata(tmp_path: Path) -> None:
    """The COALESCE never-overwrite rule outlived the merge it was written for."""

    db_path = tmp_path / "issuer-rename-metadata.db"
    _seed_two_venue_catalog(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE issuer SET description = 'kept', sector = NULL "
            "WHERE name = 'PEARL GOLD'"
        )

    SupportedTickerRepository(db_path).replace_for_exchange(
        "EODHD", "BE", [_BE_ROW_NEW]
    )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        renamed = conn.execute(
            "SELECT name, description FROM issuer WHERE description = 'kept'"
        ).fetchone()

    # The payload supplied a name but no description, so the stored one stands.
    assert renamed["name"] == "Pearl Gold AG"
    assert renamed["description"] == "kept"
