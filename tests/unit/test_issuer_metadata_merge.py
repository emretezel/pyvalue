"""Unit tests: metadata gap-fill onto issuers.

``SecurityRepository.upsert_metadata_many`` promotes ``General.Description`` /
``Sector`` / ``Industry`` from stored fundamentals onto a listing's issuer with
never-overwrite (COALESCE) semantics.

It used to promote ``General.Name`` too, and that had two landmines. While
``(name, country)`` was the unique issuer identity, a promoted name another
issuer already held aborted the batch, so the code merged the two rows instead
-- fusing unrelated companies. Migration 089 removed that premise (``issuer.lei``
is the natural key, names are descriptive). The second landmine outlived it: a
*security* name promoted per listing renamed a shared issuer after whichever
listing ran last, flipping Alphabet between "Class A" and "Class C". So the name
left this path altogether; the universe reconciler derives it once per legal
entity, and these tests pin that the metadata writer never touches it.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from conftest import seed_exchange
from pyvalue.persistence.storage import (
    SecurityMetadataUpdate,
    SecurityRepository,
    SupportedTickerRepository,
)

_US_ROW = {
    "Code": "PGLD",
    "Name": "Pearl Gold AG",
    "Type": "Common Stock",
    "Currency": "USD",
}
_BE_ROW = {
    "Code": "PGLD",
    "Name": "PEARL GOLD",
    "Type": "Common Stock",
    "Currency": "EUR",
}


def _seed_two_venue_catalog(db_path: Path) -> tuple[int, int]:
    """Catalog PGLD on US/BE with diverging names and backfilled countries.

    Returns ``(us_listing_id, be_listing_id)``. The country is set with direct
    SQL because the runtime catalog path always writes ``issuer.country = NULL``;
    the live values came from the migration-era metadata backfill.
    """
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US", "BE")
    ticker_repo.replace_for_exchange("EODHD", "US", [_US_ROW])
    ticker_repo.replace_for_exchange("EODHD", "BE", [_BE_ROW])
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE issuer SET country = 'Germany'")
    by_symbol = {row.symbol: row for row in ticker_repo.list_for_provider("EODHD")}
    return by_symbol["PGLD.US"].security_id, by_symbol["PGLD.BE"].security_id


def _issuer_rows(db_path: Path) -> dict[str, sqlite3.Row]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return {
            str(row["name"]): row
            for row in conn.execute(
                "SELECT name, description, sector, industry, country FROM issuer"
            )
        }


def test_gap_fill_never_overwrites_stored_metadata(tmp_path: Path) -> None:
    """COALESCE never-overwrite: a NULL payload field leaves the stored one."""

    db_path = tmp_path / "metadata-coalesce.db"
    _us_id, be_id = _seed_two_venue_catalog(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE issuer SET sector = 'Basic Materials' WHERE name = 'PEARL GOLD'"
        )

    SecurityRepository(db_path).upsert_metadata_many(
        [
            SecurityMetadataUpdate(
                security_id=be_id,
                description="fresh desc",
                sector=None,
                industry=None,
            )
        ]
    )

    row = _issuer_rows(db_path)["PEARL GOLD"]
    assert row["description"] == "fresh desc"
    assert row["sector"] == "Basic Materials"


def test_gap_fill_leaves_the_issuer_name_alone(tmp_path: Path) -> None:
    """The metadata writer has no name field: a shared issuer cannot be renamed
    by whichever of its listings happened to be normalized last."""

    db_path = tmp_path / "metadata-name-untouched.db"
    us_id, be_id = _seed_two_venue_catalog(db_path)

    updated = SecurityRepository(db_path).upsert_metadata_many(
        [
            SecurityMetadataUpdate(security_id=be_id, sector="Basic Materials"),
            SecurityMetadataUpdate(security_id=us_id, description="kept name"),
        ]
    )

    assert updated == 2
    rows = _issuer_rows(db_path)
    assert set(rows) == {"PEARL GOLD", "Pearl Gold AG"}
    assert rows["PEARL GOLD"]["sector"] == "Basic Materials"
    assert rows["PEARL GOLD"]["description"] is None
    assert rows["Pearl Gold AG"]["description"] == "kept name"
    with sqlite3.connect(db_path) as conn:
        issuer_by_listing = {
            listing_id: issuer_id
            for listing_id, issuer_id in conn.execute(
                "SELECT listing_id, issuer_id FROM listing"
            )
        }
    # Two venues, two issuer rows: metadata writes never merge or repoint.
    assert issuer_by_listing[us_id] != issuer_by_listing[be_id]


def test_unknown_listing_contributes_zero_updates(tmp_path: Path) -> None:
    db_path = tmp_path / "metadata-unknown-listing.db"
    _seed_two_venue_catalog(db_path)

    updated = SecurityRepository(db_path).upsert_metadata_many(
        [SecurityMetadataUpdate(security_id=999_999, description="Ghost Corp")]
    )

    assert updated == 0
    with sqlite3.connect(db_path) as conn:
        ghost_rows = conn.execute(
            "SELECT COUNT(*) FROM issuer WHERE description = 'Ghost Corp'"
        ).fetchone()[0]
    assert ghost_rows == 0


def test_update_with_no_fields_is_not_counted(tmp_path: Path) -> None:
    """An update carrying nothing to fill issues no write and counts for nothing."""

    db_path = tmp_path / "metadata-empty-update.db"
    _us_id, be_id = _seed_two_venue_catalog(db_path)

    updated = SecurityRepository(db_path).upsert_metadata_many(
        [SecurityMetadataUpdate(security_id=be_id)]
    )

    assert updated == 0
    assert set(_issuer_rows(db_path)) == {"PEARL GOLD", "Pearl Gold AG"}
