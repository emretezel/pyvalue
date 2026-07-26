"""Unit tests: metadata promotion onto issuers.

``SecurityRepository.upsert_metadata_many`` promotes ``General.*`` metadata from
stored fundamentals onto issuers, renaming them as providers restyle names.

This used to have a landmine: while ``(name, country)`` was the unique issuer
identity, a promoted name another issuer already held aborted the batch, so the
code merged the two rows instead. Migration 089 removed the premise -- names are
descriptive metadata, ``issuer.lei`` is the natural key, and entity grouping
belongs to ``reconcile-issuer-identity`` working from identifiers. A promoted
name is now just a rename, and two issuers may share one.

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


def test_promoted_name_collision_does_not_merge_issuers(tmp_path: Path) -> None:
    """Two issuers may now share a name; a rename must not fuse them.

    A provider restyling one listing's name onto another's is not evidence that
    they are the same business -- merging on it is what produced 2,862 fused
    parent rows on the live catalog.
    """

    db_path = tmp_path / "metadata-no-merge.db"
    us_id, be_id = _seed_two_venue_catalog(db_path)

    SecurityRepository(db_path).upsert_metadata_many(
        [
            SecurityMetadataUpdate(
                security_id=be_id,
                entity_name="Pearl Gold AG",
                description=None,
                sector=None,
                industry=None,
            )
        ]
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
    assert names == ["Pearl Gold AG", "Pearl Gold AG"]


def test_promotion_never_overwrites_stored_metadata(tmp_path: Path) -> None:
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
                entity_name="Pearl Gold AG",
                description="fresh desc",
                sector=None,
                industry=None,
            )
        ]
    )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT name, description, sector FROM issuer "
            "WHERE sector = 'Basic Materials'"
        ).fetchone()

    assert row["name"] == "Pearl Gold AG"
    assert row["description"] == "fresh desc"
    assert row["sector"] == "Basic Materials"


def test_non_colliding_batch_keeps_coalesce_semantics(tmp_path: Path) -> None:
    db_path = tmp_path / "metadata-plain-batch.db"
    us_id, be_id = _seed_two_venue_catalog(db_path)
    security_repo = SecurityRepository(db_path)

    updated = security_repo.upsert_metadata_many(
        [
            # Free rename (no other issuer holds this name+country) plus new
            # sector; the description stays absent (NULL payload keeps NULL).
            SecurityMetadataUpdate(
                security_id=be_id,
                entity_name="Pearl Gold Renamed",
                sector="Basic Materials",
            ),
            # Metadata-only update: the name must survive the NULL payload.
            SecurityMetadataUpdate(security_id=us_id, description="kept name"),
        ]
    )

    assert updated == 2
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        by_name = {
            row["name"]: row
            for row in conn.execute(
                "SELECT name, description, sector, country FROM issuer"
            )
        }
    assert set(by_name) == {"Pearl Gold Renamed", "Pearl Gold AG"}
    assert by_name["Pearl Gold Renamed"]["sector"] == "Basic Materials"
    assert by_name["Pearl Gold Renamed"]["description"] is None
    assert by_name["Pearl Gold AG"]["description"] == "kept name"


def test_unknown_listing_contributes_zero_updates(tmp_path: Path) -> None:
    db_path = tmp_path / "metadata-unknown-listing.db"
    _seed_two_venue_catalog(db_path)

    updated = SecurityRepository(db_path).upsert_metadata_many(
        [SecurityMetadataUpdate(security_id=999_999, entity_name="Ghost Corp")]
    )

    assert updated == 0
    with sqlite3.connect(db_path) as conn:
        ghost_rows = conn.execute(
            "SELECT COUNT(*) FROM issuer WHERE name = 'Ghost Corp'"
        ).fetchone()[0]
    assert ghost_rows == 0


def test_rename_to_own_current_name_stays_plain_update(tmp_path: Path) -> None:
    db_path = tmp_path / "metadata-same-name.db"
    us_id, be_id = _seed_two_venue_catalog(db_path)

    updated = SecurityRepository(db_path).upsert_metadata_many(
        [SecurityMetadataUpdate(security_id=be_id, entity_name="PEARL GOLD")]
    )

    assert updated == 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        issuer_ids = {
            row["listing_id"]: row["issuer_id"]
            for row in conn.execute("SELECT listing_id, issuer_id FROM listing")
        }
        issuer_count = conn.execute("SELECT COUNT(*) FROM issuer").fetchone()[0]
    # Re-asserting the current name is not a rename: both venue issuers stay
    # separate rows.
    assert issuer_count == 2
    assert issuer_ids[us_id] != issuer_ids[be_id]
