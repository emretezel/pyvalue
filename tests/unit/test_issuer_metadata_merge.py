"""Unit tests: metadata promotion honours the (name, country) issuer identity.

``SecurityRepository.upsert_metadata_many`` promotes ``General.*`` metadata
from stored fundamentals onto issuers. Like the catalog refresh it renames
issuers, so it carries the same landmine: a promoted name that another issuer
already holds with the same country must merge the two rows (migration 060
semantics) instead of violating ``idx_issuer_name_country`` and aborting the
batch. Plain, non-colliding batches must keep their original COALESCE
behaviour.

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


def test_colliding_promoted_name_merges_issuers(tmp_path: Path) -> None:
    db_path = tmp_path / "metadata-merge.db"
    us_id, be_id = _seed_two_venue_catalog(db_path)
    security_repo = SecurityRepository(db_path)

    updated = security_repo.upsert_metadata_many(
        [SecurityMetadataUpdate(security_id=be_id, entity_name="Pearl Gold AG")]
    )

    assert updated == 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        issuers = conn.execute("SELECT name, country FROM issuer").fetchall()
        issuer_ids = {
            row["listing_id"]: row["issuer_id"]
            for row in conn.execute("SELECT listing_id, issuer_id FROM listing")
        }
    assert [(row["name"], row["country"]) for row in issuers] == [
        ("Pearl Gold AG", "Germany")
    ]
    assert issuer_ids[us_id] == issuer_ids[be_id]
    # The repository cache was invalidated and refetched: the renamed listing
    # resolves to the surviving identity's name.
    fetched = security_repo.fetch(be_id)
    assert fetched is not None
    assert fetched.entity_name == "Pearl Gold AG"


def test_merge_prefers_payload_metadata_over_merged_away_row(tmp_path: Path) -> None:
    db_path = tmp_path / "metadata-merge-payload-wins.db"
    _, be_id = _seed_two_venue_catalog(db_path)
    with sqlite3.connect(db_path) as conn:
        # The merged-away row carries a stale sector; the payload carries a
        # fresh one; the survivor has none. The payload must win the backfill.
        conn.execute(
            "UPDATE issuer SET sector = 'Stale Sector' WHERE name = 'PEARL GOLD'"
        )

    SecurityRepository(db_path).upsert_metadata_many(
        [
            SecurityMetadataUpdate(
                security_id=be_id,
                entity_name="Pearl Gold AG",
                sector="Fresh Sector",
            )
        ]
    )

    with sqlite3.connect(db_path) as conn:
        sector = conn.execute("SELECT sector FROM issuer").fetchone()[0]
    assert sector == "Fresh Sector"


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
