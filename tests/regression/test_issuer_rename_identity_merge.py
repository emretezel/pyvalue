"""Regression: issuer renames merge into an existing (name, country) identity.

Migration 060 made ``(name, country)`` the unique issuer identity
(``idx_issuer_name_country``) and merged the duplicates that the per-listing
ingest path had accumulated, but the runtime rename paths kept issuing a blind
``UPDATE issuer SET name = ...``. When a provider restyles a listing's display
name to one that *another* issuer row already holds with the same country
(EODHD renamed ~2k Berlin listings this way, e.g. ``PEARL GOLD`` ->
``Pearl Gold AG``), the UPDATE violated the UNIQUE index and the whole
per-exchange refresh transaction rolled back with ``sqlite3.IntegrityError``.

The fix converges instead of crashing: the listing's issuer is merged into the
existing identity row exactly like migration 060 did wholesale -- COALESCE
metadata promotion, listings repointed, the emptied row deleted. These tests
fail on the old blind-rename code.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from conftest import seed_exchange, seed_facts
from pyvalue.persistence.storage import FactRecord, SupportedTickerRepository

# The same company catalogued on two venues under diverging display names.
# ``country`` is deliberately NOT part of the payload: the runtime catalog path
# always inserts ``issuer.country = NULL``; the live values came from the
# migration-era metadata backfill and are emulated with direct SQL below.
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


def _seed_two_venue_catalog(
    db_path: Path, *, backfill_country: bool
) -> tuple[int, int]:
    """Catalog PGLD on US and BE with diverging issuer names.

    Returns ``(us_listing_id, be_listing_id)``. With ``backfill_country`` the
    issuers receive the country the legacy backfill would have stored -- the
    precondition for a UNIQUE(name, country) collision (NULL countries are
    distinct under SQLite's UNIQUE semantics and can never collide).
    """
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US", "BE")
    ticker_repo.replace_for_exchange("EODHD", "US", [_US_ROW])
    ticker_repo.replace_for_exchange("EODHD", "BE", [_BE_ROW_OLD])
    if backfill_country:
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE issuer SET country = 'Germany'")
    by_symbol = {row.symbol: row for row in ticker_repo.list_for_provider("EODHD")}
    return by_symbol["PGLD.US"].security_id, by_symbol["PGLD.BE"].security_id


def test_rename_onto_existing_identity_merges_instead_of_crashing(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "issuer-rename-merge.db"
    us_id, be_id = _seed_two_venue_catalog(db_path, backfill_country=True)
    # Downstream data on the listing being repointed must survive the merge
    # untouched (facts are keyed by listing_id, not issuer_id).
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
    ticker_repo = SupportedTickerRepository(db_path)

    # EODHD restyles the BE display name to the one the US-venue issuer already
    # holds with the same country. The old code died here with
    # sqlite3.IntegrityError: UNIQUE constraint failed: issuer.name, issuer.country.
    result = ticker_repo.replace_for_exchange("EODHD", "BE", [_BE_ROW_NEW])

    assert result.inserted == 1
    assert result.removed == 0
    assert result.purged_listings == 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        issuers = conn.execute("SELECT issuer_id, name, country FROM issuer").fetchall()
        listings = {
            row["listing_id"]: row["issuer_id"]
            for row in conn.execute("SELECT listing_id, issuer_id FROM listing")
        }
        fact_rows = conn.execute(
            "SELECT COUNT(*) FROM financial_facts WHERE listing_id = ?", (be_id,)
        ).fetchone()[0]
        mapping_rows = conn.execute("SELECT COUNT(*) FROM provider_listing").fetchone()[
            0
        ]
        conn.execute("PRAGMA foreign_keys=ON")
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()

    # One issuer identity survives; the losing row is gone; both venue listings
    # (their listing_ids unchanged) point at the survivor.
    assert [(row["name"], row["country"]) for row in issuers] == [
        ("Pearl Gold AG", "Germany")
    ]
    surviving_issuer_id = issuers[0]["issuer_id"]
    assert listings == {us_id: surviving_issuer_id, be_id: surviving_issuer_id}
    assert fact_rows == 1
    assert mapping_rows == 2
    assert fk_violations == []


def test_merge_backfills_missing_metadata_without_overwriting(tmp_path: Path) -> None:
    db_path = tmp_path / "issuer-merge-backfill.db"
    _seed_two_venue_catalog(db_path, backfill_country=True)
    with sqlite3.connect(db_path) as conn:
        # Source (the row being merged away) carries metadata the target lacks,
        # plus a description the target already has its own value for.
        conn.execute(
            "UPDATE issuer SET sector = 'Basic Materials', description = 'source desc' "
            "WHERE name = 'PEARL GOLD'"
        )
        conn.execute(
            "UPDATE issuer SET description = 'target desc' WHERE name = 'Pearl Gold AG'"
        )

    SupportedTickerRepository(db_path).replace_for_exchange(
        "EODHD", "BE", [_BE_ROW_NEW]
    )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        issuer = conn.execute("SELECT name, description, sector FROM issuer").fetchone()
    # 060's promotion rule: NULL columns are backfilled from the merged-away
    # row, non-NULL columns on the survivor are never overwritten.
    assert issuer["name"] == "Pearl Gold AG"
    assert issuer["sector"] == "Basic Materials"
    assert issuer["description"] == "target desc"


def test_null_country_rename_never_merges(tmp_path: Path) -> None:
    db_path = tmp_path / "issuer-null-country.db"
    us_id, be_id = _seed_two_venue_catalog(db_path, backfill_country=False)

    SupportedTickerRepository(db_path).replace_for_exchange(
        "EODHD", "BE", [_BE_ROW_NEW]
    )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        issuers = conn.execute(
            "SELECT name, country FROM issuer ORDER BY issuer_id"
        ).fetchall()
        issuer_ids = {
            row["listing_id"]: row["issuer_id"]
            for row in conn.execute("SELECT listing_id, issuer_id FROM listing")
        }
    # NULL countries are distinct under the UNIQUE index and, per migration
    # 060's documented semantics, must never be merged (a NULL key would
    # conflate unrelated companies). The rename lands as a plain update and the
    # same-name duplicate pair is legitimate.
    assert [(row["name"], row["country"]) for row in issuers] == [
        ("Pearl Gold AG", None),
        ("Pearl Gold AG", None),
    ]
    assert issuer_ids[us_id] != issuer_ids[be_id]


def test_re_refresh_after_merge_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "issuer-merge-idempotent.db"
    _seed_two_venue_catalog(db_path, backfill_country=True)
    ticker_repo = SupportedTickerRepository(db_path)

    def _identity_snapshot() -> tuple[
        list[tuple[object, ...]], list[tuple[object, ...]]
    ]:
        with sqlite3.connect(db_path) as conn:
            issuers = conn.execute(
                "SELECT issuer_id, name, description, sector, industry, country "
                "FROM issuer ORDER BY issuer_id"
            ).fetchall()
            listings = conn.execute(
                "SELECT listing_id, issuer_id, exchange_id, symbol, currency "
                "FROM listing ORDER BY listing_id"
            ).fetchall()
        return issuers, listings

    first = ticker_repo.replace_for_exchange("EODHD", "BE", [_BE_ROW_NEW])
    snapshot_after_merge = _identity_snapshot()
    second = ticker_repo.replace_for_exchange("EODHD", "BE", [_BE_ROW_NEW])

    assert (first.inserted, first.removed) == (second.inserted, second.removed)
    assert _identity_snapshot() == snapshot_after_merge
