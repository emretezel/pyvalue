"""Lightweight SQLite schema migration runner.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Callable, List, Sequence, Tuple, Union

Migration = Callable[[sqlite3.Connection], None]


def _ensure_migrations_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER NOT NULL
        )
        """
    )
    row = conn.execute("SELECT version FROM schema_migrations LIMIT 1").fetchone()
    if row is None:
        conn.execute("INSERT INTO schema_migrations (version) VALUES (0)")


def _current_version(conn: sqlite3.Connection) -> int:
    _ensure_migrations_table(conn)
    row = conn.execute("SELECT version FROM schema_migrations LIMIT 1").fetchone()
    return int(row[0]) if row else 0


def _set_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute("DELETE FROM schema_migrations")
    conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (version,))


def apply_migrations(db_path: Union[str, Path]) -> int:
    """Apply all pending migrations in order.

    Returns the number of migrations applied. Safe to call repeatedly; no-op when
    already up-to-date. Each migration runs inside its own transaction and will
    rollback on error.
    """

    db_path = Path(db_path)
    if db_path.parent:
        db_path.parent.mkdir(parents=True, exist_ok=True)

    applied = 0
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = _current_version(conn)
        target = len(MIGRATIONS)
        conn.commit()

        for version in range(current + 1, target + 1):
            migration = MIGRATIONS[version - 1]
            try:
                conn.execute("BEGIN")
                migration(conn)
                _set_version(conn, version)
                conn.commit()
                applied += 1
            except Exception:
                conn.rollback()
                raise

    return applied


def _listings_primary_key(conn: sqlite3.Connection) -> List[str]:
    info = conn.execute("PRAGMA table_info(listings)").fetchall()
    # PRAGMA table_info returns rows with "name" and "pk" columns.
    return [row[1] for row in info if row[5]] if info else []


def _migration_001_listings_composite_pk(conn: sqlite3.Connection) -> None:
    """Rebuild listings table with composite (symbol, region) primary key."""

    # If listings table does not exist yet, defer creation to repository schema init.
    info = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'listings'"
    ).fetchone()
    if info is None:
        return

    pk = _listings_primary_key(conn)
    if pk == ["symbol", "region"]:
        return

    conn.execute("ALTER TABLE listings RENAME TO listings_old")

    conn.execute(
        """
        CREATE TABLE listings (
            symbol TEXT NOT NULL,
            security_name TEXT NOT NULL,
            exchange TEXT NOT NULL,
            market_category TEXT,
            is_etf INTEGER NOT NULL,
            status TEXT,
            round_lot_size INTEGER,
            source TEXT,
            region TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            PRIMARY KEY (symbol, region)
        )
        """
    )

    conn.execute(
        """
        INSERT INTO listings (
            symbol,
            security_name,
            exchange,
            market_category,
            is_etf,
            status,
            round_lot_size,
            source,
            region,
            ingested_at
        )
        SELECT
            symbol,
            security_name,
            exchange,
            market_category,
            is_etf,
            status,
            round_lot_size,
            source,
            region,
            ingested_at
        FROM listings_old
        """
    )

    conn.execute("DROP TABLE listings_old")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_region ON listings(region)")


def _migration_002_create_uk_company_facts(conn: sqlite3.Connection) -> None:
    """Create storage for Companies House payloads."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS uk_company_facts (
            company_number TEXT PRIMARY KEY,
            symbol TEXT,
            data TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        )
        """
    )


def _migration_003_add_isin_to_listings(conn: sqlite3.Connection) -> None:
    """Add ISIN column to listings if missing."""

    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='listings'"
    ).fetchone()
    if exists is None:
        return

    info = conn.execute("PRAGMA table_info(listings)").fetchall()
    columns = {row[1] for row in info}
    if "isin" in columns:
        return

    conn.execute("ALTER TABLE listings ADD COLUMN isin TEXT")


def _migration_004_create_uk_symbol_map(conn: sqlite3.Connection) -> None:
    """Create mapping table from UK symbols to identifiers."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS uk_symbol_map (
            symbol TEXT PRIMARY KEY,
            isin TEXT,
            lei TEXT,
            company_number TEXT,
            match_confidence TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )


def _migration_005_drop_unique_isin_index(conn: sqlite3.Connection) -> None:
    """Drop unique constraint on uk_symbol_map.isin to allow duplicate ISINs."""

    conn.execute("DROP INDEX IF EXISTS idx_uk_symbol_map_isin")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_uk_symbol_map_isin
        ON uk_symbol_map(isin)
        """
    )


MIGRATIONS: Sequence[Migration] = [
    _migration_001_listings_composite_pk,
    _migration_002_create_uk_company_facts,
    _migration_003_add_isin_to_listings,
    _migration_004_create_uk_symbol_map,
    _migration_005_drop_unique_isin_index,
]


__all__ = ["apply_migrations", "MIGRATIONS"]
