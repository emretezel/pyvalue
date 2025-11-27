import sqlite3

from pyvalue.migrations import MIGRATIONS, apply_migrations


def _create_legacy_listings_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE listings (
            symbol TEXT PRIMARY KEY,
            security_name TEXT NOT NULL,
            exchange TEXT NOT NULL,
            market_category TEXT,
            is_etf INTEGER NOT NULL,
            status TEXT,
            round_lot_size INTEGER,
            source TEXT,
            region TEXT NOT NULL,
            ingested_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO listings (symbol, security_name, exchange, market_category, is_etf, status,
                               round_lot_size, source, region, ingested_at)
        VALUES ('ABC', 'ABC Corp', 'NYSE', 'N', 0, 'N', 100, 'legacy', 'US', '2020-01-01T00:00:00Z')
        """
    )


def test_migration_updates_listings_primary_key(tmp_path):
    db_path = tmp_path / "db.sqlite"
    with sqlite3.connect(db_path) as conn:
        _create_legacy_listings_table(conn)

    applied = apply_migrations(db_path)
    assert applied >= 1

    with sqlite3.connect(db_path) as conn:
        pk_cols = [row[1] for row in conn.execute("PRAGMA table_info(listings)").fetchall() if row[5]]
        rows = conn.execute("SELECT symbol, region FROM listings").fetchall()
        version = conn.execute("SELECT version FROM schema_migrations").fetchone()[0]

    assert pk_cols == ["symbol", "region"]
    assert rows == [("ABC", "US")]
    assert version == len(MIGRATIONS)


def test_apply_migrations_is_idempotent(tmp_path):
    db_path = tmp_path / "db.sqlite"
    with sqlite3.connect(db_path) as conn:
        _create_legacy_listings_table(conn)

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first >= 1
    assert second == 0
