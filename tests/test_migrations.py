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
        info = conn.execute("PRAGMA table_info(listings)").fetchall()
        pk_cols = [row[1] for row in info if row[5]]
        columns = {row[1] for row in info}
        rows = conn.execute("SELECT symbol FROM listings").fetchall()
        version = conn.execute("SELECT version FROM schema_migrations").fetchone()[0]

        assert pk_cols == ["symbol"]
        assert "region" not in columns
        assert rows == [("ABC.NYSE",)]
    assert version == len(MIGRATIONS)


def test_apply_migrations_is_idempotent(tmp_path):
    db_path = tmp_path / "db.sqlite"
    with sqlite3.connect(db_path) as conn:
        _create_legacy_listings_table(conn)

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first >= 1
    assert second == 0


def test_migration_creates_supported_exchanges_table(tmp_path):
    db_path = tmp_path / "supported-exchanges.sqlite"

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(supported_exchanges)").fetchall()
        columns = {row[1] for row in info}
        pk_cols = [row[1] for row in info if row[5]]

    assert columns == {
        "provider",
        "code",
        "name",
        "country",
        "currency",
        "operating_mic",
        "country_iso2",
        "country_iso3",
        "updated_at",
    }
    assert pk_cols == ["provider", "code"]


def test_migration_creates_supported_tickers_table(tmp_path):
    db_path = tmp_path / "supported-tickers.sqlite"

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(supported_tickers)").fetchall()
        columns = {row[1] for row in info}
        pk_cols = [row[1] for row in info if row[5]]
        indexes = conn.execute("PRAGMA index_list(supported_tickers)").fetchall()
        index_names = {row[1] for row in indexes}

    assert columns == {
        "provider",
        "exchange_code",
        "symbol",
        "code",
        "listing_exchange",
        "security_name",
        "security_type",
        "country",
        "currency",
        "isin",
        "updated_at",
    }
    assert pk_cols == ["provider", "symbol"]
    assert "idx_supported_tickers_provider_exchange" in index_names
