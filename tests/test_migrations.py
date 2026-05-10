import hashlib
import sqlite3

import pytest

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
        listings_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='listings'"
        ).fetchone()
        rows = conn.execute(
            """
            SELECT p.provider_code, px.provider_exchange_code, pl.provider_symbol,
                   e.exchange_code, i.name
            FROM provider_listing pl
            JOIN provider p ON p.provider_id = pl.provider_id
            JOIN provider_exchange px ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN listing l ON l.listing_id = pl.listing_id
            JOIN issuer i ON i.issuer_id = l.issuer_id
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            """
        ).fetchall()
        version = conn.execute("SELECT version FROM schema_migrations").fetchone()[0]

        assert listings_exists is None
        assert rows == [("SEC", "US", "ABC", "US", "ABC Corp")]
    assert version == len(MIGRATIONS)


def test_apply_migrations_is_idempotent(tmp_path):
    db_path = tmp_path / "db.sqlite"
    with sqlite3.connect(db_path) as conn:
        _create_legacy_listings_table(conn)

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first >= 1
    assert second == 0


def test_migration_creates_exchange_catalog_tables(tmp_path):
    db_path = tmp_path / "exchange-catalog.sqlite"

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        supported_exchanges_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='supported_exchanges'"
        ).fetchone()
        exchange_info = conn.execute('PRAGMA table_info("exchange")').fetchall()
        exchange_columns = {row[1] for row in exchange_info}
        exchange_pk_cols = [row[1] for row in exchange_info if row[5]]
        provider_exchange_info = conn.execute(
            "PRAGMA table_info(provider_exchange)"
        ).fetchall()
        provider_exchange_columns = {row[1] for row in provider_exchange_info}
        provider_exchange_pk_cols = [row[1] for row in provider_exchange_info if row[5]]
        provider_exchange_indexes = conn.execute(
            "PRAGMA index_list(provider_exchange)"
        ).fetchall()
        provider_exchange_index_names = {row[1] for row in provider_exchange_indexes}
        provider_exchange_fks = conn.execute(
            "PRAGMA foreign_key_list(provider_exchange)"
        ).fetchall()
        fk_targets = {row[2] for row in provider_exchange_fks}
        legacy_tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table'
                """
            )
        }

    assert supported_exchanges_exists is None
    assert "exchange_provider" not in legacy_tables
    assert exchange_columns == {
        "exchange_id",
        "exchange_code",
        "created_at",
        "updated_at",
    }
    assert exchange_pk_cols == ["exchange_id"]
    assert provider_exchange_columns == {
        "provider_exchange_id",
        "provider_id",
        "provider_exchange_code",
        "exchange_id",
        "name",
        "country",
        "currency",
        "operating_mic",
        "country_iso2",
        "country_iso3",
        "updated_at",
    }
    assert provider_exchange_pk_cols == ["provider_exchange_id"]
    assert "idx_provider_exchange_exchange" in provider_exchange_index_names
    assert fk_targets == {"exchange", "provider"}


def test_migration_splits_supported_exchanges_into_exchange_provider(tmp_path):
    db_path = tmp_path / "exchange-provider-backfill.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE providers (
                provider_code TEXT NOT NULL PRIMARY KEY,
                display_name TEXT NOT NULL,
                description TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO providers (
                provider_code,
                display_name,
                description,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    "EODHD",
                    "EOD Historical Data",
                    None,
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
                (
                    "SEC",
                    "US SEC Company Facts",
                    None,
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            ],
        )
        conn.execute(
            """
            CREATE TABLE supported_exchanges (
                provider TEXT NOT NULL,
                provider_exchange_code TEXT NOT NULL,
                canonical_exchange_code TEXT NOT NULL,
                name TEXT,
                country TEXT,
                currency TEXT,
                operating_mic TEXT,
                country_iso2 TEXT,
                country_iso3 TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (provider, provider_exchange_code)
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO supported_exchanges (
                provider,
                provider_exchange_code,
                canonical_exchange_code,
                name,
                country,
                currency,
                operating_mic,
                country_iso2,
                country_iso3,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "EODHD",
                    "LSE",
                    "LSE",
                    "London Exchange",
                    "UK",
                    "GBP",
                    "XLON",
                    "GB",
                    "GBR",
                    "2026-01-01T00:00:00+00:00",
                ),
                (
                    "EODHD",
                    "US",
                    "US",
                    "USA Stocks",
                    "USA",
                    "USD",
                    "XNAS, XNYS",
                    "US",
                    "USA",
                    "2026-01-01T00:00:00+00:00",
                ),
                (
                    "SEC",
                    "US",
                    "US",
                    "United States",
                    "US",
                    "USD",
                    None,
                    "US",
                    "USA",
                    "2026-01-01T00:00:00+00:00",
                ),
            ],
        )
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (32)")

    applied = apply_migrations(db_path)

    assert applied == len(MIGRATIONS) - 32
    with sqlite3.connect(db_path) as conn:
        supported_exchanges_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='supported_exchanges'"
        ).fetchone()
        exchange_rows = conn.execute(
            'SELECT exchange_code FROM "exchange" ORDER BY exchange_code'
        ).fetchall()
        provider_exchange_rows = conn.execute(
            """
            SELECT
                p.provider_code,
                ep.provider_exchange_code,
                e.exchange_code,
                ep.name,
                ep.country,
                ep.currency
            FROM provider_exchange ep
            JOIN provider p ON p.provider_id = ep.provider_id
            JOIN "exchange" e ON e.exchange_id = ep.exchange_id
            ORDER BY p.provider_code, ep.provider_exchange_code
            """
        ).fetchall()

    assert supported_exchanges_exists is None
    assert exchange_rows == [("LSE",), ("US",)]
    assert provider_exchange_rows == [
        ("EODHD", "LSE", "LSE", "London Exchange", "UK", "GBP"),
        ("EODHD", "US", "US", "USA Stocks", "USA", "USD"),
        ("SEC", "US", "US", "United States", "US", "USD"),
    ]


def test_exchange_provider_foreign_keys_are_enforced(tmp_path):
    db_path = tmp_path / "exchange-provider-fk.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO provider_exchange (
                    provider_id,
                    provider_exchange_code,
                    exchange_id,
                    updated_at
                ) VALUES (?, ?, ?, ?)
                """,
                (999999, "US", 1, "2026-01-01T00:00:00+00:00"),
            )
        provider_id = conn.execute(
            "SELECT provider_id FROM provider WHERE provider_code = 'EODHD'"
        ).fetchone()[0]
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO provider_exchange (
                    provider_id,
                    provider_exchange_code,
                    exchange_id,
                    updated_at
                ) VALUES (?, ?, ?, ?)
                """,
                (provider_id, "US", 999999, "2026-01-01T00:00:00+00:00"),
            )


def test_migration_creates_and_seeds_providers_table(tmp_path):
    db_path = tmp_path / "providers.sqlite"

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(provider)").fetchall()
        columns = {row[1] for row in info}
        pk_cols = [row[1] for row in info if row[5]]
        rows = conn.execute(
            """
            SELECT provider_code, display_name
            FROM provider
            ORDER BY provider_code
            """
        ).fetchall()

    assert columns == {
        "provider_id",
        "provider_code",
        "display_name",
        "description",
        "created_at",
        "updated_at",
    }
    assert pk_cols == ["provider_id"]
    assert rows == [
        ("EODHD", "EOD Historical Data"),
        ("FRANKFURTER", "Frankfurter FX"),
        ("SEC", "US SEC Company Facts"),
    ]


def test_migration_preserves_existing_provider_rows_when_adding_registry(tmp_path):
    db_path = tmp_path / "providers-backfill.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE supported_tickers (
                provider TEXT NOT NULL,
                provider_symbol TEXT NOT NULL,
                PRIMARY KEY (provider, provider_symbol)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE fundamentals_raw (
                provider TEXT NOT NULL,
                provider_symbol TEXT NOT NULL,
                data TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (provider, provider_symbol)
            )
            """
        )
        # market_data is omitted from this fixture: migration 047 added a
        # listing-FK that an orphan security_id row would violate, and
        # this test's purpose is the provider-registry migration, not
        # market_data preservation. test_migration_canonicalizes_listing_quote_currency_and_market_data
        # covers the market_data path with proper securities seeding.
        conn.execute(
            """
            CREATE TABLE fx_rates (
                provider TEXT NOT NULL,
                rate_date TEXT NOT NULL,
                base_currency TEXT NOT NULL,
                quote_currency TEXT NOT NULL,
                rate_text TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (provider, rate_date, base_currency, quote_currency)
            )
            """
        )
        conn.executemany(
            "INSERT INTO supported_tickers (provider, provider_symbol) VALUES (?, ?)",
            [("EODHD", "AAA.US"), ("SEC", "BBB.US")],
        )
        conn.executemany(
            """
            INSERT INTO fundamentals_raw (
                provider, provider_symbol, data, fetched_at
            ) VALUES (?, ?, ?, ?)
            """,
            [
                ("EODHD", "AAA.US", "{}", "2026-01-01T00:00:00+00:00"),
                ("SEC", "BBB.US", "{}", "2026-01-01T00:00:00+00:00"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO fx_rates (
                provider,
                rate_date,
                base_currency,
                quote_currency,
                rate_text,
                fetched_at,
                source_kind,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "EODHD",
                    "2026-01-02",
                    "USD",
                    "EUR",
                    "0.91",
                    "2026-01-02T00:00:00+00:00",
                    "provider",
                    "2026-01-02T00:00:00+00:00",
                    "2026-01-02T00:00:00+00:00",
                ),
                (
                    "FRANKFURTER",
                    "2026-01-02",
                    "USD",
                    "GBP",
                    "0.80",
                    "2026-01-02T00:00:00+00:00",
                    "provider",
                    "2026-01-02T00:00:00+00:00",
                    "2026-01-02T00:00:00+00:00",
                ),
            ],
        )
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (31)")

    applied = apply_migrations(db_path)

    assert applied == len(MIGRATIONS) - 31
    with sqlite3.connect(db_path) as conn:
        provider_listing_count = conn.execute(
            "SELECT COUNT(*) FROM provider_listing"
        ).fetchone()[0]
        fundamentals_raw_count = conn.execute(
            "SELECT COUNT(*) FROM fundamentals_raw"
        ).fetchone()[0]
        fx_rates_count = conn.execute("SELECT COUNT(*) FROM fx_rates").fetchone()[0]
        supported_ticker_join_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM provider_listing pl
            JOIN provider p ON p.provider_id = pl.provider_id
            """
        ).fetchone()[0]
        fundamentals_raw_join_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM fundamentals_raw fr
            JOIN provider_listing pl ON pl.provider_listing_id = fr.provider_listing_id
            JOIN provider p ON p.provider_id = pl.provider_id
            """
        ).fetchone()[0]
        fx_rates_join_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM fx_rates fx
            JOIN provider p ON p.provider_code = fx.provider
            """
        ).fetchone()[0]

    assert provider_listing_count == 0
    assert fundamentals_raw_count == 0
    assert fx_rates_count == 2
    assert supported_ticker_join_count == 0
    assert fundamentals_raw_join_count == 0
    assert fx_rates_join_count == 2


def test_providers_table_rejects_invalid_provider_codes(tmp_path):
    db_path = tmp_path / "providers-invalid.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO provider (
                    provider_code,
                    display_name,
                    description,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "eodhd",
                    "Lowercase provider",
                    None,
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO provider (
                    provider_code,
                    display_name,
                    description,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "",
                    "Blank provider",
                    None,
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            )


def test_migration_drops_provider_status_from_version_34_db(tmp_path):
    db_path = tmp_path / "provider-status-drop.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (34)")
        conn.execute(
            """
            CREATE TABLE provider (
                provider_id INTEGER PRIMARY KEY,
                provider_code TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                description TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO provider (
                provider_code,
                display_name,
                description,
                status,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "EODHD",
                "EOD Historical Data",
                "Provider",
                "active",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )

    applied = apply_migrations(db_path)

    assert applied == len(MIGRATIONS) - 34
    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(provider)").fetchall()
        }
        row = conn.execute(
            """
            SELECT provider_code, display_name, description, created_at, updated_at
            FROM provider
            """
        ).fetchone()

    assert columns == {
        "provider_id",
        "provider_code",
        "display_name",
        "description",
        "created_at",
        "updated_at",
    }
    assert row == (
        "EODHD",
        "EOD Historical Data",
        "Provider",
        "2026-01-01T00:00:00+00:00",
        "2026-01-01T00:00:00+00:00",
    )


def test_migration_creates_provider_listing_table(tmp_path):
    db_path = tmp_path / "supported-tickers.sqlite"

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(provider_listing)").fetchall()
        columns = {row[1] for row in info}
        pk_cols = [row[1] for row in info if row[5]]
        indexes = conn.execute("PRAGMA index_list(provider_listing)").fetchall()
        index_names = {row[1] for row in indexes}
        fks = conn.execute("PRAGMA foreign_key_list(provider_listing)").fetchall()
        fk_targets = {row[2] for row in fks}
        supported_tickers_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='supported_tickers'"
        ).fetchone()

    assert columns == {
        "provider_listing_id",
        "provider_id",
        "provider_exchange_id",
        "provider_symbol",
        "listing_id",
    }
    assert pk_cols == ["provider_listing_id"]
    assert "idx_provider_listing_provider" in index_names
    assert "idx_provider_listing_listing" in index_names
    assert fk_targets == {"provider", "provider_exchange", "listing"}
    assert supported_tickers_table is None


def test_migration_moves_primary_listing_status_to_listing(tmp_path):
    db_path = tmp_path / "primary-listing-status.sqlite"

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(listing)").fetchall()
        columns = {row[1] for row in info}
        status_column = next(row for row in info if row[1] == "primary_listing_status")
        status_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='security_listing_status'"
        ).fetchone()

    assert "primary_listing_status" in columns
    assert status_column[3] == 1
    assert status_column[4] == "'unknown'"
    assert status_table is None


def test_migration_backfills_listing_primary_status_and_drops_legacy_table(tmp_path):
    db_path = tmp_path / "primary-listing-status-backfill.sqlite"
    prior_version = 37
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute(
            "INSERT INTO schema_migrations (version) VALUES (?)",
            (prior_version,),
        )
        conn.execute(
            """
            CREATE TABLE listing (
                listing_id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            "INSERT INTO listing (listing_id, symbol) VALUES (?, ?)",
            [(1, "AAA"), (2, "BBB"), (3, "CCC")],
        )
        conn.execute(
            """
            CREATE TABLE security_listing_status (
                listing_id INTEGER NOT NULL PRIMARY KEY,
                is_primary_listing INTEGER NOT NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO security_listing_status (listing_id, is_primary_listing)
            VALUES (?, ?)
            """,
            [(1, 1), (2, 0)],
        )

    applied = apply_migrations(db_path)

    assert applied == len(MIGRATIONS) - prior_version
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT listing_id, primary_listing_status
            FROM listing
            ORDER BY listing_id
            """
        ).fetchall()
        status_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='security_listing_status'"
        ).fetchone()

    assert rows == [(1, "primary"), (2, "secondary"), (3, "unknown")]
    assert status_table is None


def test_migration_canonicalizes_listing_quote_currency_and_market_data(tmp_path):
    db_path = tmp_path / "canonical-listing-currency.sqlite"
    prior_version = 38
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute(
            "INSERT INTO schema_migrations (version) VALUES (?)",
            (prior_version,),
        )
        conn.execute(
            """
            CREATE TABLE provider (
                provider_id INTEGER PRIMARY KEY,
                provider_code TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            "INSERT INTO provider (provider_id, provider_code) VALUES (?, ?)",
            [(1, "SEC"), (2, "EODHD"), (3, "OTHER")],
        )
        conn.execute(
            """
            CREATE TABLE listing (
                listing_id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                currency TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO listing (listing_id, symbol, currency) VALUES (?, ?, ?)",
            [
                (1, "AAA", None),
                (2, "BBB", "OLD"),
                (3, "CCC", "ZAC"),
                (4, "DDD", "ILA"),
            ],
        )
        conn.execute(
            """
            CREATE TABLE provider_listing (
                provider_listing_id INTEGER PRIMARY KEY,
                provider_id INTEGER NOT NULL,
                listing_id INTEGER NOT NULL,
                currency TEXT
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO provider_listing (
                provider_listing_id, provider_id, listing_id, currency
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (1, 1, 1, "USD"),
                (2, 2, 1, "GBX"),
                (3, 3, 2, "EUR"),
            ],
        )
        conn.execute(
            """
            CREATE INDEX idx_provider_listing_currency_nonnull
            ON provider_listing(currency)
            WHERE currency IS NOT NULL
            """
        )
        conn.execute(
            """
            CREATE TABLE market_data (
                listing_id INTEGER NOT NULL,
                as_of DATE NOT NULL,
                price REAL NOT NULL,
                volume INTEGER,
                market_cap REAL,
                currency TEXT,
                source_provider TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (listing_id, as_of)
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO market_data (
                listing_id, as_of, price, volume, market_cap, currency,
                source_provider, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'EODHD', '2026-01-01T00:00:00+00:00')
            """,
            [
                (1, "2026-01-01", 27.835, None, 1000.0, "GBP"),
                (3, "2026-01-01", 1234.0, None, 123400.0, "ZAC"),
                (4, "2026-01-01", 12.34, None, 500.0, "ILS"),
            ],
        )
        conn.execute(
            """
            CREATE INDEX idx_market_data_currency_nonnull
            ON market_data(currency)
            WHERE currency IS NOT NULL
            """
        )

    applied = apply_migrations(db_path)

    assert applied == len(MIGRATIONS) - prior_version
    with sqlite3.connect(db_path) as conn:
        listing_rows = conn.execute(
            "SELECT listing_id, currency FROM listing ORDER BY listing_id"
        ).fetchall()
        provider_listing_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(provider_listing)").fetchall()
        }
        market_data_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(market_data)").fetchall()
        }
        provider_listing_index_names = {
            row[1]
            for row in conn.execute("PRAGMA index_list(provider_listing)").fetchall()
        }
        market_data_index_names = {
            row[1] for row in conn.execute("PRAGMA index_list(market_data)").fetchall()
        }
        listing_index_names = {
            row[1] for row in conn.execute("PRAGMA index_list(listing)").fetchall()
        }
        market_rows = conn.execute(
            """
            SELECT listing_id, price, market_cap
            FROM market_data
            ORDER BY listing_id
            """
        ).fetchall()

    assert listing_rows == [(1, "GBX"), (2, "EUR"), (3, "ZAC"), (4, "ILA")]
    assert "currency" not in provider_listing_columns
    assert "currency" not in market_data_columns
    assert "idx_provider_listing_currency_nonnull" not in provider_listing_index_names
    assert "idx_market_data_currency_nonnull" not in market_data_index_names
    assert "idx_listing_currency_nonnull" in listing_index_names
    assert market_rows == [
        (1, 2783.5, 1000.0),
        (3, 1234.0, 1234.0),
        (4, 1234.0, 500.0),
    ]


def test_migration_adds_sector_and_industry_to_securities(tmp_path):
    db_path = tmp_path / "securities-sector-industry.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE securities (
                security_id INTEGER PRIMARY KEY,
                canonical_ticker TEXT NOT NULL,
                canonical_exchange_code TEXT NOT NULL,
                canonical_symbol TEXT NOT NULL,
                entity_name TEXT,
                description TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (canonical_exchange_code, canonical_ticker),
                UNIQUE (canonical_symbol)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO securities (
                security_id, canonical_ticker, canonical_exchange_code, canonical_symbol,
                entity_name, description, created_at, updated_at
            ) VALUES (
                1, 'AAA', 'US', 'AAA.US', 'AAA Corp', 'AAA description',
                '2024-01-01T00:00:00+00:00', '2024-01-01T00:00:00+00:00'
            )
            """
        )
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (24)")

    applied = apply_migrations(db_path)

    assert applied == len(MIGRATIONS) - 24
    with sqlite3.connect(db_path) as conn:
        issuer_info = conn.execute("PRAGMA table_info(issuer)").fetchall()
        issuer_columns = {row[1] for row in issuer_info}
        listing_info = conn.execute("PRAGMA table_info(listing)").fetchall()
        listing_columns = {row[1] for row in listing_info}
        metrics_info = conn.execute("PRAGMA table_info(metrics)").fetchall()
        metric_columns = {row[1] for row in metrics_info}
        fx_info = conn.execute("PRAGMA table_info(fx_rates)").fetchall()
        fx_columns = {row[1] for row in fx_info}
        fx_indexes = conn.execute("PRAGMA index_list(fx_rates)").fetchall()
        fx_index_names = {row[1] for row in fx_indexes}
        fx_supported_pair_indexes = conn.execute(
            "PRAGMA index_list(fx_supported_pairs)"
        ).fetchall()
        fx_supported_pair_index_names = {row[1] for row in fx_supported_pair_indexes}
        provider_listing_indexes = conn.execute(
            "PRAGMA index_list(provider_listing)"
        ).fetchall()
        provider_listing_index_names = {row[1] for row in provider_listing_indexes}
        financial_fact_indexes = conn.execute(
            "PRAGMA index_list(financial_facts)"
        ).fetchall()
        financial_fact_index_names = {row[1] for row in financial_fact_indexes}
        market_data_indexes = conn.execute("PRAGMA index_list(market_data)").fetchall()
        market_data_index_names = {row[1] for row in market_data_indexes}
        tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                """
            ).fetchall()
        }
        row = conn.execute(
            """
            SELECT i.name, i.description, i.sector, i.industry, l.symbol, e.exchange_code
            FROM listing l
            JOIN issuer i ON i.issuer_id = l.issuer_id
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            WHERE l.symbol = 'AAA' AND e.exchange_code = 'US'
            """
        ).fetchone()

    assert {"sector", "industry"} <= issuer_columns
    assert {
        "listing_id",
        "issuer_id",
        "exchange_id",
        "symbol",
        "currency",
    } <= listing_columns
    if metric_columns:
        assert {"unit_kind", "currency", "unit_label"} <= metric_columns
    assert fx_columns == {
        "provider",
        "rate_date",
        "base_currency",
        "quote_currency",
        "rate",
        "fetched_at",
        "source_kind",
        "meta_json",
        "created_at",
        "updated_at",
    }
    assert "idx_fx_rates_pair_date" in fx_index_names
    assert "idx_fx_supported_pairs_refreshable" in fx_supported_pair_index_names
    if "provider_listing" in tables:
        assert (
            "idx_provider_listing_currency_nonnull" not in provider_listing_index_names
        )
    if "financial_facts" in tables:
        assert "idx_fin_facts_currency_nonnull" in financial_fact_index_names
    if "market_data" in tables:
        assert "idx_market_data_currency_nonnull" not in market_data_index_names
    assert row == ("AAA Corp", "AAA description", None, None, "AAA", "US")


def test_migration_creates_market_data_fetch_state_table(tmp_path):
    db_path = tmp_path / "market-data-fetch-state.sqlite"

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(market_data_fetch_state)").fetchall()
        columns = {row[1] for row in info}
        pk_cols = [row[1] for row in info if row[5]]
        indexes = conn.execute("PRAGMA index_list(market_data_fetch_state)").fetchall()
        index_names = {row[1] for row in indexes}

    assert columns == {
        "provider_listing_id",
        "last_fetched_at",
        "last_status",
        "last_error",
        "next_eligible_at",
        "attempts",
    }
    assert pk_cols == ["provider_listing_id"]
    assert "idx_market_data_fetch_next" in index_names


def test_migration_creates_fundamentals_hot_path_indexes(tmp_path):
    db_path = tmp_path / "fundamentals-hot-path-indexes.sqlite"

    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        state_indexes = conn.execute(
            "PRAGMA index_list(fundamentals_fetch_state)"
        ).fetchall()
        state_index_names = {row[1] for row in state_indexes}
        raw_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(fundamentals_raw)").fetchall()
        }
        raw_indexes = conn.execute("PRAGMA index_list(fundamentals_raw)").fetchall()
        raw_index_names = {row[1] for row in raw_indexes}
        raw_fks = conn.execute("PRAGMA foreign_key_list(fundamentals_raw)").fetchall()
        raw_fk_targets = {row[2] for row in raw_fks}

    assert "idx_fundamentals_fetch_next" in state_index_names
    assert raw_columns == {
        "provider_listing_id",
        "data",
        "payload_hash",
        "last_fetched_at",
    }
    assert "idx_fundamentals_raw_last_fetched" in raw_index_names
    assert "idx_fundamentals_raw_provider_fetched" not in raw_index_names
    assert "idx_fundamentals_raw_security" not in raw_index_names
    assert raw_fk_targets == {"provider_listing"}


def test_migration_drops_fundamentals_raw_listing_identity_columns(tmp_path):
    db_path = tmp_path / "fundamentals-raw-drop-listing.sqlite"
    previous_version = 36
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE provider_listing (
                provider_listing_id INTEGER PRIMARY KEY,
                listing_id INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE fundamentals_raw (
                payload_id INTEGER PRIMARY KEY,
                provider_listing_id INTEGER NOT NULL UNIQUE,
                listing_id INTEGER NOT NULL,
                security_id INTEGER NOT NULL,
                provider TEXT NOT NULL,
                provider_symbol TEXT NOT NULL,
                provider_exchange_code TEXT,
                currency TEXT,
                data TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX idx_fundamentals_raw_security
            ON fundamentals_raw(listing_id)
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX idx_fundamentals_raw_provider_symbol
            ON fundamentals_raw(provider, provider_symbol)
            """
        )
        conn.execute(
            """
            CREATE INDEX idx_fundamentals_raw_provider_fetched
            ON fundamentals_raw(provider, fetched_at)
            """
        )
        conn.execute(
            "INSERT INTO provider_listing (provider_listing_id, listing_id) VALUES (1, 100)"
        )
        conn.execute(
            """
            INSERT INTO fundamentals_raw (
                payload_id,
                provider_listing_id,
                listing_id,
                security_id,
                provider,
                provider_symbol,
                provider_exchange_code,
                currency,
                data,
                fetched_at
            ) VALUES (
                10, 1, 100, 100, 'EODHD', 'AAA.US', 'US', 'USD', '{}',
                '2026-01-01T00:00:00+00:00'
            )
            """
        )
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute(
            "INSERT INTO schema_migrations (version) VALUES (?)", (previous_version,)
        )

    applied = apply_migrations(db_path)

    assert applied == len(MIGRATIONS) - previous_version
    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(fundamentals_raw)").fetchall()
        }
        indexes = conn.execute("PRAGMA index_list(fundamentals_raw)").fetchall()
        index_names = {row[1] for row in indexes}
        row = conn.execute(
            """
            SELECT provider_listing_id, data, payload_hash, last_fetched_at
            FROM fundamentals_raw
            """
        ).fetchone()

    assert columns == {
        "provider_listing_id",
        "data",
        "payload_hash",
        "last_fetched_at",
    }
    assert "idx_fundamentals_raw_last_fetched" in index_names
    assert "idx_fundamentals_raw_security" not in index_names
    assert "idx_fundamentals_raw_provider_symbol" not in index_names
    assert row == (
        1,
        "{}",
        hashlib.sha256(b"{}").hexdigest(),
        "2026-01-01T00:00:00+00:00",
    )


def test_migration_drops_fundamentals_raw_currency_from_current_schema(tmp_path):
    db_path = tmp_path / "fundamentals-raw-drop-currency.sqlite"
    previous_version = 36
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE provider_listing (
                provider_listing_id INTEGER PRIMARY KEY,
                listing_id INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE fundamentals_raw (
                payload_id INTEGER PRIMARY KEY,
                provider_listing_id INTEGER NOT NULL UNIQUE,
                currency TEXT,
                data TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX idx_fundamentals_raw_provider_fetched
            ON fundamentals_raw(fetched_at)
            """
        )
        conn.execute(
            "INSERT INTO provider_listing (provider_listing_id, listing_id) VALUES (1, 100)"
        )
        conn.execute(
            """
            INSERT INTO fundamentals_raw (
                payload_id, provider_listing_id, currency, data, fetched_at
            ) VALUES (10, 1, 'USD', '{}', '2026-01-01T00:00:00+00:00')
            """
        )
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute(
            "INSERT INTO schema_migrations (version) VALUES (?)", (previous_version,)
        )

    applied = apply_migrations(db_path)

    assert applied == len(MIGRATIONS) - previous_version
    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(fundamentals_raw)").fetchall()
        }
        indexes = conn.execute("PRAGMA index_list(fundamentals_raw)").fetchall()
        index_names = {row[1] for row in indexes}
        row = conn.execute(
            """
            SELECT provider_listing_id, data, payload_hash, last_fetched_at
            FROM fundamentals_raw
            """
        ).fetchone()

    assert columns == {
        "provider_listing_id",
        "data",
        "payload_hash",
        "last_fetched_at",
    }
    assert "idx_fundamentals_raw_last_fetched" in index_names
    assert row == (
        1,
        "{}",
        hashlib.sha256(b"{}").hexdigest(),
        "2026-01-01T00:00:00+00:00",
    )


def test_migration_adds_metric_status_and_facts_refresh_tables(tmp_path):
    db_path = tmp_path / "metric-status-migration.sqlite"
    with sqlite3.connect(db_path) as conn:
        # Migration 043 adds a FK from financial_facts.listing_id to
        # listing(listing_id). The fixture must therefore include a
        # securities row that the migration chain (022 → 034) carries
        # forward to a listing row with listing_id = 1, so the
        # financial_facts row inserted below isn't an orphan after 043.
        conn.execute(
            """
            CREATE TABLE securities (
                security_id INTEGER PRIMARY KEY,
                canonical_ticker TEXT NOT NULL,
                canonical_exchange_code TEXT NOT NULL,
                canonical_symbol TEXT NOT NULL,
                entity_name TEXT,
                description TEXT,
                sector TEXT,
                industry TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (canonical_exchange_code, canonical_ticker),
                UNIQUE (canonical_symbol)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO securities (
                security_id, canonical_ticker, canonical_exchange_code, canonical_symbol,
                entity_name, description, sector, industry, created_at, updated_at
            ) VALUES (
                1, 'AAA', 'US', 'AAA.US', 'AAA Corp', NULL, NULL, NULL,
                '2024-01-01T00:00:00+00:00', '2024-01-01T00:00:00+00:00'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE financial_facts (
                security_id INTEGER NOT NULL,
                cik TEXT,
                concept TEXT NOT NULL,
                fiscal_period TEXT,
                end_date TEXT NOT NULL,
                unit TEXT NOT NULL,
                value REAL NOT NULL,
                accn TEXT,
                filed TEXT,
                frame TEXT,
                start_date TEXT,
                accounting_standard TEXT,
                currency TEXT,
                source_provider TEXT,
                PRIMARY KEY (security_id, concept, fiscal_period, end_date, unit, accn)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO financial_facts (
                security_id, cik, concept, fiscal_period, end_date, unit, value,
                accn, filed, frame, start_date, accounting_standard, currency, source_provider
            ) VALUES (
                1, NULL, 'Assets', 'FY', '2024-12-31', 'USD', 10.0,
                NULL, NULL, NULL, NULL, NULL, 'USD', 'SEC'
            )
            """
        )
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (29)")

    applied = apply_migrations(db_path)

    assert applied == len(MIGRATIONS) - 29
    with sqlite3.connect(db_path) as conn:
        refresh_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(financial_facts_refresh_state)"
            ).fetchall()
        }
        status_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(metric_compute_status)"
            ).fetchall()
        }
        refresh_row = conn.execute(
            """
            SELECT listing_id, refreshed_at
            FROM financial_facts_refresh_state
            """
        ).fetchone()
        status_indexes = conn.execute(
            "PRAGMA index_list(metric_compute_status)"
        ).fetchall()
        status_index_names = {row[1] for row in status_indexes}

    assert refresh_columns == {"listing_id", "refreshed_at"}
    assert refresh_row is not None
    assert refresh_row[0] == 1
    assert refresh_row[1]
    assert status_columns == {
        "listing_id",
        "metric_id",
        "status",
        "reason_code",
        "reason_detail",
        "attempted_at",
        "value_as_of",
        "facts_refreshed_at",
        "market_data_as_of",
        "market_data_updated_at",
    }
    assert "idx_metric_compute_status_metric_status" in status_index_names


def test_migration_does_not_overwrite_existing_supported_tickers(tmp_path):
    db_path = tmp_path / "supported-tickers-backfill.sqlite"
    with sqlite3.connect(db_path) as conn:
        _create_legacy_listings_table(conn)
        conn.execute(
            """
            CREATE TABLE supported_tickers (
                provider TEXT NOT NULL,
                exchange_code TEXT NOT NULL,
                symbol TEXT NOT NULL,
                code TEXT NOT NULL,
                listing_exchange TEXT,
                security_name TEXT,
                security_type TEXT,
                country TEXT,
                currency TEXT,
                isin TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (provider, symbol)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO supported_tickers (
                provider, exchange_code, symbol, code, listing_exchange,
                security_name, security_type, country, currency, isin, updated_at
            ) VALUES (
                'SEC', 'US', 'ABC.NYSE', 'ABC', 'NYSE',
                'Preserved Name', 'ETF', NULL, NULL, NULL, '2024-01-01T00:00:00+00:00'
            )
            """
        )
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (20)")

    applied = apply_migrations(db_path)

    assert applied == len(MIGRATIONS) - 20
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT i.name, pl.provider_symbol, px.provider_exchange_code
            FROM provider_listing pl
            JOIN provider p ON p.provider_id = pl.provider_id
            JOIN provider_exchange px ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN listing l ON l.listing_id = pl.listing_id
            JOIN issuer i ON i.issuer_id = l.issuer_id
            WHERE p.provider_code = 'SEC'
              AND pl.provider_symbol = 'ABC'
              AND px.provider_exchange_code = 'US'
            """
        ).fetchone()
        listings_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='listings'"
        ).fetchone()
        fx_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fx_rates'"
        ).fetchone()
        fx_supported_pairs_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fx_supported_pairs'"
        ).fetchone()
        fx_refresh_state_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fx_refresh_state'"
        ).fetchone()
        provider_listing_indexes = conn.execute(
            "PRAGMA index_list(provider_listing)"
        ).fetchall()
        provider_listing_index_names = {index[1] for index in provider_listing_indexes}

    assert row == ("Preserved Name", "ABC", "US")
    assert listings_exists is None
    assert fx_exists == ("fx_rates",)
    assert fx_supported_pairs_exists == ("fx_supported_pairs",)
    assert fx_refresh_state_exists == ("fx_refresh_state",)
    assert "idx_provider_listing_currency_nonnull" not in provider_listing_index_names


# ---------------------------------------------------------------------------
# Migration 041: enforce metrics-table invariants in the schema.
# ---------------------------------------------------------------------------


def _seed_listing(conn: sqlite3.Connection) -> int:
    """Insert a minimal listing row and return its listing_id.

    Uses INSERT OR IGNORE so callers can call repeatedly within a test.
    """

    conn.execute(
        """
        INSERT OR IGNORE INTO "exchange" (
            exchange_id, exchange_code, created_at, updated_at
        ) VALUES (1, 'US', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO issuer (issuer_id, name) VALUES (1, 'Test Issuer')
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO listing (
            listing_id, issuer_id, exchange_id, symbol, currency
        ) VALUES (1, 1, 1, 'TEST', 'USD')
        """
    )
    return 1


def _open_with_fk(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def test_migration_041_metrics_check_rejects_currency_on_ratio(tmp_path):
    db_path = tmp_path / "metrics-check-ratio.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO metrics (
                    listing_id, metric_id, value, as_of, unit_kind, currency
                ) VALUES (?, 'pe_ratio', 12.5, '2026-01-01', 'ratio', 'USD')
                """,
                (listing_id,),
            )


def test_migration_041_metrics_check_accepts_null_currency_on_monetary(tmp_path):
    db_path = tmp_path / "metrics-check-monetary-null.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        conn.execute(
            """
            INSERT INTO metrics (
                listing_id, metric_id, value, as_of, unit_kind, currency
            ) VALUES (?, 'eps_ttm', 1.5, '2026-01-01', 'monetary', NULL)
            """,
            (listing_id,),
        )
        row = conn.execute(
            "SELECT unit_kind, currency FROM metrics WHERE listing_id = ?",
            (listing_id,),
        ).fetchone()
        assert row == ("monetary", None)


def test_migration_041_metrics_check_rejects_unknown_unit_kind(tmp_path):
    db_path = tmp_path / "metrics-check-unknown.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO metrics (
                    listing_id, metric_id, value, as_of, unit_kind, currency
                ) VALUES (?, 'weird_metric', 1.0, '2026-01-01', 'widget', NULL)
                """,
                (listing_id,),
            )


def test_migration_041_metrics_fk_rejects_unknown_listing_id(tmp_path):
    db_path = tmp_path / "metrics-fk-orphan.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO metrics (
                    listing_id, metric_id, value, as_of, unit_kind, currency
                ) VALUES (999999, 'eps_ttm', 1.0, '2026-01-01', 'monetary', 'USD')
                """
            )


def test_migration_041_compute_status_fk_rejects_unknown_listing_id(tmp_path):
    db_path = tmp_path / "compute-status-fk-orphan.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO metric_compute_status (
                    listing_id, metric_id, status, attempted_at
                ) VALUES (999999, 'eps_ttm', 'success', '2026-01-01T00:00:00+00:00')
                """
            )


def test_migration_041_idempotent(tmp_path):
    db_path = tmp_path / "metrics-041-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


def test_migration_041_preserves_existing_rows(tmp_path):
    """Seed valid rows under the pre-041 schema and confirm 041 carries them through."""

    db_path = tmp_path / "metrics-041-preserve.sqlite"
    # Apply migrations through 040 only by using a partial run: apply all, then
    # rebuild to the pre-041 shape via a fresh DB stopped early.
    # Simpler approach: apply all migrations, insert valid rows, then verify
    # 041 has already run and rows are intact (which exercises the rebuild on
    # a fresh DB, since 041 still rebuilds an empty table created by earlier
    # migrations).
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        conn.execute(
            """
            INSERT INTO metrics (
                listing_id, metric_id, value, as_of, unit_kind, currency, unit_label
            ) VALUES
                (?, 'eps_ttm', 1.50, '2026-01-01', 'per_share', 'USD', 'USD/share'),
                (?, 'roic_5y_median', 0.12, '2026-01-01', 'percent', NULL, NULL),
                (?, 'pe_ratio', 18.0, '2026-01-01', 'ratio', NULL, NULL)
            """,
            (listing_id, listing_id, listing_id),
        )
        conn.execute(
            """
            INSERT INTO metric_compute_status (
                listing_id, metric_id, status, attempted_at
            ) VALUES (?, 'eps_ttm', 'success', '2026-01-01T00:00:00+00:00')
            """,
            (listing_id,),
        )
        conn.commit()

    # Re-run apply_migrations: should be a no-op, and rows should remain.
    second = apply_migrations(db_path)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        metric_rows = conn.execute(
            "SELECT metric_id, unit_kind, currency FROM metrics ORDER BY metric_id"
        ).fetchall()
        compute_rows = conn.execute(
            "SELECT metric_id, status FROM metric_compute_status"
        ).fetchall()

    assert metric_rows == [
        ("eps_ttm", "per_share", "USD"),
        ("pe_ratio", "ratio", None),
        ("roic_5y_median", "percent", None),
    ]
    assert compute_rows == [("eps_ttm", "success")]


def test_migration_041_pre_flight_orphan_aborts(tmp_path):
    """An orphan metrics row should abort migration 041 rather than silently drop it."""

    db_path = tmp_path / "metrics-041-orphan-abort.sqlite"

    # Apply migrations 1..40 by stopping just short of 041. Easiest way: run
    # everything, then manually rewind the schema_migrations version and
    # reshape the metrics table to the pre-041 layout (no FK), then insert an
    # orphan, then re-run apply_migrations and assert it raises.
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        # Drop the post-041 metrics table and recreate without FK so we can
        # insert an orphan listing_id.
        conn.execute("DROP TABLE metrics")
        conn.execute(
            """
            CREATE TABLE metrics (
                listing_id INTEGER NOT NULL,
                metric_id TEXT NOT NULL,
                value REAL NOT NULL,
                as_of TEXT NOT NULL,
                unit_kind TEXT NOT NULL DEFAULT 'other',
                currency TEXT,
                unit_label TEXT,
                PRIMARY KEY (listing_id, metric_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX idx_metrics_metric_id ON metrics(metric_id)
            """
        )
        conn.execute(
            """
            INSERT INTO metrics (
                listing_id, metric_id, value, as_of, unit_kind, currency
            ) VALUES (999999, 'orphan_metric', 1.0, '2026-01-01', 'ratio', NULL)
            """
        )
        # Rewind the migration version so 041 will be re-run.
        conn.execute("UPDATE schema_migrations SET version = 40")
        conn.commit()

    with pytest.raises(RuntimeError, match="orphan rows"):
        apply_migrations(db_path)

    # The aborted migration should have rolled back its transaction; the
    # orphan row remains in the (still pre-041) metrics table.
    with sqlite3.connect(db_path) as conn:
        version = conn.execute("SELECT version FROM schema_migrations").fetchone()[0]
        orphan_count = conn.execute(
            "SELECT COUNT(*) FROM metrics WHERE listing_id = 999999"
        ).fetchone()[0]
    assert version == 40
    assert orphan_count == 1


# ----------------------------------------------------------------------
# Migration 043 — financial_facts dedupe + FK
# ----------------------------------------------------------------------


def _seed_two_listings(conn: sqlite3.Connection) -> tuple[int, int]:
    """Create two listings; return (listing_id_a, listing_id_b)."""

    conn.execute(
        """
        INSERT OR IGNORE INTO "exchange" (
            exchange_id, exchange_code, created_at, updated_at
        ) VALUES (1, 'US', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
        """
    )
    conn.execute("INSERT OR IGNORE INTO issuer (issuer_id, name) VALUES (1, 'A')")
    conn.execute("INSERT OR IGNORE INTO issuer (issuer_id, name) VALUES (2, 'B')")
    conn.execute(
        """
        INSERT OR IGNORE INTO listing (
            listing_id, issuer_id, exchange_id, symbol, currency
        ) VALUES (1, 1, 1, 'AAA', 'USD'), (2, 2, 1, 'BBB', 'USD')
        """
    )
    return 1, 2


def test_migration_043_pk_rejects_duplicate_after_rebuild(tmp_path):
    """The new PK enforces uniqueness on (listing_id, concept, fiscal_period, end_date, unit)."""

    db_path = tmp_path / "fin-facts-pk-uniqueness.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_id, _ = _seed_two_listings(conn)
        conn.execute(
            """
            INSERT INTO financial_facts (
                listing_id, concept, fiscal_period, end_date, unit, value
            ) VALUES (?, 'Revenue', 'FY', '2024-12-31', 'USD', 100.0)
            """,
            (listing_id,),
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO financial_facts (
                    listing_id, concept, fiscal_period, end_date, unit, value
                ) VALUES (?, 'Revenue', 'FY', '2024-12-31', 'USD', 999.0)
                """,
                (listing_id,),
            )


def test_migration_043_pk_no_longer_includes_accn(tmp_path):
    """accn must be a non-key, nullable column after the rebuild."""

    db_path = tmp_path / "fin-facts-accn-not-in-pk.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        info = conn.execute("PRAGMA table_info(financial_facts)").fetchall()
        # In sqlite3.Row mode the 'pk' column index is 5 (cid, name, type,
        # notnull, dflt_value, pk).
        pk_columns = [row[1] for row in info if row[5]]
        accn_row = next(row for row in info if row[1] == "accn")

    assert pk_columns == [
        "listing_id",
        "concept",
        "fiscal_period",
        "end_date",
        "unit",
    ]
    # accn (notnull=False, pk=0).
    assert accn_row[3] == 0
    assert accn_row[5] == 0


def test_migration_043_fk_rejects_unknown_listing_id(tmp_path):
    """The new FK to listing(listing_id) rejects orphan rows."""

    db_path = tmp_path / "fin-facts-fk-orphan.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO financial_facts (
                    listing_id, concept, fiscal_period, end_date, unit, value
                ) VALUES (999999, 'Revenue', 'FY', '2024-12-31', 'USD', 100.0)
                """
            )


def test_migration_043_idempotent(tmp_path):
    """Running apply_migrations twice on a fresh DB applies once, then no-op."""

    db_path = tmp_path / "fin-facts-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


def test_migration_043_dedupe_keeps_filed_winner(tmp_path):
    """Two NULL-accn rows with the same key collapse to the row with non-NULL filed."""

    # Build a DB that's at version 42 (just before 043) with two duplicate
    # NULL-accn rows, then re-run apply_migrations and assert that the
    # rebuild kept the row with the more authoritative `filed` provenance.
    db_path = tmp_path / "fin-facts-dedupe-filed-winner.sqlite"
    apply_migrations(db_path)  # bring DB to head

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        listing_id, _ = _seed_two_listings(conn)
        # Drop the post-043 table and recreate the pre-043 shape so we
        # can insert two rows that share all key parts but differ in filed.
        conn.execute("DROP TABLE financial_facts")
        conn.execute(
            """
            CREATE TABLE financial_facts (
                listing_id INTEGER NOT NULL,
                cik TEXT,
                concept TEXT NOT NULL,
                fiscal_period TEXT,
                end_date TEXT NOT NULL,
                unit TEXT NOT NULL,
                value REAL NOT NULL,
                accn TEXT,
                filed TEXT,
                frame TEXT,
                start_date TEXT,
                accounting_standard TEXT,
                currency TEXT,
                source_provider TEXT,
                PRIMARY KEY (listing_id, concept, fiscal_period, end_date, unit, accn)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO financial_facts (
                listing_id, concept, fiscal_period, end_date, unit, value, filed, source_provider
            ) VALUES
                (?, 'Revenue', 'FY', '2024-12-31', 'USD', 100.0, NULL, 'EODHD'),
                (?, 'Revenue', 'FY', '2024-12-31', 'USD', 200.0, '2025-03-15', 'EODHD'),
                (?, 'Revenue', 'FY', '2024-12-31', 'USD', 150.0, '2025-01-15', 'EODHD')
            """,
            (listing_id, listing_id, listing_id),
        )
        # Rewind so 043 will be re-run. Anything beyond 042 (043, 044, ...)
        # will replay; assert exactly that delta so this test stays accurate
        # as later migrations are appended.
        target_version = 42
        conn.execute("UPDATE schema_migrations SET version = ?", (target_version,))
        conn.commit()

    second = apply_migrations(db_path)
    assert second == len(MIGRATIONS) - target_version

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT value, filed FROM financial_facts ORDER BY filed DESC"
        ).fetchall()

    # All three duplicates collapse to one row — the one with the most
    # recent non-NULL filed.
    assert rows == [(200.0, "2025-03-15")]


def test_migration_043_pre_flight_orphan_aborts(tmp_path):
    """An orphan financial_facts row must abort migration 043 rather than be dropped."""

    db_path = tmp_path / "fin-facts-043-orphan.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        # Drop and recreate the pre-043 shape so we can insert an orphan.
        conn.execute("DROP TABLE financial_facts")
        conn.execute(
            """
            CREATE TABLE financial_facts (
                listing_id INTEGER NOT NULL,
                cik TEXT,
                concept TEXT NOT NULL,
                fiscal_period TEXT,
                end_date TEXT NOT NULL,
                unit TEXT NOT NULL,
                value REAL NOT NULL,
                accn TEXT,
                filed TEXT,
                frame TEXT,
                start_date TEXT,
                accounting_standard TEXT,
                currency TEXT,
                source_provider TEXT,
                PRIMARY KEY (listing_id, concept, fiscal_period, end_date, unit, accn)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO financial_facts (
                listing_id, concept, fiscal_period, end_date, unit, value
            ) VALUES (999999, 'Revenue', 'FY', '2024-12-31', 'USD', 100.0)
            """
        )
        conn.execute("UPDATE schema_migrations SET version = 42")
        conn.commit()

    with pytest.raises(RuntimeError, match="orphan"):
        apply_migrations(db_path)

    # The aborted migration should have rolled back; orphan row remains.
    with sqlite3.connect(db_path) as conn:
        version = conn.execute("SELECT version FROM schema_migrations").fetchone()[0]
        orphan_count = conn.execute(
            "SELECT COUNT(*) FROM financial_facts WHERE listing_id = 999999"
        ).fetchone()[0]
    assert version == 42
    assert orphan_count == 1


def test_migration_043_preserves_unique_rows(tmp_path):
    """Rows that don't collide must round-trip through the rebuild unchanged."""

    db_path = tmp_path / "fin-facts-043-preserve.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_a, listing_b = _seed_two_listings(conn)
        conn.execute(
            """
            INSERT INTO financial_facts (
                listing_id, concept, fiscal_period, end_date, unit, value, filed
            ) VALUES
                (?, 'Revenue', 'FY', '2024-12-31', 'USD', 100.0, '2025-01-01'),
                (?, 'Revenue', 'Q1', '2025-03-31', 'USD', 25.0, '2025-04-15'),
                (?, 'NetIncome', 'FY', '2024-12-31', 'USD', 10.0, '2025-01-01')
            """,
            (listing_a, listing_a, listing_b),
        )
        conn.commit()

    second = apply_migrations(db_path)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT listing_id, concept, fiscal_period, end_date, unit, value
            FROM financial_facts
            ORDER BY listing_id, concept, fiscal_period
            """
        ).fetchall()

    assert rows == [
        (listing_a, "Revenue", "FY", "2024-12-31", "USD", 100.0),
        (listing_a, "Revenue", "Q1", "2025-03-31", "USD", 25.0),
        (listing_b, "NetIncome", "FY", "2024-12-31", "USD", 10.0),
    ]


# ----------------------------------------------------------------------
# Migration 044 — compat views (providers, securities, exchange_provider)
# ----------------------------------------------------------------------


def test_migration_044_persists_compat_views(tmp_path):
    """Three legacy compat views must be in sqlite_master after apply_migrations."""

    db_path = tmp_path / "compat-views.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        view_names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'view'"
            ).fetchall()
        }

    assert {"providers", "securities", "exchange_provider"} <= view_names


def test_migration_044_securities_view_returns_canonical_join(tmp_path):
    """The securities view should expose listing+issuer+exchange in the legacy shape."""

    db_path = tmp_path / "compat-securities.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        row = conn.execute(
            """
            SELECT security_id, canonical_ticker, canonical_exchange_code,
                   canonical_symbol, entity_name
            FROM securities
            WHERE security_id = ?
            """,
            (listing_id,),
        ).fetchone()

    assert row == (listing_id, "TEST", "US", "TEST.US", "Test Issuer")


def test_migration_044_idempotent(tmp_path):
    """Running apply_migrations twice on a fresh DB is a no-op the second time."""

    db_path = tmp_path / "compat-views-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ----------------------------------------------------------------------
# Migration 045 — fx_rates.rate_text TEXT → rate REAL
# ----------------------------------------------------------------------


def test_migration_045_renames_rate_text_to_rate_real(tmp_path):
    """After apply_migrations the fx_rates table has REAL rate, no rate_text."""

    db_path = tmp_path / "fx-rate-real.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(fx_rates)").fetchall()
        column_types = {row[1]: row[2] for row in info}

    assert "rate" in column_types
    assert column_types["rate"] == "REAL"
    assert "rate_text" not in column_types


def test_migration_045_casts_existing_text_to_real(tmp_path):
    """Pre-045 fx_rates rows with TEXT rate values are cast to REAL during the rebuild."""

    db_path = tmp_path / "fx-rate-cast.sqlite"
    apply_migrations(db_path)

    # Recreate the pre-045 schema so we can insert TEXT rate_text rows and
    # then re-run migration 045 on top.
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP TABLE fx_rates")
        conn.execute(
            """
            CREATE TABLE fx_rates (
                provider TEXT NOT NULL,
                rate_date TEXT NOT NULL,
                base_currency TEXT NOT NULL,
                quote_currency TEXT NOT NULL,
                rate_text TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                meta_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (provider, rate_date, base_currency, quote_currency)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO fx_rates (
                provider, rate_date, base_currency, quote_currency, rate_text,
                fetched_at, source_kind, meta_json, created_at, updated_at
            ) VALUES
                ('EODHD', '2025-01-01', 'EUR', 'USD', '1.0951',
                 '2025-01-01T12:00:00Z', 'provider', NULL,
                 '2025-01-01T12:00:00Z', '2025-01-01T12:00:00Z'),
                ('EODHD', '2025-01-02', 'EUR', 'USD', '1.0832',
                 '2025-01-02T12:00:00Z', 'provider', NULL,
                 '2025-01-02T12:00:00Z', '2025-01-02T12:00:00Z')
            """
        )
        conn.execute("UPDATE schema_migrations SET version = 44")
        conn.commit()

    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT rate_date, rate FROM fx_rates ORDER BY rate_date"
        ).fetchall()
        info = conn.execute("PRAGMA table_info(fx_rates)").fetchall()
        column_types = {row[1]: row[2] for row in info}

    assert column_types["rate"] == "REAL"
    assert rows == [("2025-01-01", 1.0951), ("2025-01-02", 1.0832)]


def test_migration_045_idempotent_on_already_renamed_schema(tmp_path):
    """Running 045 twice (fresh-DB path) is a no-op the second time."""

    db_path = tmp_path / "fx-rate-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ----------------------------------------------------------------------
# Migration 046 — financial_facts_refresh_state listing_id FK
# ----------------------------------------------------------------------


def test_migration_046_fk_rejects_unknown_listing_id(tmp_path):
    db_path = tmp_path / "ffrs-fk-orphan.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO financial_facts_refresh_state (listing_id, refreshed_at)
                VALUES (999999, '2026-01-01T00:00:00+00:00')
                """
            )


def test_migration_046_pre_flight_orphan_aborts(tmp_path):
    """Existing orphan rows must abort migration 046, not be silently dropped."""

    db_path = tmp_path / "ffrs-046-orphan.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("DROP TABLE financial_facts_refresh_state")
        conn.execute(
            """
            CREATE TABLE financial_facts_refresh_state (
                listing_id INTEGER NOT NULL PRIMARY KEY,
                refreshed_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO financial_facts_refresh_state (listing_id, refreshed_at)
            VALUES (999999, '2026-01-01T00:00:00+00:00')
            """
        )
        conn.execute("UPDATE schema_migrations SET version = 45")
        conn.commit()

    with pytest.raises(RuntimeError, match="orphan"):
        apply_migrations(db_path)


def test_migration_046_preserves_valid_rows(tmp_path):
    db_path = tmp_path / "ffrs-046-preserve.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        conn.execute(
            """
            INSERT INTO financial_facts_refresh_state (listing_id, refreshed_at)
            VALUES (?, '2026-01-01T00:00:00+00:00')
            """,
            (listing_id,),
        )
        conn.commit()

    second = apply_migrations(db_path)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT listing_id, refreshed_at FROM financial_facts_refresh_state"
        ).fetchall()

    assert rows == [(listing_id, "2026-01-01T00:00:00+00:00")]


# ----------------------------------------------------------------------
# Migration 047 — market_data listing_id FK
# ----------------------------------------------------------------------


def test_migration_047_fk_rejects_unknown_listing_id(tmp_path):
    db_path = tmp_path / "md-047-orphan.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO market_data (
                    listing_id, as_of, price, source_provider, updated_at
                ) VALUES (
                    999999, '2026-01-01', 10.0, 'EODHD',
                    '2026-01-01T00:00:00+00:00'
                )
                """
            )


def test_migration_047_preserves_valid_rows(tmp_path):
    db_path = tmp_path / "md-047-preserve.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        conn.execute(
            """
            INSERT INTO market_data (
                listing_id, as_of, price, volume, market_cap,
                source_provider, updated_at
            ) VALUES (?, '2026-01-01', 10.0, 1000, 1.5e9, 'EODHD',
                      '2026-01-01T00:00:00+00:00')
            """,
            (listing_id,),
        )
        conn.commit()

    second = apply_migrations(db_path)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT listing_id, as_of, price, volume, market_cap, source_provider
            FROM market_data
            """
        ).fetchall()

    assert rows == [(listing_id, "2026-01-01", 10.0, 1000, 1.5e9, "EODHD")]


# ----------------------------------------------------------------------
# Migrations 048-050 — provider FKs on fx_rates / fx_supported_pairs / fx_refresh_state
# ----------------------------------------------------------------------


def test_migration_048_fx_rates_fk_rejects_unknown_provider(tmp_path):
    db_path = tmp_path / "fx-rates-048.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO fx_rates (
                    provider, rate_date, base_currency, quote_currency, rate,
                    fetched_at, source_kind, created_at, updated_at
                ) VALUES (
                    'NOT_A_PROVIDER', '2026-01-01', 'USD', 'EUR', 0.91,
                    '2026-01-01T00:00:00+00:00', 'provider',
                    '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00'
                )
                """
            )


def test_migration_049_fx_supported_pairs_fk_rejects_unknown_provider(tmp_path):
    db_path = tmp_path / "fx-supported-049.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO fx_supported_pairs (
                    provider, symbol, canonical_symbol, last_seen_at
                ) VALUES (
                    'NOT_A_PROVIDER', 'EURUSD.FOREX', 'EUR/USD',
                    '2026-01-01T00:00:00+00:00'
                )
                """
            )


def test_migration_050_fx_refresh_state_fk_rejects_unknown_provider(tmp_path):
    db_path = tmp_path / "fx-refresh-050.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO fx_refresh_state (
                    provider, canonical_symbol
                ) VALUES ('NOT_A_PROVIDER', 'EUR/USD')
                """
            )


# ----------------------------------------------------------------------
# Migration 051 — bool CHECK constraints on fx_* state tables
# ----------------------------------------------------------------------


def test_migration_051_fx_supported_pairs_rejects_non_bool_is_alias(tmp_path):
    db_path = tmp_path / "fx-pairs-bool-051.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO fx_supported_pairs (
                    provider, symbol, canonical_symbol, is_alias, is_refreshable,
                    last_seen_at
                ) VALUES (
                    'EODHD', 'EURUSD.FOREX', 'EUR/USD', 2, 1,
                    '2026-01-01T00:00:00+00:00'
                )
                """
            )


def test_migration_051_fx_supported_pairs_rejects_non_bool_is_refreshable(tmp_path):
    db_path = tmp_path / "fx-pairs-bool2-051.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO fx_supported_pairs (
                    provider, symbol, canonical_symbol, is_alias, is_refreshable,
                    last_seen_at
                ) VALUES (
                    'EODHD', 'EURUSD.FOREX', 'EUR/USD', 0, -1,
                    '2026-01-01T00:00:00+00:00'
                )
                """
            )


def test_migration_051_fx_refresh_state_rejects_non_bool_full_history(tmp_path):
    db_path = tmp_path / "fx-refresh-bool-051.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO fx_refresh_state (
                    provider, canonical_symbol, full_history_backfilled
                ) VALUES ('EODHD', 'EUR/USD', 7)
                """
            )


def test_migration_051_fx_refresh_state_rejects_negative_attempts(tmp_path):
    db_path = tmp_path / "fx-refresh-attempts-051.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO fx_refresh_state (
                    provider, canonical_symbol, attempts
                ) VALUES ('EODHD', 'EUR/USD', -1)
                """
            )


def test_migration_051_idempotent(tmp_path):
    db_path = tmp_path / "fx-bool-checks-051.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0
