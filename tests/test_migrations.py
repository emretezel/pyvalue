import hashlib
import sqlite3

import pytest

from pyvalue.migrations import MIGRATIONS, _ensure_migrations_table, apply_migrations


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
        # The legacy ``listings`` table cannot carry a quote currency through
        # the chain (migration 002 rebuilds it without the column), so the
        # canonicalised listing would land with currency NULL and migration
        # 069 would then purge it. Seed the matching catalog row in the
        # ``supported_tickers`` shape migration 022 reads, carrying USD, so the
        # listing survives. Migration 021 uses INSERT OR IGNORE, so this
        # pre-seeded row (keyed by provider + the symbol 021 derives,
        # 'ABC.NYSE') wins and keeps the expected issuer name 'ABC Corp'.
        # Pinning the start at version 20 makes this modern supported_tickers
        # shape valid (mirrors test_migration_does_not_overwrite_existing_supported_tickers).
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
                'ABC Corp', 'Common Stock', NULL, 'USD', NULL,
                '2020-01-01T00:00:00Z'
            )
            """
        )
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (20)")

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
            JOIN provider_exchange px ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN provider p ON p.provider_id = px.provider_id
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
            JOIN provider_exchange px
              ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN provider p ON p.provider_id = px.provider_id
            """
        ).fetchone()[0]
        fundamentals_raw_join_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM fundamentals_raw fr
            JOIN provider_listing pl ON pl.provider_listing_id = fr.provider_listing_id
            JOIN provider_exchange px
              ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN provider p ON p.provider_id = px.provider_id
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
        "provider_exchange_id",
        "provider_symbol",
        "listing_id",
    }
    assert pk_cols == ["provider_listing_id"]
    assert "idx_provider_listing_provider" not in index_names
    assert "idx_provider_listing_listing" in index_names
    assert fk_targets == {"provider_exchange", "listing"}
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
    # Migration 069 then converts the subunit-listing prices to the major
    # currency (divide by 100); market_cap is left unchanged.
    assert market_rows == [
        (1, 27.835, 1000.0),
        (3, 12.34, 1234.0),
        (4, 12.34, 500.0),
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
        # The bare ``securities`` fixture canonicalises to a listing with no
        # provider_listing and therefore a NULL currency, which migration 069
        # now purges. Seed the matching ``supported_tickers`` catalog row
        # (canonical symbol 'AAA.US') carrying USD so migration 022 builds a
        # provider_listing whose currency migration 039 backfills onto the
        # listing -- keeping the listing alive for the assertions below.
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
                'EODHD', 'US', 'AAA.US', 'AAA', 'US',
                'AAA Corp', 'Common Stock', NULL, 'USD', NULL,
                '2024-01-01T00:00:00+00:00'
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
    # ``idx_market_data_fetch_next`` was dropped by migration 067 as
    # unused; see ``test_migration_067_drops_unused_indexes``.
    assert "idx_market_data_fetch_next" not in index_names


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

    # ``idx_fundamentals_fetch_next`` and
    # ``idx_fundamentals_raw_last_fetched`` were dropped by migration
    # 067 as unused; see ``test_migration_067_drops_unused_indexes``.
    assert "idx_fundamentals_fetch_next" not in state_index_names
    assert raw_columns == {
        "provider_listing_id",
        "data",
        "payload_hash",
        "last_fetched_at",
    }
    assert "idx_fundamentals_raw_last_fetched" not in raw_index_names
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
    # ``idx_fundamentals_raw_last_fetched`` was dropped by migration
    # 067; see ``test_migration_067_drops_unused_indexes``.
    assert "idx_fundamentals_raw_last_fetched" not in index_names
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
    # ``idx_fundamentals_raw_last_fetched`` was dropped by migration
    # 067; see ``test_migration_067_drops_unused_indexes``.
    assert "idx_fundamentals_raw_last_fetched" not in index_names
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
        # Migration 069 purges listings with a NULL currency (and their
        # financial_facts / refresh-state rows). The bare securities fixture
        # yields a currency-less listing, so seed the matching
        # ``supported_tickers`` catalog row (canonical symbol 'AAA.US')
        # carrying USD; migration 022 then builds a provider_listing whose
        # currency migration 039 backfills onto listing 1, keeping it and its
        # financial_facts row alive for the refresh-state assertions below.
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
                'SEC', 'US', 'AAA.US', 'AAA', 'US',
                'AAA Corp', 'Common Stock', NULL, 'USD', NULL,
                '2024-01-01T00:00:00+00:00'
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
    # ``idx_metric_compute_status_metric_status`` was dropped by
    # migration 067; see ``test_migration_067_drops_unused_indexes``.
    assert "idx_metric_compute_status_metric_status" not in status_index_names


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
                'Preserved Name', 'ETF', NULL, 'USD', NULL,
                '2024-01-01T00:00:00+00:00'
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
            JOIN provider_exchange px ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN provider p ON p.provider_id = px.provider_id
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
        # Pre-041 also created ``idx_metrics_metric_id`` on this
        # table, but migration 067 has since dropped it as unused; the
        # fixture no longer recreates it because the rest of the test
        # only exercises orphan detection in migration 041.
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
    """The PK enforces uniqueness on (listing_id, concept, fiscal_period, end_date).

    Migration 071 dropped ``unit`` from the key, so two rows that share those
    four columns collide regardless of their (now enum) ``unit_kind``.
    """

    db_path = tmp_path / "fin-facts-pk-uniqueness.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_id, _ = _seed_two_listings(conn)
        conn.execute(
            """
            INSERT INTO financial_facts (
                listing_id, concept, fiscal_period, end_date, unit_kind, value, currency
            ) VALUES (?, 'Revenue', 'FY', '2024-12-31', 'monetary', 100.0, 'USD')
            """,
            (listing_id,),
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO financial_facts (
                    listing_id, concept, fiscal_period, end_date, unit_kind, value,
                    currency
                ) VALUES (?, 'Revenue', 'FY', '2024-12-31', 'monetary', 999.0, 'USD')
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
                    listing_id, concept, fiscal_period, end_date, unit_kind, value,
                    currency
                ) VALUES (999999, 'Revenue', 'FY', '2024-12-31', 'monetary', 100.0, 'USD')
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
        # Rewind so 043 will be re-run.
        rewind_to = 42
        conn.execute("UPDATE schema_migrations SET version = ?", (rewind_to,))
        conn.commit()

    # Apply only THROUGH migration 043: the later migration 071 rebuilds
    # financial_facts empty, which would wipe the rows this dedupe regression
    # asserts on. ``target_version`` isolates 043 so its behaviour stays testable.
    second = apply_migrations(db_path, target_version=43)
    assert second == 43 - rewind_to

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
                listing_id, concept, fiscal_period, end_date, unit_kind, value,
                filed, currency
            ) VALUES
                (?, 'Revenue', 'FY', '2024-12-31', 'monetary', 100.0, '2025-01-01', 'USD'),
                (?, 'Revenue', 'Q1', '2025-03-31', 'monetary', 25.0, '2025-04-15', 'USD'),
                (?, 'NetIncome', 'FY', '2024-12-31', 'monetary', 10.0, '2025-01-01', 'USD')
            """,
            (listing_a, listing_a, listing_b),
        )
        conn.commit()

    second = apply_migrations(db_path)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT listing_id, concept, fiscal_period, end_date, unit_kind, value
            FROM financial_facts
            ORDER BY listing_id, concept, fiscal_period
            """
        ).fetchall()

    assert rows == [
        (listing_a, "Revenue", "FY", "2024-12-31", "monetary", 100.0),
        (listing_a, "Revenue", "Q1", "2025-03-31", "monetary", 25.0),
        (listing_b, "NetIncome", "FY", "2024-12-31", "monetary", 10.0),
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

    # Apply only THROUGH migration 045. Re-running the whole chain would replay
    # migration 059, whose pre-flight queries the legacy ``financial_facts.unit``
    # column that migration 071 has already renamed to ``unit_kind`` on this
    # head-shaped schema.
    apply_migrations(db_path, target_version=45)

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


# ---------------------------------------------------------------------------
# Migration 053: drop runtime ``provider`` / ``provider_symbol`` columns from
# ``market_data_fetch_state`` and the legacy unique index that paired them.
# ---------------------------------------------------------------------------


def test_migration_053_market_data_fetch_state_columns_are_canonical(tmp_path):
    """Fresh DBs end with the canonical fetch-state shape — no provider cols."""

    db_path = tmp_path / "fetch-state-053.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(market_data_fetch_state)").fetchall()
        columns = {row[1] for row in info}
        index_names = {
            row[1]
            for row in conn.execute(
                "PRAGMA index_list(market_data_fetch_state)"
            ).fetchall()
        }

    assert columns == {
        "provider_listing_id",
        "last_fetched_at",
        "last_status",
        "last_error",
        "next_eligible_at",
        "attempts",
    }
    assert "provider" not in columns
    assert "provider_symbol" not in columns
    assert "idx_market_data_fetch_state_provider_symbol" not in index_names


def test_migration_053_drops_runtime_added_columns(tmp_path):
    """Simulate a DB whose runtime path re-added provider/provider_symbol;
    migration 053 must drop them back out without losing other rows."""

    db_path = tmp_path / "fetch-state-053-runtime.sqlite"

    # Bring the DB up to migration 052 (the last migration before 053).
    target_version = 52
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
    apply_migrations(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM schema_migrations")
        conn.execute(
            "INSERT INTO schema_migrations (version) VALUES (?)",
            (target_version,),
        )
        # apply_migrations already pre-seeds provider, provider_exchange,
        # and exchange. Reuse those IDs to avoid UNIQUE collisions.
        conn.execute("INSERT INTO issuer (issuer_id, name) VALUES (1, 'Issuer A')")
        # Migration 069 makes ``listing.currency`` NOT NULL and purges any
        # currency-less listing (and its provider_listing/fetch-state
        # children), so the fixture must give this listing a currency to
        # survive to the assertions below.
        conn.execute(
            """
            INSERT INTO listing (listing_id, issuer_id, exchange_id, symbol, currency)
            VALUES (1, 1, 1, 'AAA', 'USD')
            """
        )
        conn.execute(
            """
            INSERT INTO provider_listing (
                provider_listing_id, provider_exchange_id, provider_symbol, listing_id
            ) VALUES (1, 1, 'AAA', 1)
            """
        )
        # Recreate the runtime drift: add the legacy columns + index
        # after migration 040 had cleaned them up, then write a row
        # using the legacy shape.
        conn.execute("ALTER TABLE market_data_fetch_state ADD COLUMN provider TEXT")
        conn.execute(
            "ALTER TABLE market_data_fetch_state ADD COLUMN provider_symbol TEXT"
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX idx_market_data_fetch_state_provider_symbol
            ON market_data_fetch_state(provider, provider_symbol)
            """
        )
        conn.execute(
            """
            INSERT INTO market_data_fetch_state (
                provider_listing_id, provider, provider_symbol,
                last_fetched_at, last_status, attempts
            ) VALUES (1, 'EODHD', 'AAA.US', '2026-04-01T00:00:00+00:00', 'ok', 0)
            """
        )

    # Apply only THROUGH migration 053. Re-running the whole chain would replay
    # migration 059, whose pre-flight queries the legacy ``financial_facts.unit``
    # column that migration 071 has already renamed to ``unit_kind`` on this
    # head-shaped schema.
    applied = apply_migrations(db_path, target_version=53)
    assert applied == 53 - target_version

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(market_data_fetch_state)").fetchall()
        columns = {row[1] for row in info}
        index_names = {
            row[1]
            for row in conn.execute(
                "PRAGMA index_list(market_data_fetch_state)"
            ).fetchall()
        }
        row = conn.execute(
            """
            SELECT provider_listing_id, last_fetched_at, last_status, attempts
            FROM market_data_fetch_state
            """
        ).fetchone()

    assert "provider" not in columns
    assert "provider_symbol" not in columns
    assert "idx_market_data_fetch_state_provider_symbol" not in index_names
    assert row == (1, "2026-04-01T00:00:00+00:00", "ok", 0)


def test_migration_053_idempotent(tmp_path):
    db_path = tmp_path / "fetch-state-053-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 054: drop derivable ``provider_listing.provider_id`` column,
# rebuild ``provider_listing_catalog`` view to join through provider_exchange.
# ---------------------------------------------------------------------------


def test_migration_054_provider_listing_drops_provider_id(tmp_path):
    """Fresh DB ends without provider_id on provider_listing."""

    db_path = tmp_path / "provider-listing-054.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(provider_listing)").fetchall()
        columns = {row[1] for row in info}
        index_names = {
            row[1]
            for row in conn.execute("PRAGMA index_list(provider_listing)").fetchall()
        }
        fk_targets = {
            row[2]
            for row in conn.execute(
                "PRAGMA foreign_key_list(provider_listing)"
            ).fetchall()
        }

    assert columns == {
        "provider_listing_id",
        "provider_exchange_id",
        "provider_symbol",
        "listing_id",
    }
    assert "idx_provider_listing_provider" not in index_names
    assert "idx_provider_listing_listing" in index_names
    # provider_id FK + composite (provider_exchange_id, provider_id) FK
    # should both be gone; only provider_exchange_id and listing_id
    # references remain.
    assert fk_targets == {"provider_exchange", "listing"}


def test_migration_054_view_resolves_provider_through_exchange(tmp_path):
    """The rebuilt provider_listing_catalog view must still surface
    ``provider`` correctly even though provider_id was dropped from the
    base table — it should join through provider_exchange instead."""

    db_path = tmp_path / "provider-listing-catalog-054.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        # apply_migrations seeds provider/provider_exchange/exchange.
        # Reuse the SEC/US row at exchange_id=1, provider_exchange_id=1,
        # provider_id=3 (provider_code='SEC').
        conn.execute("INSERT INTO issuer (issuer_id, name) VALUES (1, 'A')")
        # Migration 069 made ``listing.currency`` NOT NULL; supply a currency
        # so this listing survives the (already-applied) purge and the
        # catalog-view lookup below still finds it.
        conn.execute(
            """
            INSERT INTO listing (listing_id, issuer_id, exchange_id, symbol, currency)
            VALUES (1, 1, 1, 'AAA', 'USD')
            """
        )
        conn.execute(
            """
            INSERT INTO provider_listing (
                provider_listing_id, provider_exchange_id, provider_symbol,
                listing_id
            ) VALUES (1000, 1, 'AAA', 1)
            """
        )

        row = conn.execute(
            """
            SELECT provider, provider_id, provider_listing_id, provider_symbol
            FROM provider_listing_catalog
            WHERE provider_listing_id = 1000
            """
        ).fetchone()

    # SEC provider has provider_id=3 in the seeded registry; provider_symbol
    # for SEC is built without the exchange suffix per the catalog view's
    # CASE expression, so the canonical form is 'AAA.US'.
    assert row == ("SEC", 3, 1000, "AAA.US")


def test_migration_054_blocks_on_drift_between_provider_listing_and_exchange(
    tmp_path,
):
    """If provider_listing.provider_id disagrees with
    provider_exchange.provider_id, the migration must abort to avoid
    silently losing information."""

    db_path = tmp_path / "provider-listing-drift-054.sqlite"

    # Bring the DB up through migration 053 (one before 054).
    apply_migrations(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM schema_migrations")
        conn.execute(
            "INSERT INTO schema_migrations (version) VALUES (?)",
            (53,),
        )
        # The fresh DB already has providers (EODHD=1, FRANKFURTER=2,
        # SEC=3), an exchange (id=1, code='US'), and a single
        # provider_exchange (id=1, provider_id=3, code='US'). Drop and
        # rebuild provider_listing in the pre-054 shape so we can
        # introduce drift, then insert a row whose provider_id (1, EODHD)
        # disagrees with provider_exchange.provider_id (3, SEC).
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("DROP VIEW IF EXISTS supported_tickers")
        conn.execute("DROP VIEW IF EXISTS provider_listing_catalog")
        conn.execute("DROP TABLE provider_listing")
        conn.execute(
            """
            CREATE TABLE provider_listing (
                provider_listing_id INTEGER PRIMARY KEY,
                provider_id INTEGER NOT NULL,
                provider_exchange_id INTEGER NOT NULL,
                provider_symbol TEXT NOT NULL,
                listing_id INTEGER NOT NULL
            )
            """
        )
        conn.execute("INSERT INTO issuer (issuer_id, name) VALUES (1, 'A')")
        # ``apply_migrations`` already brought the DB to head, so the
        # ``listing`` table carries the migration-069 ``currency NOT NULL``
        # constraint; the insert must supply a currency.
        conn.execute(
            """
            INSERT INTO listing (listing_id, issuer_id, exchange_id, symbol, currency)
            VALUES (1, 1, 1, 'AAA', 'USD')
            """
        )
        # Drift: provider_listing.provider_id = 1 (EODHD) but the
        # matching provider_exchange row carries provider_id = 3 (SEC).
        conn.execute(
            """
            INSERT INTO provider_listing (
                provider_listing_id, provider_id, provider_exchange_id,
                provider_symbol, listing_id
            ) VALUES (1, 1, 1, 'AAA', 1)
            """
        )

    with pytest.raises(RuntimeError, match="migration 054 aborted"):
        apply_migrations(db_path)


def test_migration_054_idempotent(tmp_path):
    db_path = tmp_path / "provider-listing-054-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 055: status enum CHECK constraints on metric_compute_status,
# market_data_fetch_state, fx_refresh_state.
# ---------------------------------------------------------------------------


def test_migration_055_metric_compute_status_rejects_unknown_status(tmp_path):
    db_path = tmp_path / "metric-status-055.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO metric_compute_status (
                    listing_id, metric_id, status, attempted_at
                ) VALUES (?, 'm1', 'wat', '2026-01-01T00:00:00+00:00')
                """,
                (listing_id,),
            )


def test_migration_055_market_data_fetch_state_rejects_unknown_status(tmp_path):
    db_path = tmp_path / "fetch-status-055.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        _seed_listing(conn)
        conn.execute(
            """
            INSERT INTO provider_listing (
                provider_listing_id, provider_exchange_id, provider_symbol,
                listing_id
            ) VALUES (1, 1, 'TEST', 1)
            """
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO market_data_fetch_state (
                    provider_listing_id, last_status
                ) VALUES (1, 'pending')
                """
            )


def test_migration_055_fx_refresh_state_rejects_unknown_status(tmp_path):
    db_path = tmp_path / "fx-refresh-status-055.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO fx_refresh_state (
                    provider, canonical_symbol, last_status
                ) VALUES ('EODHD', 'EUR/USD', 'pending')
                """
            )


def test_migration_055_idempotent(tmp_path):
    db_path = tmp_path / "status-checks-055-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 056: listing.symbol + listing.currency format CHECKs.
# ---------------------------------------------------------------------------


def test_migration_056_listing_rejects_lowercase_symbol(tmp_path):
    db_path = tmp_path / "listing-symbol-056.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO "exchange" (
                exchange_id, exchange_code, created_at, updated_at
            ) VALUES (1, 'US', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO issuer (issuer_id, name) VALUES (1, 'Test')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO listing (
                    listing_id, issuer_id, exchange_id, symbol
                ) VALUES (99, 1, 1, 'aapl')
                """
            )


def test_migration_056_listing_rejects_bad_currency_form(tmp_path):
    db_path = tmp_path / "listing-currency-056.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO "exchange" (
                exchange_id, exchange_code, created_at, updated_at
            ) VALUES (1, 'US', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO issuer (issuer_id, name) VALUES (1, 'Test')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO listing (
                    listing_id, issuer_id, exchange_id, symbol, currency
                ) VALUES (99, 1, 1, 'AAA', 'usd')
                """
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO listing (
                    listing_id, issuer_id, exchange_id, symbol, currency
                ) VALUES (98, 1, 1, 'BBB', 'GBP0.01')
                """
            )


def test_migration_056_listing_accepts_subunit_currencies(tmp_path):
    """Subunit codes (GBX, ZAC, ILA) are 3-char uppercase and must pass."""

    db_path = tmp_path / "listing-subunit-056.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO "exchange" (
                exchange_id, exchange_code, created_at, updated_at
            ) VALUES (1, 'LSE', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO issuer (issuer_id, name) VALUES (1, 'Test')"
        )
        conn.execute(
            """
            INSERT INTO listing (
                listing_id, issuer_id, exchange_id, symbol, currency
            ) VALUES (1, 1, 1, 'AAPL', 'GBX')
            """
        )

        row = conn.execute(
            "SELECT currency FROM listing WHERE listing_id = 1"
        ).fetchone()
        assert row[0] == "GBX"


def test_migration_056_idempotent(tmp_path):
    db_path = tmp_path / "listing-checks-056-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 057: provider_exchange.currency cleanup + format CHECK.
# ---------------------------------------------------------------------------


def test_migration_057_provider_exchange_normalizes_unknown_currency(tmp_path):
    """A pre-057 row with currency='UNKNOWN' is rewritten to NULL."""

    db_path = tmp_path / "provider-exchange-unknown-057.sqlite"

    # Apply migrations through 056 (one before 057) so the
    # provider_exchange table is in its pre-057 shape (no currency
    # CHECK), then seed the dirty row directly. apply_migrations()
    # always runs to len(MIGRATIONS) so we drive each migration by
    # hand here.
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")
    _ensure_migrations_table(conn)
    conn.commit()
    for i, migration in enumerate(MIGRATIONS[:56], start=1):
        conn.execute("BEGIN")
        migration(conn)
        conn.execute("DELETE FROM schema_migrations")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (i,))
        conn.commit()
    conn.execute(
        """
        UPDATE provider_exchange SET currency = 'UNKNOWN'
        WHERE provider_exchange_id = 1
        """
    )
    conn.commit()
    conn.close()

    apply_migrations(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT currency FROM provider_exchange WHERE provider_exchange_id = 1"
        ).fetchone()
    assert row[0] is None


def test_migration_057_provider_exchange_rejects_bad_currency(tmp_path):
    db_path = tmp_path / "provider-exchange-currency-057.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO provider_exchange (
                    provider_id, provider_exchange_code, exchange_id,
                    currency, updated_at
                ) VALUES (1, 'XYZ', 1, 'usd',
                          '2026-01-01T00:00:00+00:00')
                """
            )


def test_migration_057_idempotent(tmp_path):
    db_path = tmp_path / "provider-exchange-057-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 058: fx_rates source_kind + base/quote currency CHECKs.
# ---------------------------------------------------------------------------


def test_migration_058_fx_rates_rejects_lowercase_currency(tmp_path):
    db_path = tmp_path / "fx-rates-currency-058.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO fx_rates (
                    provider, rate_date, base_currency, quote_currency,
                    rate, fetched_at, source_kind, created_at, updated_at
                ) VALUES (
                    'EODHD', '2026-01-01', 'usd', 'EUR', 1.1,
                    '2026-01-02', 'provider',
                    '2026-01-02', '2026-01-02'
                )
                """
            )


def test_migration_058_fx_rates_rejects_unknown_source_kind(tmp_path):
    db_path = tmp_path / "fx-rates-source-058.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO fx_rates (
                    provider, rate_date, base_currency, quote_currency,
                    rate, fetched_at, source_kind, created_at, updated_at
                ) VALUES (
                    'EODHD', '2026-01-01', 'USD', 'EUR', 1.1,
                    '2026-01-02', 'synthesized',
                    '2026-01-02', '2026-01-02'
                )
                """
            )


def test_migration_058_idempotent(tmp_path):
    db_path = tmp_path / "fx-rates-058-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 059: financial_facts.currency + unit format CHECKs (heavy
# rebuild on the largest table in the project; tests run on an empty
# fixture so the rebuild is fast).
# ---------------------------------------------------------------------------


def test_migration_059_financial_facts_rejects_lowercase_currency(tmp_path):
    db_path = tmp_path / "fin-facts-currency-059.sqlite"
    # Pin to v59: migration 059's financial_facts CHECKs operate on the legacy
    # ``unit`` column, which migration 071 later replaces with ``unit_kind``.
    apply_migrations(db_path, target_version=59)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO financial_facts (
                    listing_id, concept, fiscal_period, end_date, unit,
                    value, currency
                ) VALUES (?, 'Revenues', 'FY', '2025-12-31', 'USD',
                          1.0, 'usd')
                """,
                (listing_id,),
            )


def test_migration_059_financial_facts_rejects_empty_unit(tmp_path):
    db_path = tmp_path / "fin-facts-unit-059.sqlite"
    # Pin to v59: this asserts migration 059's ``unit`` format CHECK, which
    # migration 071 supersedes with the ``unit_kind`` enum CHECK.
    apply_migrations(db_path, target_version=59)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO financial_facts (
                    listing_id, concept, fiscal_period, end_date, unit, value
                ) VALUES (?, 'Revenues', 'FY', '2025-12-31', '', 1.0)
                """,
                (listing_id,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO financial_facts (
                    listing_id, concept, fiscal_period, end_date, unit, value
                ) VALUES (?, 'Revenues', 'FY', '2025-12-31', 'USD shares', 1.0)
                """,
                (listing_id,),
            )


def test_migration_059_financial_facts_accepts_composite_unit(tmp_path):
    """``USD/shares`` is a valid SEC fact unit and must pass the CHECK."""

    db_path = tmp_path / "fin-facts-composite-unit-059.sqlite"
    # Pin to v59: ``USD/shares`` is a valid value only for the legacy ``unit``
    # column (migration 059). Migration 071 replaces it with ``unit_kind``.
    apply_migrations(db_path, target_version=59)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        conn.execute(
            """
            INSERT INTO financial_facts (
                listing_id, concept, fiscal_period, end_date, unit, value
            ) VALUES (?, 'EarningsPerShareBasic', 'FY', '2025-12-31',
                      'USD/shares', 1.5)
            """,
            (listing_id,),
        )
        row = conn.execute(
            "SELECT unit, value FROM financial_facts WHERE listing_id = ?",
            (listing_id,),
        ).fetchone()
    assert row == ("USD/shares", 1.5)


def test_migration_059_idempotent(tmp_path):
    db_path = tmp_path / "fin-facts-059-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 060: UNIQUE (name, country) on issuer.
# ---------------------------------------------------------------------------


def test_migration_060_issuer_rejects_duplicate_name_country(tmp_path):
    db_path = tmp_path / "issuer-uniq-060.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        conn.execute(
            """
            INSERT INTO issuer (issuer_id, name, country)
            VALUES (1, 'Acme Corp', 'US')
            """
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO issuer (issuer_id, name, country)
                VALUES (2, 'Acme Corp', 'US')
                """
            )


def test_migration_060_issuer_allows_duplicate_with_null_country(tmp_path):
    """SQLite UNIQUE INDEX treats NULLs as distinct, so a name with a
    NULL country can coexist with the same name + non-NULL country."""

    db_path = tmp_path / "issuer-uniq-null-060.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        conn.execute(
            """
            INSERT INTO issuer (issuer_id, name, country)
            VALUES (1, 'Acme Corp', 'US')
            """
        )
        conn.execute(
            """
            INSERT INTO issuer (issuer_id, name, country)
            VALUES (2, 'Acme Corp', NULL)
            """
        )
        # And another NULL-country row with the same name still works
        # because UNIQUE indexes treat NULL as distinct.
        conn.execute(
            """
            INSERT INTO issuer (issuer_id, name, country)
            VALUES (3, 'Acme Corp', NULL)
            """
        )
        rows = conn.execute(
            "SELECT COUNT(*) FROM issuer WHERE name = 'Acme Corp'"
        ).fetchone()[0]
    assert rows == 3


def test_migration_060_dedups_existing_duplicates(tmp_path):
    """Pre-existing (name, country) duplicates collapse to one canonical
    issuer (lowest issuer_id) with the UNIQUE index in place."""

    db_path = tmp_path / "issuer-dup-pre-060.sqlite"

    apply_migrations(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP INDEX IF EXISTS idx_issuer_name_country")
        conn.execute("DELETE FROM schema_migrations")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (59,))
        conn.executemany(
            "INSERT INTO issuer (issuer_id, name, country) VALUES (?, ?, ?)",
            [(1, "Acme", "US"), (2, "Acme", "US"), (3, "Acme", "US")],
        )

    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT issuer_id, name, country FROM issuer WHERE name = 'Acme'"
        ).fetchall()
    assert rows == [(1, "Acme", "US")]


def test_migration_060_dedup_remaps_listings(tmp_path):
    """Dedup must reassign listing.issuer_id from non-canonical rows to
    the canonical row before deleting the losers."""

    db_path = tmp_path / "issuer-dedup-remap-060.sqlite"

    apply_migrations(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP INDEX IF EXISTS idx_issuer_name_country")
        conn.execute("DELETE FROM schema_migrations")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (59,))
        conn.execute("DELETE FROM listing")
        conn.execute("DELETE FROM issuer")
        conn.executemany(
            """
            INSERT INTO "exchange" (
                exchange_id, exchange_code, created_at, updated_at
            ) VALUES (?, ?, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
            """,
            [(501, "F"), (502, "XETRA")],
        )
        conn.executemany(
            "INSERT INTO issuer (issuer_id, name, country) VALUES (?, ?, ?)",
            [(10, "Petrobras", "Germany"), (11, "Petrobras", "Germany")],
        )
        conn.execute(
            "INSERT INTO listing (listing_id, issuer_id, exchange_id, "
            "symbol, currency) VALUES (?, ?, ?, ?, ?)",
            (100, 10, 501, "PBR", "EUR"),
        )
        conn.execute(
            "INSERT INTO listing (listing_id, issuer_id, exchange_id, "
            "symbol, currency) VALUES (?, ?, ?, ?, ?)",
            (101, 11, 502, "PBR", "EUR"),
        )

    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        issuer_rows = conn.execute(
            "SELECT issuer_id FROM issuer WHERE name = 'Petrobras' "
            "AND country = 'Germany'"
        ).fetchall()
        listing_rows = conn.execute(
            "SELECT listing_id, issuer_id FROM listing ORDER BY listing_id"
        ).fetchall()

    assert issuer_rows == [(10,)]
    assert listing_rows == [(100, 10), (101, 10)]


def test_migration_060_dedup_backfills_metadata(tmp_path):
    """Canonical row's nullable columns (description/sector/industry) get
    filled from the first non-NULL value found in non-canonical rows.
    Existing non-NULL values on the canonical row are never overwritten.
    """

    db_path = tmp_path / "issuer-dedup-meta-060.sqlite"

    apply_migrations(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP INDEX IF EXISTS idx_issuer_name_country")
        conn.execute("DELETE FROM schema_migrations")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (59,))
        conn.execute("DELETE FROM listing")
        conn.execute("DELETE FROM issuer")
        # Canonical row (lowest id) has sector but no industry/description.
        # Non-canonical row 21 provides industry; non-canonical row 22
        # provides description. Row 22 also has a *different* sector,
        # which must NOT clobber the canonical's existing sector.
        conn.executemany(
            "INSERT INTO issuer (issuer_id, name, description, sector, "
            "industry, country) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (20, "Acme", None, "Tech", None, "US"),
                (21, "Acme", None, None, "Software", "US"),
                (22, "Acme", "Maker of Acme products", "Other", None, "US"),
            ],
        )

    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT issuer_id, name, description, sector, industry, country "
            "FROM issuer WHERE name = 'Acme'"
        ).fetchone()
    assert row == (
        20,
        "Acme",
        "Maker of Acme products",
        "Tech",
        "Software",
        "US",
    )


def test_migration_060_idempotent(tmp_path):
    db_path = tmp_path / "issuer-uniq-060-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 061: row-level error invariant on market_data_fetch_state.
# ---------------------------------------------------------------------------


def test_migration_061_rejects_error_status_with_null_last_error(tmp_path):
    db_path = tmp_path / "fetch-state-error-061.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        _seed_listing(conn)
        conn.execute(
            """
            INSERT INTO provider_listing (
                provider_listing_id, provider_exchange_id, provider_symbol,
                listing_id
            ) VALUES (1, 1, 'AAA', 1)
            """
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO market_data_fetch_state (
                    provider_listing_id, last_status, last_error
                ) VALUES (1, 'error', NULL)
                """
            )


def test_migration_061_accepts_error_status_with_error_text(tmp_path):
    db_path = tmp_path / "fetch-state-error-ok-061.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        _seed_listing(conn)
        conn.execute(
            """
            INSERT INTO provider_listing (
                provider_listing_id, provider_exchange_id, provider_symbol,
                listing_id
            ) VALUES (1, 1, 'AAA', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO market_data_fetch_state (
                provider_listing_id, last_status, last_error
            ) VALUES (1, 'error', 'rate limit')
            """
        )
        row = conn.execute(
            "SELECT last_status, last_error FROM market_data_fetch_state"
        ).fetchone()
    assert row == ("error", "rate limit")


def test_migration_061_idempotent(tmp_path):
    db_path = tmp_path / "fetch-state-061-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 062: primary_provider_listing_catalog view.
# ---------------------------------------------------------------------------


def test_migration_062_creates_primary_view(tmp_path):
    db_path = tmp_path / "primary-view-062.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        view_def = conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'view' AND name = 'primary_provider_listing_catalog'"
        ).fetchone()
    assert view_def is not None
    assert "primary_listing_status != 'secondary'" in view_def[0]


def test_migration_062_view_excludes_secondary_listings(tmp_path):
    db_path = tmp_path / "primary-view-content-062.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO issuer (issuer_id, name) VALUES (1, 'A')")
        # Migration 069 made ``listing.currency`` NOT NULL, so every fixture
        # listing must carry a currency.
        conn.execute(
            """
            INSERT INTO listing (
                listing_id, issuer_id, exchange_id, symbol, currency,
                primary_listing_status
            ) VALUES
                (1, 1, 1, 'AAA', 'USD', 'primary'),
                (2, 1, 1, 'BBB', 'USD', 'secondary'),
                (3, 1, 1, 'CCC', 'USD', 'unknown')
            """
        )
        conn.execute(
            """
            INSERT INTO provider_listing (
                provider_listing_id, provider_exchange_id, provider_symbol,
                listing_id
            ) VALUES
                (10, 1, 'AAA', 1),
                (20, 1, 'BBB', 2),
                (30, 1, 'CCC', 3)
            """
        )
        primary_only = sorted(
            row[0]
            for row in conn.execute(
                "SELECT provider_listing_id FROM primary_provider_listing_catalog"
            ).fetchall()
        )
        full = sorted(
            row[0]
            for row in conn.execute(
                "SELECT provider_listing_id FROM provider_listing_catalog"
            ).fetchall()
        )

    # 'unknown' is treated as primary (only 'secondary' is excluded).
    assert primary_only == [10, 30]
    assert full == [10, 20, 30]


def test_migration_062_idempotent(tmp_path):
    db_path = tmp_path / "primary-view-062-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 063: schema_migrations PK + single-row guard.
# ---------------------------------------------------------------------------


def test_migration_063_schema_migrations_has_pk_and_check(tmp_path):
    db_path = tmp_path / "schema-mig-pk-063.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(schema_migrations)").fetchall()
        columns = {row[1]: row for row in info}
        sql = conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'table' AND name = 'schema_migrations'"
        ).fetchone()[0]
    assert "id" in columns
    assert columns["id"][5] == 1  # part of primary key
    assert "CHECK (id = 1)" in sql


def test_migration_063_rejects_second_row(tmp_path):
    db_path = tmp_path / "schema-mig-second-row-063.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO schema_migrations (id, version) VALUES (2, 999)")


def test_migration_063_rejects_id_other_than_one(tmp_path):
    db_path = tmp_path / "schema-mig-bad-id-063.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE schema_migrations SET id = 99")


def test_migration_063_preserves_version_through_rebuild(tmp_path):
    """An existing pre-063 DB at version N should land at version N
    (well, len(MIGRATIONS)) without losing the marker mid-migration."""

    db_path = tmp_path / "schema-mig-preserve-063.sqlite"

    # Hand-build a legacy schema_migrations table at version 62 to
    # simulate an existing DB caught mid-chain.
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")
    _ensure_migrations_table(conn)
    conn.commit()
    for i, migration in enumerate(MIGRATIONS[:62], start=1):
        conn.execute("BEGIN")
        migration(conn)
        conn.execute("DELETE FROM schema_migrations")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (i,))
        conn.commit()
    # Force the legacy schema (no id column) on the schema_migrations
    # table so 063 has work to do.
    conn.execute("BEGIN")
    conn.execute("DROP TABLE schema_migrations")
    conn.execute("CREATE TABLE schema_migrations (version INTEGER NOT NULL)")
    conn.execute("INSERT INTO schema_migrations (version) VALUES (62)")
    conn.commit()
    conn.close()

    apply_migrations(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT id, version FROM schema_migrations").fetchall()
    assert rows == [(1, len(MIGRATIONS))]


def test_migration_063_idempotent(tmp_path):
    db_path = tmp_path / "schema-mig-063-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 064: drop orphan NULL-name issuers and tighten issuer.name.
# ---------------------------------------------------------------------------


def test_migration_064_tightens_issuer_name_not_null(tmp_path):
    db_path = tmp_path / "issuer-name-064.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO issuer (issuer_id, name) VALUES (99, NULL)")


def test_migration_064_drops_orphan_null_name_issuers(tmp_path):
    """Pre-064 fixtures with NULL-name issuers + matching listings +
    stale market_data are scrubbed by the migration."""

    db_path = tmp_path / "issuer-orphan-064.sqlite"

    # Bring the DB up to migration 063 (one before 064), then seed an
    # orphan issuer/listing/market_data triple in the pre-064 shape.
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")
    _ensure_migrations_table(conn)
    conn.commit()
    for i, migration in enumerate(MIGRATIONS[:63], start=1):
        conn.execute("BEGIN")
        migration(conn)
        conn.execute("DELETE FROM schema_migrations")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (i,))
        conn.commit()
    # Seed: an orphan issuer (NULL name) with one listing and one
    # market_data row, plus a populated issuer to confirm it survives.
    # Both listings carry a currency so the only reason the orphan (99) is
    # removed is migration 064's NULL-name rule -- not migration 069's
    # currency-less purge, which would otherwise also delete listing 1.
    conn.execute(
        "INSERT INTO issuer (issuer_id, name, country) VALUES (1, 'Acme', 'US')"
    )
    conn.execute("INSERT INTO issuer (issuer_id, name) VALUES (99, NULL)")
    conn.execute(
        """
        INSERT INTO listing (listing_id, issuer_id, exchange_id, symbol, currency)
        VALUES (1, 1, 1, 'ACME', 'USD'), (99, 99, 1, 'ORPHAN', 'USD')
        """
    )
    conn.execute(
        """
        INSERT INTO market_data (
            listing_id, as_of, price, source_provider, updated_at
        ) VALUES
            (1, '2026-01-01', 100.0, 'EODHD', '2026-01-02T00:00:00+00:00'),
            (99, '2026-01-01', 50.0, 'EODHD', '2026-01-02T00:00:00+00:00')
        """
    )
    conn.commit()
    conn.close()

    apply_migrations(db_path)
    with sqlite3.connect(db_path) as conn:
        issuer_rows = sorted(
            (row[0], row[1])
            for row in conn.execute("SELECT issuer_id, name FROM issuer").fetchall()
        )
        listing_rows = sorted(
            row[0] for row in conn.execute("SELECT listing_id FROM listing").fetchall()
        )
        market_rows = sorted(
            row[0]
            for row in conn.execute("SELECT listing_id FROM market_data").fetchall()
        )
    assert (99, None) not in issuer_rows
    assert 99 not in listing_rows
    assert 99 not in market_rows
    # The populated issuer/listing/market_data survives.
    assert (1, "Acme") in issuer_rows
    assert 1 in listing_rows
    assert 1 in market_rows


def test_migration_064_aborts_when_orphan_has_facts(tmp_path):
    """If an orphan listing surprisingly carries financial_facts, the
    migration should abort so the operator can investigate."""

    db_path = tmp_path / "issuer-orphan-with-facts-064.sqlite"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")
    _ensure_migrations_table(conn)
    conn.commit()
    for i, migration in enumerate(MIGRATIONS[:63], start=1):
        conn.execute("BEGIN")
        migration(conn)
        conn.execute("DELETE FROM schema_migrations")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (i,))
        conn.commit()
    conn.execute("INSERT INTO issuer (issuer_id, name) VALUES (99, NULL)")
    conn.execute(
        """
        INSERT INTO listing (listing_id, issuer_id, exchange_id, symbol)
        VALUES (99, 99, 1, 'ORPHAN')
        """
    )
    conn.execute(
        """
        INSERT INTO financial_facts (
            listing_id, concept, fiscal_period, end_date, unit, value
        ) VALUES (99, 'Revenue', 'FY', '2025-12-31', 'USD', 1.0)
        """
    )
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="migration 064 aborted"):
        apply_migrations(db_path)


def test_migration_064_idempotent(tmp_path):
    db_path = tmp_path / "issuer-064-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 065: financial_facts.fiscal_period NOT NULL.
# ---------------------------------------------------------------------------


def test_migration_065_rejects_null_fiscal_period(tmp_path):
    db_path = tmp_path / "fin-facts-fiscal-065.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        listing_id = _seed_listing(conn)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO financial_facts (
                    listing_id, concept, fiscal_period, end_date, unit_kind, value,
                    currency
                ) VALUES (?, 'Revenue', NULL, '2025-12-31', 'monetary', 1.0, 'USD')
                """,
                (listing_id,),
            )


def test_migration_065_aborts_on_pre_existing_null_fiscal_period(tmp_path):
    db_path = tmp_path / "fin-facts-null-fp-065.sqlite"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")
    _ensure_migrations_table(conn)
    conn.commit()
    for i, migration in enumerate(MIGRATIONS[:64], start=1):
        conn.execute("BEGIN")
        migration(conn)
        conn.execute("DELETE FROM schema_migrations")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (i,))
        conn.commit()
    conn.execute("INSERT INTO issuer (issuer_id, name) VALUES (1, 'X')")
    conn.execute(
        """
        INSERT INTO listing (listing_id, issuer_id, exchange_id, symbol)
        VALUES (1, 1, 1, 'X')
        """
    )
    conn.execute(
        """
        INSERT INTO financial_facts (
            listing_id, concept, fiscal_period, end_date, unit, value
        ) VALUES (1, 'Revenue', NULL, '2025-12-31', 'USD', 1.0)
        """
    )
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="migration 065 aborted"):
        apply_migrations(db_path)


def test_migration_065_idempotent(tmp_path):
    db_path = tmp_path / "fin-facts-065-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 066: provider_exchange name + country NOT NULL.
# ---------------------------------------------------------------------------


def test_migration_066_rejects_null_name(tmp_path):
    db_path = tmp_path / "provider-exchange-null-name-066.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO provider_exchange (
                    provider_id, provider_exchange_code, exchange_id,
                    name, country, updated_at
                ) VALUES (1, 'XYZ', 1, NULL, 'US',
                          '2026-01-01T00:00:00+00:00')
                """
            )


def test_migration_066_rejects_null_country(tmp_path):
    db_path = tmp_path / "provider-exchange-null-country-066.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO provider_exchange (
                    provider_id, provider_exchange_code, exchange_id,
                    name, country, updated_at
                ) VALUES (1, 'XYZ', 1, 'Bourse', NULL,
                          '2026-01-01T00:00:00+00:00')
                """
            )


def test_migration_066_backfills_legacy_nulls(tmp_path):
    """A pre-066 DB with NULL name/country rows must come out clean
    after the migration backfills sensible placeholders."""

    db_path = tmp_path / "provider-exchange-backfill-066.sqlite"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")
    _ensure_migrations_table(conn)
    conn.commit()
    for i, migration in enumerate(MIGRATIONS[:65], start=1):
        conn.execute("BEGIN")
        migration(conn)
        conn.execute("DELETE FROM schema_migrations")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (i,))
        conn.commit()
    # Seed two rows with NULL name and NULL country.
    conn.execute(
        """
        INSERT INTO provider_exchange (
            provider_id, provider_exchange_code, exchange_id,
            name, country, updated_at
        ) VALUES
            (1, 'LEGACY1', 1, NULL, NULL, '2026-01-01T00:00:00+00:00'),
            (1, 'LEGACY2', 1, '', '', '2026-01-01T00:00:00+00:00')
        """
    )
    conn.commit()
    conn.close()

    apply_migrations(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = sorted(
            (row[0], row[1])
            for row in conn.execute(
                "SELECT name, country FROM provider_exchange "
                "WHERE provider_exchange_code IN ('LEGACY1', 'LEGACY2')"
            ).fetchall()
        )
    assert rows == [
        ("LEGACY1", "Unknown"),
        ("LEGACY2", "Unknown"),
    ]


def test_migration_066_idempotent(tmp_path):
    db_path = tmp_path / "provider-exchange-066-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# Indexes that migration 067 must drop. Audit found they were either
# never picked by the optimizer or strictly covered by a PK / UNIQUE
# auto-index. See the migration docstring for per-index rationale.
_MIGRATION_067_DROPPED_INDEXES = frozenset(
    {
        "idx_fin_facts_concept",
        "idx_metric_compute_status_metric_status",
        "idx_metrics_metric_id",
        "idx_market_data_latest",
        "idx_fundamentals_raw_last_fetched",
        "idx_market_data_fetch_next",
        "idx_listing_exchange",
        "idx_fundamentals_fetch_next",
    }
)

# Indexes that must survive the full migration chain — every other
# secondary index the schema declares at head. Asserting on this set
# catches the copy-paste mistake of accidentally dropping the wrong index.
_MIGRATION_067_RETAINED_INDEXES = frozenset(
    {
        "idx_fin_facts_security_concept_latest",
        "idx_fin_facts_currency_nonnull",
        "idx_fx_rates_pair_date",
        "idx_fx_supported_pairs_refreshable",
        "idx_issuer_name_country",
        "idx_listing_currency_nonnull",
        "idx_provider_exchange_exchange",
        "idx_provider_listing_listing",
    }
)


def test_migration_067_drops_unused_indexes(tmp_path):
    db_path = tmp_path / "drop-unused-indexes-067.sqlite"
    applied = apply_migrations(db_path)

    assert applied == len(MIGRATIONS)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_%'"
        ).fetchall()
    index_names = {row[0] for row in rows}

    # The eight doomed indexes must be gone after a fresh apply.
    assert _MIGRATION_067_DROPPED_INDEXES.isdisjoint(index_names), (
        "migration 067 left a doomed index in place: "
        f"{sorted(_MIGRATION_067_DROPPED_INDEXES & index_names)}"
    )

    # And the eight indexes that the audit kept must still exist —
    # protects against a future change that drops the wrong one.
    missing_retained = _MIGRATION_067_RETAINED_INDEXES - index_names
    assert not missing_retained, (
        f"migration 067 (or a later change) dropped indexes that the "
        f"audit retained: {sorted(missing_retained)}"
    )


def test_migration_067_idempotent(tmp_path):
    db_path = tmp_path / "drop-unused-indexes-067-idempotent.sqlite"
    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0


# ---------------------------------------------------------------------------
# Migration 068 — fiscal_period CHECK + empty-string backfill
# ---------------------------------------------------------------------------

# The CHECK introduced by migration 068 is the schema-level guarantee that no
# fact ever leaks into the table with an empty ``fiscal_period``. The two
# regression tests below pin that guarantee plus the backfill behaviour. They
# focus on migration 068 specifically rather than the full migration chain.

_VALID_FISCAL_PERIODS = ("FY", "Q1", "Q2", "Q3", "Q4", "TTM", "INSTANT")


def _drop_fiscal_period_check(conn: sqlite3.Connection) -> None:
    """Recreate ``financial_facts`` without the CHECK so we can seed
    legacy empty-period rows for the backfill test.

    SQLite can't ALTER a CHECK away in place, so we mirror the live DDL
    minus the new constraint. Indexes are recreated to match the current
    schema (post migration 067 + 068).
    """

    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_security_concept_latest")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_currency_nonnull")
    conn.execute("DROP TABLE financial_facts")
    conn.execute(
        """
        CREATE TABLE financial_facts (
            listing_id INTEGER NOT NULL,
            cik TEXT,
            concept TEXT NOT NULL,
            fiscal_period TEXT NOT NULL,
            end_date TEXT NOT NULL,
            unit TEXT NOT NULL
                CHECK (length(trim(unit)) > 0
                       AND instr(unit, ' ') = 0
                       AND instr(unit, char(9)) = 0
                       AND instr(unit, char(10)) = 0),
            value REAL NOT NULL,
            accn TEXT,
            filed TEXT,
            frame TEXT,
            start_date TEXT,
            accounting_standard TEXT,
            currency TEXT,
            source_provider TEXT,
            PRIMARY KEY (listing_id, concept, fiscal_period, end_date, unit),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fin_facts_security_concept_latest
        ON financial_facts(listing_id, concept, end_date DESC, filed DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fin_facts_currency_nonnull
        ON financial_facts(currency)
        WHERE currency IS NOT NULL
        """
    )


def test_migration_068_check_rejects_empty_fiscal_period(tmp_path):
    """Post-migration the CHECK must reject any fiscal_period='' insert."""

    db_path = tmp_path / "fiscal-period-check.sqlite"
    applied = apply_migrations(db_path)
    assert applied == len(MIGRATIONS)

    with sqlite3.connect(db_path) as conn:
        # FK is off by default in tests, so we can target the CHECK without
        # also tripping over listing_id=1 not existing in `listing`.
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO financial_facts (
                    listing_id, concept, fiscal_period, end_date, unit_kind, value,
                    currency
                ) VALUES (1, 'EnterpriseValue', '', '2025-12-31', 'monetary', 1.0, 'USD')
                """
            )
        # And it must accept every value in the allow-list.
        for period in _VALID_FISCAL_PERIODS:
            conn.execute(
                """
                INSERT INTO financial_facts (
                    listing_id, concept, fiscal_period, end_date, unit_kind, value,
                    currency
                ) VALUES (1, 'Revenues', ?, '2025-12-31', 'monetary', 1.0, 'USD')
                """,
                (period,),
            )


def _seed_catalog_row(
    conn: sqlite3.Connection,
    *,
    listing_id: int,
    name: str,
    provider_symbol: str,
) -> int:
    """Insert a minimal issuer/listing/provider_listing chain and return its
    ``provider_listing_id``. ``apply_migrations`` pre-seeds the US exchange
    and one provider_exchange row (SEC-US); we just point at that row since
    the backfill SQL doesn't care which provider supplied the cached payload.
    """

    exchange_id = conn.execute(
        "SELECT exchange_id FROM exchange WHERE exchange_code = 'US'"
    ).fetchone()[0]
    provider_exchange_id = conn.execute(
        "SELECT provider_exchange_id FROM provider_exchange LIMIT 1"
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO issuer (issuer_id, name) VALUES (?, ?)",
        (listing_id, name),
    )
    # Migration 069 makes ``listing.currency`` NOT NULL; this helper runs
    # after ``apply_migrations`` has reached head, so the listing must carry
    # a currency (the catalog row points at a US listing -> USD).
    conn.execute(
        """
        INSERT INTO listing (listing_id, issuer_id, exchange_id, symbol, currency)
        VALUES (?, ?, ?, ?, 'USD')
        """,
        (listing_id, listing_id, exchange_id, provider_symbol),
    )
    conn.execute(
        """
        INSERT INTO provider_listing (
            provider_exchange_id, provider_symbol, listing_id
        ) VALUES (?, ?, ?)
        """,
        (provider_exchange_id, provider_symbol, listing_id),
    )
    return int(
        conn.execute(
            "SELECT provider_listing_id FROM provider_listing WHERE listing_id = ?",
            (listing_id,),
        ).fetchone()[0]
    )


def test_migration_068_backfills_from_updated_at(tmp_path):
    """Migration 068 maps empty-period rows to INSTANT/TTM and re-dates them
    from ``General.UpdatedAt`` in the cached fundamentals payload."""

    from pyvalue.migrations import _migration_068_fiscal_period_check

    db_path = tmp_path / "fiscal-period-backfill.sqlite"
    applied = apply_migrations(db_path)
    assert applied == len(MIGRATIONS)

    with sqlite3.connect(db_path) as conn:
        # Recreate the table without the CHECK so we can seed legacy rows
        # that the production code is no longer able to insert.
        _drop_fiscal_period_check(conn)

        provider_listing_id = _seed_catalog_row(
            conn, listing_id=42, name="Acme", provider_symbol="ACME"
        )
        conn.execute(
            """
            INSERT INTO fundamentals_raw (
                provider_listing_id, data, payload_hash, last_fetched_at
            ) VALUES (
                ?, '{"General":{"UpdatedAt":"2026-03-27"}}', ?,
                '2026-03-28T08:42:36+00:00'
            )
            """,
            (provider_listing_id, "0" * 64),
        )

        # Two legacy empty-period snapshots — one EV, one DPS — plus a
        # well-formed FY row that must survive the migration unchanged.
        conn.execute(
            """
            INSERT INTO financial_facts (
                listing_id, concept, fiscal_period, end_date, unit, value,
                source_provider, currency
            ) VALUES
                (42, 'EnterpriseValue', '', '2025-12-31', 'USD', 3700.0,
                 'EODHD', 'USD'),
                (42, 'CommonStockDividendsPerShareCashPaid', '',
                 '2025-12-31', 'USD', 1.03, 'EODHD', 'USD'),
                (42, 'Revenues', 'FY', '2024-12-31', 'USD', 100.0,
                 'EODHD', 'USD')
            """
        )

        _migration_068_fiscal_period_check(conn)

        rows = conn.execute(
            """
            SELECT concept, fiscal_period, end_date, value
            FROM financial_facts
            ORDER BY concept
            """
        ).fetchall()
        assert rows == [
            (
                "CommonStockDividendsPerShareCashPaid",
                "TTM",
                "2026-03-27",
                1.03,
            ),
            ("EnterpriseValue", "INSTANT", "2026-03-27", 3700.0),
            ("Revenues", "FY", "2024-12-31", 100.0),
        ]

        # The CHECK is now in place and must reject another empty insert.
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO financial_facts (
                    listing_id, concept, fiscal_period, end_date, unit, value
                ) VALUES (42, 'EnterpriseValue', '', '2026-04-01', 'USD', 1.0)
                """
            )


def test_migration_068_falls_back_to_last_fetched_at_when_updated_at_missing(
    tmp_path,
):
    """If General.UpdatedAt is absent we fall back to DATE(last_fetched_at)."""

    from pyvalue.migrations import _migration_068_fiscal_period_check

    db_path = tmp_path / "fiscal-period-backfill-fallback.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        _drop_fiscal_period_check(conn)

        provider_listing_id = _seed_catalog_row(
            conn, listing_id=7, name="Beta", provider_symbol="BETA"
        )
        # Payload deliberately lacks General.UpdatedAt — should trigger the
        # DATE(last_fetched_at) fallback.
        conn.execute(
            """
            INSERT INTO fundamentals_raw (
                provider_listing_id, data, payload_hash, last_fetched_at
            ) VALUES (
                ?, '{"General":{}}', ?, '2026-02-14T11:22:33+00:00'
            )
            """,
            (provider_listing_id, "1" * 64),
        )
        conn.execute(
            """
            INSERT INTO financial_facts (
                listing_id, concept, fiscal_period, end_date, unit, value,
                source_provider, currency
            ) VALUES (7, 'EnterpriseValue', '', '2025-09-30', 'USD', 50.0,
                      'EODHD', 'USD')
            """
        )

        _migration_068_fiscal_period_check(conn)

        end_date, fiscal_period = conn.execute(
            """
            SELECT end_date, fiscal_period FROM financial_facts
            WHERE listing_id = 7 AND concept = 'EnterpriseValue'
            """
        ).fetchone()
        assert fiscal_period == "INSTANT"
        assert end_date == "2026-02-14"


def test_migration_070_divides_subunit_prices_to_major(tmp_path):
    """Migration 070 converts ``market_data.price`` to the major currency.

    Prices for listings quoted in a subunit (GBX/GBP0.01/ZAC/ILA) are divided
    by 100; prices for major-currency listings are left untouched. Uses a
    minimal two-table fixture because the migration only reads
    ``listing.currency`` and rewrites ``market_data.price``.
    """

    from pyvalue.migrations import _migration_070_market_data_price_major_currency

    db_path = tmp_path / "subunit-price.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE listing (listing_id INTEGER PRIMARY KEY, currency TEXT)"
        )
        conn.execute(
            """
            CREATE TABLE market_data (
                listing_id INTEGER NOT NULL,
                as_of TEXT NOT NULL,
                price REAL NOT NULL,
                PRIMARY KEY (listing_id, as_of)
            )
            """
        )
        conn.executemany(
            "INSERT INTO listing (listing_id, currency) VALUES (?, ?)",
            [(1, "GBX"), (2, "ZAC"), (3, "ILA"), (4, "USD"), (5, "GBP")],
        )
        conn.executemany(
            "INSERT INTO market_data (listing_id, as_of, price) VALUES (?, ?, ?)",
            [
                (1, "2024-01-02", 2783.5),  # GBX pence -> 27.835 GBP
                (2, "2024-01-02", 23750.0),  # ZAC cents -> 237.5 ZAR
                (3, "2024-01-02", 1234.0),  # ILA agorot -> 12.34 ILS
                (4, "2024-01-02", 150.0),  # USD major, unchanged
                (5, "2024-01-02", 22.04),  # GBP major, unchanged
            ],
        )

        _migration_070_market_data_price_major_currency(conn)

        prices = dict(
            conn.execute("SELECT listing_id, price FROM market_data").fetchall()
        )

    assert prices[1] == pytest.approx(27.835)
    assert prices[2] == pytest.approx(237.5)
    assert prices[3] == pytest.approx(12.34)
    assert prices[4] == 150.0
    assert prices[5] == 22.04


def test_migration_069_purges_currencyless_listings_and_dependents(tmp_path):
    """Migration 069 deletes NULL-currency listings + every dependent row, then
    makes ``listing.currency`` NOT NULL.

    A currency-less listing and the rows hanging off it -- ``provider_listing``
    and its ``fundamentals_raw`` child (keyed by provider_listing_id), plus
    ``financial_facts`` / ``market_data`` (keyed by listing_id) -- are removed;
    a currency-bearing listing and its rows survive. The rebuilt table enforces
    ``currency TEXT NOT NULL``.
    """

    from pyvalue.migrations import _migration_069_purge_currencyless_listings

    db_path = tmp_path / "purge-currencyless.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE issuer (issuer_id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute(
            "CREATE TABLE exchange (exchange_id INTEGER PRIMARY KEY, exchange_code TEXT)"
        )
        conn.execute(
            """
            CREATE TABLE listing (
                listing_id INTEGER PRIMARY KEY,
                issuer_id INTEGER NOT NULL,
                exchange_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                currency TEXT,
                primary_listing_status TEXT NOT NULL DEFAULT 'unknown'
            )
            """
        )
        conn.execute(
            "CREATE TABLE provider_listing "
            "(provider_listing_id INTEGER PRIMARY KEY, listing_id INTEGER NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE fundamentals_raw "
            "(provider_listing_id INTEGER NOT NULL, data TEXT)"
        )
        conn.execute(
            "CREATE TABLE financial_facts (listing_id INTEGER NOT NULL, concept TEXT)"
        )
        conn.execute(
            "CREATE TABLE market_data "
            "(listing_id INTEGER NOT NULL, as_of TEXT, price REAL)"
        )
        conn.execute("INSERT INTO issuer (issuer_id, name) VALUES (1, 'I1'), (2, 'I2')")
        conn.execute(
            "INSERT INTO exchange (exchange_id, exchange_code) VALUES (1, 'US')"
        )
        conn.execute(
            """
            INSERT INTO listing (listing_id, issuer_id, exchange_id, symbol, currency)
            VALUES (1, 1, 1, 'AAA', NULL), (2, 2, 1, 'BBB', 'USD')
            """
        )
        conn.execute(
            "INSERT INTO provider_listing (provider_listing_id, listing_id) "
            "VALUES (10, 1), (20, 2)"
        )
        conn.execute(
            "INSERT INTO fundamentals_raw (provider_listing_id, data) "
            "VALUES (10, '{}'), (20, '{}')"
        )
        conn.execute(
            "INSERT INTO financial_facts (listing_id, concept) "
            "VALUES (1, 'Revenues'), (2, 'Revenues')"
        )
        conn.execute(
            "INSERT INTO market_data (listing_id, as_of, price) "
            "VALUES (1, '2024-01-02', 5.0), (2, '2024-01-02', 6.0)"
        )

        _migration_069_purge_currencyless_listings(conn)

        listings = conn.execute(
            "SELECT listing_id, currency FROM listing ORDER BY listing_id"
        ).fetchall()
        provider_listings = [
            r[0]
            for r in conn.execute("SELECT provider_listing_id FROM provider_listing")
        ]
        raw = [
            r[0]
            for r in conn.execute("SELECT provider_listing_id FROM fundamentals_raw")
        ]
        fact_listings = [
            r[0] for r in conn.execute("SELECT listing_id FROM financial_facts")
        ]
        md_listings = [r[0] for r in conn.execute("SELECT listing_id FROM market_data")]
        ddl = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='listing'"
        ).fetchone()[0]

    # Only the currency-bearing listing (and its dependents) survive.
    assert listings == [(2, "USD")]
    assert provider_listings == [20]
    assert raw == [20]
    assert fact_listings == [2]
    assert md_listings == [2]
    # The rebuilt table enforces a non-null currency.
    assert "currency TEXT NOT NULL" in ddl


# ---------------------------------------------------------------------------
# Migration 071: financial_facts.unit -> unit_kind enum.
# ---------------------------------------------------------------------------


def test_migration_071_unit_kind_replaces_unit_with_coupled_checks(tmp_path):
    """Migration 071 swaps ``financial_facts.unit`` for the ``unit_kind`` enum.

    The rebuilt table holds ``unit_kind`` (not ``unit``), drops it from the PK,
    couples monetary/per_share rows to a non-null *major* currency, and forces
    every other kind to a NULL currency.
    """

    db_path = tmp_path / "fin-facts-071-shape.sqlite"
    apply_migrations(db_path)

    with _open_with_fk(db_path) as conn:
        info = conn.execute("PRAGMA table_info(financial_facts)").fetchall()
        columns = {row[1] for row in info}
        pk_columns = [row[1] for row in info if row[5]]

        assert "unit_kind" in columns
        assert "unit" not in columns
        assert pk_columns == ["listing_id", "concept", "fiscal_period", "end_date"]

        listing_id = _seed_listing(conn)

        # Accepted: monetary + major currency; count + NULL; per_share + currency.
        for concept, kind, value, currency in (
            ("Assets", "monetary", 10.0, "USD"),
            ("CommonStockSharesOutstanding", "count", 5.0, None),
            ("EarningsPerShareDiluted", "per_share", 1.5, "EUR"),
        ):
            conn.execute(
                """
                INSERT INTO financial_facts (
                    listing_id, concept, fiscal_period, end_date, unit_kind, value,
                    currency
                ) VALUES (?, ?, 'FY', '2024-12-31', ?, ?, ?)
                """,
                (listing_id, concept, kind, value, currency),
            )
        conn.commit()

        # Rejected combinations — each violates exactly one CHECK clause.
        rejected = [
            # monetary without a currency (coupling).
            ("Liabilities", "monetary", 3.0, None),
            # count carrying a currency (coupling).
            ("ShareCountB", "count", 7.0, "USD"),
            # subunit currency on a monetary fact (major-only).
            ("Sales", "monetary", 9.0, "GBX"),
            # unit_kind outside the enum.
            ("Mystery", "bogus", 1.0, None),
        ]
        for concept, kind, value, currency in rejected:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO financial_facts (
                        listing_id, concept, fiscal_period, end_date, unit_kind,
                        value, currency
                    ) VALUES (?, ?, 'FY', '2024-12-31', ?, ?, ?)
                    """,
                    (listing_id, concept, kind, value, currency),
                )


def test_migration_071_rebuilds_empty_and_clears_normalization_state(tmp_path):
    """Migration 071 drops legacy ``financial_facts`` rows and clears the
    normalization state so ``normalise`` rebuilds every fact from raw."""

    db_path = tmp_path / "fin-facts-071-rebuild.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        provider_listing_id = _seed_catalog_row(
            conn, listing_id=7, name="Acme", provider_symbol="ACME"
        )
        # Recreate the pre-071 ``unit``-column shape and seed a fact, a cached
        # payload, and a normalization-state row, then rewind so 071 replays.
        _drop_fiscal_period_check(conn)
        conn.execute(
            """
            INSERT INTO financial_facts (
                listing_id, concept, fiscal_period, end_date, unit, value, currency
            ) VALUES (7, 'Assets', 'FY', '2024-12-31', 'USD', 10.0, 'USD')
            """
        )
        conn.execute(
            """
            INSERT INTO fundamentals_raw (
                provider_listing_id, data, payload_hash, last_fetched_at
            ) VALUES (?, '{}', ?, '2026-01-01T00:00:00+00:00')
            """,
            (provider_listing_id, "0" * 64),
        )
        conn.execute(
            """
            INSERT INTO fundamentals_normalization_state (
                provider_listing_id, normalized_payload_hash, normalized_at
            ) VALUES (?, ?, '2026-01-02T00:00:00+00:00')
            """,
            (provider_listing_id, "0" * 64),
        )
        conn.execute("UPDATE schema_migrations SET version = 70")
        conn.commit()

    applied = apply_migrations(db_path, target_version=71)
    assert applied == 1

    with _open_with_fk(db_path) as conn:
        fact_count = conn.execute("SELECT COUNT(*) FROM financial_facts").fetchone()[0]
        state_count = conn.execute(
            "SELECT COUNT(*) FROM fundamentals_normalization_state"
        ).fetchone()[0]
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(financial_facts)").fetchall()
        }

    # Legacy rows are dropped (rebuilt empty) and the normalization state is
    # cleared so every cached payload is re-normalized.
    assert fact_count == 0
    assert state_count == 0
    assert "unit_kind" in columns
