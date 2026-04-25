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
        conn.execute(
            """
            CREATE TABLE market_data (
                security_id INTEGER NOT NULL,
                as_of TEXT NOT NULL,
                price REAL NOT NULL,
                source_provider TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (security_id, as_of)
            )
            """
        )
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
        conn.execute(
            """
            INSERT INTO market_data (
                security_id, as_of, price, source_provider, updated_at
            ) VALUES (1, '2026-01-02', 10.0, 'EODHD', '2026-01-02T00:00:00+00:00')
            """
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
        market_data_count = conn.execute("SELECT COUNT(*) FROM market_data").fetchone()[
            0
        ]
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
        market_data_join_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM market_data md
            JOIN provider p ON p.provider_code = md.source_provider
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
    assert market_data_count == 1
    assert fx_rates_count == 2
    assert supported_ticker_join_count == 0
    assert fundamentals_raw_join_count == 0
    assert market_data_join_count == 1
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
        "currency",
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
    prior_version = len(MIGRATIONS) - 1
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

    assert applied == 1
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
        "rate_text",
        "fetched_at",
        "source_kind",
        "meta_json",
        "created_at",
        "updated_at",
    }
    assert "idx_fx_rates_pair_date" in fx_index_names
    assert "idx_fx_supported_pairs_refreshable" in fx_supported_pair_index_names
    if "provider_listing" in tables:
        assert "idx_provider_listing_currency_nonnull" in provider_listing_index_names
    if "financial_facts" in tables:
        assert "idx_fin_facts_currency_nonnull" in financial_fact_index_names
    if "market_data" in tables:
        assert "idx_market_data_currency_nonnull" in market_data_index_names
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
        "payload_id",
        "provider_listing_id",
        "data",
        "fetched_at",
    }
    assert "idx_fundamentals_raw_provider_fetched" in raw_index_names
    assert "idx_fundamentals_raw_security" not in raw_index_names
    assert raw_fk_targets == {"provider_listing"}


def test_migration_drops_fundamentals_raw_listing_identity_columns(tmp_path):
    db_path = tmp_path / "fundamentals-raw-drop-listing.sqlite"
    previous_version = len(MIGRATIONS) - 2
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

    assert applied == 2
    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(fundamentals_raw)").fetchall()
        }
        indexes = conn.execute("PRAGMA index_list(fundamentals_raw)").fetchall()
        index_names = {row[1] for row in indexes}
        row = conn.execute(
            """
            SELECT payload_id, provider_listing_id, data, fetched_at
            FROM fundamentals_raw
            """
        ).fetchone()

    assert columns == {
        "payload_id",
        "provider_listing_id",
        "data",
        "fetched_at",
    }
    assert "idx_fundamentals_raw_provider_fetched" in index_names
    assert "idx_fundamentals_raw_security" not in index_names
    assert "idx_fundamentals_raw_provider_symbol" not in index_names
    assert row == (10, 1, "{}", "2026-01-01T00:00:00+00:00")


def test_migration_drops_fundamentals_raw_currency_from_current_schema(tmp_path):
    db_path = tmp_path / "fundamentals-raw-drop-currency.sqlite"
    previous_version = len(MIGRATIONS) - 2
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

    assert applied == 2
    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(fundamentals_raw)").fetchall()
        }
        indexes = conn.execute("PRAGMA index_list(fundamentals_raw)").fetchall()
        index_names = {row[1] for row in indexes}
        row = conn.execute(
            """
            SELECT payload_id, provider_listing_id, data, fetched_at
            FROM fundamentals_raw
            """
        ).fetchone()

    assert columns == {"payload_id", "provider_listing_id", "data", "fetched_at"}
    assert "idx_fundamentals_raw_provider_fetched" in index_names
    assert row == (10, 1, "{}", "2026-01-01T00:00:00+00:00")


def test_migration_adds_metric_status_and_facts_refresh_tables(tmp_path):
    db_path = tmp_path / "metric-status-migration.sqlite"
    with sqlite3.connect(db_path) as conn:
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
    assert "idx_provider_listing_currency_nonnull" in provider_listing_index_names
