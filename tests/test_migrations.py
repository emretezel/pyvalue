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
            SELECT provider, provider_exchange_code, provider_symbol, provider_ticker,
                   listing_exchange, security_type
            FROM supported_tickers
            """
        ).fetchall()
        version = conn.execute("SELECT version FROM schema_migrations").fetchone()[0]

        assert listings_exists is None
        assert rows == [("SEC", "US", "ABC.US", "ABC", "NYSE", "Common Stock")]
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
        exchange_provider_info = conn.execute(
            "PRAGMA table_info(exchange_provider)"
        ).fetchall()
        exchange_provider_columns = {row[1] for row in exchange_provider_info}
        exchange_provider_pk_cols = [row[1] for row in exchange_provider_info if row[5]]
        exchange_provider_indexes = conn.execute(
            "PRAGMA index_list(exchange_provider)"
        ).fetchall()
        exchange_provider_index_names = {row[1] for row in exchange_provider_indexes}
        exchange_provider_fks = conn.execute(
            "PRAGMA foreign_key_list(exchange_provider)"
        ).fetchall()
        fk_targets = {row[2] for row in exchange_provider_fks}

    assert supported_exchanges_exists is None
    assert exchange_columns == {
        "exchange_id",
        "exchange_code",
        "created_at",
        "updated_at",
    }
    assert exchange_pk_cols == ["exchange_id"]
    assert exchange_provider_columns == {
        "provider",
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
    assert exchange_provider_pk_cols == ["provider", "provider_exchange_code"]
    assert "idx_exchange_provider_exchange" in exchange_provider_index_names
    assert fk_targets == {"exchange", "providers"}


def test_migration_splits_supported_exchanges_into_exchange_provider(tmp_path):
    db_path = tmp_path / "exchange-provider-backfill.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE providers (
                provider_code TEXT NOT NULL PRIMARY KEY,
                display_name TEXT NOT NULL,
                description TEXT,
                status TEXT NOT NULL DEFAULT 'active',
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
                status,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "EODHD",
                    "EOD Historical Data",
                    None,
                    "active",
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
                (
                    "SEC",
                    "US SEC Company Facts",
                    None,
                    "active",
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

    assert applied == 1
    with sqlite3.connect(db_path) as conn:
        supported_exchanges_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='supported_exchanges'"
        ).fetchone()
        exchange_rows = conn.execute(
            'SELECT exchange_code FROM "exchange" ORDER BY exchange_code'
        ).fetchall()
        exchange_provider_rows = conn.execute(
            """
            SELECT
                ep.provider,
                ep.provider_exchange_code,
                e.exchange_code,
                ep.name,
                ep.country,
                ep.currency
            FROM exchange_provider ep
            JOIN "exchange" e ON e.exchange_id = ep.exchange_id
            ORDER BY ep.provider, ep.provider_exchange_code
            """
        ).fetchall()

    assert supported_exchanges_exists is None
    assert exchange_rows == [("LSE",), ("US",)]
    assert exchange_provider_rows == [
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
                INSERT INTO exchange_provider (
                    provider,
                    provider_exchange_code,
                    exchange_id,
                    updated_at
                ) VALUES (?, ?, ?, ?)
                """,
                ("UNKNOWN", "US", 1, "2026-01-01T00:00:00+00:00"),
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO exchange_provider (
                    provider,
                    provider_exchange_code,
                    exchange_id,
                    updated_at
                ) VALUES (?, ?, ?, ?)
                """,
                ("EODHD", "US", 999999, "2026-01-01T00:00:00+00:00"),
            )


def test_migration_creates_and_seeds_providers_table(tmp_path):
    db_path = tmp_path / "providers.sqlite"

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(providers)").fetchall()
        columns = {row[1] for row in info}
        pk_cols = [row[1] for row in info if row[5]]
        rows = conn.execute(
            """
            SELECT provider_code, display_name, status
            FROM providers
            ORDER BY provider_code
            """
        ).fetchall()

    assert columns == {
        "provider_code",
        "display_name",
        "description",
        "status",
        "created_at",
        "updated_at",
    }
    assert pk_cols == ["provider_code"]
    assert rows == [
        ("EODHD", "EOD Historical Data", "active"),
        ("FRANKFURTER", "Frankfurter FX", "active"),
        ("SEC", "US SEC Company Facts", "active"),
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

    assert applied == 2
    with sqlite3.connect(db_path) as conn:
        supported_ticker_count = conn.execute(
            "SELECT COUNT(*) FROM supported_tickers"
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
            FROM supported_tickers st
            JOIN providers p ON p.provider_code = st.provider
            """
        ).fetchone()[0]
        fundamentals_raw_join_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM fundamentals_raw fr
            JOIN providers p ON p.provider_code = fr.provider
            """
        ).fetchone()[0]
        market_data_join_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM market_data md
            JOIN providers p ON p.provider_code = md.source_provider
            """
        ).fetchone()[0]
        fx_rates_join_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM fx_rates fx
            JOIN providers p ON p.provider_code = fx.provider
            """
        ).fetchone()[0]

    assert supported_ticker_count == 2
    assert fundamentals_raw_count == 2
    assert market_data_count == 1
    assert fx_rates_count == 2
    assert supported_ticker_join_count == 2
    assert fundamentals_raw_join_count == 2
    assert market_data_join_count == 1
    assert fx_rates_join_count == 2


def test_providers_table_rejects_invalid_provider_codes(tmp_path):
    db_path = tmp_path / "providers-invalid.sqlite"
    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO providers (
                    provider_code,
                    display_name,
                    description,
                    status,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "eodhd",
                    "Lowercase provider",
                    None,
                    "active",
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO providers (
                    provider_code,
                    display_name,
                    description,
                    status,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "",
                    "Blank provider",
                    None,
                    "active",
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            )


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
        "provider_symbol",
        "provider_ticker",
        "provider_exchange_code",
        "security_id",
        "listing_exchange",
        "security_name",
        "security_type",
        "country",
        "currency",
        "isin",
        "updated_at",
    }
    assert pk_cols == ["provider", "provider_symbol"]
    assert "idx_supported_tickers_provider_exchange" in index_names


def test_migration_creates_security_listing_status_table(tmp_path):
    db_path = tmp_path / "security-listing-status.sqlite"

    first = apply_migrations(db_path)
    second = apply_migrations(db_path)

    assert first == len(MIGRATIONS)
    assert second == 0

    with sqlite3.connect(db_path) as conn:
        info = conn.execute("PRAGMA table_info(security_listing_status)").fetchall()
        columns = {row[1] for row in info}
        pk_cols = [row[1] for row in info if row[5]]
        indexes = conn.execute("PRAGMA index_list(security_listing_status)").fetchall()
        index_names = {row[1] for row in indexes}

    assert columns == {
        "security_id",
        "source_provider",
        "provider_symbol",
        "raw_fetched_at",
        "is_primary_listing",
        "primary_provider_symbol",
        "classification_basis",
        "updated_at",
    }
    assert pk_cols == ["security_id"]
    assert "idx_security_listing_status_primary" in index_names


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
        info = conn.execute("PRAGMA table_info(securities)").fetchall()
        columns = {row[1] for row in info}
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
        supported_ticker_indexes = conn.execute(
            "PRAGMA index_list(supported_tickers)"
        ).fetchall()
        supported_ticker_index_names = {row[1] for row in supported_ticker_indexes}
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
            SELECT entity_name, description, sector, industry
            FROM securities
            WHERE canonical_symbol = 'AAA.US'
            """
        ).fetchone()

    assert "sector" in columns
    assert "industry" in columns
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
    if "supported_tickers" in tables:
        assert "idx_supported_tickers_currency_nonnull" in supported_ticker_index_names
    if "financial_facts" in tables:
        assert "idx_fin_facts_currency_nonnull" in financial_fact_index_names
    if "market_data" in tables:
        assert "idx_market_data_currency_nonnull" in market_data_index_names
    assert row == ("AAA Corp", "AAA description", None, None)


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
        "provider",
        "provider_symbol",
        "last_fetched_at",
        "last_status",
        "last_error",
        "next_eligible_at",
        "attempts",
    }
    assert pk_cols == ["provider", "provider_symbol"]
    assert "idx_market_data_fetch_next" in index_names


def test_migration_creates_fundamentals_hot_path_indexes(tmp_path):
    db_path = tmp_path / "fundamentals-hot-path-indexes.sqlite"

    apply_migrations(db_path)

    with sqlite3.connect(db_path) as conn:
        state_indexes = conn.execute(
            "PRAGMA index_list(fundamentals_fetch_state)"
        ).fetchall()
        state_index_names = {row[1] for row in state_indexes}

    assert "idx_fundamentals_fetch_state_provider_fetched_symbol" in state_index_names
    assert (
        "idx_fundamentals_fetch_state_provider_status_next_symbol" in state_index_names
    )


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
            SELECT security_id, refreshed_at
            FROM financial_facts_refresh_state
            """
        ).fetchone()
        status_indexes = conn.execute(
            "PRAGMA index_list(metric_compute_status)"
        ).fetchall()
        status_index_names = {row[1] for row in status_indexes}

    assert refresh_columns == {"security_id", "refreshed_at"}
    assert refresh_row is not None
    assert refresh_row[0] == 1
    assert refresh_row[1]
    assert status_columns == {
        "security_id",
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
            SELECT security_name, security_type
            FROM supported_tickers
            WHERE provider = 'SEC' AND provider_symbol = 'ABC.US'
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
        supported_ticker_indexes = conn.execute(
            "PRAGMA index_list(supported_tickers)"
        ).fetchall()
        supported_ticker_index_names = {index[1] for index in supported_ticker_indexes}

    assert row == ("Preserved Name", "ETF")
    assert listings_exists is None
    assert fx_exists == ("fx_rates",)
    assert fx_supported_pairs_exists == ("fx_supported_pairs",)
    assert fx_refresh_state_exists == ("fx_refresh_state",)
    assert "idx_supported_tickers_currency_nonnull" in supported_ticker_index_names
