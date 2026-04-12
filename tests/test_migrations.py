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
        "provider_exchange_code",
        "canonical_exchange_code",
        "name",
        "country",
        "currency",
        "operating_mic",
        "country_iso2",
        "country_iso3",
        "updated_at",
    }
    assert pk_cols == ["provider", "provider_exchange_code"]


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

    assert applied == 6
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

    assert applied == 1
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

    assert applied == 10
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
