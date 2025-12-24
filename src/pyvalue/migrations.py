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
    """No-op legacy migration (UK tables removed)."""

    return


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
    """No-op legacy migration (UK tables removed)."""

    return


def _migration_005_drop_unique_isin_index(conn: sqlite3.Connection) -> None:
    """No-op legacy migration (UK tables removed)."""

    return


def _migration_006_create_uk_filing_documents(conn: sqlite3.Connection) -> None:
    """No-op legacy migration (UK tables removed)."""

    return


def _migration_007_fundamentals_provider_columns(conn: sqlite3.Connection) -> None:
    """Add provider-aware storage for fundamentals and normalized facts."""

    # Create raw fundamentals storage if missing.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_raw (
            provider TEXT NOT NULL,
            symbol TEXT NOT NULL,
            region TEXT,
            currency TEXT,
            data TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (provider, symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_region
        ON fundamentals_raw(region)
        """
    )

    # Rebuild financial_facts to include provider/accounting_standard.
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='financial_facts'"
    ).fetchone()
    if exists is None:
        return

    info = conn.execute("PRAGMA table_info(financial_facts)").fetchall()
    columns = {row[1] for row in info}
    needs_provider = "provider" not in columns
    needs_accounting = "accounting_standard" not in columns
    needs_currency = "currency" not in columns
    if not (needs_provider or needs_accounting or needs_currency):
        return

    conn.execute("ALTER TABLE financial_facts RENAME TO financial_facts_old")
    conn.execute(
        """
        CREATE TABLE financial_facts (
            symbol TEXT NOT NULL,
            provider TEXT NOT NULL,
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
            PRIMARY KEY (symbol, provider, concept, fiscal_period, end_date, unit, accn)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO financial_facts (
            symbol, provider, cik, concept, fiscal_period, end_date, unit,
            value, accn, filed, frame, start_date, accounting_standard, currency
        )
        SELECT
            symbol,
            'SEC' AS provider,
            cik,
            concept,
            fiscal_period,
            end_date,
            unit,
            value,
            accn,
            filed,
            frame,
            start_date,
            NULL AS accounting_standard,
            NULL AS currency
        FROM financial_facts_old
        """
    )
    conn.execute("DROP TABLE financial_facts_old")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fin_facts_symbol_concept
        ON financial_facts(symbol, concept, provider)
        """
    )


def _migration_008_create_exchange_metadata(conn: sqlite3.Connection) -> None:
    """Store EODHD exchange metadata for region lookups."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS exchange_metadata (
            code TEXT PRIMARY KEY,
            name TEXT,
            country TEXT,
            currency TEXT,
            operating_mic TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )


def _migration_009_add_exchange_to_fundamentals(conn: sqlite3.Connection) -> None:
    """Track exchange code on fundamentals_raw."""

    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='fundamentals_raw'"
    ).fetchone()
    if exists is None:
        return

    info = conn.execute("PRAGMA table_info(fundamentals_raw)").fetchall()
    columns = {row[1] for row in info}
    if "exchange" in columns:
        return

    conn.execute("ALTER TABLE fundamentals_raw ADD COLUMN exchange TEXT")


def _migration_010_qualify_listings_symbols(conn: sqlite3.Connection) -> None:
    """Suffix listing symbols with exchange or region code when missing."""

    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='listings'"
    ).fetchone()
    if exists is None:
        return

    rows = conn.execute(
        "SELECT symbol, exchange, region FROM listings WHERE symbol NOT LIKE '%.%'"
    ).fetchall()
    for symbol, exchange, region in rows:
        exch_code = (exchange or region or "US").upper().replace(" ", "")
        qualified = f"{symbol}.{exch_code}"
        conn.execute(
            "UPDATE listings SET symbol = ? WHERE symbol = ? AND region = ?",
            (qualified, symbol, region),
        )


def _migration_011_add_currency_to_listings(conn: sqlite3.Connection) -> None:
    """Add currency column to listings."""

    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='listings'"
    ).fetchone()
    if exists is None:
        return

    info = conn.execute("PRAGMA table_info(listings)").fetchall()
    columns = {row[1] for row in info}
    if "currency" in columns:
        return

    conn.execute("ALTER TABLE listings ADD COLUMN currency TEXT")


def _migration_012_drop_provider_from_financial_facts(conn: sqlite3.Connection) -> None:
    """Remove provider column from financial_facts and dedupe by fact key."""

    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='financial_facts'"
    ).fetchone()
    if exists is None:
        return

    info = conn.execute("PRAGMA table_info(financial_facts)").fetchall()
    columns = {row[1] for row in info}
    if "provider" not in columns:
        return

    conn.execute("ALTER TABLE financial_facts RENAME TO financial_facts_old")
    conn.execute(
        """
        CREATE TABLE financial_facts (
            symbol TEXT NOT NULL,
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
            PRIMARY KEY (symbol, concept, fiscal_period, end_date, unit, accn)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO financial_facts (
            symbol,
            cik,
            concept,
            fiscal_period,
            end_date,
            unit,
            value,
            accn,
            filed,
            frame,
            start_date,
            accounting_standard,
            currency
        )
        SELECT
            symbol,
            cik,
            concept,
            fiscal_period,
            end_date,
            unit,
            value,
            accn,
            filed,
            frame,
            start_date,
            accounting_standard,
            currency
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, concept, fiscal_period, end_date, unit, accn
                    ORDER BY
                        CASE provider
                            WHEN 'SEC' THEN 0
                            WHEN 'EODHD' THEN 1
                            ELSE 2
                        END,
                        filed DESC
                ) AS rn
            FROM financial_facts_old
        )
        WHERE rn = 1
        """
    )
    conn.execute("DROP TABLE financial_facts_old")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fin_facts_symbol_concept
        ON financial_facts(symbol, concept)
        """
    )


def _migration_013_create_fundamentals_fetch_state(conn: sqlite3.Connection) -> None:
    """Track per-symbol fundamentals fetch status for resumable ingestion."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_fetch_state (
            provider TEXT NOT NULL,
            symbol TEXT NOT NULL,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            next_eligible_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (provider, symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_fetch_next
        ON fundamentals_fetch_state(provider, next_eligible_at)
        """
    )


def _migration_014_drop_uk_tables(conn: sqlite3.Connection) -> None:
    """Drop legacy UK ingestion tables."""

    conn.execute("DROP TABLE IF EXISTS uk_filing_documents")
    conn.execute("DROP TABLE IF EXISTS uk_symbol_map")
    conn.execute("DROP TABLE IF EXISTS uk_company_facts")
    conn.execute("DROP INDEX IF EXISTS idx_uk_symbol_map_isin")


def _migration_015_drop_region_columns(conn: sqlite3.Connection) -> None:
    """Remove region columns from listings and fundamentals_raw."""

    listings_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='listings'"
    ).fetchone()
    if listings_exists is not None:
        info = conn.execute("PRAGMA table_info(listings)").fetchall()
        columns = {row[1] for row in info}
        if "region" in columns:
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
                    isin TEXT,
                    currency TEXT,
                    ingested_at TEXT NOT NULL,
                    PRIMARY KEY (symbol)
                )
                """
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO listings (
                    symbol,
                    security_name,
                    exchange,
                    market_category,
                    is_etf,
                    status,
                    round_lot_size,
                    source,
                    isin,
                    currency,
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
                    isin,
                    currency,
                    ingested_at
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY symbol
                               ORDER BY ingested_at DESC
                           ) AS rn
                    FROM listings_old
                )
                WHERE rn = 1
                """
            )
            conn.execute("DROP TABLE listings_old")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_listings_exchange
                ON listings(exchange)
                """
            )

    fundamentals_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='fundamentals_raw'"
    ).fetchone()
    if fundamentals_exists is None:
        return

    info = conn.execute("PRAGMA table_info(fundamentals_raw)").fetchall()
    columns = {row[1] for row in info}
    if "region" not in columns:
        return

    conn.execute("ALTER TABLE fundamentals_raw RENAME TO fundamentals_raw_old")
    conn.execute(
        """
        CREATE TABLE fundamentals_raw (
            provider TEXT NOT NULL,
            symbol TEXT NOT NULL,
            currency TEXT,
            exchange TEXT,
            data TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (provider, symbol)
        )
        """
    )
    if "exchange" in columns:
        exchange_select = "exchange"
    else:
        exchange_select = "NULL AS exchange"
    conn.execute(
        f"""
        INSERT INTO fundamentals_raw (
            provider,
            symbol,
            currency,
            exchange,
            data,
            fetched_at
        )
        SELECT
            provider,
            symbol,
            currency,
            {exchange_select},
            data,
            fetched_at
        FROM fundamentals_raw_old
        """
    )
    conn.execute("DROP TABLE fundamentals_raw_old")


def _migration_016_drop_exchange_metadata_and_company_facts(conn: sqlite3.Connection) -> None:
    """Drop unused exchange metadata and company facts tables."""

    conn.execute("DROP TABLE IF EXISTS exchange_metadata")
    conn.execute("DROP INDEX IF EXISTS idx_company_facts_symbol")
    conn.execute("DROP TABLE IF EXISTS company_facts")


def _migration_017_add_description_to_entity_metadata(conn: sqlite3.Connection) -> None:
    """Add description column to entity_metadata."""

    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='entity_metadata'"
    ).fetchone()
    if exists is None:
        return

    info = conn.execute("PRAGMA table_info(entity_metadata)").fetchall()
    columns = {row[1] for row in info}
    if "description" in columns:
        return

    conn.execute("ALTER TABLE entity_metadata ADD COLUMN description TEXT")


MIGRATIONS: Sequence[Migration] = [
    _migration_001_listings_composite_pk,
    _migration_002_create_uk_company_facts,
    _migration_003_add_isin_to_listings,
    _migration_004_create_uk_symbol_map,
    _migration_005_drop_unique_isin_index,
    _migration_006_create_uk_filing_documents,
    _migration_007_fundamentals_provider_columns,
    _migration_008_create_exchange_metadata,
    _migration_009_add_exchange_to_fundamentals,
    _migration_010_qualify_listings_symbols,
    _migration_011_add_currency_to_listings,
    _migration_012_drop_provider_from_financial_facts,
    _migration_013_create_fundamentals_fetch_state,
    _migration_014_drop_uk_tables,
    _migration_015_drop_region_columns,
    _migration_016_drop_exchange_metadata_and_company_facts,
    _migration_017_add_description_to_entity_metadata,
]


__all__ = ["apply_migrations", "MIGRATIONS"]
