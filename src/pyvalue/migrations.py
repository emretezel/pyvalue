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


def _migration_006_create_uk_filing_documents(conn: sqlite3.Connection) -> None:
    """Create storage for Companies House filing documents (iXBRL only)."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS uk_filing_documents (
            company_number TEXT NOT NULL,
            symbol TEXT,
            filing_id TEXT NOT NULL,
            period_start TEXT,
            period_end TEXT,
            doc_type TEXT,
            is_ixbrl INTEGER NOT NULL,
            fetched_at TEXT NOT NULL,
            content BLOB NOT NULL,
            PRIMARY KEY (company_number, filing_id)
        )
        """
    )


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
]


__all__ = ["apply_migrations", "MIGRATIONS"]
