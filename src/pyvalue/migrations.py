"""Lightweight SQLite schema migration runner.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union, cast

Migration = Callable[[sqlite3.Connection], None]

_US_VENUE_CODES = {
    "AMEX",
    "ARCA",
    "BATS",
    "CBOEBZX",
    "NASDAQ",
    "NYSE",
    "NYSEARCA",
    "NYSEMKT",
    "OTC",
    "US",
}

_PROVIDER_REGISTRY_ROWS: Tuple[Tuple[str, str, Optional[str], str], ...] = (
    (
        "EODHD",
        "EOD Historical Data",
        "Exchange, fundamentals, market-data, and FX provider.",
        "active",
    ),
    (
        "SEC",
        "US SEC Company Facts",
        "US issuer fundamentals provider backed by SEC company facts.",
        "active",
    ),
    (
        "FRANKFURTER",
        "Frankfurter FX",
        "FX rates provider used for direct currency history refreshes.",
        "active",
    ),
)


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


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    return {
        row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def _normalize_optional_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_upper(value: object) -> Optional[str]:
    text = _normalize_optional_text(value)
    return text.upper() if text is not None else None


def _split_symbol(symbol: str) -> Tuple[str, Optional[str]]:
    cleaned = symbol.strip().upper()
    if "." not in cleaned:
        return cleaned, None
    ticker, exchange = cleaned.rsplit(".", 1)
    return ticker, exchange


def _infer_canonical_exchange(symbol: str) -> Optional[str]:
    _, suffix = _split_symbol(symbol)
    if suffix is None:
        return None
    suffix = suffix.upper().replace(" ", "")
    if suffix in _US_VENUE_CODES:
        return "US"
    return suffix


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
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
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
    finally:
        conn.close()

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


def _migration_016_drop_exchange_metadata_and_company_facts(
    conn: sqlite3.Connection,
) -> None:
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


def _migration_018_create_supported_exchanges(conn: sqlite3.Connection) -> None:
    """Store provider-supported exchange metadata."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS supported_exchanges (
            provider TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            country TEXT,
            currency TEXT,
            operating_mic TEXT,
            country_iso2 TEXT,
            country_iso3 TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (provider, code)
        )
        """
    )


def _migration_019_create_supported_tickers(conn: sqlite3.Connection) -> None:
    """Store provider-supported tickers by exchange."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS supported_tickers (
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
        CREATE INDEX IF NOT EXISTS idx_supported_tickers_provider_exchange
        ON supported_tickers(provider, exchange_code)
        """
    )


def _migration_020_create_market_data_fetch_state(conn: sqlite3.Connection) -> None:
    """Track market-data fetch status for resumable ingestion."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_data_fetch_state (
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
        CREATE INDEX IF NOT EXISTS idx_market_data_fetch_next
        ON market_data_fetch_state(provider, next_eligible_at)
        """
    )


def _migration_021_drop_listings_in_favor_of_supported_tickers(
    conn: sqlite3.Connection,
) -> None:
    """Backfill canonical supported_tickers rows and remove listings."""

    _migration_019_create_supported_tickers(conn)
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='listings'"
    ).fetchone()
    if exists is None:
        return

    info = conn.execute("PRAGMA table_info(listings)").fetchall()
    columns = {row[1] for row in info}
    select_columns = [
        "symbol",
        "security_name",
        "exchange",
        "is_etf",
        "source",
        "ingested_at",
    ]
    if "isin" in columns:
        select_columns.append("isin")
    if "currency" in columns:
        select_columns.append("currency")
    rows = conn.execute(
        f"SELECT {', '.join(select_columns)} FROM listings ORDER BY symbol"
    ).fetchall()

    payload = []
    for row in rows:
        symbol_raw = str(row["symbol"] or "").strip().upper()
        if not symbol_raw:
            continue
        source = str(row["source"] or "").strip().lower()
        provider = "EODHD" if source == "eodhd" else "SEC"
        listing_exchange = str(row["exchange"] or "").strip().upper() or None
        exchange_code = "US" if provider == "SEC" else None
        if exchange_code is None:
            if listing_exchange:
                exchange_code = listing_exchange
            elif "." in symbol_raw:
                exchange_code = symbol_raw.split(".", 1)[1].upper()
            else:
                exchange_code = "US"
        symbol = symbol_raw if "." in symbol_raw else f"{symbol_raw}.{exchange_code}"
        security_name = str(row["security_name"] or "").strip() or None
        security_type = "ETF" if int(row["is_etf"] or 0) else "Common Stock"
        isin = str(row["isin"] or "").strip() if "isin" in columns else ""
        currency = (
            str(row["currency"] or "").strip().upper() if "currency" in columns else ""
        )
        updated_at = (
            str(row["ingested_at"] or "").strip()
            or datetime.now(timezone.utc).isoformat()
        )
        payload.append(
            (
                provider,
                exchange_code,
                symbol,
                symbol.split(".", 1)[0],
                listing_exchange,
                security_name,
                security_type,
                None,
                currency or None,
                isin or None,
                updated_at,
            )
        )

    if payload:
        conn.executemany(
            """
            INSERT OR IGNORE INTO supported_tickers (
                provider,
                exchange_code,
                symbol,
                code,
                listing_exchange,
                security_name,
                security_type,
                country,
                currency,
                isin,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )

    conn.execute("DROP INDEX IF EXISTS idx_listings_exchange")
    conn.execute("DROP INDEX IF EXISTS idx_listings_region")
    conn.execute("DROP TABLE IF EXISTS listings")


def _migration_022_canonical_security_model(conn: sqlite3.Connection) -> None:
    """Rebuild storage around canonical securities and provider mappings."""

    now = datetime.now(timezone.utc).isoformat()

    def read_supported_exchanges() -> List[Dict[str, Optional[str]]]:
        if not _table_exists(conn, "supported_exchanges"):
            return []
        columns = _table_columns(conn, "supported_exchanges")
        if {"provider_exchange_code", "canonical_exchange_code"}.issubset(columns):
            rows = conn.execute(
                """
                SELECT provider, provider_exchange_code, canonical_exchange_code, name,
                       country, currency, operating_mic, country_iso2, country_iso3,
                       updated_at
                FROM supported_exchanges
                """
            ).fetchall()
            return [dict(row) for row in rows]
        rows = conn.execute(
            """
            SELECT provider, code AS provider_exchange_code, code AS canonical_exchange_code,
                   name, country, currency, operating_mic, country_iso2, country_iso3,
                   updated_at
            FROM supported_exchanges
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def read_supported_tickers() -> List[Dict[str, Optional[str]]]:
        if not _table_exists(conn, "supported_tickers"):
            return []
        columns = _table_columns(conn, "supported_tickers")
        if {"provider_symbol", "provider_ticker", "provider_exchange_code"}.issubset(
            columns
        ):
            rows = conn.execute(
                """
                SELECT provider, provider_symbol, provider_ticker, provider_exchange_code,
                       listing_exchange, security_name, security_type, country, currency,
                       isin, updated_at
                FROM supported_tickers
                """
            ).fetchall()
            return [dict(row) for row in rows]
        rows = conn.execute(
            """
            SELECT provider, symbol AS provider_symbol, code AS provider_ticker,
                   exchange_code AS provider_exchange_code, listing_exchange,
                   security_name, security_type, country, currency, isin, updated_at
            FROM supported_tickers
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def read_fundamentals_raw() -> List[Dict[str, Optional[str]]]:
        if not _table_exists(conn, "fundamentals_raw"):
            return []
        columns = _table_columns(conn, "fundamentals_raw")
        if {"provider_symbol", "provider_exchange_code", "security_id"}.issubset(
            columns
        ):
            rows = conn.execute(
                """
                SELECT provider, provider_symbol, provider_exchange_code, currency, data,
                       fetched_at
                FROM fundamentals_raw
                """
            ).fetchall()
            return [dict(row) for row in rows]
        rows = conn.execute(
            """
            SELECT provider, symbol AS provider_symbol, exchange AS provider_exchange_code,
                   currency, data, fetched_at
            FROM fundamentals_raw
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def read_fetch_state(table_name: str) -> List[Dict[str, Optional[str]]]:
        if not _table_exists(conn, table_name):
            return []
        columns = _table_columns(conn, table_name)
        if "provider_symbol" in columns:
            rows = conn.execute(
                f"""
                SELECT provider, provider_symbol, last_fetched_at, last_status, last_error,
                       next_eligible_at, attempts
                FROM {table_name}
                """
            ).fetchall()
            return [dict(row) for row in rows]
        rows = conn.execute(
            f"""
            SELECT provider, symbol AS provider_symbol, last_fetched_at, last_status,
                   last_error, next_eligible_at, attempts
            FROM {table_name}
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def read_financial_facts() -> List[Dict[str, object]]:
        if not _table_exists(conn, "financial_facts"):
            return []
        columns = _table_columns(conn, "financial_facts")
        if "security_id" in columns:
            rows = conn.execute(
                """
                SELECT security_id, cik, concept, fiscal_period, end_date, unit, value,
                       accn, filed, frame, start_date, accounting_standard, currency,
                       source_provider
                FROM financial_facts
                """
            ).fetchall()
            return [dict(row) for row in rows]
        rows = conn.execute(
            """
            SELECT symbol, cik, concept, fiscal_period, end_date, unit, value, accn,
                   filed, frame, start_date, accounting_standard, currency
            FROM financial_facts
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def read_market_data() -> List[Dict[str, object]]:
        if not _table_exists(conn, "market_data"):
            return []
        columns = _table_columns(conn, "market_data")
        if "security_id" in columns:
            rows = conn.execute(
                """
                SELECT security_id, as_of, price, volume, market_cap, currency,
                       source_provider, updated_at
                FROM market_data
                """
            ).fetchall()
            return [dict(row) for row in rows]
        rows = conn.execute(
            """
            SELECT symbol, as_of, price, volume, market_cap, currency
            FROM market_data
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def read_metrics() -> List[Dict[str, object]]:
        if not _table_exists(conn, "metrics"):
            return []
        columns = _table_columns(conn, "metrics")
        if "security_id" in columns:
            rows = conn.execute(
                "SELECT security_id, metric_id, value, as_of FROM metrics"
            ).fetchall()
            return [dict(row) for row in rows]
        rows = conn.execute(
            "SELECT symbol, metric_id, value, as_of FROM metrics"
        ).fetchall()
        return [dict(row) for row in rows]

    def read_entity_metadata() -> List[Dict[str, Optional[str]]]:
        if not _table_exists(conn, "entity_metadata"):
            return []
        columns = _table_columns(conn, "entity_metadata")
        select_columns = ["symbol", "entity_name"]
        if "description" in columns:
            select_columns.append("description")
        rows = conn.execute(
            f"SELECT {', '.join(select_columns)} FROM entity_metadata"
        ).fetchall()
        return [dict(row) for row in rows]

    old_supported_exchanges = read_supported_exchanges()
    old_supported_tickers = read_supported_tickers()
    old_fundamentals_raw = read_fundamentals_raw()
    old_fundamentals_fetch_state = read_fetch_state("fundamentals_fetch_state")
    old_market_data_fetch_state = read_fetch_state("market_data_fetch_state")
    old_financial_facts = read_financial_facts()
    old_market_data = read_market_data()
    old_metrics = read_metrics()
    old_entity_metadata = read_entity_metadata()

    exchange_records: Dict[Tuple[str, str], Dict[str, Optional[str]]] = {}
    for row in old_supported_exchanges:
        provider = _normalize_upper(row.get("provider"))
        provider_exchange_code = _normalize_upper(
            row.get("provider_exchange_code") or row.get("code")
        )
        if provider is None or provider_exchange_code is None:
            continue
        exchange_records[(provider, provider_exchange_code)] = {
            "provider": provider,
            "provider_exchange_code": provider_exchange_code,
            "canonical_exchange_code": _normalize_upper(
                row.get("canonical_exchange_code") or provider_exchange_code
            )
            or provider_exchange_code,
            "name": _normalize_optional_text(row.get("name")),
            "country": _normalize_optional_text(row.get("country")),
            "currency": _normalize_upper(row.get("currency")),
            "operating_mic": _normalize_optional_text(row.get("operating_mic")),
            "country_iso2": _normalize_upper(row.get("country_iso2")),
            "country_iso3": _normalize_upper(row.get("country_iso3")),
            "updated_at": _normalize_optional_text(row.get("updated_at")) or now,
        }

    exchange_records[("SEC", "US")] = {
        "provider": "SEC",
        "provider_exchange_code": "US",
        "canonical_exchange_code": "US",
        "name": exchange_records.get(("SEC", "US"), {}).get("name") or "United States",
        "country": exchange_records.get(("SEC", "US"), {}).get("country") or "US",
        "currency": exchange_records.get(("SEC", "US"), {}).get("currency") or "USD",
        "operating_mic": exchange_records.get(("SEC", "US"), {}).get("operating_mic"),
        "country_iso2": exchange_records.get(("SEC", "US"), {}).get("country_iso2")
        or "US",
        "country_iso3": exchange_records.get(("SEC", "US"), {}).get("country_iso3")
        or "USA",
        "updated_at": now,
    }

    symbol_identity_map: Dict[str, Tuple[str, str]] = {}
    security_metadata: Dict[Tuple[str, str], Dict[str, Optional[str]]] = {}
    source_provider_by_symbol: Dict[str, str] = {}

    def provider_identity(
        provider: object,
        symbol: object,
        provider_exchange_code: object = None,
    ) -> Tuple[str, str, str, str]:
        provider_norm = _normalize_upper(provider)
        symbol_norm = _normalize_upper(symbol)
        if provider_norm is None or symbol_norm is None:
            raise RuntimeError("Provider-owned symbol row is missing required identity")
        ticker, suffix = _split_symbol(symbol_norm)
        if provider_norm == "SEC":
            provider_exchange_norm = "US"
            provider_symbol = f"{ticker}.US"
            canonical_exchange = "US"
        else:
            resolved_provider_exchange = (
                _normalize_upper(provider_exchange_code) or suffix
            )
            if resolved_provider_exchange is None:
                raise RuntimeError(
                    f"Could not resolve provider exchange code for {provider_norm}:{symbol_norm}"
                )
            provider_exchange_norm = resolved_provider_exchange
            provider_symbol = (
                symbol_norm
                if suffix is not None
                else f"{ticker}.{provider_exchange_norm}"
            )
            if (provider_norm, provider_exchange_norm) not in exchange_records:
                exchange_records[(provider_norm, provider_exchange_norm)] = {
                    "provider": provider_norm,
                    "provider_exchange_code": provider_exchange_norm,
                    "canonical_exchange_code": provider_exchange_norm,
                    "name": None,
                    "country": None,
                    "currency": None,
                    "operating_mic": None,
                    "country_iso2": None,
                    "country_iso3": None,
                    "updated_at": now,
                }
            canonical_exchange = (
                exchange_records[(provider_norm, provider_exchange_norm)][
                    "canonical_exchange_code"
                ]
                or provider_exchange_norm
            )
        symbol_identity_map[symbol_norm] = (ticker, canonical_exchange)
        symbol_identity_map[provider_symbol] = (ticker, canonical_exchange)
        if symbol_norm not in source_provider_by_symbol:
            source_provider_by_symbol[symbol_norm] = provider_norm
        if provider_symbol not in source_provider_by_symbol:
            source_provider_by_symbol[provider_symbol] = provider_norm
        return provider_symbol, ticker, provider_exchange_norm, canonical_exchange

    for row in old_supported_tickers:
        provider_symbol, provider_ticker, provider_exchange_code, canonical_exchange = (
            provider_identity(
                row.get("provider"),
                row.get("provider_symbol") or row.get("symbol"),
                row.get("provider_exchange_code") or row.get("exchange_code"),
            )
        )
        key = (provider_ticker, canonical_exchange)
        meta = security_metadata.setdefault(
            key, {"entity_name": None, "description": None}
        )
        meta["entity_name"] = meta["entity_name"] or _normalize_optional_text(
            row.get("security_name")
        )

    for row in old_fundamentals_raw:
        provider_identity(
            row.get("provider"),
            row.get("provider_symbol") or row.get("symbol"),
            row.get("provider_exchange_code") or row.get("exchange"),
        )

    for row in old_fundamentals_fetch_state:
        provider_identity(row.get("provider"), row.get("provider_symbol"))

    for row in old_market_data_fetch_state:
        provider_identity(row.get("provider"), row.get("provider_symbol"))

    def resolve_symbol_identity(symbol: object) -> Tuple[str, str]:
        symbol_norm = _normalize_upper(symbol)
        if symbol_norm is None:
            raise RuntimeError("Encountered empty symbol while backfilling security_id")
        identity = symbol_identity_map.get(symbol_norm)
        if identity is not None:
            return identity
        ticker, _ = _split_symbol(symbol_norm)
        canonical_exchange = _infer_canonical_exchange(symbol_norm)
        if canonical_exchange is None:
            raise RuntimeError(
                f"Could not infer canonical exchange code for symbol {symbol_norm}"
            )
        symbol_identity_map[symbol_norm] = (ticker, canonical_exchange)
        return ticker, canonical_exchange

    for fact_row in old_financial_facts:
        if "symbol" in fact_row:
            resolve_symbol_identity(fact_row["symbol"])

    for market_row in old_market_data:
        if "symbol" in market_row:
            resolve_symbol_identity(market_row["symbol"])

    for metric_row in old_metrics:
        if "symbol" in metric_row:
            resolve_symbol_identity(metric_row["symbol"])

    for row in old_entity_metadata:
        ticker, canonical_exchange = resolve_symbol_identity(row.get("symbol"))
        key = (ticker, canonical_exchange)
        meta = security_metadata.setdefault(
            key, {"entity_name": None, "description": None}
        )
        meta["entity_name"] = meta["entity_name"] or _normalize_optional_text(
            row.get("entity_name")
        )
        meta["description"] = meta["description"] or _normalize_optional_text(
            row.get("description")
        )

    security_keys = sorted(
        {identity for identity in symbol_identity_map.values()},
        key=lambda item: (item[1], item[0]),
    )

    for table_name in [
        "supported_exchanges",
        "supported_tickers",
        "securities",
        "fundamentals_raw",
        "fundamentals_fetch_state",
        "market_data_fetch_state",
        "financial_facts",
        "market_data",
        "metrics",
        "entity_metadata",
    ]:
        if _table_exists(conn, table_name):
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")

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
    conn.execute(
        """
        CREATE INDEX idx_supported_exchanges_canonical
        ON supported_exchanges(canonical_exchange_code)
        """
    )
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
        CREATE INDEX idx_securities_exchange
        ON securities(canonical_exchange_code)
        """
    )
    conn.execute(
        """
        CREATE TABLE supported_tickers (
            provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            provider_ticker TEXT NOT NULL,
            provider_exchange_code TEXT NOT NULL,
            security_id INTEGER NOT NULL,
            listing_exchange TEXT,
            security_name TEXT,
            security_type TEXT,
            country TEXT,
            currency TEXT,
            isin TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (provider, provider_symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX idx_supported_tickers_provider_exchange_ticker
        ON supported_tickers(provider, provider_exchange_code, provider_ticker)
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_supported_tickers_provider_exchange
        ON supported_tickers(provider, provider_exchange_code)
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_supported_tickers_security
        ON supported_tickers(security_id)
        """
    )
    conn.execute(
        """
        CREATE TABLE fundamentals_raw (
            provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            security_id INTEGER NOT NULL,
            provider_exchange_code TEXT,
            currency TEXT,
            data TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (provider, provider_symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_fundamentals_raw_security
        ON fundamentals_raw(security_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_fundamentals_raw_provider_fetched
        ON fundamentals_raw(provider, fetched_at)
        """
    )
    conn.execute(
        """
        CREATE TABLE fundamentals_fetch_state (
            provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            next_eligible_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (provider, provider_symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_fundamentals_fetch_next
        ON fundamentals_fetch_state(provider, next_eligible_at)
        """
    )
    conn.execute(
        """
        CREATE TABLE market_data_fetch_state (
            provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            next_eligible_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (provider, provider_symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_market_data_fetch_next
        ON market_data_fetch_state(provider, next_eligible_at)
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
        CREATE INDEX idx_fin_facts_security_concept
        ON financial_facts(security_id, concept)
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_fin_facts_concept
        ON financial_facts(concept)
        """
    )
    conn.execute(
        """
        CREATE TABLE market_data (
            security_id INTEGER NOT NULL,
            as_of DATE NOT NULL,
            price REAL NOT NULL,
            volume INTEGER,
            market_cap REAL,
            currency TEXT,
            source_provider TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (security_id, as_of)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_market_data_latest
        ON market_data(security_id, as_of DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE metrics (
            security_id INTEGER NOT NULL,
            metric_id TEXT NOT NULL,
            value REAL NOT NULL,
            as_of TEXT NOT NULL,
            PRIMARY KEY (security_id, metric_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_metrics_metric_id
        ON metrics(metric_id)
        """
    )

    for record in sorted(
        exchange_records.values(),
        key=lambda item: (item["provider"], item["provider_exchange_code"]),
    ):
        conn.execute(
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
            (
                record["provider"],
                record["provider_exchange_code"],
                record["canonical_exchange_code"],
                record["name"],
                record["country"],
                record["currency"],
                record["operating_mic"],
                record["country_iso2"],
                record["country_iso3"],
                record["updated_at"] or now,
            ),
        )

    security_id_map: Dict[Tuple[str, str], int] = {}
    for canonical_ticker, canonical_exchange in security_keys:
        canonical_symbol = f"{canonical_ticker}.{canonical_exchange}"
        meta = security_metadata.get((canonical_ticker, canonical_exchange), {})
        conn.execute(
            """
            INSERT INTO securities (
                canonical_ticker,
                canonical_exchange_code,
                canonical_symbol,
                entity_name,
                description,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                canonical_ticker,
                canonical_exchange,
                canonical_symbol,
                meta.get("entity_name"),
                meta.get("description"),
                now,
                now,
            ),
        )
        row = conn.execute(
            """
            SELECT security_id
            FROM securities
            WHERE canonical_exchange_code = ? AND canonical_ticker = ?
            """,
            (canonical_exchange, canonical_ticker),
        ).fetchone()
        if row is None:
            raise RuntimeError(f"Failed to backfill security_id for {canonical_symbol}")
        security_id_map[(canonical_ticker, canonical_exchange)] = int(row[0])

    supported_ticker_payload: Dict[Tuple[str, str, str], Tuple[object, ...]] = {}
    supported_ticker_updated_at: Dict[Tuple[str, str, str], str] = {}
    for row in old_supported_tickers:
        provider_symbol, provider_ticker, provider_exchange_code, canonical_exchange = (
            provider_identity(
                row.get("provider"),
                row.get("provider_symbol") or row.get("symbol"),
                row.get("provider_exchange_code") or row.get("exchange_code"),
            )
        )
        security_id = security_id_map[(provider_ticker, canonical_exchange)]
        supported_ticker_key = (
            _normalize_upper(row.get("provider")) or "",
            provider_exchange_code,
            provider_ticker,
        )
        updated_at = _normalize_optional_text(row.get("updated_at")) or now
        if (
            supported_ticker_key in supported_ticker_updated_at
            and supported_ticker_updated_at[supported_ticker_key] >= updated_at
        ):
            continue
        supported_ticker_updated_at[supported_ticker_key] = updated_at
        supported_ticker_payload[supported_ticker_key] = (
            _normalize_upper(row.get("provider")),
            provider_symbol,
            provider_ticker,
            provider_exchange_code,
            security_id,
            _normalize_upper(row.get("listing_exchange")),
            _normalize_optional_text(row.get("security_name")),
            _normalize_optional_text(row.get("security_type")),
            _normalize_optional_text(row.get("country")),
            _normalize_upper(row.get("currency")),
            _normalize_optional_text(row.get("isin")),
            updated_at,
        )

    for payload in supported_ticker_payload.values():
        conn.execute(
            """
            INSERT INTO supported_tickers (
                provider,
                provider_symbol,
                provider_ticker,
                provider_exchange_code,
                security_id,
                listing_exchange,
                security_name,
                security_type,
                country,
                currency,
                isin,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )

    fundamentals_raw_payload: Dict[Tuple[str, str], Tuple[object, ...]] = {}
    for row in old_fundamentals_raw:
        provider_symbol, provider_ticker, provider_exchange_code, canonical_exchange = (
            provider_identity(
                row.get("provider"),
                row.get("provider_symbol") or row.get("symbol"),
                row.get("provider_exchange_code") or row.get("exchange"),
            )
        )
        security_id = security_id_map[(provider_ticker, canonical_exchange)]
        key = (_normalize_upper(row.get("provider")) or "", provider_symbol)
        fundamentals_raw_payload[key] = (
            _normalize_upper(row.get("provider")),
            provider_symbol,
            security_id,
            provider_exchange_code,
            _normalize_upper(row.get("currency")),
            row.get("data"),
            _normalize_optional_text(row.get("fetched_at")) or now,
        )

    for payload in fundamentals_raw_payload.values():
        conn.execute(
            """
            INSERT INTO fundamentals_raw (
                provider,
                provider_symbol,
                security_id,
                provider_exchange_code,
                currency,
                data,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )

    for table_name, rows in [
        ("fundamentals_fetch_state", old_fundamentals_fetch_state),
        ("market_data_fetch_state", old_market_data_fetch_state),
    ]:
        payload_map: Dict[Tuple[str, str], Tuple[object, ...]] = {}
        for row in rows:
            provider_symbol, _, _, _ = provider_identity(
                row.get("provider"),
                row.get("provider_symbol") or row.get("symbol"),
            )
            key = (_normalize_upper(row.get("provider")) or "", provider_symbol)
            payload_map[key] = (
                _normalize_upper(row.get("provider")),
                provider_symbol,
                _normalize_optional_text(row.get("last_fetched_at")),
                _normalize_optional_text(row.get("last_status")),
                _normalize_optional_text(row.get("last_error")),
                _normalize_optional_text(row.get("next_eligible_at")),
                int(row.get("attempts") or 0),
            )
        for payload in payload_map.values():
            conn.execute(
                f"""
                INSERT INTO {table_name} (
                    provider,
                    provider_symbol,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )

    for fact_row in old_financial_facts:
        if "security_id" in fact_row:
            security_id = int(cast(Union[str, int], fact_row["security_id"]))
        else:
            ticker, canonical_exchange = resolve_symbol_identity(fact_row.get("symbol"))
            security_id = security_id_map[(ticker, canonical_exchange)]
        source_provider = fact_row.get("source_provider")
        if source_provider is None and "symbol" in fact_row:
            source_provider = source_provider_by_symbol.get(
                _normalize_upper(fact_row.get("symbol")) or ""
            )
        conn.execute(
            """
            INSERT INTO financial_facts (
                security_id,
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
                currency,
                source_provider
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                security_id,
                _normalize_optional_text(fact_row.get("cik")),
                _normalize_optional_text(fact_row.get("concept")),
                _normalize_optional_text(fact_row.get("fiscal_period")),
                _normalize_optional_text(fact_row.get("end_date")),
                _normalize_optional_text(fact_row.get("unit")),
                fact_row.get("value"),
                _normalize_optional_text(fact_row.get("accn")),
                _normalize_optional_text(fact_row.get("filed")),
                _normalize_optional_text(fact_row.get("frame")),
                _normalize_optional_text(fact_row.get("start_date")),
                _normalize_optional_text(fact_row.get("accounting_standard")),
                _normalize_upper(fact_row.get("currency")),
                _normalize_upper(source_provider),
            ),
        )

    for market_row in old_market_data:
        if "security_id" in market_row:
            security_id = int(cast(Union[str, int], market_row["security_id"]))
            source_provider = (
                _normalize_upper(market_row.get("source_provider")) or "EODHD"
            )
            updated_at = _normalize_optional_text(market_row.get("updated_at")) or now
        else:
            ticker, canonical_exchange = resolve_symbol_identity(
                market_row.get("symbol")
            )
            security_id = security_id_map[(ticker, canonical_exchange)]
            source_provider = "EODHD"
            updated_at = now
        conn.execute(
            """
            INSERT INTO market_data (
                security_id,
                as_of,
                price,
                volume,
                market_cap,
                currency,
                source_provider,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                security_id,
                _normalize_optional_text(market_row.get("as_of")),
                market_row.get("price"),
                market_row.get("volume"),
                market_row.get("market_cap"),
                _normalize_upper(market_row.get("currency")),
                source_provider,
                updated_at,
            ),
        )

    for metric_row in old_metrics:
        if "security_id" in metric_row:
            security_id = int(cast(Union[str, int], metric_row["security_id"]))
        else:
            ticker, canonical_exchange = resolve_symbol_identity(
                metric_row.get("symbol")
            )
            security_id = security_id_map[(ticker, canonical_exchange)]
        conn.execute(
            """
            INSERT INTO metrics (
                security_id,
                metric_id,
                value,
                as_of
            ) VALUES (?, ?, ?, ?)
            """,
            (
                security_id,
                _normalize_optional_text(metric_row.get("metric_id")),
                metric_row.get("value"),
                _normalize_optional_text(metric_row.get("as_of")) or now,
            ),
        )


def _migration_023_optimize_fundamentals_hot_paths(conn: sqlite3.Connection) -> None:
    """Add lightweight fetch-state indexes for fundamentals selection and reporting."""

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_fetch_state_provider_fetched_symbol
        ON fundamentals_fetch_state(provider, last_fetched_at, provider_symbol)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_fetch_state_provider_status_next_symbol
        ON fundamentals_fetch_state(provider, last_status, next_eligible_at, provider_symbol)
        """
    )


def _migration_024_create_fundamentals_normalization_state(
    conn: sqlite3.Connection,
) -> None:
    """Track successful normalization watermarks for raw fundamentals payloads."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_normalization_state (
            provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            security_id INTEGER NOT NULL,
            raw_fetched_at TEXT NOT NULL,
            last_normalized_at TEXT NOT NULL,
            PRIMARY KEY (provider, provider_symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_norm_state_security
        ON fundamentals_normalization_state(security_id)
        """
    )


def _migration_025_add_sector_industry_to_securities(
    conn: sqlite3.Connection,
) -> None:
    """Add sector and industry columns to canonical securities."""

    if not _table_exists(conn, "securities"):
        return

    columns = _table_columns(conn, "securities")
    if "sector" not in columns:
        conn.execute("ALTER TABLE securities ADD COLUMN sector TEXT")
    if "industry" not in columns:
        conn.execute("ALTER TABLE securities ADD COLUMN industry TEXT")


def _migration_026_add_fx_rates_and_metric_metadata(
    conn: sqlite3.Connection,
) -> None:
    """Add DB-backed FX storage plus explicit metric unit metadata."""

    if _table_exists(conn, "metrics"):
        columns = _table_columns(conn, "metrics")
        if "unit_kind" not in columns:
            conn.execute(
                "ALTER TABLE metrics ADD COLUMN unit_kind TEXT NOT NULL DEFAULT 'other'"
            )
        if "currency" not in columns:
            conn.execute("ALTER TABLE metrics ADD COLUMN currency TEXT")
        if "unit_label" not in columns:
            conn.execute("ALTER TABLE metrics ADD COLUMN unit_label TEXT")
        conn.execute(
            """
            UPDATE metrics
            SET unit_kind = COALESCE(NULLIF(unit_kind, ''), 'other')
            """
        )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fx_rates (
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
        CREATE INDEX IF NOT EXISTS idx_fx_rates_pair_date
        ON fx_rates(provider, base_currency, quote_currency, rate_date DESC)
        """
    )


def _migration_027_add_currency_discovery_indexes(
    conn: sqlite3.Connection,
) -> None:
    """Add narrow currency indexes for FX refresh currency discovery."""

    existing_tables = {
        str(row[0])
        for row in conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            """
        ).fetchall()
    }
    if "supported_tickers" in existing_tables:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_supported_tickers_currency_nonnull
            ON supported_tickers(currency)
            WHERE currency IS NOT NULL
            """
        )
    if "financial_facts" in existing_tables:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fin_facts_currency_nonnull
            ON financial_facts(currency)
            WHERE currency IS NOT NULL
            """
        )
    if "market_data" in existing_tables:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_data_currency_nonnull
            ON market_data(currency)
            WHERE currency IS NOT NULL
            """
        )


def _migration_028_add_fx_catalog_tables(
    conn: sqlite3.Connection,
) -> None:
    """Add EODHD FX catalog and refresh coverage tables."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fx_supported_pairs (
            provider TEXT NOT NULL,
            symbol TEXT NOT NULL,
            canonical_symbol TEXT NOT NULL,
            base_currency TEXT,
            quote_currency TEXT,
            name TEXT,
            is_alias INTEGER NOT NULL DEFAULT 0,
            is_refreshable INTEGER NOT NULL DEFAULT 0,
            last_seen_at TEXT NOT NULL,
            PRIMARY KEY (provider, symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fx_supported_pairs_refreshable
        ON fx_supported_pairs(provider, is_refreshable, canonical_symbol)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fx_refresh_state (
            provider TEXT NOT NULL,
            canonical_symbol TEXT NOT NULL,
            min_rate_date TEXT,
            max_rate_date TEXT,
            full_history_backfilled INTEGER NOT NULL DEFAULT 0,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (provider, canonical_symbol)
        )
        """
    )


def _migration_029_add_fin_facts_security_concept_latest_index(
    conn: sqlite3.Connection,
) -> None:
    # The compute-metrics fact preload (storage.facts_for_symbols_many) issues
    # a query that pins this exact composite ordering via INDEXED BY. Without
    # this index that query would either fail outright or fall back to a
    # slower path. The index used to be created opportunistically inside
    # FinancialFactsRepository.initialize_schema(), which races with parallel
    # workers; promoting it here guarantees presence on every database that
    # already holds the financial_facts table. On older snapshots that have
    # not yet bootstrapped that table, FinancialFactsRepository's own schema
    # init will create the index alongside the table the first time the
    # repository is touched, so the migration is a no-op in that case.
    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='financial_facts'"
    ).fetchone()
    if not table_exists:
        return
    columns = _table_columns(conn, "financial_facts")
    key_column = "listing_id" if "listing_id" in columns else "security_id"
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_fin_facts_security_concept_latest
        ON financial_facts({key_column}, concept, end_date DESC, filed DESC)
        """
    )


def _migration_030_add_metric_compute_status_tables(
    conn: sqlite3.Connection,
) -> None:
    """Add latest metric-attempt status and financial-facts refresh state tables."""

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS financial_facts_refresh_state (
            security_id INTEGER NOT NULL PRIMARY KEY,
            refreshed_at TEXT NOT NULL
        )
        """
    )
    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='financial_facts'"
    ).fetchone()
    if table_exists:
        refresh_columns = _table_columns(conn, "financial_facts_refresh_state")
        fact_columns = _table_columns(conn, "financial_facts")
        key_column = "listing_id" if "listing_id" in fact_columns else "security_id"
        refresh_key_column = (
            "listing_id" if "listing_id" in refresh_columns else "security_id"
        )
        conn.execute(
            f"""
            INSERT INTO financial_facts_refresh_state (
                {refresh_key_column},
                refreshed_at
            )
            SELECT DISTINCT ff.{key_column}, ?
            FROM financial_facts ff
            WHERE NOT EXISTS (
                SELECT 1
                FROM financial_facts_refresh_state ffrs
                WHERE ffrs.{refresh_key_column} = ff.{key_column}
            )
            """,
            (now,),
        )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metric_compute_status (
            security_id INTEGER NOT NULL,
            metric_id TEXT NOT NULL,
            status TEXT NOT NULL,
            reason_code TEXT,
            reason_detail TEXT,
            attempted_at TEXT NOT NULL,
            value_as_of TEXT,
            facts_refreshed_at TEXT,
            market_data_as_of TEXT,
            market_data_updated_at TEXT,
            PRIMARY KEY (security_id, metric_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_metric_compute_status_metric_status
        ON metric_compute_status(metric_id, status)
        """
    )


def _migration_031_add_security_listing_status_table(
    conn: sqlite3.Connection,
) -> None:
    """Cache primary-vs-secondary listing classification from raw fundamentals."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS security_listing_status (
            security_id INTEGER NOT NULL PRIMARY KEY,
            source_provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            raw_fetched_at TEXT NOT NULL,
            is_primary_listing INTEGER NOT NULL CHECK (is_primary_listing IN (0, 1)),
            primary_provider_symbol TEXT,
            classification_basis TEXT NOT NULL CHECK (
                classification_basis IN (
                    'matched_primary_ticker',
                    'different_primary_ticker',
                    'missing_primary_ticker'
                )
            ),
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_security_listing_status_primary
        ON security_listing_status(is_primary_listing, security_id)
        """
    )


def _migration_032_create_providers_registry(conn: sqlite3.Connection) -> None:
    """Create and seed the provider registry."""

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS providers (
            provider_code TEXT NOT NULL PRIMARY KEY CHECK (
                provider_code = UPPER(TRIM(provider_code))
                AND LENGTH(TRIM(provider_code)) > 0
            ),
            display_name TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'active' CHECK (
                status IN ('active', 'deprecated', 'disabled')
            ),
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
        ON CONFLICT(provider_code) DO UPDATE SET
            display_name = excluded.display_name,
            description = excluded.description,
            status = excluded.status,
            updated_at = excluded.updated_at
        """,
        [
            (provider_code, display_name, description, status, now, now)
            for provider_code, display_name, description, status in _PROVIDER_REGISTRY_ROWS
        ],
    )


def _migration_033_split_exchange_catalog(conn: sqlite3.Connection) -> None:
    """Split supported_exchanges into canonical exchange and exchange_provider."""

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS "exchange" (
            exchange_id INTEGER PRIMARY KEY,
            exchange_code TEXT NOT NULL UNIQUE CHECK (
                exchange_code = UPPER(TRIM(exchange_code))
                AND LENGTH(TRIM(exchange_code)) > 0
            ),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS exchange_provider (
            provider TEXT NOT NULL,
            provider_exchange_code TEXT NOT NULL,
            exchange_id INTEGER NOT NULL,
            name TEXT,
            country TEXT,
            currency TEXT,
            operating_mic TEXT,
            country_iso2 TEXT,
            country_iso3 TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (provider, provider_exchange_code),
            FOREIGN KEY (provider) REFERENCES providers(provider_code),
            FOREIGN KEY (exchange_id) REFERENCES "exchange"(exchange_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_exchange_provider_exchange
        ON exchange_provider(exchange_id)
        """
    )

    if not _table_exists(conn, "supported_exchanges"):
        return

    rows = conn.execute(
        """
        SELECT
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
        FROM supported_exchanges
        ORDER BY provider, provider_exchange_code
        """
    ).fetchall()

    exchange_id_by_code: Dict[str, int] = {}
    canonical_rows = {
        _normalize_upper(row["canonical_exchange_code"]) or "": row
        for row in rows
        if _normalize_upper(row["canonical_exchange_code"]) is not None
    }
    for exchange_code, row in canonical_rows.items():
        exchange_timestamp = _normalize_optional_text(row["updated_at"]) or now
        conn.execute(
            """
            INSERT INTO "exchange" (
                exchange_code,
                created_at,
                updated_at
            ) VALUES (?, ?, ?)
            ON CONFLICT(exchange_code) DO UPDATE SET
                updated_at = excluded.updated_at
            """,
            (exchange_code, exchange_timestamp, exchange_timestamp),
        )
        exchange_row = conn.execute(
            """
            SELECT exchange_id
            FROM "exchange"
            WHERE exchange_code = ?
            """,
            (exchange_code,),
        ).fetchone()
        if exchange_row is None:
            raise RuntimeError(f"Failed to persist canonical exchange {exchange_code}")
        exchange_id_by_code[exchange_code] = int(exchange_row[0])

    conn.executemany(
        """
        INSERT INTO exchange_provider (
            provider,
            provider_exchange_code,
            exchange_id,
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
                _normalize_upper(row["provider"]),
                _normalize_upper(row["provider_exchange_code"]),
                exchange_id_by_code[
                    _normalize_upper(row["canonical_exchange_code"]) or ""
                ],
                _normalize_optional_text(row["name"]),
                _normalize_optional_text(row["country"]),
                _normalize_optional_text(row["currency"]),
                _normalize_optional_text(row["operating_mic"]),
                _normalize_optional_text(row["country_iso2"]),
                _normalize_optional_text(row["country_iso3"]),
                _normalize_optional_text(row["updated_at"]) or now,
            )
            for row in rows
        ],
    )

    conn.execute("DROP INDEX IF EXISTS idx_supported_exchanges_canonical")
    conn.execute("DROP TABLE IF EXISTS supported_exchanges")


def _migration_034_rename_catalog_identity_tables(conn: sqlite3.Connection) -> None:
    """Rename the catalog identity layer around provider/listing/provider_listing."""

    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider (
            provider_id INTEGER PRIMARY KEY,
            provider_code TEXT NOT NULL UNIQUE CHECK (
                provider_code = UPPER(TRIM(provider_code))
                AND LENGTH(TRIM(provider_code)) > 0
            ),
            display_name TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'active' CHECK (
                status IN ('active', 'deprecated', 'disabled')
            ),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS issuer (
            issuer_id INTEGER PRIMARY KEY,
            name TEXT,
            description TEXT,
            sector TEXT,
            industry TEXT,
            country TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS listing (
            listing_id INTEGER PRIMARY KEY,
            issuer_id INTEGER NOT NULL,
            exchange_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            currency TEXT,
            UNIQUE (exchange_id, symbol),
            FOREIGN KEY (issuer_id) REFERENCES issuer(issuer_id),
            FOREIGN KEY (exchange_id) REFERENCES "exchange"(exchange_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_listing_exchange
        ON listing(exchange_id)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_exchange (
            provider_exchange_id INTEGER PRIMARY KEY,
            provider_id INTEGER NOT NULL,
            provider_exchange_code TEXT NOT NULL,
            exchange_id INTEGER NOT NULL,
            name TEXT,
            country TEXT,
            currency TEXT,
            operating_mic TEXT,
            country_iso2 TEXT,
            country_iso3 TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE (provider_id, provider_exchange_code),
            UNIQUE (provider_exchange_id, provider_id),
            FOREIGN KEY (provider_id) REFERENCES provider(provider_id),
            FOREIGN KEY (exchange_id) REFERENCES "exchange"(exchange_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_provider_exchange_exchange
        ON provider_exchange(exchange_id)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_listing (
            provider_listing_id INTEGER PRIMARY KEY,
            provider_id INTEGER NOT NULL,
            provider_exchange_id INTEGER NOT NULL,
            provider_symbol TEXT NOT NULL,
            currency TEXT,
            listing_id INTEGER NOT NULL,
            UNIQUE (provider_exchange_id, provider_symbol),
            FOREIGN KEY (provider_id) REFERENCES provider(provider_id),
            FOREIGN KEY (provider_exchange_id) REFERENCES provider_exchange(provider_exchange_id),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id),
            FOREIGN KEY (provider_exchange_id, provider_id)
                REFERENCES provider_exchange(provider_exchange_id, provider_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_provider_listing_provider
        ON provider_listing(provider_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_provider_listing_listing
        ON provider_listing(listing_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_provider_listing_currency_nonnull
        ON provider_listing(currency)
        WHERE currency IS NOT NULL
        """
    )

    provider_rows = []
    if _table_exists(conn, "providers"):
        provider_rows = conn.execute(
            """
            SELECT
                provider_code,
                display_name,
                description,
                status,
                created_at,
                updated_at
            FROM providers
            ORDER BY provider_code
            """
        ).fetchall()
    if provider_rows:
        conn.executemany(
            """
            INSERT INTO provider (
                provider_code,
                display_name,
                description,
                status,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider_code) DO UPDATE SET
                display_name = excluded.display_name,
                description = excluded.description,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            [
                (
                    _normalize_upper(row["provider_code"]),
                    _normalize_optional_text(row["display_name"]) or "",
                    _normalize_optional_text(row["description"]),
                    _normalize_optional_text(row["status"]) or "active",
                    _normalize_optional_text(row["created_at"]) or now,
                    _normalize_optional_text(row["updated_at"]) or now,
                )
                for row in provider_rows
            ],
        )
    else:
        conn.executemany(
            """
            INSERT INTO provider (
                provider_code,
                display_name,
                description,
                status,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider_code) DO UPDATE SET
                display_name = excluded.display_name,
                description = excluded.description,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            [
                (provider_code, display_name, description, status, now, now)
                for provider_code, display_name, description, status in _PROVIDER_REGISTRY_ROWS
            ],
        )

    provider_id_by_code = {
        str(row["provider_code"]): int(row["provider_id"])
        for row in conn.execute(
            """
            SELECT provider_id, provider_code
            FROM provider
            """
        ).fetchall()
    }

    if _table_exists(conn, "exchange_provider"):
        exchange_provider_rows = conn.execute(
            """
            SELECT
                provider,
                provider_exchange_code,
                exchange_id,
                name,
                country,
                currency,
                operating_mic,
                country_iso2,
                country_iso3,
                updated_at
            FROM exchange_provider
            ORDER BY provider, provider_exchange_code
            """
        ).fetchall()
        conn.executemany(
            """
            INSERT INTO provider_exchange (
                provider_id,
                provider_exchange_code,
                exchange_id,
                name,
                country,
                currency,
                operating_mic,
                country_iso2,
                country_iso3,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider_id, provider_exchange_code) DO UPDATE SET
                exchange_id = excluded.exchange_id,
                name = excluded.name,
                country = excluded.country,
                currency = excluded.currency,
                operating_mic = excluded.operating_mic,
                country_iso2 = excluded.country_iso2,
                country_iso3 = excluded.country_iso3,
                updated_at = excluded.updated_at
            """,
            [
                (
                    provider_id_by_code[_normalize_upper(row["provider"]) or ""],
                    _normalize_upper(row["provider_exchange_code"]),
                    int(row["exchange_id"]),
                    _normalize_optional_text(row["name"]),
                    _normalize_optional_text(row["country"]),
                    _normalize_optional_text(row["currency"]),
                    _normalize_optional_text(row["operating_mic"]),
                    _normalize_optional_text(row["country_iso2"]),
                    _normalize_optional_text(row["country_iso3"]),
                    _normalize_optional_text(row["updated_at"]) or now,
                )
                for row in exchange_provider_rows
                if _normalize_upper(row["provider"]) in provider_id_by_code
                and _normalize_upper(row["provider_exchange_code"]) is not None
            ],
        )

    if _table_exists(conn, "securities"):
        security_catalog_rows = {}
        supported_ticker_columns = _table_columns(conn, "supported_tickers")
        if supported_ticker_columns and "security_id" in supported_ticker_columns:
            country_expr = (
                "country" if "country" in supported_ticker_columns else "NULL"
            )
            currency_expr = (
                "currency" if "currency" in supported_ticker_columns else "NULL"
            )
            security_catalog_rows = {
                int(row["security_id"]): {
                    "country": _normalize_optional_text(row["country"]),
                    "currency": _normalize_upper(row["currency"]),
                }
                for row in conn.execute(
                    f"""
                    SELECT
                        security_id,
                        MAX({country_expr}) AS country,
                        MAX({currency_expr}) AS currency
                    FROM supported_tickers
                    GROUP BY security_id
                    """
                ).fetchall()
            }

        securities_rows = conn.execute(
            """
            SELECT
                security_id,
                canonical_ticker,
                canonical_exchange_code,
                entity_name,
                description,
                sector,
                industry
            FROM securities
            ORDER BY security_id
            """
        ).fetchall()
        conn.executemany(
            """
            INSERT INTO issuer (
                issuer_id,
                name,
                description,
                sector,
                industry,
                country
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(issuer_id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                sector = excluded.sector,
                industry = excluded.industry,
                country = excluded.country
            """,
            [
                (
                    int(row["security_id"]),
                    _normalize_optional_text(row["entity_name"]),
                    _normalize_optional_text(row["description"]),
                    _normalize_optional_text(row["sector"]),
                    _normalize_optional_text(row["industry"]),
                    security_catalog_rows.get(int(row["security_id"]), {}).get(
                        "country"
                    ),
                )
                for row in securities_rows
            ],
        )
        exchange_codes = {
            exchange_code
            for exchange_code in (
                _normalize_upper(row["canonical_exchange_code"])
                for row in securities_rows
            )
            if exchange_code is not None
        }
        conn.executemany(
            """
            INSERT INTO "exchange" (
                exchange_code,
                created_at,
                updated_at
            ) VALUES (?, ?, ?)
            ON CONFLICT(exchange_code) DO UPDATE SET
                updated_at = excluded.updated_at
            """,
            [(exchange_code, now, now) for exchange_code in sorted(exchange_codes)],
        )
        exchange_id_by_code = {
            str(row["exchange_code"]): int(row["exchange_id"])
            for row in conn.execute(
                """
                SELECT exchange_id, exchange_code
                FROM "exchange"
                """
            ).fetchall()
        }
        conn.executemany(
            """
            INSERT INTO listing (
                listing_id,
                issuer_id,
                exchange_id,
                symbol,
                currency
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(listing_id) DO UPDATE SET
                issuer_id = excluded.issuer_id,
                exchange_id = excluded.exchange_id,
                symbol = excluded.symbol,
                currency = excluded.currency
            """,
            [
                (
                    int(row["security_id"]),
                    int(row["security_id"]),
                    exchange_id_by_code[
                        _normalize_upper(row["canonical_exchange_code"]) or ""
                    ],
                    _normalize_upper(row["canonical_ticker"]),
                    security_catalog_rows.get(int(row["security_id"]), {}).get(
                        "currency"
                    ),
                )
                for row in securities_rows
                if _normalize_upper(row["canonical_exchange_code"])
                in exchange_id_by_code
            ],
        )

    provider_exchange_id_by_key = {
        (int(row["provider_id"]), str(row["provider_exchange_code"])): int(
            row["provider_exchange_id"]
        )
        for row in conn.execute(
            """
            SELECT
                provider_exchange_id,
                provider_id,
                provider_exchange_code
            FROM provider_exchange
            """
        ).fetchall()
    }

    def _ensure_provider_exchange_mapping(
        provider_id: int,
        provider_exchange_code: str,
    ) -> Optional[int]:
        key = (provider_id, provider_exchange_code)
        provider_exchange_id = provider_exchange_id_by_key.get(key)
        if provider_exchange_id is not None:
            return provider_exchange_id
        exchange_code = provider_exchange_code
        exchange_row = conn.execute(
            """
            SELECT exchange_id
            FROM "exchange"
            WHERE exchange_code = ?
            """,
            (exchange_code,),
        ).fetchone()
        if exchange_row is None:
            cursor = conn.execute(
                """
                INSERT INTO "exchange" (
                    exchange_code,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?)
                """,
                (exchange_code, now, now),
            )
            if cursor.lastrowid is None:
                raise RuntimeError(f"Failed to create exchange {exchange_code}")
            exchange_id = int(cursor.lastrowid)
        else:
            exchange_id = int(exchange_row["exchange_id"])
        cursor = conn.execute(
            """
            INSERT INTO provider_exchange (
                provider_id,
                provider_exchange_code,
                exchange_id,
                name,
                country,
                currency,
                operating_mic,
                country_iso2,
                country_iso3,
                updated_at
            ) VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?)
            ON CONFLICT(provider_id, provider_exchange_code) DO UPDATE SET
                exchange_id = excluded.exchange_id,
                updated_at = excluded.updated_at
            """,
            (provider_id, provider_exchange_code, exchange_id, now),
        )
        provider_exchange_id = int(cursor.lastrowid or 0)
        if provider_exchange_id == 0:
            row = conn.execute(
                """
                SELECT provider_exchange_id
                FROM provider_exchange
                WHERE provider_id = ? AND provider_exchange_code = ?
                """,
                (provider_id, provider_exchange_code),
            ).fetchone()
            provider_exchange_id = int(row["provider_exchange_id"]) if row else 0
        if provider_exchange_id:
            provider_exchange_id_by_key[key] = provider_exchange_id
            return provider_exchange_id
        return None

    if _table_exists(conn, "supported_tickers"):
        supported_ticker_columns = _table_columns(conn, "supported_tickers")
        provider_symbol_expr = (
            "provider_symbol"
            if "provider_symbol" in supported_ticker_columns
            else "symbol"
            if "symbol" in supported_ticker_columns
            else "code"
            if "code" in supported_ticker_columns
            else "NULL"
        )
        provider_ticker_expr = (
            "provider_ticker"
            if "provider_ticker" in supported_ticker_columns
            else "code"
            if "code" in supported_ticker_columns
            else "NULL"
        )
        provider_exchange_expr = (
            "provider_exchange_code"
            if "provider_exchange_code" in supported_ticker_columns
            else "exchange_code"
            if "exchange_code" in supported_ticker_columns
            else "NULL"
        )
        security_id_expr = (
            "security_id" if "security_id" in supported_ticker_columns else "NULL"
        )
        currency_expr = "currency" if "currency" in supported_ticker_columns else "NULL"
        provider_listing_rows = conn.execute(
            f"""
            SELECT
                provider,
                {provider_symbol_expr} AS provider_symbol,
                {provider_ticker_expr} AS provider_ticker,
                {provider_exchange_expr} AS provider_exchange_code,
                {security_id_expr} AS security_id,
                {currency_expr} AS currency
            FROM supported_tickers
            ORDER BY provider, provider_exchange_code, provider_ticker, provider_symbol
            """
        ).fetchall()
        payload = []
        for row in provider_listing_rows:
            provider_code = _normalize_upper(row["provider"])
            provider_symbol_text = _normalize_upper(row["provider_symbol"]) or ""
            provider_exchange_code = _normalize_upper(row["provider_exchange_code"])
            if provider_exchange_code is None:
                provider_exchange_code = _infer_canonical_exchange(provider_symbol_text)
            if provider_code == "SEC":
                provider_exchange_code = "US"
            if provider_code is None or provider_exchange_code is None:
                continue
            provider_id = provider_id_by_code.get(provider_code)
            if provider_id is None:
                continue
            provider_exchange_id = _ensure_provider_exchange_mapping(
                provider_id,
                provider_exchange_code,
            )
            if provider_exchange_id is None:
                continue
            bare_symbol = _normalize_upper(row["provider_ticker"])
            if bare_symbol is None:
                if provider_symbol_text.endswith(f".{provider_exchange_code}"):
                    bare_symbol = provider_symbol_text[
                        : -(len(provider_exchange_code) + 1)
                    ]
                else:
                    bare_symbol, _ = _split_symbol(provider_symbol_text)
            if not bare_symbol:
                continue
            listing_id = row["security_id"]
            if listing_id is None:
                listing_row = conn.execute(
                    """
                    SELECT l.listing_id
                    FROM listing l
                    JOIN "exchange" e ON e.exchange_id = l.exchange_id
                    WHERE e.exchange_code = ? AND l.symbol = ?
                    """,
                    (provider_exchange_code, bare_symbol),
                ).fetchone()
                if listing_row is None:
                    continue
                listing_id = listing_row["listing_id"]
            payload.append(
                (
                    provider_id,
                    provider_exchange_id,
                    bare_symbol,
                    _normalize_upper(row["currency"]),
                    int(listing_id),
                )
            )
        conn.executemany(
            """
            INSERT INTO provider_listing (
                provider_id,
                provider_exchange_id,
                provider_symbol,
                currency,
                listing_id
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(provider_exchange_id, provider_symbol) DO UPDATE SET
                currency = COALESCE(excluded.currency, provider_listing.currency),
                listing_id = excluded.listing_id
            """,
            payload,
        )

    def _ensure_provider_scoped_table(
        table_name: str,
        ddl: str,
        index_ddls: Sequence[str],
        legacy_insert_sql: Optional[str] = None,
    ) -> None:
        columns = _table_columns(conn, table_name)
        if not columns:
            conn.execute(ddl)
            for index_ddl in index_ddls:
                conn.execute(index_ddl)
            return
        if "provider_listing_id" in columns or (
            table_name == "security_listing_status" and "listing_id" in columns
        ):
            return

        temp_table = f"{table_name}__new"
        conn.execute(ddl.replace(table_name, temp_table, 1))
        if (
            legacy_insert_sql is not None
            and _table_exists(conn, "supported_tickers")
            and "provider_ticker" in _table_columns(conn, "supported_tickers")
        ):
            conn.execute(legacy_insert_sql.replace(table_name, temp_table, 1))
        conn.execute(f"DROP TABLE {table_name}")
        conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
        for index_ddl in index_ddls:
            conn.execute(index_ddl)

    def _ensure_listing_rooted_table(
        table_name: str,
        ddl: str,
        index_ddls: Sequence[str],
        insert_select_sql: Optional[str] = None,
    ) -> None:
        columns = _table_columns(conn, table_name)
        if not columns:
            conn.execute(ddl)
            for index_ddl in index_ddls:
                conn.execute(index_ddl)
            return
        if "listing_id" in columns:
            return

        temp_table = f"{table_name}__new"
        conn.execute(ddl.replace(table_name, temp_table, 1))
        if insert_select_sql is not None:
            conn.execute(insert_select_sql.replace(table_name, temp_table, 1))
        conn.execute(f"DROP TABLE {table_name}")
        conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
        for index_ddl in index_ddls:
            conn.execute(index_ddl)

    _ensure_provider_scoped_table(
        "fundamentals_raw",
        """
        CREATE TABLE fundamentals_raw (
            payload_id INTEGER PRIMARY KEY,
            provider_listing_id INTEGER NOT NULL UNIQUE,
            listing_id INTEGER NOT NULL,
            currency TEXT,
            data TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        )
        """,
        [
            """
            CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_security
            ON fundamentals_raw(listing_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_provider_fetched
            ON fundamentals_raw(fetched_at)
            """,
        ],
        legacy_insert_sql="""
        INSERT INTO fundamentals_raw (
            provider_listing_id,
            listing_id,
            currency,
            data,
            fetched_at
        )
        SELECT
            pl.provider_listing_id,
            pl.listing_id,
            fr.currency,
            fr.data,
            fr.fetched_at
        FROM fundamentals_raw fr
        JOIN supported_tickers st
          ON st.provider = fr.provider
         AND st.provider_symbol = fr.provider_symbol
        JOIN provider p
          ON p.provider_code = st.provider
        JOIN provider_exchange px
          ON px.provider_id = p.provider_id
         AND px.provider_exchange_code = st.provider_exchange_code
        JOIN provider_listing pl
          ON pl.provider_id = p.provider_id
         AND pl.provider_exchange_id = px.provider_exchange_id
         AND pl.provider_symbol = st.provider_ticker
        """,
    )
    _ensure_provider_scoped_table(
        "fundamentals_fetch_state",
        """
        CREATE TABLE fundamentals_fetch_state (
            provider_listing_id INTEGER NOT NULL PRIMARY KEY,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            next_eligible_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
        )
        """,
        [
            """
            CREATE INDEX IF NOT EXISTS idx_fundamentals_fetch_next
            ON fundamentals_fetch_state(next_eligible_at)
            """,
        ],
        legacy_insert_sql="""
        INSERT INTO fundamentals_fetch_state (
            provider_listing_id,
            last_fetched_at,
            last_status,
            last_error,
            next_eligible_at,
            attempts
        )
        SELECT
            pl.provider_listing_id,
            fs.last_fetched_at,
            fs.last_status,
            fs.last_error,
            fs.next_eligible_at,
            fs.attempts
        FROM fundamentals_fetch_state fs
        JOIN supported_tickers st
          ON st.provider = fs.provider
         AND st.provider_symbol = fs.provider_symbol
        JOIN provider p
          ON p.provider_code = st.provider
        JOIN provider_exchange px
          ON px.provider_id = p.provider_id
         AND px.provider_exchange_code = st.provider_exchange_code
        JOIN provider_listing pl
          ON pl.provider_id = p.provider_id
         AND pl.provider_exchange_id = px.provider_exchange_id
         AND pl.provider_symbol = st.provider_ticker
        """,
    )
    _ensure_provider_scoped_table(
        "market_data_fetch_state",
        """
        CREATE TABLE market_data_fetch_state (
            provider_listing_id INTEGER NOT NULL PRIMARY KEY,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            next_eligible_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
        )
        """,
        [
            """
            CREATE INDEX IF NOT EXISTS idx_market_data_fetch_next
            ON market_data_fetch_state(next_eligible_at)
            """,
        ],
        legacy_insert_sql="""
        INSERT INTO market_data_fetch_state (
            provider_listing_id,
            last_fetched_at,
            last_status,
            last_error,
            next_eligible_at,
            attempts
        )
        SELECT
            pl.provider_listing_id,
            fs.last_fetched_at,
            fs.last_status,
            fs.last_error,
            fs.next_eligible_at,
            fs.attempts
        FROM market_data_fetch_state fs
        JOIN supported_tickers st
          ON st.provider = fs.provider
         AND st.provider_symbol = fs.provider_symbol
        JOIN provider p
          ON p.provider_code = st.provider
        JOIN provider_exchange px
          ON px.provider_id = p.provider_id
         AND px.provider_exchange_code = st.provider_exchange_code
        JOIN provider_listing pl
          ON pl.provider_id = p.provider_id
         AND pl.provider_exchange_id = px.provider_exchange_id
         AND pl.provider_symbol = st.provider_ticker
        """,
    )
    _ensure_provider_scoped_table(
        "fundamentals_normalization_state",
        """
        CREATE TABLE fundamentals_normalization_state (
            provider_listing_id INTEGER NOT NULL PRIMARY KEY,
            listing_id INTEGER NOT NULL,
            raw_fetched_at TEXT NOT NULL,
            last_normalized_at TEXT NOT NULL,
            FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        )
        """,
        [
            """
            CREATE INDEX IF NOT EXISTS idx_fundamentals_norm_state_security
            ON fundamentals_normalization_state(listing_id)
            """,
        ],
        legacy_insert_sql="""
        INSERT INTO fundamentals_normalization_state (
            provider_listing_id,
            listing_id,
            raw_fetched_at,
            last_normalized_at
        )
        SELECT
            pl.provider_listing_id,
            pl.listing_id,
            ns.raw_fetched_at,
            ns.last_normalized_at
        FROM fundamentals_normalization_state ns
        JOIN supported_tickers st
          ON st.provider = ns.provider
         AND st.provider_symbol = ns.provider_symbol
        JOIN provider p
          ON p.provider_code = st.provider
        JOIN provider_exchange px
          ON px.provider_id = p.provider_id
         AND px.provider_exchange_code = st.provider_exchange_code
        JOIN provider_listing pl
          ON pl.provider_id = p.provider_id
         AND pl.provider_exchange_id = px.provider_exchange_id
         AND pl.provider_symbol = st.provider_ticker
        """,
    )
    _ensure_provider_scoped_table(
        "security_listing_status",
        """
        CREATE TABLE security_listing_status (
            listing_id INTEGER NOT NULL PRIMARY KEY,
            source_provider TEXT NOT NULL,
            provider_listing_id INTEGER NOT NULL,
            raw_fetched_at TEXT NOT NULL,
            is_primary_listing INTEGER NOT NULL CHECK (is_primary_listing IN (0, 1)),
            primary_provider_listing_id INTEGER,
            classification_basis TEXT NOT NULL CHECK (
                classification_basis IN (
                    'matched_primary_ticker',
                    'different_primary_ticker',
                    'missing_primary_ticker'
                )
            ),
            updated_at TEXT NOT NULL,
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id),
            FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id),
            FOREIGN KEY (primary_provider_listing_id) REFERENCES provider_listing(provider_listing_id)
        )
        """,
        [
            """
            CREATE INDEX IF NOT EXISTS idx_security_listing_status_primary
            ON security_listing_status(is_primary_listing, listing_id)
            """,
        ],
        legacy_insert_sql="""
        INSERT INTO security_listing_status (
            listing_id,
            source_provider,
            provider_listing_id,
            raw_fetched_at,
            is_primary_listing,
            primary_provider_listing_id,
            classification_basis,
            updated_at
        )
        SELECT
            sls.security_id,
            sls.source_provider,
            pl.provider_listing_id,
            sls.raw_fetched_at,
            sls.is_primary_listing,
            primary_pl.provider_listing_id,
            sls.classification_basis,
            sls.updated_at
        FROM security_listing_status sls
        JOIN supported_tickers st
          ON st.provider = sls.source_provider
         AND st.provider_symbol = sls.provider_symbol
        JOIN provider p
          ON p.provider_code = st.provider
        JOIN provider_exchange px
          ON px.provider_id = p.provider_id
         AND px.provider_exchange_code = st.provider_exchange_code
        JOIN provider_listing pl
          ON pl.provider_id = p.provider_id
         AND pl.provider_exchange_id = px.provider_exchange_id
         AND pl.provider_symbol = st.provider_ticker
        LEFT JOIN supported_tickers primary_st
          ON primary_st.provider = sls.source_provider
         AND primary_st.provider_symbol = sls.primary_provider_symbol
        LEFT JOIN provider primary_p
          ON primary_p.provider_code = primary_st.provider
        LEFT JOIN provider_exchange primary_px
          ON primary_px.provider_id = primary_p.provider_id
         AND primary_px.provider_exchange_code = primary_st.provider_exchange_code
        LEFT JOIN provider_listing primary_pl
          ON primary_pl.provider_id = primary_p.provider_id
         AND primary_pl.provider_exchange_id = primary_px.provider_exchange_id
         AND primary_pl.provider_symbol = primary_st.provider_ticker
        """,
    )
    _ensure_listing_rooted_table(
        "financial_facts_refresh_state",
        """
        CREATE TABLE financial_facts_refresh_state (
            listing_id INTEGER NOT NULL PRIMARY KEY,
            refreshed_at TEXT NOT NULL
        )
        """,
        [],
        insert_select_sql="""
        INSERT INTO financial_facts_refresh_state (
            listing_id,
            refreshed_at
        )
        SELECT
            security_id,
            refreshed_at
        FROM financial_facts_refresh_state
        """,
    )
    _ensure_listing_rooted_table(
        "financial_facts",
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
        """,
        [
            """
            CREATE INDEX IF NOT EXISTS idx_fin_facts_security_concept
            ON financial_facts(listing_id, concept)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_fin_facts_concept
            ON financial_facts(concept)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_fin_facts_security_concept_latest
            ON financial_facts(listing_id, concept, end_date DESC, filed DESC)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_fin_facts_currency_nonnull
            ON financial_facts(currency)
            WHERE currency IS NOT NULL
            """,
        ],
        insert_select_sql="""
        INSERT INTO financial_facts (
            listing_id,
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
            currency,
            source_provider
        )
        SELECT
            security_id,
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
            currency,
            source_provider
        FROM financial_facts
        """,
    )
    market_data_columns = _table_columns(conn, "market_data")
    market_volume_expr = "volume" if "volume" in market_data_columns else "NULL"
    market_cap_expr = "market_cap" if "market_cap" in market_data_columns else "NULL"
    market_currency_expr = "currency" if "currency" in market_data_columns else "NULL"
    _ensure_listing_rooted_table(
        "market_data",
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
        """,
        [
            """
            CREATE INDEX IF NOT EXISTS idx_market_data_latest
            ON market_data(listing_id, as_of DESC)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_market_data_currency_nonnull
            ON market_data(currency)
            WHERE currency IS NOT NULL
            """,
        ],
        insert_select_sql=f"""
        INSERT INTO market_data (
            listing_id,
            as_of,
            price,
            volume,
            market_cap,
            currency,
            source_provider,
            updated_at
        )
        SELECT
            security_id,
            as_of,
            price,
            {market_volume_expr},
            {market_cap_expr},
            {market_currency_expr},
            source_provider,
            updated_at
        FROM market_data
        """,
    )
    _ensure_listing_rooted_table(
        "metrics",
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
        """,
        [
            """
            CREATE INDEX IF NOT EXISTS idx_metrics_metric_id
            ON metrics(metric_id)
            """,
        ],
        insert_select_sql="""
        INSERT INTO metrics (
            listing_id,
            metric_id,
            value,
            as_of,
            unit_kind,
            currency,
            unit_label
        )
        SELECT
            security_id,
            metric_id,
            value,
            as_of,
            unit_kind,
            currency,
            unit_label
        FROM metrics
        """,
    )
    _ensure_listing_rooted_table(
        "metric_compute_status",
        """
        CREATE TABLE metric_compute_status (
            listing_id INTEGER NOT NULL,
            metric_id TEXT NOT NULL,
            status TEXT NOT NULL,
            reason_code TEXT,
            reason_detail TEXT,
            attempted_at TEXT NOT NULL,
            value_as_of TEXT,
            facts_refreshed_at TEXT,
            market_data_as_of TEXT,
            market_data_updated_at TEXT,
            PRIMARY KEY (listing_id, metric_id)
        )
        """,
        [
            """
            CREATE INDEX IF NOT EXISTS idx_metric_compute_status_metric_status
            ON metric_compute_status(metric_id, status)
            """,
        ],
        insert_select_sql="""
        INSERT INTO metric_compute_status (
            listing_id,
            metric_id,
            status,
            reason_code,
            reason_detail,
            attempted_at,
            value_as_of,
            facts_refreshed_at,
            market_data_as_of,
            market_data_updated_at
        )
        SELECT
            security_id,
            metric_id,
            status,
            reason_code,
            reason_detail,
            attempted_at,
            value_as_of,
            facts_refreshed_at,
            market_data_as_of,
            market_data_updated_at
        FROM metric_compute_status
        """,
    )

    for legacy_name in (
        "exchange_provider",
        "providers",
        "supported_tickers",
        "securities",
    ):
        legacy_row = conn.execute(
            """
            SELECT type
            FROM sqlite_master
            WHERE name = ?
              AND type IN ('table', 'view')
            """,
            (legacy_name,),
        ).fetchone()
        if legacy_row is None:
            continue
        if str(legacy_row["type"]) == "view":
            conn.execute(f"DROP VIEW {legacy_name}")
        else:
            conn.execute(f"DROP TABLE {legacy_name}")


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
    _migration_018_create_supported_exchanges,
    _migration_019_create_supported_tickers,
    _migration_020_create_market_data_fetch_state,
    _migration_021_drop_listings_in_favor_of_supported_tickers,
    _migration_022_canonical_security_model,
    _migration_023_optimize_fundamentals_hot_paths,
    _migration_024_create_fundamentals_normalization_state,
    _migration_025_add_sector_industry_to_securities,
    _migration_026_add_fx_rates_and_metric_metadata,
    _migration_027_add_currency_discovery_indexes,
    _migration_028_add_fx_catalog_tables,
    _migration_029_add_fin_facts_security_concept_latest_index,
    _migration_030_add_metric_compute_status_tables,
    _migration_031_add_security_listing_status_table,
    _migration_032_create_providers_registry,
    _migration_033_split_exchange_catalog,
    _migration_034_rename_catalog_identity_tables,
]


__all__ = ["apply_migrations", "MIGRATIONS"]
