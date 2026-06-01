"""Lightweight SQLite schema migration runner.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
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

_PROVIDER_REGISTRY_ROWS: Tuple[Tuple[str, str, Optional[str]], ...] = (
    (
        "EODHD",
        "EOD Historical Data",
        "Exchange, fundamentals, market-data, and FX provider.",
    ),
    (
        "SEC",
        "US SEC Company Facts",
        "US issuer fundamentals provider backed by SEC company facts.",
    ),
    (
        "FRANKFURTER",
        "Frankfurter FX",
        "FX rates provider used for direct currency history refreshes.",
    ),
)


def _ensure_migrations_table(conn: sqlite3.Connection) -> None:
    """Create the ``schema_migrations`` table on a fresh DB.

    Fresh DBs land directly at the audit P3 #12 shape — a single row
    pinned to ``id = 1`` so duplicate version markers can't accumulate.
    Existing DBs running the older ``CREATE TABLE schema_migrations
    (version INTEGER NOT NULL)`` shape are left untouched here (the
    ``IF NOT EXISTS`` clause no-ops) and migrated by
    ``_migration_063_schema_migrations_pk_and_guard``.

    ``_set_version`` works against either shape — its
    ``INSERT INTO schema_migrations (version) VALUES (?)`` form lets
    SQLite auto-assign the ``id`` rowid (which is ``1`` for an empty
    table, satisfying the CHECK).
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY CHECK (id = 1),
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
    # The DELETE + INSERT pattern works for both the legacy
    # (version INTEGER NOT NULL) shape and the post-063 single-row
    # shape: the new shape's ``id`` is an INTEGER PRIMARY KEY rowid
    # alias, so an INSERT that omits it auto-picks ``id = 1`` on an
    # empty table, which the CHECK accepts.
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


def _canonical_json_hash(data: object) -> str:
    text = "" if data is None else str(data)
    try:
        canonical = json.dumps(
            json.loads(text),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
    except (TypeError, ValueError):
        canonical = text
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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


def apply_migrations(
    db_path: Union[str, Path], *, target_version: Optional[int] = None
) -> int:
    """Apply pending migrations in order.

    Returns the number of migrations applied. Safe to call repeatedly;
    no-op when already up-to-date. Each migration runs inside its own
    transaction and rolls back on error.

    ``target_version`` caps the highest migration applied (default: the
    latest, ``len(MIGRATIONS)``). It exists so a single migration can be
    exercised in isolation — applying *through* version N without later
    migrations running. This matters because some later migrations are
    destructive to earlier state (e.g. migration 071 rebuilds
    ``financial_facts`` empty), which would otherwise erase the very rows a
    migration-N regression test asserts on. Production callers omit it and
    always migrate to head. Values are clamped to ``[0, len(MIGRATIONS)]``.

    Foreign-key enforcement is disabled for the duration of the
    migration session and re-enabled afterwards. The SQLite manual
    (https://www.sqlite.org/lang_altertable.html, "Making Other Kinds
    Of Table Schema Changes") recommends this pattern for any rebuild
    that drops a parent table while child tables retain rows: with
    enforcement on, ``DROP TABLE parent`` poisons the deferred-FK queue
    even when the rename immediately restores the parent's name and
    every row remains reachable, and ``COMMIT`` then fails with
    "FOREIGN KEY constraint failed" despite ``PRAGMA foreign_key_check``
    returning empty.

    A single ``PRAGMA foreign_key_check`` runs at the end of the
    session — running it after every migration is correct but cripples
    large-DB applies: each invocation scans every FK in every child
    table, which on the live production DB (~67 GiB, 103M financial
    facts) is dominated by I/O and adds ~3 minutes per migration.
    Running the check once at the end keeps the safety net (a violation
    still aborts before re-enabling FK enforcement) without paying the
    per-migration tax. If a specific migration must be bisected, run
    ``apply_migrations`` against a copy and add a debug check after the
    failing version.
    """

    db_path = Path(db_path)
    if db_path.parent:
        db_path.parent.mkdir(parents=True, exist_ok=True)

    applied = 0
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        # Off for the duration of migrations; re-enabled before close.
        conn.execute("PRAGMA foreign_keys = OFF")
        current = _current_version(conn)
        target = len(MIGRATIONS) if target_version is None else target_version
        target = max(0, min(target, len(MIGRATIONS)))
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

        # Single end-of-session integrity gate.
        if applied:
            violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise RuntimeError(
                    "post-migration foreign_key_check reported "
                    f"violations: {violations!r}"
                )
        conn.execute("PRAGMA foreign_keys = ON")
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
    if "market_data" in existing_tables and "currency" in _table_columns(
        conn, "market_data"
    ):
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
    # Migration 044 (later in the chain) creates a `providers` VIEW over the
    # renamed `provider` table. If migrations are replayed (a test rewinds
    # schema_version below 32 after a head-of-tree run, for instance), the
    # view from 044 is still in the database when this migration retries.
    # CREATE TABLE IF NOT EXISTS would silently no-op against the view and
    # the subsequent INSERT would fail with "cannot modify providers because
    # it is a view". Drop any conflicting view first so the table-shaped
    # provider registry can be (re-)created cleanly. On a fresh forward run
    # the DROP is a no-op.
    conn.execute("DROP VIEW IF EXISTS providers")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS providers (
            provider_code TEXT NOT NULL PRIMARY KEY CHECK (
                provider_code = UPPER(TRIM(provider_code))
                AND LENGTH(TRIM(provider_code)) > 0
            ),
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
        ON CONFLICT(provider_code) DO UPDATE SET
            display_name = excluded.display_name,
            description = excluded.description,
            updated_at = excluded.updated_at
        """,
        [
            (provider_code, display_name, description, now, now)
            for provider_code, display_name, description in _PROVIDER_REGISTRY_ROWS
        ],
    )


def _migration_033_split_exchange_catalog(conn: sqlite3.Connection) -> None:
    """Split supported_exchanges into canonical exchange and exchange_provider."""

    now = datetime.now(timezone.utc).isoformat()
    # See migration 032 for the rationale. Migration 044 creates an
    # ``exchange_provider`` VIEW that conflicts with the table-shape this
    # migration installs; dropping the view first lets a replay re-create
    # the table cleanly. On a fresh forward run the DROP is a no-op.
    conn.execute("DROP VIEW IF EXISTS exchange_provider")
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
    # ``idx_provider_listing_provider`` is only created when the
    # ``provider_id`` column is still on the table. Migration 054 drops
    # the column; a test harness that rewinds ``schema_migrations.version``
    # below 034 and re-applies migrations would otherwise hit
    # "no such column: provider_id" here even though the table already
    # has its post-054 shape.
    if "provider_id" in _table_columns(conn, "provider_listing"):
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
    provider_listing_has_currency = "currency" in _table_columns(
        conn, "provider_listing"
    )
    if provider_listing_has_currency:
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
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(provider_code) DO UPDATE SET
                display_name = excluded.display_name,
                description = excluded.description,
                updated_at = excluded.updated_at
            """,
            [
                (
                    _normalize_upper(row["provider_code"]),
                    _normalize_optional_text(row["display_name"]) or "",
                    _normalize_optional_text(row["description"]),
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
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(provider_code) DO UPDATE SET
                display_name = excluded.display_name,
                description = excluded.description,
                updated_at = excluded.updated_at
            """,
            [
                (provider_code, display_name, description, now, now)
                for provider_code, display_name, description in _PROVIDER_REGISTRY_ROWS
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
        if provider_listing_has_currency:
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
        else:
            conn.executemany(
                """
                UPDATE listing
                SET currency = COALESCE(?, currency)
                WHERE listing_id = ?
                """,
                [(currency, listing_id) for *_, currency, listing_id in payload],
            )
            conn.executemany(
                """
                INSERT INTO provider_listing (
                    provider_id,
                    provider_exchange_id,
                    provider_symbol,
                    listing_id
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(provider_exchange_id, provider_symbol) DO UPDATE SET
                    listing_id = excluded.listing_id
                """,
                [
                    (provider_id, provider_exchange_id, bare_symbol, listing_id)
                    for (
                        provider_id,
                        provider_exchange_id,
                        bare_symbol,
                        _currency,
                        listing_id,
                    ) in payload
                ],
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


def _migration_035_drop_provider_status(conn: sqlite3.Connection) -> None:
    """Drop the unused provider lifecycle status column."""

    if not _table_exists(conn, "provider"):
        return
    if "status" not in _table_columns(conn, "provider"):
        return
    conn.execute("DROP VIEW IF EXISTS providers")
    conn.execute("ALTER TABLE provider DROP COLUMN status")


def _migration_036_drop_fundamentals_raw_listing_columns(
    conn: sqlite3.Connection,
) -> None:
    """Remove denormalized listing identity columns from raw fundamentals."""

    columns = _table_columns(conn, "fundamentals_raw")
    if not columns:
        return

    target_columns = {
        "payload_id",
        "provider_listing_id",
        "currency",
        "data",
        "fetched_at",
    }
    conn.execute("DROP INDEX IF EXISTS idx_fundamentals_raw_security")
    conn.execute("DROP INDEX IF EXISTS idx_fundamentals_raw_provider_symbol")
    if columns == target_columns:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_provider_fetched
            ON fundamentals_raw(fetched_at)
            """
        )
        return

    if "provider_listing_id" not in columns:
        return

    payload_id_expr = "payload_id" if "payload_id" in columns else "NULL"
    currency_expr = "currency" if "currency" in columns else "NULL"
    fetched_at_expr = (
        "fetched_at" if "fetched_at" in columns else "'1970-01-01T00:00:00+00:00'"
    )

    conn.execute("DROP INDEX IF EXISTS idx_fundamentals_raw_provider_fetched")
    conn.execute("ALTER TABLE fundamentals_raw RENAME TO fundamentals_raw_old")
    conn.execute(
        """
        CREATE TABLE fundamentals_raw (
            payload_id INTEGER PRIMARY KEY,
            provider_listing_id INTEGER NOT NULL UNIQUE,
            currency TEXT,
            data TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO fundamentals_raw (
            payload_id,
            provider_listing_id,
            currency,
            data,
            fetched_at
        )
        SELECT
            {payload_id_expr},
            provider_listing_id,
            {currency_expr},
            data,
            {fetched_at_expr}
        FROM fundamentals_raw_old
        WHERE provider_listing_id IS NOT NULL
          AND data IS NOT NULL
        """
    )
    conn.execute("DROP TABLE fundamentals_raw_old")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_provider_fetched
        ON fundamentals_raw(fetched_at)
        """
    )


def _migration_037_drop_fundamentals_raw_currency(conn: sqlite3.Connection) -> None:
    """Remove duplicated payload currency from raw fundamentals."""

    columns = _table_columns(conn, "fundamentals_raw")
    if not columns:
        return

    target_columns = {
        "payload_id",
        "provider_listing_id",
        "data",
        "fetched_at",
    }
    if columns == target_columns:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_provider_fetched
            ON fundamentals_raw(fetched_at)
            """
        )
        return

    if "provider_listing_id" not in columns or "data" not in columns:
        return

    payload_id_expr = "payload_id" if "payload_id" in columns else "NULL"
    fetched_at_expr = (
        "fetched_at" if "fetched_at" in columns else "'1970-01-01T00:00:00+00:00'"
    )

    conn.execute("DROP INDEX IF EXISTS idx_fundamentals_raw_provider_fetched")
    conn.execute("ALTER TABLE fundamentals_raw RENAME TO fundamentals_raw_old")
    conn.execute(
        """
        CREATE TABLE fundamentals_raw (
            payload_id INTEGER PRIMARY KEY,
            provider_listing_id INTEGER NOT NULL UNIQUE,
            data TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO fundamentals_raw (
            payload_id,
            provider_listing_id,
            data,
            fetched_at
        )
        SELECT
            {payload_id_expr},
            provider_listing_id,
            data,
            {fetched_at_expr}
        FROM fundamentals_raw_old
        WHERE provider_listing_id IS NOT NULL
          AND data IS NOT NULL
        """
    )
    conn.execute("DROP TABLE fundamentals_raw_old")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_provider_fetched
        ON fundamentals_raw(fetched_at)
        """
    )


def _migration_038_move_primary_listing_status_to_listing(
    conn: sqlite3.Connection,
) -> None:
    """Store primary listing classification directly on canonical listings."""

    if not _table_exists(conn, "listing"):
        return

    listing_columns = _table_columns(conn, "listing")
    if "primary_listing_status" not in listing_columns:
        conn.execute(
            """
            ALTER TABLE listing
            ADD COLUMN primary_listing_status TEXT NOT NULL DEFAULT 'unknown'
            CHECK (primary_listing_status IN ('unknown', 'primary', 'secondary'))
            """
        )

    if _table_exists(conn, "security_listing_status"):
        status_columns = _table_columns(conn, "security_listing_status")
        key_column = "listing_id" if "listing_id" in status_columns else "security_id"
        if key_column in status_columns and "is_primary_listing" in status_columns:
            conn.execute(
                f"""
                UPDATE listing
                SET primary_listing_status = (
                    SELECT CASE
                        WHEN sls.is_primary_listing = 1 THEN 'primary'
                        ELSE 'secondary'
                    END
                    FROM security_listing_status sls
                    WHERE sls.{key_column} = listing.listing_id
                )
                WHERE EXISTS (
                    SELECT 1
                    FROM security_listing_status sls
                    WHERE sls.{key_column} = listing.listing_id
                )
                """
            )
        conn.execute("DROP INDEX IF EXISTS idx_security_listing_status_primary")
        conn.execute("DROP TABLE security_listing_status")


def _migration_039_canonical_listing_quote_currency(
    conn: sqlite3.Connection,
) -> None:
    """Keep listing quote currency as the only persisted currency truth."""

    # primary_provider_listing_catalog (migration 062) is a downstream
    # projection of provider_listing_catalog and must be dropped first so
    # SQLite's view re-validation on the next ALTER does not trip on a
    # stale reference. Migration 062 will recreate it at the tail of the
    # chain.
    conn.execute("DROP VIEW IF EXISTS primary_provider_listing_catalog")
    conn.execute("DROP VIEW IF EXISTS supported_tickers")
    conn.execute("DROP VIEW IF EXISTS provider_listing_catalog")

    if _table_exists(conn, "listing") and "currency" in _table_columns(conn, "listing"):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_listing_currency_nonnull
            ON listing(currency)
            WHERE currency IS NOT NULL
            """
        )

    if _table_exists(conn, "provider_listing"):
        provider_listing_columns = _table_columns(conn, "provider_listing")
        if "currency" in provider_listing_columns and _table_exists(conn, "listing"):
            conn.execute(
                """
                UPDATE listing
                SET currency = (
                    SELECT UPPER(TRIM(pl.currency))
                    FROM provider_listing pl
                    LEFT JOIN provider p ON p.provider_id = pl.provider_id
                    WHERE pl.listing_id = listing.listing_id
                      AND pl.currency IS NOT NULL
                      AND TRIM(pl.currency) <> ''
                    ORDER BY
                        CASE
                            WHEN p.provider_code = 'EODHD' THEN 0
                            WHEN p.provider_code = 'SEC' THEN 1
                            ELSE 2
                        END,
                        pl.provider_listing_id
                    LIMIT 1
                )
                WHERE EXISTS (
                    SELECT 1
                    FROM provider_listing pl
                    WHERE pl.listing_id = listing.listing_id
                      AND pl.currency IS NOT NULL
                      AND TRIM(pl.currency) <> ''
                )
                """
            )
            conn.execute("DROP INDEX IF EXISTS idx_provider_listing_currency_nonnull")
            conn.execute("ALTER TABLE provider_listing DROP COLUMN currency")

    if _table_exists(conn, "market_data"):
        market_data_columns = _table_columns(conn, "market_data")
        if "currency" in market_data_columns:
            conn.execute(
                """
                UPDATE market_data
                SET
                    price = CASE
                        WHEN (
                            SELECT UPPER(TRIM(l.currency))
                            FROM listing l
                            WHERE l.listing_id = market_data.listing_id
                        ) IN ('GBX', 'GBP0.01')
                         AND UPPER(TRIM(currency)) = 'GBP'
                            THEN price * 100.0
                        WHEN (
                            SELECT UPPER(TRIM(l.currency))
                            FROM listing l
                            WHERE l.listing_id = market_data.listing_id
                        ) = 'ZAC'
                         AND UPPER(TRIM(currency)) = 'ZAR'
                            THEN price * 100.0
                        WHEN (
                            SELECT UPPER(TRIM(l.currency))
                            FROM listing l
                            WHERE l.listing_id = market_data.listing_id
                        ) = 'ILA'
                         AND UPPER(TRIM(currency)) = 'ILS'
                            THEN price * 100.0
                        ELSE price
                    END,
                    market_cap = CASE
                        WHEN market_cap IS NOT NULL
                         AND UPPER(TRIM(currency)) IN ('GBX', 'GBP0.01', 'ZAC', 'ILA')
                            THEN market_cap / 100.0
                        ELSE market_cap
                    END
                WHERE currency IS NOT NULL
                """
            )
            conn.execute("DROP INDEX IF EXISTS idx_market_data_currency_nonnull")
            conn.execute("ALTER TABLE market_data DROP COLUMN currency")


def _migration_040_pure_fundamentals_state(conn: sqlite3.Connection) -> None:
    """Separate raw payload, active fetch failure, and normalization state."""

    now = datetime.now(timezone.utc).isoformat()

    if _table_exists(conn, "fundamentals_raw"):
        raw_columns = _table_columns(conn, "fundamentals_raw")
        conn.execute("DROP INDEX IF EXISTS idx_fundamentals_raw_provider_fetched")
        conn.execute("DROP INDEX IF EXISTS idx_fundamentals_raw_last_fetched")
        conn.execute("DROP INDEX IF EXISTS idx_fundamentals_raw_security")
        conn.execute("DROP INDEX IF EXISTS idx_fundamentals_raw_provider_symbol")
        conn.execute("ALTER TABLE fundamentals_raw RENAME TO fundamentals_raw_old")
        conn.execute(
            """
            CREATE TABLE fundamentals_raw (
                provider_listing_id INTEGER PRIMARY KEY,
                data TEXT NOT NULL,
                payload_hash TEXT NOT NULL CHECK (length(payload_hash) = 64),
                last_fetched_at TEXT NOT NULL,
                FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
            )
            """
        )
        select_columns = ["provider_listing_id", "data"]
        if "payload_hash" in raw_columns:
            select_columns.append("payload_hash")
        if "last_fetched_at" in raw_columns:
            select_columns.append("last_fetched_at")
        if "fetched_at" in raw_columns:
            select_columns.append("fetched_at")
        rows = conn.execute(
            f"""
            SELECT {", ".join(select_columns)}
            FROM fundamentals_raw_old
            WHERE provider_listing_id IS NOT NULL
              AND data IS NOT NULL
            """
        )
        for row in rows:
            data = str(row["data"])
            payload_hash = (
                str(row["payload_hash"])
                if "payload_hash" in row.keys()
                and _normalize_optional_text(row["payload_hash"]) is not None
                else _canonical_json_hash(data)
            )
            last_fetched_at = (
                _normalize_optional_text(row["last_fetched_at"])
                if "last_fetched_at" in row.keys()
                else None
            )
            if last_fetched_at is None and "fetched_at" in row.keys():
                last_fetched_at = _normalize_optional_text(row["fetched_at"])
            conn.execute(
                """
                INSERT OR REPLACE INTO fundamentals_raw (
                    provider_listing_id,
                    data,
                    payload_hash,
                    last_fetched_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    int(row["provider_listing_id"]),
                    data,
                    payload_hash,
                    last_fetched_at or now,
                ),
            )
        conn.execute("DROP TABLE fundamentals_raw_old")
    else:
        conn.execute(
            """
            CREATE TABLE fundamentals_raw (
                provider_listing_id INTEGER PRIMARY KEY,
                data TEXT NOT NULL,
                payload_hash TEXT NOT NULL CHECK (length(payload_hash) = 64),
                last_fetched_at TEXT NOT NULL,
                FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
            )
            """
        )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_last_fetched
        ON fundamentals_raw(last_fetched_at)
        """
    )

    if _table_exists(conn, "fundamentals_fetch_state"):
        fetch_columns = _table_columns(conn, "fundamentals_fetch_state")
        conn.execute("DROP INDEX IF EXISTS idx_fundamentals_fetch_next")
        conn.execute(
            "DROP INDEX IF EXISTS idx_fundamentals_fetch_state_provider_symbol"
        )
        conn.execute(
            "DROP INDEX IF EXISTS idx_fundamentals_fetch_state_provider_fetched_symbol"
        )
        conn.execute(
            "DROP INDEX IF EXISTS idx_fundamentals_fetch_state_provider_status_next_symbol"
        )
        conn.execute(
            "ALTER TABLE fundamentals_fetch_state RENAME TO fundamentals_fetch_state_old"
        )
        conn.execute(
            """
            CREATE TABLE fundamentals_fetch_state (
                provider_listing_id INTEGER PRIMARY KEY,
                failed_at TEXT NOT NULL,
                error TEXT NOT NULL,
                next_eligible_at TEXT NOT NULL,
                attempts INTEGER NOT NULL CHECK (attempts > 0),
                FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
            )
            """
        )
        if "provider_listing_id" in fetch_columns:
            status_expr = "last_status" if "last_status" in fetch_columns else "'error'"
            error_expr = (
                "last_error"
                if "last_error" in fetch_columns
                else "error"
                if "error" in fetch_columns
                else "'unknown error'"
            )
            failed_expr = (
                "failed_at"
                if "failed_at" in fetch_columns
                else (
                    "last_fetched_at"
                    if "last_fetched_at" in fetch_columns
                    else f"'{now}'"
                )
            )
            next_expr = (
                "next_eligible_at"
                if "next_eligible_at" in fetch_columns
                else f"'{now}'"
            )
            attempts_expr = "attempts" if "attempts" in fetch_columns else "1"
            rows = conn.execute(
                f"""
                SELECT
                    provider_listing_id,
                    {failed_expr} AS failed_at,
                    {error_expr} AS error,
                    {next_expr} AS next_eligible_at,
                    {attempts_expr} AS attempts
                FROM fundamentals_fetch_state_old
                WHERE provider_listing_id IS NOT NULL
                  AND {status_expr} = 'error'
                """
            )
            for row in rows:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO fundamentals_fetch_state (
                        provider_listing_id,
                        failed_at,
                        error,
                        next_eligible_at,
                        attempts
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        int(row["provider_listing_id"]),
                        _normalize_optional_text(row["failed_at"]) or now,
                        _normalize_optional_text(row["error"]) or "unknown error",
                        _normalize_optional_text(row["next_eligible_at"]) or now,
                        max(int(row["attempts"] or 1), 1),
                    ),
                )
        conn.execute("DROP TABLE fundamentals_fetch_state_old")
    else:
        conn.execute(
            """
            CREATE TABLE fundamentals_fetch_state (
                provider_listing_id INTEGER PRIMARY KEY,
                failed_at TEXT NOT NULL,
                error TEXT NOT NULL,
                next_eligible_at TEXT NOT NULL,
                attempts INTEGER NOT NULL CHECK (attempts > 0),
                FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
            )
            """
        )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_fetch_next
        ON fundamentals_fetch_state(next_eligible_at)
        """
    )

    if _table_exists(conn, "fundamentals_normalization_state"):
        norm_columns = _table_columns(conn, "fundamentals_normalization_state")
        conn.execute("DROP INDEX IF EXISTS idx_fundamentals_norm_state_security")
        conn.execute("DROP INDEX IF EXISTS idx_fundamentals_norm_state_provider_symbol")
        conn.execute(
            "ALTER TABLE fundamentals_normalization_state RENAME TO fundamentals_normalization_state_old"
        )
        conn.execute(
            """
            CREATE TABLE fundamentals_normalization_state (
                provider_listing_id INTEGER PRIMARY KEY,
                normalized_payload_hash TEXT NOT NULL CHECK (length(normalized_payload_hash) = 64),
                normalized_at TEXT NOT NULL,
                FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
            )
            """
        )
        if "provider_listing_id" in norm_columns:
            normalized_hash_expr = (
                "ns.normalized_payload_hash"
                if "normalized_payload_hash" in norm_columns
                else "fr.payload_hash"
            )
            normalized_at_expr = (
                "ns.normalized_at"
                if "normalized_at" in norm_columns
                else (
                    "ns.last_normalized_at"
                    if "last_normalized_at" in norm_columns
                    else f"'{now}'"
                )
            )
            rows = conn.execute(
                f"""
                SELECT
                    ns.provider_listing_id,
                    {normalized_hash_expr} AS normalized_payload_hash,
                    {normalized_at_expr} AS normalized_at
                FROM fundamentals_normalization_state_old ns
                JOIN fundamentals_raw fr
                  ON fr.provider_listing_id = ns.provider_listing_id
                WHERE ns.provider_listing_id IS NOT NULL
                  AND {normalized_hash_expr} IS NOT NULL
                """
            )
            for row in rows:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO fundamentals_normalization_state (
                        provider_listing_id,
                        normalized_payload_hash,
                        normalized_at
                    ) VALUES (?, ?, ?)
                    """,
                    (
                        int(row["provider_listing_id"]),
                        str(row["normalized_payload_hash"]),
                        _normalize_optional_text(row["normalized_at"]) or now,
                    ),
                )
        conn.execute("DROP TABLE fundamentals_normalization_state_old")
    else:
        conn.execute(
            """
            CREATE TABLE fundamentals_normalization_state (
                provider_listing_id INTEGER PRIMARY KEY,
                normalized_payload_hash TEXT NOT NULL CHECK (length(normalized_payload_hash) = 64),
                normalized_at TEXT NOT NULL,
                FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
            )
            """
        )


def _migration_041_add_metrics_constraints(conn: sqlite3.Connection) -> None:
    """Enforce metrics-table invariants in the schema.

    Adds:
      * ``FOREIGN KEY (listing_id) REFERENCES listing(listing_id)`` to ``metrics``
        and ``metric_compute_status`` (every other listing-rooted table already
        declares it).
      * Two CHECK constraints on ``metrics``:
          - ``unit_kind`` must be one of the documented metric unit kinds.
          - ``currency`` is non-NULL only when ``unit_kind`` is monetary
            (``'monetary'`` or ``'per_share'``). The rule is one-directional:
            monetary kinds may still have NULL currency for unresolved cases,
            matching ``metric_currency_or_none()`` in ``pyvalue.currency``.

    The migration aborts cleanly if either table contains rows whose
    ``listing_id`` does not exist in ``listing``; orphans must be cleaned
    before the rebuild can proceed (see ``purge_secondary_security_data``).
    """

    # If neither table exists yet (fresh database before earlier migrations
    # created them), nothing to do here. Earlier migrations are responsible
    # for creating the initial tables.
    if not _table_exists(conn, "metrics") and not _table_exists(
        conn, "metric_compute_status"
    ):
        return

    # Pre-flight: refuse to migrate if any orphan rows exist. Failing loud is
    # better than silently dropping rows during the rebuild.
    orphan_counts = []
    for table_name in ("metrics", "metric_compute_status"):
        if not _table_exists(conn, table_name):
            continue
        count = conn.execute(
            f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE listing_id NOT IN (SELECT listing_id FROM listing)
            """
        ).fetchone()[0]
        if count:
            orphan_counts.append((table_name, count))
    if orphan_counts:
        details = ", ".join(f"{name}={count}" for name, count in orphan_counts)
        raise RuntimeError(
            "migration 041 aborted: orphan rows reference missing listings "
            f"({details}). Clean them via purge_secondary_security_data() "
            "or a targeted DELETE before retrying."
        )

    # Defer FK checks within the rebuild so intermediate states (the temp
    # table, the post-INSERT moment) don't trip enforcement. The transaction
    # commit at the end of apply_migrations() will run the deferred checks.
    conn.execute("PRAGMA defer_foreign_keys = ON")

    if _table_exists(conn, "metrics"):
        conn.execute("DROP INDEX IF EXISTS idx_metrics_metric_id")
        conn.execute(
            """
            CREATE TABLE metrics__new (
                listing_id INTEGER NOT NULL,
                metric_id TEXT NOT NULL,
                value REAL NOT NULL,
                as_of TEXT NOT NULL,
                unit_kind TEXT NOT NULL DEFAULT 'other',
                currency TEXT,
                unit_label TEXT,
                PRIMARY KEY (listing_id, metric_id),
                FOREIGN KEY (listing_id) REFERENCES listing(listing_id),
                CHECK (
                    unit_kind IN (
                        'monetary', 'per_share', 'ratio', 'percent',
                        'multiple', 'count', 'other'
                    )
                ),
                CHECK (
                    currency IS NULL
                    OR unit_kind IN ('monetary', 'per_share')
                )
            )
            """
        )
        conn.execute(
            """
            INSERT INTO metrics__new (
                listing_id, metric_id, value, as_of, unit_kind, currency, unit_label
            )
            SELECT
                listing_id, metric_id, value, as_of, unit_kind, currency, unit_label
            FROM metrics
            """
        )
        conn.execute("DROP TABLE metrics")
        conn.execute("ALTER TABLE metrics__new RENAME TO metrics")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_metrics_metric_id
            ON metrics(metric_id)
            """
        )

    if _table_exists(conn, "metric_compute_status"):
        conn.execute("DROP INDEX IF EXISTS idx_metric_compute_status_metric_status")
        conn.execute(
            """
            CREATE TABLE metric_compute_status__new (
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
                PRIMARY KEY (listing_id, metric_id),
                FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO metric_compute_status__new (
                listing_id, metric_id, status, reason_code, reason_detail,
                attempted_at, value_as_of, facts_refreshed_at,
                market_data_as_of, market_data_updated_at
            )
            SELECT
                listing_id, metric_id, status, reason_code, reason_detail,
                attempted_at, value_as_of, facts_refreshed_at,
                market_data_as_of, market_data_updated_at
            FROM metric_compute_status
            """
        )
        conn.execute("DROP TABLE metric_compute_status")
        conn.execute(
            "ALTER TABLE metric_compute_status__new RENAME TO metric_compute_status"
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_metric_compute_status_metric_status
            ON metric_compute_status(metric_id, status)
            """
        )

    # Verify the rebuild left the database self-consistent. foreign_key_check
    # returns one row per offending FK violation; integrity_check returns 'ok'
    # on a healthy database.
    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 041 left foreign key violations: {fk_violations!r}"
        )
    integrity = conn.execute("PRAGMA integrity_check").fetchone()
    if integrity is None or integrity[0] != "ok":
        raise RuntimeError(f"migration 041 integrity_check failed: {integrity!r}")


def _migration_043_financial_facts_dedupe_and_fk(conn: sqlite3.Connection) -> None:
    """Drop ``accn`` from the ``financial_facts`` PK, dedupe collisions, add FK.

    The previous PK was ``(listing_id, concept, fiscal_period, end_date,
    unit, accn)``. Audit findings on the live DB:

    * 103,126,997 of 103,188,287 rows (99.94%) have ``accn IS NULL``;
      the bulk EODHD source never populates the column.
    * Across the entire table, **zero** ``(listing_id, concept,
      fiscal_period, end_date, unit)`` groups are disambiguated by
      multiple distinct non-NULL ``accn`` values — ``accn`` therefore
      plays no role in de-facto uniqueness.
    * Because SQLite treats ``NULL <> NULL`` for PK uniqueness,
      24,837 duplicate-key groups currently coexist with the same
      non-``accn`` key parts. Every duplicate is from EODHD.

    This migration:

    1. Builds a new table whose PK is
       ``(listing_id, concept, fiscal_period, end_date, unit)`` and
       declares ``FOREIGN KEY (listing_id) REFERENCES listing(listing_id)``,
       closing the missing-FK gap that audit finding 3.2 flagged.
    2. ``accn`` remains a nullable, non-key column for the ~89K rows that
       carry meaningful filing accession values.
    3. ``fiscal_period`` remains nullable in the schema. The live DB has
       zero NULL-fiscal_period rows today, but ``FactRecord`` allows
       ``fiscal_period = None`` and several callers rely on that, so
       enforcing ``NOT NULL`` here would be an API break for marginal
       benefit. Two rows that share every other PK column and both have
       ``fiscal_period IS NULL`` could in principle coexist (NULL ≠ NULL
       in PK semantics), but no such rows exist on the live DB.
    4. Deduplicates the 24,837 colliding groups via ``ROW_NUMBER()``,
       keeping the row with the most authoritative provenance:
       non-NULL ``filed`` first, then ``filed DESC``, then
       ``rowid ASC`` for a deterministic tie-break.

    The migration aborts cleanly if any orphan rows (``listing_id`` not
    present in ``listing``) exist; orphans must be cleaned before the FK
    can be added.
    """

    if not _table_exists(conn, "financial_facts"):
        return

    orphan_count = conn.execute(
        """
        SELECT COUNT(*) FROM financial_facts
        WHERE listing_id NOT IN (SELECT listing_id FROM listing)
        """
    ).fetchone()[0]
    if orphan_count:
        raise RuntimeError(
            f"migration 043 aborted: {orphan_count} orphan financial_facts "
            "rows reference missing listings. Clean these before retrying."
        )

    # Defer FK checks within the rebuild so intermediate states don't
    # trip enforcement; the transaction commit at the end of
    # apply_migrations() runs the deferred checks.
    conn.execute("PRAGMA defer_foreign_keys = ON")

    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_concept")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_security_concept")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_security_concept_latest")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_currency_nonnull")

    conn.execute(
        """
        CREATE TABLE financial_facts__new (
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
            PRIMARY KEY (listing_id, concept, fiscal_period, end_date, unit),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        )
        """
    )

    # Dedupe within each (listing_id, concept, fiscal_period, end_date,
    # unit) group, keeping a single winning row per group:
    #   1. Prefer rows with a non-NULL `filed` over rows without one.
    #   2. Among those, prefer the most recently filed.
    #   3. Tie-break by rowid ASC for determinism.
    # ``rowid`` is exposed by ``financial_facts.rowid`` even though the
    # table has an explicit composite PK — SQLite always tracks an
    # implicit rowid unless ``WITHOUT ROWID`` is used.
    conn.execute(
        """
        INSERT INTO financial_facts__new (
            listing_id, cik, concept, fiscal_period, end_date, unit,
            value, accn, filed, frame, start_date, accounting_standard,
            currency, source_provider
        )
        SELECT
            listing_id, cik, concept, fiscal_period, end_date, unit,
            value, accn, filed, frame, start_date, accounting_standard,
            currency, source_provider
        FROM (
            SELECT
                financial_facts.*,
                ROW_NUMBER() OVER (
                    PARTITION BY listing_id, concept, fiscal_period, end_date, unit
                    ORDER BY
                        (filed IS NOT NULL) DESC,
                        filed DESC,
                        financial_facts.rowid ASC
                ) AS rn
            FROM financial_facts
        )
        WHERE rn = 1
        """
    )

    conn.execute("DROP TABLE financial_facts")
    conn.execute("ALTER TABLE financial_facts__new RENAME TO financial_facts")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fin_facts_concept
        ON financial_facts(concept)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fin_facts_security_concept
        ON financial_facts(listing_id, concept)
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

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 043 left foreign key violations: {fk_violations!r}"
        )


def _migration_047_add_fk_market_data(conn: sqlite3.Connection) -> None:
    """Add ``FOREIGN KEY (listing_id) REFERENCES listing`` to ``market_data``.

    Audit finding 3.2: ``market_data.listing_id`` referenced ``listing``
    only logically. The rebuild attaches the missing physical FK.
    """

    if not _table_exists(conn, "market_data"):
        return

    fk_rows = conn.execute("PRAGMA foreign_key_list(market_data)").fetchall()
    if any(row[2] == "listing" for row in fk_rows):
        return

    orphan_count = conn.execute(
        """
        SELECT COUNT(*) FROM market_data
        WHERE listing_id NOT IN (SELECT listing_id FROM listing)
        """
    ).fetchone()[0]
    if orphan_count:
        raise RuntimeError(
            f"migration 047 aborted: {orphan_count} orphan market_data rows "
            "reference missing listings. Clean these before retrying."
        )

    conn.execute("PRAGMA defer_foreign_keys = ON")

    conn.execute("DROP INDEX IF EXISTS idx_market_data_latest")
    conn.execute("DROP INDEX IF EXISTS idx_market_data_currency_nonnull")

    columns = _table_columns(conn, "market_data")
    # Older schemas could carry a `currency` column even though migration
    # 039 removed it from the canonical shape; preserve it in the rebuild
    # if present so the migration is robust against historical fixtures.
    has_currency = "currency" in columns
    new_currency_decl = "currency TEXT,\n            " if has_currency else ""
    select_currency_expr = "currency, " if has_currency else ""

    conn.execute(
        f"""
        CREATE TABLE market_data__new (
            listing_id INTEGER NOT NULL,
            as_of DATE NOT NULL,
            price REAL NOT NULL,
            volume INTEGER,
            market_cap REAL,
            {new_currency_decl}source_provider TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (listing_id, as_of),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO market_data__new (
            listing_id, as_of, price, volume, market_cap,
            {"currency, " if has_currency else ""}source_provider, updated_at
        )
        SELECT
            listing_id, as_of, price, volume, market_cap,
            {select_currency_expr}source_provider, updated_at
        FROM market_data
        """
    )
    conn.execute("DROP TABLE market_data")
    conn.execute("ALTER TABLE market_data__new RENAME TO market_data")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_data_latest
        ON market_data(listing_id, as_of DESC)
        """
    )
    if has_currency:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_data_currency_nonnull
            ON market_data(currency)
            WHERE currency IS NOT NULL
            """
        )

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 047 left foreign key violations: {fk_violations!r}"
        )


def _migration_048_add_fk_fx_rates_provider(conn: sqlite3.Connection) -> None:
    """Add ``FOREIGN KEY (provider) REFERENCES provider(provider_code)`` to ``fx_rates``."""

    if not _table_exists(conn, "fx_rates"):
        return

    fk_rows = conn.execute("PRAGMA foreign_key_list(fx_rates)").fetchall()
    if any(row[2] == "provider" for row in fk_rows):
        return

    orphan_count = conn.execute(
        """
        SELECT COUNT(*) FROM fx_rates
        WHERE provider NOT IN (SELECT provider_code FROM provider)
        """
    ).fetchone()[0]
    if orphan_count:
        raise RuntimeError(
            f"migration 048 aborted: {orphan_count} orphan fx_rates rows "
            "reference missing providers. Clean these before retrying."
        )

    conn.execute("PRAGMA defer_foreign_keys = ON")
    conn.execute("DROP INDEX IF EXISTS idx_fx_rates_pair_date")

    conn.execute(
        """
        CREATE TABLE fx_rates__new (
            provider TEXT NOT NULL,
            rate_date TEXT NOT NULL,
            base_currency TEXT NOT NULL,
            quote_currency TEXT NOT NULL,
            rate REAL NOT NULL,
            fetched_at TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            meta_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (provider, rate_date, base_currency, quote_currency),
            FOREIGN KEY (provider) REFERENCES provider(provider_code)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO fx_rates__new (
            provider, rate_date, base_currency, quote_currency, rate,
            fetched_at, source_kind, meta_json, created_at, updated_at
        )
        SELECT
            provider, rate_date, base_currency, quote_currency, rate,
            fetched_at, source_kind, meta_json, created_at, updated_at
        FROM fx_rates
        """
    )
    conn.execute("DROP TABLE fx_rates")
    conn.execute("ALTER TABLE fx_rates__new RENAME TO fx_rates")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fx_rates_pair_date
        ON fx_rates(provider, base_currency, quote_currency, rate_date DESC)
        """
    )

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 048 left foreign key violations: {fk_violations!r}"
        )


def _migration_049_add_fk_fx_supported_pairs_provider(
    conn: sqlite3.Connection,
) -> None:
    """Add provider FK to ``fx_supported_pairs``."""

    if not _table_exists(conn, "fx_supported_pairs"):
        return

    fk_rows = conn.execute("PRAGMA foreign_key_list(fx_supported_pairs)").fetchall()
    if any(row[2] == "provider" for row in fk_rows):
        return

    orphan_count = conn.execute(
        """
        SELECT COUNT(*) FROM fx_supported_pairs
        WHERE provider NOT IN (SELECT provider_code FROM provider)
        """
    ).fetchone()[0]
    if orphan_count:
        raise RuntimeError(
            f"migration 049 aborted: {orphan_count} orphan fx_supported_pairs "
            "rows reference missing providers. Clean these before retrying."
        )

    conn.execute("PRAGMA defer_foreign_keys = ON")
    conn.execute("DROP INDEX IF EXISTS idx_fx_supported_pairs_refreshable")

    conn.execute(
        """
        CREATE TABLE fx_supported_pairs__new (
            provider TEXT NOT NULL,
            symbol TEXT NOT NULL,
            canonical_symbol TEXT NOT NULL,
            base_currency TEXT,
            quote_currency TEXT,
            name TEXT,
            is_alias INTEGER NOT NULL DEFAULT 0,
            is_refreshable INTEGER NOT NULL DEFAULT 0,
            last_seen_at TEXT NOT NULL,
            PRIMARY KEY (provider, symbol),
            FOREIGN KEY (provider) REFERENCES provider(provider_code)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO fx_supported_pairs__new (
            provider, symbol, canonical_symbol, base_currency, quote_currency,
            name, is_alias, is_refreshable, last_seen_at
        )
        SELECT
            provider, symbol, canonical_symbol, base_currency, quote_currency,
            name, is_alias, is_refreshable, last_seen_at
        FROM fx_supported_pairs
        """
    )
    conn.execute("DROP TABLE fx_supported_pairs")
    conn.execute("ALTER TABLE fx_supported_pairs__new RENAME TO fx_supported_pairs")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fx_supported_pairs_refreshable
        ON fx_supported_pairs(provider, is_refreshable, canonical_symbol)
        """
    )

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 049 left foreign key violations: {fk_violations!r}"
        )


def _migration_050_add_fk_fx_refresh_state_provider(
    conn: sqlite3.Connection,
) -> None:
    """Add provider FK to ``fx_refresh_state``."""

    if not _table_exists(conn, "fx_refresh_state"):
        return

    fk_rows = conn.execute("PRAGMA foreign_key_list(fx_refresh_state)").fetchall()
    if any(row[2] == "provider" for row in fk_rows):
        return

    orphan_count = conn.execute(
        """
        SELECT COUNT(*) FROM fx_refresh_state
        WHERE provider NOT IN (SELECT provider_code FROM provider)
        """
    ).fetchone()[0]
    if orphan_count:
        raise RuntimeError(
            f"migration 050 aborted: {orphan_count} orphan fx_refresh_state "
            "rows reference missing providers. Clean these before retrying."
        )

    conn.execute("PRAGMA defer_foreign_keys = ON")

    conn.execute(
        """
        CREATE TABLE fx_refresh_state__new (
            provider TEXT NOT NULL,
            canonical_symbol TEXT NOT NULL,
            min_rate_date TEXT,
            max_rate_date TEXT,
            full_history_backfilled INTEGER NOT NULL DEFAULT 0,
            last_fetched_at TEXT,
            last_status TEXT,
            last_error TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (provider, canonical_symbol),
            FOREIGN KEY (provider) REFERENCES provider(provider_code)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO fx_refresh_state__new (
            provider, canonical_symbol, min_rate_date, max_rate_date,
            full_history_backfilled, last_fetched_at, last_status, last_error,
            attempts
        )
        SELECT
            provider, canonical_symbol, min_rate_date, max_rate_date,
            full_history_backfilled, last_fetched_at, last_status, last_error,
            attempts
        FROM fx_refresh_state
        """
    )
    conn.execute("DROP TABLE fx_refresh_state")
    conn.execute("ALTER TABLE fx_refresh_state__new RENAME TO fx_refresh_state")

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 050 left foreign key violations: {fk_violations!r}"
        )


def _migration_052_drop_redundant_fin_facts_index(conn: sqlite3.Connection) -> None:
    """Drop ``idx_fin_facts_security_concept`` — fully covered by ``_latest``.

    Audit P3 #11: ``idx_fin_facts_security_concept (listing_id, concept)``
    is a strict prefix of
    ``idx_fin_facts_security_concept_latest
    (listing_id, concept, end_date DESC, filed DESC)``. SQLite's query
    planner can serve any query that filters on ``(listing_id, concept)``
    from the longer index. Keeping both means every fact insert pays
    the write cost of two near-identical b-trees on the largest table
    in the DB (~103M rows, ~8.5 GiB).

    Forward fresh-DB runs of earlier migrations created both indexes —
    dropping the redundant one here keeps the schema in sync regardless
    of how the DB was migrated.
    """

    if not _table_exists(conn, "financial_facts"):
        return
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_security_concept")


def _migration_051_add_bool_checks(conn: sqlite3.Connection) -> None:
    """Add ``CHECK (col IN (0, 1))`` to boolean-flagged INTEGER columns.

    Audit finding 3.3: ``fx_supported_pairs.is_alias``,
    ``fx_supported_pairs.is_refreshable``, and
    ``fx_refresh_state.full_history_backfilled`` were declared as plain
    INTEGER without enum constraints. The Python code only ever writes
    0 or 1, but the schema previously didn't enforce that. Adding a
    CHECK closes the invariant at the database level. ``attempts``
    columns also gain ``CHECK (attempts >= 0)``.
    """

    if _table_exists(conn, "fx_supported_pairs"):
        ddl_row = conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'table' AND name = 'fx_supported_pairs'"
        ).fetchone()
        if ddl_row and "CHECK (is_alias IN (0, 1))" not in ddl_row[0]:
            fk_already_present = bool(
                conn.execute("PRAGMA foreign_key_list(fx_supported_pairs)").fetchall()
            )
            conn.execute("PRAGMA defer_foreign_keys = ON")
            conn.execute("DROP INDEX IF EXISTS idx_fx_supported_pairs_refreshable")

            fk_clause = (
                ",\n            FOREIGN KEY (provider) REFERENCES provider(provider_code)"
                if fk_already_present
                else ""
            )
            conn.execute(
                f"""
                CREATE TABLE fx_supported_pairs__new (
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    canonical_symbol TEXT NOT NULL,
                    base_currency TEXT,
                    quote_currency TEXT,
                    name TEXT,
                    is_alias INTEGER NOT NULL DEFAULT 0
                        CHECK (is_alias IN (0, 1)),
                    is_refreshable INTEGER NOT NULL DEFAULT 0
                        CHECK (is_refreshable IN (0, 1)),
                    last_seen_at TEXT NOT NULL,
                    PRIMARY KEY (provider, symbol){fk_clause}
                )
                """
            )
            conn.execute(
                """
                INSERT INTO fx_supported_pairs__new (
                    provider, symbol, canonical_symbol, base_currency,
                    quote_currency, name, is_alias, is_refreshable, last_seen_at
                )
                SELECT
                    provider, symbol, canonical_symbol, base_currency,
                    quote_currency, name, is_alias, is_refreshable, last_seen_at
                FROM fx_supported_pairs
                """
            )
            conn.execute("DROP TABLE fx_supported_pairs")
            conn.execute(
                "ALTER TABLE fx_supported_pairs__new RENAME TO fx_supported_pairs"
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_fx_supported_pairs_refreshable
                ON fx_supported_pairs(provider, is_refreshable, canonical_symbol)
                """
            )

    if _table_exists(conn, "fx_refresh_state"):
        ddl_row = conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'table' AND name = 'fx_refresh_state'"
        ).fetchone()
        if ddl_row and "CHECK (full_history_backfilled IN (0, 1))" not in ddl_row[0]:
            fk_already_present = bool(
                conn.execute("PRAGMA foreign_key_list(fx_refresh_state)").fetchall()
            )
            conn.execute("PRAGMA defer_foreign_keys = ON")
            fk_clause = (
                ",\n            FOREIGN KEY (provider) REFERENCES provider(provider_code)"
                if fk_already_present
                else ""
            )
            conn.execute(
                f"""
                CREATE TABLE fx_refresh_state__new (
                    provider TEXT NOT NULL,
                    canonical_symbol TEXT NOT NULL,
                    min_rate_date TEXT,
                    max_rate_date TEXT,
                    full_history_backfilled INTEGER NOT NULL DEFAULT 0
                        CHECK (full_history_backfilled IN (0, 1)),
                    last_fetched_at TEXT,
                    last_status TEXT,
                    last_error TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0
                        CHECK (attempts >= 0),
                    PRIMARY KEY (provider, canonical_symbol){fk_clause}
                )
                """
            )
            conn.execute(
                """
                INSERT INTO fx_refresh_state__new (
                    provider, canonical_symbol, min_rate_date, max_rate_date,
                    full_history_backfilled, last_fetched_at, last_status,
                    last_error, attempts
                )
                SELECT
                    provider, canonical_symbol, min_rate_date, max_rate_date,
                    full_history_backfilled, last_fetched_at, last_status,
                    last_error, attempts
                FROM fx_refresh_state
                """
            )
            conn.execute("DROP TABLE fx_refresh_state")
            conn.execute("ALTER TABLE fx_refresh_state__new RENAME TO fx_refresh_state")

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 051 left foreign key violations: {fk_violations!r}"
        )


def _migration_046_add_fk_financial_facts_refresh_state(
    conn: sqlite3.Connection,
) -> None:
    """Add ``FOREIGN KEY (listing_id) REFERENCES listing`` to
    ``financial_facts_refresh_state``.

    Per audit finding 3.2, this watermark table referenced ``listing_id``
    only logically. The rebuild attaches the same physical FK pattern
    that migrations 041 (metrics/metric_compute_status) and 043
    (financial_facts) already use.

    The migration aborts cleanly if any orphan rows exist.
    """

    if not _table_exists(conn, "financial_facts_refresh_state"):
        return

    # Already migrated? Inspect the existing FK list and skip if present.
    fk_rows = conn.execute(
        "PRAGMA foreign_key_list(financial_facts_refresh_state)"
    ).fetchall()
    if any(row[2] == "listing" for row in fk_rows):
        return

    orphan_count = conn.execute(
        """
        SELECT COUNT(*) FROM financial_facts_refresh_state
        WHERE listing_id NOT IN (SELECT listing_id FROM listing)
        """
    ).fetchone()[0]
    if orphan_count:
        raise RuntimeError(
            f"migration 046 aborted: {orphan_count} orphan "
            "financial_facts_refresh_state rows reference missing listings. "
            "Clean these before retrying."
        )

    conn.execute("PRAGMA defer_foreign_keys = ON")

    conn.execute(
        """
        CREATE TABLE financial_facts_refresh_state__new (
            listing_id INTEGER NOT NULL PRIMARY KEY,
            refreshed_at TEXT NOT NULL,
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO financial_facts_refresh_state__new (listing_id, refreshed_at)
        SELECT listing_id, refreshed_at
        FROM financial_facts_refresh_state
        """
    )
    conn.execute("DROP TABLE financial_facts_refresh_state")
    conn.execute(
        "ALTER TABLE financial_facts_refresh_state__new "
        "RENAME TO financial_facts_refresh_state"
    )

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 046 left foreign key violations: {fk_violations!r}"
        )


def _migration_045_fx_rates_rate_to_real(conn: sqlite3.Connection) -> None:
    """Rebuild fx_rates with a REAL ``rate`` column (was TEXT ``rate_text``).

    Per the project's REAL-everywhere policy, FX rates should be stored
    as REAL rather than serialised through a TEXT column. The historical
    ``rate_text TEXT`` column was a workaround for the previous
    "no REAL for monetary values" guideline; that guideline does not
    apply to pyvalue.

    The migration:
      * Builds a new fx_rates table with ``rate REAL NOT NULL`` in place
        of ``rate_text TEXT NOT NULL``. Column shape, PK, and the
        idx_fx_rates_pair_date index are otherwise preserved.
      * Casts the existing string values to REAL during INSERT...SELECT.
        SQLite's CAST(text AS REAL) reads the leading numeric prefix and
        returns the float.
      * Renames the temp table back to ``fx_rates`` and re-creates the
        index.

    The Python boundary changes (``FXRateRecord.rate_text: str`` →
    ``FXRateRecord.rate: float`` and the ``str(...)`` / ``float(...)``
    coercion at the storage layer) ship in the same commit. After this
    migration, callers consume ``rate`` as ``float`` and the
    ``rate_text`` field name no longer appears in the codebase.
    """

    if not _table_exists(conn, "fx_rates"):
        return

    columns = _table_columns(conn, "fx_rates")
    if "rate" in columns and "rate_text" not in columns:
        # Already migrated.
        return

    conn.execute("DROP INDEX IF EXISTS idx_fx_rates_pair_date")

    conn.execute(
        """
        CREATE TABLE fx_rates__new (
            provider TEXT NOT NULL,
            rate_date TEXT NOT NULL,
            base_currency TEXT NOT NULL,
            quote_currency TEXT NOT NULL,
            rate REAL NOT NULL,
            fetched_at TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            meta_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (provider, rate_date, base_currency, quote_currency)
        )
        """
    )
    # `meta_json` was added by a later migration than the original
    # fx_rates create, so historical fixtures may not carry the column.
    # Substitute NULL when it is missing so the rebuild stays compatible
    # with the older schemas the migration test fixtures emulate.
    meta_json_expr = "meta_json" if "meta_json" in columns else "NULL"
    conn.execute(
        f"""
        INSERT INTO fx_rates__new (
            provider, rate_date, base_currency, quote_currency, rate,
            fetched_at, source_kind, meta_json, created_at, updated_at
        )
        SELECT
            provider, rate_date, base_currency, quote_currency,
            CAST(rate_text AS REAL),
            fetched_at, source_kind, {meta_json_expr}, created_at, updated_at
        FROM fx_rates
        """
    )
    conn.execute("DROP TABLE fx_rates")
    conn.execute("ALTER TABLE fx_rates__new RENAME TO fx_rates")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fx_rates_pair_date
        ON fx_rates(provider, base_currency, quote_currency, rate_date DESC)
        """
    )


def _migration_044_persist_compat_views(conn: sqlite3.Connection) -> None:
    """Take migration ownership of the legacy compatibility views.

    Three views still lived only in ``storage.py`` runtime DDL:

    * ``providers`` — old plural name for the renamed ``provider`` table.
    * ``securities`` — pre-canonical-rename name for the
      ``listing JOIN issuer JOIN exchange`` shape.
    * ``exchange_provider`` — old name for the renamed
      ``provider_exchange`` table.

    Each is heavily consumed by both production code (e.g. financial
    fact joins, market data joins) and tests, so they cannot simply be
    deleted — they must be persisted. Migration 042 took ownership of
    ``provider_listing_catalog`` and ``supported_tickers`` for the same
    reason; this migration extends that pattern to the remaining three.
    Future shape changes ship as a new migration, not a runtime
    ``DROP VIEW`` / ``CREATE VIEW`` pair in ``storage.py``.

    Each view is created only when its underlying tables exist, and only
    when no table with the same name is in the way (an early migration
    may have used the view name for a TABLE before the rename chain).
    Creating a view against a missing table would later poison every
    DDL statement because SQLite re-validates views on schema change.
    """

    provider_columns_needed = {
        "provider_code",
        "display_name",
        "description",
        "created_at",
        "updated_at",
    }
    if (
        _table_exists(conn, "provider")
        and provider_columns_needed <= _table_columns(conn, "provider")
        and not _table_exists(conn, "providers")
    ):
        conn.execute("DROP VIEW IF EXISTS providers")
        conn.execute(
            """
            CREATE VIEW providers AS
            SELECT
                provider_code,
                display_name,
                description,
                created_at,
                updated_at
            FROM provider
            """
        )

    listing_columns_needed = {"listing_id", "symbol", "issuer_id", "exchange_id"}
    issuer_columns_needed = {
        "issuer_id",
        "name",
        "description",
        "sector",
        "industry",
    }
    exchange_columns_needed = {"exchange_id", "exchange_code"}
    if (
        _table_exists(conn, "listing")
        and listing_columns_needed <= _table_columns(conn, "listing")
        and _table_exists(conn, "issuer")
        and issuer_columns_needed <= _table_columns(conn, "issuer")
        and _table_exists(conn, "exchange")
        and exchange_columns_needed <= _table_columns(conn, "exchange")
        and not _table_exists(conn, "securities")
    ):
        conn.execute("DROP VIEW IF EXISTS securities")
        conn.execute(
            """
            CREATE VIEW securities AS
            SELECT
                l.listing_id AS security_id,
                l.symbol AS canonical_ticker,
                e.exchange_code AS canonical_exchange_code,
                l.symbol || '.' || e.exchange_code AS canonical_symbol,
                i.name AS entity_name,
                i.description,
                i.sector,
                i.industry,
                NULL AS created_at,
                NULL AS updated_at
            FROM listing l
            JOIN issuer i ON i.issuer_id = l.issuer_id
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            """
        )

    provider_exchange_columns_needed = {
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
    if (
        _table_exists(conn, "provider_exchange")
        and provider_exchange_columns_needed
        <= _table_columns(conn, "provider_exchange")
        and _table_exists(conn, "provider")
        and {"provider_id", "provider_code"} <= _table_columns(conn, "provider")
        and not _table_exists(conn, "exchange_provider")
    ):
        conn.execute("DROP VIEW IF EXISTS exchange_provider")
        conn.execute(
            """
            CREATE VIEW exchange_provider AS
            SELECT
                p.provider_code AS provider,
                ep.provider_exchange_code,
                ep.exchange_id,
                ep.name,
                ep.country,
                ep.currency,
                ep.operating_mic,
                ep.country_iso2,
                ep.country_iso3,
                ep.updated_at
            FROM provider_exchange ep
            JOIN provider p ON p.provider_id = ep.provider_id
            """
        )


def _migration_042_persist_provider_listing_views(conn: sqlite3.Connection) -> None:
    """Move provider-listing view DDL from runtime code into the schema.

    Historically ``_ensure_provider_listing_catalog_views()`` in
    ``storage.py`` created ``provider_listing_catalog`` and
    ``supported_tickers`` at runtime, gated behind various
    ``initialize_schema()`` paths. That left the views off the migration
    record and out of the persisted schema for any DB whose application
    boot path did not touch one of those repositories (per audit, the
    live DB held zero views). Splitting schema ownership between
    migrations and runtime code is the root cause of the constraint-drift
    issues that motivated migration 041; migration 042 closes the same
    door for views.

    From this point onward, view definitions for ``provider_listing_catalog``
    and ``supported_tickers`` are owned by migrations. Future shape
    changes ship as a new migration, not as a runtime ``DROP VIEW`` /
    ``CREATE VIEW`` pair in ``storage.py``.

    The view is only created when its underlying tables are present.
    Migration tests sometimes start from very old fixtures that don't
    finish the catalog rename chain (migration 034 conditionally creates
    these tables based on what's in the DB). Creating a view against a
    missing table would later poison every DDL statement, because
    SQLite re-validates views on schema change and the validation would
    fail with "no such table" on every subsequent ALTER / DROP.
    """

    required = {
        "provider_listing": {
            "provider_listing_id",
            "provider_id",
            "provider_exchange_id",
            "provider_symbol",
            "listing_id",
        },
        "provider": {"provider_id", "provider_code"},
        "provider_exchange": {
            "provider_exchange_id",
            "provider_exchange_code",
        },
        "listing": {
            "listing_id",
            "issuer_id",
            "exchange_id",
            "symbol",
            "currency",
            "primary_listing_status",
        },
        "issuer": {"issuer_id", "name", "country"},
        "exchange": {"exchange_id", "exchange_code"},
    }
    for table_name, needed_cols in required.items():
        if not _table_exists(conn, table_name):
            return
        if not needed_cols <= _table_columns(conn, table_name):
            return

    # Drop any transient views the legacy runtime code may have created
    # earlier in this DB's lifetime so the CREATE VIEW below is the
    # single canonical definition. primary_provider_listing_catalog
    # (migration 062) is a projection of provider_listing_catalog so it
    # must come down first.
    conn.execute("DROP VIEW IF EXISTS primary_provider_listing_catalog")
    conn.execute("DROP VIEW IF EXISTS supported_tickers")
    conn.execute("DROP VIEW IF EXISTS provider_listing_catalog")

    conn.execute(
        """
        CREATE VIEW provider_listing_catalog AS
        SELECT
            pl.provider_listing_id,
            p.provider_id,
            p.provider_code AS provider,
            px.provider_exchange_id,
            px.provider_exchange_code,
            CASE
                WHEN p.provider_code = 'SEC' THEN pl.provider_symbol || '.US'
                ELSE pl.provider_symbol || '.' || px.provider_exchange_code
            END AS provider_symbol,
            pl.provider_symbol AS provider_ticker,
            l.listing_id AS security_id,
            e.exchange_code AS listing_exchange,
            i.name AS security_name,
            NULL AS security_type,
            i.country AS country,
            l.currency AS currency,
            l.primary_listing_status,
            NULL AS isin,
            NULL AS updated_at
        FROM provider_listing pl
        JOIN provider p ON p.provider_id = pl.provider_id
        JOIN provider_exchange px
          ON px.provider_exchange_id = pl.provider_exchange_id
        JOIN listing l ON l.listing_id = pl.listing_id
        JOIN issuer i ON i.issuer_id = l.issuer_id
        JOIN "exchange" e ON e.exchange_id = l.exchange_id
        """
    )
    conn.execute(
        """
        CREATE VIEW supported_tickers AS
        SELECT
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
            primary_listing_status,
            isin,
            updated_at
        FROM provider_listing_catalog
        """
    )


def _migration_053_drop_market_data_fetch_state_runtime_columns(
    conn: sqlite3.Connection,
) -> None:
    """Drop runtime-added ``provider`` / ``provider_symbol`` columns from
    ``market_data_fetch_state``.

    Audit P1 #1.c: ``_FetchStateRepository.initialize_schema()`` historically
    ALTER-ed ``market_data_fetch_state`` to add ``provider`` and
    ``provider_symbol`` columns plus a ``UNIQUE (provider, provider_symbol)``
    index. Migration 040 rebuilt the table without these columns, but the
    runtime path would re-add them on the next ``initialize_schema()``
    call, undoing the migration.

    The columns duplicate values that already live in
    ``provider_listing`` / ``provider_exchange`` / ``provider`` and were
    only there to let the repository upsert by ``(provider,
    provider_symbol)`` without joining. The repository is being rewritten
    in this same change to resolve ``provider_listing_id`` via
    ``provider_listing_catalog`` and use the table's ``provider_listing_id``
    PK directly, so the columns are no longer needed.

    This migration is idempotent — it only acts when the columns are
    actually present (e.g. on a DB whose runtime path re-added them
    after migration 040). Live DBs that have not run the runtime path
    since migration 040 already have the clean schema and this migration
    is a no-op.
    """

    if not _table_exists(conn, "market_data_fetch_state"):
        return

    columns = _table_columns(conn, "market_data_fetch_state")
    has_provider_cols = "provider" in columns or "provider_symbol" in columns

    # Drop the legacy unique index unconditionally — a DB that ran the
    # runtime path may have it even after we drop the columns.
    conn.execute("DROP INDEX IF EXISTS idx_market_data_fetch_state_provider_symbol")

    if not has_provider_cols:
        return

    # SQLite cannot drop a column that participates in a UNIQUE index, so
    # the index drop above must run first. ``ALTER TABLE DROP COLUMN`` is
    # available since SQLite 3.35 (March 2021); the project's minimum
    # SQLite is well above that.
    if "provider" in columns:
        conn.execute("ALTER TABLE market_data_fetch_state DROP COLUMN provider")
    if "provider_symbol" in columns:
        conn.execute("ALTER TABLE market_data_fetch_state DROP COLUMN provider_symbol")

    fk_violations = conn.execute(
        "PRAGMA foreign_key_check(market_data_fetch_state)"
    ).fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 053 left foreign key violations: {fk_violations!r}"
        )


def _migration_054_drop_provider_listing_provider_id(
    conn: sqlite3.Connection,
) -> None:
    """Drop ``provider_listing.provider_id`` denormalisation.

    Audit P2 #9: ``provider_listing.provider_id`` is fully derivable from
    ``provider_listing.provider_exchange_id`` via
    ``provider_exchange.provider_id``. Holding it on the row violates the
    *single source of truth* rule even though the composite FK
    ``(provider_exchange_id, provider_id) -> provider_exchange
    (provider_exchange_id, provider_id)`` keeps the values consistent.

    This migration drops:
    - the supporting index ``idx_provider_listing_provider``;
    - the column ``provider_id`` (and the associated FK clauses, since
      they're recreated as part of the table rebuild);

    Queries that previously filtered ``provider_listing.provider_id``
    directly are rewritten to join through ``provider_exchange`` in the
    same change.

    SQLite cannot ``DROP COLUMN`` for a column that participates in a
    foreign-key constraint or composite UNIQUE; we therefore rebuild
    the table.
    """

    if not _table_exists(conn, "provider_listing"):
        return

    columns = _table_columns(conn, "provider_listing")
    if "provider_id" not in columns:
        return
    # The whole point of this migration is that ``provider_id`` is
    # derivable from ``provider_exchange.provider_id`` via the
    # ``provider_exchange_id`` link. If the canonical shape is not in
    # place yet (no ``provider_exchange_id`` column, or
    # ``provider_exchange`` table missing entirely), ``provider_id`` is
    # not a derivable denormalisation here and must not be dropped.
    # Test fixtures that pin the schema at version 38 hit this branch
    # because earlier migrations left ``provider_listing`` in a partial
    # shape that the canonical-rename chain (migration 034) would
    # normally complete; bail out cleanly rather than blowing up the
    # migration chain.
    if "provider_exchange_id" not in columns or not _table_exists(
        conn, "provider_exchange"
    ):
        return

    # Confirm that every (provider_exchange_id, provider_id) on
    # provider_listing matches the provider_id that provider_exchange
    # carries; if it doesn't, the denormalisation has already drifted
    # from its source of truth and we'd lose information by dropping.
    drift = conn.execute(
        """
        SELECT COUNT(*)
        FROM provider_listing pl
        LEFT JOIN provider_exchange px
          ON px.provider_exchange_id = pl.provider_exchange_id
        WHERE px.provider_id IS NULL OR px.provider_id != pl.provider_id
        """
    ).fetchone()[0]
    if drift:
        raise RuntimeError(
            f"migration 054 aborted: {drift} provider_listing rows have "
            "provider_id that disagrees with provider_exchange. Resolve "
            "the drift before dropping the column."
        )

    conn.execute("PRAGMA defer_foreign_keys = ON")
    conn.execute("DROP INDEX IF EXISTS idx_provider_listing_provider")

    # SQLite re-validates every view on schema change. The
    # provider_listing_catalog view (and supported_tickers, which derives
    # from it) currently SELECT pl.provider_id; rebuilding the table
    # without that column would fail validation at COMMIT. Drop the
    # views, rebuild the table, then recreate the views with a join
    # through provider_exchange to recover provider_id.
    catalog_view_existed = (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name='provider_listing_catalog'"
        ).fetchone()
        is not None
    )
    supported_view_existed = (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name='supported_tickers'"
        ).fetchone()
        is not None
    )
    primary_view_existed = (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name='primary_provider_listing_catalog'"
        ).fetchone()
        is not None
    )
    # Drop dependents first; primary_provider_listing_catalog (062) is a
    # projection of provider_listing_catalog. After the rebuild we
    # recreate them in the same order they depend on each other.
    if primary_view_existed:
        conn.execute("DROP VIEW primary_provider_listing_catalog")
    if supported_view_existed:
        conn.execute("DROP VIEW supported_tickers")
    if catalog_view_existed:
        conn.execute("DROP VIEW provider_listing_catalog")

    conn.execute(
        """
        CREATE TABLE provider_listing__new (
            provider_listing_id INTEGER PRIMARY KEY,
            provider_exchange_id INTEGER NOT NULL,
            provider_symbol TEXT NOT NULL,
            listing_id INTEGER NOT NULL,
            UNIQUE (provider_exchange_id, provider_symbol),
            FOREIGN KEY (provider_exchange_id)
                REFERENCES provider_exchange(provider_exchange_id),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO provider_listing__new (
            provider_listing_id, provider_exchange_id, provider_symbol, listing_id
        )
        SELECT
            provider_listing_id, provider_exchange_id, provider_symbol, listing_id
        FROM provider_listing
        """
    )

    # Dropping and recreating the parent under the same name lets every
    # child table's FK keep pointing at the right name without an
    # explicit rewrite. defer_foreign_keys defers the integrity check
    # to COMMIT, so the gap between DROP and RENAME doesn't trigger a
    # spurious violation against the children that already reference
    # provider_listing(provider_listing_id).
    conn.execute("DROP TABLE provider_listing")
    conn.execute("ALTER TABLE provider_listing__new RENAME TO provider_listing")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_provider_listing_listing
        ON provider_listing(listing_id)
        """
    )

    if catalog_view_existed:
        conn.execute(
            """
            CREATE VIEW provider_listing_catalog AS
            SELECT
                pl.provider_listing_id,
                p.provider_id,
                p.provider_code AS provider,
                px.provider_exchange_id,
                px.provider_exchange_code,
                CASE
                    WHEN p.provider_code = 'SEC' THEN pl.provider_symbol || '.US'
                    ELSE pl.provider_symbol || '.' || px.provider_exchange_code
                END AS provider_symbol,
                pl.provider_symbol AS provider_ticker,
                l.listing_id AS security_id,
                e.exchange_code AS listing_exchange,
                i.name AS security_name,
                NULL AS security_type,
                i.country AS country,
                l.currency AS currency,
                l.primary_listing_status,
                NULL AS isin,
                NULL AS updated_at
            FROM provider_listing pl
            JOIN provider_exchange px
              ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN provider p ON p.provider_id = px.provider_id
            JOIN listing l ON l.listing_id = pl.listing_id
            JOIN issuer i ON i.issuer_id = l.issuer_id
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            """
        )
    if supported_view_existed:
        conn.execute(
            """
            CREATE VIEW supported_tickers AS
            SELECT
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
                primary_listing_status,
                isin,
                updated_at
            FROM provider_listing_catalog
            """
        )
    if primary_view_existed:
        conn.execute(
            """
            CREATE VIEW primary_provider_listing_catalog AS
            SELECT *
            FROM provider_listing_catalog
            WHERE primary_listing_status != 'secondary'
            """
        )

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 054 left foreign key violations: {fk_violations!r}"
        )


def _view_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name=?",
            (name,),
        ).fetchone()
        is not None
    )


def _create_securities_view(conn: sqlite3.Connection) -> None:
    """(Re)create the ``securities`` compat view defined in migration 044."""

    conn.execute(
        """
        CREATE VIEW securities AS
        SELECT
            l.listing_id AS security_id,
            l.symbol AS canonical_ticker,
            e.exchange_code AS canonical_exchange_code,
            l.symbol || '.' || e.exchange_code AS canonical_symbol,
            i.name AS entity_name,
            i.description,
            i.sector,
            i.industry,
            NULL AS created_at,
            NULL AS updated_at
        FROM listing l
        JOIN issuer i ON i.issuer_id = l.issuer_id
        JOIN "exchange" e ON e.exchange_id = l.exchange_id
        """
    )


def _create_exchange_provider_view(conn: sqlite3.Connection) -> None:
    """(Re)create the ``exchange_provider`` compat view from migration 044."""

    conn.execute(
        """
        CREATE VIEW exchange_provider AS
        SELECT
            p.provider_code AS provider,
            ep.provider_exchange_code,
            ep.exchange_id,
            ep.name,
            ep.country,
            ep.currency,
            ep.operating_mic,
            ep.country_iso2,
            ep.country_iso3,
            ep.updated_at
        FROM provider_exchange ep
        JOIN provider p ON p.provider_id = ep.provider_id
        """
    )


def _create_provider_listing_catalog_view(conn: sqlite3.Connection) -> None:
    """(Re)create ``provider_listing_catalog`` in its post-054 shape.

    Joins through ``provider_exchange`` to recover ``provider_id`` /
    ``provider_code`` since migration 054 dropped the denormalised
    ``provider_listing.provider_id`` column.
    """

    conn.execute(
        """
        CREATE VIEW provider_listing_catalog AS
        SELECT
            pl.provider_listing_id,
            p.provider_id,
            p.provider_code AS provider,
            px.provider_exchange_id,
            px.provider_exchange_code,
            CASE
                WHEN p.provider_code = 'SEC' THEN pl.provider_symbol || '.US'
                ELSE pl.provider_symbol || '.' || px.provider_exchange_code
            END AS provider_symbol,
            pl.provider_symbol AS provider_ticker,
            l.listing_id AS security_id,
            e.exchange_code AS listing_exchange,
            i.name AS security_name,
            NULL AS security_type,
            i.country AS country,
            l.currency AS currency,
            l.primary_listing_status,
            NULL AS isin,
            NULL AS updated_at
        FROM provider_listing pl
        JOIN provider_exchange px
          ON px.provider_exchange_id = pl.provider_exchange_id
        JOIN provider p ON p.provider_id = px.provider_id
        JOIN listing l ON l.listing_id = pl.listing_id
        JOIN issuer i ON i.issuer_id = l.issuer_id
        JOIN "exchange" e ON e.exchange_id = l.exchange_id
        """
    )


def _create_supported_tickers_view(conn: sqlite3.Connection) -> None:
    """(Re)create ``supported_tickers`` (projection over the catalog view)."""

    conn.execute(
        """
        CREATE VIEW supported_tickers AS
        SELECT
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
            primary_listing_status,
            isin,
            updated_at
        FROM provider_listing_catalog
        """
    )


def _create_primary_provider_listing_catalog_view(conn: sqlite3.Connection) -> None:
    """(Re)create ``primary_provider_listing_catalog`` from migration 062.

    A pre-filtered projection of ``provider_listing_catalog`` that
    excludes secondary listings, so callers can query the view directly
    instead of inlining the predicate.
    """

    conn.execute(
        """
        CREATE VIEW primary_provider_listing_catalog AS
        SELECT *
        FROM provider_listing_catalog
        WHERE primary_listing_status != 'secondary'
        """
    )


def _migration_055_status_enum_checks(conn: sqlite3.Connection) -> None:
    """Add CHECK constraints for status / source_kind enum columns.

    Audit finding 3.3: the following columns hold a small fixed set of
    values in practice but had no schema-level enforcement.

    * ``metric_compute_status.status``: only ``'success'`` / ``'failure'``
      ever written.
    * ``market_data_fetch_state.last_status``: ``'ok'`` / ``'error'`` (or
      ``NULL`` before the first fetch).
    * ``fx_refresh_state.last_status``: same pattern as above.

    Each CHECK is encoded as part of a temp-table rebuild because SQLite
    does not support ``ALTER TABLE ADD CHECK``. ``defer_foreign_keys``
    keeps child FKs intact across the rebuild.

    ``fx_rates.source_kind`` is deferred to migration 058, which also
    rebuilds ``fx_rates`` to add currency-format CHECKs in the same
    pass.
    """

    if _table_exists(conn, "metric_compute_status"):
        ddl_row = conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'table' AND name = 'metric_compute_status'"
        ).fetchone()
        if ddl_row and "CHECK (status IN ('success', 'failure'))" not in ddl_row[0]:
            conn.execute("PRAGMA defer_foreign_keys = ON")
            conn.execute("DROP INDEX IF EXISTS idx_metric_compute_status_metric_status")
            conn.execute(
                """
                CREATE TABLE metric_compute_status__new (
                    listing_id INTEGER NOT NULL,
                    metric_id TEXT NOT NULL,
                    status TEXT NOT NULL
                        CHECK (status IN ('success', 'failure')),
                    reason_code TEXT,
                    reason_detail TEXT,
                    attempted_at TEXT NOT NULL,
                    value_as_of TEXT,
                    facts_refreshed_at TEXT,
                    market_data_as_of TEXT,
                    market_data_updated_at TEXT,
                    PRIMARY KEY (listing_id, metric_id),
                    FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO metric_compute_status__new (
                    listing_id, metric_id, status, reason_code, reason_detail,
                    attempted_at, value_as_of, facts_refreshed_at,
                    market_data_as_of, market_data_updated_at
                )
                SELECT
                    listing_id, metric_id, status, reason_code, reason_detail,
                    attempted_at, value_as_of, facts_refreshed_at,
                    market_data_as_of, market_data_updated_at
                FROM metric_compute_status
                """
            )
            conn.execute("DROP TABLE metric_compute_status")
            conn.execute(
                "ALTER TABLE metric_compute_status__new RENAME TO metric_compute_status"
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_metric_compute_status_metric_status
                ON metric_compute_status(metric_id, status)
                """
            )

    if _table_exists(conn, "market_data_fetch_state"):
        ddl_row = conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'table' AND name = 'market_data_fetch_state'"
        ).fetchone()
        if ddl_row and "CHECK (last_status IN ('ok', 'error'))" not in ddl_row[0]:
            conn.execute("PRAGMA defer_foreign_keys = ON")
            conn.execute("DROP INDEX IF EXISTS idx_market_data_fetch_next")
            conn.execute(
                """
                CREATE TABLE market_data_fetch_state__new (
                    provider_listing_id INTEGER NOT NULL PRIMARY KEY,
                    last_fetched_at TEXT,
                    last_status TEXT
                        CHECK (last_status IS NULL
                               OR last_status IN ('ok', 'error')),
                    last_error TEXT,
                    next_eligible_at TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0
                        CHECK (attempts >= 0),
                    FOREIGN KEY (provider_listing_id)
                        REFERENCES provider_listing(provider_listing_id)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO market_data_fetch_state__new (
                    provider_listing_id, last_fetched_at, last_status,
                    last_error, next_eligible_at, attempts
                )
                SELECT
                    provider_listing_id, last_fetched_at, last_status,
                    last_error, next_eligible_at, attempts
                FROM market_data_fetch_state
                """
            )
            conn.execute("DROP TABLE market_data_fetch_state")
            conn.execute(
                "ALTER TABLE market_data_fetch_state__new "
                "RENAME TO market_data_fetch_state"
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_market_data_fetch_next
                ON market_data_fetch_state(next_eligible_at)
                """
            )

    if _table_exists(conn, "fx_refresh_state"):
        ddl_row = conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'table' AND name = 'fx_refresh_state'"
        ).fetchone()
        if ddl_row and "CHECK (last_status IN ('ok', 'error'))" not in ddl_row[0]:
            fk_already_present = bool(
                conn.execute("PRAGMA foreign_key_list(fx_refresh_state)").fetchall()
            )
            conn.execute("PRAGMA defer_foreign_keys = ON")
            fk_clause = (
                ",\n            FOREIGN KEY (provider) REFERENCES provider(provider_code)"
                if fk_already_present
                else ""
            )
            conn.execute(
                f"""
                CREATE TABLE fx_refresh_state__new (
                    provider TEXT NOT NULL,
                    canonical_symbol TEXT NOT NULL,
                    min_rate_date TEXT,
                    max_rate_date TEXT,
                    full_history_backfilled INTEGER NOT NULL DEFAULT 0
                        CHECK (full_history_backfilled IN (0, 1)),
                    last_fetched_at TEXT,
                    last_status TEXT
                        CHECK (last_status IS NULL
                               OR last_status IN ('ok', 'error')),
                    last_error TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0
                        CHECK (attempts >= 0),
                    PRIMARY KEY (provider, canonical_symbol){fk_clause}
                )
                """
            )
            conn.execute(
                """
                INSERT INTO fx_refresh_state__new (
                    provider, canonical_symbol, min_rate_date, max_rate_date,
                    full_history_backfilled, last_fetched_at, last_status,
                    last_error, attempts
                )
                SELECT
                    provider, canonical_symbol, min_rate_date, max_rate_date,
                    full_history_backfilled, last_fetched_at, last_status,
                    last_error, attempts
                FROM fx_refresh_state
                """
            )
            conn.execute("DROP TABLE fx_refresh_state")
            conn.execute("ALTER TABLE fx_refresh_state__new RENAME TO fx_refresh_state")

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 055 left foreign key violations: {fk_violations!r}"
        )


# Currency-code format predicate used by migrations 056-059.
# All currency columns store either a 3-char ISO-4217 code (USD, EUR,
# ...) or one of the project's accepted subunit codes (GBX = pence,
# ZAC = South African cent, ILA = Israeli agora). All three are exactly
# 3 uppercase ASCII letters, so a single GLOB suffices.
_CURRENCY_FORMAT_CHECK = (
    "length({col}) = 3 AND {col} = upper({col}) AND {col} GLOB '[A-Z][A-Z][A-Z]'"
)


# Major-currency predicate for ``financial_facts.currency`` (migration 071+).
# Subunit codes (GBX pence, ZAC cent, ILA agora — and the 7-char GBP0.01
# alias) must never enter the data boundary: the EODHD normalizer collapses
# every subunit amount to its base currency before a fact is built (see
# ``currency.normalize_monetary_amount``). This extends the ISO-shape check
# with an explicit subunit exclusion so the schema itself rejects any subunit
# code that slips through. GBP0.01 is already excluded by the length=3 clause;
# it is listed for parity with the migration-070 price-scaling registry.
_MAJOR_CURRENCY_CHECK = (
    _CURRENCY_FORMAT_CHECK + " AND {col} NOT IN ('GBX', 'GBP0.01', 'ZAC', 'ILA')"
)


def _migration_056_listing_format_checks(conn: sqlite3.Connection) -> None:
    """Add format CHECKs to ``listing.symbol`` and ``listing.currency``.

    Audit finding 3.3:

    * ``listing.symbol`` was declared NOT NULL but otherwise unconstrained.
      Live values are uppercase tickers, sometimes containing ``&``
      (B-corp tickers like ``F&D``) or ``^`` (index tickers like
      ``^SET``), but always non-empty and never containing whitespace.
    * ``listing.currency`` had no length / uppercase / ISO-shape check.
      Live values are 3-char ISO-4217 codes plus the accepted subunits
      ``GBX``, ``ZAC``, ``ILA``.

    The CHECKs encode those invariants. ``listing`` is referenced by
    several child tables (``financial_facts``, ``metric_compute_status``,
    ``metrics``, ``provider_listing``); the rebuild defers FK
    enforcement to commit time so the children aren't disturbed.
    """

    if not _table_exists(conn, "listing"):
        return

    columns = _table_columns(conn, "listing")
    # Some test fixtures pin the schema at versions before the canonical
    # listing shape (issuer_id / exchange_id) is in place. Skip cleanly
    # in that case — the rebuild only makes sense once those columns
    # exist; earlier migrations are responsible for getting them there.
    if not {"issuer_id", "exchange_id", "symbol"} <= columns:
        return

    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='listing'"
    ).fetchone()
    if ddl_row is None:
        return
    existing_ddl = ddl_row[0]
    if "CHECK (length(symbol)" in existing_ddl:
        return

    conn.execute("DROP INDEX IF EXISTS idx_listing_exchange")
    conn.execute("DROP INDEX IF EXISTS idx_listing_currency_nonnull")

    # SQLite re-validates every view on schema change. The views from
    # migrations 042/044 reference ``listing l`` directly; rebuilding
    # the table without dropping them first fails at the RENAME with
    # "no such table: main.listing". Drop them up front and recreate
    # with the same DDL post-rebuild so future-readers see no shape
    # change.
    catalog_view_existed = _view_exists(conn, "provider_listing_catalog")
    supported_view_existed = _view_exists(conn, "supported_tickers")
    securities_view_existed = _view_exists(conn, "securities")
    primary_view_existed = _view_exists(conn, "primary_provider_listing_catalog")
    # Drop dependents before parents (migration 062's
    # primary_provider_listing_catalog projects from
    # provider_listing_catalog).
    if primary_view_existed:
        conn.execute("DROP VIEW primary_provider_listing_catalog")
    if supported_view_existed:
        conn.execute("DROP VIEW supported_tickers")
    if catalog_view_existed:
        conn.execute("DROP VIEW provider_listing_catalog")
    if securities_view_existed:
        conn.execute("DROP VIEW securities")

    has_primary_listing_status = "primary_listing_status" in columns
    primary_status_column = (
        ",\n            primary_listing_status TEXT NOT NULL DEFAULT 'unknown'"
        if has_primary_listing_status
        else ""
    )
    primary_status_select = (
        ", primary_listing_status" if has_primary_listing_status else ""
    )

    currency_check = _CURRENCY_FORMAT_CHECK.format(col="currency")
    conn.execute(
        f"""
        CREATE TABLE listing__new (
            listing_id INTEGER PRIMARY KEY,
            issuer_id INTEGER NOT NULL,
            exchange_id INTEGER NOT NULL,
            symbol TEXT NOT NULL
                CHECK (length(symbol) > 0
                       AND symbol = upper(trim(symbol))
                       AND instr(symbol, ' ') = 0
                       AND symbol GLOB '[A-Z0-9.&^*-]*'),
            currency TEXT
                CHECK (currency IS NULL OR ({currency_check})){primary_status_column},
            UNIQUE (exchange_id, symbol),
            FOREIGN KEY (issuer_id) REFERENCES issuer(issuer_id),
            FOREIGN KEY (exchange_id) REFERENCES "exchange"(exchange_id)
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO listing__new (
            listing_id, issuer_id, exchange_id, symbol, currency{primary_status_select}
        )
        SELECT
            listing_id, issuer_id, exchange_id, symbol, currency{primary_status_select}
        FROM listing
        """
    )
    conn.execute("DROP TABLE listing")
    conn.execute("ALTER TABLE listing__new RENAME TO listing")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_listing_exchange
        ON listing(exchange_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_listing_currency_nonnull
        ON listing(currency)
        WHERE currency IS NOT NULL
        """
    )

    if securities_view_existed:
        _create_securities_view(conn)
    if catalog_view_existed:
        _create_provider_listing_catalog_view(conn)
    if supported_view_existed:
        _create_supported_tickers_view(conn)
    if primary_view_existed:
        _create_primary_provider_listing_catalog_view(conn)

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 056 left foreign key violations: {fk_violations!r}"
        )


def _migration_057_provider_exchange_currency_check(
    conn: sqlite3.Connection,
) -> None:
    """Clean ``'UNKNOWN'`` placeholder currencies and add a format CHECK.

    Audit finding 3.3: ``provider_exchange.currency`` had no shape
    constraint. Live data was clean except for three rows whose
    currency was the literal ``'UNKNOWN'`` (FOREX / GBOND / MONEY
    virtual exchanges). Those exchanges don't have a real quote
    currency; the column is already nullable, so coercing to ``NULL``
    matches what callers already handle.

    Migration steps:
    1. Coerce ``UNKNOWN`` → ``NULL`` (data cleanup).
    2. Rebuild the table with the format CHECK.
    """

    if not _table_exists(conn, "provider_exchange"):
        return

    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type = 'table' AND name = 'provider_exchange'"
    ).fetchone()
    if ddl_row is None:
        return
    existing_ddl = ddl_row[0]
    if "CHECK (currency IS NULL OR (length(currency)" in existing_ddl:
        return

    conn.execute(
        "UPDATE provider_exchange SET currency = NULL WHERE currency = 'UNKNOWN'"
    )

    conn.execute("PRAGMA defer_foreign_keys = ON")
    conn.execute("DROP INDEX IF EXISTS idx_provider_exchange_exchange")

    # provider_listing_catalog (042), supported_tickers (042), and
    # exchange_provider (044) all reference provider_exchange directly.
    # SQLite re-validates them on schema change; drop and recreate
    # around the rebuild. primary_provider_listing_catalog (062)
    # depends on the catalog so it must come down first.
    catalog_view_existed = _view_exists(conn, "provider_listing_catalog")
    supported_view_existed = _view_exists(conn, "supported_tickers")
    exchange_provider_view_existed = _view_exists(conn, "exchange_provider")
    primary_view_existed = _view_exists(conn, "primary_provider_listing_catalog")
    if primary_view_existed:
        conn.execute("DROP VIEW primary_provider_listing_catalog")
    if supported_view_existed:
        conn.execute("DROP VIEW supported_tickers")
    if catalog_view_existed:
        conn.execute("DROP VIEW provider_listing_catalog")
    if exchange_provider_view_existed:
        conn.execute("DROP VIEW exchange_provider")

    currency_check = _CURRENCY_FORMAT_CHECK.format(col="currency")
    conn.execute(
        f"""
        CREATE TABLE provider_exchange__new (
            provider_exchange_id INTEGER PRIMARY KEY,
            provider_id INTEGER NOT NULL,
            provider_exchange_code TEXT NOT NULL,
            exchange_id INTEGER NOT NULL,
            name TEXT,
            country TEXT,
            currency TEXT
                CHECK (currency IS NULL OR ({currency_check})),
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
        INSERT INTO provider_exchange__new (
            provider_exchange_id, provider_id, provider_exchange_code,
            exchange_id, name, country, currency, operating_mic,
            country_iso2, country_iso3, updated_at
        )
        SELECT
            provider_exchange_id, provider_id, provider_exchange_code,
            exchange_id, name, country, currency, operating_mic,
            country_iso2, country_iso3, updated_at
        FROM provider_exchange
        """
    )
    conn.execute("DROP TABLE provider_exchange")
    conn.execute("ALTER TABLE provider_exchange__new RENAME TO provider_exchange")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_provider_exchange_exchange
        ON provider_exchange(exchange_id)
        """
    )

    if exchange_provider_view_existed:
        _create_exchange_provider_view(conn)
    if catalog_view_existed:
        _create_provider_listing_catalog_view(conn)
    if supported_view_existed:
        _create_supported_tickers_view(conn)
    if primary_view_existed:
        _create_primary_provider_listing_catalog_view(conn)

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 057 left foreign key violations: {fk_violations!r}"
        )


def _migration_058_fx_rates_format_checks(conn: sqlite3.Connection) -> None:
    """Add ``source_kind``, ``base_currency`` and ``quote_currency`` CHECKs to ``fx_rates``.

    Audit finding 3.3:
    * ``fx_rates.source_kind``: only ``'provider'`` is ever written today;
      future direct/synthetic sources will land via a new migration that
      widens the CHECK explicitly.
    * ``fx_rates.base_currency`` / ``quote_currency``: 3-char ISO codes
      in practice; the existing schema didn't enforce that.

    Same temp-table rebuild pattern as migrations 048 and 045.
    """

    if not _table_exists(conn, "fx_rates"):
        return

    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='fx_rates'"
    ).fetchone()
    if ddl_row is None:
        return
    existing_ddl = ddl_row[0]
    if "CHECK (source_kind IN ('provider'))" in existing_ddl:
        return

    fk_already_present = bool(
        conn.execute("PRAGMA foreign_key_list(fx_rates)").fetchall()
    )
    fk_clause = (
        ",\n            FOREIGN KEY (provider) REFERENCES provider(provider_code)"
        if fk_already_present
        else ""
    )

    columns = _table_columns(conn, "fx_rates")
    rate_column = "rate" if "rate" in columns else "rate_text"
    rate_decl = (
        "rate REAL NOT NULL" if rate_column == "rate" else "rate_text TEXT NOT NULL"
    )

    conn.execute("DROP INDEX IF EXISTS idx_fx_rates_pair_date")

    base_check = _CURRENCY_FORMAT_CHECK.format(col="base_currency")
    quote_check = _CURRENCY_FORMAT_CHECK.format(col="quote_currency")
    meta_column_decl = ",\n            meta_json TEXT" if "meta_json" in columns else ""
    meta_select = ", meta_json" if "meta_json" in columns else ""

    conn.execute(
        f"""
        CREATE TABLE fx_rates__new (
            provider TEXT NOT NULL,
            rate_date TEXT NOT NULL,
            base_currency TEXT NOT NULL CHECK ({base_check}),
            quote_currency TEXT NOT NULL CHECK ({quote_check}),
            {rate_decl},
            fetched_at TEXT NOT NULL,
            source_kind TEXT NOT NULL
                CHECK (source_kind IN ('provider')){meta_column_decl},
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (
                provider, rate_date, base_currency, quote_currency
            ){fk_clause}
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO fx_rates__new (
            provider, rate_date, base_currency, quote_currency,
            {rate_column}, fetched_at, source_kind{meta_select},
            created_at, updated_at
        )
        SELECT
            provider, rate_date, base_currency, quote_currency,
            {rate_column}, fetched_at, source_kind{meta_select},
            created_at, updated_at
        FROM fx_rates
        """
    )
    conn.execute("DROP TABLE fx_rates")
    conn.execute("ALTER TABLE fx_rates__new RENAME TO fx_rates")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fx_rates_pair_date
        ON fx_rates(provider, base_currency, quote_currency, rate_date DESC)
        """
    )

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 058 left foreign key violations: {fk_violations!r}"
        )


def _migration_059_financial_facts_format_checks(
    conn: sqlite3.Connection,
) -> None:
    """Add ``currency`` and ``unit`` format CHECKs to ``financial_facts``.

    Audit finding 3.3: ``financial_facts.currency`` was unconstrained
    (live values are 3-char ISO codes including subunits GBX/ILA);
    ``financial_facts.unit`` was NOT NULL but empty / whitespace
    values were possible (one such row exists on the live DB).

    The CHECKs:
    * ``currency`` — NULL or 3-char uppercase ISO form (same predicate
      as listing/provider_exchange/fx_rates).
    * ``unit`` — non-empty after trim and free of internal whitespace.
      ``unit`` is a free-form metadata field that varies with the source
      payload (ISO codes, ``shares``, ``EPS``, composites such as
      ``USD/shares``); a strict enum would lock out future sources, so
      the CHECK targets the *shape* invariants instead.

    **Cost note:** ``financial_facts`` is the largest table in the
    project (live DB: 103M rows, ~8.5 GiB). The temp-table rebuild this
    migration performs is the same shape as migration 043 (which ran
    in ~72 minutes on production). Apply on a quiet window.

    The migration aborts cleanly if any orphan rows exist or if any
    row would violate either CHECK; the latter is a pre-flight rather
    than a silent INSERT...SELECT filter so dirty data is surfaced
    rather than dropped.
    """

    if not _table_exists(conn, "financial_facts"):
        return

    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type = 'table' AND name = 'financial_facts'"
    ).fetchone()
    if ddl_row is None:
        return
    existing_ddl = ddl_row[0]
    if "CHECK (currency IS NULL OR" in existing_ddl:
        return

    orphan_count = conn.execute(
        """
        SELECT COUNT(*) FROM financial_facts
        WHERE listing_id NOT IN (SELECT listing_id FROM listing)
        """
    ).fetchone()[0]
    if orphan_count:
        raise RuntimeError(
            f"migration 059 aborted: {orphan_count} orphan financial_facts "
            "rows reference missing listings. Clean these before retrying."
        )

    bad_currency = conn.execute(
        f"""
        SELECT COUNT(*) FROM financial_facts
        WHERE currency IS NOT NULL
          AND NOT ({_CURRENCY_FORMAT_CHECK.format(col="currency")})
        """
    ).fetchone()[0]
    if bad_currency:
        raise RuntimeError(
            f"migration 059 aborted: {bad_currency} financial_facts rows "
            "have a currency that fails the 3-char uppercase shape check."
        )

    bad_unit = conn.execute(
        """
        SELECT COUNT(*) FROM financial_facts
        WHERE unit IS NULL
           OR length(trim(unit)) = 0
           OR instr(unit, ' ') > 0
           OR instr(unit, char(9)) > 0
           OR instr(unit, char(10)) > 0
        """
    ).fetchone()[0]
    if bad_unit:
        raise RuntimeError(
            f"migration 059 aborted: {bad_unit} financial_facts rows have "
            "an empty or whitespace-bearing unit. Clean these before "
            "retrying."
        )

    conn.execute("PRAGMA defer_foreign_keys = ON")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_concept")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_security_concept")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_security_concept_latest")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_currency_nonnull")

    currency_check = _CURRENCY_FORMAT_CHECK.format(col="currency")
    conn.execute(
        f"""
        CREATE TABLE financial_facts__new (
            listing_id INTEGER NOT NULL,
            cik TEXT,
            concept TEXT NOT NULL,
            fiscal_period TEXT,
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
            currency TEXT
                CHECK (currency IS NULL OR ({currency_check})),
            source_provider TEXT,
            PRIMARY KEY (listing_id, concept, fiscal_period, end_date, unit),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO financial_facts__new (
            listing_id, cik, concept, fiscal_period, end_date, unit,
            value, accn, filed, frame, start_date, accounting_standard,
            currency, source_provider
        )
        SELECT
            listing_id, cik, concept, fiscal_period, end_date, unit,
            value, accn, filed, frame, start_date, accounting_standard,
            currency, source_provider
        FROM financial_facts
        """
    )
    conn.execute("DROP TABLE financial_facts")
    conn.execute("ALTER TABLE financial_facts__new RENAME TO financial_facts")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fin_facts_concept
        ON financial_facts(concept)
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

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 059 left foreign key violations: {fk_violations!r}"
        )


def _migration_060_issuer_unique_name_country(conn: sqlite3.Connection) -> None:
    """Deduplicate ``issuer`` by ``(name, country)`` and add a UNIQUE index.

    Audit finding 3.7: ``issuer`` had no UNIQUE on the natural identity
    columns. Two issuers with the same canonical name and country are
    almost certainly the same entity, and tightening the constraint
    keeps duplicates from creeping in via parallel ingest paths.

    The ingest path in ``SecurityRepository.ensure`` keys its existence
    check on ``(exchange_id, symbol)`` rather than ``(name, country)``,
    so the same real-world issuer (e.g. Petrobras on 22 German venues,
    or one Korean ticker on KOSPI + KOSDAQ) accumulates one ``issuer``
    row per listing instead of one row per entity. The live DB had
    ~4,696 such groups (~13,121 duplicate rows, ~8,425 listings to
    remap) at the time this migration was authored.

    Dedup rule:
      * Canonical row per group = the row with the lowest ``issuer_id``.
        That preserves the oldest surrogate-key reference and matches
        the order in which ingest first observed the entity.
      * For nullable columns (``description``, ``sector``, ``industry``)
        the canonical's own value wins when non-NULL; otherwise the
        first non-NULL across the rest of the group (by ascending
        ``issuer_id``) is promoted. ``COALESCE``-style backfill — no
        column value is ever overwritten, only filled.
      * Listings pointing at non-canonical issuers are remapped to the
        canonical id (``listing.issuer_id`` is the only physical FK to
        ``issuer``).
      * Non-canonical issuer rows are then deleted.

    NULL semantics: a row with NULL ``name`` or NULL ``country`` is
    skipped by the dedup. SQLite's UNIQUE INDEX treats NULLs as
    distinct, so those rows do not block the index, and merging them
    on a NULL key would conflate unrelated companies (the live DB has
    a 260-row "group" of NULL/NULL closed-end-fund issuers whose
    listings are unrelated and must stay separate). They remain as
    legitimate-under-the-constraint duplicates until a future
    migration backfills their metadata.

    The UNIQUE INDEX is applied after the dedup. SQLite re-validates
    views referencing ``issuer`` on the next query, not on DML, so
    no view drop/recreate is needed here.
    """

    if not _table_exists(conn, "issuer"):
        return
    if not _table_exists(conn, "listing"):
        return

    # Identify duplicate groups where both name and country are non-NULL.
    # SQLite's UNIQUE INDEX semantics treat NULLs as distinct, so rows
    # with a NULL component cannot violate the constraint regardless of
    # how many siblings they have.
    groups = conn.execute(
        """
        SELECT name, country, COUNT(*) AS n
        FROM issuer
        WHERE name IS NOT NULL AND country IS NOT NULL
        GROUP BY name, country
        HAVING n > 1
        """
    ).fetchall()

    if groups:
        # Build a (old_id -> canonical_id) mapping for every non-canonical
        # row in every duplicate group. We use a TEMP table so the UPDATE
        # against ``listing`` can join it directly without dragging the
        # whole mapping into Python memory.
        conn.execute("DROP TABLE IF EXISTS issuer_dedup_map_060")
        conn.execute(
            """
            CREATE TEMP TABLE issuer_dedup_map_060 (
                old_id INTEGER PRIMARY KEY,
                canonical_id INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO issuer_dedup_map_060 (old_id, canonical_id)
            SELECT i.issuer_id, c.canonical_id
            FROM issuer i
            JOIN (
                SELECT name, country, MIN(issuer_id) AS canonical_id
                FROM issuer
                WHERE name IS NOT NULL AND country IS NOT NULL
                GROUP BY name, country
                HAVING COUNT(*) > 1
            ) c ON c.name = i.name AND c.country = i.country
            WHERE i.name IS NOT NULL AND i.country IS NOT NULL
              AND i.issuer_id != c.canonical_id
            """
        )

        # Promote the first non-NULL ``description`` / ``sector`` /
        # ``industry`` from any non-canonical row onto the canonical row,
        # using COALESCE so an existing non-NULL value is never overwritten.
        # Each column is backfilled independently — a non-canonical row may
        # contribute its industry while a different one contributes the
        # description.
        for column in ("description", "sector", "industry"):
            conn.execute(
                f"""
                UPDATE issuer
                SET {column} = COALESCE(
                    {column},
                    (
                        SELECT src.{column}
                        FROM issuer src
                        JOIN issuer_dedup_map_060 m ON m.old_id = src.issuer_id
                        WHERE m.canonical_id = issuer.issuer_id
                          AND src.{column} IS NOT NULL
                        ORDER BY src.issuer_id
                        LIMIT 1
                    )
                )
                WHERE issuer_id IN (
                    SELECT DISTINCT canonical_id FROM issuer_dedup_map_060
                )
                """
            )

        # Remap listings off the soon-to-be-deleted issuer rows.
        conn.execute(
            """
            UPDATE listing
            SET issuer_id = (
                SELECT canonical_id
                FROM issuer_dedup_map_060
                WHERE old_id = listing.issuer_id
            )
            WHERE issuer_id IN (SELECT old_id FROM issuer_dedup_map_060)
            """
        )

        # Drop the non-canonical issuer rows.
        conn.execute(
            """
            DELETE FROM issuer
            WHERE issuer_id IN (SELECT old_id FROM issuer_dedup_map_060)
            """
        )

        conn.execute("DROP TABLE issuer_dedup_map_060")

    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_issuer_name_country
        ON issuer(name, country)
        """
    )


def _migration_061_market_data_fetch_state_error_invariant(
    conn: sqlite3.Connection,
) -> None:
    """Tighten ``market_data_fetch_state`` so error rows must carry an error.

    Audit P3 #13: ``fundamentals_fetch_state`` (migration 040) keeps a
    tight invariant that a failure row must have ``failed_at`` and
    ``error`` populated. ``market_data_fetch_state`` is wider (one row
    per provider listing, not just on failure) but the same idea
    applies to its error rows: ``last_status = 'error'`` is meaningless
    without ``last_error``.

    Live data confirms zero error rows currently violate this; 230
    error rows have NULL ``last_fetched_at`` (a fetch that never
    succeeded), so this column is **not** tightened.
    """

    if not _table_exists(conn, "market_data_fetch_state"):
        return

    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type = 'table' AND name = 'market_data_fetch_state'"
    ).fetchone()
    if ddl_row is None:
        return
    if "last_status != 'error' OR last_error IS NOT NULL" in ddl_row[0]:
        return

    bad = conn.execute(
        """
        SELECT COUNT(*) FROM market_data_fetch_state
        WHERE last_status = 'error' AND last_error IS NULL
        """
    ).fetchone()[0]
    if bad:
        raise RuntimeError(
            f"migration 061 aborted: {bad} market_data_fetch_state rows "
            "have last_status='error' with NULL last_error. Backfill or "
            "clear them before retrying."
        )

    conn.execute("DROP INDEX IF EXISTS idx_market_data_fetch_next")
    conn.execute(
        """
        CREATE TABLE market_data_fetch_state__new (
            provider_listing_id INTEGER NOT NULL PRIMARY KEY,
            last_fetched_at TEXT,
            last_status TEXT
                CHECK (last_status IS NULL
                       OR last_status IN ('ok', 'error')),
            last_error TEXT,
            next_eligible_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0
                CHECK (attempts >= 0),
            CHECK (last_status != 'error' OR last_error IS NOT NULL),
            FOREIGN KEY (provider_listing_id)
                REFERENCES provider_listing(provider_listing_id)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO market_data_fetch_state__new (
            provider_listing_id, last_fetched_at, last_status,
            last_error, next_eligible_at, attempts
        )
        SELECT
            provider_listing_id, last_fetched_at, last_status,
            last_error, next_eligible_at, attempts
        FROM market_data_fetch_state
        """
    )
    conn.execute("DROP TABLE market_data_fetch_state")
    conn.execute(
        "ALTER TABLE market_data_fetch_state__new RENAME TO market_data_fetch_state"
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_data_fetch_next
        ON market_data_fetch_state(next_eligible_at)
        """
    )


def _migration_063_schema_migrations_pk_and_guard(
    conn: sqlite3.Connection,
) -> None:
    """Tighten ``schema_migrations`` to a single-row, PK-guarded shape.

    Audit P3 #12: the original table was
    ``CREATE TABLE schema_migrations (version INTEGER NOT NULL)`` with
    no PK and no row-count guard, relying on application discipline to
    keep exactly one row. The new shape adds ``id INTEGER PRIMARY KEY
    CHECK (id = 1)`` so duplicate version markers can't accumulate
    (a second insert collides on the PK; a hand-edit to ``id <> 1``
    fails the CHECK).

    The migration is meta — we're rewriting the table that tracks
    migrations themselves. The framework re-records the version after
    the migration body via ``_set_version``, which works against both
    shapes (its INSERT omits ``id`` and SQLite auto-picks the rowid).
    """

    if not _table_exists(conn, "schema_migrations"):
        return

    columns = _table_columns(conn, "schema_migrations")
    if "id" in columns:
        # Already migrated.
        return

    # Preserve the current version marker through the rebuild.
    row = conn.execute("SELECT version FROM schema_migrations LIMIT 1").fetchone()
    current_version = int(row[0]) if row else 0

    conn.execute(
        """
        CREATE TABLE schema_migrations__new (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        "INSERT INTO schema_migrations__new (id, version) VALUES (1, ?)",
        (current_version,),
    )
    conn.execute("DROP TABLE schema_migrations")
    conn.execute("ALTER TABLE schema_migrations__new RENAME TO schema_migrations")


def _migration_062_primary_provider_listing_catalog_view(
    conn: sqlite3.Connection,
) -> None:
    """Add ``primary_provider_listing_catalog`` view for the primary-only filter.

    Audit P3 #10: 8+ call sites in ``storage.py`` inline
    ``catalog.primary_listing_status <> 'secondary'`` via the
    ``_primary_listing_predicate()`` helper. CLAUDE.md: *Do not repeat
    logic that belongs in the schema.* A view consolidates the filter
    so callers can ``FROM primary_provider_listing_catalog catalog``
    instead of remembering the predicate.

    The view depends on ``provider_listing_catalog`` from migration 042
    (rebuilt by migration 054). Created only when the parent view
    exists; otherwise this is a no-op so older fixtures stay healthy.
    """

    if not _view_exists(conn, "provider_listing_catalog"):
        return
    if _view_exists(conn, "primary_provider_listing_catalog"):
        return

    conn.execute(
        """
        CREATE VIEW primary_provider_listing_catalog AS
        SELECT *
        FROM provider_listing_catalog
        WHERE primary_listing_status != 'secondary'
        """
    )


def _migration_064_drop_orphan_issuers_tighten_name(
    conn: sqlite3.Connection,
) -> None:
    """Delete orphan NULL-name issuers and require ``issuer.name``.

    The legacy ingest path created 260 ``issuer`` rows with NULL
    ``name`` plus matching ``listing`` rows that were never registered
    in ``provider_listing`` (SEC closed-end funds whose canonical
    promotion didn't complete). On the live DB those orphans carry no
    provider_listing, no fundamentals, no metrics — only stale
    ``market_data``. With no provider mapping there is no path to
    refresh them, so they are pure dead weight.

    This migration:
    1. Identifies issuers with ``name IS NULL``.
    2. Asserts (pre-flight) that none of them have any
       ``provider_listing``, ``financial_facts``, ``metrics``, or
       ``metric_compute_status`` references; if they do, the migration
       aborts so the operator can investigate.
    3. Deletes the corresponding ``market_data`` rows, then the
       ``listing`` rows, then the ``issuer`` rows.
    4. Rebuilds ``issuer`` with ``name TEXT NOT NULL``, preserving the
       UNIQUE INDEX (name, country) from migration 060.

    Fresh DBs without orphans (e.g. unit-test fixtures, freshly
    initialised production DBs) skip the deletion and go straight to
    the rebuild.
    """

    if not _table_exists(conn, "issuer"):
        return

    columns = _table_columns(conn, "issuer")
    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='issuer'"
    ).fetchone()
    if ddl_row is None:
        return
    if "name TEXT NOT NULL" in ddl_row[0]:
        # Already migrated.
        return
    if "name" not in columns:
        # Fixture without the canonical issuer shape; earlier migrations
        # are responsible for getting it there.
        return

    orphan_issuer_ids = [
        int(row[0])
        for row in conn.execute(
            "SELECT issuer_id FROM issuer WHERE name IS NULL"
        ).fetchall()
    ]
    if orphan_issuer_ids:
        placeholders = ", ".join("?" for _ in orphan_issuer_ids)
        orphan_listing_ids = [
            int(row[0])
            for row in conn.execute(
                f"""
                SELECT listing_id FROM listing
                WHERE issuer_id IN ({placeholders})
                """,
                orphan_issuer_ids,
            ).fetchall()
        ]
        for table_name, fk_column in (
            ("provider_listing", "listing_id"),
            ("financial_facts", "listing_id"),
            ("metrics", "listing_id"),
            ("metric_compute_status", "listing_id"),
            ("financial_facts_refresh_state", "listing_id"),
        ):
            if not _table_exists(conn, table_name):
                continue
            if not orphan_listing_ids:
                continue
            listing_placeholders = ", ".join("?" for _ in orphan_listing_ids)
            offending = conn.execute(
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE {fk_column} IN ({listing_placeholders})",
                orphan_listing_ids,
            ).fetchone()[0]
            if offending:
                raise RuntimeError(
                    f"migration 064 aborted: {offending} {table_name} rows "
                    f"reference orphan NULL-name issuers' listings. Resolve "
                    "the references before retrying."
                )

        if orphan_listing_ids and _table_exists(conn, "market_data"):
            listing_placeholders = ", ".join("?" for _ in orphan_listing_ids)
            conn.execute(
                f"DELETE FROM market_data WHERE listing_id IN ({listing_placeholders})",
                orphan_listing_ids,
            )
        if orphan_listing_ids:
            listing_placeholders = ", ".join("?" for _ in orphan_listing_ids)
            conn.execute(
                f"DELETE FROM listing WHERE listing_id IN ({listing_placeholders})",
                orphan_listing_ids,
            )
        conn.execute(
            f"DELETE FROM issuer WHERE issuer_id IN ({placeholders})",
            orphan_issuer_ids,
        )

    # Issuer has one child table (listing); the framework's
    # foreign_keys=OFF window covers the rebuild. UNIQUE INDEX
    # idx_issuer_name_country from migration 060 must be re-created.
    # securities (044) and provider_listing_catalog (042/054) reference
    # issuer.name; drop and recreate around the rebuild.
    securities_view_existed = _view_exists(conn, "securities")
    catalog_view_existed = _view_exists(conn, "provider_listing_catalog")
    supported_view_existed = _view_exists(conn, "supported_tickers")
    primary_view_existed = _view_exists(conn, "primary_provider_listing_catalog")
    if primary_view_existed:
        conn.execute("DROP VIEW primary_provider_listing_catalog")
    if supported_view_existed:
        conn.execute("DROP VIEW supported_tickers")
    if catalog_view_existed:
        conn.execute("DROP VIEW provider_listing_catalog")
    if securities_view_existed:
        conn.execute("DROP VIEW securities")

    conn.execute("DROP INDEX IF EXISTS idx_issuer_name_country")
    conn.execute(
        """
        CREATE TABLE issuer__new (
            issuer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            sector TEXT,
            industry TEXT,
            country TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO issuer__new (
            issuer_id, name, description, sector, industry, country
        )
        SELECT issuer_id, name, description, sector, industry, country
        FROM issuer
        """
    )
    conn.execute("DROP TABLE issuer")
    conn.execute("ALTER TABLE issuer__new RENAME TO issuer")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_issuer_name_country
        ON issuer(name, country)
        """
    )

    if securities_view_existed:
        _create_securities_view(conn)
    if catalog_view_existed:
        _create_provider_listing_catalog_view(conn)
    if supported_view_existed:
        _create_supported_tickers_view(conn)
    if primary_view_existed:
        _create_primary_provider_listing_catalog_view(conn)


def _migration_065_financial_facts_fiscal_period_not_null(
    conn: sqlite3.Connection,
) -> None:
    """Tighten ``financial_facts.fiscal_period`` to NOT NULL.

    Migration 043 deliberately kept ``fiscal_period`` nullable in the
    schema because ``FactRecord.fiscal_period: Optional[str]`` and
    several callers relied on that. With the API change in this commit
    (FactRecord.fiscal_period defaulted to ``'INSTANT'`` when the
    upstream payload doesn't carry one) the schema can finally enforce
    the invariant.

    Live DB carries zero NULL ``fiscal_period`` rows today (audit
    confirmed against the 103M-row table), so the pre-flight is a
    formality. The rebuild itself is heavy on production
    (~70min on live DB) — same shape as migration 043 / 059.
    """

    if not _table_exists(conn, "financial_facts"):
        return

    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type = 'table' AND name = 'financial_facts'"
    ).fetchone()
    if ddl_row is None:
        return
    if "fiscal_period TEXT NOT NULL" in ddl_row[0]:
        return

    null_count = conn.execute(
        "SELECT COUNT(*) FROM financial_facts WHERE fiscal_period IS NULL"
    ).fetchone()[0]
    if null_count:
        raise RuntimeError(
            f"migration 065 aborted: {null_count} financial_facts rows "
            "have NULL fiscal_period. Backfill them (e.g. to 'INSTANT') "
            "before retrying."
        )

    orphan_count = conn.execute(
        """
        SELECT COUNT(*) FROM financial_facts
        WHERE listing_id NOT IN (SELECT listing_id FROM listing)
        """
    ).fetchone()[0]
    if orphan_count:
        raise RuntimeError(
            f"migration 065 aborted: {orphan_count} orphan financial_facts "
            "rows reference missing listings."
        )

    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_concept")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_security_concept")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_security_concept_latest")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_currency_nonnull")

    currency_check = _CURRENCY_FORMAT_CHECK.format(col="currency")
    conn.execute(
        f"""
        CREATE TABLE financial_facts__new (
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
            currency TEXT
                CHECK (currency IS NULL OR ({currency_check})),
            source_provider TEXT,
            PRIMARY KEY (listing_id, concept, fiscal_period, end_date, unit),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO financial_facts__new (
            listing_id, cik, concept, fiscal_period, end_date, unit,
            value, accn, filed, frame, start_date, accounting_standard,
            currency, source_provider
        )
        SELECT
            listing_id, cik, concept, fiscal_period, end_date, unit,
            value, accn, filed, frame, start_date, accounting_standard,
            currency, source_provider
        FROM financial_facts
        """
    )
    conn.execute("DROP TABLE financial_facts")
    conn.execute("ALTER TABLE financial_facts__new RENAME TO financial_facts")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fin_facts_concept
        ON financial_facts(concept)
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


def _migration_066_provider_exchange_name_country_not_null(
    conn: sqlite3.Connection,
) -> None:
    """Tighten ``provider_exchange.name`` and ``country`` to NOT NULL.

    Both columns are 100% populated on the live DB (verified during the
    audit), so this is a pure guard against future bad writes. The
    rebuild also handles the catalog/exchange_provider view
    drop/recreate dance.
    """

    if not _table_exists(conn, "provider_exchange"):
        return

    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type = 'table' AND name = 'provider_exchange'"
    ).fetchone()
    if ddl_row is None:
        return
    if "name TEXT NOT NULL" in ddl_row[0]:
        return

    # Live DB has 0 NULL rows on both columns. Older test fixtures and
    # any pre-canonical-rename DB may have NULL name/country because
    # earlier migrations backfilled provider_exchange from supported
    # tickers without resolving display metadata. Use the provider
    # exchange code as a name fallback and 'Unknown' as the country
    # fallback rather than aborting: both are recoverable by a
    # subsequent supported-tickers refresh.
    conn.execute(
        """
        UPDATE provider_exchange
        SET name = provider_exchange_code
        WHERE name IS NULL OR trim(name) = ''
        """
    )
    conn.execute(
        """
        UPDATE provider_exchange
        SET country = 'Unknown'
        WHERE country IS NULL OR trim(country) = ''
        """
    )

    conn.execute("DROP INDEX IF EXISTS idx_provider_exchange_exchange")

    catalog_view_existed = _view_exists(conn, "provider_listing_catalog")
    supported_view_existed = _view_exists(conn, "supported_tickers")
    exchange_provider_view_existed = _view_exists(conn, "exchange_provider")
    primary_view_existed = _view_exists(conn, "primary_provider_listing_catalog")
    if primary_view_existed:
        conn.execute("DROP VIEW primary_provider_listing_catalog")
    if supported_view_existed:
        conn.execute("DROP VIEW supported_tickers")
    if catalog_view_existed:
        conn.execute("DROP VIEW provider_listing_catalog")
    if exchange_provider_view_existed:
        conn.execute("DROP VIEW exchange_provider")

    currency_check = _CURRENCY_FORMAT_CHECK.format(col="currency")
    conn.execute(
        f"""
        CREATE TABLE provider_exchange__new (
            provider_exchange_id INTEGER PRIMARY KEY,
            provider_id INTEGER NOT NULL,
            provider_exchange_code TEXT NOT NULL,
            exchange_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            country TEXT NOT NULL,
            currency TEXT
                CHECK (currency IS NULL OR ({currency_check})),
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
        INSERT INTO provider_exchange__new (
            provider_exchange_id, provider_id, provider_exchange_code,
            exchange_id, name, country, currency, operating_mic,
            country_iso2, country_iso3, updated_at
        )
        SELECT
            provider_exchange_id, provider_id, provider_exchange_code,
            exchange_id, name, country, currency, operating_mic,
            country_iso2, country_iso3, updated_at
        FROM provider_exchange
        """
    )
    conn.execute("DROP TABLE provider_exchange")
    conn.execute("ALTER TABLE provider_exchange__new RENAME TO provider_exchange")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_provider_exchange_exchange
        ON provider_exchange(exchange_id)
        """
    )

    if exchange_provider_view_existed:
        _create_exchange_provider_view(conn)
    if catalog_view_existed:
        _create_provider_listing_catalog_view(conn)
    if supported_view_existed:
        _create_supported_tickers_view(conn)
    if primary_view_existed:
        _create_primary_provider_listing_catalog_view(conn)


def _migration_067_drop_unused_indexes(conn: sqlite3.Connection) -> None:
    """Drop eight secondary indexes that the audit found unused or redundant.

    Each of these indexes either had no current query that exercised it,
    or was strictly covered by a PRIMARY KEY / UNIQUE auto-index:

    * ``idx_fin_facts_concept`` — concept is always paired with
      ``listing_id`` in the codebase, in which case
      ``idx_fin_facts_security_concept_latest`` already covers the
      query. The standalone-concept index alone occupied ~3.18 GB.
    * ``idx_metric_compute_status_metric_status`` — no query filters on
      ``(metric_id)`` or ``(metric_id, status)``; all reads match the
      PK ``(listing_id, metric_id)``.
    * ``idx_metrics_metric_id`` — same story as above for ``metrics``.
    * ``idx_market_data_latest`` — strictly redundant with the PK
      ``(listing_id, as_of)``. SQLite traverses the PK in either
      direction at no cost, so the descending-ordered duplicate index
      added write amplification without any optimizer benefit.
    * ``idx_fundamentals_raw_last_fetched`` — the only candidate filter
      (``_fetch_stale``) joins through ``provider_listing_catalog`` and
      reaches ``fundamentals_raw`` via PK; the optimizer never picks the
      ``last_fetched_at`` index.
    * ``idx_market_data_fetch_next`` /
      ``idx_fundamentals_fetch_next`` — both fetch-state tables are
      always reached via PK in JOINs. No code path scans them by
      ``next_eligible_at``.
    * ``idx_listing_exchange`` — overlaps with
      ``sqlite_autoindex_listing_1 (exchange_id, symbol)`` whose
      leading column is already ``exchange_id``.

    ``DROP INDEX IF EXISTS`` keeps the migration safely re-runnable
    against databases that never created one or more of these indexes
    (e.g. fresh test DBs that took different paths through earlier
    migrations).
    """

    for index_name in (
        "idx_fin_facts_concept",
        "idx_metric_compute_status_metric_status",
        "idx_metrics_metric_id",
        "idx_market_data_latest",
        "idx_fundamentals_raw_last_fetched",
        "idx_market_data_fetch_next",
        "idx_listing_exchange",
        "idx_fundamentals_fetch_next",
    ):
        conn.execute(f"DROP INDEX IF EXISTS {index_name}")


def _migration_068_fiscal_period_check(conn: sqlite3.Connection) -> None:
    """Constrain ``financial_facts.fiscal_period`` to a known enum.

    Adds ``CHECK (fiscal_period IN ('FY','Q1','Q2','Q3','Q4','TTM','INSTANT'))``
    and backfills the rows that earlier EODHD code persisted with
    ``fiscal_period=''`` — EODHD's snapshot facts ``EnterpriseValue``,
    ``CommonStockDividendsPerShareCashPaid`` and the dormant
    ``CommonStockSharesOutstanding`` SharesStats writer all emitted an
    empty string because the source payload exposes them as scalars
    (not period-keyed time series).

    Backfill mapping:
      * ``CommonStockDividendsPerShareCashPaid`` → ``'TTM'`` (per EODHD's
        glossary: ``Highlights.DividendShare`` is a trailing-12-month figure).
      * Anything else with empty period → ``'INSTANT'`` (covers EV and the
        SharesOutstanding snapshot; both are point-in-time scalars).

    For the backfilled rows, ``end_date`` is re-derived from the cached
    fundamentals payload's ``General.UpdatedAt`` (the real EODHD-side
    refresh timestamp for the snapshot), falling back to
    ``DATE(fundamentals_raw.last_fetched_at)`` when ``UpdatedAt`` is
    absent, and finally to the row's existing ``end_date`` so we never
    insert a NULL ``end_date``. The legacy ``end_date`` was the
    balance-sheet ``MostRecentQuarter`` — wrong by up to ~90 days for
    these market-driven snapshots.

    PK uniqueness is preserved with a ``ROW_NUMBER()`` filter: if a
    listing has several historical empty-period rows for the same
    concept (different legacy ``end_date`` values), they all map onto
    the same new key after the backfill, and only the row with the
    latest legacy ``end_date`` survives.

    The migration is idempotent — re-running against a DB that already
    has the CHECK is a no-op.
    """

    if not _table_exists(conn, "financial_facts"):
        return

    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type = 'table' AND name = 'financial_facts'"
    ).fetchone()
    if ddl_row is None:
        return
    if "fiscal_period IN ('FY'" in ddl_row[0]:
        return

    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_security_concept_latest")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_currency_nonnull")

    currency_check = _CURRENCY_FORMAT_CHECK.format(col="currency")
    conn.execute(
        f"""
        CREATE TABLE financial_facts__new (
            listing_id INTEGER NOT NULL,
            cik TEXT,
            concept TEXT NOT NULL,
            fiscal_period TEXT NOT NULL
                CHECK (fiscal_period IN ('FY','Q1','Q2','Q3','Q4','TTM','INSTANT')),
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
            currency TEXT
                CHECK (currency IS NULL OR ({currency_check})),
            source_provider TEXT,
            PRIMARY KEY (listing_id, concept, fiscal_period, end_date, unit),
            FOREIGN KEY (listing_id) REFERENCES listing(listing_id)
        )
        """
    )

    # Backfill via a single INSERT ... SELECT. The CTE chain:
    #   listing_as_of — resolves each listing's snapshot date from the cached
    #     fundamentals_raw payload (General.UpdatedAt preferred, last_fetched_at
    #     as a fallback).
    #   backfilled    — rewrites fiscal_period and end_date for empty-period
    #     rows, passes everything else through unchanged. legacy_end_date is
    #     kept so the dedup step can prefer the most recent historical snapshot.
    # ROW_NUMBER() picks one row per new (listing_id, concept, fiscal_period,
    # end_date, unit) tuple — only meaningful for collapsed empty-period rows;
    # non-empty rows have unique tuples already.
    conn.execute(
        """
        INSERT INTO financial_facts__new (
            listing_id, cik, concept, fiscal_period, end_date, unit,
            value, accn, filed, frame, start_date, accounting_standard,
            currency, source_provider
        )
        WITH listing_as_of AS (
            SELECT
                pl.listing_id,
                COALESCE(
                    json_extract(fr.data, '$.General.UpdatedAt'),
                    SUBSTR(fr.last_fetched_at, 1, 10)
                ) AS as_of
            FROM provider_listing pl
            JOIN fundamentals_raw fr
              ON fr.provider_listing_id = pl.provider_listing_id
        ),
        backfilled AS (
            SELECT
                ff.listing_id,
                ff.cik,
                ff.concept,
                CASE
                    WHEN ff.fiscal_period <> '' THEN ff.fiscal_period
                    WHEN ff.concept = 'CommonStockDividendsPerShareCashPaid'
                        THEN 'TTM'
                    ELSE 'INSTANT'
                END AS fiscal_period,
                CASE
                    WHEN ff.fiscal_period <> '' THEN ff.end_date
                    ELSE COALESCE(la.as_of, ff.end_date)
                END AS end_date,
                ff.end_date AS legacy_end_date,
                ff.unit,
                ff.value,
                ff.accn,
                ff.filed,
                ff.frame,
                ff.start_date,
                ff.accounting_standard,
                ff.currency,
                ff.source_provider
            FROM financial_facts ff
            LEFT JOIN listing_as_of la ON la.listing_id = ff.listing_id
        )
        SELECT
            listing_id, cik, concept, fiscal_period, end_date, unit,
            value, accn, filed, frame, start_date, accounting_standard,
            currency, source_provider
        FROM (
            SELECT
                b.*,
                ROW_NUMBER() OVER (
                    PARTITION BY b.listing_id, b.concept, b.fiscal_period,
                                 b.end_date, b.unit
                    ORDER BY b.legacy_end_date DESC, b.value DESC
                ) AS rn
            FROM backfilled b
        )
        WHERE rn = 1
        """
    )

    conn.execute("DROP TABLE financial_facts")
    conn.execute("ALTER TABLE financial_facts__new RENAME TO financial_facts")
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


def _migration_069_purge_currencyless_listings(conn: sqlite3.Connection) -> None:
    """Delete listings with no quote currency, then make ``currency`` NOT NULL.

    Listings created by the price-ingest path (``SecurityRepository.ensure``)
    for symbols that were never catalogued carry a NULL ``currency``: migration
    039 backfilled ``listing.currency`` from ``provider_listing``, so a listing
    with no ``provider_listing`` row stayed NULL. These listings are
    currency-blind (every metric skips them) and, lacking a ``fundamentals_raw``
    payload, cannot be rebuilt by ``normalise`` -- so they are deleted outright
    rather than backfilled.

    The purge cascades to every dependent row: the ``provider_listing`` children
    keyed by ``provider_listing_id`` (``fundamentals_raw``,
    ``fundamentals_normalization_state``, ``fundamentals_fetch_state``,
    ``market_data_fetch_state``) first, then the direct children keyed by
    ``listing_id`` (``financial_facts``, ``financial_facts_refresh_state``,
    ``market_data``, ``metric_compute_status``, ``metrics``,
    ``provider_listing``), then the ``listing`` rows themselves.

    With no NULL currencies left, ``listing`` is rebuilt with ``currency TEXT
    NOT NULL`` so the gap cannot recur (``SecurityRepository.ensure`` now
    resolves the currency from the exchange instead of inserting NULL). The
    table is referenced by several child tables and the catalog views, so FK
    enforcement is deferred to commit time and the views are dropped/recreated
    around the rebuild (matching migration 056).

    Idempotent: a listing table that already declares ``currency TEXT NOT NULL``
    is left untouched.
    """

    if not _table_exists(conn, "listing"):
        return
    columns = _table_columns(conn, "listing")
    if not {"issuer_id", "exchange_id", "symbol", "currency"} <= columns:
        return
    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='listing'"
    ).fetchone()
    if ddl_row is None:
        return
    if "currency TEXT NOT NULL" in ddl_row[0]:
        return

    conn.execute("PRAGMA defer_foreign_keys = ON")

    # --- 1. Purge currency-less listings and every dependent row. ---
    # Grandchildren keyed by provider_listing_id (delete before provider_listing).
    for table in (
        "fundamentals_raw",
        "fundamentals_normalization_state",
        "fundamentals_fetch_state",
        "market_data_fetch_state",
    ):
        if _table_exists(conn, table):
            conn.execute(
                f"""
                DELETE FROM {table}
                WHERE provider_listing_id IN (
                    SELECT provider_listing_id FROM provider_listing
                    WHERE listing_id IN (
                        SELECT listing_id FROM listing WHERE currency IS NULL
                    )
                )
                """
            )
    # Direct children keyed by listing_id.
    for table in (
        "financial_facts",
        "financial_facts_refresh_state",
        "market_data",
        "metric_compute_status",
        "metrics",
        "provider_listing",
    ):
        if _table_exists(conn, table):
            conn.execute(
                f"""
                DELETE FROM {table}
                WHERE listing_id IN (
                    SELECT listing_id FROM listing WHERE currency IS NULL
                )
                """
            )
    conn.execute("DELETE FROM listing WHERE currency IS NULL")

    # --- 2. Rebuild listing with currency NOT NULL. ---
    # Only idx_listing_currency_nonnull is recreated below. idx_listing_exchange
    # was dropped as unused by migration 067 and is deliberately NOT resurrected.
    conn.execute("DROP INDEX IF EXISTS idx_listing_currency_nonnull")

    catalog_view_existed = _view_exists(conn, "provider_listing_catalog")
    supported_view_existed = _view_exists(conn, "supported_tickers")
    securities_view_existed = _view_exists(conn, "securities")
    primary_view_existed = _view_exists(conn, "primary_provider_listing_catalog")
    if primary_view_existed:
        conn.execute("DROP VIEW primary_provider_listing_catalog")
    if supported_view_existed:
        conn.execute("DROP VIEW supported_tickers")
    if catalog_view_existed:
        conn.execute("DROP VIEW provider_listing_catalog")
    if securities_view_existed:
        conn.execute("DROP VIEW securities")

    has_primary_listing_status = "primary_listing_status" in columns
    primary_status_column = (
        ",\n            primary_listing_status TEXT NOT NULL DEFAULT 'unknown'"
        if has_primary_listing_status
        else ""
    )
    primary_status_select = (
        ", primary_listing_status" if has_primary_listing_status else ""
    )
    currency_check = _CURRENCY_FORMAT_CHECK.format(col="currency")
    conn.execute(
        f"""
        CREATE TABLE listing__new (
            listing_id INTEGER PRIMARY KEY,
            issuer_id INTEGER NOT NULL,
            exchange_id INTEGER NOT NULL,
            symbol TEXT NOT NULL
                CHECK (length(symbol) > 0
                       AND symbol = upper(trim(symbol))
                       AND instr(symbol, ' ') = 0
                       AND symbol GLOB '[A-Z0-9.&^*-]*'),
            currency TEXT NOT NULL
                CHECK ({currency_check}){primary_status_column},
            UNIQUE (exchange_id, symbol),
            FOREIGN KEY (issuer_id) REFERENCES issuer(issuer_id),
            FOREIGN KEY (exchange_id) REFERENCES "exchange"(exchange_id)
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO listing__new (
            listing_id, issuer_id, exchange_id, symbol, currency{primary_status_select}
        )
        SELECT
            listing_id, issuer_id, exchange_id, symbol, currency{primary_status_select}
        FROM listing
        """
    )
    conn.execute("DROP TABLE listing")
    conn.execute("ALTER TABLE listing__new RENAME TO listing")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_listing_currency_nonnull
        ON listing(currency)
        WHERE currency IS NOT NULL
        """
    )

    if securities_view_existed:
        _create_securities_view(conn)
    if catalog_view_existed:
        _create_provider_listing_catalog_view(conn)
    if supported_view_existed:
        _create_supported_tickers_view(conn)
    if primary_view_existed:
        _create_primary_provider_listing_catalog_view(conn)

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_violations:
        raise RuntimeError(
            f"migration 069 left foreign key violations: {fk_violations!r}"
        )


def _migration_070_market_data_price_major_currency(
    conn: sqlite3.Connection,
) -> None:
    """Store ``market_data.price`` in the major currency for subunit listings.

    Historically, prices for listings quoted in a subunit (GBX/GBP0.01 pence,
    ZAC cents, ILA agorot) were stored in that subunit -- migration 039 even
    multiplied the formerly GBP/ZAR/ILS rows by 100 to match the pence-quoted
    feed. The data boundary now keeps every stored price in the *major*
    currency (the Money type and the snapshot read path both assume this), so
    divide those rows by 100 exactly once.

    Runs after migration 069 has purged currency-less listings, so every
    remaining listing has a non-null quote currency. ``listing.currency`` is the
    source of truth for whether a row is a subunit quote and itself stays as the
    raw quote code (e.g. 'GBX'); only the stored price changes. After this runs,
    the snapshot read path reports ``canonical_trading_currency(listing.currency)``
    (the base code), so the stored price and the reported currency are
    consistent.

    This is a data-only migration with no schema marker; the schema-version
    gate guarantees it runs exactly once (re-running would wrongly divide
    again). It must be deployed together with the Phase-2 code change.
    """

    if not _table_exists(conn, "market_data") or not _table_exists(conn, "listing"):
        return

    # Subunit codes whose major value is the stored amount / 100. Mirrors
    # SUBUNIT_CURRENCY_REGISTRY in currency.py; inlined so the migration stays a
    # frozen snapshot independent of later registry edits.
    conn.execute(
        """
        UPDATE market_data
        SET price = price / 100.0
        WHERE listing_id IN (
            SELECT listing_id FROM listing
            WHERE UPPER(TRIM(currency)) IN ('GBX', 'GBP0.01', 'ZAC', 'ILA')
        )
        """
    )


def _migration_071_financial_facts_unit_kind(conn: sqlite3.Connection) -> None:
    """Rename ``financial_facts.unit`` to ``unit_kind`` and drop it from the PK.

    The legacy ``unit`` column conflated two ideas: it held a *currency code*
    (USD, EUR, GBX...) for monetary rows and a *type token* (``shares``,
    ``EPS``, ``USD/shares``) for the rest -- even though a separate
    ``currency`` column already existed. ``unit_kind`` now holds ONLY the
    documented enum (monetary / per_share / ratio / percent / multiple /
    count / other -- ``MetricUnitKind`` in currency.py); the ISO code lives in
    ``currency`` alone; and the primary key drops the old ``unit`` member (new
    PK ``(listing_id, concept, fiscal_period, end_date)``).

    Two CHECKs encode the refactor's invariants at the schema level:

    * ``currency`` is *major-only* (no subunit GBX/ZAC/ILA) -- subunits are
      collapsed to their base currency before a fact is ever built.
    * ``unit_kind`` and ``currency`` are *coupled*: monetary / per_share rows
      MUST carry a currency, and every other kind MUST NOT.

    The table is rebuilt EMPTY rather than copied row-by-row. The author
    rebuilds ``financial_facts`` from ``fundamentals_raw`` via the CLI
    ``normalise`` command after this lands (a decision recorded in the refactor
    plan), so copying ~100M legacy rows -- whose ``unit`` would need per-row
    reclassification into the enum anyway -- would be wasted work. Dropping the
    table also frees its storage immediately, reused by the rebuild.
    ``fundamentals_normalization_state`` is cleared so ``normalise`` re-processes
    every cached payload instead of treating them as already-normalized.

    ``financial_facts`` is a leaf table (only ``listing`` references it, via its
    own outgoing FK); nothing references it and no view selects from it, so the
    drop/recreate disturbs nothing else.

    Idempotent: a table that already declares ``unit_kind`` is left untouched.
    """

    if not _table_exists(conn, "financial_facts"):
        return
    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type = 'table' AND name = 'financial_facts'"
    ).fetchone()
    if ddl_row is None:
        return
    if "unit_kind" in ddl_row[0]:
        return

    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_security_concept_latest")
    conn.execute("DROP INDEX IF EXISTS idx_fin_facts_currency_nonnull")
    conn.execute("DROP TABLE financial_facts")

    major_currency_check = _MAJOR_CURRENCY_CHECK.format(col="currency")
    conn.execute(
        f"""
        CREATE TABLE financial_facts (
            listing_id INTEGER NOT NULL,
            cik TEXT,
            concept TEXT NOT NULL,
            fiscal_period TEXT NOT NULL
                CHECK (fiscal_period IN ('FY','Q1','Q2','Q3','Q4','TTM','INSTANT')),
            end_date TEXT NOT NULL,
            unit_kind TEXT NOT NULL
                CHECK (unit_kind IN (
                    'monetary','per_share','ratio','percent','multiple','count','other'
                )),
            value REAL NOT NULL,
            accn TEXT,
            filed TEXT,
            frame TEXT,
            start_date TEXT,
            accounting_standard TEXT,
            currency TEXT
                CHECK (
                    (currency IS NULL OR ({major_currency_check}))
                    AND (
                        (unit_kind IN ('monetary','per_share') AND currency IS NOT NULL)
                        OR (unit_kind NOT IN ('monetary','per_share') AND currency IS NULL)
                    )
                ),
            source_provider TEXT,
            PRIMARY KEY (listing_id, concept, fiscal_period, end_date),
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

    # Force ``normalise`` to re-build every fact from the cached raw payloads:
    # the normalization gate compares the payload hash stored here, so an empty
    # table means "nothing has been normalized yet".
    if _table_exists(conn, "fundamentals_normalization_state"):
        conn.execute("DELETE FROM fundamentals_normalization_state")


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
    _migration_035_drop_provider_status,
    _migration_036_drop_fundamentals_raw_listing_columns,
    _migration_037_drop_fundamentals_raw_currency,
    _migration_038_move_primary_listing_status_to_listing,
    _migration_039_canonical_listing_quote_currency,
    _migration_040_pure_fundamentals_state,
    _migration_041_add_metrics_constraints,
    _migration_042_persist_provider_listing_views,
    _migration_043_financial_facts_dedupe_and_fk,
    _migration_044_persist_compat_views,
    _migration_045_fx_rates_rate_to_real,
    _migration_046_add_fk_financial_facts_refresh_state,
    _migration_047_add_fk_market_data,
    _migration_048_add_fk_fx_rates_provider,
    _migration_049_add_fk_fx_supported_pairs_provider,
    _migration_050_add_fk_fx_refresh_state_provider,
    _migration_051_add_bool_checks,
    _migration_052_drop_redundant_fin_facts_index,
    _migration_053_drop_market_data_fetch_state_runtime_columns,
    _migration_054_drop_provider_listing_provider_id,
    _migration_055_status_enum_checks,
    _migration_056_listing_format_checks,
    _migration_057_provider_exchange_currency_check,
    _migration_058_fx_rates_format_checks,
    _migration_059_financial_facts_format_checks,
    _migration_060_issuer_unique_name_country,
    _migration_061_market_data_fetch_state_error_invariant,
    _migration_062_primary_provider_listing_catalog_view,
    _migration_063_schema_migrations_pk_and_guard,
    _migration_064_drop_orphan_issuers_tighten_name,
    _migration_065_financial_facts_fiscal_period_not_null,
    _migration_066_provider_exchange_name_country_not_null,
    _migration_067_drop_unused_indexes,
    _migration_068_fiscal_period_check,
    _migration_069_purge_currencyless_listings,
    _migration_070_market_data_price_major_currency,
    _migration_071_financial_facts_unit_kind,
]


__all__ = ["apply_migrations", "MIGRATIONS"]
