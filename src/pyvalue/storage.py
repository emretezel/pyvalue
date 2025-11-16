# Author: Emre Tezel
"""Local persistence helpers for universe data."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import sqlite3
from typing import Any, Dict, List, Optional, Sequence, Union

from pyvalue.universe import Listing


class SQLiteStore:
    """Shared helpers for repositories backed by SQLite."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn


class UniverseRepository(SQLiteStore):
    """Persist and retrieve listing data using a SQLite backend."""

    def initialize_schema(self) -> None:
        """Create the listings table if it does not exist yet."""

        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS listings (
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
                CREATE INDEX IF NOT EXISTS idx_listings_region
                ON listings(region)
                """
            )

    def replace_universe(self, listings: Sequence[Listing], region: str) -> int:
        """Replace all listings for ``region`` with the provided list."""

        if not listings:
            return 0

        ingested_at = datetime.now(timezone.utc).isoformat()
        payload: List[tuple] = []
        for listing in listings:
            # Normalize booleans to integers because SQLite lacks boolean type.
            payload.append(
                (
                    listing.symbol,
                    listing.security_name,
                    listing.exchange,
                    listing.market_category,
                    int(listing.is_etf),
                    listing.status,
                    listing.round_lot_size,
                    listing.source,
                    region,
                    ingested_at,
                )
            )

        with self._connect() as conn:
            conn.execute("DELETE FROM listings WHERE region = ?", (region,))
            conn.executemany(
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
                    region,
                    ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
        return len(payload)

    def fetch_symbols(self, region: str) -> List[str]:
        """Return the list of symbols currently stored for a region."""

        with self._connect() as conn:
            rows = conn.execute(
                "SELECT symbol FROM listings WHERE region = ? ORDER BY symbol", (region,)
            ).fetchall()
        return [row[0] for row in rows]


class CompanyFactsRepository(SQLiteStore):
    """Store SEC company facts payloads for later metric calculations."""

    def initialize_schema(self) -> None:
        """Create the company_facts table."""

        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS company_facts (
                    cik TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    data TEXT NOT NULL,
                    fetched_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_company_facts_symbol
                ON company_facts(symbol)
                """
            )

    def upsert_company_facts(self, symbol: str, cik: str, payload: Dict[str, Any]) -> None:
        """Persist the SEC payload for a company."""

        serialized = json.dumps(payload)
        fetched_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO company_facts (cik, symbol, data, fetched_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(cik) DO UPDATE SET
                    symbol = excluded.symbol,
                    data = excluded.data,
                    fetched_at = excluded.fetched_at
                """,
                (cik, symbol, serialized, fetched_at),
            )

    def fetch_fact(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Return the stored payload for ``symbol`` if present."""

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT data FROM company_facts
                WHERE symbol = ?
                """,
                (symbol,),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])


__all__ = ["UniverseRepository", "CompanyFactsRepository"]
