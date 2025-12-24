"""Local persistence helpers for universe data.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from pyvalue.marketdata.base import PriceData
from pyvalue.migrations import apply_migrations
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

        apply_migrations(self.db_path)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS listings (
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
                CREATE INDEX IF NOT EXISTS idx_listings_exchange
                ON listings(exchange)
                """
            )

    def replace_universe(self, listings: Sequence[Listing]) -> int:
        """Replace all listings for the exchanges present in ``listings``."""

        if not listings:
            return 0

        ingested_at = datetime.now(timezone.utc).isoformat()
        payload: List[tuple] = []
        exchanges = sorted(
            {
                listing.exchange.strip().upper()
                for listing in listings
                if listing.exchange
            }
        )
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
                    listing.isin,
                    listing.currency,
                    ingested_at,
                )
            )

        with self._connect() as conn:
            if exchanges:
                placeholders = ", ".join("?" for _ in exchanges)
                conn.execute(
                    f"DELETE FROM listings WHERE UPPER(exchange) IN ({placeholders})",
                    exchanges,
                )
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
                    isin,
                    currency,
                    ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
        return len(payload)

    def fetch_symbols_by_exchange(self, exchange: str) -> List[str]:
        """Return the list of symbols currently stored for an exchange."""

        exchange_norm = exchange.upper()
        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT symbol FROM listings WHERE UPPER(exchange) = ? ORDER BY symbol",
                (exchange_norm,),
            ).fetchall()
        return [row[0] for row in rows]

    def fetch_symbols_by_exchange_pairs(self, exchange: str) -> List[Tuple[str, str]]:
        """Return (symbol, exchange) pairs for an exchange code."""

        exchange_norm = exchange.upper()
        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT symbol, exchange FROM listings WHERE UPPER(exchange) = ? ORDER BY symbol",
                (exchange_norm,),
            ).fetchall()
        return [(row[0], row[1]) for row in rows]

    def fetch_currency(self, symbol: str) -> Optional[str]:
        query = ["SELECT currency FROM listings WHERE symbol = ?"]
        params: List[Any] = [symbol.upper()]
        query.append("LIMIT 1")
        sql = " ".join(query)
        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return row[0] if row else None


class FundamentalsRepository(SQLiteStore):
    """Persist raw fundamentals payloads by provider."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fundamentals_raw (
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

    def upsert(
        self,
        provider: str,
        symbol: str,
        payload: Dict[str, Any],
        currency: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> None:
        serialized = json.dumps(payload)
        fetched_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fundamentals_raw (provider, symbol, currency, exchange, data, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, symbol) DO UPDATE SET
                    currency = COALESCE(excluded.currency, fundamentals_raw.currency),
                    exchange = COALESCE(excluded.exchange, fundamentals_raw.exchange),
                    data = excluded.data,
                    fetched_at = excluded.fetched_at
                """,
                (provider.upper(), symbol.upper(), currency, exchange, serialized, fetched_at),
            )

    def fetch(self, provider: str, symbol: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT data FROM fundamentals_raw
                WHERE provider = ? AND symbol = ?
                """,
                (provider.upper(), symbol.upper()),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def fetch_record(self, provider: str, symbol: str) -> Optional[Tuple[str, Optional[str], Dict[str, Any]]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, exchange, data FROM fundamentals_raw
                WHERE provider = ? AND symbol = ?
                """,
                (provider.upper(), symbol.upper()),
            ).fetchone()
        if row is None:
            return None
        return row[0], row[1], json.loads(row[2])

    def symbols(self, provider: str) -> List[str]:
        query = ["SELECT symbol FROM fundamentals_raw WHERE provider = ?"]
        params: List[Any] = [provider.upper()]
        query.append("ORDER BY symbol")
        sql = " ".join(query)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [row[0] for row in rows]

    def symbol_exchanges(self, provider: str) -> List[Tuple[str, Optional[str]]]:
        query = ["SELECT symbol, exchange FROM fundamentals_raw WHERE provider = ?"]
        params: List[Any] = [provider.upper()]
        query.append("ORDER BY symbol")
        sql = " ".join(query)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [(row[0], row[1]) for row in rows]


class FundamentalsFetchStateRepository(SQLiteStore):
    """Track fundamentals fetch status for resumable ingestion."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
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

    def fetch(self, provider: str, symbol: str) -> Optional[Dict[str, Optional[str]]]:
        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT last_fetched_at, last_status, last_error, next_eligible_at, attempts
                FROM fundamentals_fetch_state
                WHERE provider = ? AND symbol = ?
                """,
                (provider.upper(), symbol.upper()),
            ).fetchone()
        if row is None:
            return None
        return {
            "last_fetched_at": row[0],
            "last_status": row[1],
            "last_error": row[2],
            "next_eligible_at": row[3],
            "attempts": row[4],
        }

    def mark_success(
        self,
        provider: str,
        symbol: str,
        fetched_at: Optional[str] = None,
    ) -> None:
        timestamp = fetched_at or datetime.now(timezone.utc).isoformat()
        self.initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fundamentals_fetch_state (
                    provider,
                    symbol,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, 'ok', NULL, NULL, 0)
                ON CONFLICT(provider, symbol) DO UPDATE SET
                    last_fetched_at = excluded.last_fetched_at,
                    last_status = 'ok',
                    last_error = NULL,
                    next_eligible_at = NULL,
                    attempts = 0
                """,
                (provider.upper(), symbol.upper(), timestamp),
            )

    def mark_failure(
        self,
        provider: str,
        symbol: str,
        error: str,
        base_backoff_seconds: int = 3600,
        max_backoff_seconds: int = 86400,
    ) -> None:
        self.initialize_schema()
        state = self.fetch(provider, symbol)
        attempts = (state.get("attempts") if state else 0) or 0
        attempts += 1
        backoff = min(base_backoff_seconds * (2 ** (attempts - 1)), max_backoff_seconds)
        now = datetime.now(timezone.utc)
        next_eligible_at = (now + timedelta(seconds=backoff)).isoformat()
        last_fetched_at = state.get("last_fetched_at") if state else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fundamentals_fetch_state (
                    provider,
                    symbol,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, 'error', ?, ?, ?)
                ON CONFLICT(provider, symbol) DO UPDATE SET
                    last_fetched_at = COALESCE(excluded.last_fetched_at, fundamentals_fetch_state.last_fetched_at),
                    last_status = 'error',
                    last_error = excluded.last_error,
                    next_eligible_at = excluded.next_eligible_at,
                    attempts = excluded.attempts
                """,
                (
                    provider.upper(),
                    symbol.upper(),
                    last_fetched_at,
                    error,
                    next_eligible_at,
                    attempts,
                ),
            )


@dataclass(frozen=True)
class FactRecord:
    """Normalized financial fact ready for storage."""

    symbol: str
    cik: Optional[str] = None
    concept: str = ""
    fiscal_period: str = ""
    end_date: str = ""
    unit: str = ""
    value: float = 0.0
    accn: Optional[str] = None
    filed: Optional[str] = None
    frame: Optional[str] = None
    start_date: Optional[str] = None
    accounting_standard: Optional[str] = None
    currency: Optional[str] = None


class FinancialFactsRepository(SQLiteStore):
    """Persist normalized financial facts for downstream metrics."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS financial_facts (
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
                CREATE INDEX IF NOT EXISTS idx_fin_facts_symbol_concept
                ON financial_facts(symbol, concept)
                """
            )

    def replace_facts(
        self,
        symbol: str,
        records: Iterable[FactRecord],
    ) -> int:
        """Replace all facts for ``symbol`` with the provided batch.

        Existing facts are removed even if the batch is empty.
        """

        target_symbol = symbol.upper()
        rows = [
            (
                target_symbol,
                record.cik,
                record.concept,
                record.fiscal_period,
                record.end_date,
                record.unit,
                record.value,
                record.accn,
                record.filed,
                record.frame,
                getattr(record, "start_date", None),
                getattr(record, "accounting_standard", None),
                getattr(record, "currency", None),
            )
            for record in records
        ]
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM financial_facts WHERE symbol = ?",
                (target_symbol,),
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO financial_facts (
                    symbol, cik, concept, fiscal_period,
                    end_date, unit, value, accn, filed, frame, start_date,
                    accounting_standard, currency
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def latest_fact(
        self,
        symbol: str,
        concept: str,
    ) -> Optional[FactRecord]:
        """Return the most recent FactRecord for a symbol/concept."""

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, cik, concept, fiscal_period, end_date, unit,
                       value, accn, filed, frame, start_date, accounting_standard, currency
                FROM financial_facts
                WHERE symbol = ? AND concept = ?
                ORDER BY end_date DESC, filed DESC
                LIMIT 1
                """,
                [symbol.upper(), concept],
            ).fetchone()
        if row is None:
            return None
        return FactRecord(*row)

    def facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[FactRecord]:
        """Return ordered fact records for the symbol/concept."""

        query = [
            "SELECT symbol, cik, concept, fiscal_period, end_date, unit, value, accn, filed, frame, start_date, accounting_standard, currency",
            "FROM financial_facts",
            "WHERE symbol = ? AND concept = ?",
        ]
        params: List[Any] = [symbol.upper(), concept]
        if fiscal_period:
            query.append("AND fiscal_period = ?")
            params.append(fiscal_period)
        query.append("ORDER BY end_date DESC, filed DESC")
        if limit:
            query.append("LIMIT ?")
            params.append(limit)
        sql = " ".join(query)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [FactRecord(*row) for row in rows]


class MetricsRepository(SQLiteStore):
    """Persist computed metric values."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics (
                    symbol TEXT NOT NULL,
                    metric_id TEXT NOT NULL,
                    value REAL NOT NULL,
                    as_of TEXT NOT NULL,
                    PRIMARY KEY (symbol, metric_id)
                )
                """
            )

    def upsert(self, symbol: str, metric_id: str, value: float, as_of: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO metrics (symbol, metric_id, value, as_of)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol, metric_id) DO UPDATE SET
                    value = excluded.value,
                    as_of = excluded.as_of
                """,
                (symbol.upper(), metric_id, value, as_of),
            )

    def fetch(self, symbol: str, metric_id: str) -> Optional[Tuple[float, str]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT value, as_of FROM metrics
                WHERE symbol = ? AND metric_id = ?
                """,
                (symbol.upper(), metric_id),
            ).fetchone()
        if row is None:
            return None
        return row[0], row[1]


class MarketDataRepository(SQLiteStore):
    """Persist market data snapshots (prices, volume, market cap)."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_data (
                    symbol TEXT NOT NULL,
                    as_of DATE NOT NULL,
                    price REAL NOT NULL,
                    volume INTEGER,
                    market_cap REAL,
                    currency TEXT,
                    PRIMARY KEY (symbol, as_of)
                )
                """
            )

    def upsert_price(
        self,
        symbol: str,
        as_of: str,
        price: float,
        volume: Optional[int] = None,
        market_cap: Optional[float] = None,
        currency: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO market_data (symbol, as_of, price, volume, market_cap, currency)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, as_of) DO UPDATE SET
                    price = excluded.price,
                    volume = excluded.volume,
                    market_cap = excluded.market_cap,
                    currency = excluded.currency
                """,
                (symbol.upper(), as_of, price, volume, market_cap, currency),
            )

    def latest_snapshot(self, symbol: str) -> Optional[PriceData]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, as_of, price, volume, market_cap, currency
                FROM market_data
                WHERE symbol = ?
                ORDER BY as_of DESC
                LIMIT 1
                """,
                (symbol.upper(),),
            ).fetchone()
        if row is None:
            return None
        return PriceData(
            symbol=row["symbol"],
            price=row["price"],
            as_of=row["as_of"],
            volume=row["volume"],
            market_cap=row["market_cap"],
            currency=row["currency"],
        )

    def latest_price(self, symbol: str) -> Optional[Tuple[str, float]]:
        snapshot = self.latest_snapshot(symbol)
        if snapshot is None:
            return None
        return snapshot.as_of, snapshot.price


class EntityMetadataRepository(SQLiteStore):
    """Store SEC entity names for quick lookup."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS entity_metadata (
                    symbol TEXT PRIMARY KEY,
                    entity_name TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def upsert(self, symbol: str, entity_name: str) -> None:
        if not entity_name:
            return
        updated_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO entity_metadata (symbol, entity_name, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    entity_name = excluded.entity_name,
                    updated_at = excluded.updated_at
                """,
                (symbol.upper(), entity_name.strip(), updated_at),
            )

    def fetch(self, symbol: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT entity_name FROM entity_metadata WHERE symbol = ?",
                (symbol.upper(),),
            ).fetchone()
        return row[0] if row else None


__all__ = [
    "UniverseRepository",
    "FundamentalsRepository",
    "FundamentalsFetchStateRepository",
    "FinancialFactsRepository",
    "MarketDataRepository",
    "FactRecord",
    "EntityMetadataRepository",
]
