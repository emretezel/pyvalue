"""Local persistence helpers for universe data.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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
                    region TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, region)
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
                    listing.isin,
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
                    isin,
                    region,
                    ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

        apply_migrations(self.db_path)
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

    def fetch_fact_record(self, symbol: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        """Return the CIK and payload tuple for ``symbol`` if present."""

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT cik, data FROM company_facts
                WHERE symbol = ?
                """,
                (symbol.upper(),),
            ).fetchone()
        if row is None:
            return None
        return row[0], json.loads(row[1])


class UKCompanyFactsRepository(SQLiteStore):
    """Store Companies House payloads for UK listings."""

    def initialize_schema(self) -> None:
        """Create the uk_company_facts table."""

        apply_migrations(self.db_path)
        with self._connect() as conn:
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

    def upsert_company_facts(self, company_number: str, payload: Dict[str, Any], symbol: Optional[str] = None) -> None:
        """Persist the Companies House payload for a company."""

        serialized = json.dumps(payload)
        fetched_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO uk_company_facts (company_number, symbol, data, fetched_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(company_number) DO UPDATE SET
                    symbol = COALESCE(excluded.symbol, uk_company_facts.symbol),
                    data = excluded.data,
                    fetched_at = excluded.fetched_at
                """,
                (company_number, symbol, serialized, fetched_at),
            )

    def fetch_fact(self, company_number: str) -> Optional[Dict[str, Any]]:
        """Return the Companies House payload for ``company_number`` if present."""

        with self._connect() as conn:
            row = conn.execute(
                "SELECT data FROM uk_company_facts WHERE company_number = ?",
                (company_number,),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])


class UKSymbolMapRepository(SQLiteStore):
    """Persist mappings between UK symbols and corporate identifiers."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
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
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_uk_symbol_map_isin
                ON uk_symbol_map(isin)
                """
            )

    def upsert_mapping(
        self,
        symbol: str,
        isin: Optional[str] = None,
        lei: Optional[str] = None,
        company_number: Optional[str] = None,
        match_confidence: Optional[str] = None,
    ) -> None:
        updated_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO uk_symbol_map (symbol, isin, lei, company_number, match_confidence, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    isin = COALESCE(excluded.isin, uk_symbol_map.isin),
                    lei = COALESCE(excluded.lei, uk_symbol_map.lei),
                    company_number = COALESCE(excluded.company_number, uk_symbol_map.company_number),
                    match_confidence = COALESCE(excluded.match_confidence, uk_symbol_map.match_confidence),
                    updated_at = excluded.updated_at
                """,
                (symbol.upper(), isin, lei, company_number, match_confidence, updated_at),
            )

    def fetch_company_number(self, symbol: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT company_number FROM uk_symbol_map WHERE symbol = ?",
                (symbol.upper(),),
            ).fetchone()
        return row[0] if row else None

    def fetch_symbols_with_company_number(self) -> List[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT symbol FROM uk_symbol_map WHERE company_number IS NOT NULL ORDER BY symbol"
            ).fetchall()
        return [row[0] for row in rows]

    def bulk_upsert(self, rows: Iterable[Tuple[str, str, str, str]]) -> int:
        updated_at = datetime.now(timezone.utc).isoformat()
        payload = []
        for symbol, isin, lei, company_number in rows:
            payload.append((symbol.upper(), isin, lei, company_number, updated_at))
        if not payload:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO uk_symbol_map (symbol, isin, lei, company_number, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    isin = COALESCE(excluded.isin, uk_symbol_map.isin),
                    lei = COALESCE(excluded.lei, uk_symbol_map.lei),
                    company_number = COALESCE(excluded.company_number, uk_symbol_map.company_number),
                    updated_at = excluded.updated_at
                """,
                payload,
            )
        return len(payload)


class UKFilingRepository(SQLiteStore):
    """Store Companies House filing documents (iXBRL)."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
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

    def upsert_document(
        self,
        company_number: str,
        filing_id: str,
        content: bytes,
        symbol: Optional[str] = None,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        doc_type: Optional[str] = None,
        is_ixbrl: bool = True,
    ) -> None:
        fetched_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO uk_filing_documents (
                    company_number,
                    symbol,
                    filing_id,
                    period_start,
                    period_end,
                    doc_type,
                    is_ixbrl,
                    fetched_at,
                    content
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_number, filing_id) DO UPDATE SET
                    symbol = COALESCE(excluded.symbol, uk_filing_documents.symbol),
                    period_start = COALESCE(excluded.period_start, uk_filing_documents.period_start),
                    period_end = COALESCE(excluded.period_end, uk_filing_documents.period_end),
                    doc_type = COALESCE(excluded.doc_type, uk_filing_documents.doc_type),
                    is_ixbrl = excluded.is_ixbrl,
                    fetched_at = excluded.fetched_at,
                    content = excluded.content
                """,
                (
                    company_number,
                    symbol,
                    filing_id,
                    period_start,
                    period_end,
                    doc_type,
                    int(is_ixbrl),
                    fetched_at,
                    content,
                ),
            )

    def latest_for_company(self, company_number: str) -> Optional[bytes]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT content FROM uk_filing_documents
                WHERE company_number = ?
                ORDER BY fetched_at DESC
                LIMIT 1
                """,
                (company_number,),
            ).fetchone()
        return row[0] if row else None


@dataclass(frozen=True)
class FactRecord:
    """Normalized financial fact ready for storage."""

    symbol: str
    cik: str
    concept: str
    fiscal_period: str
    end_date: str
    unit: str
    value: float
    accn: Optional[str]
    filed: Optional[str]
    frame: Optional[str]
    start_date: Optional[str] = None


class FinancialFactsRepository(SQLiteStore):
    """Persist normalized financial facts for downstream metrics."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS financial_facts (
                    symbol TEXT NOT NULL,
                    cik TEXT NOT NULL,
                    concept TEXT NOT NULL,
                    fiscal_period TEXT,
                    end_date TEXT NOT NULL,
                    unit TEXT NOT NULL,
                    value REAL NOT NULL,
                    accn TEXT,
                    filed TEXT,
                    frame TEXT,
                    start_date TEXT,
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

    def replace_facts(self, symbol: str, records: Iterable[FactRecord]) -> int:
        """Replace all facts for ``symbol`` with the provided batch."""

        rows = [
            (
                record.symbol,
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
            )
            for record in records
        ]
        if not rows:
            return 0

        with self._connect() as conn:
            conn.execute("DELETE FROM financial_facts WHERE symbol = ?", (symbol,))
            conn.executemany(
                """
                INSERT OR REPLACE INTO financial_facts (
                    symbol, cik, concept, fiscal_period,
                    end_date, unit, value, accn, filed, frame, start_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def latest_fact(self, symbol: str, concept: str) -> Optional[FactRecord]:
        """Return the most recent FactRecord for a symbol/concept."""

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, cik, concept, fiscal_period, end_date, unit,
                       value, accn, filed, frame, start_date
                FROM financial_facts
                WHERE symbol = ? AND concept = ?
                ORDER BY end_date DESC
                LIMIT 1
                """,
                (symbol.upper(), concept),
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
            "SELECT symbol, cik, concept, fiscal_period, end_date, unit, value, accn, filed, frame, start_date",
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
    "CompanyFactsRepository",
    "UKCompanyFactsRepository",
    "UKSymbolMapRepository",
    "UKFilingRepository",
    "FinancialFactsRepository",
    "MarketDataRepository",
    "FactRecord",
    "EntityMetadataRepository",
]
