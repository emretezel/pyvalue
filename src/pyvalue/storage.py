"""Local persistence helpers for universe data.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sqlite3
import time
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from pyvalue.marketdata.base import MarketDataUpdate, PriceData
from pyvalue.migrations import apply_migrations
from pyvalue.universe import Listing


SQLITE_BUSY_TIMEOUT_MS = 30000
SQLITE_LOCK_RETRY_ATTEMPTS = 5
SQLITE_LOCK_RETRY_SLEEP_SECONDS = 0.5


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_required_text(value: Any, field_name: str) -> str:
    text = _normalize_optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required")
    return text


def _normalize_symbol_base(symbol: str) -> Tuple[str, Optional[str]]:
    cleaned = symbol.strip().upper()
    if "." not in cleaned:
        return cleaned, None
    ticker, exchange = cleaned.rsplit(".", 1)
    return ticker, exchange


def _coerce_int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _batched(values: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


@dataclass(frozen=True)
class Security:
    """Canonical security identity."""

    security_id: int
    canonical_ticker: str
    canonical_exchange_code: str
    canonical_symbol: str
    entity_name: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class SupportedExchange:
    """Persisted provider-supported exchange metadata."""

    provider: str
    provider_exchange_code: str
    canonical_exchange_code: str
    name: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    operating_mic: Optional[str] = None
    country_iso2: Optional[str] = None
    country_iso3: Optional[str] = None
    updated_at: Optional[str] = None

    @property
    def code(self) -> str:
        return self.provider_exchange_code


@dataclass(frozen=True)
class SupportedTicker:
    """Persisted provider-supported ticker metadata."""

    provider: str
    provider_exchange_code: str
    provider_symbol: str
    provider_ticker: str
    security_id: int
    listing_exchange: Optional[str] = None
    security_name: Optional[str] = None
    security_type: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    isin: Optional[str] = None
    updated_at: Optional[str] = None

    @property
    def exchange_code(self) -> str:
        return self.provider_exchange_code

    @property
    def symbol(self) -> str:
        return self.provider_symbol

    @property
    def code(self) -> str:
        return self.provider_ticker


@dataclass(frozen=True)
class IngestProgressSummary:
    """Aggregate ingest progress for a supported-ticker scope."""

    total_supported: int
    stored: int
    missing: int
    stale: int
    blocked: int
    error_rows: int


@dataclass(frozen=True)
class IngestProgressExchange:
    """Per-exchange ingest progress for a supported-ticker scope."""

    exchange_code: str
    total_supported: int
    stored: int
    missing: int
    stale: int
    blocked: int
    error_rows: int


@dataclass(frozen=True)
class IngestProgressFailure:
    """Recent ingest failure details for reporting."""

    symbol: str
    exchange_code: str
    last_status: Optional[str] = None
    last_error: Optional[str] = None
    next_eligible_at: Optional[str] = None
    attempts: int = 0


@dataclass(frozen=True)
class FactRecord:
    """Normalized financial fact ready for storage."""

    symbol: str
    cik: Optional[str] = None
    concept: str = ""
    fiscal_period: Optional[str] = None
    end_date: str = ""
    unit: str = ""
    value: float = 0.0
    accn: Optional[str] = None
    filed: Optional[str] = None
    frame: Optional[str] = None
    start_date: Optional[str] = None
    accounting_standard: Optional[str] = None
    currency: Optional[str] = None


@dataclass(frozen=True)
class MarketSnapshotRecord:
    """Stored latest market-data row keyed to a canonical security."""

    security_id: int
    symbol: str
    as_of: str
    price: float
    volume: Optional[int] = None
    market_cap: Optional[float] = None
    currency: Optional[str] = None


StoredFactRow = Tuple[
    Optional[str],
    str,
    Optional[str],
    str,
    str,
    float,
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]

StoredMetricRow = Tuple[str, str, float, str]


@dataclass(frozen=True)
class FundamentalsUpdate:
    """Raw fundamentals payload prepared for batch persistence."""

    security_id: int
    provider_symbol: str
    provider_exchange_code: Optional[str]
    currency: Optional[str]
    data: str
    fetched_at: str


@dataclass(frozen=True)
class FundamentalsNormalizationCandidate:
    """Normalization freshness inputs for one stored raw fundamentals payload."""

    provider_symbol: str
    security_id: int
    raw_fetched_at: str
    normalized_raw_fetched_at: Optional[str] = None
    last_normalized_at: Optional[str] = None
    current_source_provider: Optional[str] = None


class _ManagedSQLiteConnection(sqlite3.Connection):
    """SQLite connection that closes the file handle when the context exits."""

    def __exit__(self, exc_type, exc_value, traceback) -> Literal[False]:
        try:
            super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()
        return False


class SQLiteStore:
    """Shared helpers for repositories backed by SQLite."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._security_repo_cache: Optional[SecurityRepository] = None
        self._supported_exchange_repo_cache: Optional[SupportedExchangeRepository] = (
            None
        )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=SQLITE_BUSY_TIMEOUT_MS / 1000.0,
            factory=_ManagedSQLiteConnection,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        return conn

    def current_journal_mode(self) -> str:
        with self._connect() as conn:
            row = conn.execute("PRAGMA journal_mode").fetchone()
        return str(row[0]).lower() if row is not None else ""

    def enable_wal_mode(self) -> str:
        def _enable() -> str:
            with self._connect() as conn:
                row = conn.execute("PRAGMA journal_mode=WAL").fetchone()
            return str(row[0]).lower() if row is not None else ""

        return self._run_with_locked_retry(_enable)

    @staticmethod
    def _is_locked_error(exc: sqlite3.OperationalError) -> bool:
        return "database is locked" in str(exc).lower()

    def _run_with_locked_retry(self, operation: Any) -> Any:
        last_exc: Optional[sqlite3.OperationalError] = None
        for attempt in range(SQLITE_LOCK_RETRY_ATTEMPTS):
            try:
                return operation()
            except sqlite3.OperationalError as exc:
                if not self._is_locked_error(exc):
                    raise
                last_exc = exc
                if attempt >= SQLITE_LOCK_RETRY_ATTEMPTS - 1:
                    break
                time.sleep(SQLITE_LOCK_RETRY_SLEEP_SECONDS * (attempt + 1))
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(
            "SQLite retry loop exited without a result"
        )  # pragma: no cover

    def _security_repo(self) -> SecurityRepository:
        if self._security_repo_cache is None:
            self._security_repo_cache = SecurityRepository(self.db_path)
        return self._security_repo_cache

    def _supported_exchange_repo(self) -> SupportedExchangeRepository:
        if self._supported_exchange_repo_cache is None:
            self._supported_exchange_repo_cache = SupportedExchangeRepository(
                self.db_path
            )
        return self._supported_exchange_repo_cache


class SecurityRepository(SQLiteStore):
    """Persist canonical security identities."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._by_symbol: Dict[str, Security] = {}
        self._by_id: Dict[int, Security] = {}

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS securities (
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
                CREATE INDEX IF NOT EXISTS idx_securities_exchange
                ON securities(canonical_exchange_code)
                """
            )

    def ensure(
        self,
        canonical_ticker: str,
        canonical_exchange_code: str,
        entity_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Security:
        self.initialize_schema()
        ticker = _normalize_required_text(canonical_ticker, "canonical_ticker").upper()
        exchange_code = _normalize_required_text(
            canonical_exchange_code, "canonical_exchange_code"
        ).upper()
        canonical_symbol = f"{ticker}.{exchange_code}"
        entity_name = _normalize_optional_text(entity_name)
        description = _normalize_optional_text(description)
        now = _utc_now_iso()
        with self._connect() as conn:
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
                ON CONFLICT(canonical_exchange_code, canonical_ticker) DO UPDATE SET
                    entity_name = COALESCE(excluded.entity_name, securities.entity_name),
                    description = COALESCE(excluded.description, securities.description),
                    updated_at = excluded.updated_at
                """,
                (
                    ticker,
                    exchange_code,
                    canonical_symbol,
                    entity_name,
                    description,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                """
                SELECT security_id, canonical_ticker, canonical_exchange_code,
                       canonical_symbol, entity_name, description, created_at, updated_at
                FROM securities
                WHERE canonical_exchange_code = ? AND canonical_ticker = ?
                """,
                (exchange_code, ticker),
            ).fetchone()
        if row is None:  # pragma: no cover - defensive
            raise RuntimeError(f"Failed to create or load security {canonical_symbol}")
        security = Security(*row)
        self._remember(security)
        return security

    def ensure_from_symbol(
        self,
        symbol: str,
        exchange_code: Optional[str] = None,
        entity_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Security:
        ticker, suffix = _normalize_symbol_base(symbol)
        canonical_exchange = (exchange_code or suffix or "").strip().upper()
        if not canonical_exchange:
            raise ValueError(
                f"Could not infer canonical exchange code for security symbol {symbol}"
            )
        return self.ensure(
            ticker,
            canonical_exchange,
            entity_name=entity_name,
            description=description,
        )

    def fetch_by_symbol(self, symbol: str) -> Optional[Security]:
        self.initialize_schema()
        normalized = symbol.strip().upper()
        cached = self._by_symbol.get(normalized)
        if cached is not None:
            return cached
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT security_id, canonical_ticker, canonical_exchange_code,
                       canonical_symbol, entity_name, description, created_at, updated_at
                FROM securities
                WHERE canonical_symbol = ?
                """,
                (normalized,),
            ).fetchone()
        if row is None:
            return None
        security = Security(*row)
        self._remember(security)
        return security

    def fetch(self, security_id: int) -> Optional[Security]:
        self.initialize_schema()
        cached = self._by_id.get(security_id)
        if cached is not None:
            return cached
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT security_id, canonical_ticker, canonical_exchange_code,
                       canonical_symbol, entity_name, description, created_at, updated_at
                FROM securities
                WHERE security_id = ?
                """,
                (security_id,),
            ).fetchone()
        if row is None:
            return None
        security = Security(*row)
        self._remember(security)
        return security

    def resolve_id(self, symbol: str) -> Optional[int]:
        security = self.fetch_by_symbol(symbol)
        return security.security_id if security else None

    def resolve_ids_many(
        self,
        symbols: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[str, int]:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}

        resolved: Dict[str, int] = {}
        uncached: List[str] = []
        for symbol in normalized:
            cached = self._by_symbol.get(symbol)
            if cached is not None:
                resolved[symbol] = cached.security_id
            else:
                uncached.append(symbol)
        if not uncached:
            return resolved

        with self._connect() as conn:
            for chunk in _batched(uncached, chunk_size):
                placeholders = ", ".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT security_id, canonical_symbol
                    FROM securities
                    WHERE canonical_symbol IN ({placeholders})
                    """,
                    list(chunk),
                ).fetchall()
                for row in rows:
                    resolved[row["canonical_symbol"]] = row["security_id"]
        return resolved

    def canonical_symbol(self, security_id: int) -> Optional[str]:
        security = self.fetch(security_id)
        return security.canonical_symbol if security else None

    def upsert_metadata(
        self,
        symbol: str,
        entity_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        if not entity_name and not description:
            return
        security = self.ensure_from_symbol(
            symbol, entity_name=entity_name, description=description
        )
        self._remember(security)

    def fetch_name(self, symbol: str) -> Optional[str]:
        security = self.fetch_by_symbol(symbol)
        return security.entity_name if security else None

    def fetch_description(self, symbol: str) -> Optional[str]:
        security = self.fetch_by_symbol(symbol)
        return security.description if security else None

    def list_supported_symbols(
        self,
        exchange_codes: Optional[Sequence[str]] = None,
    ) -> List[str]:
        self.initialize_schema()
        params: List[object] = []
        query = [
            "SELECT DISTINCT s.canonical_symbol",
            "FROM supported_tickers st",
            "JOIN securities s ON s.security_id = st.security_id",
        ]
        normalized = _normalized_codes(exchange_codes)
        if normalized:
            placeholders = ", ".join("?" for _ in normalized)
            query.append(f"WHERE UPPER(s.canonical_exchange_code) IN ({placeholders})")
            params.extend(normalized)
        query.append("ORDER BY s.canonical_symbol")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [row[0] for row in rows]

    def list_supported_symbol_name_pairs(
        self,
        exchange_codes: Optional[Sequence[str]] = None,
    ) -> List[Tuple[str, Optional[str]]]:
        self.initialize_schema()
        params: List[object] = []
        query = [
            "SELECT s.canonical_symbol,",
            "COALESCE(s.entity_name, MAX(st.security_name), s.canonical_symbol) AS entity_name",
            "FROM supported_tickers st",
            "JOIN securities s ON s.security_id = st.security_id",
        ]
        normalized = _normalized_codes(exchange_codes)
        if normalized:
            placeholders = ", ".join("?" for _ in normalized)
            query.append(f"WHERE UPPER(s.canonical_exchange_code) IN ({placeholders})")
            params.extend(normalized)
        query.append("GROUP BY s.security_id, s.canonical_symbol, s.entity_name")
        query.append("ORDER BY s.canonical_symbol")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [(row["canonical_symbol"], row["entity_name"]) for row in rows]

    def _remember(self, security: Security) -> None:
        self._by_id[security.security_id] = security
        self._by_symbol[security.canonical_symbol] = security


class SupportedExchangeRepository(SQLiteStore):
    """Store exchange catalogs published by data providers."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS supported_exchanges (
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
                CREATE INDEX IF NOT EXISTS idx_supported_exchanges_canonical
                ON supported_exchanges(canonical_exchange_code)
                """
            )

    def replace_for_provider(
        self,
        provider: str,
        rows: Sequence[Dict[str, Any]],
    ) -> int:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        updated_at = _utc_now_iso()
        payload: List[Tuple[object, ...]] = []
        for row in rows:
            code = _normalize_optional_text(
                row.get("Code") or row.get("provider_exchange_code")
            )
            if not code:
                continue
            code_norm = code.upper()
            canonical_exchange_code = _normalize_optional_text(
                row.get("CanonicalExchangeCode") or row.get("canonical_exchange_code")
            )
            payload.append(
                (
                    provider_norm,
                    code_norm,
                    (canonical_exchange_code or code_norm).upper(),
                    _normalize_optional_text(row.get("Name") or row.get("name")),
                    _normalize_optional_text(row.get("Country") or row.get("country")),
                    _normalize_optional_text(
                        row.get("Currency") or row.get("currency")
                    ),
                    _normalize_optional_text(
                        row.get("OperatingMIC") or row.get("operating_mic")
                    ),
                    _normalize_optional_text(
                        row.get("CountryISO2") or row.get("country_iso2")
                    ),
                    _normalize_optional_text(
                        row.get("CountryISO3") or row.get("country_iso3")
                    ),
                    updated_at,
                )
            )

        with self._connect() as conn:
            conn.execute(
                "DELETE FROM supported_exchanges WHERE UPPER(provider) = ?",
                (provider_norm,),
            )
            if payload:
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
                    payload,
                )
        return len(payload)

    def ensure_fixed_exchange(
        self,
        provider: str,
        provider_exchange_code: str,
        canonical_exchange_code: str,
        name: Optional[str] = None,
        country: Optional[str] = None,
        currency: Optional[str] = None,
    ) -> None:
        self.initialize_schema()
        with self._connect() as conn:
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
                ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
                ON CONFLICT(provider, provider_exchange_code) DO UPDATE SET
                    canonical_exchange_code = excluded.canonical_exchange_code,
                    name = COALESCE(excluded.name, supported_exchanges.name),
                    country = COALESCE(excluded.country, supported_exchanges.country),
                    currency = COALESCE(excluded.currency, supported_exchanges.currency),
                    updated_at = excluded.updated_at
                """,
                (
                    provider.strip().upper(),
                    provider_exchange_code.strip().upper(),
                    canonical_exchange_code.strip().upper(),
                    _normalize_optional_text(name),
                    _normalize_optional_text(country),
                    _normalize_optional_text(currency),
                    _utc_now_iso(),
                ),
            )

    def fetch(self, provider: str, code: str) -> Optional[SupportedExchange]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        code_norm = code.strip().upper()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT provider, provider_exchange_code, canonical_exchange_code, name,
                       country, currency, operating_mic, country_iso2, country_iso3,
                       updated_at
                FROM supported_exchanges
                WHERE UPPER(provider) = ? AND UPPER(provider_exchange_code) = ?
                """,
                (provider_norm, code_norm),
            ).fetchone()
        return SupportedExchange(*row) if row else None

    def list_all(self, provider: Optional[str] = None) -> List[SupportedExchange]:
        self.initialize_schema()
        params: List[object] = []
        query = [
            "SELECT provider, provider_exchange_code, canonical_exchange_code, name,",
            "country, currency, operating_mic, country_iso2, country_iso3, updated_at",
            "FROM supported_exchanges",
        ]
        if provider:
            query.append("WHERE UPPER(provider) = ?")
            params.append(provider.strip().upper())
        query.append("ORDER BY provider, provider_exchange_code")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [SupportedExchange(*row) for row in rows]

    def resolve_canonical_code(self, provider: str, provider_exchange_code: str) -> str:
        record = self.fetch(provider, provider_exchange_code)
        if record is not None:
            return record.canonical_exchange_code
        return provider_exchange_code.strip().upper()


class SupportedTickerRepository(SQLiteStore):
    """Store provider-supported ticker catalogs by exchange."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._supported_exchange_repo().initialize_schema()
        self._security_repo().initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS supported_tickers (
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
                CREATE UNIQUE INDEX IF NOT EXISTS idx_supported_tickers_provider_exchange_ticker
                ON supported_tickers(provider, provider_exchange_code, provider_ticker)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_supported_tickers_provider_exchange
                ON supported_tickers(provider, provider_exchange_code)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_supported_tickers_security
                ON supported_tickers(security_id)
                """
            )
        FundamentalsRepository(self.db_path).initialize_schema()
        FundamentalsFetchStateRepository(self.db_path).initialize_schema()
        MarketDataRepository(self.db_path).initialize_schema()
        MarketDataFetchStateRepository(self.db_path).initialize_schema()

    def replace_from_listings(
        self,
        provider: str,
        exchange_code: str,
        listings: Sequence[Listing],
    ) -> int:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        provider_exchange_code = exchange_code.strip().upper()
        payload = [
            self._payload_from_listing(provider_norm, provider_exchange_code, listing)
            for listing in listings
        ]
        rows = [row for row in payload if row is not None]
        self._replace_payload(provider_norm, provider_exchange_code, rows)
        return len(rows)

    def replace_for_exchange(
        self,
        provider: str,
        exchange_code: str,
        rows: Sequence[Dict[str, Any]],
    ) -> int:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        provider_exchange_code = exchange_code.strip().upper()
        payload: List[Tuple[object, ...]] = []
        for row in rows:
            prepared = self._payload_from_row(
                provider_norm, provider_exchange_code, row
            )
            if prepared is not None:
                payload.append(prepared)
        self._replace_payload(provider_norm, provider_exchange_code, payload)
        return len(payload)

    def fetch_for_symbol(self, provider: str, symbol: str) -> Optional[SupportedTicker]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        symbol_norm = symbol.strip().upper()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT provider, provider_exchange_code, provider_symbol, provider_ticker,
                       security_id, listing_exchange, security_name, security_type,
                       country, currency, isin, updated_at
                FROM supported_tickers
                WHERE UPPER(provider) = ? AND UPPER(provider_symbol) = ?
                """,
                (provider_norm, symbol_norm),
            ).fetchone()
        if row is None:
            return None
        return SupportedTicker(*row)

    def list_for_provider(
        self,
        provider: str,
        exchange_codes: Optional[Sequence[str]] = None,
        provider_symbols: Optional[Sequence[str]] = None,
    ) -> List[SupportedTicker]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        params: List[object] = [provider_norm]
        query = [
            "SELECT provider, provider_exchange_code, provider_symbol, provider_ticker,",
            "security_id, listing_exchange, security_name, security_type, country, currency, isin, updated_at",
            "FROM supported_tickers",
            "WHERE UPPER(provider) = ?",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND UPPER(provider_exchange_code) IN ({placeholders})")
            params.extend(normalized_codes)
        normalized_symbols = _normalized_codes(provider_symbols)
        if normalized_symbols:
            placeholders = ", ".join("?" for _ in normalized_symbols)
            query.append(f"AND UPPER(provider_symbol) IN ({placeholders})")
            params.extend(normalized_symbols)
        query.append("ORDER BY provider_exchange_code, provider_symbol")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [SupportedTicker(*row) for row in rows]

    def list_symbols_by_exchange(self, provider: str, exchange_code: str) -> List[str]:
        rows = self.list_for_provider(provider, exchange_codes=[exchange_code])
        return [row.provider_symbol for row in rows]

    def list_symbol_name_pairs_by_exchange(
        self, provider: str, exchange_code: str
    ) -> List[Tuple[str, Optional[str]]]:
        rows = self.list_for_provider(provider, exchange_codes=[exchange_code])
        return [(row.provider_symbol, row.security_name) for row in rows]

    def list_canonical_symbols(
        self,
        exchange_codes: Optional[Sequence[str]] = None,
    ) -> List[str]:
        return self._security_repo().list_supported_symbols(exchange_codes)

    def list_canonical_symbol_name_pairs(
        self,
        exchange_codes: Optional[Sequence[str]] = None,
    ) -> List[Tuple[str, Optional[str]]]:
        return self._security_repo().list_supported_symbol_name_pairs(exchange_codes)

    def available_exchanges(self, provider: Optional[str] = None) -> List[str]:
        self.initialize_schema()
        params: List[object] = []
        query = ["SELECT DISTINCT provider_exchange_code FROM supported_tickers"]
        if provider:
            query.append("WHERE UPPER(provider) = ?")
            params.append(provider.strip().upper())
        query.append("ORDER BY provider_exchange_code")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [row[0] for row in rows]

    def clear(
        self,
        provider: Optional[str] = None,
        exchange_code: Optional[str] = None,
    ) -> int:
        self.initialize_schema()
        params: List[object] = []
        query = ["DELETE FROM supported_tickers WHERE 1 = 1"]
        if provider:
            query.append("AND UPPER(provider) = ?")
            params.append(provider.strip().upper())
        if exchange_code:
            query.append("AND UPPER(provider_exchange_code) = ?")
            params.append(exchange_code.strip().upper())
        with self._connect() as conn:
            cursor = conn.execute(" ".join(query), params)
        return int(cursor.rowcount or 0)

    def delete_symbols(self, provider: str, symbols: Sequence[str]) -> int:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return 0
        placeholders = ", ".join("?" for _ in normalized)
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                DELETE FROM supported_tickers
                WHERE UPPER(provider) = ? AND UPPER(provider_symbol) IN ({placeholders})
                """,
                [provider.strip().upper(), *normalized],
            )
        return int(cursor.rowcount or 0)

    def list_for_exchange(
        self, provider: str, exchange_code: str
    ) -> List[SupportedTicker]:
        return self.list_for_provider(provider, exchange_codes=[exchange_code])

    def fetch_currency(
        self,
        symbol: str,
        provider: Optional[str] = None,
    ) -> Optional[str]:
        self.initialize_schema()
        symbol_norm = symbol.strip().upper()
        params: List[object] = [symbol_norm, symbol_norm]
        query = [
            "SELECT st.currency",
            "FROM supported_tickers st",
            "JOIN securities s ON s.security_id = st.security_id",
            "WHERE st.currency IS NOT NULL",
            "AND (UPPER(st.provider_symbol) = ? OR UPPER(s.canonical_symbol) = ?)",
        ]
        if provider:
            query.append("AND UPPER(st.provider) = ?")
            params.append(provider.strip().upper())
        query.append(
            "ORDER BY CASE WHEN st.provider = 'EODHD' THEN 0 WHEN st.provider = 'SEC' THEN 1 ELSE 2 END, st.updated_at DESC"
        )
        query.append("LIMIT 1")
        with self._connect() as conn:
            row = conn.execute(" ".join(query), params).fetchone()
        return row[0] if row else None

    def list_all_exchanges(self, provider: str) -> List[str]:
        return self.available_exchanges(provider)

    def list_eligible_for_fundamentals(
        self,
        provider: str,
        exchange_codes: Optional[Sequence[str]] = None,
        max_age_days: Optional[int] = None,
        max_symbols: Optional[int] = None,
        resume: bool = False,
        missing_only: bool = False,
        provider_symbols: Optional[Sequence[str]] = None,
    ) -> List[SupportedTicker]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        now = datetime.now(timezone.utc)
        normalized_codes = _normalized_codes(exchange_codes)
        normalized_symbols = _normalized_codes(provider_symbols)

        def _apply_scope_filters(query: List[str], params: List[object]) -> None:
            if normalized_codes:
                placeholders = ", ".join("?" for _ in normalized_codes)
                query.append(f"AND st.provider_exchange_code IN ({placeholders})")
                params.extend(normalized_codes)
            if normalized_symbols:
                placeholders = ", ".join("?" for _ in normalized_symbols)
                query.append(f"AND st.provider_symbol IN ({placeholders})")
                params.extend(normalized_symbols)

        def _fetch_missing(limit: Optional[int]) -> List[SupportedTicker]:
            params: List[object] = [provider_norm, provider_norm]
            query = [
                "SELECT st.provider, st.provider_exchange_code, st.provider_symbol, st.provider_ticker,",
                "st.security_id, st.listing_exchange, st.security_name, st.security_type,",
                "st.country, st.currency, st.isin, st.updated_at",
                "FROM supported_tickers st",
                "LEFT JOIN fundamentals_fetch_state fs ON fs.provider = ? AND fs.provider_symbol = st.provider_symbol",
                "WHERE st.provider = ?",
                "AND fs.last_fetched_at IS NULL",
            ]
            _apply_scope_filters(query, params)
            if resume:
                query.append(
                    "AND (fs.next_eligible_at IS NULL OR fs.next_eligible_at <= ?)"
                )
                params.append(now.isoformat())
            query.append("ORDER BY st.provider_symbol ASC")
            if limit is not None:
                query.append("LIMIT ?")
                params.append(limit)
            with self._connect() as conn:
                rows = conn.execute(" ".join(query), params).fetchall()
            return [SupportedTicker(*row) for row in rows]

        def _fetch_stale(limit: Optional[int], cutoff: str) -> List[SupportedTicker]:
            params: List[object] = [provider_norm, provider_norm, cutoff]
            query = [
                "SELECT st.provider, st.provider_exchange_code, st.provider_symbol, st.provider_ticker,",
                "st.security_id, st.listing_exchange, st.security_name, st.security_type,",
                "st.country, st.currency, st.isin, st.updated_at",
                "FROM fundamentals_fetch_state fs",
                "JOIN supported_tickers st ON st.provider = fs.provider AND st.provider_symbol = fs.provider_symbol",
                "WHERE fs.provider = ?",
                "AND st.provider = ?",
                "AND fs.last_fetched_at IS NOT NULL",
                "AND fs.last_fetched_at <= ?",
            ]
            _apply_scope_filters(query, params)
            if resume:
                query.append(
                    "AND (fs.next_eligible_at IS NULL OR fs.next_eligible_at <= ?)"
                )
                params.append(now.isoformat())
            query.append("ORDER BY fs.last_fetched_at ASC, fs.provider_symbol ASC")
            if limit is not None:
                query.append("LIMIT ?")
                params.append(limit)
            with self._connect() as conn:
                rows = conn.execute(" ".join(query), params).fetchall()
            return [SupportedTicker(*row) for row in rows]

        if max_age_days is None and not missing_only:
            params: List[object] = [provider_norm, provider_norm]
            query = [
                "SELECT st.provider, st.provider_exchange_code, st.provider_symbol, st.provider_ticker,",
                "st.security_id, st.listing_exchange, st.security_name, st.security_type,",
                "st.country, st.currency, st.isin, st.updated_at",
                "FROM supported_tickers st",
                "LEFT JOIN fundamentals_fetch_state fs ON fs.provider = ? AND fs.provider_symbol = st.provider_symbol",
                "WHERE st.provider = ?",
            ]
            _apply_scope_filters(query, params)
            if resume:
                query.append(
                    "AND (fs.next_eligible_at IS NULL OR fs.next_eligible_at <= ?)"
                )
                params.append(now.isoformat())
            query.append("ORDER BY st.provider_symbol ASC")
            if max_symbols is not None:
                query.append("LIMIT ?")
                params.append(max_symbols)
            with self._connect() as conn:
                rows = conn.execute(" ".join(query), params).fetchall()
            return [SupportedTicker(*row) for row in rows]

        missing_rows = _fetch_missing(max_symbols)
        if missing_only:
            return missing_rows

        assert max_age_days is not None
        cutoff = (now - timedelta(days=max_age_days)).isoformat()
        remaining = (
            None if max_symbols is None else max(max_symbols - len(missing_rows), 0)
        )
        if remaining == 0:
            return missing_rows
        stale_rows = _fetch_stale(remaining, cutoff)
        return [*missing_rows, *stale_rows]

    def progress_summary(
        self,
        provider: str,
        exchange_codes: Optional[Sequence[str]] = None,
        max_age_days: Optional[int] = None,
        missing_only: bool = False,
    ) -> IngestProgressSummary:
        rows = self.progress_by_exchange(
            provider=provider,
            exchange_codes=exchange_codes,
            max_age_days=max_age_days,
            missing_only=missing_only,
        )
        return IngestProgressSummary(
            total_supported=sum(row.total_supported for row in rows),
            stored=sum(row.stored for row in rows),
            missing=sum(row.missing for row in rows),
            stale=sum(row.stale for row in rows),
            blocked=sum(row.blocked for row in rows),
            error_rows=sum(row.error_rows for row in rows),
        )

    def progress_by_exchange(
        self,
        provider: str,
        exchange_codes: Optional[Sequence[str]] = None,
        max_age_days: Optional[int] = None,
        missing_only: bool = False,
    ) -> List[IngestProgressExchange]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        now = datetime.now(timezone.utc).isoformat()
        stale_expr = "0"
        params: List[object] = []
        if not missing_only and max_age_days is not None:
            cutoff = (
                datetime.now(timezone.utc) - timedelta(days=max_age_days)
            ).isoformat()
            stale_expr = (
                "SUM(CASE WHEN fs.last_fetched_at IS NOT NULL AND fs.last_fetched_at <= ? "
                "THEN 1 ELSE 0 END)"
            )
            params.append(cutoff)
        params.extend([now, provider_norm])
        normalized_codes = _normalized_codes(exchange_codes)
        query = [
            "SELECT",
            "st.provider_exchange_code AS exchange_code,",
            "COUNT(*) AS total_supported,",
            "SUM(CASE WHEN fs.last_fetched_at IS NOT NULL THEN 1 ELSE 0 END) AS stored,",
            "SUM(CASE WHEN fs.last_fetched_at IS NULL THEN 1 ELSE 0 END) AS missing,",
            f"{stale_expr} AS stale,",
            "SUM(CASE WHEN fs.next_eligible_at IS NOT NULL AND fs.next_eligible_at > ? THEN 1 ELSE 0 END) AS blocked,",
            "SUM(CASE WHEN fs.last_status = 'error' THEN 1 ELSE 0 END) AS error_rows",
            "FROM supported_tickers st",
            "LEFT JOIN fundamentals_fetch_state fs ON fs.provider = st.provider AND fs.provider_symbol = st.provider_symbol",
            "WHERE st.provider = ?",
        ]
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND st.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        query.append("GROUP BY st.provider_exchange_code")
        query.append("ORDER BY st.provider_exchange_code")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [
            IngestProgressExchange(
                exchange_code=row["exchange_code"],
                total_supported=_coerce_int(row["total_supported"]),
                stored=_coerce_int(row["stored"]),
                missing=_coerce_int(row["missing"]),
                stale=_coerce_int(row["stale"]),
                blocked=_coerce_int(row["blocked"]),
                error_rows=_coerce_int(row["error_rows"]),
            )
            for row in rows
        ]

    def recent_failures(
        self,
        provider: str,
        exchange_codes: Optional[Sequence[str]] = None,
        limit: int = 10,
    ) -> List[IngestProgressFailure]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        params: List[object] = [provider_norm, provider_norm]
        query = [
            "SELECT st.provider_symbol AS symbol, st.provider_exchange_code AS exchange_code,",
            "fs.last_status, fs.last_error, fs.next_eligible_at, fs.attempts",
            "FROM supported_tickers st",
            "JOIN fundamentals_fetch_state fs ON fs.provider = ? AND fs.provider_symbol = st.provider_symbol",
            "WHERE st.provider = ? AND fs.last_status = 'error'",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND st.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        query.append(
            "ORDER BY CASE WHEN fs.next_eligible_at IS NULL THEN 1 ELSE 0 END, fs.next_eligible_at ASC, st.provider_symbol ASC"
        )
        query.append("LIMIT ?")
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [
            IngestProgressFailure(
                symbol=row["symbol"],
                exchange_code=row["exchange_code"],
                last_status=row["last_status"],
                last_error=row["last_error"],
                next_eligible_at=row["next_eligible_at"],
                attempts=_coerce_int(row["attempts"]),
            )
            for row in rows
        ]

    def list_eligible_for_market_data(
        self,
        provider: str,
        exchange_codes: Optional[Sequence[str]] = None,
        max_age_days: int = 7,
        max_symbols: Optional[int] = None,
        resume: bool = False,
        provider_symbols: Optional[Sequence[str]] = None,
    ) -> List[SupportedTicker]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        now = datetime.now(timezone.utc)
        cutoff = (now.date() - timedelta(days=max_age_days)).isoformat()
        params: List[object] = [provider_norm, provider_norm]
        query = [
            "SELECT st.provider, st.provider_exchange_code, st.provider_symbol, st.provider_ticker,",
            "st.security_id, st.listing_exchange, st.security_name, st.security_type,",
            "st.country, st.currency, st.isin, st.updated_at",
            "FROM supported_tickers st",
            "LEFT JOIN (",
            "    SELECT security_id, MAX(as_of) AS latest_as_of",
            "    FROM market_data",
            "    GROUP BY security_id",
            ") md ON md.security_id = st.security_id",
            "LEFT JOIN market_data_fetch_state ms ON ms.provider = ? AND ms.provider_symbol = st.provider_symbol",
            "WHERE st.provider = ?",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND st.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        normalized_symbols = _normalized_codes(provider_symbols)
        if normalized_symbols:
            placeholders = ", ".join("?" for _ in normalized_symbols)
            query.append(f"AND st.provider_symbol IN ({placeholders})")
            params.extend(normalized_symbols)
        query.append("AND (md.latest_as_of IS NULL OR md.latest_as_of <= ?)")
        params.append(cutoff)
        if resume:
            query.append(
                "AND (ms.next_eligible_at IS NULL OR ms.next_eligible_at <= ?)"
            )
            params.append(now.isoformat())
        query.append(
            "ORDER BY CASE WHEN md.latest_as_of IS NULL THEN 0 ELSE 1 END, md.latest_as_of ASC, st.provider_exchange_code ASC, st.provider_symbol ASC"
        )
        if max_symbols is not None:
            query.append("LIMIT ?")
            params.append(max_symbols)
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [SupportedTicker(*row) for row in rows]

    def market_data_progress_summary(
        self,
        provider: str,
        exchange_codes: Optional[Sequence[str]] = None,
        max_age_days: int = 7,
    ) -> IngestProgressSummary:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        now = datetime.now(timezone.utc).isoformat()
        cutoff = (
            datetime.now(timezone.utc).date() - timedelta(days=max_age_days)
        ).isoformat()
        params: List[object] = [cutoff, now, provider_norm, provider_norm]
        query = [
            "SELECT",
            "COUNT(*) AS total_supported,",
            "SUM(CASE WHEN md.latest_as_of IS NOT NULL THEN 1 ELSE 0 END) AS stored,",
            "SUM(CASE WHEN md.latest_as_of IS NULL THEN 1 ELSE 0 END) AS missing,",
            "SUM(CASE WHEN md.latest_as_of IS NOT NULL AND md.latest_as_of <= ? THEN 1 ELSE 0 END) AS stale,",
            "SUM(CASE WHEN ms.next_eligible_at IS NOT NULL AND ms.next_eligible_at > ? THEN 1 ELSE 0 END) AS blocked,",
            "SUM(CASE WHEN ms.last_status = 'error' THEN 1 ELSE 0 END) AS error_rows",
            "FROM supported_tickers st",
            "LEFT JOIN (",
            "    SELECT security_id, MAX(as_of) AS latest_as_of",
            "    FROM market_data",
            "    GROUP BY security_id",
            ") md ON md.security_id = st.security_id",
            "LEFT JOIN market_data_fetch_state ms ON ms.provider = ? AND ms.provider_symbol = st.provider_symbol",
            "WHERE st.provider = ?",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND st.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        with self._connect() as conn:
            row = conn.execute(" ".join(query), params).fetchone()
        return IngestProgressSummary(
            total_supported=_coerce_int(row["total_supported"] if row else 0),
            stored=_coerce_int(row["stored"] if row else 0),
            missing=_coerce_int(row["missing"] if row else 0),
            stale=_coerce_int(row["stale"] if row else 0),
            blocked=_coerce_int(row["blocked"] if row else 0),
            error_rows=_coerce_int(row["error_rows"] if row else 0),
        )

    def market_data_progress_by_exchange(
        self,
        provider: str,
        exchange_codes: Optional[Sequence[str]] = None,
        max_age_days: int = 7,
    ) -> List[IngestProgressExchange]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        now = datetime.now(timezone.utc).isoformat()
        cutoff = (
            datetime.now(timezone.utc).date() - timedelta(days=max_age_days)
        ).isoformat()
        params: List[object] = [cutoff, now, provider_norm, provider_norm]
        query = [
            "SELECT",
            "st.provider_exchange_code AS exchange_code,",
            "COUNT(*) AS total_supported,",
            "SUM(CASE WHEN md.latest_as_of IS NOT NULL THEN 1 ELSE 0 END) AS stored,",
            "SUM(CASE WHEN md.latest_as_of IS NULL THEN 1 ELSE 0 END) AS missing,",
            "SUM(CASE WHEN md.latest_as_of IS NOT NULL AND md.latest_as_of <= ? THEN 1 ELSE 0 END) AS stale,",
            "SUM(CASE WHEN ms.next_eligible_at IS NOT NULL AND ms.next_eligible_at > ? THEN 1 ELSE 0 END) AS blocked,",
            "SUM(CASE WHEN ms.last_status = 'error' THEN 1 ELSE 0 END) AS error_rows",
            "FROM supported_tickers st",
            "LEFT JOIN (",
            "    SELECT security_id, MAX(as_of) AS latest_as_of",
            "    FROM market_data",
            "    GROUP BY security_id",
            ") md ON md.security_id = st.security_id",
            "LEFT JOIN market_data_fetch_state ms ON ms.provider = ? AND ms.provider_symbol = st.provider_symbol",
            "WHERE st.provider = ?",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND st.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        query.append("GROUP BY st.provider_exchange_code")
        query.append("ORDER BY st.provider_exchange_code")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [
            IngestProgressExchange(
                exchange_code=row["exchange_code"],
                total_supported=_coerce_int(row["total_supported"]),
                stored=_coerce_int(row["stored"]),
                missing=_coerce_int(row["missing"]),
                stale=_coerce_int(row["stale"]),
                blocked=_coerce_int(row["blocked"]),
                error_rows=_coerce_int(row["error_rows"]),
            )
            for row in rows
        ]

    def recent_market_data_failures(
        self,
        provider: str,
        exchange_codes: Optional[Sequence[str]] = None,
        limit: int = 10,
    ) -> List[IngestProgressFailure]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        params: List[object] = [provider_norm, provider_norm]
        query = [
            "SELECT st.provider_symbol AS symbol, st.provider_exchange_code AS exchange_code,",
            "ms.last_status, ms.last_error, ms.next_eligible_at, ms.attempts",
            "FROM supported_tickers st",
            "JOIN market_data_fetch_state ms ON ms.provider = ? AND ms.provider_symbol = st.provider_symbol",
            "WHERE st.provider = ? AND ms.last_status = 'error'",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND st.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        query.append(
            "ORDER BY CASE WHEN ms.next_eligible_at IS NULL THEN 1 ELSE 0 END, ms.next_eligible_at ASC, st.provider_symbol ASC"
        )
        query.append("LIMIT ?")
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [
            IngestProgressFailure(
                symbol=row["symbol"],
                exchange_code=row["exchange_code"],
                last_status=row["last_status"],
                last_error=row["last_error"],
                next_eligible_at=row["next_eligible_at"],
                attempts=_coerce_int(row["attempts"]),
            )
            for row in rows
        ]

    def _payload_from_listing(
        self,
        provider: str,
        provider_exchange_code: str,
        listing: Listing,
    ) -> Optional[Tuple[object, ...]]:
        symbol = listing.symbol.strip().upper()
        provider_ticker, _ = _normalize_symbol_base(symbol)
        if not provider_ticker:
            return None
        if provider == "SEC":
            provider_exchange_code = "US"
            provider_symbol = f"{provider_ticker}.US"
        else:
            provider_symbol = self._normalize_provider_symbol(
                provider_ticker, provider_exchange_code
            )
        canonical_exchange = self._supported_exchange_repo().resolve_canonical_code(
            provider, provider_exchange_code
        )
        security = self._security_repo().ensure(
            provider_ticker,
            canonical_exchange,
            entity_name=_normalize_optional_text(listing.security_name),
        )
        listing_exchange = _normalize_optional_text(listing.exchange)
        if listing_exchange is not None:
            listing_exchange = listing_exchange.upper()
        currency = _normalize_optional_text(listing.currency)
        if currency is not None:
            currency = currency.upper()
        return (
            provider,
            provider_symbol,
            provider_ticker,
            provider_exchange_code,
            security.security_id,
            listing_exchange,
            _normalize_optional_text(listing.security_name),
            "ETF" if listing.is_etf else "Common Stock",
            None,
            currency,
            _normalize_optional_text(listing.isin),
            _utc_now_iso(),
        )

    def _payload_from_row(
        self,
        provider: str,
        provider_exchange_code: str,
        row: Dict[str, Any],
    ) -> Optional[Tuple[object, ...]]:
        code = _normalize_optional_text(row.get("Code") or row.get("code"))
        if not code:
            return None
        provider_ticker = code.upper()
        provider_symbol = self._normalize_provider_symbol(
            provider_ticker, provider_exchange_code
        )
        if provider == "SEC":
            provider_exchange_code = "US"
            provider_symbol = f"{provider_ticker}.US"
        canonical_exchange = self._supported_exchange_repo().resolve_canonical_code(
            provider, provider_exchange_code
        )
        security = self._security_repo().ensure(
            provider_ticker,
            canonical_exchange,
            entity_name=_normalize_optional_text(row.get("Name") or row.get("name")),
        )
        listing_exchange = _normalize_optional_text(
            row.get("Exchange") or row.get("exchange")
        )
        if listing_exchange is not None:
            listing_exchange = listing_exchange.upper()
        currency = _normalize_optional_text(row.get("Currency") or row.get("currency"))
        if currency is not None:
            currency = currency.upper()
        isin = _normalize_optional_text(
            row.get("ISIN") or row.get("Isin") or row.get("isin")
        )
        return (
            provider,
            provider_symbol,
            provider_ticker,
            provider_exchange_code,
            security.security_id,
            listing_exchange,
            _normalize_optional_text(row.get("Name") or row.get("name")),
            _normalize_optional_text(row.get("Type") or row.get("security_type")),
            _normalize_optional_text(row.get("Country") or row.get("country")),
            currency,
            isin,
            _utc_now_iso(),
        )

    def _replace_payload(
        self,
        provider: str,
        provider_exchange_code: str,
        payload: Sequence[Tuple[object, ...]],
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM supported_tickers
                WHERE UPPER(provider) = ? AND UPPER(provider_exchange_code) = ?
                """,
                (provider, provider_exchange_code),
            )
            if payload:
                conn.executemany(
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

    @staticmethod
    def _normalize_provider_symbol(
        provider_ticker: str,
        provider_exchange_code: str,
    ) -> str:
        return f"{provider_ticker.strip().upper()}.{provider_exchange_code.strip().upper()}"


class FundamentalsRepository(SQLiteStore):
    """Persist raw fundamentals payloads by provider."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fundamentals_raw (
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
                CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_security
                ON fundamentals_raw(security_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_provider_fetched
                ON fundamentals_raw(provider, fetched_at)
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
        self.initialize_schema()
        provider_symbol, provider_exchange_code, security_id = self._resolve_security(
            provider, symbol, exchange
        )
        provider_norm = provider.strip().upper()
        fetched_at = _utc_now_iso()
        self.upsert_many(
            provider_norm,
            [
                FundamentalsUpdate(
                    security_id=int(security_id or 0),
                    provider_symbol=str(provider_symbol or ""),
                    provider_exchange_code=provider_exchange_code,
                    currency=_normalize_optional_text(
                        currency.upper() if currency else None
                    ),
                    data=json.dumps(payload),
                    fetched_at=fetched_at,
                )
            ],
        )
        FundamentalsFetchStateRepository(self.db_path).mark_success(
            provider_norm,
            str(provider_symbol or ""),
            fetched_at=fetched_at,
        )

    def upsert_many(
        self,
        provider: str,
        updates: Sequence[FundamentalsUpdate],
    ) -> None:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        rows = [
            (
                provider_norm,
                update.provider_symbol.strip().upper(),
                int(update.security_id),
                _normalize_optional_text(update.provider_exchange_code),
                _normalize_optional_text(
                    update.currency.upper() if update.currency else None
                ),
                update.data,
                update.fetched_at,
            )
            for update in updates
            if update.provider_symbol and update.security_id
        ]
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
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
                ON CONFLICT(provider, provider_symbol) DO UPDATE SET
                    security_id = excluded.security_id,
                    provider_exchange_code = COALESCE(excluded.provider_exchange_code, fundamentals_raw.provider_exchange_code),
                    currency = COALESCE(excluded.currency, fundamentals_raw.currency),
                    data = excluded.data,
                    fetched_at = excluded.fetched_at
                """,
                rows,
            )

    def fetch(self, provider: str, symbol: str) -> Optional[Dict[str, Any]]:
        self.initialize_schema()
        provider_symbol, _, _ = self._resolve_security(
            provider, symbol, None, create=False
        )
        if provider_symbol is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT data FROM fundamentals_raw
                WHERE provider = ? AND provider_symbol = ?
                """,
                (provider.strip().upper(), provider_symbol),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def fetch_record(
        self, provider: str, symbol: str
    ) -> Optional[Tuple[str, Optional[str], Dict[str, Any]]]:
        self.initialize_schema()
        provider_symbol, _, _ = self._resolve_security(
            provider, symbol, None, create=False
        )
        if provider_symbol is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT provider_symbol, provider_exchange_code, data
                FROM fundamentals_raw
                WHERE provider = ? AND provider_symbol = ?
                """,
                (provider.strip().upper(), provider_symbol),
            ).fetchone()
        if row is None:
            return None
        return row[0], row[1], json.loads(row[2])

    def fetch_payload_with_fetched_at(
        self, provider: str, symbol: str
    ) -> Optional[Tuple[Dict[str, Any], str]]:
        self.initialize_schema()
        provider_symbol, _, _ = self._resolve_security(
            provider, symbol, None, create=False
        )
        if provider_symbol is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT data, fetched_at
                FROM fundamentals_raw
                WHERE provider = ? AND provider_symbol = ?
                """,
                (provider.strip().upper(), provider_symbol),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["data"]), str(row["fetched_at"])

    def symbols(self, provider: str) -> List[str]:
        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT provider_symbol
                FROM fundamentals_raw
                WHERE provider = ?
                ORDER BY provider_symbol
                """,
                (provider.strip().upper(),),
            ).fetchall()
        return [row[0] for row in rows]

    def symbol_exchanges(self, provider: str) -> List[Tuple[str, Optional[str]]]:
        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT provider_symbol, provider_exchange_code
                FROM fundamentals_raw
                WHERE provider = ?
                ORDER BY provider_symbol
                """,
                (provider.strip().upper(),),
            ).fetchall()
        return [(row[0], row[1]) for row in rows]

    def normalization_candidates(
        self,
        provider: str,
        symbols: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[str, FundamentalsNormalizationCandidate]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}
        if len(normalized) > chunk_size * 4:
            return self._normalization_candidates_for_provider_scan(
                provider_norm,
                requested_symbols=set(normalized),
                chunk_size=chunk_size,
            )

        candidates: Dict[str, FundamentalsNormalizationCandidate] = {}
        with self._connect() as conn:
            for chunk in _batched(normalized, chunk_size):
                candidates.update(
                    self._build_normalization_candidates_for_rows(
                        conn,
                        rows=self._normalization_candidate_rows_for_chunk(
                            conn, provider_norm, chunk
                        ),
                    )
                )
        return candidates

    def _normalization_candidate_rows_for_chunk(
        self,
        conn: sqlite3.Connection,
        provider: str,
        symbols: Sequence[str],
    ) -> List[sqlite3.Row]:
        placeholders = ", ".join("?" for _ in symbols)
        return conn.execute(
            f"""
            SELECT
                fr.provider_symbol,
                fr.security_id,
                fr.fetched_at,
                ns.raw_fetched_at AS normalized_raw_fetched_at,
                ns.last_normalized_at
            FROM fundamentals_raw fr
            LEFT JOIN fundamentals_normalization_state ns
              ON ns.provider = fr.provider
             AND ns.provider_symbol = fr.provider_symbol
            WHERE fr.provider = ? AND fr.provider_symbol IN ({placeholders})
            """,
            [provider, *symbols],
        ).fetchall()

    def _build_normalization_candidates_for_rows(
        self,
        conn: sqlite3.Connection,
        rows: Sequence[sqlite3.Row],
        chunk_size: int = 500,
    ) -> Dict[str, FundamentalsNormalizationCandidate]:
        rows_by_symbol: Dict[str, sqlite3.Row] = {}
        security_ids_needing_provider: List[int] = []
        for row in rows:
            symbol_key = str(row["provider_symbol"])
            rows_by_symbol[symbol_key] = row
            normalized_raw_fetched_at = _normalize_optional_text(
                row["normalized_raw_fetched_at"]
            )
            raw_fetched_at = str(row["fetched_at"])
            if (
                normalized_raw_fetched_at is not None
                and raw_fetched_at <= normalized_raw_fetched_at
            ):
                security_ids_needing_provider.append(int(row["security_id"]))

        source_provider_by_security: Dict[int, str] = {}
        if security_ids_needing_provider:
            for security_chunk in _batched(
                sorted(set(security_ids_needing_provider)), chunk_size
            ):
                provider_placeholders = ", ".join("?" for _ in security_chunk)
                provider_rows = conn.execute(
                    f"""
                    SELECT
                        security_id,
                        MAX(source_provider) AS current_source_provider
                    FROM financial_facts
                    WHERE security_id IN ({provider_placeholders})
                    GROUP BY security_id
                    """,
                    list(security_chunk),
                ).fetchall()
                source_provider_by_security.update(
                    {
                        int(provider_row["security_id"]): str(
                            provider_row["current_source_provider"]
                        )
                        for provider_row in provider_rows
                        if provider_row["current_source_provider"] is not None
                    }
                )

        return {
            symbol_key: FundamentalsNormalizationCandidate(
                provider_symbol=symbol_key,
                security_id=int(row["security_id"]),
                raw_fetched_at=str(row["fetched_at"]),
                normalized_raw_fetched_at=_normalize_optional_text(
                    row["normalized_raw_fetched_at"]
                ),
                last_normalized_at=_normalize_optional_text(row["last_normalized_at"]),
                current_source_provider=source_provider_by_security.get(
                    int(row["security_id"])
                ),
            )
            for symbol_key, row in rows_by_symbol.items()
        }

    def _normalization_candidates_for_provider_scan(
        self,
        provider: str,
        requested_symbols: set[str],
        chunk_size: int = 500,
    ) -> Dict[str, FundamentalsNormalizationCandidate]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    fr.provider_symbol,
                    fr.security_id,
                    fr.fetched_at,
                    ns.raw_fetched_at AS normalized_raw_fetched_at,
                    ns.last_normalized_at
                FROM fundamentals_raw fr
                LEFT JOIN fundamentals_normalization_state ns
                  ON ns.provider = fr.provider
                 AND ns.provider_symbol = fr.provider_symbol
                WHERE fr.provider = ?
                """,
                (provider,),
            ).fetchall()
            filtered_rows = [
                row for row in rows if str(row["provider_symbol"]) in requested_symbols
            ]
            return self._build_normalization_candidates_for_rows(
                conn,
                rows=filtered_rows,
                chunk_size=chunk_size,
            )

    def _resolve_security(
        self,
        provider: str,
        symbol: str,
        exchange: Optional[str],
        create: bool = True,
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        provider_norm = provider.strip().upper()
        ticker, suffix = _normalize_symbol_base(symbol)
        if not ticker:
            return None, None, None
        if provider_norm == "SEC":
            provider_exchange_code = "US"
            provider_symbol = f"{ticker}.US"
        else:
            provider_exchange_code = (exchange or suffix or "").strip().upper()
            if not provider_exchange_code:
                return None, None, None
            provider_symbol = f"{ticker}.{provider_exchange_code}"
        canonical_exchange = self._supported_exchange_repo().resolve_canonical_code(
            provider_norm, provider_exchange_code
        )
        if create:
            security = self._security_repo().ensure(ticker, canonical_exchange)
            return provider_symbol, provider_exchange_code, security.security_id
        existing_security = self._security_repo().fetch_by_symbol(
            f"{ticker}.{canonical_exchange}"
        )
        return (
            provider_symbol,
            provider_exchange_code,
            existing_security.security_id if existing_security else None,
        )


class _FetchStateRepository(SQLiteStore):
    table_name: str
    index_name: str

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
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
                f"""
                CREATE INDEX IF NOT EXISTS {self.index_name}
                ON {self.table_name}(provider, next_eligible_at)
                """
            )

    def fetch(
        self, provider: str, symbol: str
    ) -> Optional[Dict[str, Optional[str] | int]]:
        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT last_fetched_at, last_status, last_error, next_eligible_at, attempts
                FROM {self.table_name}
                WHERE provider = ? AND provider_symbol = ?
                """,
                (provider.strip().upper(), symbol.strip().upper()),
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
        self.initialize_schema()
        timestamp = fetched_at or _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.table_name} (
                    provider,
                    provider_symbol,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, 'ok', NULL, NULL, 0)
                ON CONFLICT(provider, provider_symbol) DO UPDATE SET
                    last_fetched_at = excluded.last_fetched_at,
                    last_status = 'ok',
                    last_error = NULL,
                    next_eligible_at = NULL,
                    attempts = 0
                """,
                (provider.strip().upper(), symbol.strip().upper(), timestamp),
            )

    def mark_success_many(
        self,
        provider: str,
        symbols: Sequence[str],
        fetched_at: Optional[str] = None,
    ) -> None:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return
        provider_norm = provider.strip().upper()
        timestamp = fetched_at or _utc_now_iso()
        rows = [(provider_norm, symbol, timestamp) for symbol in normalized]
        with self._connect() as conn:
            conn.executemany(
                f"""
                INSERT INTO {self.table_name} (
                    provider,
                    provider_symbol,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, 'ok', NULL, NULL, 0)
                ON CONFLICT(provider, provider_symbol) DO UPDATE SET
                    last_fetched_at = excluded.last_fetched_at,
                    last_status = 'ok',
                    last_error = NULL,
                    next_eligible_at = NULL,
                    attempts = 0
                """,
                rows,
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
        attempts = int(state.get("attempts") or 0) if state else 0
        attempts += 1
        backoff = min(base_backoff_seconds * (2 ** (attempts - 1)), max_backoff_seconds)
        now = datetime.now(timezone.utc)
        next_eligible_at = (now + timedelta(seconds=backoff)).isoformat()
        last_fetched_at = state.get("last_fetched_at") if state else None
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {self.table_name} (
                    provider,
                    provider_symbol,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, 'error', ?, ?, ?)
                ON CONFLICT(provider, provider_symbol) DO UPDATE SET
                    last_fetched_at = COALESCE(excluded.last_fetched_at, {self.table_name}.last_fetched_at),
                    last_status = 'error',
                    last_error = excluded.last_error,
                    next_eligible_at = excluded.next_eligible_at,
                    attempts = excluded.attempts
                """,
                (
                    provider.strip().upper(),
                    symbol.strip().upper(),
                    last_fetched_at,
                    error,
                    next_eligible_at,
                    attempts,
                ),
            )

    def mark_failure_many(
        self,
        provider: str,
        errors: Sequence[Tuple[str, str]],
        base_backoff_seconds: int = 3600,
        max_backoff_seconds: int = 86400,
    ) -> None:
        self.initialize_schema()
        normalized_errors = [
            (symbol.strip().upper(), str(error))
            for symbol, error in errors
            if symbol and str(error)
        ]
        if not normalized_errors:
            return
        provider_norm = provider.strip().upper()
        symbols = [symbol for symbol, _ in normalized_errors]
        state_by_symbol: Dict[str, Dict[str, Optional[str] | int]] = {}
        with self._connect() as conn:
            for chunk in _batched(symbols, 500):
                placeholders = ", ".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT provider_symbol, last_fetched_at, attempts
                    FROM {self.table_name}
                    WHERE provider = ? AND provider_symbol IN ({placeholders})
                    """,
                    [provider_norm, *chunk],
                ).fetchall()
                for row in rows:
                    state_by_symbol[row["provider_symbol"]] = {
                        "last_fetched_at": row["last_fetched_at"],
                        "attempts": row["attempts"],
                    }

            now = datetime.now(timezone.utc)
            rows = []
            for symbol, error in normalized_errors:
                state = state_by_symbol.get(symbol)
                attempts = int(state.get("attempts") or 0) if state else 0
                attempts += 1
                backoff = min(
                    base_backoff_seconds * (2 ** (attempts - 1)),
                    max_backoff_seconds,
                )
                next_eligible_at = (now + timedelta(seconds=backoff)).isoformat()
                last_fetched_at = state.get("last_fetched_at") if state else None
                rows.append(
                    (
                        provider_norm,
                        symbol,
                        last_fetched_at,
                        error,
                        next_eligible_at,
                        attempts,
                    )
                )
            conn.executemany(
                f"""
                INSERT INTO {self.table_name} (
                    provider,
                    provider_symbol,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, 'error', ?, ?, ?)
                ON CONFLICT(provider, provider_symbol) DO UPDATE SET
                    last_fetched_at = COALESCE(excluded.last_fetched_at, {self.table_name}.last_fetched_at),
                    last_status = 'error',
                    last_error = excluded.last_error,
                    next_eligible_at = excluded.next_eligible_at,
                    attempts = excluded.attempts
                """,
                rows,
            )

    def delete_symbols(self, provider: str, symbols: Sequence[str]) -> int:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return 0
        placeholders = ", ".join("?" for _ in normalized)
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                DELETE FROM {self.table_name}
                WHERE provider = ? AND provider_symbol IN ({placeholders})
                """,
                [provider.strip().upper(), *normalized],
            )
        return int(cursor.rowcount or 0)


class FundamentalsFetchStateRepository(_FetchStateRepository):
    """Track fundamentals fetch status for resumable ingestion."""

    table_name = "fundamentals_fetch_state"
    index_name = "idx_fundamentals_fetch_next"


class MarketDataFetchStateRepository(_FetchStateRepository):
    """Track market-data fetch status for resumable ingestion."""

    table_name = "market_data_fetch_state"
    index_name = "idx_market_data_fetch_next"


class FundamentalsNormalizationStateRepository(SQLiteStore):
    """Track successful normalization watermarks for stored raw fundamentals."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
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

    def fetch(
        self, provider: str, symbol: str
    ) -> Optional[Dict[str, Optional[str] | int]]:
        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT security_id, raw_fetched_at, last_normalized_at
                FROM fundamentals_normalization_state
                WHERE provider = ? AND provider_symbol = ?
                """,
                (provider.strip().upper(), symbol.strip().upper()),
            ).fetchone()
        if row is None:
            return None
        return {
            "security_id": int(row["security_id"]),
            "raw_fetched_at": row["raw_fetched_at"],
            "last_normalized_at": row["last_normalized_at"],
        }

    def mark_success(
        self,
        provider: str,
        symbol: str,
        security_id: int,
        raw_fetched_at: str,
        normalized_at: Optional[str] = None,
    ) -> None:
        self.initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fundamentals_normalization_state (
                    provider,
                    provider_symbol,
                    security_id,
                    raw_fetched_at,
                    last_normalized_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(provider, provider_symbol) DO UPDATE SET
                    security_id = excluded.security_id,
                    raw_fetched_at = excluded.raw_fetched_at,
                    last_normalized_at = excluded.last_normalized_at
                """,
                (
                    provider.strip().upper(),
                    symbol.strip().upper(),
                    int(security_id),
                    raw_fetched_at,
                    normalized_at or _utc_now_iso(),
                ),
            )

    def delete_symbols(self, provider: str, symbols: Sequence[str]) -> int:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return 0
        placeholders = ", ".join("?" for _ in normalized)
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                DELETE FROM fundamentals_normalization_state
                WHERE provider = ? AND provider_symbol IN ({placeholders})
                """,
                [provider.strip().upper(), *normalized],
            )
        return int(cursor.rowcount or 0)


class FinancialFactsRepository(SQLiteStore):
    """Persist normalized financial facts for downstream metrics."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS financial_facts (
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
                CREATE INDEX IF NOT EXISTS idx_fin_facts_security_concept
                ON financial_facts(security_id, concept)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_fin_facts_concept
                ON financial_facts(concept)
                """
            )
            try:
                # This index is a performance optimization for bulk latest-fact reads.
                # If another process holds the database open, keep the command path
                # working and try again on a later initialize_schema() call.
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_fin_facts_security_concept_latest
                    ON financial_facts(security_id, concept, end_date DESC, filed DESC)
                    """
                )
            except sqlite3.OperationalError as exc:
                if "database is locked" not in str(exc).lower():
                    raise

    def replace_facts(
        self,
        symbol: str,
        records: Iterable[FactRecord],
        source_provider: Optional[str] = None,
    ) -> int:
        rows = [
            (
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
        return self.replace_fact_rows(
            symbol=symbol,
            rows=rows,
            source_provider=source_provider,
        )

    def replace_fact_rows(
        self,
        symbol: str,
        rows: Iterable[StoredFactRow],
        source_provider: Optional[str] = None,
    ) -> int:
        self.initialize_schema()
        security = self._security_repo().ensure_from_symbol(symbol)
        provider = source_provider.strip().upper() if source_provider else None
        prepared_rows = [
            (
                security.security_id,
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
                provider,
            )
            for (
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
            ) in rows
        ]
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM financial_facts WHERE security_id = ?",
                (security.security_id,),
            )
            if prepared_rows:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO financial_facts (
                        security_id, cik, concept, fiscal_period, end_date, unit,
                        value, accn, filed, frame, start_date, accounting_standard,
                        currency, source_provider
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    prepared_rows,
                )
        return len(prepared_rows)

    def latest_fact(
        self,
        symbol: str,
        concept: str,
    ) -> Optional[FactRecord]:
        self.initialize_schema()
        security_id = self._security_repo().resolve_id(symbol)
        if security_id is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT s.canonical_symbol, ff.cik, ff.concept, ff.fiscal_period,
                       ff.end_date, ff.unit, ff.value, ff.accn, ff.filed, ff.frame,
                       ff.start_date, ff.accounting_standard, ff.currency
                FROM financial_facts ff
                JOIN securities s ON s.security_id = ff.security_id
                WHERE ff.security_id = ? AND ff.concept = ?
                ORDER BY ff.end_date DESC, ff.filed DESC
                LIMIT 1
                """,
                [security_id, concept],
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
        self.initialize_schema()
        security_id = self._security_repo().resolve_id(symbol)
        if security_id is None:
            return []
        query = [
            "SELECT s.canonical_symbol, ff.cik, ff.concept, ff.fiscal_period, ff.end_date,",
            "ff.unit, ff.value, ff.accn, ff.filed, ff.frame, ff.start_date, ff.accounting_standard, ff.currency",
            "FROM financial_facts ff",
            "JOIN securities s ON s.security_id = ff.security_id",
            "WHERE ff.security_id = ? AND ff.concept = ?",
        ]
        params: List[Any] = [security_id, concept]
        if fiscal_period:
            query.append("AND ff.fiscal_period = ?")
            params.append(fiscal_period)
        query.append("ORDER BY ff.end_date DESC, ff.filed DESC")
        if limit:
            query.append("LIMIT ?")
            params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [FactRecord(*row) for row in rows]

    def facts_for_symbol(self, symbol: str) -> List[FactRecord]:
        self.initialize_schema()
        security_id = self._security_repo().resolve_id(symbol)
        if security_id is None:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT s.canonical_symbol, ff.cik, ff.concept, ff.fiscal_period,
                       ff.end_date, ff.unit, ff.value, ff.accn, ff.filed, ff.frame,
                       ff.start_date, ff.accounting_standard, ff.currency
                FROM financial_facts ff
                JOIN securities s ON s.security_id = ff.security_id
                WHERE ff.security_id = ?
                ORDER BY ff.concept, ff.end_date DESC, ff.filed DESC
                """,
                (security_id,),
            ).fetchall()
        return [FactRecord(*row) for row in rows]

    def latest_numeric_values_for_concept_many(
        self,
        symbols: Sequence[str],
        concept: str,
        chunk_size: int = 500,
    ) -> Dict[str, float]:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}
        security_ids_by_symbol = self._security_repo().resolve_ids_many(
            normalized,
            chunk_size=chunk_size,
        )
        with self._connect() as conn:
            self._populate_temp_selected_securities(
                conn,
                security_ids_by_symbol,
                chunk_size=chunk_size,
            )
            return self._latest_numeric_values_for_temp_selected_securities(
                conn,
                concept,
            )

    def _populate_temp_selected_securities(
        self,
        conn: sqlite3.Connection,
        security_ids_by_symbol: Mapping[str, int],
        chunk_size: int = 500,
    ) -> None:
        conn.execute("DROP TABLE IF EXISTS temp_selected_securities")
        conn.execute(
            """
            CREATE TEMP TABLE temp_selected_securities (
                security_id INTEGER PRIMARY KEY,
                canonical_symbol TEXT NOT NULL
            )
            """
        )
        rows = [
            (security_id, symbol)
            for symbol, security_id in security_ids_by_symbol.items()
            if security_id is not None
        ]
        for chunk in _batched(rows, chunk_size):
            conn.executemany(
                """
                INSERT INTO temp_selected_securities (security_id, canonical_symbol)
                VALUES (?, ?)
                """,
                list(chunk),
            )

    def _latest_numeric_values_for_temp_selected_securities(
        self,
        conn: sqlite3.Connection,
        concept: str,
    ) -> Dict[str, float]:
        values: Dict[str, float] = {}
        rows = conn.execute(
            """
            SELECT
                selected.canonical_symbol,
                (
                    SELECT ff.value
                    FROM financial_facts ff INDEXED BY idx_fin_facts_security_concept_latest
                    WHERE ff.security_id = selected.security_id
                      AND ff.concept = ?
                    ORDER BY ff.end_date DESC, ff.filed DESC
                    LIMIT 1
                ) AS value
            FROM temp_selected_securities selected
            """,
            (concept,),
        ).fetchall()
        for row in rows:
            try:
                if row["value"] is None:
                    continue
                values[row["canonical_symbol"]] = float(row["value"])
            except (TypeError, ValueError):
                continue
        return values

    def _latest_share_counts_for_temp_selected_securities(
        self,
        conn: sqlite3.Connection,
        primary_concept: str,
        fallback_concept: str,
    ) -> Dict[str, float]:
        counts: Dict[str, float] = {}
        rows = conn.execute(
            """
            SELECT
                selected.canonical_symbol,
                COALESCE(
                    (
                        SELECT ff.value
                        FROM financial_facts ff INDEXED BY idx_fin_facts_security_concept_latest
                        WHERE ff.security_id = selected.security_id
                          AND ff.concept = ?
                        ORDER BY ff.end_date DESC, ff.filed DESC
                        LIMIT 1
                    ),
                    (
                        SELECT ff.value
                        FROM financial_facts ff INDEXED BY idx_fin_facts_security_concept_latest
                        WHERE ff.security_id = selected.security_id
                          AND ff.concept = ?
                        ORDER BY ff.end_date DESC, ff.filed DESC
                        LIMIT 1
                    )
                ) AS value
            FROM temp_selected_securities selected
            """,
            (primary_concept, fallback_concept),
        ).fetchall()
        for row in rows:
            try:
                if row["value"] is None:
                    continue
                counts[row["canonical_symbol"]] = float(row["value"])
            except (TypeError, ValueError):
                continue
        return counts

    def latest_share_counts_many(
        self,
        symbols: Sequence[str],
        concepts: Optional[Sequence[str]] = None,
        chunk_size: int = 500,
        security_ids_by_symbol: Optional[Mapping[str, int]] = None,
    ) -> Dict[str, float]:
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}
        normalized_set = set(normalized)

        if security_ids_by_symbol is None:
            selected_security_ids = self._security_repo().resolve_ids_many(
                normalized,
                chunk_size=chunk_size,
            )
        else:
            selected_security_ids = {
                symbol: security_id
                for symbol, security_id in security_ids_by_symbol.items()
                if symbol in normalized_set and security_id is not None
            }

        counts: Dict[str, float] = {}
        concept_order = list(concepts or ())
        if not concept_order:
            concept_order = [
                "EntityCommonStockSharesOutstanding",
                "CommonStockSharesOutstanding",
            ]
        with self._connect() as conn:
            self._populate_temp_selected_securities(
                conn,
                selected_security_ids,
                chunk_size=chunk_size,
            )
            if concept_order == [
                "EntityCommonStockSharesOutstanding",
                "CommonStockSharesOutstanding",
            ]:
                return self._latest_share_counts_for_temp_selected_securities(
                    conn,
                    primary_concept=concept_order[0],
                    fallback_concept=concept_order[1],
                )
            for concept in concept_order:
                latest_values = (
                    self._latest_numeric_values_for_temp_selected_securities(
                        conn,
                        concept,
                    )
                )
                for symbol, value in latest_values.items():
                    counts.setdefault(symbol, value)
        return counts


class MetricsRepository(SQLiteStore):
    """Persist computed metric values."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics (
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
                CREATE INDEX IF NOT EXISTS idx_metrics_metric_id
                ON metrics(metric_id)
                """
            )

    def upsert(self, symbol: str, metric_id: str, value: float, as_of: str) -> None:
        self.upsert_many([(symbol, metric_id, value, as_of)])

    def upsert_many(
        self,
        rows: Iterable[StoredMetricRow],
    ) -> int:
        self.initialize_schema()
        metric_rows = list(rows)
        if not metric_rows:
            return 0

        unique_symbols = []
        seen_symbols = set()
        for symbol, _, _, _ in metric_rows:
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            unique_symbols.append(symbol)

        security_ids = self._security_repo().resolve_ids_many(unique_symbols)
        for symbol in unique_symbols:
            if symbol in security_ids:
                continue
            security = self._security_repo().ensure_from_symbol(symbol)
            security_ids[symbol] = security.security_id
        persisted_rows = [
            (
                security_ids[symbol],
                metric_id,
                value,
                as_of,
            )
            for symbol, metric_id, value, as_of in metric_rows
            if symbol in security_ids
        ]
        if not persisted_rows:
            return 0

        def _persist() -> None:
            with self._connect() as conn:
                conn.executemany(
                    """
                    INSERT INTO metrics (security_id, metric_id, value, as_of)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(security_id, metric_id) DO UPDATE SET
                        value = excluded.value,
                        as_of = excluded.as_of
                    """,
                    persisted_rows,
                )

        self._run_with_locked_retry(_persist)
        return len(persisted_rows)

    def fetch(self, symbol: str, metric_id: str) -> Optional[Tuple[float, str]]:
        self.initialize_schema()
        security_id = self._security_repo().resolve_id(symbol)
        if security_id is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT value, as_of
                FROM metrics
                WHERE security_id = ? AND metric_id = ?
                """,
                (security_id, metric_id),
            ).fetchone()
        if row is None:
            return None
        return row[0], row[1]


class MarketDataRepository(SQLiteStore):
    """Persist canonical market data snapshots."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_data (
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
                CREATE INDEX IF NOT EXISTS idx_market_data_latest
                ON market_data(security_id, as_of DESC)
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
        source_provider: Optional[str] = None,
    ) -> None:
        self.initialize_schema()
        security = self._security_repo().ensure_from_symbol(symbol)
        self.upsert_prices(
            [
                MarketDataUpdate(
                    security_id=security.security_id,
                    symbol=symbol.strip().upper(),
                    as_of=as_of,
                    price=price,
                    volume=volume,
                    market_cap=market_cap,
                    currency=currency,
                    source_provider=(source_provider or "EODHD").strip().upper(),
                )
            ]
        )

    def upsert_prices(self, rows: Sequence[MarketDataUpdate]) -> None:
        self.initialize_schema()
        if not rows:
            return
        updated_at = _utc_now_iso()
        payload = [
            (
                row.security_id,
                row.as_of,
                row.price,
                row.volume,
                row.market_cap,
                row.currency,
                row.source_provider.strip().upper(),
                updated_at,
            )
            for row in rows
        ]
        with self._connect() as conn:
            conn.executemany(
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
                ON CONFLICT(security_id, as_of) DO UPDATE SET
                    price = excluded.price,
                    volume = excluded.volume,
                    market_cap = excluded.market_cap,
                    currency = excluded.currency,
                    source_provider = excluded.source_provider,
                    updated_at = excluded.updated_at
                """,
                payload,
            )

    def latest_snapshot(self, symbol: str) -> Optional[PriceData]:
        self.initialize_schema()
        security_id = self._security_repo().resolve_id(symbol)
        if security_id is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT s.canonical_symbol, md.as_of, md.price, md.volume,
                       md.market_cap, md.currency
                FROM market_data md
                JOIN securities s ON s.security_id = md.security_id
                WHERE md.security_id = ?
                ORDER BY md.as_of DESC
                LIMIT 1
                """,
                (security_id,),
            ).fetchone()
        if row is None:
            return None
        return PriceData(
            symbol=row["canonical_symbol"],
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

    def latest_snapshots_many(
        self,
        symbols: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[str, MarketSnapshotRecord]:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}

        snapshots: Dict[str, MarketSnapshotRecord] = {}
        with self._connect() as conn:
            for chunk in _batched(normalized, chunk_size):
                placeholders = ", ".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT
                        ranked.security_id,
                        ranked.canonical_symbol,
                        ranked.as_of,
                        ranked.price,
                        ranked.volume,
                        ranked.market_cap,
                        ranked.currency
                    FROM (
                        SELECT
                            s.security_id,
                            s.canonical_symbol,
                            md.as_of,
                            md.price,
                            md.volume,
                            md.market_cap,
                            md.currency,
                            ROW_NUMBER() OVER (
                                PARTITION BY md.security_id
                                ORDER BY md.as_of DESC
                            ) AS row_num
                        FROM market_data md
                        JOIN securities s ON s.security_id = md.security_id
                        WHERE s.canonical_symbol IN ({placeholders})
                    ) ranked
                    WHERE ranked.row_num = 1
                    ORDER BY ranked.canonical_symbol
                    """,
                    list(chunk),
                ).fetchall()
                for row in rows:
                    snapshots[row["canonical_symbol"]] = MarketSnapshotRecord(
                        security_id=row["security_id"],
                        symbol=row["canonical_symbol"],
                        as_of=row["as_of"],
                        price=row["price"],
                        volume=row["volume"],
                        market_cap=row["market_cap"],
                        currency=row["currency"],
                    )
        return snapshots

    def update_market_cap(self, symbol: str, market_cap: float) -> int:
        self.initialize_schema()
        security_id = self._security_repo().resolve_id(symbol)
        if security_id is None:
            return 0
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE market_data
                SET market_cap = ?, updated_at = ?
                WHERE security_id = ?
                  AND as_of = (
                      SELECT MAX(as_of)
                      FROM market_data
                      WHERE security_id = ?
                  )
                """,
                (market_cap, _utc_now_iso(), security_id, security_id),
            )
        return int(cursor.rowcount or 0)

    def update_market_caps_many(
        self,
        rows: Sequence[Tuple[int, str, float]],
    ) -> int:
        self.initialize_schema()
        if not rows:
            return 0
        updated_at = _utc_now_iso()
        payload = [
            (market_cap, updated_at, security_id, as_of)
            for security_id, as_of, market_cap in rows
        ]
        with self._connect() as conn:
            before = conn.total_changes
            conn.executemany(
                """
                UPDATE market_data
                SET market_cap = ?, updated_at = ?
                WHERE security_id = ? AND as_of = ?
                """,
                payload,
            )
            return int(conn.total_changes - before)


class EntityMetadataRepository(SQLiteStore):
    """Compatibility wrapper backed by canonical securities metadata."""

    def initialize_schema(self) -> None:
        self._security_repo().initialize_schema()

    def upsert(
        self,
        symbol: str,
        entity_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        self._security_repo().upsert_metadata(
            symbol,
            entity_name=entity_name,
            description=description,
        )

    def fetch(self, symbol: str) -> Optional[str]:
        return self._security_repo().fetch_name(symbol)

    def fetch_description(self, symbol: str) -> Optional[str]:
        return self._security_repo().fetch_description(symbol)


def _normalized_codes(values: Optional[Sequence[str]]) -> List[str]:
    if not values:
        return []
    normalized: List[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        for part in str(value).split(","):
            candidate = part.strip().upper()
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            normalized.append(candidate)
    return sorted(normalized)


__all__ = [
    "Security",
    "SecurityRepository",
    "FundamentalsRepository",
    "SupportedExchange",
    "SupportedExchangeRepository",
    "IngestProgressSummary",
    "IngestProgressExchange",
    "IngestProgressFailure",
    "SupportedTicker",
    "SupportedTickerRepository",
    "FundamentalsFetchStateRepository",
    "MarketDataFetchStateRepository",
    "FinancialFactsRepository",
    "MarketDataRepository",
    "FactRecord",
    "MarketSnapshotRecord",
    "MetricsRepository",
    "EntityMetadataRepository",
]
