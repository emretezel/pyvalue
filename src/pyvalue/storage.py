"""Local persistence helpers for universe data.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
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
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from pyvalue.currency import (
    MetricUnitKind,
    SHARES_UNIT,
    fact_currency_or_none,
    metric_currency_or_none,
    normalize_currency_code,
)
from pyvalue.marketdata.base import MarketDataUpdate, PriceData
from pyvalue.migrations import apply_migrations
from pyvalue.universe import Listing


SQLITE_BUSY_TIMEOUT_MS = 30000
SQLITE_LOCK_RETRY_ATTEMPTS = 5
SQLITE_LOCK_RETRY_SLEEP_SECONDS = 0.5
SQLITE_MAX_BOUND_PARAMETERS = 999

# Connection-scoped pragmas applied on every SQLiteStore._connect() to amortise
# fact reads, metric writes, and per-symbol metadata lookups. See _connect()
# for the rationale on each value.
_CONNECTION_PERFORMANCE_PRAGMAS: Tuple[str, ...] = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA cache_size=-65536",
    "PRAGMA temp_store=MEMORY",
    "PRAGMA mmap_size=268435456",
)

_PRIMARY_LISTING_SOURCE_PROVIDER = "EODHD"
_LISTING_STATUS_UNKNOWN = "unknown"
_LISTING_STATUS_PRIMARY = "primary"
_LISTING_STATUS_SECONDARY = "secondary"
_LISTING_CLASS_MATCHED_PRIMARY = "matched_primary_ticker"
_LISTING_CLASS_DIFFERENT_PRIMARY = "different_primary_ticker"
_LISTING_CLASS_MISSING_PRIMARY = "missing_primary_ticker"


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


def _normalize_qualified_symbol(value: Any) -> Optional[str]:
    text = _normalize_optional_text(value)
    if text is None:
        return None
    ticker, exchange = _normalize_symbol_base(text)
    if not ticker or exchange is None:
        return None
    return f"{ticker}.{exchange.upper()}"


def _coerce_int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _normalize_provider_identity(
    provider: str,
    symbol: str,
    exchange_code: Optional[str] = None,
) -> Tuple[str, str, str, str]:
    provider_norm = _normalize_required_text(provider, "provider").upper()
    symbol_norm = _normalize_required_text(symbol, "symbol").upper()
    explicit_exchange = _normalize_optional_text(exchange_code)
    if explicit_exchange is not None:
        provider_exchange_code = explicit_exchange.upper()
        suffix = f".{provider_exchange_code}"
        bare_symbol = (
            symbol_norm[: -len(suffix)] if symbol_norm.endswith(suffix) else symbol_norm
        )
    else:
        provider_symbol, inferred_exchange = _normalize_symbol_base(symbol_norm)
        bare_symbol = _normalize_required_text(provider_symbol, "symbol").upper()
        provider_exchange_code = inferred_exchange or ""
    if provider_norm == "SEC":
        provider_exchange_code = "US"
        if bare_symbol.endswith(".US"):
            bare_symbol = bare_symbol[:-3]
    elif not provider_exchange_code:
        raise ValueError(
            f"Could not infer provider exchange code for {provider_norm}:{symbol}"
        )
    return (
        provider_norm,
        bare_symbol,
        provider_exchange_code,
        f"{bare_symbol}.{provider_exchange_code}",
    )


def _ensure_provider_listing_catalog_views(conn: sqlite3.Connection) -> None:
    """Create compatibility catalog views over the physical provider_listing table."""

    def create_view(sql: str) -> None:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError as exc:
            if "already exists" not in str(exc).lower():
                raise

    conn.execute("DROP VIEW IF EXISTS supported_tickers")
    conn.execute("DROP VIEW IF EXISTS provider_listing_catalog")
    create_view(
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
            COALESCE(pl.currency, l.currency) AS currency,
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
    create_view(
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


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row["name"])
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def _batched(values: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _primary_listing_predicate(alias: str = "catalog") -> str:
    return f"{alias}.primary_listing_status <> '{_LISTING_STATUS_SECONDARY}'"


@dataclass(frozen=True)
class Security:
    """Canonical security identity."""

    security_id: int
    canonical_ticker: str
    canonical_exchange_code: str
    canonical_symbol: str
    entity_name: Optional[str] = None
    description: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class Exchange:
    """Canonical exchange identity."""

    exchange_id: int
    exchange_code: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @property
    def code(self) -> str:
        return self.exchange_code


@dataclass(frozen=True)
class Provider:
    """Persisted provider registry entry."""

    provider_id: int
    provider_code: str
    display_name: str
    description: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class ExchangeProvider:
    """Persisted provider-supported exchange metadata."""

    provider: str
    provider_exchange_code: str
    exchange_id: int
    exchange_code: str
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

    @property
    def canonical_exchange_code(self) -> str:
        return self.exchange_code


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

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "currency",
            fact_currency_or_none(self.currency, self.unit),
        )


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
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class FinancialFactsRefreshStateRecord:
    """Latest financial-facts refresh watermark for one symbol."""

    symbol: str
    refreshed_at: str


@dataclass(frozen=True)
class MetricComputeStatusRecord:
    """Latest persisted metric-computation attempt for one symbol/metric."""

    symbol: str
    metric_id: str
    status: Literal["success", "failure"]
    attempted_at: str
    reason_code: Optional[str] = None
    reason_detail: Optional[str] = None
    value_as_of: Optional[str] = None
    facts_refreshed_at: Optional[str] = None
    market_data_as_of: Optional[str] = None
    market_data_updated_at: Optional[str] = None


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


class MetricRecord(NamedTuple):
    """Stored metric value with explicit unit metadata."""

    value: float
    as_of: str
    unit_kind: MetricUnitKind
    currency: Optional[str]
    unit_label: Optional[str]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, tuple) and len(other) == 2:
            return (self.value, self.as_of) == other
        return tuple.__eq__(self, other)


StoredMetricRow = Tuple[
    str,
    str,
    float,
    str,
    MetricUnitKind,
    Optional[str],
    Optional[str],
]


@dataclass(frozen=True)
class FXRateRecord:
    """Persisted direct FX rate observation."""

    provider: str
    rate_date: str
    base_currency: str
    quote_currency: str
    rate_text: str
    fetched_at: str
    source_kind: str
    meta_json: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class FXSupportedPairRecord:
    """Persisted FX catalog entry for one provider symbol."""

    provider: str
    symbol: str
    canonical_symbol: str
    base_currency: Optional[str]
    quote_currency: Optional[str]
    name: Optional[str]
    is_alias: bool
    is_refreshable: bool
    last_seen_at: Optional[str] = None


@dataclass(frozen=True)
class FXRefreshStateRecord:
    """Persisted refresh coverage metadata for one canonical FX symbol."""

    provider: str
    canonical_symbol: str
    min_rate_date: Optional[str]
    max_rate_date: Optional[str]
    full_history_backfilled: bool
    last_fetched_at: Optional[str]
    last_status: Optional[str]
    last_error: Optional[str]
    attempts: int


@dataclass(frozen=True)
class FundamentalsUpdate:
    """Raw fundamentals payload prepared for batch persistence."""

    security_id: int
    provider_symbol: str
    provider_exchange_code: Optional[str]
    listing_currency: Optional[str]
    data: str
    fetched_at: str


@dataclass(frozen=True)
class SecurityListingStatusRecord:
    """Primary-vs-secondary listing classification for one canonical listing."""

    security_id: int
    source_provider: str
    provider_symbol: str
    raw_fetched_at: str
    is_primary_listing: bool
    primary_provider_symbol: Optional[str]
    classification_basis: Literal[
        "matched_primary_ticker",
        "different_primary_ticker",
        "missing_primary_ticker",
    ]
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class SecurityMetadataCandidate:
    """Canonical metadata extracted from stored raw fundamentals."""

    entity_name: Optional[str] = None
    description: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None

    def to_update_fields(self) -> Dict[str, str]:
        """Return only metadata fields that should overwrite canonicals."""

        update_fields: Dict[str, str] = {}
        if self.entity_name is not None:
            update_fields["entity_name"] = self.entity_name
        if self.description is not None:
            update_fields["description"] = self.description
        if self.sector is not None:
            update_fields["sector"] = self.sector
        if self.industry is not None:
            update_fields["industry"] = self.industry
        return update_fields


@dataclass(frozen=True)
class SecurityMetadataUpdate:
    """Canonical security metadata prepared for batched persistence."""

    security_id: int
    entity_name: Optional[str] = None
    description: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None


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
        self._provider_repo_cache: Optional[ProviderRepository] = None
        self._security_repo_cache: Optional[SecurityRepository] = None
        self._supported_ticker_repo_cache: Optional[SupportedTickerRepository] = None
        self._exchange_repo_cache: Optional[ExchangeRepository] = None
        self._exchange_provider_repo_cache: Optional[ExchangeProviderRepository] = None

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=SQLITE_BUSY_TIMEOUT_MS / 1000.0,
            factory=_ManagedSQLiteConnection,
        )
        self._configure_connection(conn)
        return conn

    def open_persistent_connection(self) -> sqlite3.Connection:
        """Open a long-lived connection for callers that batch many writes.

        Unlike ``_connect()``, the returned connection uses the standard
        :class:`sqlite3.Connection` factory -- its ``__exit__`` commits/rolls
        back without closing the file handle, so the same connection can be
        reused across many ``with conn:`` blocks. The caller is responsible
        for closing it.
        """

        conn = sqlite3.connect(
            self.db_path,
            timeout=SQLITE_BUSY_TIMEOUT_MS / 1000.0,
        )
        self._configure_connection(conn)
        return conn

    @staticmethod
    def _configure_connection(conn: sqlite3.Connection) -> None:
        """Apply row factory + busy/perf pragmas to a fresh connection."""

        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA foreign_keys=ON")
        # Performance pragmas applied to every connection. journal_mode=WAL is
        # safe to set repeatedly (no-op when already enabled) and lets readers
        # run while a writer holds the database. synchronous=NORMAL is the
        # WAL-recommended durability level: still crash-safe across application
        # restarts, only loses committed-but-unflushed transactions on a host
        # power failure -- acceptable for the batch ETL workloads in this repo.
        # cache_size negative -> KiB, raising the per-connection page cache
        # from the SQLite default of ~2 MiB to 64 MiB. temp_store=MEMORY keeps
        # CTE/subquery scratch in RAM, and mmap_size enables memory-mapped
        # reads on hot pages.
        for pragma in _CONNECTION_PERFORMANCE_PRAGMAS:
            try:
                conn.execute(pragma)
            except sqlite3.OperationalError:
                # WAL toggling can fail transiently if another process is
                # mid-checkpoint; the rest of the pragmas are independent and
                # we never want connection setup to abort the caller.
                pass

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

    def _provider_repo(self) -> ProviderRepository:
        if self._provider_repo_cache is None:
            self._provider_repo_cache = ProviderRepository(self.db_path)
        return self._provider_repo_cache

    def _exchange_repo(self) -> ExchangeRepository:
        if self._exchange_repo_cache is None:
            self._exchange_repo_cache = ExchangeRepository(self.db_path)
        return self._exchange_repo_cache

    def _exchange_provider_repo(self) -> ExchangeProviderRepository:
        if self._exchange_provider_repo_cache is None:
            self._exchange_provider_repo_cache = ExchangeProviderRepository(
                self.db_path
            )
        return self._exchange_provider_repo_cache

    def _supported_ticker_repo(self) -> SupportedTickerRepository:
        if self._supported_ticker_repo_cache is None:
            self._supported_ticker_repo_cache = SupportedTickerRepository(self.db_path)
        return self._supported_ticker_repo_cache

    def ticker_currency(self, symbol: str) -> Optional[str]:
        """Return the catalog listing currency for ``symbol``.

        Provider-listing currency is preferred over canonical listing currency.
        Market data rows store quote-row currency only and are not a source of
        truth for the listing's normalization/metric currency.
        """

        apply_migrations(self.db_path)
        symbol_norm = symbol.strip().upper()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(pl.currency, l.currency) AS currency
                FROM listing l
                JOIN "exchange" e ON e.exchange_id = l.exchange_id
                LEFT JOIN provider_listing pl ON pl.listing_id = l.listing_id
                LEFT JOIN provider p ON p.provider_id = pl.provider_id
                WHERE UPPER(l.symbol || '.' || e.exchange_code) = ?
                  AND COALESCE(pl.currency, l.currency) IS NOT NULL
                ORDER BY
                    CASE WHEN pl.currency IS NOT NULL THEN 0 ELSE 1 END,
                    CASE
                        WHEN p.provider_code = 'EODHD' THEN 0
                        WHEN p.provider_code = 'SEC' THEN 1
                        ELSE 2
                    END,
                    pl.provider_listing_id
                LIMIT 1
                """,
                (symbol_norm,),
            ).fetchone()
        return normalize_currency_code(row[0]) if row else None


class ProviderRepository(SQLiteStore):
    """Persist and resolve provider registry rows."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
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

    def ensure(
        self,
        provider_code: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        *,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Provider:
        if connection is None:
            self.initialize_schema()
        provider_norm = _normalize_required_text(provider_code, "provider_code").upper()
        now = _utc_now_iso()
        name = _normalize_optional_text(display_name) or provider_norm

        def _ensure(conn: sqlite3.Connection) -> Provider:
            conn.execute(
                """
                INSERT INTO provider (
                    provider_code,
                    display_name,
                    description,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(provider_code) DO UPDATE SET
                    display_name = COALESCE(excluded.display_name, provider.display_name),
                    description = COALESCE(excluded.description, provider.description),
                    updated_at = excluded.updated_at
                """,
                (
                    provider_norm,
                    name,
                    _normalize_optional_text(description),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                """
                SELECT
                    provider_id,
                    provider_code,
                    display_name,
                    description,
                    created_at,
                    updated_at
                FROM provider
                WHERE provider_code = ?
                """,
                (provider_norm,),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"Failed to persist provider {provider_norm}")
            return Provider(*row)

        if connection is not None:
            return _ensure(connection)
        with self._connect() as conn:
            return _ensure(conn)

    def fetch(self, provider_code: str) -> Optional[Provider]:
        self.initialize_schema()
        provider_norm = provider_code.strip().upper()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    provider_id,
                    provider_code,
                    display_name,
                    description,
                    created_at,
                    updated_at
                FROM provider
                WHERE provider_code = ?
                """,
                (provider_norm,),
            ).fetchone()
        return Provider(*row) if row else None

    def resolve_id(
        self,
        provider_code: str,
        *,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Optional[int]:
        provider_norm = provider_code.strip().upper()
        if connection is not None:
            row = connection.execute(
                """
                SELECT provider_id
                FROM provider
                WHERE provider_code = ?
                """,
                (provider_norm,),
            ).fetchone()
            return int(row["provider_id"]) if row else None
        provider = self.fetch(provider_norm)
        return provider.provider_id if provider else None


class SecurityRepository(SQLiteStore):
    """Persist canonical security identities."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._by_symbol: Dict[str, Security] = {}
        self._by_id: Dict[int, Security] = {}

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._exchange_repo().initialize_schema()
        with self._connect() as conn:
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
                    primary_listing_status TEXT NOT NULL DEFAULT 'unknown'
                        CHECK (primary_listing_status IN ('unknown', 'primary', 'secondary')),
                    UNIQUE (exchange_id, symbol),
                    FOREIGN KEY (issuer_id) REFERENCES issuer(issuer_id),
                    FOREIGN KEY (exchange_id) REFERENCES "exchange"(exchange_id)
                )
                """
            )
            columns = _table_columns(conn, "listing")
            if "primary_listing_status" not in columns:
                conn.execute(
                    """
                    ALTER TABLE listing
                    ADD COLUMN primary_listing_status TEXT NOT NULL DEFAULT 'unknown'
                    CHECK (primary_listing_status IN ('unknown', 'primary', 'secondary'))
                    """
                )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_listing_exchange
                ON listing(exchange_id)
                """
            )
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

    def _select_identity_sql(self, where_sql: str) -> str:
        return f"""
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
            WHERE {where_sql}
        """

    def _load_by_exchange_and_symbol(
        self,
        conn: sqlite3.Connection,
        exchange_id: int,
        ticker: str,
    ) -> Optional[Security]:
        row = conn.execute(
            self._select_identity_sql("l.exchange_id = ? AND l.symbol = ?"),
            (exchange_id, ticker),
        ).fetchone()
        if row is None:
            return None
        security = Security(*row)
        self._remember(security)
        return security

    def ensure(
        self,
        canonical_ticker: str,
        canonical_exchange_code: str,
        entity_name: Optional[str] = None,
        description: Optional[str] = None,
        sector: Optional[str] = None,
        industry: Optional[str] = None,
        *,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Security:
        if connection is None:
            self.initialize_schema()
        ticker = _normalize_required_text(canonical_ticker, "canonical_ticker").upper()
        exchange_code = _normalize_required_text(
            canonical_exchange_code, "canonical_exchange_code"
        ).upper()
        canonical_symbol = f"{ticker}.{exchange_code}"
        entity_name = _normalize_optional_text(entity_name)
        description = _normalize_optional_text(description)
        sector = _normalize_optional_text(sector)
        industry = _normalize_optional_text(industry)

        def _ensure(conn: sqlite3.Connection) -> Optional[Security]:
            exchange = self._exchange_repo().ensure(exchange_code, connection=conn)
            security = self._load_by_exchange_and_symbol(
                conn, exchange.exchange_id, ticker
            )
            if security is None:
                cursor = conn.execute(
                    """
                    INSERT INTO issuer (
                        name,
                        description,
                        sector,
                        industry,
                        country
                    ) VALUES (?, ?, ?, ?, NULL)
                    """,
                    (entity_name, description, sector, industry),
                )
                if cursor.lastrowid is None:
                    raise RuntimeError(
                        f"Failed to create issuer for {canonical_symbol}"
                    )
                issuer_id = int(cursor.lastrowid)
                conn.execute(
                    """
                    INSERT INTO listing (
                        issuer_id,
                        exchange_id,
                        symbol,
                        currency
                    ) VALUES (?, ?, ?, NULL)
                    """,
                    (issuer_id, exchange.exchange_id, ticker),
                )
            else:
                conn.execute(
                    """
                    UPDATE issuer
                    SET name = COALESCE(?, name),
                        description = COALESCE(?, description),
                        sector = COALESCE(?, sector),
                        industry = COALESCE(?, industry)
                    WHERE issuer_id = (
                        SELECT issuer_id
                        FROM listing
                        WHERE listing_id = ?
                    )
                    """,
                    (
                        entity_name,
                        description,
                        sector,
                        industry,
                        security.security_id,
                    ),
                )
            return self._load_by_exchange_and_symbol(conn, exchange.exchange_id, ticker)

        if connection is not None:
            loaded = _ensure(connection)
        else:
            with self._connect() as conn:
                loaded = _ensure(conn)
        if loaded is None:  # pragma: no cover - defensive
            raise RuntimeError(f"Failed to create or load security {canonical_symbol}")
        return loaded

    def ensure_from_symbol(
        self,
        symbol: str,
        exchange_code: Optional[str] = None,
        entity_name: Optional[str] = None,
        description: Optional[str] = None,
        sector: Optional[str] = None,
        industry: Optional[str] = None,
        *,
        connection: Optional[sqlite3.Connection] = None,
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
            sector=sector,
            industry=industry,
            connection=connection,
        )

    def fetch_by_symbol(self, symbol: str) -> Optional[Security]:
        self.initialize_schema()
        normalized = symbol.strip().upper()
        cached = self._by_symbol.get(normalized)
        if cached is not None:
            return cached
        with self._connect() as conn:
            row = conn.execute(
                self._select_identity_sql(
                    "UPPER(l.symbol || '.' || e.exchange_code) = ?"
                ),
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
                self._select_identity_sql("l.listing_id = ?"),
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
        *,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, int]:
        if connection is None:
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

        def _query(conn: sqlite3.Connection) -> None:
            for chunk in _batched(uncached, chunk_size):
                placeholders = ", ".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT
                        l.listing_id AS security_id,
                        l.symbol || '.' || e.exchange_code AS canonical_symbol
                    FROM listing l
                    JOIN "exchange" e ON e.exchange_id = l.exchange_id
                    WHERE UPPER(l.symbol || '.' || e.exchange_code) IN ({placeholders})
                    """,
                    list(chunk),
                ).fetchall()
                for row in rows:
                    resolved[row["canonical_symbol"]] = row["security_id"]

        if connection is not None:
            _query(connection)
        else:
            with self._connect() as conn:
                _query(conn)
        return resolved

    def fetch_many_by_symbol(
        self,
        symbols: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[str, Security]:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}

        resolved: Dict[str, Security] = {}
        uncached: List[str] = []
        for symbol in normalized:
            cached = self._by_symbol.get(symbol)
            if cached is not None:
                resolved[symbol] = cached
            else:
                uncached.append(symbol)
        if not uncached:
            return resolved

        with self._connect() as conn:
            for chunk in _batched(uncached, chunk_size):
                placeholders = ", ".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
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
                    WHERE UPPER(l.symbol || '.' || e.exchange_code) IN ({placeholders})
                    """,
                    list(chunk),
                ).fetchall()
                for row in rows:
                    security = Security(*row)
                    resolved[security.canonical_symbol] = security
                    self._remember(security)
        return resolved

    def fetch_many_by_id(
        self,
        security_ids: Sequence[int],
        chunk_size: int = 500,
    ) -> Dict[int, Security]:
        self.initialize_schema()
        normalized = sorted(
            {int(security_id) for security_id in security_ids if security_id}
        )
        if not normalized:
            return {}

        resolved: Dict[int, Security] = {}
        uncached: List[int] = []
        for security_id in normalized:
            cached = self._by_id.get(security_id)
            if cached is not None:
                resolved[security_id] = cached
            else:
                uncached.append(security_id)
        if not uncached:
            return resolved

        with self._connect() as conn:
            for chunk in _batched(uncached, chunk_size):
                placeholders = ", ".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
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
                    WHERE l.listing_id IN ({placeholders})
                    """,
                    list(chunk),
                ).fetchall()
                for row in rows:
                    security = Security(*row)
                    resolved[security.security_id] = security
                    self._remember(security)
        return resolved

    def canonical_symbol(self, security_id: int) -> Optional[str]:
        security = self.fetch(security_id)
        return security.canonical_symbol if security else None

    def upsert_metadata(
        self,
        symbol: str,
        entity_name: Optional[str] = None,
        description: Optional[str] = None,
        sector: Optional[str] = None,
        industry: Optional[str] = None,
    ) -> None:
        if not entity_name and not description and not sector and not industry:
            return
        security = self.ensure_from_symbol(
            symbol,
            entity_name=entity_name,
            description=description,
            sector=sector,
            industry=industry,
        )
        self._remember(security)

    def upsert_metadata_many(
        self,
        updates: Sequence[SecurityMetadataUpdate],
    ) -> int:
        """Persist many canonical metadata updates in one transaction."""

        self.initialize_schema()
        rows = [
            (
                _normalize_optional_text(update.entity_name),
                _normalize_optional_text(update.description),
                _normalize_optional_text(update.sector),
                _normalize_optional_text(update.industry),
                int(update.security_id),
            )
            for update in updates
            if update.security_id
            and (
                update.entity_name is not None
                or update.description is not None
                or update.sector is not None
                or update.industry is not None
            )
        ]
        if not rows:
            return 0

        security_ids = [row[-1] for row in rows]
        with self._connect() as conn:
            before = conn.total_changes
            conn.executemany(
                """
                UPDATE issuer
                SET name = COALESCE(?, name),
                    description = COALESCE(?, description),
                    sector = COALESCE(?, sector),
                    industry = COALESCE(?, industry)
                WHERE issuer_id = (
                    SELECT issuer_id
                    FROM listing
                    WHERE listing_id = ?
                )
                """,
                rows,
            )
            updated = int(conn.total_changes - before)

        for security_id in security_ids:
            cached = self._by_id.pop(security_id, None)
            if cached is not None:
                self._by_symbol.pop(cached.canonical_symbol, None)
        self.fetch_many_by_id(security_ids)
        return updated

    def fetch_name(self, symbol: str) -> Optional[str]:
        security = self.fetch_by_symbol(symbol)
        return security.entity_name if security else None

    def fetch_description(self, symbol: str) -> Optional[str]:
        security = self.fetch_by_symbol(symbol)
        return security.description if security else None

    def fetch_sector(self, symbol: str) -> Optional[str]:
        security = self.fetch_by_symbol(symbol)
        return security.sector if security else None

    def fetch_industry(self, symbol: str) -> Optional[str]:
        security = self.fetch_by_symbol(symbol)
        return security.industry if security else None

    def list_supported_symbols(
        self,
        exchange_codes: Optional[Sequence[str]] = None,
        *,
        primary_only: bool = False,
    ) -> List[str]:
        self.initialize_schema()
        params: List[object] = []
        query = [
            "SELECT DISTINCT l.symbol || '.' || e.exchange_code AS canonical_symbol",
            "FROM provider_listing pl",
            "JOIN listing l ON l.listing_id = pl.listing_id",
            'JOIN "exchange" e ON e.exchange_id = l.exchange_id',
        ]
        normalized = _normalized_codes(exchange_codes)
        if normalized:
            placeholders = ", ".join("?" for _ in normalized)
            query.append(f"WHERE UPPER(e.exchange_code) IN ({placeholders})")
            params.extend(normalized)
            if primary_only:
                query.append(f"AND {_primary_listing_predicate('l')}")
        elif primary_only:
            query.append(f"WHERE {_primary_listing_predicate('l')}")
        query.append("ORDER BY canonical_symbol")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [row[0] for row in rows]

    def list_supported_symbol_name_pairs(
        self,
        exchange_codes: Optional[Sequence[str]] = None,
        *,
        primary_only: bool = False,
    ) -> List[Tuple[str, Optional[str]]]:
        self.initialize_schema()
        params: List[object] = []
        query = [
            "SELECT l.symbol || '.' || e.exchange_code AS canonical_symbol,",
            "COALESCE(i.name, l.symbol || '.' || e.exchange_code) AS entity_name",
            "FROM provider_listing pl",
            "JOIN listing l ON l.listing_id = pl.listing_id",
            "JOIN issuer i ON i.issuer_id = l.issuer_id",
            'JOIN "exchange" e ON e.exchange_id = l.exchange_id',
        ]
        normalized = _normalized_codes(exchange_codes)
        if normalized:
            placeholders = ", ".join("?" for _ in normalized)
            query.append(f"WHERE UPPER(e.exchange_code) IN ({placeholders})")
            params.extend(normalized)
            if primary_only:
                query.append(f"AND {_primary_listing_predicate('l')}")
        elif primary_only:
            query.append(f"WHERE {_primary_listing_predicate('l')}")
        query.append("GROUP BY l.listing_id, canonical_symbol, i.name")
        query.append("ORDER BY canonical_symbol")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [(row["canonical_symbol"], row["entity_name"]) for row in rows]

    def _remember(self, security: Security) -> None:
        self._by_id[security.security_id] = security
        self._by_symbol[security.canonical_symbol] = security


class ExchangeRepository(SQLiteStore):
    """Persist canonical exchange identities."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
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

    def ensure(
        self,
        exchange_code: str,
        *,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Exchange:
        if connection is None:
            self.initialize_schema()
        code_norm = _normalize_required_text(exchange_code, "exchange_code").upper()
        now = _utc_now_iso()

        def _ensure(conn: sqlite3.Connection) -> Exchange:
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
                (code_norm, now, now),
            )
            row = conn.execute(
                """
                SELECT exchange_id, exchange_code, created_at, updated_at
                FROM "exchange"
                WHERE exchange_code = ?
                """,
                (code_norm,),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"Failed to persist exchange {code_norm}")
            return Exchange(*row)

        if connection is not None:
            return _ensure(connection)
        with self._connect() as conn:
            return _ensure(conn)

    def fetch(self, exchange_code: str) -> Optional[Exchange]:
        self.initialize_schema()
        code_norm = exchange_code.strip().upper()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT exchange_id, exchange_code, created_at, updated_at
                FROM "exchange"
                WHERE exchange_code = ?
                """,
                (code_norm,),
            ).fetchone()
        return Exchange(*row) if row else None

    def list_all(self) -> List[Exchange]:
        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange_id, exchange_code, created_at, updated_at
                FROM "exchange"
                ORDER BY exchange_code
                """
            ).fetchall()
        return [Exchange(*row) for row in rows]


class ExchangeProviderRepository(SQLiteStore):
    """Store exchange catalogs published by data providers."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._provider_repo().initialize_schema()
        self._exchange_repo().initialize_schema()
        with self._connect() as conn:
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

    def replace_for_provider(
        self,
        provider: str,
        rows: Sequence[Dict[str, Any]],
    ) -> int:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        updated_at = _utc_now_iso()
        payload: List[Tuple[object, ...]] = []
        with self._connect() as conn:
            provider_row = self._provider_repo().ensure(
                provider_norm,
                connection=conn,
            )
            for row in rows:
                code = _normalize_optional_text(
                    row.get("Code") or row.get("provider_exchange_code")
                )
                if not code:
                    continue
                code_norm = code.upper()
                canonical_exchange_code = _normalize_optional_text(
                    row.get("CanonicalExchangeCode")
                    or row.get("canonical_exchange_code")
                )
                exchange = self._exchange_repo().ensure(
                    (canonical_exchange_code or code_norm).upper(),
                    connection=conn,
                )
                payload.append(
                    (
                        provider_row.provider_id,
                        code_norm,
                        exchange.exchange_id,
                        _normalize_optional_text(row.get("Name") or row.get("name")),
                        _normalize_optional_text(
                            row.get("Country") or row.get("country")
                        ),
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

            if payload:
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
                    payload,
                )
            retained_codes = {row[1] for row in payload}
            stale_rows = conn.execute(
                """
                SELECT provider_exchange_id
                FROM provider_exchange
                WHERE provider_id = ?
                """,
                (provider_row.provider_id,),
            ).fetchall()
            for stale_row in stale_rows:
                provider_exchange_id = int(stale_row["provider_exchange_id"])
                current = conn.execute(
                    """
                    SELECT provider_exchange_code
                    FROM provider_exchange
                    WHERE provider_exchange_id = ?
                    """,
                    (provider_exchange_id,),
                ).fetchone()
                if current is None:
                    continue
                if str(current["provider_exchange_code"]) in retained_codes:
                    continue
                provider_listing_ref = conn.execute(
                    """
                    SELECT 1
                    FROM provider_listing
                    WHERE provider_exchange_id = ?
                    LIMIT 1
                    """,
                    (provider_exchange_id,),
                ).fetchone()
                if provider_listing_ref is None:
                    conn.execute(
                        """
                        DELETE FROM provider_exchange
                        WHERE provider_exchange_id = ?
                        """,
                        (provider_exchange_id,),
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
            provider_row = self._provider_repo().ensure(
                provider.strip().upper(),
                connection=conn,
            )
            exchange = self._exchange_repo().ensure(
                canonical_exchange_code.strip().upper(),
                connection=conn,
            )
            conn.execute(
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
                ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
                ON CONFLICT(provider_id, provider_exchange_code) DO UPDATE SET
                    exchange_id = excluded.exchange_id,
                    name = COALESCE(excluded.name, provider_exchange.name),
                    country = COALESCE(excluded.country, provider_exchange.country),
                    currency = COALESCE(excluded.currency, provider_exchange.currency),
                    updated_at = excluded.updated_at
                """,
                (
                    provider_row.provider_id,
                    provider_exchange_code.strip().upper(),
                    exchange.exchange_id,
                    _normalize_optional_text(name),
                    _normalize_optional_text(country),
                    _normalize_optional_text(currency),
                    _utc_now_iso(),
                ),
            )

    def fetch(self, provider: str, code: str) -> Optional[ExchangeProvider]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        code_norm = code.strip().upper()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    p.provider_code AS provider,
                    ep.provider_exchange_code,
                    ep.exchange_id,
                    e.exchange_code,
                    ep.name,
                    ep.country,
                    ep.currency,
                    ep.operating_mic,
                    ep.country_iso2,
                    ep.country_iso3,
                    ep.updated_at
                FROM provider_exchange ep
                JOIN provider p ON p.provider_id = ep.provider_id
                JOIN "exchange" e ON e.exchange_id = ep.exchange_id
                WHERE UPPER(p.provider_code) = ? AND UPPER(ep.provider_exchange_code) = ?
                """,
                (provider_norm, code_norm),
            ).fetchone()
        return ExchangeProvider(*row) if row else None

    def list_all(self, provider: Optional[str] = None) -> List[ExchangeProvider]:
        self.initialize_schema()
        params: List[object] = []
        query = [
            "SELECT p.provider_code AS provider, ep.provider_exchange_code, ep.exchange_id,",
            "e.exchange_code, ep.name, ep.country, ep.currency, ep.operating_mic,",
            "ep.country_iso2, ep.country_iso3, ep.updated_at",
            "FROM provider_exchange ep",
            "JOIN provider p ON p.provider_id = ep.provider_id",
            'JOIN "exchange" e ON e.exchange_id = ep.exchange_id',
        ]
        if provider:
            query.append("WHERE UPPER(p.provider_code) = ?")
            params.append(provider.strip().upper())
        query.append("ORDER BY p.provider_code, ep.provider_exchange_code")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [ExchangeProvider(*row) for row in rows]

    def resolve_canonical_code(self, provider: str, provider_exchange_code: str) -> str:
        record = self.fetch(provider, provider_exchange_code)
        if record is not None:
            return record.exchange_code
        return provider_exchange_code.strip().upper()


class SupportedTickerRepository(SQLiteStore):
    """Store provider-supported ticker catalogs by exchange."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._provider_repo().initialize_schema()
        self._exchange_provider_repo().initialize_schema()
        self._security_repo().initialize_schema()
        with self._connect() as conn:
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
            _ensure_provider_listing_catalog_views(conn)
        FundamentalsRepository(self.db_path).initialize_schema()
        FundamentalsFetchStateRepository(self.db_path).initialize_schema()
        MarketDataRepository(self.db_path).initialize_schema()
        MarketDataFetchStateRepository(self.db_path).initialize_schema()

    @staticmethod
    def _catalog_select_columns(alias: str = "catalog") -> str:
        return (
            f"{alias}.provider, {alias}.provider_exchange_code, "
            f"{alias}.provider_symbol, {alias}.provider_ticker, "
            f"{alias}.security_id, {alias}.listing_exchange, "
            f"{alias}.security_name, {alias}.security_type, "
            f"{alias}.country, {alias}.currency, {alias}.isin, {alias}.updated_at"
        )

    def _ensure_provider_exchange_row(
        self,
        conn: sqlite3.Connection,
        provider: str,
        provider_exchange_code: str,
        *,
        canonical_exchange_code: Optional[str] = None,
        name: Optional[str] = None,
        country: Optional[str] = None,
        currency: Optional[str] = None,
        operating_mic: Optional[str] = None,
        country_iso2: Optional[str] = None,
        country_iso3: Optional[str] = None,
    ) -> sqlite3.Row:
        provider_row = self._provider_repo().ensure(provider, connection=conn)
        exchange_code = (
            _normalize_optional_text(canonical_exchange_code) or provider_exchange_code
        ).upper()
        exchange = self._exchange_repo().ensure(exchange_code, connection=conn)
        now = _utc_now_iso()
        conn.execute(
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
                name = COALESCE(excluded.name, provider_exchange.name),
                country = COALESCE(excluded.country, provider_exchange.country),
                currency = COALESCE(excluded.currency, provider_exchange.currency),
                operating_mic = COALESCE(excluded.operating_mic, provider_exchange.operating_mic),
                country_iso2 = COALESCE(excluded.country_iso2, provider_exchange.country_iso2),
                country_iso3 = COALESCE(excluded.country_iso3, provider_exchange.country_iso3),
                updated_at = excluded.updated_at
            """,
            (
                provider_row.provider_id,
                provider_exchange_code,
                exchange.exchange_id,
                _normalize_optional_text(name),
                _normalize_optional_text(country),
                _normalize_optional_text(currency),
                _normalize_optional_text(operating_mic),
                _normalize_optional_text(country_iso2),
                _normalize_optional_text(country_iso3),
                now,
            ),
        )
        row = conn.execute(
            """
            SELECT
                ep.provider_exchange_id,
                ep.provider_id,
                ep.provider_exchange_code,
                e.exchange_code
            FROM provider_exchange ep
            JOIN "exchange" e ON e.exchange_id = ep.exchange_id
            WHERE ep.provider_id = ? AND ep.provider_exchange_code = ?
            """,
            (provider_row.provider_id, provider_exchange_code),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"Failed to persist provider exchange {provider}:{provider_exchange_code}"
            )
        return row

    def _ensure_provider_listing(
        self,
        conn: sqlite3.Connection,
        provider: str,
        symbol: str,
        *,
        exchange_code: Optional[str] = None,
        currency: Optional[str] = None,
        entity_name: Optional[str] = None,
    ) -> sqlite3.Row:
        provider_norm, bare_symbol, provider_exchange_code, _ = (
            _normalize_provider_identity(
                provider,
                symbol,
                exchange_code,
            )
        )
        provider_exchange_row = self._ensure_provider_exchange_row(
            conn,
            provider_norm,
            provider_exchange_code,
        )
        security = self._security_repo().ensure(
            bare_symbol,
            str(provider_exchange_row["exchange_code"]),
            entity_name=entity_name,
            connection=conn,
        )
        conn.execute(
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
            (
                int(provider_exchange_row["provider_id"]),
                int(provider_exchange_row["provider_exchange_id"]),
                bare_symbol,
                _normalize_optional_text(currency.upper() if currency else None),
                security.security_id,
            ),
        )
        row = conn.execute(
            """
            SELECT *
            FROM provider_listing_catalog
            WHERE provider = ?
              AND provider_exchange_code = ?
              AND provider_ticker = ?
            """,
            (provider_norm, provider_exchange_code, bare_symbol),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"Failed to persist provider listing {provider_norm}:{bare_symbol}.{provider_exchange_code}"
            )
        return row

    def _delete_provider_listing_ids(
        self,
        conn: sqlite3.Connection,
        provider_listing_ids: Sequence[int],
    ) -> None:
        normalized = sorted({int(value) for value in provider_listing_ids if value})
        if not normalized:
            return
        for chunk in _batched(normalized, 500):
            placeholders = ", ".join("?" for _ in chunk)
            conn.execute(
                f"""
                DELETE FROM fundamentals_raw
                WHERE provider_listing_id IN ({placeholders})
                """,
                list(chunk),
            )
            conn.execute(
                f"""
                DELETE FROM fundamentals_fetch_state
                WHERE provider_listing_id IN ({placeholders})
                """,
                list(chunk),
            )
            conn.execute(
                f"""
                DELETE FROM fundamentals_normalization_state
                WHERE provider_listing_id IN ({placeholders})
                """,
                list(chunk),
            )
            conn.execute(
                f"""
                DELETE FROM market_data_fetch_state
                WHERE provider_listing_id IN ({placeholders})
                """,
                list(chunk),
            )
            conn.execute(
                f"""
                DELETE FROM provider_listing
                WHERE provider_listing_id IN ({placeholders})
                """,
                list(chunk),
            )

    def replace_from_listings(
        self,
        provider: str,
        exchange_code: str,
        listings: Sequence[Listing],
    ) -> int:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        provider_exchange_code = exchange_code.strip().upper()
        retained_tickers: List[str] = []
        with self._connect() as conn:
            provider_exchange_row = self._ensure_provider_exchange_row(
                conn,
                provider_norm,
                provider_exchange_code,
            )
            for listing in listings:
                symbol = listing.symbol.strip().upper()
                bare_symbol, _ = _normalize_symbol_base(symbol)
                if not bare_symbol:
                    continue
                retained_tickers.append(bare_symbol)
                self._ensure_provider_listing(
                    conn,
                    provider_norm,
                    symbol,
                    exchange_code=provider_exchange_code,
                    currency=listing.currency,
                    entity_name=listing.security_name,
                )
            existing_rows = conn.execute(
                """
                SELECT provider_listing_id, provider_symbol
                FROM provider_listing
                WHERE provider_exchange_id = ?
                """,
                (int(provider_exchange_row["provider_exchange_id"]),),
            ).fetchall()
            to_delete = [
                int(row["provider_listing_id"])
                for row in existing_rows
                if str(row["provider_symbol"]) not in set(retained_tickers)
            ]
            self._delete_provider_listing_ids(conn, to_delete)
        return len(retained_tickers)

    def replace_for_exchange(
        self,
        provider: str,
        exchange_code: str,
        rows: Sequence[Dict[str, Any]],
    ) -> int:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        provider_exchange_code = exchange_code.strip().upper()
        retained_tickers: List[str] = []
        with self._connect() as conn:
            provider_exchange_row = self._ensure_provider_exchange_row(
                conn,
                provider_norm,
                provider_exchange_code,
            )
            for row in rows:
                code = _normalize_optional_text(row.get("Code") or row.get("code"))
                if not code:
                    continue
                bare_symbol = code.upper()
                retained_tickers.append(bare_symbol)
                self._ensure_provider_exchange_row(
                    conn,
                    provider_norm,
                    provider_exchange_code,
                    canonical_exchange_code=(
                        row.get("CanonicalExchangeCode")
                        or row.get("canonical_exchange_code")
                    ),
                    name=row.get("Name") or row.get("name"),
                    country=row.get("Country") or row.get("country"),
                    currency=row.get("Currency") or row.get("currency"),
                    operating_mic=row.get("OperatingMIC") or row.get("operating_mic"),
                    country_iso2=row.get("CountryISO2") or row.get("country_iso2"),
                    country_iso3=row.get("CountryISO3") or row.get("country_iso3"),
                )
                self._ensure_provider_listing(
                    conn,
                    provider_norm,
                    bare_symbol,
                    exchange_code=provider_exchange_code,
                    currency=row.get("Currency") or row.get("currency"),
                    entity_name=row.get("Name") or row.get("name"),
                )
            existing_rows = conn.execute(
                """
                SELECT provider_listing_id, provider_symbol
                FROM provider_listing
                WHERE provider_exchange_id = ?
                """,
                (int(provider_exchange_row["provider_exchange_id"]),),
            ).fetchall()
            retained = set(retained_tickers)
            to_delete = [
                int(row["provider_listing_id"])
                for row in existing_rows
                if str(row["provider_symbol"]) not in retained
            ]
            self._delete_provider_listing_ids(conn, to_delete)
        return len(retained_tickers)

    def fetch_for_symbol(self, provider: str, symbol: str) -> Optional[SupportedTicker]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        symbol_norm = symbol.strip().upper()
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT {self._catalog_select_columns()}
                FROM provider_listing_catalog catalog
                WHERE UPPER(catalog.provider) = ? AND UPPER(catalog.provider_symbol) = ?
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
        *,
        primary_only: bool = False,
    ) -> List[SupportedTicker]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        params: List[object] = [provider_norm]
        query = [
            f"SELECT {self._catalog_select_columns('catalog')}",
            "FROM provider_listing_catalog catalog",
        ]
        query.append("WHERE UPPER(catalog.provider) = ?")
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(
                f"AND UPPER(catalog.provider_exchange_code) IN ({placeholders})"
            )
            params.extend(normalized_codes)
        normalized_symbols = _normalized_codes(provider_symbols)
        if normalized_symbols:
            placeholders = ", ".join("?" for _ in normalized_symbols)
            query.append(f"AND UPPER(catalog.provider_symbol) IN ({placeholders})")
            params.extend(normalized_symbols)
        if primary_only:
            query.append(f"AND {_primary_listing_predicate()}")
        query.append("ORDER BY catalog.provider_exchange_code, catalog.provider_symbol")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [SupportedTicker(*row) for row in rows]

    def list_symbols_by_exchange(
        self,
        provider: str,
        exchange_code: str,
        *,
        primary_only: bool = False,
    ) -> List[str]:
        rows = self.list_for_provider(
            provider,
            exchange_codes=[exchange_code],
            primary_only=primary_only,
        )
        return [row.provider_symbol for row in rows]

    def list_symbol_name_pairs_by_exchange(
        self,
        provider: str,
        exchange_code: str,
        *,
        primary_only: bool = False,
    ) -> List[Tuple[str, Optional[str]]]:
        rows = self.list_for_provider(
            provider,
            exchange_codes=[exchange_code],
            primary_only=primary_only,
        )
        return [(row.provider_symbol, row.security_name) for row in rows]

    def list_canonical_symbols(
        self,
        exchange_codes: Optional[Sequence[str]] = None,
        *,
        primary_only: bool = False,
    ) -> List[str]:
        return self._security_repo().list_supported_symbols(
            exchange_codes,
            primary_only=primary_only,
        )

    def list_canonical_symbol_name_pairs(
        self,
        exchange_codes: Optional[Sequence[str]] = None,
        *,
        primary_only: bool = False,
    ) -> List[Tuple[str, Optional[str]]]:
        return self._security_repo().list_supported_symbol_name_pairs(
            exchange_codes,
            primary_only=primary_only,
        )

    def available_exchanges(self, provider: Optional[str] = None) -> List[str]:
        self.initialize_schema()
        params: List[object] = []
        query = ["SELECT DISTINCT provider_exchange_code FROM provider_listing_catalog"]
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
        with self._connect() as conn:
            params: List[object] = []
            query = [
                "SELECT provider_listing_id FROM provider_listing_catalog WHERE 1 = 1"
            ]
            if provider:
                query.append("AND UPPER(provider) = ?")
                params.append(provider.strip().upper())
            if exchange_code:
                query.append("AND UPPER(provider_exchange_code) = ?")
                params.append(exchange_code.strip().upper())
            rows = conn.execute(" ".join(query), params).fetchall()
            provider_listing_ids = [int(row["provider_listing_id"]) for row in rows]
            self._delete_provider_listing_ids(conn, provider_listing_ids)
        return len(provider_listing_ids)

    def delete_symbols(self, provider: str, symbols: Sequence[str]) -> int:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return 0
        placeholders = ", ".join("?" for _ in normalized)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT provider_listing_id
                FROM provider_listing_catalog
                WHERE UPPER(provider) = ? AND UPPER(provider_symbol) IN ({placeholders})
                """,
                [provider.strip().upper(), *normalized],
            ).fetchall()
            provider_listing_ids = [int(row["provider_listing_id"]) for row in rows]
            self._delete_provider_listing_ids(conn, provider_listing_ids)
        return len(provider_listing_ids)

    def list_for_exchange(
        self,
        provider: str,
        exchange_code: str,
        *,
        primary_only: bool = False,
    ) -> List[SupportedTicker]:
        return self.list_for_provider(
            provider,
            exchange_codes=[exchange_code],
            primary_only=primary_only,
        )

    def fetch_currency(
        self,
        symbol: str,
        provider: Optional[str] = None,
    ) -> Optional[str]:
        """Return listing currency from provider-listing/catalog metadata."""

        self.initialize_schema()
        symbol_norm = symbol.strip().upper()
        params: List[object] = []
        query = [
            "SELECT catalog.currency",
            "FROM provider_listing_catalog catalog",
            "JOIN listing l ON l.listing_id = catalog.security_id",
            'JOIN "exchange" e ON e.exchange_id = l.exchange_id',
            "WHERE catalog.currency IS NOT NULL",
        ]
        if provider:
            params.append(provider.strip().upper())
            query.append("AND UPPER(catalog.provider) = ?")
        params.extend([symbol_norm, symbol_norm])
        query.append(
            "AND (UPPER(catalog.provider_symbol) = ? OR UPPER(l.symbol || '.' || e.exchange_code) = ?)"
        )
        query.append(
            "ORDER BY CASE WHEN catalog.provider = 'EODHD' THEN 0 WHEN catalog.provider = 'SEC' THEN 1 ELSE 2 END"
        )
        query.append("LIMIT 1")
        with self._connect() as conn:
            row = conn.execute(" ".join(query), params).fetchone()
        return normalize_currency_code(row[0]) if row else None

    def list_all_exchanges(self, provider: str) -> List[str]:
        return self.available_exchanges(provider)

    def list_eligible_for_fundamentals(
        self,
        provider: str,
        exchange_codes: Optional[Sequence[str]] = None,
        max_age_days: Optional[int] = None,
        max_symbols: Optional[int] = None,
        respect_backoff: bool = True,
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
                query.append(f"AND catalog.provider_exchange_code IN ({placeholders})")
                params.extend(normalized_codes)
            if normalized_symbols:
                placeholders = ", ".join("?" for _ in normalized_symbols)
                query.append(f"AND catalog.provider_symbol IN ({placeholders})")
                params.extend(normalized_symbols)

        def _fetch_missing(limit: Optional[int]) -> List[SupportedTicker]:
            params: List[object] = [provider_norm]
            query = [
                f"SELECT {self._catalog_select_columns('catalog')}",
                "FROM provider_listing_catalog catalog",
                "LEFT JOIN fundamentals_fetch_state fs "
                "ON fs.provider_listing_id = catalog.provider_listing_id",
                "WHERE catalog.provider = ?",
                "AND fs.last_fetched_at IS NULL",
            ]
            _apply_scope_filters(query, params)
            if respect_backoff:
                query.append(
                    "AND (fs.next_eligible_at IS NULL OR fs.next_eligible_at <= ?)"
                )
                params.append(now.isoformat())
            query.append("ORDER BY catalog.provider_symbol ASC")
            if limit is not None:
                query.append("LIMIT ?")
                params.append(limit)
            with self._connect() as conn:
                rows = conn.execute(" ".join(query), params).fetchall()
            return [SupportedTicker(*row) for row in rows]

        def _fetch_stale(limit: Optional[int], cutoff: str) -> List[SupportedTicker]:
            params: List[object] = [provider_norm, cutoff]
            query = [
                f"SELECT {self._catalog_select_columns('catalog')}",
                "FROM fundamentals_fetch_state fs",
                "JOIN provider_listing_catalog catalog "
                "ON catalog.provider_listing_id = fs.provider_listing_id",
                "WHERE catalog.provider = ?",
                "AND fs.last_fetched_at IS NOT NULL",
                "AND fs.last_fetched_at <= ?",
            ]
            _apply_scope_filters(query, params)
            if respect_backoff:
                query.append(
                    "AND (fs.next_eligible_at IS NULL OR fs.next_eligible_at <= ?)"
                )
                params.append(now.isoformat())
            query.append("ORDER BY fs.last_fetched_at ASC, catalog.provider_symbol ASC")
            if limit is not None:
                query.append("LIMIT ?")
                params.append(limit)
            with self._connect() as conn:
                rows = conn.execute(" ".join(query), params).fetchall()
            return [SupportedTicker(*row) for row in rows]

        if max_age_days is None and not missing_only:
            params: List[object] = [provider_norm]
            query = [
                f"SELECT {self._catalog_select_columns('catalog')}",
                "FROM provider_listing_catalog catalog",
                "LEFT JOIN fundamentals_fetch_state fs "
                "ON fs.provider_listing_id = catalog.provider_listing_id",
                "WHERE catalog.provider = ?",
            ]
            _apply_scope_filters(query, params)
            if respect_backoff:
                query.append(
                    "AND (fs.next_eligible_at IS NULL OR fs.next_eligible_at <= ?)"
                )
                params.append(now.isoformat())
            query.append("ORDER BY catalog.provider_symbol ASC")
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
            "catalog.provider_exchange_code AS exchange_code,",
            "COUNT(*) AS total_supported,",
            "SUM(CASE WHEN fs.last_fetched_at IS NOT NULL THEN 1 ELSE 0 END) AS stored,",
            "SUM(CASE WHEN fs.last_fetched_at IS NULL THEN 1 ELSE 0 END) AS missing,",
            f"{stale_expr} AS stale,",
            "SUM(CASE WHEN fs.next_eligible_at IS NOT NULL AND fs.next_eligible_at > ? THEN 1 ELSE 0 END) AS blocked,",
            "SUM(CASE WHEN fs.last_status = 'error' THEN 1 ELSE 0 END) AS error_rows",
            "FROM provider_listing_catalog catalog",
            "LEFT JOIN fundamentals_fetch_state fs "
            "ON fs.provider_listing_id = catalog.provider_listing_id",
            "WHERE catalog.provider = ?",
        ]
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND catalog.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        query.append("GROUP BY catalog.provider_exchange_code")
        query.append("ORDER BY catalog.provider_exchange_code")
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
        params: List[object] = [provider_norm]
        query = [
            "SELECT catalog.provider_symbol AS symbol, catalog.provider_exchange_code AS exchange_code,",
            "fs.last_status, fs.last_error, fs.next_eligible_at, fs.attempts",
            "FROM provider_listing_catalog catalog",
            "JOIN fundamentals_fetch_state fs "
            "ON fs.provider_listing_id = catalog.provider_listing_id",
            "WHERE catalog.provider = ? AND fs.last_status = 'error'",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND catalog.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        query.append(
            "ORDER BY CASE WHEN fs.next_eligible_at IS NULL THEN 1 ELSE 0 END, "
            "fs.next_eligible_at ASC, catalog.provider_symbol ASC"
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
        respect_backoff: bool = True,
        provider_symbols: Optional[Sequence[str]] = None,
        *,
        primary_only: bool = False,
    ) -> List[SupportedTicker]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        now = datetime.now(timezone.utc)
        cutoff = (now.date() - timedelta(days=max_age_days)).isoformat()
        params: List[object] = [provider_norm]
        query = [
            f"SELECT {self._catalog_select_columns('catalog')}",
            "FROM provider_listing_catalog catalog",
            "LEFT JOIN (",
            "    SELECT listing_id, MAX(as_of) AS latest_as_of",
            "    FROM market_data",
            "    GROUP BY listing_id",
            ") md ON md.listing_id = catalog.security_id",
            "LEFT JOIN market_data_fetch_state ms "
            "ON ms.provider_listing_id = catalog.provider_listing_id",
            "WHERE catalog.provider = ?",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND catalog.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        normalized_symbols = _normalized_codes(provider_symbols)
        if normalized_symbols:
            placeholders = ", ".join("?" for _ in normalized_symbols)
            query.append(f"AND catalog.provider_symbol IN ({placeholders})")
            params.extend(normalized_symbols)
        if primary_only:
            query.append(f"AND {_primary_listing_predicate()}")
        query.append("AND (md.latest_as_of IS NULL OR md.latest_as_of <= ?)")
        params.append(cutoff)
        if respect_backoff:
            query.append(
                "AND (ms.next_eligible_at IS NULL OR ms.next_eligible_at <= ?)"
            )
            params.append(now.isoformat())
        query.append(
            "ORDER BY CASE WHEN md.latest_as_of IS NULL THEN 0 ELSE 1 END, "
            "md.latest_as_of ASC, catalog.provider_exchange_code ASC, catalog.provider_symbol ASC"
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
        *,
        primary_only: bool = False,
    ) -> IngestProgressSummary:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        now = datetime.now(timezone.utc).isoformat()
        cutoff = (
            datetime.now(timezone.utc).date() - timedelta(days=max_age_days)
        ).isoformat()
        params: List[object] = [cutoff, now, provider_norm]
        query = [
            "SELECT",
            "COUNT(*) AS total_supported,",
            "SUM(CASE WHEN md.latest_as_of IS NOT NULL THEN 1 ELSE 0 END) AS stored,",
            "SUM(CASE WHEN md.latest_as_of IS NULL THEN 1 ELSE 0 END) AS missing,",
            "SUM(CASE WHEN md.latest_as_of IS NOT NULL AND md.latest_as_of <= ? THEN 1 ELSE 0 END) AS stale,",
            "SUM(CASE WHEN ms.next_eligible_at IS NOT NULL AND ms.next_eligible_at > ? THEN 1 ELSE 0 END) AS blocked,",
            "SUM(CASE WHEN ms.last_status = 'error' THEN 1 ELSE 0 END) AS error_rows",
            "FROM provider_listing_catalog catalog",
            "LEFT JOIN (",
            "    SELECT listing_id, MAX(as_of) AS latest_as_of",
            "    FROM market_data",
            "    GROUP BY listing_id",
            ") md ON md.listing_id = catalog.security_id",
            "LEFT JOIN market_data_fetch_state ms "
            "ON ms.provider_listing_id = catalog.provider_listing_id",
            "WHERE catalog.provider = ?",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND catalog.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        if primary_only:
            query.append(f"AND {_primary_listing_predicate()}")
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
        *,
        primary_only: bool = False,
    ) -> List[IngestProgressExchange]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        now = datetime.now(timezone.utc).isoformat()
        cutoff = (
            datetime.now(timezone.utc).date() - timedelta(days=max_age_days)
        ).isoformat()
        params: List[object] = [cutoff, now, provider_norm]
        query = [
            "SELECT",
            "catalog.provider_exchange_code AS exchange_code,",
            "COUNT(*) AS total_supported,",
            "SUM(CASE WHEN md.latest_as_of IS NOT NULL THEN 1 ELSE 0 END) AS stored,",
            "SUM(CASE WHEN md.latest_as_of IS NULL THEN 1 ELSE 0 END) AS missing,",
            "SUM(CASE WHEN md.latest_as_of IS NOT NULL AND md.latest_as_of <= ? THEN 1 ELSE 0 END) AS stale,",
            "SUM(CASE WHEN ms.next_eligible_at IS NOT NULL AND ms.next_eligible_at > ? THEN 1 ELSE 0 END) AS blocked,",
            "SUM(CASE WHEN ms.last_status = 'error' THEN 1 ELSE 0 END) AS error_rows",
            "FROM provider_listing_catalog catalog",
            "LEFT JOIN (",
            "    SELECT listing_id, MAX(as_of) AS latest_as_of",
            "    FROM market_data",
            "    GROUP BY listing_id",
            ") md ON md.listing_id = catalog.security_id",
            "LEFT JOIN market_data_fetch_state ms "
            "ON ms.provider_listing_id = catalog.provider_listing_id",
            "WHERE catalog.provider = ?",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND catalog.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        if primary_only:
            query.append(f"AND {_primary_listing_predicate()}")
        query.append("GROUP BY catalog.provider_exchange_code")
        query.append("ORDER BY catalog.provider_exchange_code")
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
        *,
        primary_only: bool = False,
    ) -> List[IngestProgressFailure]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        params: List[object] = [provider_norm]
        query = [
            "SELECT catalog.provider_symbol AS symbol, catalog.provider_exchange_code AS exchange_code,",
            "ms.last_status, ms.last_error, ms.next_eligible_at, ms.attempts",
            "FROM provider_listing_catalog catalog",
            "JOIN market_data_fetch_state ms "
            "ON ms.provider_listing_id = catalog.provider_listing_id",
            "WHERE catalog.provider = ? AND ms.last_status = 'error'",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND catalog.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        if primary_only:
            query.append(f"AND {_primary_listing_predicate()}")
        query.append(
            "ORDER BY CASE WHEN ms.next_eligible_at IS NULL THEN 1 ELSE 0 END, "
            "ms.next_eligible_at ASC, catalog.provider_symbol ASC"
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


class FundamentalsRepository(SQLiteStore):
    """Persist raw fundamentals payloads by provider."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fundamentals_raw (
                    payload_id INTEGER PRIMARY KEY,
                    provider_listing_id INTEGER NOT NULL UNIQUE,
                    data TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    FOREIGN KEY (provider_listing_id) REFERENCES provider_listing(provider_listing_id)
                )
                """
            )
            conn.execute("DROP INDEX IF EXISTS idx_fundamentals_raw_security")
            conn.execute("DROP INDEX IF EXISTS idx_fundamentals_raw_provider_symbol")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_fundamentals_raw_provider_fetched
                ON fundamentals_raw(fetched_at)
                """
            )
            _ensure_provider_listing_catalog_views(conn)

    def upsert(
        self,
        provider: str,
        symbol: str,
        payload: Dict[str, Any],
        listing_currency: Optional[str] = None,
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
                    listing_currency=_normalize_optional_text(
                        listing_currency.upper() if listing_currency else None
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
        listing_repo = SecurityListingStatusRepository(self.db_path)
        listing_repo.initialize_schema()
        listing_updates: List[SecurityListingStatusRecord] = []
        with self._connect() as conn:
            rows = []
            ticker_repo = self._supported_ticker_repo()
            for update in updates:
                if not update.provider_symbol:
                    continue
                provider_symbol = update.provider_symbol.strip().upper()
                provider_listing_row = conn.execute(
                    """
                    SELECT provider_listing_id, security_id, provider_exchange_code
                    FROM provider_listing_catalog
                    WHERE provider = ? AND provider_symbol = ?
                    """,
                    (provider_norm, provider_symbol),
                ).fetchone()
                if provider_listing_row is None or update.listing_currency is not None:
                    provider_listing_row = ticker_repo._ensure_provider_listing(
                        conn,
                        provider_norm,
                        provider_symbol,
                        exchange_code=update.provider_exchange_code,
                        currency=update.listing_currency,
                    )
                rows.append(
                    (
                        int(provider_listing_row["provider_listing_id"]),
                        update.data,
                        update.fetched_at,
                    )
                )
            if not rows:
                return
            conn.executemany(
                """
                INSERT INTO fundamentals_raw (
                    provider_listing_id,
                    data,
                    fetched_at
                ) VALUES (?, ?, ?)
                ON CONFLICT(provider_listing_id) DO UPDATE SET
                    data = excluded.data,
                    fetched_at = excluded.fetched_at
                """,
                rows,
            )
            listing_updates = listing_repo.upsert_many_from_fundamentals_updates(
                provider_norm,
                updates,
                connection=conn,
            )
        secondary_updates = [
            update for update in listing_updates if not update.is_primary_listing
        ]
        if secondary_updates:
            listing_repo.purge_secondary_security_data(
                security_ids=[update.security_id for update in secondary_updates],
                provider_symbols=[
                    update.provider_symbol for update in secondary_updates
                ],
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
                SELECT fr.data
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ? AND catalog.provider_symbol = ?
                """,
                (provider.strip().upper(), provider_symbol),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def fetch_many(
        self,
        provider: str,
        symbols: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[str, Dict[str, Any]]:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}
        security_ids_by_symbol = self._security_repo().resolve_ids_many(
            normalized,
            chunk_size=chunk_size,
        )
        results: Dict[str, Dict[str, Any]] = {}
        provider_norm = provider.strip().upper()
        with self._connect() as conn:
            for chunk in _batched(list(security_ids_by_symbol.items()), chunk_size):
                rows = [
                    (symbol, security_id)
                    for symbol, security_id in chunk
                    if security_id
                ]
                if not rows:
                    continue
                placeholders = ", ".join("?" for _ in rows)
                query_params: List[object] = [
                    provider_norm,
                    *[security_id for _, security_id in rows],
                ]
                query_rows = conn.execute(
                    f"""
                    SELECT s.canonical_symbol, fr.data
                    FROM fundamentals_raw fr
                    JOIN provider_listing_catalog catalog
                      ON catalog.provider_listing_id = fr.provider_listing_id
                    JOIN securities s ON s.security_id = catalog.security_id
                    WHERE catalog.provider = ?
                      AND catalog.security_id IN ({placeholders})
                    """,
                    query_params,
                ).fetchall()
                for row in query_rows:
                    results[row["canonical_symbol"]] = json.loads(row["data"])
        return results

    def fetch_metadata_candidates(
        self,
        security_ids: Sequence[int],
        chunk_size: int = 500,
    ) -> Dict[int, SecurityMetadataCandidate]:
        """Extract canonical metadata fields from stored raw fundamentals."""

        self.initialize_schema()
        normalized_ids = sorted(
            {int(security_id) for security_id in security_ids if security_id}
        )
        if not normalized_ids:
            return {}

        results: Dict[int, SecurityMetadataCandidate] = {}
        with self._connect() as conn:
            for chunk in _batched(normalized_ids, chunk_size):
                placeholders = ", ".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT catalog.security_id, catalog.provider, fr.data
                    FROM fundamentals_raw fr
                    JOIN provider_listing_catalog catalog
                      ON catalog.provider_listing_id = fr.provider_listing_id
                    WHERE catalog.security_id IN ({placeholders})
                    ORDER BY catalog.security_id
                    """,
                    list(chunk),
                ).fetchall()
                for row in rows:
                    provider = str(row["provider"]).upper()
                    if provider not in {"EODHD", "SEC"}:
                        continue

                    security_id = int(row["security_id"])
                    payload = json.loads(row["data"])
                    current = results.get(security_id) or SecurityMetadataCandidate()

                    if provider == "EODHD":
                        general = payload.get("General") or {}
                        eodhd_entity_name = _normalize_optional_text(
                            general.get("Name")
                        )
                        results[security_id] = SecurityMetadataCandidate(
                            entity_name=eodhd_entity_name or current.entity_name,
                            description=_normalize_optional_text(
                                general.get("Description")
                            ),
                            sector=_normalize_optional_text(general.get("Sector")),
                            industry=_normalize_optional_text(general.get("Industry")),
                        )
                        continue

                    sec_entity_name = _normalize_optional_text(
                        payload.get("entityName")
                    )
                    if current.entity_name is None and sec_entity_name is not None:
                        results[security_id] = SecurityMetadataCandidate(
                            entity_name=sec_entity_name,
                            description=current.description,
                            sector=current.sector,
                            industry=current.industry,
                        )
        return results

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
                SELECT catalog.provider_symbol, catalog.provider_exchange_code, fr.data
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ? AND catalog.provider_symbol = ?
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
                SELECT fr.data, fr.fetched_at
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ? AND catalog.provider_symbol = ?
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
                SELECT catalog.provider_symbol
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ?
                ORDER BY catalog.provider_symbol
                """,
                (provider.strip().upper(),),
            ).fetchall()
        return [row[0] for row in rows]

    def symbol_exchanges(self, provider: str) -> List[Tuple[str, Optional[str]]]:
        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT catalog.provider_symbol, catalog.provider_exchange_code
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ?
                ORDER BY catalog.provider_symbol
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
        FundamentalsNormalizationStateRepository(self.db_path).initialize_schema()
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
                catalog.provider_symbol,
                catalog.security_id,
                fr.fetched_at,
                ns.raw_fetched_at AS normalized_raw_fetched_at,
                ns.last_normalized_at
            FROM fundamentals_raw fr
            JOIN provider_listing_catalog catalog
              ON catalog.provider_listing_id = fr.provider_listing_id
            LEFT JOIN fundamentals_normalization_state ns
              ON ns.provider_listing_id = fr.provider_listing_id
            WHERE catalog.provider = ?
              AND catalog.provider_symbol IN ({placeholders})
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
                        listing_id AS security_id,
                        MAX(source_provider) AS current_source_provider
                    FROM financial_facts
                    WHERE listing_id IN ({provider_placeholders})
                    GROUP BY listing_id
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
                    catalog.provider_symbol,
                    catalog.security_id,
                    fr.fetched_at,
                    ns.raw_fetched_at AS normalized_raw_fetched_at,
                    ns.last_normalized_at
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                LEFT JOIN fundamentals_normalization_state ns
                  ON ns.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ?
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
        try:
            (
                provider_norm,
                provider_ticker,
                provider_exchange_code,
                provider_symbol,
            ) = _normalize_provider_identity(provider, symbol, exchange)
        except ValueError:
            return None, None, None
        canonical_exchange = self._exchange_provider_repo().resolve_canonical_code(
            provider_norm, provider_exchange_code
        )
        if create:
            security = self._security_repo().ensure(provider_ticker, canonical_exchange)
            return provider_symbol, provider_exchange_code, security.security_id
        existing_security = self._security_repo().fetch_by_symbol(
            f"{provider_ticker}.{canonical_exchange}"
        )
        return (
            provider_symbol,
            provider_exchange_code,
            existing_security.security_id if existing_security else None,
        )


class SecurityListingStatusRepository(SQLiteStore):
    """Persist and reconcile canonical primary-listing classification."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()

    @staticmethod
    def _build_status_record(
        *,
        security_id: int,
        provider_symbol: str,
        raw_fetched_at: str,
        payload: Mapping[str, Any],
    ) -> SecurityListingStatusRecord:
        provider_symbol_norm = _normalize_qualified_symbol(provider_symbol)
        if provider_symbol_norm is None:
            raise ValueError(f"provider_symbol must be qualified: {provider_symbol}")

        general = payload.get("General") if isinstance(payload, Mapping) else None
        primary_provider_symbol = (
            _normalize_qualified_symbol(general.get("PrimaryTicker"))
            if isinstance(general, Mapping)
            else None
        )
        classification_basis: Literal[
            "matched_primary_ticker",
            "different_primary_ticker",
            "missing_primary_ticker",
        ]
        if primary_provider_symbol is None:
            is_primary_listing = True
            classification_basis = "missing_primary_ticker"
        elif primary_provider_symbol == provider_symbol_norm:
            is_primary_listing = True
            classification_basis = "matched_primary_ticker"
        else:
            is_primary_listing = False
            classification_basis = "different_primary_ticker"

        return SecurityListingStatusRecord(
            security_id=int(security_id),
            source_provider=_PRIMARY_LISTING_SOURCE_PROVIDER,
            provider_symbol=provider_symbol_norm,
            raw_fetched_at=raw_fetched_at,
            is_primary_listing=is_primary_listing,
            primary_provider_symbol=primary_provider_symbol,
            classification_basis=classification_basis,
            updated_at=_utc_now_iso(),
        )

    def upsert_many(
        self,
        rows: Sequence[SecurityListingStatusRecord],
        *,
        connection: Optional[sqlite3.Connection] = None,
    ) -> int:
        if connection is None:
            self.initialize_schema()
        if not rows:
            return 0
        payload = [
            (
                _LISTING_STATUS_PRIMARY
                if row.is_primary_listing
                else _LISTING_STATUS_SECONDARY,
                int(row.security_id),
            )
            for row in rows
            if row.provider_symbol and row.security_id
        ]
        if not payload:
            return 0

        sql = """
            UPDATE listing
            SET primary_listing_status = ?
            WHERE listing_id = ?
        """
        if connection is not None:
            connection.executemany(sql, payload)
            return len(payload)
        with self._connect() as conn:
            conn.executemany(sql, payload)
        return len(payload)

    def upsert_many_from_fundamentals_updates(
        self,
        provider: str,
        updates: Sequence[FundamentalsUpdate],
        *,
        connection: Optional[sqlite3.Connection] = None,
    ) -> List[SecurityListingStatusRecord]:
        provider_norm = provider.strip().upper()
        if provider_norm != _PRIMARY_LISTING_SOURCE_PROVIDER:
            return []

        records: List[SecurityListingStatusRecord] = []
        for update in updates:
            if not update.provider_symbol or not update.security_id:
                continue
            try:
                payload = json.loads(update.data)
            except (TypeError, ValueError):
                payload = {}
            records.append(
                self._build_status_record(
                    security_id=update.security_id,
                    provider_symbol=update.provider_symbol,
                    raw_fetched_at=update.fetched_at,
                    payload=payload if isinstance(payload, Mapping) else {},
                )
            )
        self.upsert_many(records, connection=connection)
        return records

    def list_missing_eodhd_provider_symbols(
        self,
        *,
        provider_symbols: Optional[Sequence[str]] = None,
        exchange_codes: Optional[Sequence[str]] = None,
        security_ids: Optional[Sequence[int]] = None,
        chunk_size: int = 500,
    ) -> List[str]:
        """Return supported EODHD symbols with unknown listing status."""

        self.initialize_schema()
        provider_norm = _PRIMARY_LISTING_SOURCE_PROVIDER
        normalized_symbols = _normalized_codes(provider_symbols)
        normalized_exchanges = _normalized_codes(exchange_codes)
        normalized_security_ids = sorted(
            {int(security_id) for security_id in security_ids or () if security_id}
        )

        def _select_rows(
            conn: sqlite3.Connection,
            *,
            symbols_chunk: Optional[Sequence[str]] = None,
            security_chunk: Optional[Sequence[int]] = None,
        ) -> List[sqlite3.Row]:
            params: List[Any] = [provider_norm]
            query = [
                "SELECT st.provider_symbol",
                "FROM supported_tickers st",
                "WHERE st.provider = ?",
                f"AND st.primary_listing_status = '{_LISTING_STATUS_UNKNOWN}'",
            ]
            if normalized_exchanges:
                placeholders = ", ".join("?" for _ in normalized_exchanges)
                query.append(f"AND st.provider_exchange_code IN ({placeholders})")
                params.extend(normalized_exchanges)
            if symbols_chunk:
                placeholders = ", ".join("?" for _ in symbols_chunk)
                query.append(f"AND st.provider_symbol IN ({placeholders})")
                params.extend(symbols_chunk)
            if security_chunk:
                placeholders = ", ".join("?" for _ in security_chunk)
                query.append(f"AND st.security_id IN ({placeholders})")
                params.extend(security_chunk)
            query.append("ORDER BY st.provider_symbol ASC")
            return conn.execute(" ".join(query), params).fetchall()

        missing_provider_symbols: List[str] = []
        with self._connect() as conn:
            if normalized_symbols:
                for symbol_chunk in _batched(normalized_symbols, chunk_size):
                    missing_provider_symbols.extend(
                        str(row["provider_symbol"])
                        for row in _select_rows(conn, symbols_chunk=symbol_chunk)
                    )
            elif normalized_security_ids:
                for security_chunk in _batched(normalized_security_ids, chunk_size):
                    missing_provider_symbols.extend(
                        str(row["provider_symbol"])
                        for row in _select_rows(conn, security_chunk=security_chunk)
                    )
            else:
                missing_provider_symbols.extend(
                    str(row["provider_symbol"]) for row in _select_rows(conn)
                )
        return missing_provider_symbols

    def reconcile_eodhd_fundamentals(
        self,
        *,
        provider_symbols: Optional[Sequence[str]] = None,
        exchange_codes: Optional[Sequence[str]] = None,
        security_ids: Optional[Sequence[int]] = None,
        chunk_size: int = 500,
    ) -> List[SecurityListingStatusRecord]:
        self.initialize_schema()
        provider_norm = _PRIMARY_LISTING_SOURCE_PROVIDER
        normalized_symbols = _normalized_codes(provider_symbols)
        normalized_exchanges = _normalized_codes(exchange_codes)
        normalized_security_ids = sorted(
            {int(security_id) for security_id in security_ids or () if security_id}
        )

        def _select_rows(
            conn: sqlite3.Connection,
            *,
            symbols_chunk: Optional[Sequence[str]] = None,
            security_chunk: Optional[Sequence[int]] = None,
        ) -> List[sqlite3.Row]:
            params: List[Any] = [provider_norm]
            query = [
                "SELECT catalog.security_id, catalog.provider_symbol, fr.fetched_at, fr.data",
                "FROM fundamentals_raw fr",
                "JOIN provider_listing_catalog catalog",
                "  ON catalog.provider_listing_id = fr.provider_listing_id",
                "WHERE catalog.provider = ?",
            ]
            if normalized_exchanges:
                placeholders = ", ".join("?" for _ in normalized_exchanges)
                query.append(f"AND catalog.provider_exchange_code IN ({placeholders})")
                params.extend(normalized_exchanges)
            if symbols_chunk:
                placeholders = ", ".join("?" for _ in symbols_chunk)
                query.append(f"AND catalog.provider_symbol IN ({placeholders})")
                params.extend(symbols_chunk)
            if security_chunk:
                placeholders = ", ".join("?" for _ in security_chunk)
                query.append(f"AND catalog.security_id IN ({placeholders})")
                params.extend(security_chunk)
            query.append("ORDER BY catalog.provider_symbol ASC")
            return conn.execute(" ".join(query), params).fetchall()

        fetched_rows: List[sqlite3.Row] = []
        with self._connect() as conn:
            if normalized_symbols:
                for symbol_chunk in _batched(normalized_symbols, chunk_size):
                    fetched_rows.extend(_select_rows(conn, symbols_chunk=symbol_chunk))
            elif normalized_security_ids:
                for security_chunk in _batched(normalized_security_ids, chunk_size):
                    fetched_rows.extend(
                        _select_rows(conn, security_chunk=security_chunk)
                    )
            else:
                fetched_rows.extend(_select_rows(conn))

        records: List[SecurityListingStatusRecord] = []
        for row in fetched_rows:
            try:
                payload = json.loads(row["data"])
            except (TypeError, ValueError):
                payload = {}
            records.append(
                self._build_status_record(
                    security_id=int(row["security_id"]),
                    provider_symbol=str(row["provider_symbol"]),
                    raw_fetched_at=str(row["fetched_at"]),
                    payload=payload if isinstance(payload, Mapping) else {},
                )
            )
        self.upsert_many(records)
        return records

    def purge_secondary_security_data(
        self,
        *,
        security_ids: Sequence[int],
        provider_symbols: Sequence[str],
    ) -> None:
        normalized_security_ids = sorted(
            {int(security_id) for security_id in security_ids if security_id}
        )
        normalized_symbols = _normalized_codes(provider_symbols)
        if not normalized_security_ids and not normalized_symbols:
            return

        provider_norm = _PRIMARY_LISTING_SOURCE_PROVIDER
        FinancialFactsRepository(self.db_path).initialize_schema()
        FinancialFactsRefreshStateRepository(self.db_path).initialize_schema()
        MarketDataRepository(self.db_path).initialize_schema()
        MetricsRepository(self.db_path).initialize_schema()
        MetricComputeStatusRepository(self.db_path).initialize_schema()
        FundamentalsNormalizationStateRepository(self.db_path).initialize_schema()
        MarketDataFetchStateRepository(self.db_path).initialize_schema()

        def _delete_security_rows(
            conn: sqlite3.Connection,
            table_name: str,
        ) -> None:
            for security_chunk in _batched(normalized_security_ids, 500):
                placeholders = ", ".join("?" for _ in security_chunk)
                conn.execute(
                    f"DELETE FROM {table_name} WHERE listing_id IN ({placeholders})",
                    list(security_chunk),
                )

        with self._connect() as conn:
            if normalized_security_ids:
                for table_name in (
                    "financial_facts",
                    "financial_facts_refresh_state",
                    "market_data",
                    "metrics",
                    "metric_compute_status",
                ):
                    _delete_security_rows(conn, table_name)
            if normalized_symbols:
                for symbol_chunk in _batched(normalized_symbols, 500):
                    placeholders = ", ".join("?" for _ in symbol_chunk)
                    conn.execute(
                        f"""
                        DELETE FROM fundamentals_normalization_state
                        WHERE provider = ? AND provider_symbol IN ({placeholders})
                        """,
                        [provider_norm, *symbol_chunk],
                    )
                    conn.execute(
                        f"""
                        DELETE FROM market_data_fetch_state
                        WHERE provider = ? AND provider_symbol IN ({placeholders})
                        """,
                        [provider_norm, *symbol_chunk],
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
                    provider_listing_id INTEGER NOT NULL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    provider_symbol TEXT NOT NULL,
                    last_fetched_at TEXT,
                    last_status TEXT,
                    last_error TEXT,
                    next_eligible_at TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    UNIQUE (provider, provider_symbol)
                )
                """
            )
            columns = _table_columns(conn, self.table_name)
            alter_statements = [
                (
                    "provider_listing_id",
                    f"ALTER TABLE {self.table_name} ADD COLUMN provider_listing_id INTEGER",
                ),
                ("provider", f"ALTER TABLE {self.table_name} ADD COLUMN provider TEXT"),
                (
                    "provider_symbol",
                    f"ALTER TABLE {self.table_name} ADD COLUMN provider_symbol TEXT",
                ),
            ]
            for column_name, statement in alter_statements:
                if column_name not in columns:
                    conn.execute(statement)
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self.index_name}
                ON {self.table_name}(provider, next_eligible_at)
                """
            )
            conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_{self.table_name}_provider_symbol
                ON {self.table_name}(provider, provider_symbol)
                """
            )
            _ensure_provider_listing_catalog_views(conn)
            conn.execute(
                f"""
                UPDATE {self.table_name}
                SET provider = (
                        SELECT catalog.provider
                        FROM provider_listing_catalog catalog
                        WHERE catalog.provider_listing_id = {self.table_name}.provider_listing_id
                    ),
                    provider_symbol = (
                        SELECT catalog.provider_symbol
                        FROM provider_listing_catalog catalog
                        WHERE catalog.provider_listing_id = {self.table_name}.provider_listing_id
                    )
                WHERE provider_listing_id IS NOT NULL
                  AND (provider IS NULL OR provider_symbol IS NULL)
                """
            )

    def _resolve_provider_listing_id(
        self,
        conn: sqlite3.Connection,
        provider: str,
        symbol: str,
    ) -> Optional[int]:
        row = conn.execute(
            """
            SELECT provider_listing_id
            FROM provider_listing_catalog
            WHERE provider = ? AND provider_symbol = ?
            """,
            (provider.strip().upper(), symbol.strip().upper()),
        ).fetchone()
        return int(row["provider_listing_id"]) if row else None

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
            provider_listing_id = self._resolve_provider_listing_id(
                conn, provider, symbol
            )
            if provider_listing_id is None:
                return
            conn.execute(
                f"""
                INSERT INTO {self.table_name} (
                    provider_listing_id,
                    provider,
                    provider_symbol,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, ?, 'ok', NULL, NULL, 0)
                ON CONFLICT(provider, provider_symbol) DO UPDATE SET
                    provider_listing_id = excluded.provider_listing_id,
                    last_fetched_at = excluded.last_fetched_at,
                    last_status = 'ok',
                    last_error = NULL,
                    next_eligible_at = NULL,
                    attempts = 0
                """,
                (
                    provider_listing_id,
                    provider.strip().upper(),
                    symbol.strip().upper(),
                    timestamp,
                ),
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
        with self._connect() as conn:
            rows = []
            for symbol in normalized:
                provider_listing_id = self._resolve_provider_listing_id(
                    conn, provider_norm, symbol
                )
                if provider_listing_id is None:
                    continue
                rows.append((provider_listing_id, provider_norm, symbol, timestamp))
            conn.executemany(
                f"""
                INSERT INTO {self.table_name} (
                    provider_listing_id,
                    provider,
                    provider_symbol,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, ?, 'ok', NULL, NULL, 0)
                ON CONFLICT(provider, provider_symbol) DO UPDATE SET
                    provider_listing_id = excluded.provider_listing_id,
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
            provider_listing_id = self._resolve_provider_listing_id(
                conn, provider, symbol
            )
            if provider_listing_id is None:
                return
            conn.execute(
                f"""
                INSERT INTO {self.table_name} (
                    provider_listing_id,
                    provider,
                    provider_symbol,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, ?, 'error', ?, ?, ?)
                ON CONFLICT(provider, provider_symbol) DO UPDATE SET
                    provider_listing_id = excluded.provider_listing_id,
                    last_fetched_at = COALESCE(excluded.last_fetched_at, {self.table_name}.last_fetched_at),
                    last_status = 'error',
                    last_error = excluded.last_error,
                    next_eligible_at = excluded.next_eligible_at,
                    attempts = excluded.attempts
                """,
                (
                    provider_listing_id,
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
                provider_listing_id = self._resolve_provider_listing_id(
                    conn, provider_norm, symbol
                )
                if provider_listing_id is None:
                    continue
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
                        provider_listing_id,
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
                    provider_listing_id,
                    provider,
                    provider_symbol,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, ?, 'error', ?, ?, ?)
                ON CONFLICT(provider, provider_symbol) DO UPDATE SET
                    provider_listing_id = excluded.provider_listing_id,
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
                    provider_listing_id INTEGER NOT NULL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    provider_symbol TEXT NOT NULL,
                    security_id INTEGER NOT NULL,
                    listing_id INTEGER NOT NULL,
                    raw_fetched_at TEXT NOT NULL,
                    last_normalized_at TEXT NOT NULL,
                    UNIQUE (provider, provider_symbol)
                )
                """
            )
            columns = _table_columns(conn, "fundamentals_normalization_state")
            alter_statements = [
                (
                    "provider_listing_id",
                    """
                    ALTER TABLE fundamentals_normalization_state
                    ADD COLUMN provider_listing_id INTEGER
                    """,
                ),
                (
                    "provider",
                    """
                    ALTER TABLE fundamentals_normalization_state
                    ADD COLUMN provider TEXT
                    """,
                ),
                (
                    "provider_symbol",
                    """
                    ALTER TABLE fundamentals_normalization_state
                    ADD COLUMN provider_symbol TEXT
                    """,
                ),
                (
                    "security_id",
                    """
                    ALTER TABLE fundamentals_normalization_state
                    ADD COLUMN security_id INTEGER
                    """,
                ),
                (
                    "listing_id",
                    """
                    ALTER TABLE fundamentals_normalization_state
                    ADD COLUMN listing_id INTEGER
                    """,
                ),
            ]
            for column_name, statement in alter_statements:
                if column_name not in columns:
                    conn.execute(statement)
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_fundamentals_norm_state_security
                ON fundamentals_normalization_state(security_id)
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_fundamentals_norm_state_provider_symbol
                ON fundamentals_normalization_state(provider, provider_symbol)
                """
            )
            _ensure_provider_listing_catalog_views(conn)
            conn.execute(
                """
                UPDATE fundamentals_normalization_state
                SET provider = (
                        SELECT catalog.provider
                        FROM provider_listing_catalog catalog
                        WHERE catalog.provider_listing_id = fundamentals_normalization_state.provider_listing_id
                    ),
                    provider_symbol = (
                        SELECT catalog.provider_symbol
                        FROM provider_listing_catalog catalog
                        WHERE catalog.provider_listing_id = fundamentals_normalization_state.provider_listing_id
                    ),
                    listing_id = COALESCE(listing_id, security_id),
                    security_id = COALESCE(security_id, listing_id)
                WHERE provider_listing_id IS NOT NULL
                  AND (
                      provider IS NULL
                      OR provider_symbol IS NULL
                      OR security_id IS NULL
                      OR listing_id IS NULL
                  )
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
            provider_listing_row = conn.execute(
                """
                SELECT provider_listing_id
                FROM provider_listing_catalog
                WHERE provider = ? AND provider_symbol = ?
                """,
                (provider.strip().upper(), symbol.strip().upper()),
            ).fetchone()
            if provider_listing_row is None:
                return
            conn.execute(
                """
                INSERT INTO fundamentals_normalization_state (
                    provider_listing_id,
                    provider,
                    provider_symbol,
                    security_id,
                    listing_id,
                    raw_fetched_at,
                    last_normalized_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, provider_symbol) DO UPDATE SET
                    provider_listing_id = excluded.provider_listing_id,
                    security_id = excluded.security_id,
                    listing_id = excluded.listing_id,
                    raw_fetched_at = excluded.raw_fetched_at,
                    last_normalized_at = excluded.last_normalized_at
                """,
                (
                    int(provider_listing_row["provider_listing_id"]),
                    provider.strip().upper(),
                    symbol.strip().upper(),
                    int(security_id),
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


class FinancialFactsRefreshStateRepository(SQLiteStore):
    """Track the latest normalized financial-facts refresh per security."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS financial_facts_refresh_state (
                    listing_id INTEGER NOT NULL PRIMARY KEY,
                    refreshed_at TEXT NOT NULL
                )
                """
            )

    def mark_security_refreshed(
        self,
        security_id: int,
        refreshed_at: Optional[str] = None,
        *,
        connection: Optional[sqlite3.Connection] = None,
    ) -> None:
        timestamp = refreshed_at or _utc_now_iso()
        sql = """
            INSERT INTO financial_facts_refresh_state (
                listing_id,
                refreshed_at
            ) VALUES (?, ?)
            ON CONFLICT(listing_id) DO UPDATE SET
                refreshed_at = excluded.refreshed_at
        """
        if connection is not None:
            connection.execute(sql, (int(security_id), timestamp))
            return
        self.initialize_schema()
        with self._connect() as conn:
            conn.execute(sql, (int(security_id), timestamp))

    def fetch(self, symbol: str) -> Optional[FinancialFactsRefreshStateRecord]:
        self.initialize_schema()
        security_id = self._security_repo().resolve_id(symbol)
        if security_id is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT refreshed_at
                FROM financial_facts_refresh_state
                WHERE listing_id = ?
                """,
                (security_id,),
            ).fetchone()
        if row is None:
            return None
        return FinancialFactsRefreshStateRecord(
            symbol=symbol.strip().upper(),
            refreshed_at=row["refreshed_at"],
        )

    def fetch_many_for_symbols(
        self,
        symbols: Sequence[str],
        chunk_size: int = 500,
        *,
        security_ids_by_symbol: Optional[Mapping[str, int]] = None,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, FinancialFactsRefreshStateRecord]:
        self.initialize_schema()
        normalized_symbols = _normalized_codes(symbols)
        if not normalized_symbols:
            return {}

        resolved_security_ids = (
            dict(security_ids_by_symbol)
            if security_ids_by_symbol is not None
            else self._security_repo().resolve_ids_many(
                normalized_symbols,
                chunk_size=chunk_size,
                connection=connection,
            )
        )
        if not resolved_security_ids:
            return {}

        symbol_by_security_id = {
            security_id: symbol for symbol, security_id in resolved_security_ids.items()
        }
        rows_by_symbol: Dict[str, FinancialFactsRefreshStateRecord] = {}
        security_ids = sorted(symbol_by_security_id.keys())

        def _query(conn: sqlite3.Connection) -> None:
            for security_chunk in _batched(security_ids, chunk_size):
                placeholders = ", ".join("?" for _ in security_chunk)
                rows = conn.execute(
                    f"""
                    SELECT listing_id, refreshed_at
                    FROM financial_facts_refresh_state
                    WHERE listing_id IN ({placeholders})
                    """,
                    list(security_chunk),
                ).fetchall()
                for row in rows:
                    symbol = symbol_by_security_id[row["listing_id"]]
                    rows_by_symbol[symbol] = FinancialFactsRefreshStateRecord(
                        symbol=symbol,
                        refreshed_at=row["refreshed_at"],
                    )

        if connection is not None:
            _query(connection)
        else:
            with self._connect() as conn:
                _query(conn)
        return rows_by_symbol


class FinancialFactsRepository(SQLiteStore):
    """Persist normalized financial facts for downstream metrics."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()
        FinancialFactsRefreshStateRepository(self.db_path).initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS financial_facts (
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
                CREATE INDEX IF NOT EXISTS idx_fin_facts_concept
                ON financial_facts(concept)
                """
            )
            # ``idx_fin_facts_security_concept_latest`` is the canonical index
            # for the compute-metrics fact preload (storage.facts_for_symbols_many
            # pins it via INDEXED BY). Migration 029 creates it on existing
            # databases that already hold ``financial_facts``; this CREATE acts
            # as a backstop for fresh databases where migrations ran before
            # this table existed. Both call sites use IF NOT EXISTS so the
            # index is created exactly once. We swallow "database is locked"
            # because parallel metric workers can race here on a fresh DB; a
            # later initialize_schema() call will succeed once the lock clears.
            try:
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_fin_facts_security_concept_latest
                    ON financial_facts(listing_id, concept, end_date DESC, filed DESC)
                    """
                )
            except sqlite3.OperationalError as exc:
                if "database is locked" not in str(exc).lower():
                    raise
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_fin_facts_currency_nonnull
                ON financial_facts(currency)
                WHERE currency IS NOT NULL
                """
            )

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
                "DELETE FROM financial_facts WHERE listing_id = ?",
                (security.security_id,),
            )
            if prepared_rows:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO financial_facts (
                        listing_id, cik, concept, fiscal_period, end_date, unit,
                        value, accn, filed, frame, start_date, accounting_standard,
                        currency, source_provider
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    prepared_rows,
                )
            FinancialFactsRefreshStateRepository(self.db_path).mark_security_refreshed(
                security.security_id,
                connection=conn,
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
                JOIN securities s ON s.security_id = ff.listing_id
                WHERE ff.listing_id = ? AND ff.concept = ?
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
            "JOIN securities s ON s.security_id = ff.listing_id",
            "WHERE ff.listing_id = ? AND ff.concept = ?",
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
                JOIN securities s ON s.security_id = ff.listing_id
                WHERE ff.listing_id = ?
                ORDER BY ff.concept, ff.end_date DESC, ff.filed DESC
                """,
                (security_id,),
            ).fetchall()
        return [FactRecord(*row) for row in rows]

    def facts_for_symbols_many(
        self,
        symbols: Sequence[str],
        chunk_size: int = 25,
        *,
        concepts: Optional[Sequence[str]] = None,
        security_ids_by_symbol: Optional[Mapping[str, int]] = None,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, List[FactRecord]]:
        """Return all stored facts for many symbols grouped by canonical symbol.

        The query pattern matches ``compute-metrics`` where loading facts one
        symbol at a time becomes dominated by SQLite round trips on large
        universes. Each chunk performs indexed reads over a bounded set of
        ``security_id`` values and preserves the same per-symbol ordering as
        ``facts_for_symbol()``.

        When ``concepts`` is provided and non-empty the result is restricted
        to facts whose ``concept`` value is in the supplied set. The composite
        ``(security_id, concept, end_date DESC, filed DESC)`` index converts
        each ``(security_id, concept)`` pair into a direct seek, which is
        substantially cheaper than a security-scoped scan when the requested
        metric set only touches a subset of the stored concepts.
        """

        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}

        resolved_security_ids = (
            dict(security_ids_by_symbol)
            if security_ids_by_symbol is not None
            else self._security_repo().resolve_ids_many(
                normalized,
                chunk_size=chunk_size,
                connection=connection,
            )
        )
        resolved_symbols = [
            symbol for symbol in normalized if symbol in resolved_security_ids
        ]
        if not resolved_symbols:
            return {}

        # Deduplicate while preserving call-site ordering for stable SQL plans.
        concept_filter: Tuple[str, ...] = ()
        if concepts:
            seen_concepts: set[str] = set()
            ordered_concepts: List[str] = []
            for concept in concepts:
                if concept and concept not in seen_concepts:
                    seen_concepts.add(concept)
                    ordered_concepts.append(concept)
            concept_filter = tuple(ordered_concepts)

        grouped: Dict[str, List[FactRecord]] = {
            symbol: [] for symbol in resolved_symbols
        }

        def _query(conn: sqlite3.Connection) -> None:
            for chunk in _batched(resolved_symbols, chunk_size):
                symbol_by_security_id = {
                    resolved_security_ids[symbol]: symbol for symbol in chunk
                }
                placeholders = ", ".join("?" for _ in symbol_by_security_id)
                params: List[Any] = list(symbol_by_security_id)
                concept_clause = ""
                if concept_filter:
                    concept_placeholders = ", ".join("?" for _ in concept_filter)
                    concept_clause = f" AND ff.concept IN ({concept_placeholders})"
                    params.extend(concept_filter)
                cursor = conn.execute(
                    f"""
                    SELECT
                        ff.listing_id,
                        ff.cik,
                        ff.concept,
                        ff.fiscal_period,
                        ff.end_date,
                        ff.unit,
                        ff.value,
                        ff.accn,
                        ff.filed,
                        ff.frame,
                        ff.start_date,
                        ff.accounting_standard,
                        ff.currency
                    FROM financial_facts ff INDEXED BY idx_fin_facts_security_concept_latest
                    WHERE ff.listing_id IN ({placeholders}){concept_clause}
                    ORDER BY ff.listing_id, ff.concept, ff.end_date DESC, ff.filed DESC
                    """,
                    params,
                )
                for row in cursor:
                    symbol = symbol_by_security_id[row["listing_id"]]
                    grouped[symbol].append(
                        FactRecord(
                            symbol=symbol,
                            cik=row["cik"],
                            concept=row["concept"],
                            fiscal_period=row["fiscal_period"],
                            end_date=row["end_date"],
                            unit=row["unit"],
                            value=row["value"],
                            accn=row["accn"],
                            filed=row["filed"],
                            frame=row["frame"],
                            start_date=row["start_date"],
                            accounting_standard=row["accounting_standard"],
                            currency=row["currency"],
                        )
                    )

        if connection is not None:
            _query(connection)
        else:
            with self._connect() as conn:
                _query(conn)
        return grouped

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
                listing_id INTEGER PRIMARY KEY,
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
                INSERT INTO temp_selected_securities (listing_id, canonical_symbol)
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
                    WHERE ff.listing_id = selected.listing_id
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
                (
                    SELECT ff.value
                    FROM financial_facts ff INDEXED BY idx_fin_facts_security_concept_latest
                    WHERE ff.listing_id = selected.listing_id
                      AND ff.concept IN (?, ?)
                    ORDER BY ff.end_date DESC,
                             CASE ff.concept
                                 WHEN 'CommonStockSharesOutstanding' THEN 0
                                 ELSE 1
                             END,
                             CASE ff.unit
                                 WHEN ? THEN 0
                                 ELSE 1
                             END,
                             CASE
                                 WHEN ff.currency IS NULL THEN 0
                                 ELSE 1
                             END,
                             CASE UPPER(COALESCE(ff.fiscal_period, ''))
                                 WHEN 'Q4' THEN 0
                                 WHEN 'Q3' THEN 1
                                 WHEN 'Q2' THEN 2
                                 WHEN 'Q1' THEN 3
                                 WHEN 'FY' THEN 4
                                 ELSE 5
                             END,
                             ABS(ff.value) ASC,
                             ff.filed DESC
                    LIMIT 1
                ) AS value
            FROM temp_selected_securities selected
            """,
            (primary_concept, fallback_concept, SHARES_UNIT),
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


class FXRatesRepository(SQLiteStore):
    """Persist and query direct FX rate observations."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
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
            _ensure_provider_listing_catalog_views(conn)

    def upsert(self, record: FXRateRecord) -> None:
        self.upsert_many([record])

    def upsert_many(self, records: Sequence[FXRateRecord]) -> int:
        self.initialize_schema()
        if not records:
            return 0
        now = _utc_now_iso()
        payload = [
            (
                record.provider.strip().upper(),
                record.rate_date,
                normalize_currency_code(record.base_currency),
                normalize_currency_code(record.quote_currency),
                str(record.rate_text),
                record.fetched_at,
                record.source_kind.strip().lower(),
                record.meta_json,
                record.created_at or now,
                record.updated_at or now,
            )
            for record in records
            if normalize_currency_code(record.base_currency)
            and normalize_currency_code(record.quote_currency)
        ]
        if not payload:
            return 0
        with self._connect() as conn:
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
                    meta_json,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, rate_date, base_currency, quote_currency)
                DO UPDATE SET
                    rate_text = excluded.rate_text,
                    fetched_at = excluded.fetched_at,
                    source_kind = excluded.source_kind,
                    meta_json = excluded.meta_json,
                    updated_at = excluded.updated_at
                """,
                payload,
            )
        return len(payload)

    def latest_on_or_before(
        self,
        provider: str,
        base_currency: str,
        quote_currency: str,
        as_of: str,
    ) -> Optional[FXRateRecord]:
        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    provider,
                    rate_date,
                    base_currency,
                    quote_currency,
                    rate_text,
                    fetched_at,
                    source_kind,
                    meta_json,
                    created_at,
                    updated_at
                FROM fx_rates
                WHERE provider = ?
                  AND base_currency = ?
                  AND quote_currency = ?
                  AND rate_date <= ?
                ORDER BY rate_date DESC
                LIMIT 1
                """,
                (
                    provider.strip().upper(),
                    normalize_currency_code(base_currency),
                    normalize_currency_code(quote_currency),
                    as_of,
                ),
            ).fetchone()
        if row is None:
            return None
        return FXRateRecord(*row)

    def fetch_pair_history(
        self,
        provider: str,
        base_currency: str,
        quote_currency: str,
    ) -> list[tuple[str, str]]:
        """Return one direct pair history ordered by ascending rate date."""

        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT rate_date, rate_text
                FROM fx_rates
                WHERE provider = ?
                  AND base_currency = ?
                  AND quote_currency = ?
                ORDER BY rate_date ASC
                """,
                (
                    provider.strip().upper(),
                    normalize_currency_code(base_currency),
                    normalize_currency_code(quote_currency),
                ),
            ).fetchall()
        return [(str(row["rate_date"]), str(row["rate_text"])) for row in rows]

    def fetch_all_for_provider(
        self,
        provider: str,
    ) -> list[tuple[str, str, str, str]]:
        """Return the full direct-rate history for one provider."""

        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT base_currency, quote_currency, rate_date, rate_text
                FROM fx_rates
                WHERE provider = ?
                ORDER BY base_currency ASC, quote_currency ASC, rate_date ASC
                """,
                (provider.strip().upper(),),
            ).fetchall()
        return [
            (
                str(row["base_currency"]),
                str(row["quote_currency"]),
                str(row["rate_date"]),
                str(row["rate_text"]),
            )
            for row in rows
        ]

    def pair_coverage(
        self,
        provider: str,
        base_currency: str,
        quote_currency: str,
    ) -> tuple[Optional[str], Optional[str]]:
        """Return min/max stored direct dates for one pair."""

        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MIN(rate_date) AS min_rate_date, MAX(rate_date) AS max_rate_date
                FROM fx_rates
                WHERE provider = ?
                  AND base_currency = ?
                  AND quote_currency = ?
                """,
                (
                    provider.strip().upper(),
                    normalize_currency_code(base_currency),
                    normalize_currency_code(quote_currency),
                ),
            ).fetchone()
        if row is None:
            return None, None
        return (
            _normalize_optional_text(row["min_rate_date"]),
            _normalize_optional_text(row["max_rate_date"]),
        )

    def fully_covered_quotes_for_window(
        self,
        provider: str,
        base_currency: str,
        quote_currencies: Sequence[str],
        start_date: date,
        end_date: date,
    ) -> set[str]:
        """Return quotes whose direct rows fully cover one inclusive date window.

        The refresh command only skips a base/quote window when the stored rows
        cover every day in that exact requested window. Sparse historical rows
        must not be treated as continuous coverage just because their min/max
        dates span the window.
        """

        self.initialize_schema()
        normalized_quotes = [
            code
            for code in (
                normalize_currency_code(quote_currency)
                for quote_currency in quote_currencies
            )
            if code is not None
        ]
        if not normalized_quotes:
            return set()
        expected_days = (end_date - start_date).days + 1
        if expected_days <= 0:
            return set()
        placeholders = ", ".join("?" for _ in normalized_quotes)
        params = [
            provider.strip().upper(),
            normalize_currency_code(base_currency),
            start_date.isoformat(),
            end_date.isoformat(),
            *normalized_quotes,
        ]
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    quote_currency,
                    COUNT(*) AS row_count,
                    MIN(rate_date) AS min_rate_date,
                    MAX(rate_date) AS max_rate_date
                FROM fx_rates
                WHERE provider = ?
                  AND base_currency = ?
                  AND rate_date >= ?
                  AND rate_date <= ?
                  AND quote_currency IN ({placeholders})
                GROUP BY quote_currency
                """,
                params,
            ).fetchall()
        return {
            str(row["quote_currency"])
            for row in rows
            if row["min_rate_date"] == start_date.isoformat()
            and row["max_rate_date"] == end_date.isoformat()
            and int(row["row_count"]) == expected_days
        }

    def discover_currencies(self) -> List[str]:
        """Return distinct normalized currencies referenced by the project DB."""

        self.initialize_schema()
        currencies: set[str] = set()
        with self._connect() as conn:
            supported_rows = conn.execute(
                f"""
                SELECT DISTINCT st.currency
                FROM supported_tickers st
                WHERE st.currency IS NOT NULL
                  AND {_primary_listing_predicate("st")}
                ORDER BY st.currency
                """
            ).fetchall()
            for row in supported_rows:
                code = normalize_currency_code(row["currency"])
                if code is not None:
                    currencies.add(code)
            for table_name in ("financial_facts", "market_data"):
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT currency
                    FROM {table_name}
                    WHERE currency IS NOT NULL
                    ORDER BY currency
                    """
                ).fetchall()
                for row in rows:
                    code = normalize_currency_code(row["currency"])
                    if code is not None:
                        currencies.add(code)
        return sorted(currencies)


class FXSupportedPairsRepository(SQLiteStore):
    """Persist FX provider catalog entries."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
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

    def replace_provider_catalog(
        self,
        provider: str,
        records: Sequence[FXSupportedPairRecord],
    ) -> int:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        now = _utc_now_iso()
        rows = [
            (
                provider_norm,
                record.symbol.strip().upper(),
                record.canonical_symbol.strip().upper(),
                normalize_currency_code(record.base_currency),
                normalize_currency_code(record.quote_currency),
                _normalize_optional_text(record.name),
                1 if record.is_alias else 0,
                1 if record.is_refreshable else 0,
                record.last_seen_at or now,
            )
            for record in records
            if record.symbol and record.canonical_symbol
        ]
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM fx_supported_pairs WHERE provider = ?",
                (provider_norm,),
            )
            if rows:
                conn.executemany(
                    """
                    INSERT INTO fx_supported_pairs (
                        provider,
                        symbol,
                        canonical_symbol,
                        base_currency,
                        quote_currency,
                        name,
                        is_alias,
                        is_refreshable,
                        last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
        return len(rows)

    def list_refreshable(self, provider: str) -> list[FXSupportedPairRecord]:
        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    provider,
                    symbol,
                    canonical_symbol,
                    base_currency,
                    quote_currency,
                    name,
                    is_alias,
                    is_refreshable,
                    last_seen_at
                FROM fx_supported_pairs
                WHERE provider = ?
                  AND is_refreshable = 1
                ORDER BY canonical_symbol ASC
                """,
                (provider.strip().upper(),),
            ).fetchall()
        return [
            FXSupportedPairRecord(
                provider=str(row["provider"]),
                symbol=str(row["symbol"]),
                canonical_symbol=str(row["canonical_symbol"]),
                base_currency=_normalize_optional_text(row["base_currency"]),
                quote_currency=_normalize_optional_text(row["quote_currency"]),
                name=_normalize_optional_text(row["name"]),
                is_alias=bool(row["is_alias"]),
                is_refreshable=bool(row["is_refreshable"]),
                last_seen_at=_normalize_optional_text(row["last_seen_at"]),
            )
            for row in rows
        ]


class FXRefreshStateRepository(SQLiteStore):
    """Persist FX refresh coverage and retry state per canonical symbol."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        with self._connect() as conn:
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

    def fetch(
        self,
        provider: str,
        canonical_symbol: str,
    ) -> Optional[FXRefreshStateRecord]:
        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    provider,
                    canonical_symbol,
                    min_rate_date,
                    max_rate_date,
                    full_history_backfilled,
                    last_fetched_at,
                    last_status,
                    last_error,
                    attempts
                FROM fx_refresh_state
                WHERE provider = ? AND canonical_symbol = ?
                """,
                (provider.strip().upper(), canonical_symbol.strip().upper()),
            ).fetchone()
        if row is None:
            return None
        return FXRefreshStateRecord(
            provider=str(row["provider"]),
            canonical_symbol=str(row["canonical_symbol"]),
            min_rate_date=_normalize_optional_text(row["min_rate_date"]),
            max_rate_date=_normalize_optional_text(row["max_rate_date"]),
            full_history_backfilled=bool(row["full_history_backfilled"]),
            last_fetched_at=_normalize_optional_text(row["last_fetched_at"]),
            last_status=_normalize_optional_text(row["last_status"]),
            last_error=_normalize_optional_text(row["last_error"]),
            attempts=int(row["attempts"] or 0),
        )

    def mark_success(
        self,
        provider: str,
        canonical_symbol: str,
        *,
        min_rate_date: Optional[str],
        max_rate_date: Optional[str],
        full_history_backfilled: bool,
        fetched_at: Optional[str] = None,
    ) -> None:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        symbol_norm = canonical_symbol.strip().upper()
        timestamp = fetched_at or _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fx_refresh_state (
                    provider,
                    canonical_symbol,
                    min_rate_date,
                    max_rate_date,
                    full_history_backfilled,
                    last_fetched_at,
                    last_status,
                    last_error,
                    attempts
                ) VALUES (?, ?, ?, ?, ?, ?, 'ok', NULL, 0)
                ON CONFLICT(provider, canonical_symbol) DO UPDATE SET
                    min_rate_date = excluded.min_rate_date,
                    max_rate_date = excluded.max_rate_date,
                    full_history_backfilled = excluded.full_history_backfilled,
                    last_fetched_at = excluded.last_fetched_at,
                    last_status = 'ok',
                    last_error = NULL,
                    attempts = 0
                """,
                (
                    provider_norm,
                    symbol_norm,
                    min_rate_date,
                    max_rate_date,
                    1 if full_history_backfilled else 0,
                    timestamp,
                ),
            )

    def mark_failure(
        self,
        provider: str,
        canonical_symbol: str,
        error: str,
    ) -> None:
        self.initialize_schema()
        state = self.fetch(provider, canonical_symbol)
        attempts = 1 if state is None else state.attempts + 1
        provider_norm = provider.strip().upper()
        symbol_norm = canonical_symbol.strip().upper()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fx_refresh_state (
                    provider,
                    canonical_symbol,
                    min_rate_date,
                    max_rate_date,
                    full_history_backfilled,
                    last_fetched_at,
                    last_status,
                    last_error,
                    attempts
                ) VALUES (?, ?, NULL, NULL, 0, ?, 'error', ?, ?)
                ON CONFLICT(provider, canonical_symbol) DO UPDATE SET
                    last_fetched_at = excluded.last_fetched_at,
                    last_status = 'error',
                    last_error = excluded.last_error,
                    attempts = excluded.attempts
                """,
                (
                    provider_norm,
                    symbol_norm,
                    _utc_now_iso(),
                    str(error),
                    attempts,
                ),
            )


class MetricsRepository(SQLiteStore):
    """Persist computed metric values."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics (
                    listing_id INTEGER NOT NULL,
                    metric_id TEXT NOT NULL,
                    value REAL NOT NULL,
                    as_of TEXT NOT NULL,
                    unit_kind TEXT NOT NULL DEFAULT 'other',
                    currency TEXT,
                    unit_label TEXT,
                    PRIMARY KEY (listing_id, metric_id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_metrics_metric_id
                ON metrics(metric_id)
                """
            )

    def upsert(
        self,
        symbol: str,
        metric_id: str,
        value: float,
        as_of: str,
        unit_kind: MetricUnitKind = "other",
        currency: Optional[str] = None,
        unit_label: Optional[str] = None,
    ) -> None:
        self.upsert_many(
            [(symbol, metric_id, value, as_of, unit_kind, currency, unit_label)]
        )

    def upsert_many(
        self,
        rows: Iterable[StoredMetricRow],
        *,
        connection: Optional[sqlite3.Connection] = None,
        commit: bool = True,
    ) -> int:
        self.initialize_schema()
        metric_rows: List[StoredMetricRow] = []
        for row in rows:
            if len(row) == 4:
                symbol, metric_id, value, as_of = row
                metric_rows.append(
                    (
                        symbol,
                        metric_id,
                        value,
                        as_of,
                        "other",
                        None,
                        None,
                    )
                )
                continue
            metric_rows.append(row)
        if not metric_rows:
            return 0

        unique_symbols = []
        seen_symbols = set()
        for symbol, _, _, _, _, _, _ in metric_rows:
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            unique_symbols.append(symbol)

        security_ids = self._security_repo().resolve_ids_many(
            unique_symbols,
            connection=connection,
        )
        for symbol in unique_symbols:
            if symbol in security_ids:
                continue
            # Slow path: a metric row references a symbol that does not yet
            # exist in the canonical listing table. When the caller supplied a
            # write connection, create the row through that same transaction to
            # avoid self-locking SQLite during batched metric writes.
            security = self._security_repo().ensure_from_symbol(
                symbol,
                connection=connection,
            )
            security_ids[symbol] = security.security_id
        persisted_rows = [
            (
                security_ids[symbol],
                metric_id,
                value,
                as_of,
                unit_kind,
                metric_currency_or_none(unit_kind, currency),
                unit_label,
            )
            for (
                symbol,
                metric_id,
                value,
                as_of,
                unit_kind,
                currency,
                unit_label,
            ) in metric_rows
            if symbol in security_ids
        ]
        if not persisted_rows:
            return 0

        upsert_sql = """
            INSERT INTO metrics (
                listing_id,
                metric_id,
                value,
                as_of,
                unit_kind,
                currency,
                unit_label
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(listing_id, metric_id) DO UPDATE SET
                value = excluded.value,
                as_of = excluded.as_of,
                unit_kind = excluded.unit_kind,
                currency = excluded.currency,
                unit_label = excluded.unit_label
            """

        if connection is not None:
            # Caller owns the connection lifetime; commit the new rows so
            # other readers (workers, screeners) see them immediately.
            connection.executemany(upsert_sql, persisted_rows)
            if commit:
                connection.commit()
        else:

            def _persist() -> None:
                with self._connect() as conn:
                    conn.executemany(upsert_sql, persisted_rows)

            self._run_with_locked_retry(_persist)
        return len(persisted_rows)

    def fetch(self, symbol: str, metric_id: str) -> Optional[MetricRecord]:
        self.initialize_schema()
        security_id = self._security_repo().resolve_id(symbol)
        if security_id is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT value, as_of, unit_kind, currency, unit_label
                FROM metrics
                WHERE listing_id = ? AND metric_id = ?
                """,
                (security_id, metric_id),
            ).fetchone()
        if row is None:
            return None
        return MetricRecord(
            value=row["value"],
            as_of=row["as_of"],
            unit_kind=row["unit_kind"],
            currency=metric_currency_or_none(row["unit_kind"], row["currency"]),
            unit_label=row["unit_label"],
        )

    def fetch_many_for_symbols(
        self,
        symbols: Sequence[str],
        metric_ids: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[str, Dict[str, MetricRecord]]:
        """Fetch requested stored metrics for a symbol scope with chunked indexed reads."""

        self.initialize_schema()
        normalized_symbols = _normalized_codes(symbols)
        requested_metric_ids = sorted(
            {
                str(metric_id).strip()
                for metric_id in metric_ids
                if str(metric_id).strip()
            }
        )
        if not normalized_symbols or not requested_metric_ids:
            return {}

        security_ids_by_symbol = self._security_repo().resolve_ids_many(
            normalized_symbols,
            chunk_size=chunk_size,
        )
        if not security_ids_by_symbol:
            return {}

        symbol_by_security_id = {
            security_id: symbol
            for symbol, security_id in security_ids_by_symbol.items()
        }
        metric_rows_by_symbol: Dict[str, Dict[str, MetricRecord]] = {}

        metric_chunk_size = max(
            1,
            min(len(requested_metric_ids), SQLITE_MAX_BOUND_PARAMETERS // 2),
        )
        security_ids = sorted(symbol_by_security_id.keys())

        with self._connect() as conn:
            for metric_chunk in _batched(requested_metric_ids, metric_chunk_size):
                security_chunk_size = max(
                    1,
                    min(
                        chunk_size,
                        SQLITE_MAX_BOUND_PARAMETERS - len(metric_chunk),
                    ),
                )
                for security_chunk in _batched(security_ids, security_chunk_size):
                    security_placeholders = ", ".join("?" for _ in security_chunk)
                    metric_placeholders = ", ".join("?" for _ in metric_chunk)
                    rows = conn.execute(
                        f"""
                        SELECT listing_id, metric_id, value, as_of, unit_kind, currency, unit_label
                        FROM metrics
                        WHERE listing_id IN ({security_placeholders})
                          AND metric_id IN ({metric_placeholders})
                        """,
                        list(security_chunk) + list(metric_chunk),
                    ).fetchall()
                    for row in rows:
                        symbol = symbol_by_security_id[row["listing_id"]]
                        metric_rows_by_symbol.setdefault(symbol, {})[
                            row["metric_id"]
                        ] = MetricRecord(
                            value=row["value"],
                            as_of=row["as_of"],
                            unit_kind=row["unit_kind"],
                            currency=metric_currency_or_none(
                                row["unit_kind"], row["currency"]
                            ),
                            unit_label=row["unit_label"],
                        )

        return metric_rows_by_symbol


class MetricComputeStatusRepository(SQLiteStore):
    """Persist the latest metric-computation attempt per symbol/metric."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metric_compute_status (
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
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_metric_compute_status_metric_status
                ON metric_compute_status(metric_id, status)
                """
            )

    def upsert_many(
        self,
        rows: Iterable[MetricComputeStatusRecord],
        *,
        connection: Optional[sqlite3.Connection] = None,
        commit: bool = True,
    ) -> int:
        status_rows = list(rows)
        if not status_rows:
            return 0

        unique_symbols = []
        seen_symbols = set()
        for row in status_rows:
            symbol = row.symbol.strip().upper()
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            unique_symbols.append(symbol)

        if connection is None:
            self.initialize_schema()
        security_ids = self._security_repo().resolve_ids_many(
            unique_symbols,
            connection=connection,
        )
        for symbol in unique_symbols:
            if symbol in security_ids:
                continue
            security = self._security_repo().ensure_from_symbol(
                symbol,
                connection=connection,
            )
            security_ids[symbol] = security.security_id

        payload = [
            (
                security_ids[row.symbol.strip().upper()],
                row.metric_id,
                row.status,
                row.reason_code,
                row.reason_detail,
                row.attempted_at,
                row.value_as_of,
                row.facts_refreshed_at,
                row.market_data_as_of,
                row.market_data_updated_at,
            )
            for row in status_rows
            if row.symbol.strip().upper() in security_ids
        ]
        if not payload:
            return 0

        upsert_sql = """
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(listing_id, metric_id) DO UPDATE SET
                status = excluded.status,
                reason_code = excluded.reason_code,
                reason_detail = excluded.reason_detail,
                attempted_at = excluded.attempted_at,
                value_as_of = excluded.value_as_of,
                facts_refreshed_at = excluded.facts_refreshed_at,
                market_data_as_of = excluded.market_data_as_of,
                market_data_updated_at = excluded.market_data_updated_at
        """

        if connection is not None:
            connection.executemany(upsert_sql, payload)
            if commit:
                connection.commit()
        else:

            def _persist() -> None:
                with self._connect() as conn:
                    conn.executemany(upsert_sql, payload)

            self._run_with_locked_retry(_persist)
        return len(payload)

    def fetch(self, symbol: str, metric_id: str) -> Optional[MetricComputeStatusRecord]:
        self.initialize_schema()
        security_id = self._security_repo().resolve_id(symbol)
        if security_id is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT status, reason_code, reason_detail, attempted_at,
                       value_as_of, facts_refreshed_at, market_data_as_of,
                       market_data_updated_at
                FROM metric_compute_status
                WHERE listing_id = ? AND metric_id = ?
                """,
                (security_id, metric_id),
            ).fetchone()
        if row is None:
            return None
        return MetricComputeStatusRecord(
            symbol=symbol.strip().upper(),
            metric_id=metric_id,
            status=row["status"],
            attempted_at=row["attempted_at"],
            reason_code=row["reason_code"],
            reason_detail=row["reason_detail"],
            value_as_of=row["value_as_of"],
            facts_refreshed_at=row["facts_refreshed_at"],
            market_data_as_of=row["market_data_as_of"],
            market_data_updated_at=row["market_data_updated_at"],
        )

    def fetch_many_for_symbols(
        self,
        symbols: Sequence[str],
        metric_ids: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[str, Dict[str, MetricComputeStatusRecord]]:
        self.initialize_schema()
        normalized_symbols = _normalized_codes(symbols)
        requested_metric_ids = sorted(
            {
                str(metric_id).strip()
                for metric_id in metric_ids
                if str(metric_id).strip()
            }
        )
        if not normalized_symbols or not requested_metric_ids:
            return {}

        security_ids_by_symbol = self._security_repo().resolve_ids_many(
            normalized_symbols,
            chunk_size=chunk_size,
        )
        if not security_ids_by_symbol:
            return {}

        symbol_by_security_id = {
            security_id: symbol
            for symbol, security_id in security_ids_by_symbol.items()
        }
        rows_by_symbol: Dict[str, Dict[str, MetricComputeStatusRecord]] = {}
        metric_chunk_size = max(
            1,
            min(len(requested_metric_ids), SQLITE_MAX_BOUND_PARAMETERS // 2),
        )
        security_ids = sorted(symbol_by_security_id.keys())

        with self._connect() as conn:
            for metric_chunk in _batched(requested_metric_ids, metric_chunk_size):
                security_chunk_size = max(
                    1,
                    min(chunk_size, SQLITE_MAX_BOUND_PARAMETERS - len(metric_chunk)),
                )
                for security_chunk in _batched(security_ids, security_chunk_size):
                    security_placeholders = ", ".join("?" for _ in security_chunk)
                    metric_placeholders = ", ".join("?" for _ in metric_chunk)
                    rows = conn.execute(
                        f"""
                        SELECT listing_id, metric_id, status, reason_code, reason_detail,
                               attempted_at, value_as_of, facts_refreshed_at,
                               market_data_as_of, market_data_updated_at
                        FROM metric_compute_status
                        WHERE listing_id IN ({security_placeholders})
                          AND metric_id IN ({metric_placeholders})
                        """,
                        list(security_chunk) + list(metric_chunk),
                    ).fetchall()
                    for row in rows:
                        symbol = symbol_by_security_id[row["listing_id"]]
                        rows_by_symbol.setdefault(symbol, {})[row["metric_id"]] = (
                            MetricComputeStatusRecord(
                                symbol=symbol,
                                metric_id=row["metric_id"],
                                status=row["status"],
                                attempted_at=row["attempted_at"],
                                reason_code=row["reason_code"],
                                reason_detail=row["reason_detail"],
                                value_as_of=row["value_as_of"],
                                facts_refreshed_at=row["facts_refreshed_at"],
                                market_data_as_of=row["market_data_as_of"],
                                market_data_updated_at=row["market_data_updated_at"],
                            )
                        )
        return rows_by_symbol


class MarketDataRepository(SQLiteStore):
    """Persist canonical market data snapshots."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_data (
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
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_market_data_latest
                ON market_data(listing_id, as_of DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_market_data_currency_nonnull
                ON market_data(currency)
                WHERE currency IS NOT NULL
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
                normalize_currency_code(row.currency),
                row.source_provider.strip().upper(),
                updated_at,
            )
            for row in rows
        ]
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO market_data (
                    listing_id,
                    as_of,
                    price,
                    volume,
                    market_cap,
                    currency,
                    source_provider,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(listing_id, as_of) DO UPDATE SET
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
        record = self.latest_snapshot_record(symbol)
        if record is None:
            return None
        return PriceData(
            symbol=record.symbol,
            price=record.price,
            as_of=record.as_of,
            volume=record.volume,
            market_cap=record.market_cap,
            currency=record.currency,
        )

    def latest_price(self, symbol: str) -> Optional[Tuple[str, float]]:
        snapshot = self.latest_snapshot(symbol)
        if snapshot is None:
            return None
        return snapshot.as_of, snapshot.price

    def latest_snapshot_record(self, symbol: str) -> Optional[MarketSnapshotRecord]:
        self.initialize_schema()
        security_id = self._security_repo().resolve_id(symbol)
        if security_id is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT s.canonical_symbol, md.listing_id AS security_id, md.as_of, md.price, md.volume,
                       md.market_cap, md.currency, md.updated_at
                FROM market_data md
                JOIN securities s ON s.security_id = md.listing_id
                WHERE md.listing_id = ?
                ORDER BY md.as_of DESC
                LIMIT 1
                """,
                (security_id,),
            ).fetchone()
        if row is None:
            return None
        return MarketSnapshotRecord(
            security_id=row["security_id"],
            symbol=row["canonical_symbol"],
            as_of=row["as_of"],
            price=row["price"],
            volume=row["volume"],
            market_cap=row["market_cap"],
            currency=row["currency"],
            updated_at=row["updated_at"],
        )

    def latest_snapshots_many(
        self,
        symbols: Sequence[str],
        chunk_size: int = 500,
        *,
        security_ids_by_symbol: Optional[Mapping[str, int]] = None,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, MarketSnapshotRecord]:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}

        resolved_security_ids = (
            dict(security_ids_by_symbol)
            if security_ids_by_symbol is not None
            else self._security_repo().resolve_ids_many(
                normalized,
                chunk_size=chunk_size,
                connection=connection,
            )
        )
        resolved_symbols = [
            symbol for symbol in normalized if symbol in resolved_security_ids
        ]
        if not resolved_symbols:
            return {}

        snapshots: Dict[str, MarketSnapshotRecord] = {}

        def _query(conn: sqlite3.Connection) -> None:
            for chunk in _batched(resolved_symbols, chunk_size):
                symbol_by_security_id = {
                    resolved_security_ids[symbol]: symbol for symbol in chunk
                }
                placeholders = ", ".join("?" for _ in symbol_by_security_id)
                cursor = conn.execute(
                    f"""
                    WITH latest AS (
                        SELECT
                            listing_id,
                            MAX(as_of) AS as_of
                        FROM market_data INDEXED BY idx_market_data_latest
                        WHERE listing_id IN ({placeholders})
                        GROUP BY listing_id
                    )
                    SELECT
                        md.listing_id AS security_id,
                        md.as_of,
                        md.price,
                        md.volume,
                        md.market_cap,
                        md.currency,
                        md.updated_at
                    FROM latest
                    JOIN market_data md
                      ON md.listing_id = latest.listing_id
                     AND md.as_of = latest.as_of
                    ORDER BY md.listing_id
                    """,
                    list(symbol_by_security_id),
                )
                for row in cursor:
                    symbol = symbol_by_security_id[row["security_id"]]
                    snapshots[symbol] = MarketSnapshotRecord(
                        security_id=row["security_id"],
                        symbol=symbol,
                        as_of=row["as_of"],
                        price=row["price"],
                        volume=row["volume"],
                        market_cap=row["market_cap"],
                        currency=row["currency"],
                        updated_at=row["updated_at"],
                    )

        if connection is not None:
            _query(connection)
        else:
            with self._connect() as conn:
                _query(conn)
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
                WHERE listing_id = ?
                  AND as_of = (
                      SELECT MAX(as_of)
                      FROM market_data
                      WHERE listing_id = ?
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
                WHERE listing_id = ? AND as_of = ?
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
        sector: Optional[str] = None,
        industry: Optional[str] = None,
    ) -> None:
        self._security_repo().upsert_metadata(
            symbol,
            entity_name=entity_name,
            description=description,
            sector=sector,
            industry=industry,
        )

    def upsert_many(self, updates: Sequence[SecurityMetadataUpdate]) -> int:
        return self._security_repo().upsert_metadata_many(updates)

    def fetch(self, symbol: str) -> Optional[str]:
        return self._security_repo().fetch_name(symbol)

    def fetch_description(self, symbol: str) -> Optional[str]:
        return self._security_repo().fetch_description(symbol)

    def fetch_sector(self, symbol: str) -> Optional[str]:
        return self._security_repo().fetch_sector(symbol)

    def fetch_industry(self, symbol: str) -> Optional[str]:
        return self._security_repo().fetch_industry(symbol)

    def fetch_many(self, symbols: Sequence[str]) -> Dict[str, Security]:
        return self._security_repo().fetch_many_by_symbol(symbols)


ListingRepository = SecurityRepository
ProviderExchangeRepository = ExchangeProviderRepository
ProviderListingRepository = SupportedTickerRepository
ProviderListing = SupportedTicker


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
    "Exchange",
    "ExchangeProvider",
    "ExchangeProviderRepository",
    "ExchangeRepository",
    "FXRateRecord",
    "FXRatesRepository",
    "Security",
    "SecurityMetadataCandidate",
    "SecurityMetadataUpdate",
    "SecurityRepository",
    "ListingRepository",
    "FundamentalsRepository",
    "IngestProgressSummary",
    "IngestProgressExchange",
    "IngestProgressFailure",
    "SupportedTicker",
    "SupportedTickerRepository",
    "ProviderExchangeRepository",
    "ProviderListing",
    "ProviderListingRepository",
    "FundamentalsFetchStateRepository",
    "MarketDataFetchStateRepository",
    "FinancialFactsRepository",
    "FinancialFactsRefreshStateRecord",
    "FinancialFactsRefreshStateRepository",
    "MarketDataRepository",
    "FactRecord",
    "MarketSnapshotRecord",
    "MetricComputeStatusRecord",
    "MetricComputeStatusRepository",
    "MetricRecord",
    "MetricsRepository",
    "EntityMetadataRepository",
]
