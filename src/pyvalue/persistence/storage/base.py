"""Shared SQLite infrastructure, helpers, and the SQLiteStore base class.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path
import json
import logging
import sqlite3
import time
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from pyvalue.currency import (
    normalize_currency_code,
    raw_currency_code,
)
from .migrations import apply_migrations

if TYPE_CHECKING:
    from .entities import (
        ExchangeProviderRepository,
        ExchangeRepository,
        ProviderRepository,
        SecurityRepository,
    )
    from .supported_tickers import SupportedTickerRepository


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


def canonical_json_dumps(value: Any) -> str:
    """Serialize JSON-compatible values in a stable form for hashing."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def fundamentals_payload_hash(data: str) -> str:
    """Return the SHA-256 hash for a canonical fundamentals payload string."""

    return hashlib.sha256(data.encode("utf-8")).hexdigest()


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


def _batched(values: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _primary_listing_predicate(alias: str = "catalog") -> str:
    return f"{alias}.primary_listing_status <> '{_LISTING_STATUS_SECONDARY}'"


def _provider_listing_catalog_view(*, primary_only: bool) -> str:
    """Return the catalog view name to query.

    When ``primary_only`` is set the caller wants to exclude secondary
    listings; ``primary_provider_listing_catalog`` (migration 062) is a
    pre-filtered projection of ``provider_listing_catalog`` so callers
    can swap the FROM clause and drop the inline
    ``primary_listing_status`` predicate.
    """

    return (
        "primary_provider_listing_catalog"
        if primary_only
        else "provider_listing_catalog"
    )


class _ManagedSQLiteConnection(sqlite3.Connection):
    """SQLite connection that closes the file handle when the context exits."""

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
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

    def ensure_wal_mode(self) -> str:
        """Best-effort switch to WAL; return the resulting (or current) journal mode.

        Encapsulates the OperationalError handling the metrics-compute path needs:
        WAL lets metric workers read while the parent writes, but a locked or
        read-only database must not abort the run -- fall back to the current mode
        (or ``"unknown"``). Keeping the ``sqlite3`` exception handling here means the
        CLI never imports ``sqlite3``.
        """
        try:
            return self.enable_wal_mode()
        except sqlite3.OperationalError as exc:
            logging.getLogger(__name__).warning(
                "Could not enable WAL mode on %s: %s", self.db_path, exc
            )
            try:
                return self.current_journal_mode()
            except sqlite3.OperationalError:
                return "unknown"

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
            # Deferred import: every repository subclasses SQLiteStore, so a
            # top-level import here would create an import cycle (base ->
            # entities -> base). Importing inside the accessor breaks it.
            from .entities import SecurityRepository

            self._security_repo_cache = SecurityRepository(self.db_path)
        return self._security_repo_cache

    def _provider_repo(self) -> ProviderRepository:
        if self._provider_repo_cache is None:
            # Deferred import to avoid the base <-> entities import cycle.
            from .entities import ProviderRepository

            self._provider_repo_cache = ProviderRepository(self.db_path)
        return self._provider_repo_cache

    def _exchange_repo(self) -> ExchangeRepository:
        if self._exchange_repo_cache is None:
            # Deferred import to avoid the base <-> entities import cycle.
            from .entities import ExchangeRepository

            self._exchange_repo_cache = ExchangeRepository(self.db_path)
        return self._exchange_repo_cache

    def _exchange_provider_repo(self) -> ExchangeProviderRepository:
        if self._exchange_provider_repo_cache is None:
            # Deferred import to avoid the base <-> entities import cycle.
            from .entities import ExchangeProviderRepository

            self._exchange_provider_repo_cache = ExchangeProviderRepository(
                self.db_path
            )
        return self._exchange_provider_repo_cache

    def _supported_ticker_repo(self) -> SupportedTickerRepository:
        if self._supported_ticker_repo_cache is None:
            # Deferred import to avoid the base <-> supported_tickers cycle.
            from .supported_tickers import SupportedTickerRepository

            self._supported_ticker_repo_cache = SupportedTickerRepository(self.db_path)
        return self._supported_ticker_repo_cache

    def ticker_currency(self, symbol: str) -> Optional[str]:
        """Return the base monetary currency for ``symbol``.

        ``listing.currency`` stores the quote unit and may contain a configured
        subunit such as GBX. Metrics and normalized facts use the base currency.
        """

        apply_migrations(self.db_path)
        ticker, exchange = _normalize_symbol_base(symbol)
        if exchange is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT l.currency
                FROM listing l
                JOIN "exchange" e ON e.exchange_id = l.exchange_id
                WHERE l.symbol = ? AND e.exchange_code = ?
                LIMIT 1
                """,
                (ticker, exchange),
            ).fetchone()
        return normalize_currency_code(row[0]) if row else None

    def ticker_currency_by_id(self, listing_id: int) -> Optional[str]:
        """Return the base monetary currency for ``listing_id``.

        The natural-identity counterpart of :meth:`ticker_currency`: ``currency``
        lives on ``listing`` itself, so this is a single PK lookup with no symbol
        resolution and no exchange join. Like the symbol form it collapses a
        configured subunit (GBX) to its base (GBP).
        """

        apply_migrations(self.db_path)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT currency
                FROM listing
                WHERE listing_id = ?
                LIMIT 1
                """,
                (int(listing_id),),
            ).fetchone()
        return normalize_currency_code(row[0]) if row else None

    def listing_quote_currency(self, symbol: str) -> Optional[str]:
        """Return the stored listing quote currency for ``symbol``."""

        apply_migrations(self.db_path)
        ticker, exchange = _normalize_symbol_base(symbol)
        if exchange is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT l.currency
                FROM listing l
                JOIN "exchange" e ON e.exchange_id = l.exchange_id
                WHERE l.symbol = ? AND e.exchange_code = ?
                LIMIT 1
                """,
                (ticker, exchange),
            ).fetchone()
        return raw_currency_code(row[0]) if row else None


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
