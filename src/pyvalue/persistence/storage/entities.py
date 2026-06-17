"""Provider, security, exchange, and exchange-provider repositories.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from pyvalue.currency import (
    raw_currency_code,
)

from .base import (
    SQLITE_MAX_BOUND_PARAMETERS,
    SQLiteStore,
    _batched,
    _normalize_optional_text,
    _normalize_required_text,
    _normalize_symbol_base,
    _normalized_codes,
    _primary_listing_predicate,
    _utc_now_iso,
)
from .records import (
    Exchange,
    ExchangeProvider,
    Provider,
    Security,
    SecurityMetadataUpdate,
)
from .migrations import apply_migrations


class ProviderRepository(SQLiteStore):
    """Persist and resolve provider registry rows."""

    def initialize_schema(self) -> None:
        # The `provider` table (created by migration 034) and the legacy
        # `providers` view (owned by migration 044) are migration-managed.
        apply_migrations(self.db_path)

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


def _listing_pair_filter(
    chunk: Sequence[Tuple[str, str]],
) -> Tuple[str, List[str], set[Tuple[str, str]]]:
    """Build an index-seekable predicate for a chunk of (ticker, exchange) pairs.

    SQLite cannot turn a row-value ``(l.symbol, e.exchange_code) IN (VALUES ...)``
    spanning two tables into index seeks for more than one pair -- it degrades to
    a full ``listing`` enumeration (``SCAN exchange`` -> ``SEARCH listing``).
    Splitting it into two single-column ``IN`` lists lets the planner seek
    ``exchange`` by its UNIQUE ``exchange_code`` and ``listing`` by the UNIQUE
    ``(exchange_id, symbol)`` index instead. The two-IN form matches the cross
    product of symbols x exchange_codes, so callers filter rows back to the exact
    requested pairs via the returned ``wanted`` set -- keeping the result
    identical to the row-value form while making it index-driven.
    """

    symbols = list({ticker for ticker, _ in chunk})
    exchanges = list({exchange for _, exchange in chunk})
    symbol_placeholders = ", ".join("?" for _ in symbols)
    exchange_placeholders = ", ".join("?" for _ in exchanges)
    clause = (
        f"l.symbol IN ({symbol_placeholders}) "
        f"AND e.exchange_code IN ({exchange_placeholders})"
    )
    return clause, [*symbols, *exchanges], set(chunk)


class SecurityRepository(SQLiteStore):
    """Persist canonical security identities."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._by_symbol: Dict[str, Security] = {}
        self._by_id: Dict[int, Security] = {}

    def initialize_schema(self) -> None:
        # `issuer`, `listing`, their indexes, and the `securities` compat
        # view are owned by migrations (034 for the tables, 044 for the
        # view). The runtime ALTER for `primary_listing_status` is also
        # gone because migration 038 introduced the column with the same
        # CHECK constraint and migration 042's ownership pattern means
        # initialize_schema doesn't need to patch the schema anymore.
        apply_migrations(self.db_path)
        self._exchange_repo().initialize_schema()

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

    def _ensure_listing_for_exchange_id(
        self,
        conn: sqlite3.Connection,
        exchange_id: int,
        ticker: str,
        canonical_symbol: str,
        entity_name: Optional[str],
        description: Optional[str],
        sector: Optional[str],
        industry: Optional[str],
        listing_currency: Optional[str],
    ) -> Optional[Security]:
        """Create or update the issuer + listing for an already-resolved exchange.

        ``exchange_id`` must reference an existing ``exchange`` row -- this helper
        never writes the exchange catalog (that ownership belongs to
        refresh-supported-exchanges). Shared by :meth:`ensure` (which resolves the
        exchange by code) and :meth:`ensure_with_exchange_id` (handed the id).
        """
        security = self._load_by_exchange_and_symbol(conn, exchange_id, ticker)
        if security is None:
            if listing_currency is None:
                raise ValueError(
                    f"Cannot create listing {canonical_symbol} without a "
                    "quote currency. Listings are created from the "
                    "refresh-supported-tickers payload currency; there is "
                    "no currency fallback."
                )
            # migration 064 enforces issuer.name NOT NULL. Use the
            # canonical_symbol as a placeholder name when the caller doesn't
            # supply one; downstream metadata refreshes can promote it to the
            # real entity name.
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
                (
                    entity_name or canonical_symbol,
                    description,
                    sector,
                    industry,
                ),
            )
            if cursor.lastrowid is None:
                raise RuntimeError(f"Failed to create issuer for {canonical_symbol}")
            issuer_id = int(cursor.lastrowid)
            conn.execute(
                """
                INSERT INTO listing (
                    issuer_id,
                    exchange_id,
                    symbol,
                    currency
                ) VALUES (?, ?, ?, ?)
                """,
                (issuer_id, exchange_id, ticker, listing_currency),
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
        return self._load_by_exchange_and_symbol(conn, exchange_id, ticker)

    def ensure(
        self,
        canonical_ticker: str,
        canonical_exchange_code: str,
        entity_name: Optional[str] = None,
        description: Optional[str] = None,
        sector: Optional[str] = None,
        industry: Optional[str] = None,
        *,
        currency: Optional[str] = None,
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
        # Listings carry a currency from the provider payload only -- there is
        # no fallback or derivation. Creating a listing without one is refused.
        listing_currency = raw_currency_code(currency)

        def _ensure(conn: sqlite3.Connection) -> Optional[Security]:
            # ``ensure`` resolves (and, if absent, creates) the exchange by code.
            # Callers that already hold a resolved exchange_id and must NOT write
            # the exchange catalog use ``ensure_with_exchange_id`` instead.
            exchange = self._exchange_repo().ensure(exchange_code, connection=conn)
            return self._ensure_listing_for_exchange_id(
                conn,
                exchange.exchange_id,
                ticker,
                canonical_symbol,
                entity_name,
                description,
                sector,
                industry,
                listing_currency,
            )

        if connection is not None:
            loaded = _ensure(connection)
        else:
            with self._connect() as conn:
                loaded = _ensure(conn)
        if loaded is None:  # pragma: no cover - defensive
            raise RuntimeError(f"Failed to create or load security {canonical_symbol}")
        return loaded

    def ensure_with_exchange_id(
        self,
        exchange_id: int,
        canonical_exchange_code: str,
        canonical_ticker: str,
        entity_name: Optional[str] = None,
        description: Optional[str] = None,
        sector: Optional[str] = None,
        industry: Optional[str] = None,
        *,
        currency: Optional[str] = None,
        connection: sqlite3.Connection,
    ) -> Security:
        """Create/update the issuer + listing against an ALREADY-RESOLVED exchange.

        Unlike :meth:`ensure`, this never touches the ``exchange`` table: the
        caller resolved ``exchange_id`` from an existing row, because the exchange
        catalog is owned by refresh-supported-exchanges. ``canonical_exchange_code``
        is used only to build the canonical symbol / placeholder name. A live
        ``connection`` is required -- every caller runs inside a refresh
        transaction.
        """
        ticker = _normalize_required_text(canonical_ticker, "canonical_ticker").upper()
        exchange_code = _normalize_required_text(
            canonical_exchange_code, "canonical_exchange_code"
        ).upper()
        canonical_symbol = f"{ticker}.{exchange_code}"
        loaded = self._ensure_listing_for_exchange_id(
            connection,
            exchange_id,
            ticker,
            canonical_symbol,
            _normalize_optional_text(entity_name),
            _normalize_optional_text(description),
            _normalize_optional_text(sector),
            _normalize_optional_text(industry),
            raw_currency_code(currency),
        )
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
        currency: Optional[str] = None,
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
            currency=currency,
            connection=connection,
        )

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

        # Split each canonical symbol once in Python so the SQL probes
        # (l.symbol, e.exchange_code) directly. Concatenating the columns
        # in the WHERE clause defeats the underlying UNIQUE index on
        # listing(exchange_id, symbol).
        pairs: List[Tuple[str, str]] = []
        for canonical in uncached:
            ticker, exchange = _normalize_symbol_base(canonical)
            if exchange is None:
                continue
            pairs.append((ticker, exchange))
        if not pairs:
            return resolved

        # Two single-column INs (see _listing_pair_filter) so the existing
        # unique indexes drive the lookup instead of a full listing scan; cap the
        # chunk so the combined IN lists stay within SQLite's parameter limit.
        effective_chunk = min(chunk_size, SQLITE_MAX_BOUND_PARAMETERS // 2)

        def _query(conn: sqlite3.Connection) -> None:
            for chunk in _batched(pairs, effective_chunk):
                clause, params, wanted = _listing_pair_filter(chunk)
                rows = conn.execute(
                    f"""
                    SELECT
                        l.listing_id AS security_id,
                        l.symbol AS ticker,
                        e.exchange_code AS exchange_code,
                        l.symbol || '.' || e.exchange_code AS canonical_symbol
                    FROM listing l
                    JOIN "exchange" e ON e.exchange_id = l.exchange_id
                    WHERE {clause}
                    """,
                    params,
                ).fetchall()
                for row in rows:
                    # The two-IN predicate matches the cross product, so keep only
                    # the exact pairs that were requested.
                    if (row["ticker"], row["exchange_code"]) in wanted:
                        resolved[row["canonical_symbol"]] = row["security_id"]

        if connection is not None:
            _query(connection)
        else:
            with self._connect() as conn:
                _query(conn)
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

    def list_supported_listings(
        self,
        exchange_codes: Optional[Sequence[str]] = None,
        *,
        primary_only: bool = False,
    ) -> List[Tuple[int, str]]:
        """Return ``(listing_id, canonical_symbol)`` for supported listings in scope.

        The canonical-scope universe read: it surfaces the ``listing_id`` the
        scope join already carries so callers (compute-metrics, run-screen, the
        report-* commands) never resolve the canonical symbol back to a
        ``listing_id`` per batch or write flush. ``DISTINCT`` over
        ``(listing_id, canonical_symbol)`` collapses duplicate provider rows; the
        canonical symbol maps to exactly one listing (``listing`` is UNIQUE on
        ``(exchange_id, symbol)``).
        """

        self.initialize_schema()
        params: List[object] = []
        query = [
            "SELECT DISTINCT l.listing_id AS listing_id,",
            "l.symbol || '.' || e.exchange_code AS canonical_symbol",
            "FROM provider_listing pl",
            "JOIN listing l ON l.listing_id = pl.listing_id",
            'JOIN "exchange" e ON e.exchange_id = l.exchange_id',
        ]
        normalized = _normalized_codes(exchange_codes)
        if normalized:
            placeholders = ", ".join("?" for _ in normalized)
            query.append(f"WHERE e.exchange_code IN ({placeholders})")
            params.extend(normalized)
            if primary_only:
                query.append(f"AND {_primary_listing_predicate('l')}")
        elif primary_only:
            query.append(f"WHERE {_primary_listing_predicate('l')}")
        query.append("ORDER BY canonical_symbol")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [(int(row["listing_id"]), row["canonical_symbol"]) for row in rows]

    def list_supported_listings_for_symbols(
        self,
        symbols: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[str, Tuple[int, bool]]:
        """Targeted ``{canonical_symbol: (listing_id, is_primary)}`` for given symbols.

        The ``--symbols`` counterpart of :meth:`list_supported_listings`: rather
        than materialising the whole supported universe to validate a handful of
        requested tickers, it splits each canonical symbol into
        ``(ticker, exchange_code)`` and seeks the supported listing directly --
        an ``exchange_code`` index seek, then the ``(exchange_id, symbol)``
        composite index seek via the join, then ``provider_listing`` by
        ``listing_id``. Only supported matches appear in the map; a requested
        symbol that is absent is not supported. ``is_primary`` reuses
        :func:`_primary_listing_predicate`, so it is ``False`` only for explicitly
        secondary listings.
        """

        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}

        # Split each canonical symbol once so the SQL probes (l.symbol,
        # e.exchange_code) directly; concatenating the columns in the WHERE clause
        # would defeat the UNIQUE (exchange_id, symbol) index.
        pairs: List[Tuple[str, str]] = []
        for canonical in normalized:
            ticker, exchange = _normalize_symbol_base(canonical)
            if exchange is None:
                continue
            pairs.append((ticker, exchange))
        if not pairs:
            return {}

        effective_chunk = min(chunk_size, SQLITE_MAX_BOUND_PARAMETERS // 2)
        resolved: Dict[str, Tuple[int, bool]] = {}

        def _query(conn: sqlite3.Connection) -> None:
            for chunk in _batched(pairs, effective_chunk):
                clause, params, wanted = _listing_pair_filter(chunk)
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT
                        l.listing_id AS listing_id,
                        l.symbol AS ticker,
                        e.exchange_code AS exchange_code,
                        l.symbol || '.' || e.exchange_code AS canonical_symbol,
                        CASE WHEN {_primary_listing_predicate("l")} THEN 1 ELSE 0 END
                            AS is_primary
                    FROM provider_listing pl
                    JOIN listing l ON l.listing_id = pl.listing_id
                    JOIN "exchange" e ON e.exchange_id = l.exchange_id
                    WHERE {clause}
                    """,
                    params,
                ).fetchall()
                for row in rows:
                    # The two-IN predicate matches the cross product, so keep only
                    # the exact pairs that were requested.
                    if (row["ticker"], row["exchange_code"]) in wanted:
                        resolved[row["canonical_symbol"]] = (
                            int(row["listing_id"]),
                            bool(row["is_primary"]),
                        )

        with self._connect() as conn:
            _query(conn)
        return resolved

    def entity_names_by_ids(
        self,
        listing_ids: Sequence[int],
        chunk_size: int = 500,
    ) -> Dict[int, Optional[str]]:
        """Return ``{listing_id: issuer name}`` for the given listings (id-keyed).

        The display-label read for run-screen: it keys on the ``listing_id``s the
        scope already resolved, so no symbol/exchange lookup is needed. A listing
        whose issuer carries no name is absent from the map (the caller falls back to
        the canonical symbol).
        """
        self.initialize_schema()
        normalized = sorted(
            {int(listing_id) for listing_id in listing_ids if listing_id}
        )
        if not normalized:
            return {}
        names: Dict[int, Optional[str]] = {}
        with self._connect() as conn:
            for chunk in _batched(normalized, chunk_size):
                placeholders = ", ".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT l.listing_id AS listing_id, i.name AS entity_name
                    FROM listing l
                    JOIN issuer i ON i.issuer_id = l.issuer_id
                    WHERE l.listing_id IN ({placeholders})
                    """,
                    list(chunk),
                ).fetchall()
                for row in rows:
                    names[int(row["listing_id"])] = _normalize_optional_text(
                        row["entity_name"]
                    )
        return names

    def _remember(self, security: Security) -> None:
        self._by_id[security.security_id] = security
        self._by_symbol[security.canonical_symbol] = security


class ExchangeRepository(SQLiteStore):
    """Persist canonical exchange identities."""

    def initialize_schema(self) -> None:
        # The `exchange` table is owned by migration 034.
        apply_migrations(self.db_path)

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
        # `provider_exchange` (table + idx_provider_exchange_exchange) is
        # owned by migration 034. The `exchange_provider` compat view is
        # owned by migration 044.
        apply_migrations(self.db_path)
        self._provider_repo().initialize_schema()
        self._exchange_repo().initialize_schema()

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
                        _normalize_optional_text(row.get("Name") or row.get("name"))
                        or code_norm,
                        _normalize_optional_text(
                            row.get("Country") or row.get("country")
                        )
                        or "Unknown",
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
            code_norm = provider_exchange_code.strip().upper()
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
                    code_norm,
                    exchange.exchange_id,
                    _normalize_optional_text(name) or code_norm,
                    _normalize_optional_text(country) or "Unknown",
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
                WHERE p.provider_code = ? AND ep.provider_exchange_code = ?
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
            query.append("WHERE p.provider_code = ?")
            params.append(provider.strip().upper())
        query.append("ORDER BY p.provider_code, ep.provider_exchange_code")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [ExchangeProvider(*row) for row in rows]
