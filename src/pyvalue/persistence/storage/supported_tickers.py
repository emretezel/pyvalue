"""Supported-ticker (provider listing) repository.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import sqlite3
from typing import (
    Any,
    Dict,
    Final,
    List,
    Optional,
    Sequence,
    Tuple,
)

from pyvalue.currency import (
    shaped_currency_code,
)

from .base import (
    SQLiteStore,
    _batched,
    _coerce_int,
    _normalize_optional_text,
    _normalized_codes,
    _provider_listing_catalog_view,
)
from .records import (
    IngestProgressExchange,
    IngestProgressFailure,
    SupportedTicker,
    SupportedTickerRefreshResult,
)
from .migrations import apply_migrations
from .fundamentals import FundamentalsRepository
from .fetch_state import (
    FundamentalsFetchStateRepository,
    MarketDataFetchStateRepository,
)
from .metrics_market import MarketDataRepository


# Anomalous-payload guard for the per-exchange prune. On 2026-07-11 EODHD
# answered a plan-dropped exchange (BE) with HTTP 200 and a 30-symbol stub
# instead of a 404; the refresh trusted it and removed 2,835 provider
# listings in one transaction. A payload absence cannot distinguish real
# delisting from a plan change, a provider glitch, or a truncated response,
# so a removal that is both large in absolute terms AND a majority of the
# exchange's existing mappings is refused unless the operator explicitly
# opts in. Normal delisting churn (single-digit percent) never trips this;
# the absolute floor keeps tiny exchanges' full legitimate turnover flowing.
MASS_DELISTING_MIN_REMOVED: Final[int] = 20
MASS_DELISTING_MAX_REMOVED_FRACTION: Final[float] = 0.5


def mass_delisting_suspicious(existing: int, removed: int) -> bool:
    """True when removing ``removed`` of ``existing`` mappings looks anomalous.

    Pure predicate behind the guard: suspicious means the removal is large in
    absolute terms (>= ``MASS_DELISTING_MIN_REMOVED``) AND a majority of the
    exchange's existing mappings (> ``MASS_DELISTING_MAX_REMOVED_FRACTION``).
    Both bounds are needed: the fraction alone would block a tiny exchange's
    legitimate full turnover, the floor alone would block normal churn on
    large exchanges.
    """

    return (
        removed >= MASS_DELISTING_MIN_REMOVED
        and removed > MASS_DELISTING_MAX_REMOVED_FRACTION * existing
    )


class MassDelistingError(RuntimeError):
    """A refresh payload would remove a suspicious share of an exchange's mappings.

    Raised by :meth:`SupportedTickerRepository.replace_for_exchange` before any
    delete runs; the surrounding transaction rolls back, so a guarded exchange
    keeps every row it had (including upserts from the same payload). Carries
    the counts so callers can report precisely.
    """

    def __init__(self, exchange_code: str, existing: int, removed: int) -> None:
        self.exchange_code = exchange_code
        self.existing = existing
        self.removed = removed
        super().__init__(
            f"Refusing to remove {removed} of {existing} provider listings on "
            f"{exchange_code}: the payload looks truncated or the exchange left "
            "the provider plan. Re-run with --allow-mass-delisting to override."
        )


def _apply_catalog_scope(
    query: List[str],
    params: List[object],
    exchange_codes: Optional[Sequence[str]],
    provider_symbols: Optional[Sequence[str]],
) -> None:
    """Append the provider-scope WHERE clauses to a ``provider_listing_catalog`` read.

    The single site that narrows a catalog-view query (``list_for_provider`` /
    ``count_for_provider``) to the operator's ``--exchange-codes`` / ``--symbols``
    selection -- the scope-resolution boundary for the canonical catalog reads.
    Each filter is an optional IN-list over the already-normalised codes/symbols;
    an empty selection adds no clause (the whole provider is in scope). The query
    must alias the catalog view ``catalog``.
    """
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


def _apply_provider_listing_scope(
    query: List[str],
    params: List[object],
    exchange_codes: Optional[Sequence[str]],
    provider_symbols: Optional[Sequence[str]],
    *,
    symbol_expr: str,
) -> None:
    """Append the provider-scope WHERE clauses to a base-table eligibility query.

    The eligibility scans (``list_eligible_for_fundamentals`` /
    ``list_eligible_for_market_data``) read straight from ``provider_listing`` /
    ``provider_exchange`` rather than the catalog view, so the exchange filter
    keys on ``px.provider_exchange_code`` and the symbol filter on the caller's
    reconstructed qualified-symbol expression (``symbol_expr``, which varies by
    provider). This is the ingest/market-data scope-resolution boundary: the
    operator's selection narrows the streaming freshness scan server-side, paired
    with the query's own ``ORDER BY`` / ``LIMIT``.
    """
    normalized_codes = _normalized_codes(exchange_codes)
    if normalized_codes:
        placeholders = ", ".join("?" for _ in normalized_codes)
        query.append(f"AND px.provider_exchange_code IN ({placeholders})")
        params.extend(normalized_codes)
    normalized_symbols = _normalized_codes(provider_symbols)
    if normalized_symbols:
        placeholders = ", ".join("?" for _ in normalized_symbols)
        query.append(f"AND {symbol_expr} IN ({placeholders})")
        params.extend(normalized_symbols)


class SupportedTickerRepository(SQLiteStore):
    """Store provider-supported ticker catalogs by exchange."""

    def initialize_schema(self) -> None:
        # `provider_listing` (table + idx_provider_listing_listing) is
        # owned by migration 034 and refined by migration 054, which
        # dropped the derivable `provider_id` denormalisation column.
        apply_migrations(self.db_path)
        self._provider_repo().initialize_schema()
        self._exchange_provider_repo().initialize_schema()
        self._security_repo().initialize_schema()
        FundamentalsRepository(self.db_path).initialize_schema()
        FundamentalsFetchStateRepository(self.db_path).initialize_schema()
        MarketDataRepository(self.db_path).initialize_schema()
        MarketDataFetchStateRepository(self.db_path).initialize_schema()

    @staticmethod
    def _catalog_select_columns(alias: str = "catalog") -> str:
        # Column order matches the SupportedTicker field order (it is built via
        # ``SupportedTicker(*row)``); provider_listing_id is the trailing field.
        return (
            f"{alias}.provider, {alias}.provider_exchange_code, "
            f"{alias}.provider_symbol, {alias}.provider_ticker, "
            f"{alias}.security_id, {alias}.listing_exchange, "
            f"{alias}.security_name, {alias}.security_type, "
            f"{alias}.country, {alias}.currency, {alias}.isin, {alias}.updated_at, "
            f"{alias}.provider_listing_id"
        )

    def _resolve_provider_exchange(
        self,
        conn: sqlite3.Connection,
        provider: str,
        provider_exchange_code: str,
    ) -> Tuple[int, int, str]:
        """Resolve an existing provider_exchange to its ids + canonical code.

        Returns ``(provider_exchange_id, exchange_id, exchange_code)``. This is
        read-only on purpose: the exchange catalog (``exchange`` +
        ``provider_exchange``) is owned by refresh-supported-exchanges. If the
        row is absent we raise rather than fabricate a stub -- the operator must
        run refresh-supported-exchanges for the provider first. ``provider`` and
        ``provider_exchange_code`` are expected already upper-normalised.
        """
        row = conn.execute(
            """
            SELECT px.provider_exchange_id, px.exchange_id, e.exchange_code
            FROM provider_exchange px
            JOIN provider p ON p.provider_id = px.provider_id
            JOIN "exchange" e ON e.exchange_id = px.exchange_id
            WHERE p.provider_code = ? AND px.provider_exchange_code = ?
            """,
            (provider, provider_exchange_code),
        ).fetchone()
        if row is None:
            raise ValueError(
                f"Provider exchange {provider}:{provider_exchange_code} is not in "
                "the catalog. Run refresh-supported-exchanges for this provider "
                "first -- refresh-supported-tickers only reads the exchange catalog."
            )
        return (
            int(row["provider_exchange_id"]),
            int(row["exchange_id"]),
            str(row["exchange_code"]),
        )

    def _ensure_provider_listing(
        self,
        conn: sqlite3.Connection,
        *,
        provider_exchange_id: int,
        exchange_id: int,
        canonical_exchange_code: str,
        bare_symbol: str,
        currency: Optional[str] = None,
        entity_name: Optional[str] = None,
    ) -> bool:
        """Catalog one provider listing; return True when retained.

        Returns False only when the payload carries no usable currency --
        absent, or failing the 3-uppercase-letter shape the ``listing.currency``
        CHECK enforces (e.g. EODHD's ``'Unknown'`` placeholder) -- so the
        listing cannot be modelled and is skipped. Otherwise the ticker is
        retained and this returns True -- whether it was created, updated, or
        already current.

        Skip-unchanged: a re-refresh re-sees rows that already match everything
        this command owns (``listing.currency``, ``issuer.name``, and the
        ``provider_listing`` mapping). When the stored state already matches,
        every write would be a no-op, so a single base-table read detects that
        and returns early -- a steady-state re-refresh then issues zero writes.
        Anything new or changed (new ticker, currency correction, name promotion,
        stale mapping) falls through to the write path.
        """
        # Listings must carry a currency (listing.currency is NOT NULL, carries
        # the 3-letter shape CHECK, and there is no fallback). A catalog entry
        # whose payload omits the currency -- or publishes a placeholder that
        # fails the shape, like 'Unknown' -- is skipped entirely: neither the
        # listing nor the provider_listing row is created.
        quote_currency = shaped_currency_code(currency)
        if quote_currency is None:
            return False
        normalized_name = _normalize_optional_text(entity_name)
        # Change-detection read against base tables (never the catalog view). The
        # l.exchange_id / l.symbol predicates also confirm the existing
        # provider_listing points at the *correct* listing, so a stale mapping
        # fails the match and is repaired by the write path below.
        current = conn.execute(
            """
            SELECT l.currency AS currency, i.name AS name
            FROM provider_listing pl
            JOIN listing l ON l.listing_id = pl.listing_id
            JOIN issuer i ON i.issuer_id = l.issuer_id
            WHERE pl.provider_exchange_id = ?
              AND pl.provider_symbol = ?
              AND l.exchange_id = ?
              AND l.symbol = ?
            """,
            (provider_exchange_id, bare_symbol, exchange_id, bare_symbol),
        ).fetchone()
        # The refresh only writes issuer.name when entity_name is supplied
        # (COALESCE keeps the stored name otherwise), so a NULL payload name is
        # always "unchanged" for the name dimension.
        if (
            current is not None
            and current["currency"] == quote_currency
            and (normalized_name is None or current["name"] == normalized_name)
        ):
            return True
        # The exchange row is already resolved by the caller; build the canonical
        # issuer/listing against that exchange_id WITHOUT writing the exchange
        # catalog (ensure_with_exchange_id never touches the exchange table).
        security = self._security_repo().ensure_with_exchange_id(
            exchange_id,
            canonical_exchange_code,
            bare_symbol,
            entity_name=entity_name,
            currency=quote_currency,
            connection=conn,
        )
        # Keep listing.currency in sync when the listing pre-existed with a
        # different currency. The guard skips the no-op write when the currency
        # is already correct (e.g. a freshly inserted listing).
        conn.execute(
            """
            UPDATE listing
            SET currency = ?
            WHERE listing_id = ? AND currency != ?
            """,
            (quote_currency, security.security_id, quote_currency),
        )
        conn.execute(
            """
            INSERT INTO provider_listing (
                provider_exchange_id,
                provider_symbol,
                listing_id
            ) VALUES (?, ?, ?)
            ON CONFLICT(provider_exchange_id, provider_symbol) DO UPDATE SET
                listing_id = excluded.listing_id
            """,
            (provider_exchange_id, bare_symbol, security.security_id),
        )
        return True

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

    def _count_orphaned_listings(
        self,
        conn: sqlite3.Connection,
        listing_ids: Sequence[int],
    ) -> int:
        """Count listings left without any provider mapping after a prune.

        Purely informational: canonical rows (``listing``/``issuer``) and
        canonical data (facts, market data, metrics) are provider-independent
        and are NEVER deleted by a refresh (user design, 2026-07-11). An
        orphaned listing is simply unreachable -- every scope resolver, the
        catalog views, and the ingest/market-data eligibility scans all join
        through ``provider_listing`` -- so it sits inert until a provider maps
        it again. The count is surfaced so the operator sees how much of the
        universe went dark in this slice. The probe is a point seek per row
        via idx_provider_listing_listing.
        """
        candidates = sorted({int(value) for value in listing_ids if value})
        orphaned = 0
        for chunk in _batched(candidates, 500):
            placeholders = ", ".join("?" for _ in chunk)
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS orphaned FROM listing
                WHERE listing_id IN ({placeholders})
                  AND NOT EXISTS (
                      SELECT 1 FROM provider_listing pl
                      WHERE pl.listing_id = listing.listing_id
                  )
                """,
                list(chunk),
            ).fetchone()
            orphaned += int(row["orphaned"])
        return orphaned

    def replace_for_exchange(
        self,
        provider: str,
        exchange_code: str,
        rows: Sequence[Dict[str, Any]],
        *,
        allow_mass_delisting: bool = False,
    ) -> SupportedTickerRefreshResult:
        """Replace one provider/exchange supported-ticker slice with ``rows``.

        Upserts every payload ticker into ``issuer``/``listing``/
        ``provider_listing``, then prunes the **provider layer** of the
        listings absent from the payload: the ``provider_listing`` mapping
        plus its raw fundamentals and fetch/normalization state. Canonical
        rows (``listing``/``issuer``) and canonical data (facts, market data,
        metrics) are provider-independent and are never deleted by a refresh
        (user design, 2026-07-11): a listing that loses its last mapping is
        only counted as orphaned and becomes unreachable through the provider
        joins until some provider maps it again. Payload rows without a
        usable currency (absent or malformed) are skipped, so they count as
        absent and are pruned like delisted tickers.

        A removal that is both >= ``MASS_DELISTING_MIN_REMOVED`` and more
        than ``MASS_DELISTING_MAX_REMOVED_FRACTION`` of the exchange's
        existing mappings raises :class:`MassDelistingError` unless
        ``allow_mass_delisting`` is set. The raise rolls back the whole
        slice, so a guarded exchange keeps every row it had -- new tickers
        and currency corrections from the same payload are deliberately
        discarded with it ("skipped" must mean untouched).

        The whole slice commits as one transaction.
        """
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        provider_exchange_code = exchange_code.strip().upper()
        retained_tickers: List[str] = []
        skipped_no_currency: List[str] = []
        with self._connect() as conn:
            # The exchange catalog is owned by refresh-supported-exchanges; this
            # path only reads it. Resolve the provider_exchange once (raises if
            # the operator hasn't refreshed exchanges) and reuse it for every
            # ticker -- no per-ticker exchange writes.
            provider_exchange_id, exchange_id, canonical_exchange_code = (
                self._resolve_provider_exchange(
                    conn, provider_norm, provider_exchange_code
                )
            )
            for row in rows:
                code = _normalize_optional_text(row.get("Code") or row.get("code"))
                if not code:
                    continue
                bare_symbol = code.upper()
                cataloged = self._ensure_provider_listing(
                    conn,
                    provider_exchange_id=provider_exchange_id,
                    exchange_id=exchange_id,
                    canonical_exchange_code=canonical_exchange_code,
                    bare_symbol=bare_symbol,
                    currency=row.get("Currency") or row.get("currency"),
                    entity_name=row.get("Name") or row.get("name"),
                )
                if not cataloged:
                    skipped_no_currency.append(bare_symbol)
                    continue
                retained_tickers.append(bare_symbol)
            retained = set(retained_tickers)
            existing_rows = conn.execute(
                """
                SELECT provider_listing_id, provider_symbol, listing_id
                FROM provider_listing
                WHERE provider_exchange_id = ?
                """,
                (provider_exchange_id,),
            ).fetchall()
            to_delete = [
                int(row["provider_listing_id"])
                for row in existing_rows
                if str(row["provider_symbol"]) not in retained
            ]
            # Capture the pruned rows' listing ids before the mapping is gone:
            # they are the only candidates the orphan count must consider.
            candidate_listing_ids = [
                int(row["listing_id"])
                for row in existing_rows
                if str(row["provider_symbol"]) not in retained
            ]
            if not allow_mass_delisting and mass_delisting_suspicious(
                len(existing_rows), len(to_delete)
            ):
                # Raising here unwinds the transaction, discarding this
                # payload's upserts too -- see the docstring.
                raise MassDelistingError(
                    provider_exchange_code, len(existing_rows), len(to_delete)
                )
            self._delete_provider_listing_ids(conn, to_delete)
            orphaned_listings = self._count_orphaned_listings(
                conn, candidate_listing_ids
            )
        return SupportedTickerRefreshResult(
            inserted=len(retained_tickers),
            removed=len(to_delete),
            skipped_no_currency=tuple(skipped_no_currency),
            orphaned_listings=orphaned_listings,
        )

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
        catalog_view = _provider_listing_catalog_view(primary_only=primary_only)
        query = [
            f"SELECT {self._catalog_select_columns('catalog')}",
            f"FROM {catalog_view} catalog",
        ]
        query.append("WHERE catalog.provider = ?")
        _apply_catalog_scope(query, params, exchange_codes, provider_symbols)
        query.append("ORDER BY catalog.provider_exchange_code, catalog.provider_symbol")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [SupportedTicker(*row) for row in rows]

    def count_for_provider(
        self,
        provider: str,
        exchange_codes: Optional[Sequence[str]] = None,
        provider_symbols: Optional[Sequence[str]] = None,
        *,
        primary_only: bool = False,
    ) -> int:
        """Count supported tickers for a provider within an optional scope.

        Applies the same provider/exchange/symbol filters as
        :meth:`list_for_provider` but returns only the row count. Callers that
        need a scope *size* for reporting (e.g. ``reconcile-listing-status``'s
        summary line) use this instead of hydrating every ``SupportedTicker``
        across the 6-table catalog view just to call ``len()`` on the result.
        """
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        params: List[object] = [provider_norm]
        catalog_view = _provider_listing_catalog_view(primary_only=primary_only)
        query = [
            "SELECT COUNT(*)",
            f"FROM {catalog_view} catalog",
            "WHERE catalog.provider = ?",
        ]
        _apply_catalog_scope(query, params, exchange_codes, provider_symbols)
        with self._connect() as conn:
            row = conn.execute(" ".join(query), params).fetchone()
        return int(row[0]) if row is not None else 0

    def list_canonical_listings(
        self,
        exchange_codes: Optional[Sequence[str]] = None,
        *,
        primary_only: bool = False,
    ) -> List[Tuple[int, str]]:
        """Return ``(listing_id, canonical_symbol)`` pairs for the requested scope.

        The canonical-scope universe read used by every canonical-scope command
        (compute-metrics, run-screen, the report-* commands) to carry the natural
        ``listing_id`` from scope resolution through to reads and writes instead
        of re-resolving it.
        """

        return self._security_repo().list_supported_listings(
            exchange_codes,
            primary_only=primary_only,
        )

    def list_canonical_listings_for_symbols(
        self,
        symbols: Sequence[str],
    ) -> Dict[str, Tuple[int, bool]]:
        """Targeted ``{canonical_symbol: (listing_id, is_primary)}`` for given symbols.

        The ``--symbols`` counterpart of :meth:`list_canonical_listings`: it seeks
        only the requested tickers (index-driven) instead of loading the whole
        supported universe just to validate and resolve a handful.
        """

        return self._security_repo().list_supported_listings_for_symbols(symbols)

    def available_exchanges(self, provider: Optional[str] = None) -> List[str]:
        self.initialize_schema()
        params: List[object] = []
        query = ["SELECT DISTINCT provider_exchange_code FROM provider_listing_catalog"]
        if provider:
            query.append("WHERE provider = ?")
            params.append(provider.strip().upper())
        query.append("ORDER BY provider_exchange_code")
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [row[0] for row in rows]

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

        # Resolve the provider once so the eligibility queries can filter on
        # provider_exchange.provider_id and read straight from the base tables
        # (provider_listing + provider_exchange) instead of the six-table
        # provider_listing_catalog view. security_id is just
        # provider_listing.listing_id (the join key), and currency -- the only
        # other column ingest used to read -- is now owned solely by
        # refresh-supported-tickers, so the issuer/exchange/listing/provider
        # joins all fall away, leaving three joins per branch.
        provider_id = self._provider_repo().resolve_id(provider_norm)
        if provider_id is None:
            return []

        # Rebuild the catalog's qualified provider_symbol from base columns so
        # scope filtering and ordering stay identical to the view. SEC symbols
        # are implicitly US-listed; every other provider qualifies by its
        # provider exchange code.
        if provider_norm == "SEC":
            qualified_symbol = "(pl.provider_symbol || '.US')"
        else:
            qualified_symbol = (
                "(pl.provider_symbol || '.' || px.provider_exchange_code)"
            )
        select_columns = (
            "px.provider_exchange_code AS provider_exchange_code, "
            f"{qualified_symbol} AS provider_symbol, "
            "pl.provider_symbol AS provider_ticker, "
            "pl.listing_id AS security_id, "
            "pl.provider_listing_id AS provider_listing_id"
        )

        def _build_ticker(row: sqlite3.Row) -> SupportedTicker:
            # Project only the columns ingest consumes. The remaining
            # SupportedTicker fields (currency, name, country, ...) are catalog
            # metadata the fundamentals path never reads, so they default to
            # None instead of dragging in extra joins. provider_listing_id IS
            # carried: ingest writes fundamentals_raw by it, so the write never
            # re-resolves the provider symbol.
            return SupportedTicker(
                provider=provider_norm,
                provider_exchange_code=str(row["provider_exchange_code"]),
                provider_symbol=str(row["provider_symbol"]),
                provider_ticker=str(row["provider_ticker"]),
                security_id=int(row["security_id"]),
                provider_listing_id=int(row["provider_listing_id"]),
            )

        def _apply_scope_filters(query: List[str], params: List[object]) -> None:
            _apply_provider_listing_scope(
                query,
                params,
                exchange_codes,
                provider_symbols,
                symbol_expr=qualified_symbol,
            )

        def _fetch_missing(limit: Optional[int]) -> List[SupportedTicker]:
            params: List[object] = [provider_id]
            query = [
                f"SELECT {select_columns}",
                "FROM provider_listing pl",
                "JOIN provider_exchange px "
                "ON px.provider_exchange_id = pl.provider_exchange_id",
                "LEFT JOIN fundamentals_raw fr "
                "ON fr.provider_listing_id = pl.provider_listing_id",
                "LEFT JOIN fundamentals_fetch_state fs "
                "ON fs.provider_listing_id = pl.provider_listing_id",
                "WHERE px.provider_id = ?",
                "AND fr.provider_listing_id IS NULL",
            ]
            _apply_scope_filters(query, params)
            if respect_backoff:
                query.append(
                    "AND (fs.next_eligible_at IS NULL OR fs.next_eligible_at <= ?)"
                )
                params.append(now.isoformat())
            query.append(f"ORDER BY {qualified_symbol} ASC")
            if limit is not None:
                query.append("LIMIT ?")
                params.append(limit)
            with self._connect() as conn:
                rows = conn.execute(" ".join(query), params).fetchall()
            return [_build_ticker(row) for row in rows]

        def _fetch_stale(limit: Optional[int], cutoff: str) -> List[SupportedTicker]:
            params: List[object] = [provider_id, cutoff]
            query = [
                f"SELECT {select_columns}",
                "FROM fundamentals_raw fr",
                "JOIN provider_listing pl "
                "ON pl.provider_listing_id = fr.provider_listing_id",
                "JOIN provider_exchange px "
                "ON px.provider_exchange_id = pl.provider_exchange_id",
                "LEFT JOIN fundamentals_fetch_state fs "
                "ON fs.provider_listing_id = fr.provider_listing_id",
                "WHERE px.provider_id = ?",
                "AND fr.last_fetched_at <= ?",
            ]
            _apply_scope_filters(query, params)
            if respect_backoff:
                query.append(
                    "AND (fs.next_eligible_at IS NULL OR fs.next_eligible_at <= ?)"
                )
                params.append(now.isoformat())
            query.append(f"ORDER BY fr.last_fetched_at ASC, {qualified_symbol} ASC")
            if limit is not None:
                query.append("LIMIT ?")
                params.append(limit)
            with self._connect() as conn:
                rows = conn.execute(" ".join(query), params).fetchall()
            return [_build_ticker(row) for row in rows]

        if max_age_days is None and not missing_only:
            params: List[object] = [provider_id]
            query = [
                f"SELECT {select_columns}",
                "FROM provider_listing pl",
                "JOIN provider_exchange px "
                "ON px.provider_exchange_id = pl.provider_exchange_id",
                "LEFT JOIN fundamentals_fetch_state fs "
                "ON fs.provider_listing_id = pl.provider_listing_id",
                "WHERE px.provider_id = ?",
            ]
            _apply_scope_filters(query, params)
            if respect_backoff:
                query.append(
                    "AND (fs.next_eligible_at IS NULL OR fs.next_eligible_at <= ?)"
                )
                params.append(now.isoformat())
            query.append(f"ORDER BY {qualified_symbol} ASC")
            if max_symbols is not None:
                query.append("LIMIT ?")
                params.append(max_symbols)
            with self._connect() as conn:
                rows = conn.execute(" ".join(query), params).fetchall()
            return [_build_ticker(row) for row in rows]

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
                "SUM(CASE WHEN fr.last_fetched_at IS NOT NULL AND fr.last_fetched_at <= ? "
                "THEN 1 ELSE 0 END)"
            )
            params.append(cutoff)
        params.extend([now, provider_norm])
        normalized_codes = _normalized_codes(exchange_codes)
        query = [
            "SELECT",
            "catalog.provider_exchange_code AS exchange_code,",
            "COUNT(*) AS total_supported,",
            "SUM(CASE WHEN fr.provider_listing_id IS NOT NULL THEN 1 ELSE 0 END) AS stored,",
            "SUM(CASE WHEN fr.provider_listing_id IS NULL THEN 1 ELSE 0 END) AS missing,",
            f"{stale_expr} AS stale,",
            "SUM(CASE WHEN fs.next_eligible_at IS NOT NULL AND fs.next_eligible_at > ? THEN 1 ELSE 0 END) AS blocked,",
            "SUM(CASE WHEN fs.provider_listing_id IS NOT NULL THEN 1 ELSE 0 END) AS error_rows",
            "FROM provider_listing_catalog catalog",
            "LEFT JOIN fundamentals_raw fr "
            "ON fr.provider_listing_id = catalog.provider_listing_id",
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
            "fs.error AS last_error, fs.next_eligible_at, fs.attempts",
            "FROM provider_listing_catalog catalog",
            "JOIN fundamentals_fetch_state fs "
            "ON fs.provider_listing_id = catalog.provider_listing_id",
            "WHERE catalog.provider = ?",
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
                last_status="error",
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
        """Return supported tickers eligible for a market-data refresh.

        Mirrors :meth:`list_eligible_for_fundamentals`: the provider is resolved
        once so the eligibility query reads straight from the base tables
        (``provider_listing`` + ``provider_exchange`` + ``listing``) instead of
        the six-table ``provider_listing_catalog`` view. ``security_id`` is just
        ``provider_listing.listing_id`` and ``currency`` comes from
        ``listing.currency`` (NOT NULL); the issuer/exchange joins the view adds
        only feed metadata this command never reads, so they fall away.

        Freshness is the latest stored ``market_data.as_of`` per listing,
        computed by a correlated ``(SELECT MAX(as_of) ... WHERE listing_id = ?)``
        probe served backwards by the ``market_data`` PK ``(listing_id, as_of)``
        -- one index seek per scoped row. The probe lives in a
        ``WITH ... AS MATERIALIZED`` CTE so SQLite evaluates it exactly once per
        row: without the materialisation barrier the planner flattens the
        subquery and re-runs the probe for every reference in the outer
        ``WHERE`` / ``ORDER BY``.
        """

        self.initialize_schema()
        provider_norm = provider.strip().upper()
        # Resolve the provider once so the CTE can filter on
        # provider_exchange.provider_id and avoid the catalog view entirely.
        provider_id = self._provider_repo().resolve_id(provider_norm)
        if provider_id is None:
            return []
        now = datetime.now(timezone.utc)
        cutoff = (now.date() - timedelta(days=max_age_days)).isoformat()

        # Rebuild the catalog's qualified provider_symbol from base columns so
        # scope filtering and ordering stay identical to the view. SEC symbols
        # are implicitly US-listed; every other provider qualifies by its
        # provider exchange code.
        if provider_norm == "SEC":
            qualified_symbol = "(pl.provider_symbol || '.US')"
        else:
            qualified_symbol = (
                "(pl.provider_symbol || '.' || px.provider_exchange_code)"
            )

        params: List[object] = [provider_id]
        inner = [
            "SELECT",
            "px.provider_exchange_code AS provider_exchange_code,",
            f"{qualified_symbol} AS provider_symbol,",
            "pl.provider_symbol AS provider_ticker,",
            "pl.listing_id AS security_id,",
            "l.currency AS currency,",
            "(SELECT MAX(as_of) FROM market_data "
            "WHERE listing_id = pl.listing_id) AS latest_as_of,",
            "ms.next_eligible_at AS next_eligible_at",
            "FROM provider_listing pl",
            "JOIN provider_exchange px "
            "ON px.provider_exchange_id = pl.provider_exchange_id",
            "JOIN listing l ON l.listing_id = pl.listing_id",
            "LEFT JOIN market_data_fetch_state ms "
            "ON ms.provider_listing_id = pl.provider_listing_id",
            "WHERE px.provider_id = ?",
        ]
        if primary_only:
            # primary_listing_status lives on the listing, so the secondary
            # exclusion needs no view (matches primary_provider_listing_catalog).
            inner.append("AND l.primary_listing_status != 'secondary'")
        _apply_provider_listing_scope(
            inner,
            params,
            exchange_codes,
            provider_symbols,
            symbol_expr=qualified_symbol,
        )

        outer = [
            "WITH eligible AS MATERIALIZED (",
            " ".join(inner),
            ")",
            "SELECT provider_exchange_code, provider_symbol, provider_ticker, "
            "security_id, currency",
            "FROM eligible",
            "WHERE (latest_as_of IS NULL OR latest_as_of <= ?)",
        ]
        params.append(cutoff)
        if respect_backoff:
            outer.append("AND (next_eligible_at IS NULL OR next_eligible_at <= ?)")
            params.append(now.isoformat())
        outer.append(
            "ORDER BY CASE WHEN latest_as_of IS NULL THEN 0 ELSE 1 END, "
            "latest_as_of ASC, "
            "provider_exchange_code ASC, provider_symbol ASC"
        )
        if max_symbols is not None:
            outer.append("LIMIT ?")
            params.append(max_symbols)
        with self._connect() as conn:
            rows = conn.execute(" ".join(outer), params).fetchall()
        # Project only the columns the market-data command consumes; the other
        # SupportedTicker fields are catalog metadata it never reads.
        return [
            SupportedTicker(
                provider=provider_norm,
                provider_exchange_code=str(row["provider_exchange_code"]),
                provider_symbol=str(row["provider_symbol"]),
                provider_ticker=str(row["provider_ticker"]),
                security_id=int(row["security_id"]),
                currency=str(row["currency"]),
            )
            for row in rows
        ]

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
        catalog_view = _provider_listing_catalog_view(primary_only=primary_only)
        query = [
            "SELECT",
            "catalog.provider_exchange_code AS exchange_code,",
            "COUNT(*) AS total_supported,",
            "SUM(CASE WHEN md.latest_as_of IS NOT NULL THEN 1 ELSE 0 END) AS stored,",
            "SUM(CASE WHEN md.latest_as_of IS NULL THEN 1 ELSE 0 END) AS missing,",
            "SUM(CASE WHEN md.latest_as_of IS NOT NULL AND md.latest_as_of <= ? THEN 1 ELSE 0 END) AS stale,",
            "SUM(CASE WHEN ms.next_eligible_at IS NOT NULL AND ms.next_eligible_at > ? THEN 1 ELSE 0 END) AS blocked,",
            "SUM(CASE WHEN ms.last_status = 'error' THEN 1 ELSE 0 END) AS error_rows",
            f"FROM {catalog_view} catalog",
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
        catalog_view = _provider_listing_catalog_view(primary_only=primary_only)
        query = [
            "SELECT catalog.provider_symbol AS symbol, catalog.provider_exchange_code AS exchange_code,",
            "ms.last_status, ms.last_error, ms.next_eligible_at, ms.attempts",
            f"FROM {catalog_view} catalog",
            "JOIN market_data_fetch_state ms "
            "ON ms.provider_listing_id = catalog.provider_listing_id",
            "WHERE catalog.provider = ? AND ms.last_status = 'error'",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            query.append(f"AND catalog.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
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
