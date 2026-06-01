"""Supported-ticker (provider listing) repository.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import sqlite3
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
)

from pyvalue.currency import (
    raw_currency_code,
)
from pyvalue.universe import Listing

from .base import (
    SQLiteStore,
    _batched,
    _coerce_int,
    _normalize_optional_text,
    _normalize_provider_identity,
    _normalize_symbol_base,
    _normalized_codes,
    _provider_listing_catalog_view,
    _utc_now_iso,
)
from .records import (
    IngestProgressExchange,
    IngestProgressFailure,
    IngestProgressSummary,
    SupportedTicker,
    SupportedTickerRefreshResult,
)
from ..migrations import apply_migrations
from .fundamentals import FundamentalsRepository
from .fetch_state import (
    FundamentalsFetchStateRepository,
    MarketDataFetchStateRepository,
)
from .metrics_market import MarketDataRepository


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
        # migration 066 enforces NOT NULL on name and country. Use the
        # provider exchange code as a name fallback and 'Unknown' as a
        # country fallback when the caller doesn't supply them. The
        # ON CONFLICT branch only updates these when a non-NULL/non-empty
        # value comes in, so a richer subsequent refresh can promote the
        # placeholder.
        name_value = _normalize_optional_text(name) or provider_exchange_code
        country_value = _normalize_optional_text(country) or "Unknown"
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
                name = CASE
                    WHEN excluded.name IS NOT NULL AND excluded.name != ''
                         AND excluded.name != provider_exchange.provider_exchange_code
                    THEN excluded.name
                    ELSE provider_exchange.name
                END,
                country = CASE
                    WHEN excluded.country IS NOT NULL AND excluded.country != ''
                         AND excluded.country != 'Unknown'
                    THEN excluded.country
                    ELSE provider_exchange.country
                END,
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
                name_value,
                country_value,
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
    ) -> Optional[sqlite3.Row]:
        # Listings must carry a currency (listing.currency is NOT NULL and there
        # is no fallback). A catalog entry whose payload omits the currency is
        # skipped entirely -- neither the listing nor the provider_listing row
        # is created.
        quote_currency = raw_currency_code(currency)
        if quote_currency is None:
            return None
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
            currency=quote_currency,
            connection=conn,
        )
        # Keep listing.currency in sync when the listing pre-existed with a
        # different currency.
        conn.execute(
            """
            UPDATE listing
            SET currency = ?
            WHERE listing_id = ?
            """,
            (quote_currency, security.security_id),
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
            (
                int(provider_exchange_row["provider_exchange_id"]),
                bare_symbol,
                security.security_id,
            ),
        )
        # The only column callers consume from the returned Row is
        # provider_listing_id. Querying the full row via SELECT * was a
        # CLAUDE.md violation (audit P2 #6). Materialise the post-insert
        # provider_listing_id with an explicit projection.
        row = conn.execute(
            """
            SELECT provider_listing_id
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
    ) -> SupportedTickerRefreshResult:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        provider_exchange_code = exchange_code.strip().upper()
        retained_tickers: List[str] = []
        skipped_no_currency: List[str] = []
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
                created = self._ensure_provider_listing(
                    conn,
                    provider_norm,
                    symbol,
                    exchange_code=provider_exchange_code,
                    currency=listing.currency,
                    entity_name=listing.security_name,
                )
                if created is None:
                    skipped_no_currency.append(bare_symbol)
                    continue
                retained_tickers.append(bare_symbol)
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
        return SupportedTickerRefreshResult(
            inserted=len(retained_tickers),
            skipped_no_currency=tuple(skipped_no_currency),
        )

    def replace_for_exchange(
        self,
        provider: str,
        exchange_code: str,
        rows: Sequence[Dict[str, Any]],
    ) -> SupportedTickerRefreshResult:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        provider_exchange_code = exchange_code.strip().upper()
        retained_tickers: List[str] = []
        skipped_no_currency: List[str] = []
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
                created = self._ensure_provider_listing(
                    conn,
                    provider_norm,
                    bare_symbol,
                    exchange_code=provider_exchange_code,
                    currency=row.get("Currency") or row.get("currency"),
                    entity_name=row.get("Name") or row.get("name"),
                )
                if created is None:
                    skipped_no_currency.append(bare_symbol)
                    continue
                retained_tickers.append(bare_symbol)
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
        return SupportedTickerRefreshResult(
            inserted=len(retained_tickers),
            skipped_no_currency=tuple(skipped_no_currency),
        )

    def fetch_for_symbol(self, provider: str, symbol: str) -> Optional[SupportedTicker]:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        symbol_norm = symbol.strip().upper()
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT {self._catalog_select_columns()}
                FROM provider_listing_catalog catalog
                WHERE catalog.provider = ? AND catalog.provider_symbol = ?
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
        catalog_view = _provider_listing_catalog_view(primary_only=primary_only)
        query = [
            f"SELECT {self._catalog_select_columns('catalog')}",
            f"FROM {catalog_view} catalog",
        ]
        query.append("WHERE catalog.provider = ?")
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
            query.append("WHERE provider = ?")
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
                query.append("AND provider = ?")
                params.append(provider.strip().upper())
            if exchange_code:
                query.append("AND provider_exchange_code = ?")
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
                WHERE provider = ? AND provider_symbol IN ({placeholders})
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
        """Return the stored listing quote currency from catalog metadata."""

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
            query.append("AND catalog.provider = ?")
        params.extend([symbol_norm, symbol_norm])
        # The composite expression on l.symbol || '.' || e.exchange_code
        # cannot use an index regardless of UPPER(); the underlying columns
        # are already normalised via CHECK / Python-side .upper(), so the
        # UPPER() wrapper is dead weight.
        query.append(
            "AND (catalog.provider_symbol = ? OR l.symbol || '.' || e.exchange_code = ?)"
        )
        query.append(
            "ORDER BY CASE WHEN catalog.provider = 'EODHD' THEN 0 WHEN catalog.provider = 'SEC' THEN 1 ELSE 2 END"
        )
        query.append("LIMIT 1")
        with self._connect() as conn:
            row = conn.execute(" ".join(query), params).fetchone()
        return raw_currency_code(row[0]) if row else None

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
                "LEFT JOIN fundamentals_raw fr "
                "ON fr.provider_listing_id = catalog.provider_listing_id",
                "LEFT JOIN fundamentals_fetch_state fs "
                "ON fs.provider_listing_id = catalog.provider_listing_id",
                "WHERE catalog.provider = ?",
                "AND fr.provider_listing_id IS NULL",
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
                "FROM fundamentals_raw fr",
                "JOIN provider_listing_catalog catalog "
                "ON catalog.provider_listing_id = fr.provider_listing_id",
                "LEFT JOIN fundamentals_fetch_state fs "
                "ON fs.provider_listing_id = fr.provider_listing_id",
                "WHERE catalog.provider = ?",
                "AND fr.last_fetched_at <= ?",
            ]
            _apply_scope_filters(query, params)
            if respect_backoff:
                query.append(
                    "AND (fs.next_eligible_at IS NULL OR fs.next_eligible_at <= ?)"
                )
                params.append(now.isoformat())
            query.append("ORDER BY fr.last_fetched_at ASC, catalog.provider_symbol ASC")
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

        Audit P3 #14: the previous form materialised
        ``SELECT listing_id, MAX(as_of) FROM market_data GROUP BY listing_id``
        as a derived subquery and joined the result against the catalog,
        which forced a full scan of ``market_data`` (~6.8M rows on the
        live DB) regardless of how narrow the provider/exchange/symbol
        scope was.

        The query now scopes the catalog *first* and uses a correlated
        ``(SELECT MAX(as_of) FROM market_data WHERE listing_id = ?)``
        probe per scoped row. The probe is served by the
        ``market_data`` PK ``(listing_id, as_of)`` — SQLite traverses
        the index backwards for the ``MAX``, so each probe is a single
        index seek and total work scales with the scoped catalog size
        rather than the full ``market_data`` row count. The probe is
        computed once via an inline subquery and then both filtered on
        and ordered by in the outer ``WHERE`` / ``ORDER BY``, so the
        planner doesn't run it multiple times per row.
        """

        self.initialize_schema()
        provider_norm = provider.strip().upper()
        now = datetime.now(timezone.utc)
        cutoff = (now.date() - timedelta(days=max_age_days)).isoformat()
        params: List[object] = [provider_norm]
        catalog_view = _provider_listing_catalog_view(primary_only=primary_only)
        inner = [
            f"SELECT {self._catalog_select_columns('catalog')},",
            "(SELECT MAX(as_of) FROM market_data "
            "WHERE listing_id = catalog.security_id) AS latest_as_of,",
            "ms.next_eligible_at AS next_eligible_at",
            f"FROM {catalog_view} catalog",
            "LEFT JOIN market_data_fetch_state ms "
            "ON ms.provider_listing_id = catalog.provider_listing_id",
            "WHERE catalog.provider = ?",
        ]
        normalized_codes = _normalized_codes(exchange_codes)
        if normalized_codes:
            placeholders = ", ".join("?" for _ in normalized_codes)
            inner.append(f"AND catalog.provider_exchange_code IN ({placeholders})")
            params.extend(normalized_codes)
        normalized_symbols = _normalized_codes(provider_symbols)
        if normalized_symbols:
            placeholders = ", ".join("?" for _ in normalized_symbols)
            inner.append(f"AND catalog.provider_symbol IN ({placeholders})")
            params.extend(normalized_symbols)
        select_columns = ", ".join(
            f"sub.{col.split('.', 1)[1]}"
            for col in self._catalog_select_columns("catalog").split(", ")
        )
        outer = [
            f"SELECT {select_columns}",
            "FROM (" + " ".join(inner) + ") AS sub",
            "WHERE (sub.latest_as_of IS NULL OR sub.latest_as_of <= ?)",
        ]
        params.append(cutoff)
        if respect_backoff:
            outer.append(
                "AND (sub.next_eligible_at IS NULL OR sub.next_eligible_at <= ?)"
            )
            params.append(now.isoformat())
        outer.append(
            "ORDER BY CASE WHEN sub.latest_as_of IS NULL THEN 0 ELSE 1 END, "
            "sub.latest_as_of ASC, "
            "sub.provider_exchange_code ASC, sub.provider_symbol ASC"
        )
        if max_symbols is not None:
            outer.append("LIMIT ?")
            params.append(max_symbols)
        with self._connect() as conn:
            rows = conn.execute(" ".join(outer), params).fetchall()
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
        catalog_view = _provider_listing_catalog_view(primary_only=primary_only)
        query = [
            "SELECT",
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
