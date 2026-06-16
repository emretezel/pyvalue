"""Fundamentals and market-data fetch-state repositories.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import sqlite3
from typing import (
    Dict,
    Optional,
    Sequence,
    Tuple,
)


from .base import (
    SQLITE_MAX_BOUND_PARAMETERS,
    SQLiteStore,
    _batched,
    _normalized_codes,
    _utc_now_iso,
)
from .migrations import apply_migrations


def _resolve_provider_listing_ids(
    conn: sqlite3.Connection,
    provider: str,
    symbols: Sequence[str],
) -> Dict[str, int]:
    """Resolve qualified provider symbols to ``provider_listing_id`` in bulk.

    Reads the base tables (``provider_listing`` + ``provider_exchange`` +
    ``provider``) by the ``provider_listing`` natural key rather than the
    ``provider_listing_catalog`` view. The view exposes ``provider_symbol`` only
    as the computed expression ``pl.provider_symbol || '.' || provider_exchange``
    (``|| '.US'`` for SEC), which no index can serve -- so a per-symbol view
    lookup scans the whole catalog. Resolving an entire batch here keeps the hot
    write path (fetch-state upserts after every flush) to a single
    covering-index scan per batch instead of one full catalog scan per symbol.

    Returns a ``{qualified_symbol: provider_listing_id}`` map. Symbols with no
    matching listing are simply absent so callers can skip them. Inputs are
    normalised (upper-cased/de-duplicated) the same way the writers normalise
    their symbol lists, so the keys line up with the caller's symbols.
    """
    normalized = _normalized_codes(symbols)
    if not normalized:
        return {}
    provider_norm = provider.strip().upper()
    # Rebuild the catalog's qualified provider_symbol from base columns so the
    # match is byte-identical to the view. SEC symbols are implicitly US-listed;
    # every other provider qualifies by its provider exchange code.
    if provider_norm == "SEC":
        qualified = "(pl.provider_symbol || '.US')"
    else:
        qualified = "(pl.provider_symbol || '.' || px.provider_exchange_code)"
    resolved: Dict[str, int] = {}
    # One bound slot is spent on provider_code; the rest carry the IN list.
    for chunk in _batched(normalized, SQLITE_MAX_BOUND_PARAMETERS - 1):
        placeholders = ", ".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT {qualified} AS qualified_symbol, pl.provider_listing_id
            FROM provider_listing pl
            JOIN provider_exchange px
              ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN provider p ON p.provider_id = px.provider_id
            WHERE p.provider_code = ?
              AND {qualified} IN ({placeholders})
            """,
            [provider_norm, *chunk],
        ).fetchall()
        for row in rows:
            resolved[str(row["qualified_symbol"])] = int(row["provider_listing_id"])
    return resolved


class _FetchStateRepository(SQLiteStore):
    table_name: str

    def initialize_schema(self) -> None:
        # The table and its FK to provider_listing are owned by
        # migrations 040+ (see migration 053 for the runtime-column
        # cleanup, migration 067 for the next_eligible_at index drop).
        # All provider/symbol resolution is done by
        # ``_resolve_provider_listing_ids`` against the base tables; this
        # repository writes directly against the ``provider_listing_id`` PK.
        apply_migrations(self.db_path)

    def _resolve_provider_listing_id(
        self,
        conn: sqlite3.Connection,
        provider: str,
        symbol: str,
    ) -> Optional[int]:
        # Single-symbol convenience over the bulk resolver so every
        # provider_listing lookup shares one base-table query path and never the
        # catalog view's un-indexable computed provider_symbol.
        return _resolve_provider_listing_ids(conn, provider, [symbol]).get(
            symbol.strip().upper()
        )

    def fetch(
        self, provider: str, symbol: str
    ) -> Optional[Dict[str, Optional[str] | int]]:
        self.initialize_schema()
        with self._connect() as conn:
            provider_listing_id = self._resolve_provider_listing_id(
                conn, provider, symbol
            )
            if provider_listing_id is None:
                return None
            row = conn.execute(
                f"""
                SELECT last_fetched_at, last_status, last_error, next_eligible_at, attempts
                FROM {self.table_name}
                WHERE provider_listing_id = ?
                """,
                (provider_listing_id,),
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
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, 'ok', NULL, NULL, 0)
                ON CONFLICT(provider_listing_id) DO UPDATE SET
                    last_fetched_at = excluded.last_fetched_at,
                    last_status = 'ok',
                    last_error = NULL,
                    next_eligible_at = NULL,
                    attempts = 0
                """,
                (provider_listing_id, timestamp),
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
            id_by_symbol = _resolve_provider_listing_ids(
                conn, provider_norm, normalized
            )
            rows = [
                (id_by_symbol[symbol], timestamp)
                for symbol in normalized
                if symbol in id_by_symbol
            ]
            conn.executemany(
                f"""
                INSERT INTO {self.table_name} (
                    provider_listing_id,
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, 'ok', NULL, NULL, 0)
                ON CONFLICT(provider_listing_id) DO UPDATE SET
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
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, 'error', ?, ?, ?)
                ON CONFLICT(provider_listing_id) DO UPDATE SET
                    last_fetched_at = COALESCE(excluded.last_fetched_at, {self.table_name}.last_fetched_at),
                    last_status = 'error',
                    last_error = excluded.last_error,
                    next_eligible_at = excluded.next_eligible_at,
                    attempts = excluded.attempts
                """,
                (
                    provider_listing_id,
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
        # Resolve provider_listing_id for every error symbol in one bulk query,
        # then fetch the existing attempt counter / last_fetched_at by PK so the
        # upsert rows are built from the same mapping. Symbols that don't resolve
        # are skipped -- there is nothing to write state for.
        with self._connect() as conn:
            listing_id_by_symbol = _resolve_provider_listing_ids(
                conn,
                provider_norm,
                [symbol for symbol, _error in normalized_errors],
            )
            if not listing_id_by_symbol:
                return

            state_by_id: Dict[int, Dict[str, Optional[str] | int]] = {}
            for chunk in _batched(list(listing_id_by_symbol.values()), 500):
                placeholders = ", ".join("?" for _ in chunk)
                rows_state = conn.execute(
                    f"""
                    SELECT provider_listing_id, last_fetched_at, attempts
                    FROM {self.table_name}
                    WHERE provider_listing_id IN ({placeholders})
                    """,
                    list(chunk),
                ).fetchall()
                for row in rows_state:
                    state_by_id[int(row["provider_listing_id"])] = {
                        "last_fetched_at": row["last_fetched_at"],
                        "attempts": row["attempts"],
                    }

            now = datetime.now(timezone.utc)
            rows = []
            for symbol, error in normalized_errors:
                provider_listing_id = listing_id_by_symbol.get(symbol)
                if provider_listing_id is None:
                    continue
                state = state_by_id.get(provider_listing_id)
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
                    last_fetched_at,
                    last_status,
                    last_error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, 'error', ?, ?, ?)
                ON CONFLICT(provider_listing_id) DO UPDATE SET
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
        provider_norm = provider.strip().upper()
        with self._connect() as conn:
            provider_listing_ids = list(
                _resolve_provider_listing_ids(conn, provider_norm, normalized).values()
            )
            if not provider_listing_ids:
                return 0
            placeholders = ", ".join("?" for _ in provider_listing_ids)
            cursor = conn.execute(
                f"""
                DELETE FROM {self.table_name}
                WHERE provider_listing_id IN ({placeholders})
                """,
                provider_listing_ids,
            )
        return int(cursor.rowcount or 0)


class FundamentalsFetchStateRepository(SQLiteStore):
    """Track active fundamentals fetch failures for resumable ingestion."""

    def initialize_schema(self) -> None:
        # `fundamentals_fetch_state` is owned by migration 040, which
        # also dropped the legacy provider-keyed indexes that the
        # runtime path used to reset.
        apply_migrations(self.db_path)

    def _resolve_provider_listing_id(
        self,
        conn: sqlite3.Connection,
        provider: str,
        symbol: str,
    ) -> Optional[int]:
        # Single-symbol convenience over the bulk resolver so every
        # provider_listing lookup shares one base-table query path and never the
        # catalog view's un-indexable computed provider_symbol.
        return _resolve_provider_listing_ids(conn, provider, [symbol]).get(
            symbol.strip().upper()
        )

    def fetch(
        self, provider: str, symbol: str
    ) -> Optional[Dict[str, Optional[str] | int]]:
        self.initialize_schema()
        with self._connect() as conn:
            provider_listing_id = self._resolve_provider_listing_id(
                conn, provider, symbol
            )
            if provider_listing_id is None:
                return None
            row = conn.execute(
                """
                SELECT failed_at, error, next_eligible_at, attempts
                FROM fundamentals_fetch_state
                WHERE provider_listing_id = ?
                """,
                (provider_listing_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "failed_at": row["failed_at"],
            "error": row["error"],
            "last_status": "error",
            "last_error": row["error"],
            "next_eligible_at": row["next_eligible_at"],
            "attempts": row["attempts"],
        }

    def mark_success(
        self,
        provider: str,
        symbol: str,
        fetched_at: Optional[str] = None,
    ) -> None:
        del fetched_at
        self.mark_success_many(provider, [symbol])

    def mark_success_many(
        self,
        provider: str,
        symbols: Sequence[str],
        fetched_at: Optional[str] = None,
    ) -> None:
        del fetched_at
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return
        provider_norm = provider.strip().upper()
        with self._connect() as conn:
            provider_listing_ids = list(
                _resolve_provider_listing_ids(conn, provider_norm, normalized).values()
            )
            if not provider_listing_ids:
                return
            placeholders = ", ".join("?" for _ in provider_listing_ids)
            conn.execute(
                f"""
                DELETE FROM fundamentals_fetch_state
                WHERE provider_listing_id IN ({placeholders})
                """,
                provider_listing_ids,
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
        with self._connect() as conn:
            provider_listing_id = self._resolve_provider_listing_id(
                conn, provider, symbol
            )
            if provider_listing_id is None:
                return
            row = conn.execute(
                """
                SELECT attempts
                FROM fundamentals_fetch_state
                WHERE provider_listing_id = ?
                """,
                (provider_listing_id,),
            ).fetchone()
            attempts = int(row["attempts"]) if row else 0
            attempts += 1
            backoff = min(
                base_backoff_seconds * (2 ** (attempts - 1)),
                max_backoff_seconds,
            )
            failed_at = datetime.now(timezone.utc)
            next_eligible_at = (failed_at + timedelta(seconds=backoff)).isoformat()
            conn.execute(
                """
                INSERT INTO fundamentals_fetch_state (
                    provider_listing_id,
                    failed_at,
                    error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(provider_listing_id) DO UPDATE SET
                    failed_at = excluded.failed_at,
                    error = excluded.error,
                    next_eligible_at = excluded.next_eligible_at,
                    attempts = excluded.attempts
                """,
                (
                    provider_listing_id,
                    failed_at.isoformat(),
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
        now = datetime.now(timezone.utc)
        # Resolve every error symbol in one bulk query, then read existing
        # attempt counters by PK in batches so the upsert rows are built without
        # a per-symbol round-trip. Symbols that don't resolve are skipped.
        with self._connect() as conn:
            listing_id_by_symbol = _resolve_provider_listing_ids(
                conn,
                provider_norm,
                [symbol for symbol, _error in normalized_errors],
            )
            if not listing_id_by_symbol:
                return

            attempts_by_id: Dict[int, int] = {}
            for chunk in _batched(list(listing_id_by_symbol.values()), 500):
                placeholders = ", ".join("?" for _ in chunk)
                rows_state = conn.execute(
                    f"""
                    SELECT provider_listing_id, attempts
                    FROM fundamentals_fetch_state
                    WHERE provider_listing_id IN ({placeholders})
                    """,
                    list(chunk),
                ).fetchall()
                for row in rows_state:
                    attempts_by_id[int(row["provider_listing_id"])] = int(
                        row["attempts"]
                    )

            rows = []
            for symbol, error in normalized_errors:
                provider_listing_id = listing_id_by_symbol.get(symbol)
                if provider_listing_id is None:
                    continue
                attempts = attempts_by_id.get(provider_listing_id, 0) + 1
                backoff = min(
                    base_backoff_seconds * (2 ** (attempts - 1)),
                    max_backoff_seconds,
                )
                rows.append(
                    (
                        provider_listing_id,
                        now.isoformat(),
                        error,
                        (now + timedelta(seconds=backoff)).isoformat(),
                        attempts,
                    )
                )
            conn.executemany(
                """
                INSERT INTO fundamentals_fetch_state (
                    provider_listing_id,
                    failed_at,
                    error,
                    next_eligible_at,
                    attempts
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(provider_listing_id) DO UPDATE SET
                    failed_at = excluded.failed_at,
                    error = excluded.error,
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
        provider_norm = provider.strip().upper()
        with self._connect() as conn:
            provider_listing_ids = list(
                _resolve_provider_listing_ids(conn, provider_norm, normalized).values()
            )
            if not provider_listing_ids:
                return 0
            placeholders = ", ".join("?" for _ in provider_listing_ids)
            cursor = conn.execute(
                f"""
                DELETE FROM fundamentals_fetch_state
                WHERE provider_listing_id IN ({placeholders})
                """,
                provider_listing_ids,
            )
        return int(cursor.rowcount or 0)


class MarketDataFetchStateRepository(_FetchStateRepository):
    """Track market-data fetch status for resumable ingestion."""

    table_name = "market_data_fetch_state"
