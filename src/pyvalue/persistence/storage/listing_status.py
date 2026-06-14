"""Security listing-status (primary/secondary) repository.

Author: Emre Tezel
"""

from __future__ import annotations

import json
import sqlite3
from typing import (
    Any,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
)


from .base import (
    SQLiteStore,
    _LISTING_STATUS_PRIMARY,
    _LISTING_STATUS_SECONDARY,
    _PRIMARY_LISTING_SOURCE_PROVIDER,
    _batched,
    _normalize_qualified_symbol,
    _normalized_codes,
    _utc_now_iso,
)
from .records import (
    FundamentalsUpdate,
    SecurityListingStatusRecord,
)
from ..migrations import apply_migrations
from .financial_facts import (
    FinancialFactsRefreshStateRepository,
    FinancialFactsRepository,
)
from .fetch_state import MarketDataFetchStateRepository
from .metrics_market import (
    MarketDataRepository,
    MetricComputeStatusRepository,
    MetricsRepository,
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
                    raw_fetched_at=update.last_fetched_at,
                    payload=payload if isinstance(payload, Mapping) else {},
                )
            )
        self.upsert_many(records, connection=connection)
        return records

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
                "SELECT catalog.security_id, catalog.provider_symbol, fr.last_fetched_at, fr.data",
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
                    raw_fetched_at=str(row["last_fetched_at"]),
                    payload=payload if isinstance(payload, Mapping) else {},
                )
            )
        self.upsert_many(records)
        return records

    def purge_downstream_for_secondary(
        self,
        records: Sequence[SecurityListingStatusRecord],
    ) -> List[SecurityListingStatusRecord]:
        """Purge downstream data for every record now classified secondary.

        The invariant "a secondary listing owns no facts/metrics/market-data"
        is maintained eagerly by whoever writes the listing status, so both
        ``ingest-fundamentals`` and ``reconcile-listing-status`` route their
        secondary reclassifications through this one method instead of
        duplicating the filter-and-purge step.

        Returns the secondary records that were purged (empty when none), so the
        caller can report how many listings were reclassified.
        """
        secondary = [record for record in records if not record.is_primary_listing]
        if secondary:
            self.purge_secondary_security_data(
                security_ids=[record.security_id for record in secondary],
                provider_symbols=[record.provider_symbol for record in secondary],
            )
        return secondary

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
        # Deferred import: ``fundamentals`` imports ``SecurityListingStatusRepository``
        # from this module at top level, so importing
        # ``FundamentalsNormalizationStateRepository`` at the top of this module
        # would form a fundamentals <-> listing_status import cycle. A local
        # import inside the method breaks that one edge.
        from .fundamentals import FundamentalsNormalizationStateRepository

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
                        WHERE provider_listing_id IN (
                            SELECT catalog.provider_listing_id
                            FROM provider_listing_catalog catalog
                            WHERE catalog.provider = ?
                              AND catalog.provider_symbol IN ({placeholders})
                        )
                        """,
                        [provider_norm, *symbol_chunk],
                    )
                    conn.execute(
                        f"""
                        DELETE FROM market_data_fetch_state
                        WHERE provider_listing_id IN (
                            SELECT catalog.provider_listing_id
                            FROM provider_listing_catalog catalog
                            WHERE catalog.provider = ?
                              AND catalog.provider_symbol IN ({placeholders})
                        )
                        """,
                        [provider_norm, *symbol_chunk],
                    )
