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
from .migrations import apply_migrations
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
        primary_ticker: Optional[str],
    ) -> SecurityListingStatusRecord:
        """Classify one listing as primary/secondary from its EODHD PrimaryTicker.

        ``primary_ticker`` is the raw ``General.PrimaryTicker`` value already
        extracted from the stored payload -- by SQL ``json_extract`` on the
        reconcile path, or by dict access on the ingest path. Extraction lives in
        the callers so reconcile never has to load the full ~228 KB payload per
        listing just to read this one field; the classification rule lives here,
        in one place, regardless of where the ticker came from.
        """
        provider_symbol_norm = _normalize_qualified_symbol(provider_symbol)
        if provider_symbol_norm is None:
            raise ValueError(f"provider_symbol must be qualified: {provider_symbol}")

        primary_provider_symbol = _normalize_qualified_symbol(primary_ticker)
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
            # The ingest path already holds the parsed payload in memory, so it
            # reads General.PrimaryTicker directly (the reconcile path extracts
            # the same field in SQL); both feed the shared classifier below.
            general = payload.get("General") if isinstance(payload, Mapping) else None
            primary_ticker = (
                general.get("PrimaryTicker") if isinstance(general, Mapping) else None
            )
            records.append(
                self._build_status_record(
                    security_id=update.security_id,
                    provider_symbol=update.provider_symbol,
                    raw_fetched_at=update.last_fetched_at,
                    primary_ticker=primary_ticker,
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

        # Query the base catalog tables directly instead of the 6-table
        # ``provider_listing_catalog`` view. Reconcile only needs the listing id,
        # the composed provider symbol, and ``General.PrimaryTicker``; the view's
        # ``listing``/``issuer``/``exchange`` joins contribute no consumed column
        # (FKs guarantee those inner joins never drop rows), and the view's SEC
        # ``||'.US'`` branch never applies because reconcile is EODHD-only.
        # ``PrimaryTicker`` is pulled with ``json_extract`` so each ~228 KB raw
        # payload is parsed inside SQLite and never crosses into Python.
        def _select_rows(
            conn: sqlite3.Connection,
            *,
            symbols_chunk: Optional[Sequence[str]] = None,
            security_chunk: Optional[Sequence[int]] = None,
        ) -> sqlite3.Cursor:
            params: List[Any] = [provider_norm]
            query = [
                "SELECT pl.listing_id AS security_id,",
                "  pl.provider_symbol || '.' || px.provider_exchange_code"
                " AS provider_symbol,",
                "  fr.last_fetched_at AS last_fetched_at,",
                "  json_extract(fr.data, '$.General.PrimaryTicker') AS primary_ticker",
                "FROM fundamentals_raw fr",
                "JOIN provider_listing pl"
                "  ON pl.provider_listing_id = fr.provider_listing_id",
                "JOIN provider_exchange px"
                "  ON px.provider_exchange_id = pl.provider_exchange_id",
                "JOIN provider p ON p.provider_id = px.provider_id",
                "WHERE p.provider_code = ?",
            ]
            if normalized_exchanges:
                placeholders = ", ".join("?" for _ in normalized_exchanges)
                query.append(f"AND px.provider_exchange_code IN ({placeholders})")
                params.extend(normalized_exchanges)
            if symbols_chunk:
                placeholders = ", ".join("?" for _ in symbols_chunk)
                query.append(
                    "AND (pl.provider_symbol || '.' || px.provider_exchange_code)"
                    f" IN ({placeholders})"
                )
                params.extend(symbols_chunk)
            if security_chunk:
                placeholders = ", ".join("?" for _ in security_chunk)
                query.append(f"AND pl.listing_id IN ({placeholders})")
                params.extend(security_chunk)
            # Keep the sorted return contract (a test and callers rely on it); the
            # composed symbol is not indexable, but sorting the tiny projected
            # rows is cheap next to the raw-payload read.
            query.append("ORDER BY provider_symbol ASC")
            return conn.execute(" ".join(query), params)

        records: List[SecurityListingStatusRecord] = []

        def _consume(cursor: sqlite3.Cursor) -> None:
            # Stream the cursor: with only the four small columns projected, at
            # most one row is materialised at a time and the raw payloads never
            # accumulate in Python memory.
            for row in cursor:
                records.append(
                    self._build_status_record(
                        security_id=int(row["security_id"]),
                        provider_symbol=str(row["provider_symbol"]),
                        raw_fetched_at=str(row["last_fetched_at"]),
                        primary_ticker=row["primary_ticker"],
                    )
                )

        with self._connect() as conn:
            if normalized_symbols:
                for symbol_chunk in _batched(normalized_symbols, chunk_size):
                    _consume(_select_rows(conn, symbols_chunk=symbol_chunk))
            elif normalized_security_ids:
                for security_chunk in _batched(normalized_security_ids, chunk_size):
                    _consume(_select_rows(conn, security_chunk=security_chunk))
            else:
                _consume(_select_rows(conn))

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
            )
        return secondary

    def purge_secondary_security_data(
        self,
        *,
        security_ids: Sequence[int],
    ) -> None:
        normalized_security_ids = sorted(
            {int(security_id) for security_id in security_ids if security_id}
        )
        if not normalized_security_ids:
            return

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

        def _delete_by_listing_id(
            conn: sqlite3.Connection,
            table_name: str,
        ) -> None:
            for security_chunk in _batched(normalized_security_ids, 500):
                placeholders = ", ".join("?" for _ in security_chunk)
                conn.execute(
                    f"DELETE FROM {table_name} WHERE listing_id IN ({placeholders})",
                    list(security_chunk),
                )

        def _delete_state_by_listing_id(
            conn: sqlite3.Connection,
            table_name: str,
        ) -> None:
            # These two state tables key on provider_listing_id; resolve it from
            # listing_id through the provider_listing FK (served by
            # idx_provider_listing_listing) instead of looking the symbol up via
            # the 6-table catalog view. A secondary listing's state is purged for
            # every provider_listing it has, matching the listing_id purge above.
            for security_chunk in _batched(normalized_security_ids, 500):
                placeholders = ", ".join("?" for _ in security_chunk)
                conn.execute(
                    f"""
                    DELETE FROM {table_name}
                    WHERE provider_listing_id IN (
                        SELECT provider_listing_id FROM provider_listing
                        WHERE listing_id IN ({placeholders})
                    )
                    """,
                    list(security_chunk),
                )

        with self._connect() as conn:
            for table_name in (
                "financial_facts",
                "financial_facts_refresh_state",
                "market_data",
                "metrics",
                "metric_compute_status",
            ):
                _delete_by_listing_id(conn, table_name)
            for table_name in (
                "fundamentals_normalization_state",
                "market_data_fetch_state",
            ):
                _delete_state_by_listing_id(conn, table_name)
