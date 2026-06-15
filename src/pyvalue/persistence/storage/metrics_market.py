"""Metrics, metric-compute-status, market-data, and entity-metadata repositories.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
import sqlite3
from typing import (
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from pyvalue.currency import (
    MetricUnitKind,
    canonical_trading_currency,
    metric_currency_or_none,
)
from pyvalue.marketdata.base import MarketDataUpdate, PriceData

from .base import (
    SQLITE_MAX_BOUND_PARAMETERS,
    SQLiteStore,
    _batched,
    _normalized_codes,
    _utc_now_iso,
)
from .records import (
    MarketSnapshotRecord,
    MetricComputeStatusRecord,
    MetricRecord,
    Security,
    SecurityMetadataUpdate,
    StoredMetricRow,
)
from ..migrations import apply_migrations


logger = logging.getLogger(__name__)


def _warn_uncataloged_symbols(
    symbols: Sequence[str],
    security_ids: Mapping[str, int],
    row_kind: str,
) -> None:
    """Warn about symbols with no catalog listing; their rows are skipped.

    The metrics and metric-compute-status writers are catalog-read-only: the
    issuer/listing catalog is owned by refresh-supported-tickers, so a row that
    references a symbol with no listing is dropped (the upsert payload keeps only
    resolved symbols) rather than minting issuer/listing rows from a metric
    write. In normal operation the compute scope is drawn from the catalog, so
    nothing is dropped; this surfaces a genuinely uncataloged ticker instead of
    losing it silently.
    """

    missing = sorted(symbol for symbol in symbols if symbol not in security_ids)
    if not missing:
        return
    logger.warning(
        "Skipping %s rows for %d uncataloged symbol(s); the writer is "
        "catalog-read-only (issuer/listing are owned by refresh-supported-tickers). "
        "Examples: %s",
        row_kind,
        len(missing),
        ", ".join(missing[:5]),
    )


class MetricsRepository(SQLiteStore):
    """Persist computed metric values."""

    def initialize_schema(self) -> None:
        # The `metrics` table and its indexes are owned by migrations
        # (created in #034, rebuilt in #041 to add FK + CHECK constraints).
        # apply_migrations() is the single source of truth — re-issuing
        # CREATE TABLE here would either no-op or, after a hypothetical
        # DROP, recreate the table with the legacy unconstrained DDL and
        # silently strip migration 041's constraints.
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()

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
        ids_by_symbol: Optional[Mapping[str, int]] = None,
        connection: Optional[sqlite3.Connection] = None,
        commit: bool = True,
    ) -> int:
        self.initialize_schema()
        # Materialise the iterable once; it is scanned several times below
        # (unique-symbol collection, security-id resolution, the executemany).
        metric_rows: List[StoredMetricRow] = list(rows)
        if not metric_rows:
            return 0

        unique_symbols = []
        seen_symbols = set()
        for symbol, _, _, _, _, _, _ in metric_rows:
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            unique_symbols.append(symbol)

        # Callers that already hold the listing_id (compute-metrics carries it
        # from scope resolution) supply ``ids_by_symbol`` so this write performs
        # no symbol->id resolution at all. Only symbols absent from the map fall
        # through to the resolver, preserving the standalone-caller contract.
        security_ids: Dict[str, int] = dict(ids_by_symbol or {})
        unresolved = [symbol for symbol in unique_symbols if symbol not in security_ids]
        if unresolved:
            security_ids.update(
                self._security_repo().resolve_ids_many(
                    unresolved,
                    connection=connection,
                )
            )
        # Catalog-read-only: a row for an uncataloged symbol is skipped (the
        # comprehension below keeps only resolved symbols), never created. The
        # issuer/listing catalog is owned by refresh-supported-tickers.
        _warn_uncataloged_symbols(unique_symbols, security_ids, "metric")
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
        *,
        security_ids_by_symbol: Optional[Mapping[str, int]] = None,
    ) -> Dict[str, Dict[str, MetricRecord]]:
        """Fetch requested stored metrics for a symbol scope with chunked indexed reads.

        ``security_ids_by_symbol`` lets callers that already resolved their scope
        (run-screen / report-* via ``_resolve_canonical_scope_listings``) carry the
        natural ``listing_id`` straight in, eliminating the symbol->id round trip.
        A superset map is fine -- only the requested ``symbols`` are queried -- so
        the ranking pass can reuse the full scope map for a subset of passers.
        """

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

        resolved_security_ids = (
            dict(security_ids_by_symbol)
            if security_ids_by_symbol is not None
            else self._security_repo().resolve_ids_many(
                normalized_symbols,
                chunk_size=chunk_size,
            )
        )
        symbol_by_security_id = {
            resolved_security_ids[symbol]: symbol
            for symbol in normalized_symbols
            if symbol in resolved_security_ids
        }
        if not symbol_by_security_id:
            return {}

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
        # `metric_compute_status` is owned by migrations (created in #034,
        # rebuilt in #041 to add FK to listing). See MetricsRepository for
        # the full rationale on why the runtime CREATE TABLE was removed.
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()

    def upsert_many(
        self,
        rows: Iterable[MetricComputeStatusRecord],
        *,
        ids_by_symbol: Optional[Mapping[str, int]] = None,
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
        # See MetricsRepository.upsert_many: a supplied ``ids_by_symbol`` (from
        # compute-metrics' scope-resolved listing ids) means zero resolution;
        # only unmapped symbols hit the resolver.
        security_ids: Dict[str, int] = dict(ids_by_symbol or {})
        unresolved = [symbol for symbol in unique_symbols if symbol not in security_ids]
        if unresolved:
            security_ids.update(
                self._security_repo().resolve_ids_many(
                    unresolved,
                    connection=connection,
                )
            )
        # Catalog-read-only, like the metric values writer: an uncataloged symbol
        # is skipped (the payload below keeps only resolved symbols), never
        # created.
        _warn_uncataloged_symbols(unique_symbols, security_ids, "metric-compute-status")

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
        *,
        security_ids_by_symbol: Optional[Mapping[str, int]] = None,
    ) -> Dict[str, Dict[str, MetricComputeStatusRecord]]:
        """Fetch the latest compute-status rows for a symbol scope.

        ``security_ids_by_symbol`` carries scope-resolved ``listing_id`` values in
        (see :meth:`MetricsRepository.fetch_many_for_symbols`) so report-* commands
        avoid the symbol->id re-resolution; a superset map only queries the
        requested ``symbols``.
        """

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

        resolved_security_ids = (
            dict(security_ids_by_symbol)
            if security_ids_by_symbol is not None
            else self._security_repo().resolve_ids_many(
                normalized_symbols,
                chunk_size=chunk_size,
            )
        )
        symbol_by_security_id = {
            resolved_security_ids[symbol]: symbol
            for symbol in normalized_symbols
            if symbol in resolved_security_ids
        }
        if not symbol_by_security_id:
            return {}

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
        # `market_data` is owned by migration 034.
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()

    def upsert_price(
        self,
        symbol: str,
        as_of: str,
        price: float,
        volume: Optional[int] = None,
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
                    source_provider,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(listing_id, as_of) DO UPDATE SET
                    price = excluded.price,
                    volume = excluded.volume,
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
                       l.currency, md.updated_at
                FROM market_data md
                JOIN securities s ON s.security_id = md.listing_id
                JOIN listing l ON l.listing_id = md.listing_id
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
            # market_data.price is stored in the major currency, so report the
            # listing currency collapsed to its base (GBX -> GBP) -- never a
            # subunit. This keeps the (price, currency) pair self-consistent so
            # downstream Money/normalization does not divide by 100 a second time.
            currency=canonical_trading_currency(row["currency"]),
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
                        FROM market_data
                        WHERE listing_id IN ({placeholders})
                        GROUP BY listing_id
                    )
                    SELECT
                        md.listing_id AS security_id,
                        md.as_of,
                        md.price,
                        md.volume,
                        l.currency,
                        md.updated_at
                    FROM latest
                    JOIN market_data md
                      ON md.listing_id = latest.listing_id
                     AND md.as_of = latest.as_of
                    JOIN listing l ON l.listing_id = md.listing_id
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
                        # Stored price is major; collapse listing currency to
                        # its base so the (price, currency) pair is consistent.
                        currency=canonical_trading_currency(row["currency"]),
                        updated_at=row["updated_at"],
                    )

        if connection is not None:
            _query(connection)
        else:
            with self._connect() as conn:
                _query(conn)
        return snapshots


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
