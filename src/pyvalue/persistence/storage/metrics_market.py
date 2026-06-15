"""Metrics, metric-compute-status, market-data, and entity-metadata repositories.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import replace
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
    _utc_now_iso,
)
from .records import (
    IdKeyedStoredMetricRow,
    MarketSnapshotRecord,
    MetricComputeStatusRecord,
    MetricRecord,
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
        """Symbol-keyed wrapper over :meth:`upsert_many_by_id`.

        Resolves each row's symbol to its ``listing_id`` (using a supplied
        ``ids_by_symbol`` map when present, else the bulk resolver) and defers the
        write to the natural-identity path. Catalog-read-only: a row for an
        uncataloged symbol is skipped (never created); the issuer/listing catalog
        is owned by refresh-supported-tickers.
        """

        self.initialize_schema()
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

        security_ids: Dict[str, int] = dict(ids_by_symbol or {})
        unresolved = [symbol for symbol in unique_symbols if symbol not in security_ids]
        if unresolved:
            security_ids.update(
                self._security_repo().resolve_ids_many(
                    unresolved,
                    connection=connection,
                )
            )
        _warn_uncataloged_symbols(unique_symbols, security_ids, "metric")
        id_rows: List[IdKeyedStoredMetricRow] = [
            (
                security_ids[symbol],
                metric_id,
                value,
                as_of,
                unit_kind,
                currency,
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
        return self.upsert_many_by_id(id_rows, connection=connection, commit=commit)

    def upsert_many_by_id(
        self,
        rows: Iterable[IdKeyedStoredMetricRow],
        *,
        connection: Optional[sqlite3.Connection] = None,
        commit: bool = True,
    ) -> int:
        """Persist metric values by natural ``listing_id`` identity (no resolution)."""

        self.initialize_schema()
        persisted_rows = [
            (
                int(listing_id),
                metric_id,
                value,
                as_of,
                unit_kind,
                metric_currency_or_none(unit_kind, currency),
                unit_label,
            )
            for (
                listing_id,
                metric_id,
                value,
                as_of,
                unit_kind,
                currency,
                unit_label,
            ) in rows
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

    def fetch_by_id(self, listing_id: int, metric_id: str) -> Optional[MetricRecord]:
        """Fetch one stored metric by its natural ``listing_id`` identity."""

        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT value, as_of, unit_kind, currency, unit_label
                FROM metrics
                WHERE listing_id = ? AND metric_id = ?
                """,
                (int(listing_id), metric_id),
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

    def fetch_many_by_ids(
        self,
        listing_ids: Sequence[int],
        metric_ids: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[int, Dict[str, MetricRecord]]:
        """Fetch requested stored metrics for a ``listing_id`` scope.

        The natural-identity read used across the pipeline: the ``metrics`` PK is
        ``(listing_id, metric_id)`` so each chunk is a pure indexed seek with no
        symbol resolution and no re-keying. Returns ``{listing_id: {metric_id:
        record}}``.
        """

        self.initialize_schema()
        normalized_ids = sorted({int(x) for x in listing_ids if x is not None})
        requested_metric_ids = sorted(
            {
                str(metric_id).strip()
                for metric_id in metric_ids
                if str(metric_id).strip()
            }
        )
        if not normalized_ids or not requested_metric_ids:
            return {}

        metric_rows_by_id: Dict[int, Dict[str, MetricRecord]] = {}
        metric_chunk_size = max(
            1,
            min(len(requested_metric_ids), SQLITE_MAX_BOUND_PARAMETERS // 2),
        )

        with self._connect() as conn:
            for metric_chunk in _batched(requested_metric_ids, metric_chunk_size):
                # A single ``listing_id IN (...)`` predicate, so the id chunk may
                # use the whole bound-parameter budget minus the metric chunk --
                # no ``// 2`` halving (that is only needed by the two-column
                # ``(ticker, exchange)`` pair filter the symbol resolver uses).
                id_chunk_size = max(
                    1,
                    min(chunk_size, SQLITE_MAX_BOUND_PARAMETERS - len(metric_chunk)),
                )
                for id_chunk in _batched(normalized_ids, id_chunk_size):
                    id_placeholders = ", ".join("?" for _ in id_chunk)
                    metric_placeholders = ", ".join("?" for _ in metric_chunk)
                    rows = conn.execute(
                        f"""
                        SELECT listing_id, metric_id, value, as_of, unit_kind, currency, unit_label
                        FROM metrics
                        WHERE listing_id IN ({id_placeholders})
                          AND metric_id IN ({metric_placeholders})
                        """,
                        list(id_chunk) + list(metric_chunk),
                    ).fetchall()
                    for row in rows:
                        metric_rows_by_id.setdefault(int(row["listing_id"]), {})[
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
        return metric_rows_by_id


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
        all_rows = list(rows)
        if not all_rows:
            return 0
        # The symbol-keyed wrapper resolves the target listing from a display
        # symbol and defers to the natural-identity writer. Rows that carry no
        # symbol are ignored here; emit those through ``upsert_many_by_id`` with
        # ``listing_id`` set.
        symbol_rows: List[Tuple[str, MetricComputeStatusRecord]] = [
            (row.symbol.strip().upper(), row)
            for row in all_rows
            if row.symbol is not None
        ]
        if not symbol_rows:
            return 0

        unique_symbols: List[str] = []
        seen_symbols: set[str] = set()
        for symbol, _row in symbol_rows:
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
        # is skipped (never created).
        _warn_uncataloged_symbols(unique_symbols, security_ids, "metric-compute-status")

        id_rows = [
            replace(row, listing_id=security_ids[symbol])
            for symbol, row in symbol_rows
            if symbol in security_ids
        ]
        return self.upsert_many_by_id(id_rows, connection=connection, commit=commit)

    def upsert_many_by_id(
        self,
        rows: Iterable[MetricComputeStatusRecord],
        *,
        connection: Optional[sqlite3.Connection] = None,
        commit: bool = True,
    ) -> int:
        """Persist latest-attempt status rows by natural ``listing_id`` identity.

        Rows must carry ``listing_id``; any with ``listing_id is None`` are
        skipped (the symbol-keyed :meth:`upsert_many` is the path for symbol-only
        rows).
        """

        if connection is None:
            self.initialize_schema()
        payload = [
            (
                int(row.listing_id),
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
            for row in rows
            if row.listing_id is not None
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

    def fetch_by_id(
        self, listing_id: int, metric_id: str
    ) -> Optional[MetricComputeStatusRecord]:
        """Fetch one latest-attempt status row by natural ``listing_id`` identity.

        Returns a record with ``symbol=None``: the availability logic that
        consumes status reads the status and freshness watermarks, never the
        display symbol.
        """

        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT status, reason_code, reason_detail, attempted_at,
                       value_as_of, facts_refreshed_at, market_data_as_of,
                       market_data_updated_at
                FROM metric_compute_status
                WHERE listing_id = ? AND metric_id = ?
                """,
                (int(listing_id), metric_id),
            ).fetchone()
        if row is None:
            return None
        return MetricComputeStatusRecord(
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

    def fetch_many_by_ids(
        self,
        listing_ids: Sequence[int],
        metric_ids: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[int, Dict[str, MetricComputeStatusRecord]]:
        """Latest-attempt status rows for a ``listing_id`` scope, keyed by id.

        Records carry ``symbol=None`` (see :meth:`fetch_by_id`). The
        ``metric_compute_status`` PK ``(listing_id, metric_id)`` serves each chunk
        as an indexed seek.
        """

        self.initialize_schema()
        normalized_ids = sorted({int(x) for x in listing_ids if x is not None})
        requested_metric_ids = sorted(
            {
                str(metric_id).strip()
                for metric_id in metric_ids
                if str(metric_id).strip()
            }
        )
        if not normalized_ids or not requested_metric_ids:
            return {}

        rows_by_id: Dict[int, Dict[str, MetricComputeStatusRecord]] = {}
        metric_chunk_size = max(
            1,
            min(len(requested_metric_ids), SQLITE_MAX_BOUND_PARAMETERS // 2),
        )

        with self._connect() as conn:
            for metric_chunk in _batched(requested_metric_ids, metric_chunk_size):
                id_chunk_size = max(
                    1,
                    min(chunk_size, SQLITE_MAX_BOUND_PARAMETERS - len(metric_chunk)),
                )
                for id_chunk in _batched(normalized_ids, id_chunk_size):
                    id_placeholders = ", ".join("?" for _ in id_chunk)
                    metric_placeholders = ", ".join("?" for _ in metric_chunk)
                    rows = conn.execute(
                        f"""
                        SELECT listing_id, metric_id, status, reason_code, reason_detail,
                               attempted_at, value_as_of, facts_refreshed_at,
                               market_data_as_of, market_data_updated_at
                        FROM metric_compute_status
                        WHERE listing_id IN ({id_placeholders})
                          AND metric_id IN ({metric_placeholders})
                        """,
                        list(id_chunk) + list(metric_chunk),
                    ).fetchall()
                    for row in rows:
                        rows_by_id.setdefault(int(row["listing_id"]), {})[
                            row["metric_id"]
                        ] = MetricComputeStatusRecord(
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
        return rows_by_id


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

    def latest_snapshot_by_id(self, listing_id: int) -> Optional[PriceData]:
        record = self.latest_snapshot_record_by_id(listing_id)
        if record is None:
            return None
        return PriceData(
            symbol=record.symbol,
            price=record.price,
            as_of=record.as_of,
            volume=record.volume,
            currency=record.currency,
        )

    def latest_price_by_id(self, listing_id: int) -> Optional[Tuple[str, float]]:
        snapshot = self.latest_snapshot_by_id(listing_id)
        if snapshot is None:
            return None
        return snapshot.as_of, snapshot.price

    def latest_snapshot_record_by_id(
        self, listing_id: int
    ) -> Optional[MarketSnapshotRecord]:
        """Latest stored snapshot for one ``listing_id``.

        Joins ``market_data ⋈ listing ⋈ exchange`` -- enough to rebuild the
        canonical display symbol and read the listing currency -- without the
        redundant ``securities`` view + second ``listing`` join the symbol path
        historically carried.
        """

        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    md.listing_id AS security_id,
                    l.symbol || '.' || e.exchange_code AS canonical_symbol,
                    md.as_of,
                    md.price,
                    md.volume,
                    l.currency,
                    md.updated_at
                FROM market_data md
                JOIN listing l ON l.listing_id = md.listing_id
                JOIN "exchange" e ON e.exchange_id = l.exchange_id
                WHERE md.listing_id = ?
                ORDER BY md.as_of DESC
                LIMIT 1
                """,
                (int(listing_id),),
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

    def latest_snapshots_many_by_ids(
        self,
        listing_ids: Sequence[int],
        chunk_size: int = 500,
        *,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Dict[int, MarketSnapshotRecord]:
        """Latest snapshot per ``listing_id`` via one ``MAX(as_of)`` CTE per chunk.

        The natural-identity bulk read: ``market_data`` PK ``(listing_id, as_of)``
        serves the grouped seek; ``listing ⋈ exchange`` rebuilds the canonical
        display symbol and currency. A single ``listing_id IN (...)`` predicate, so
        the chunk may use the full bound-parameter budget.
        """

        self.initialize_schema()
        normalized_ids = sorted({int(x) for x in listing_ids if x is not None})
        if not normalized_ids:
            return {}

        snapshots: Dict[int, MarketSnapshotRecord] = {}

        def _query(conn: sqlite3.Connection) -> None:
            for chunk in _batched(normalized_ids, chunk_size):
                placeholders = ", ".join("?" for _ in chunk)
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
                        l.symbol || '.' || e.exchange_code AS canonical_symbol,
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
                    JOIN "exchange" e ON e.exchange_id = l.exchange_id
                    ORDER BY md.listing_id
                    """,
                    list(chunk),
                )
                for row in cursor:
                    snapshots[int(row["security_id"])] = MarketSnapshotRecord(
                        security_id=row["security_id"],
                        symbol=row["canonical_symbol"],
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
