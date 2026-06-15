"""Financial-facts and financial-facts refresh-state repositories.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)


from ..migrations import apply_migrations
from .base import (
    SQLiteStore,
    _batched,
    _normalized_codes,
    _utc_now_iso,
)
from .records import (
    FactRecord,
    FinancialFactsRefreshStateRecord,
    StoredFactRow,
)


def _fact_record_from_row(row: sqlite3.Row) -> FactRecord:
    """Build a :class:`FactRecord` from an id-keyed fact row.

    The natural-identity fact readers select by ``listing_id`` and do not project
    the canonical symbol, so the resulting record carries no ``symbol`` (the
    metric layer reads facts by ``listing_id`` and never reads that field).
    """

    return FactRecord(
        concept=row["concept"],
        fiscal_period=row["fiscal_period"],
        end_date=row["end_date"],
        unit_kind=row["unit_kind"],
        value=row["value"],
        filed=row["filed"],
        currency=row["currency"],
    )


class FinancialFactsRefreshStateRepository(SQLiteStore):
    """Track the latest normalized financial-facts refresh per security."""

    def initialize_schema(self) -> None:
        # `financial_facts_refresh_state` is owned by migration 034.
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()

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
        """Symbol-keyed convenience over :meth:`fetch_by_id`."""

        self.initialize_schema()
        security_id = self._security_repo().resolve_id(symbol)
        if security_id is None:
            return None
        return self.fetch_by_id(security_id)

    def fetch_by_id(
        self, listing_id: int
    ) -> Optional[FinancialFactsRefreshStateRecord]:
        """Latest refresh watermark for one ``listing_id`` (PK lookup)."""

        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT refreshed_at
                FROM financial_facts_refresh_state
                WHERE listing_id = ?
                """,
                (int(listing_id),),
            ).fetchone()
        if row is None:
            return None
        return FinancialFactsRefreshStateRecord(
            listing_id=int(listing_id),
            refreshed_at=row["refreshed_at"],
        )

    def fetch_many_by_ids(
        self,
        listing_ids: Sequence[int],
        chunk_size: int = 500,
        *,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Dict[int, FinancialFactsRefreshStateRecord]:
        """Refresh watermarks for a ``listing_id`` scope, keyed by ``listing_id``."""

        self.initialize_schema()
        normalized_ids = sorted({int(x) for x in listing_ids if x is not None})
        if not normalized_ids:
            return {}

        rows_by_id: Dict[int, FinancialFactsRefreshStateRecord] = {}

        def _query(conn: sqlite3.Connection) -> None:
            for chunk in _batched(normalized_ids, chunk_size):
                placeholders = ", ".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT listing_id, refreshed_at
                    FROM financial_facts_refresh_state
                    WHERE listing_id IN ({placeholders})
                    """,
                    list(chunk),
                ).fetchall()
                for row in rows:
                    rows_by_id[int(row["listing_id"])] = (
                        FinancialFactsRefreshStateRecord(
                            listing_id=int(row["listing_id"]),
                            refreshed_at=row["refreshed_at"],
                        )
                    )

        if connection is not None:
            _query(connection)
        else:
            with self._connect() as conn:
                _query(conn)
        return rows_by_id

    def fetch_many_for_symbols(
        self,
        symbols: Sequence[str],
        chunk_size: int = 500,
        *,
        security_ids_by_symbol: Optional[Mapping[str, int]] = None,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, FinancialFactsRefreshStateRecord]:
        """Symbol-keyed wrapper over :meth:`fetch_many_by_ids`."""

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
        symbol_by_security_id = {
            resolved_security_ids[symbol]: symbol
            for symbol in normalized_symbols
            if symbol in resolved_security_ids
        }
        if not symbol_by_security_id:
            return {}

        rows_by_id = self.fetch_many_by_ids(
            list(symbol_by_security_id),
            chunk_size=chunk_size,
            connection=connection,
        )
        return {
            symbol_by_security_id[listing_id]: record
            for listing_id, record in rows_by_id.items()
        }


class FinancialFactsRepository(SQLiteStore):
    """Persist normalized financial facts for downstream metrics."""

    def initialize_schema(self) -> None:
        # `financial_facts` (table + all four idx_fin_facts_* indexes) is
        # owned by migration 034 (initial), 029 (latest-concept index), and
        # 043 (PK rebuild + listing FK). The defensive
        # CREATE-INDEX-with-locked-retry block is no longer needed because
        # the migration runs once inside its own transaction.
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()
        FinancialFactsRefreshStateRepository(self.db_path).initialize_schema()

    def replace_facts(
        self,
        symbol: str,
        records: Iterable[FactRecord],
    ) -> int:
        rows = [
            (
                record.concept,
                record.fiscal_period,
                record.end_date,
                record.unit_kind,
                record.value,
                record.filed,
                record.currency,
            )
            for record in records
        ]
        return self.replace_fact_rows(
            symbol=symbol,
            rows=rows,
        )

    def replace_fact_rows(
        self,
        symbol: str,
        rows: Iterable[StoredFactRow],
        *,
        security_id: Optional[int] = None,
    ) -> int:
        # Callers that already hold the security_id (the bulk normalizer resolved
        # it during its freshness scan) pass it in to skip ``ensure_from_symbol``.
        # That path runs the full create-or-update routine -- including a no-op
        # ``UPDATE issuer`` -- on every symbol, redundant work on the serialized
        # writer when the listing is already known to exist. When omitted we fall
        # back to ensuring the listing (back-compat for symbol-only callers).
        self.initialize_schema()
        if security_id is None:
            security_id = self._security_repo().ensure_from_symbol(symbol).security_id
        prepared_rows = [
            (
                security_id,
                concept,
                fiscal_period,
                end_date,
                unit_kind,
                value,
                filed,
                currency,
            )
            for (
                concept,
                fiscal_period,
                end_date,
                unit_kind,
                value,
                filed,
                currency,
            ) in rows
        ]
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM financial_facts WHERE listing_id = ?",
                (security_id,),
            )
            if prepared_rows:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO financial_facts (
                        listing_id, concept, fiscal_period, end_date, unit_kind,
                        value, filed, currency
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    prepared_rows,
                )
            FinancialFactsRefreshStateRepository(self.db_path).mark_security_refreshed(
                security_id,
                connection=conn,
            )
        return len(prepared_rows)

    def latest_fact(
        self,
        listing_id: int,
        concept: str,
    ) -> Optional[FactRecord]:
        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT ff.concept, ff.fiscal_period, ff.end_date,
                       ff.unit_kind, ff.value, ff.filed, ff.currency
                FROM financial_facts ff
                WHERE ff.listing_id = ? AND ff.concept = ?
                ORDER BY ff.end_date DESC, ff.filed DESC
                LIMIT 1
                """,
                (int(listing_id), concept),
            ).fetchone()
        if row is None:
            return None
        return _fact_record_from_row(row)

    def facts_for_concept(
        self,
        listing_id: int,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[FactRecord]:
        self.initialize_schema()
        query = [
            "SELECT ff.concept, ff.fiscal_period, ff.end_date,",
            "ff.unit_kind, ff.value, ff.filed, ff.currency",
            "FROM financial_facts ff",
            "WHERE ff.listing_id = ? AND ff.concept = ?",
        ]
        params: List[Any] = [int(listing_id), concept]
        if fiscal_period:
            query.append("AND ff.fiscal_period = ?")
            params.append(fiscal_period)
        query.append("ORDER BY ff.end_date DESC, ff.filed DESC")
        if limit:
            query.append("LIMIT ?")
            params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(" ".join(query), params).fetchall()
        return [_fact_record_from_row(row) for row in rows]

    def facts_for_id(self, listing_id: int) -> List[FactRecord]:
        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT ff.concept, ff.fiscal_period, ff.end_date,
                       ff.unit_kind, ff.value, ff.filed, ff.currency
                FROM financial_facts ff
                WHERE ff.listing_id = ?
                ORDER BY ff.concept, ff.end_date DESC, ff.filed DESC
                """,
                (int(listing_id),),
            ).fetchall()
        return [_fact_record_from_row(row) for row in rows]

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
                        ff.concept,
                        ff.fiscal_period,
                        ff.end_date,
                        ff.unit_kind,
                        ff.value,
                        ff.filed,
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
                            concept=row["concept"],
                            fiscal_period=row["fiscal_period"],
                            end_date=row["end_date"],
                            unit_kind=row["unit_kind"],
                            value=row["value"],
                            filed=row["filed"],
                            currency=row["currency"],
                        )
                    )

        if connection is not None:
            _query(connection)
        else:
            with self._connect() as conn:
                _query(conn)
        return grouped

    def facts_for_ids_many(
        self,
        listing_ids: Sequence[int],
        chunk_size: int = 25,
        *,
        concepts: Optional[Sequence[str]] = None,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Dict[int, List[FactRecord]]:
        """Return all stored facts for many listings, grouped by ``listing_id``.

        The natural-identity counterpart of :meth:`facts_for_symbols_many` (the
        ``compute-metrics`` fact prefetch): chunked indexed reads over a bounded
        set of ``listing_id`` values, no symbol resolution, no canonical-symbol
        projection. ``concepts`` restricts the read to the requested set so the
        ``(listing_id, concept, end_date DESC, filed DESC)`` index turns each
        ``(listing_id, concept)`` pair into a direct seek.
        """

        self.initialize_schema()
        normalized_ids = sorted({int(x) for x in listing_ids if x is not None})
        if not normalized_ids:
            return {}

        concept_filter: Tuple[str, ...] = ()
        if concepts:
            seen_concepts: set[str] = set()
            ordered_concepts: List[str] = []
            for concept in concepts:
                if concept and concept not in seen_concepts:
                    seen_concepts.add(concept)
                    ordered_concepts.append(concept)
            concept_filter = tuple(ordered_concepts)

        grouped: Dict[int, List[FactRecord]] = {
            listing_id: [] for listing_id in normalized_ids
        }

        def _query(conn: sqlite3.Connection) -> None:
            for chunk in _batched(normalized_ids, chunk_size):
                placeholders = ", ".join("?" for _ in chunk)
                params: List[Any] = list(chunk)
                concept_clause = ""
                if concept_filter:
                    concept_placeholders = ", ".join("?" for _ in concept_filter)
                    concept_clause = f" AND ff.concept IN ({concept_placeholders})"
                    params.extend(concept_filter)
                cursor = conn.execute(
                    f"""
                    SELECT
                        ff.listing_id,
                        ff.concept,
                        ff.fiscal_period,
                        ff.end_date,
                        ff.unit_kind,
                        ff.value,
                        ff.filed,
                        ff.currency
                    FROM financial_facts ff INDEXED BY idx_fin_facts_security_concept_latest
                    WHERE ff.listing_id IN ({placeholders}){concept_clause}
                    ORDER BY ff.listing_id, ff.concept, ff.end_date DESC, ff.filed DESC
                    """,
                    params,
                )
                for row in cursor:
                    grouped[int(row["listing_id"])].append(_fact_record_from_row(row))

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
                             CASE ff.unit_kind
                                 WHEN 'count' THEN 0
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
            (primary_concept, fallback_concept),
        ).fetchall()
        for row in rows:
            try:
                if row["value"] is None:
                    continue
                counts[row["canonical_symbol"]] = float(row["value"])
            except (TypeError, ValueError):
                continue
        return counts

    def _populate_temp_selected_listing_ids(
        self,
        conn: sqlite3.Connection,
        listing_ids: Sequence[int],
        chunk_size: int = 500,
    ) -> None:
        """Populate the id-only scratch table the ``*_by_ids`` share reads scan.

        The natural-identity counterpart of
        :meth:`_populate_temp_selected_securities`: the table carries only
        ``listing_id`` because the id-keyed reads project the natural key and
        never the display symbol.
        """

        conn.execute("DROP TABLE IF EXISTS temp_selected_securities")
        conn.execute(
            """
            CREATE TEMP TABLE temp_selected_securities (
                listing_id INTEGER PRIMARY KEY
            )
            """
        )
        rows = [(int(listing_id),) for listing_id in listing_ids]
        for chunk in _batched(rows, chunk_size):
            conn.executemany(
                "INSERT OR IGNORE INTO temp_selected_securities (listing_id) "
                "VALUES (?)",
                list(chunk),
            )

    def _latest_share_counts_for_temp_selected_ids(
        self,
        conn: sqlite3.Connection,
        primary_concept: str,
        fallback_concept: str,
    ) -> Dict[int, float]:
        """``listing_id``-keyed twin of
        :meth:`_latest_share_counts_for_temp_selected_securities`.

        The tie-break ORDER BY is copied verbatim so the share count chosen for a
        listing is independent of whether the caller keyed the read by symbol or
        by id.
        """

        counts: Dict[int, float] = {}
        rows = conn.execute(
            """
            SELECT
                selected.listing_id,
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
                             CASE ff.unit_kind
                                 WHEN 'count' THEN 0
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
            (primary_concept, fallback_concept),
        ).fetchall()
        for row in rows:
            try:
                if row["value"] is None:
                    continue
                counts[int(row["listing_id"])] = float(row["value"])
            except (TypeError, ValueError):
                continue
        return counts

    def latest_share_counts_many_by_ids(
        self,
        listing_ids: Sequence[int],
        chunk_size: int = 500,
    ) -> Dict[int, float]:
        """Latest shares-outstanding per ``listing_id`` (no symbol resolution).

        The natural-identity counterpart of :meth:`latest_share_counts_many`
        used by the report market-cap estimator. Reads the same primary/fallback
        share-count concepts (``EntityCommonStockSharesOutstanding`` then
        ``CommonStockSharesOutstanding``) with the identical tie-break ordering.
        """

        normalized_ids = [int(listing_id) for listing_id in listing_ids]
        if not normalized_ids:
            return {}
        with self._connect() as conn:
            self._populate_temp_selected_listing_ids(
                conn,
                normalized_ids,
                chunk_size=chunk_size,
            )
            return self._latest_share_counts_for_temp_selected_ids(
                conn,
                primary_concept="EntityCommonStockSharesOutstanding",
                fallback_concept="CommonStockSharesOutstanding",
            )

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
