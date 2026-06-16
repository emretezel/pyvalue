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
    Optional,
    Sequence,
    Tuple,
)


from .migrations import apply_migrations
from .base import (
    SQLiteStore,
    _batched,
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

    def clear(self) -> None:
        """Delete every ``financial_facts_refresh_state`` row (keeps constraints)."""
        self.initialize_schema()
        with self._connect() as conn:
            conn.execute("DELETE FROM financial_facts_refresh_state")

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

    def clear(self) -> None:
        """Delete every ``financial_facts`` row (DELETE FROM, keeps constraints).

        Not DROP TABLE: dropping would force ``initialize_schema`` to recreate the
        table from legacy DDL and silently strip migration-added FK/CHECK constraints.
        """
        self.initialize_schema()
        with self._connect() as conn:
            conn.execute("DELETE FROM financial_facts")

    def replace_fact_rows(
        self,
        listing_id: int,
        rows: Iterable[StoredFactRow],
    ) -> int:
        """Replace all stored facts for one ``listing_id`` by natural identity.

        The single fact write, keyed purely by ``listing_id``: there is no symbol
        resolution and no listing creation. The listing must already exist in the
        catalog (owned by refresh-supported-tickers); the canonical-scope callers
        carry the ``listing_id`` they resolved during their freshness scan, so the
        write is a direct DELETE-then-insert against that id.
        """

        self.initialize_schema()
        listing_id = int(listing_id)
        prepared_rows = [
            (
                listing_id,
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
                (listing_id,),
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
                listing_id,
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

    def facts_for_ids_many(
        self,
        listing_ids: Sequence[int],
        chunk_size: int = 25,
        *,
        concepts: Optional[Sequence[str]] = None,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Dict[int, List[FactRecord]]:
        """Return all stored facts for many listings, grouped by ``listing_id``.

        The ``compute-metrics`` fact prefetch: chunked indexed reads over a
        bounded set of ``listing_id`` values, no symbol resolution, no
        canonical-symbol projection. ``concepts`` restricts the read to the
        requested set so the ``(listing_id, concept, end_date DESC, filed DESC)``
        index turns each ``(listing_id, concept)`` pair into a direct seek.
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

    def _populate_temp_selected_listing_ids(
        self,
        conn: sqlite3.Connection,
        listing_ids: Sequence[int],
        chunk_size: int = 500,
    ) -> None:
        """Populate the id-only scratch table the ``*_by_ids`` share reads scan.

        The table carries only ``listing_id`` because the id-keyed reads project
        the natural key and never the display symbol.
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
        """Pick the latest shares-outstanding fact per selected ``listing_id``.

        Reads from the id-only ``temp_selected_securities`` scratch table
        populated by :meth:`_populate_temp_selected_listing_ids`. The tie-break
        ORDER BY prefers the most recent end date, then
        ``CommonStockSharesOutstanding`` over the entity concept, a ``count``
        unit, a currency-less fact, a Q4/Q3/Q2/Q1/FY period in that order, the
        smaller magnitude, and finally the latest filing.
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

        Used by the report market-cap estimator. Reads the primary/fallback
        share-count concepts (``EntityCommonStockSharesOutstanding`` then
        ``CommonStockSharesOutstanding``) with the tie-break ordering encoded in
        :meth:`_latest_share_counts_for_temp_selected_ids`.
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
