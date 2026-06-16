"""Raw fundamentals and fundamentals-normalization-state repositories.

Author: Emre Tezel
"""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
)


from .base import (
    SQLiteStore,
    _batched,
    _normalize_optional_text,
    _normalize_provider_identity,
    _normalized_codes,
    _utc_now_iso,
)
from .records import (
    FundamentalsUpdate,
    NormalizationUnit,
    SecurityListingStatusRecord,
    SecurityMetadataCandidate,
)
from pyvalue.currency import normalize_currency_code
from ..migrations import apply_migrations
from .listing_status import SecurityListingStatusRepository

logger = logging.getLogger(__name__)


class FundamentalsRepository(SQLiteStore):
    """Persist raw fundamentals payloads by provider."""

    def initialize_schema(self) -> None:
        # `fundamentals_raw` is owned by migration 040, which also
        # dropped the legacy `idx_fundamentals_raw_security`,
        # `..._provider_symbol`, and `..._provider_fetched` indexes — so
        # the runtime DROP INDEX statements are no longer needed either.
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()

    def upsert_many(
        self,
        provider: str,
        updates: Sequence[FundamentalsUpdate],
    ) -> int:
        """Persist a batch of raw payloads for already-catalogued listings.

        Writes each payload to ``fundamentals_raw``, clears any fetch-state
        backoff, refreshes primary-listing status, and eagerly purges the
        downstream data of any listing the payloads reclassify as secondary.

        Returns the number of listings reclassified to secondary (and therefore
        purged) in this batch, so the caller can report the cascade.
        """
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        listing_repo = SecurityListingStatusRepository(self.db_path)
        listing_repo.initialize_schema()
        listing_updates: List[SecurityListingStatusRecord] = []
        with self._connect() as conn:
            # Ingest carries the provider_listing_id from the eligibility query, so
            # the write keys on it directly -- no per-payload symbol resolution.
            # Listing identity is owned by refresh-supported-tickers; ingest only
            # attaches a payload to an already-catalogued provider_listing.
            # provider_symbol stays on the update purely as data for the
            # listing-status reconciliation below (primary/secondary classification),
            # never as a lookup key.
            rows = [
                (
                    int(update.provider_listing_id),
                    update.data,
                    update.payload_hash,
                    update.last_fetched_at,
                )
                for update in updates
            ]
            if not rows:
                return 0
            conn.executemany(
                """
                INSERT INTO fundamentals_raw (
                    provider_listing_id,
                    data,
                    payload_hash,
                    last_fetched_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(provider_listing_id) DO UPDATE SET
                    data = excluded.data,
                    payload_hash = excluded.payload_hash,
                    last_fetched_at = excluded.last_fetched_at
                """,
                rows,
            )
            provider_listing_ids = [row[0] for row in rows]
            placeholders = ", ".join("?" for _ in provider_listing_ids)
            conn.execute(
                f"""
                DELETE FROM fundamentals_fetch_state
                WHERE provider_listing_id IN ({placeholders})
                """,
                provider_listing_ids,
            )
            listing_updates = listing_repo.upsert_many_from_fundamentals_updates(
                provider_norm,
                updates,
                connection=conn,
            )
        # The purge runs in its own connection after the write transaction above
        # has closed, so it stays outside the `with conn` block. Both ingest and
        # reconcile route secondary reclassifications through this one method.
        secondary_updates = listing_repo.purge_downstream_for_secondary(listing_updates)
        return len(secondary_updates)

    def fetch(self, provider: str, symbol: str) -> Optional[Dict[str, Any]]:
        self.initialize_schema()
        provider_symbol, _, _ = self._resolve_security(provider, symbol, None)
        if provider_symbol is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT fr.data
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ? AND catalog.provider_symbol = ?
                """,
                (provider.strip().upper(), provider_symbol),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def fetch_many(
        self,
        provider: str,
        symbols: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[str, Dict[str, Any]]:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}
        security_ids_by_symbol = self._security_repo().resolve_ids_many(
            normalized,
            chunk_size=chunk_size,
        )
        results: Dict[str, Dict[str, Any]] = {}
        provider_norm = provider.strip().upper()
        with self._connect() as conn:
            for chunk in _batched(list(security_ids_by_symbol.items()), chunk_size):
                rows = [
                    (symbol, security_id)
                    for symbol, security_id in chunk
                    if security_id
                ]
                if not rows:
                    continue
                placeholders = ", ".join("?" for _ in rows)
                query_params: List[object] = [
                    provider_norm,
                    *[security_id for _, security_id in rows],
                ]
                query_rows = conn.execute(
                    f"""
                    SELECT s.canonical_symbol, fr.data
                    FROM fundamentals_raw fr
                    JOIN provider_listing_catalog catalog
                      ON catalog.provider_listing_id = fr.provider_listing_id
                    JOIN securities s ON s.security_id = catalog.security_id
                    WHERE catalog.provider = ?
                      AND catalog.security_id IN ({placeholders})
                    """,
                    query_params,
                ).fetchall()
                for row in query_rows:
                    results[row["canonical_symbol"]] = json.loads(row["data"])
        return results

    def fetch_metadata_candidates(
        self,
        security_ids: Sequence[int],
        chunk_size: int = 500,
    ) -> Dict[int, SecurityMetadataCandidate]:
        """Extract canonical metadata fields from stored raw fundamentals."""

        self.initialize_schema()
        normalized_ids = sorted(
            {int(security_id) for security_id in security_ids if security_id}
        )
        if not normalized_ids:
            return {}

        results: Dict[int, SecurityMetadataCandidate] = {}
        with self._connect() as conn:
            for chunk in _batched(normalized_ids, chunk_size):
                placeholders = ", ".join("?" for _ in chunk)
                # Read straight from the base tables, not the
                # provider_listing_catalog view. The view INNER-joins listing,
                # issuer and exchange, none of which this query needs:
                # security_id is provider_listing.listing_id, provider is
                # provider.provider_code, and the payload lives on
                # fundamentals_raw. Routing through the view forced three extra
                # per-row B-tree seeks (listing, issuer, exchange) that yield no
                # column we read -- ~3x the table touches for each of ~75k raw
                # payloads on a full refresh. No ORDER BY either: rows are merged
                # into a dict keyed by security_id and the EODHD-over-SEC
                # precedence is handled by the merge below, not by row order.
                rows = conn.execute(
                    f"""
                    SELECT
                        pl.listing_id AS security_id,
                        p.provider_code AS provider,
                        fr.data
                    FROM fundamentals_raw fr
                    JOIN provider_listing pl
                      ON pl.provider_listing_id = fr.provider_listing_id
                    JOIN provider_exchange px
                      ON px.provider_exchange_id = pl.provider_exchange_id
                    JOIN provider p
                      ON p.provider_id = px.provider_id
                    WHERE pl.listing_id IN ({placeholders})
                    """,
                    list(chunk),
                ).fetchall()
                for row in rows:
                    provider = str(row["provider"]).upper()
                    if provider not in {"EODHD", "SEC"}:
                        continue

                    security_id = int(row["security_id"])
                    payload = json.loads(row["data"])
                    current = results.get(security_id) or SecurityMetadataCandidate()

                    if provider == "EODHD":
                        general = payload.get("General") or {}
                        eodhd_entity_name = _normalize_optional_text(
                            general.get("Name")
                        )
                        results[security_id] = SecurityMetadataCandidate(
                            entity_name=eodhd_entity_name or current.entity_name,
                            description=_normalize_optional_text(
                                general.get("Description")
                            ),
                            sector=_normalize_optional_text(general.get("Sector")),
                            industry=_normalize_optional_text(general.get("Industry")),
                        )
                        continue

                    sec_entity_name = _normalize_optional_text(
                        payload.get("entityName")
                    )
                    if current.entity_name is None and sec_entity_name is not None:
                        results[security_id] = SecurityMetadataCandidate(
                            entity_name=sec_entity_name,
                            description=current.description,
                            sector=current.sector,
                            industry=current.industry,
                        )
        return results

    def fetch_payload_with_hash_by_id(
        self, provider_listing_id: int
    ) -> Optional[Tuple[Dict[str, Any], str]]:
        """Read a stored raw payload + hash by its ``provider_listing_id`` PK.

        ``fundamentals_raw`` is PK'd on ``provider_listing_id``, so this is a single
        primary-key seek with no symbol parse and no listing resolution. The normalize
        worker carries the id end-to-end, so it never re-derives it from a symbol.
        """

        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT data, payload_hash
                FROM fundamentals_raw
                WHERE provider_listing_id = ?
                """,
                (int(provider_listing_id),),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["data"]), str(row["payload_hash"])

    def normalization_units(
        self,
        provider: str,
        *,
        primary_only: bool,
        listing_ids: Optional[Sequence[int]] = None,
        chunk_size: int = 500,
    ) -> Dict[int, NormalizationUnit]:
        """Return the id-keyed normalization work items for ``provider``.

        Each unit is one stored raw payload (one ``provider_listing_id``) present in
        ``fundamentals_raw``. Because the query INNER JOINs ``fundamentals_raw``, the
        result is already "scope that has a raw payload" -- callers never intersect a
        separate "has raw" symbol set. Each unit carries the ``listing_id`` (the
        fact/metadata write target), the base ``currency`` (``listing.currency``
        collapsed to its base via :func:`normalize_currency_code`, e.g. GBX->GBP), the
        ``provider_symbol`` display label, and the freshness hashes from the
        LEFT-JOINed normalization state.

        Reads straight from the base tables, not the ``provider_listing_catalog``
        view: the view drags in ``issuer`` and ``exchange``, neither of which this
        needs (the label is built from ``provider_symbol``). ``listing_ids=None``
        enumerates the whole provider in one scan; a bounded ``listing_ids`` is
        chunked into ``IN`` seeks served by ``idx_provider_listing_listing``.
        """

        self.initialize_schema()
        FundamentalsNormalizationStateRepository(self.db_path).initialize_schema()
        provider_norm = provider.strip().upper()

        units: Dict[int, NormalizationUnit] = {}
        with self._connect() as conn:
            if listing_ids is None:
                for row in self._normalization_unit_rows(
                    conn,
                    provider_norm,
                    primary_only=primary_only,
                    listing_id_chunk=None,
                ):
                    unit = self._row_to_normalization_unit(row)
                    units[unit.provider_listing_id] = unit
                return units
            normalized_ids = sorted({int(lid) for lid in listing_ids if lid})
            for chunk in _batched(normalized_ids, chunk_size):
                for row in self._normalization_unit_rows(
                    conn,
                    provider_norm,
                    primary_only=primary_only,
                    listing_id_chunk=chunk,
                ):
                    unit = self._row_to_normalization_unit(row)
                    units[unit.provider_listing_id] = unit
        return units

    def _normalization_unit_rows(
        self,
        conn: sqlite3.Connection,
        provider: str,
        *,
        primary_only: bool,
        listing_id_chunk: Optional[Sequence[int]],
    ) -> List[sqlite3.Row]:
        clauses = ["p.provider_code = ?"]
        params: List[object] = [provider]
        if primary_only:
            clauses.append("l.primary_listing_status <> 'secondary'")
        if listing_id_chunk is not None:
            placeholders = ", ".join("?" for _ in listing_id_chunk)
            clauses.append(f"pl.listing_id IN ({placeholders})")
            params.extend(int(lid) for lid in listing_id_chunk)
        where = " AND ".join(clauses)
        # ORDER BY the label keeps dispatch (and the [idx/total] progress lines)
        # deterministic across runs, matching the old symbols() ORDER BY.
        return conn.execute(
            f"""
            SELECT
                pl.provider_listing_id,
                pl.listing_id,
                pl.provider_symbol || '.' || px.provider_exchange_code
                    AS provider_symbol,
                l.currency,
                fr.payload_hash,
                ns.normalized_payload_hash,
                ns.normalized_at
            FROM fundamentals_raw fr
            JOIN provider_listing pl
              ON pl.provider_listing_id = fr.provider_listing_id
            JOIN provider_exchange px
              ON px.provider_exchange_id = pl.provider_exchange_id
            JOIN provider p ON p.provider_id = px.provider_id
            JOIN listing l ON l.listing_id = pl.listing_id
            LEFT JOIN fundamentals_normalization_state ns
              ON ns.provider_listing_id = fr.provider_listing_id
            WHERE {where}
            ORDER BY provider_symbol
            """,
            params,
        ).fetchall()

    def _row_to_normalization_unit(self, row: sqlite3.Row) -> NormalizationUnit:
        return NormalizationUnit(
            provider_listing_id=int(row["provider_listing_id"]),
            listing_id=int(row["listing_id"]),
            provider_symbol=str(row["provider_symbol"]),
            currency=normalize_currency_code(row["currency"]),
            raw_payload_hash=str(row["payload_hash"]),
            normalized_payload_hash=_normalize_optional_text(
                row["normalized_payload_hash"]
            ),
            normalized_at=_normalize_optional_text(row["normalized_at"]),
        )

    def _resolve_security(
        self,
        provider: str,
        symbol: str,
        exchange: Optional[str],
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        # Read-only: ingest never creates catalog rows. Securities/listings are
        # owned by refresh-supported-tickers (exchanges by
        # refresh-supported-exchanges); an uncatalogued symbol resolves to a
        # None security_id and the caller skips it.
        try:
            (
                provider_norm,
                provider_ticker,
                provider_exchange_code,
                provider_symbol,
            ) = _normalize_provider_identity(provider, symbol, exchange)
        except ValueError:
            return None, None, None
        canonical_exchange = self._exchange_provider_repo().resolve_canonical_code(
            provider_norm, provider_exchange_code
        )
        existing_security = self._security_repo().fetch_by_symbol(
            f"{provider_ticker}.{canonical_exchange}"
        )
        return (
            provider_symbol,
            provider_exchange_code,
            existing_security.security_id if existing_security else None,
        )


class FundamentalsNormalizationStateRepository(SQLiteStore):
    """Track successful normalization watermarks for stored raw fundamentals."""

    def initialize_schema(self) -> None:
        # `fundamentals_normalization_state` is owned by migration 040.
        # The legacy security/provider-symbol indexes were dropped there
        # too, so the runtime cleanup is no longer necessary.
        apply_migrations(self.db_path)

    def fetch(
        self, provider: str, symbol: str
    ) -> Optional[Dict[str, Optional[str] | int]]:
        self.initialize_schema()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    catalog.security_id,
                    ns.normalized_payload_hash,
                    ns.normalized_at
                FROM fundamentals_normalization_state ns
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = ns.provider_listing_id
                WHERE catalog.provider = ? AND catalog.provider_symbol = ?
                """,
                (provider.strip().upper(), symbol.strip().upper()),
            ).fetchone()
        if row is None:
            return None
        return {
            "security_id": int(row["security_id"]),
            "normalized_payload_hash": row["normalized_payload_hash"],
            "normalized_at": row["normalized_at"],
        }

    def mark_success_by_id(
        self,
        provider_listing_id: int,
        normalized_payload_hash: str,
        normalized_at: Optional[str] = None,
    ) -> None:
        """Record a normalization watermark keyed by ``provider_listing_id``.

        The id-keyed writer for the normalize hot path.
        ``fundamentals_normalization_state`` is PK'd on ``provider_listing_id``
        (``ON CONFLICT`` upsert), so when the caller already holds the id this is a
        single write with no symbol resolution.
        """

        self.initialize_schema()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO fundamentals_normalization_state (
                    provider_listing_id,
                    normalized_payload_hash,
                    normalized_at
                ) VALUES (?, ?, ?)
                ON CONFLICT(provider_listing_id) DO UPDATE SET
                    normalized_payload_hash = excluded.normalized_payload_hash,
                    normalized_at = excluded.normalized_at
                """,
                (
                    int(provider_listing_id),
                    normalized_payload_hash,
                    normalized_at or _utc_now_iso(),
                ),
            )

    def delete_symbols(self, provider: str, symbols: Sequence[str]) -> int:
        self.initialize_schema()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return 0
        provider_norm = provider.strip().upper()
        with self._connect() as conn:
            provider_listing_ids = []
            for symbol in normalized:
                row = conn.execute(
                    """
                    SELECT provider_listing_id
                    FROM provider_listing_catalog
                    WHERE provider = ? AND provider_symbol = ?
                    """,
                    (provider_norm, symbol),
                ).fetchone()
                if row is not None:
                    provider_listing_ids.append(int(row["provider_listing_id"]))
            if not provider_listing_ids:
                return 0
            placeholders = ", ".join("?" for _ in provider_listing_ids)
            cursor = conn.execute(
                f"""
                DELETE FROM fundamentals_normalization_state
                WHERE provider_listing_id IN ({placeholders})
                """,
                provider_listing_ids,
            )
        return int(cursor.rowcount or 0)
