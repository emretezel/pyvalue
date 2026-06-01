"""Raw fundamentals and fundamentals-normalization-state repositories.

Author: Emre Tezel
"""

from __future__ import annotations

import json
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
    canonical_json_dumps,
    fundamentals_payload_hash,
)
from .records import (
    FundamentalsNormalizationCandidate,
    FundamentalsUpdate,
    SecurityListingStatusRecord,
    SecurityMetadataCandidate,
)
from ..migrations import apply_migrations
from .listing_status import SecurityListingStatusRepository


class FundamentalsRepository(SQLiteStore):
    """Persist raw fundamentals payloads by provider."""

    def initialize_schema(self) -> None:
        # `fundamentals_raw` is owned by migration 040, which also
        # dropped the legacy `idx_fundamentals_raw_security`,
        # `..._provider_symbol`, and `..._provider_fetched` indexes — so
        # the runtime DROP INDEX statements are no longer needed either.
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()

    def upsert(
        self,
        provider: str,
        symbol: str,
        payload: Dict[str, Any],
        listing_currency: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> None:
        self.initialize_schema()
        provider_symbol, provider_exchange_code, security_id = self._resolve_security(
            provider, symbol, exchange
        )
        provider_norm = provider.strip().upper()
        data = canonical_json_dumps(payload)
        last_fetched_at = _utc_now_iso()
        self.upsert_many(
            provider_norm,
            [
                FundamentalsUpdate(
                    security_id=int(security_id or 0),
                    provider_symbol=str(provider_symbol or ""),
                    provider_exchange_code=provider_exchange_code,
                    listing_currency=_normalize_optional_text(
                        listing_currency.upper() if listing_currency else None
                    ),
                    data=data,
                    payload_hash=fundamentals_payload_hash(data),
                    last_fetched_at=last_fetched_at,
                )
            ],
        )

    def upsert_many(
        self,
        provider: str,
        updates: Sequence[FundamentalsUpdate],
    ) -> None:
        self.initialize_schema()
        provider_norm = provider.strip().upper()
        listing_repo = SecurityListingStatusRepository(self.db_path)
        listing_repo.initialize_schema()
        listing_updates: List[SecurityListingStatusRecord] = []
        with self._connect() as conn:
            rows = []
            ticker_repo = self._supported_ticker_repo()
            for update in updates:
                if not update.provider_symbol:
                    continue
                provider_symbol = update.provider_symbol.strip().upper()
                provider_listing_row = conn.execute(
                    """
                    SELECT provider_listing_id, security_id, provider_exchange_code
                    FROM provider_listing_catalog
                    WHERE provider = ? AND provider_symbol = ?
                    """,
                    (provider_norm, provider_symbol),
                ).fetchone()
                if provider_listing_row is None or update.listing_currency is not None:
                    provider_listing_row = ticker_repo._ensure_provider_listing(
                        conn,
                        provider_norm,
                        provider_symbol,
                        exchange_code=update.provider_exchange_code,
                        currency=update.listing_currency,
                    )
                if provider_listing_row is None:
                    # No currency available to model the listing (listing.currency
                    # is NOT NULL with no fallback), so skip storing this payload.
                    continue
                rows.append(
                    (
                        int(provider_listing_row["provider_listing_id"]),
                        update.data,
                        update.payload_hash,
                        update.last_fetched_at,
                    )
                )
            if not rows:
                return
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
        secondary_updates = [
            update for update in listing_updates if not update.is_primary_listing
        ]
        if secondary_updates:
            listing_repo.purge_secondary_security_data(
                security_ids=[update.security_id for update in secondary_updates],
                provider_symbols=[
                    update.provider_symbol for update in secondary_updates
                ],
            )

    def fetch(self, provider: str, symbol: str) -> Optional[Dict[str, Any]]:
        self.initialize_schema()
        provider_symbol, _, _ = self._resolve_security(
            provider, symbol, None, create=False
        )
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
                rows = conn.execute(
                    f"""
                    SELECT catalog.security_id, catalog.provider, fr.data
                    FROM fundamentals_raw fr
                    JOIN provider_listing_catalog catalog
                      ON catalog.provider_listing_id = fr.provider_listing_id
                    WHERE catalog.security_id IN ({placeholders})
                    ORDER BY catalog.security_id
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

    def fetch_record(
        self, provider: str, symbol: str
    ) -> Optional[Tuple[str, Optional[str], Dict[str, Any]]]:
        self.initialize_schema()
        provider_symbol, _, _ = self._resolve_security(
            provider, symbol, None, create=False
        )
        if provider_symbol is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT catalog.provider_symbol, catalog.provider_exchange_code, fr.data
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ? AND catalog.provider_symbol = ?
                """,
                (provider.strip().upper(), provider_symbol),
            ).fetchone()
        if row is None:
            return None
        return row[0], row[1], json.loads(row[2])

    def fetch_payload_with_hash(
        self, provider: str, symbol: str
    ) -> Optional[Tuple[Dict[str, Any], str]]:
        self.initialize_schema()
        provider_symbol, _, _ = self._resolve_security(
            provider, symbol, None, create=False
        )
        if provider_symbol is None:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT fr.data, fr.payload_hash
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ? AND catalog.provider_symbol = ?
                """,
                (provider.strip().upper(), provider_symbol),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["data"]), str(row["payload_hash"])

    def symbols(self, provider: str) -> List[str]:
        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT catalog.provider_symbol
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ?
                ORDER BY catalog.provider_symbol
                """,
                (provider.strip().upper(),),
            ).fetchall()
        return [row[0] for row in rows]

    def symbol_exchanges(self, provider: str) -> List[Tuple[str, Optional[str]]]:
        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT catalog.provider_symbol, catalog.provider_exchange_code
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ?
                ORDER BY catalog.provider_symbol
                """,
                (provider.strip().upper(),),
            ).fetchall()
        return [(row[0], row[1]) for row in rows]

    def normalization_candidates(
        self,
        provider: str,
        symbols: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[str, FundamentalsNormalizationCandidate]:
        self.initialize_schema()
        FundamentalsNormalizationStateRepository(self.db_path).initialize_schema()
        provider_norm = provider.strip().upper()
        normalized = _normalized_codes(symbols)
        if not normalized:
            return {}
        if len(normalized) > chunk_size * 4:
            return self._normalization_candidates_for_provider_scan(
                provider_norm,
                requested_symbols=set(normalized),
            )

        candidates: Dict[str, FundamentalsNormalizationCandidate] = {}
        with self._connect() as conn:
            for chunk in _batched(normalized, chunk_size):
                candidates.update(
                    self._build_normalization_candidates_for_rows(
                        rows=self._normalization_candidate_rows_for_chunk(
                            conn, provider_norm, chunk
                        ),
                    )
                )
        return candidates

    def _normalization_candidate_rows_for_chunk(
        self,
        conn: sqlite3.Connection,
        provider: str,
        symbols: Sequence[str],
    ) -> List[sqlite3.Row]:
        placeholders = ", ".join("?" for _ in symbols)
        return conn.execute(
            f"""
            SELECT
                catalog.provider_symbol,
                catalog.security_id,
                fr.payload_hash,
                ns.normalized_payload_hash,
                ns.normalized_at
            FROM fundamentals_raw fr
            JOIN provider_listing_catalog catalog
              ON catalog.provider_listing_id = fr.provider_listing_id
            LEFT JOIN fundamentals_normalization_state ns
              ON ns.provider_listing_id = fr.provider_listing_id
            WHERE catalog.provider = ?
              AND catalog.provider_symbol IN ({placeholders})
            """,
            [provider, *symbols],
        ).fetchall()

    def _build_normalization_candidates_for_rows(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> Dict[str, FundamentalsNormalizationCandidate]:
        rows_by_symbol: Dict[str, sqlite3.Row] = {}
        for row in rows:
            rows_by_symbol[str(row["provider_symbol"])] = row

        return {
            symbol_key: FundamentalsNormalizationCandidate(
                provider_symbol=symbol_key,
                security_id=int(row["security_id"]),
                raw_payload_hash=str(row["payload_hash"]),
                normalized_payload_hash=_normalize_optional_text(
                    row["normalized_payload_hash"]
                ),
                normalized_at=_normalize_optional_text(row["normalized_at"]),
            )
            for symbol_key, row in rows_by_symbol.items()
        }

    def _normalization_candidates_for_provider_scan(
        self,
        provider: str,
        requested_symbols: set[str],
    ) -> Dict[str, FundamentalsNormalizationCandidate]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    catalog.provider_symbol,
                    catalog.security_id,
                    fr.payload_hash,
                    ns.normalized_payload_hash,
                    ns.normalized_at
                FROM fundamentals_raw fr
                JOIN provider_listing_catalog catalog
                  ON catalog.provider_listing_id = fr.provider_listing_id
                LEFT JOIN fundamentals_normalization_state ns
                  ON ns.provider_listing_id = fr.provider_listing_id
                WHERE catalog.provider = ?
                """,
                (provider,),
            ).fetchall()
            filtered_rows = [
                row for row in rows if str(row["provider_symbol"]) in requested_symbols
            ]
            return self._build_normalization_candidates_for_rows(
                rows=filtered_rows,
            )

    def _resolve_security(
        self,
        provider: str,
        symbol: str,
        exchange: Optional[str],
        create: bool = True,
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
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
        if create:
            security = self._security_repo().ensure(provider_ticker, canonical_exchange)
            return provider_symbol, provider_exchange_code, security.security_id
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

    def mark_success(
        self,
        provider: str,
        symbol: str,
        security_id: int,
        normalized_payload_hash: str,
        normalized_at: Optional[str] = None,
    ) -> None:
        del security_id
        self.initialize_schema()
        with self._connect() as conn:
            provider_listing_row = conn.execute(
                """
                SELECT provider_listing_id
                FROM provider_listing_catalog
                WHERE provider = ? AND provider_symbol = ?
                """,
                (provider.strip().upper(), symbol.strip().upper()),
            ).fetchone()
            if provider_listing_row is None:
                return
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
                    int(provider_listing_row["provider_listing_id"]),
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
