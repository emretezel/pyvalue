"""Security listing-status (primary/secondary) repository.

Author: Emre Tezel

The classification *rule* lives in :mod:`pyvalue.universe.listing_classification`
-- pure, DB-free domain logic. This module owns only the data movement: pulling
the evidence each rule needs out of SQLite, handing it to the resolver, and
writing the verdicts back to ``listing.primary_listing_status``.
"""

from __future__ import annotations

import json
import sqlite3
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
)


from pyvalue.identifiers import shaped_isin, shaped_lei
from pyvalue.universe.listing_classification import (
    ListingEvidence,
    ListingStatus,
    classify_listing_without_peers,
    classify_listings,
)

from .base import (
    SQLiteStore,
    _PRIMARY_LISTING_SOURCE_PROVIDER,
    _batched,
    _normalize_optional_text,
    _normalize_qualified_symbol,
    _normalize_symbol_base,
    _normalized_codes,
    _utc_now_iso,
)
from .records import (
    FundamentalsUpdate,
    SecurityListingStatusRecord,
)
from .migrations import apply_migrations


class SecurityListingStatusRepository(SQLiteStore):
    """Persist and reconcile canonical primary-listing classification."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)
        self._security_repo().initialize_schema()

    @staticmethod
    def _build_evidence(
        *,
        security_id: int,
        provider_symbol: str,
        primary_ticker: Any = None,
        home_category: Any = None,
        isin: Any = None,
        lei: Any = None,
        venue_tier: Any = None,
        hq_country: Any = None,
        venue_country_iso2: Any = None,
    ) -> ListingEvidence:
        """Normalize one listing's raw provider fields into resolver evidence.

        Every value arrives straight from a JSON payload or a catalog column, so
        normalization (trim, uppercase, shape-check) happens once here rather
        than being re-derived by each rule. ``provider_symbol`` must already be
        qualified (``0K10.LSE``) -- it is the identity the resolver compares
        ``PrimaryTicker`` against.
        """

        provider_symbol_norm = _normalize_qualified_symbol(provider_symbol)
        if provider_symbol_norm is None:
            raise ValueError(f"provider_symbol must be qualified: {provider_symbol}")
        bare_symbol, exchange_code = _normalize_symbol_base(provider_symbol_norm)

        home_category_norm = _normalize_optional_text(home_category)
        venue_tier_norm = _normalize_optional_text(venue_tier)
        venue_country_norm = _normalize_optional_text(venue_country_iso2)
        return ListingEvidence(
            listing_id=int(security_id),
            provider_symbol=provider_symbol_norm,
            bare_symbol=bare_symbol,
            exchange_code=(exchange_code or "").upper(),
            primary_ticker=_normalize_qualified_symbol(primary_ticker),
            home_category=(
                home_category_norm.upper() if home_category_norm is not None else None
            ),
            isin=shaped_isin(isin),
            lei=shaped_lei(lei),
            venue_tier=(
                venue_tier_norm.upper() if venue_tier_norm is not None else None
            ),
            # Head-office country stays verbatim: it is compared against
            # payload-side spellings in VENUE_HOME_COUNTRY, not normalized codes.
            hq_country=_normalize_optional_text(hq_country),
            venue_country_iso2=(
                venue_country_norm.upper() if venue_country_norm is not None else None
            ),
        )

    @staticmethod
    def _status_record(
        evidence: ListingEvidence,
        status: ListingStatus,
        rule: Any,
        raw_fetched_at: str,
    ) -> SecurityListingStatusRecord:
        """Pair a resolver verdict with the provenance the CLI reports."""

        return SecurityListingStatusRecord(
            security_id=evidence.listing_id,
            source_provider=_PRIMARY_LISTING_SOURCE_PROVIDER,
            provider_symbol=evidence.provider_symbol,
            raw_fetched_at=raw_fetched_at,
            status=status,
            primary_provider_symbol=evidence.primary_ticker,
            classification_rule=rule,
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
            (row.status.value, int(row.security_id))
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

    def status_distribution(self) -> Dict[str, int]:
        """Return the stored classification counts, for before/after reporting."""

        self.initialize_schema()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT primary_listing_status AS status, COUNT(*) AS listings
                FROM listing
                GROUP BY primary_listing_status
                """
            ).fetchall()
        return {str(row["status"]): int(row["listings"]) for row in rows}

    def upsert_many_from_fundamentals_updates(
        self,
        provider: str,
        updates: Sequence[FundamentalsUpdate],
        *,
        connection: Optional[sqlite3.Connection] = None,
    ) -> List[SecurityListingStatusRecord]:
        """Classify freshly ingested payloads from their own evidence alone.

        Ingest holds one payload at a time and cannot see the ISIN peer group
        without re-reading other listings' blobs, so only the per-listing rules
        run here; anything they cannot settle is written as ``unknown`` rather
        than guessed. That is deliberately conservative -- ``unknown`` stays
        eligible for primary-only scopes, so an unsettled listing is merely
        unfiltered until ``reconcile-listing-status`` resolves it against the
        whole graph, never silently dropped from the universe.
        """

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
            # reads General directly; the reconcile path extracts the same
            # fields in SQL. Both feed the shared resolver.
            general = payload.get("General") if isinstance(payload, Mapping) else None
            general_map: Mapping[str, Any] = (
                general if isinstance(general, Mapping) else {}
            )
            evidence = self._build_evidence(
                security_id=update.security_id,
                provider_symbol=update.provider_symbol,
                primary_ticker=general_map.get("PrimaryTicker"),
                home_category=general_map.get("HomeCategory"),
            )
            outcome = classify_listing_without_peers(evidence)
            records.append(
                self._status_record(
                    evidence,
                    outcome.status,
                    outcome.rule,
                    update.last_fetched_at,
                )
            )
        self.upsert_many(records, connection=connection)
        return records

    # Evidence projection shared by the scoped read and the peer expansion.
    # ``PrimaryTicker`` / ``HomeCategory`` / the venue tier and head office are
    # pulled with ``json_extract`` so each ~228 KB raw payload is parsed inside
    # SQLite and never crosses into Python. ISIN comes from ``listing.isin``
    # instead of the payload: the catalog refresh populates it for listings
    # whose payload omits the field, making the column the better source.
    _EVIDENCE_SELECT = """
        SELECT
            pl.listing_id AS security_id,
            pl.provider_symbol || '.' || px.provider_exchange_code
                AS provider_symbol,
            fr.last_fetched_at AS last_fetched_at,
            l.isin AS isin,
            l.lei AS lei,
            px.country_iso2 AS venue_country_iso2,
            json_extract(fr.data, '$.General.PrimaryTicker') AS primary_ticker,
            json_extract(fr.data, '$.General.HomeCategory') AS home_category,
            json_extract(fr.data, '$.General.Exchange') AS venue_tier,
            json_extract(fr.data, '$.General.AddressData.Country') AS hq_country
        FROM fundamentals_raw fr
        JOIN provider_listing pl
          ON pl.provider_listing_id = fr.provider_listing_id
        JOIN provider_exchange px
          ON px.provider_exchange_id = pl.provider_exchange_id
        JOIN provider p ON p.provider_id = px.provider_id
        JOIN listing l ON l.listing_id = pl.listing_id
        WHERE p.provider_code = ?
    """

    def _evidence_rows(
        self,
        conn: sqlite3.Connection,
        *,
        exchange_codes: Sequence[str] = (),
        symbols_chunk: Sequence[str] = (),
        security_chunk: Sequence[int] = (),
        isin_chunk: Sequence[str] = (),
        lei_chunk: Sequence[str] = (),
    ) -> List[sqlite3.Row]:
        """Read the evidence projection for one scope slice."""

        params: List[Any] = [_PRIMARY_LISTING_SOURCE_PROVIDER]
        query = [self._EVIDENCE_SELECT]
        if exchange_codes:
            placeholders = ", ".join("?" for _ in exchange_codes)
            query.append(f"AND px.provider_exchange_code IN ({placeholders})")
            params.extend(exchange_codes)
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
        if isin_chunk:
            placeholders = ", ".join("?" for _ in isin_chunk)
            # Seeks idx_listing_isin (partial, covering).
            query.append(f"AND l.isin IN ({placeholders})")
            params.extend(isin_chunk)
        if lei_chunk:
            # No index on listing.lei: this scan runs once per reconcile over a
            # 76k-row table, and an index would cost a write on every catalog
            # refresh to serve one query. Revisit if issuer identity starts
            # seeking by LEI.
            placeholders = ", ".join("?" for _ in lei_chunk)
            query.append(f"AND l.lei IN ({placeholders})")
            params.extend(lei_chunk)
        return list(conn.execute(" ".join(query), params).fetchall())

    def _evidence_from_row(self, row: sqlite3.Row) -> ListingEvidence:
        return self._build_evidence(
            security_id=int(row["security_id"]),
            provider_symbol=str(row["provider_symbol"]),
            primary_ticker=row["primary_ticker"],
            home_category=row["home_category"],
            isin=row["isin"],
            lei=row["lei"],
            venue_tier=row["venue_tier"],
            hq_country=row["hq_country"],
            venue_country_iso2=row["venue_country_iso2"],
        )

    def reconcile_eodhd_fundamentals(
        self,
        *,
        provider_symbols: Optional[Sequence[str]] = None,
        exchange_codes: Optional[Sequence[str]] = None,
        security_ids: Optional[Sequence[int]] = None,
        chunk_size: int = 500,
    ) -> List[SecurityListingStatusRecord]:
        """Re-derive classification for a scope and write it back.

        Only listings *in scope* are written, but the resolver is fed their ISIN
        peers as well. Without that expansion a narrow ``--symbols`` run would
        silently under-fire the peer rules -- ``0K10.LSE`` alone looks like a
        listing with no evidence, while ``0K10.LSE`` beside ``MTD.US`` is
        plainly the secondary line. Peers are read-only context; they keep their
        stored status.
        """

        self.initialize_schema()
        normalized_symbols = _normalized_codes(provider_symbols)
        normalized_exchanges = _normalized_codes(exchange_codes)
        normalized_security_ids = sorted(
            {int(security_id) for security_id in security_ids or () if security_id}
        )

        in_scope: Dict[int, sqlite3.Row] = {}
        with self._connect() as conn:
            if normalized_symbols:
                for chunk in _batched(normalized_symbols, chunk_size):
                    for row in self._evidence_rows(conn, symbols_chunk=chunk):
                        in_scope[int(row["security_id"])] = row
            elif normalized_security_ids:
                for chunk in _batched(normalized_security_ids, chunk_size):
                    for row in self._evidence_rows(conn, security_chunk=chunk):
                        in_scope[int(row["security_id"])] = row
            else:
                for row in self._evidence_rows(
                    conn, exchange_codes=normalized_exchanges
                ):
                    in_scope[int(row["security_id"])] = row

            evidence_by_id: Dict[int, ListingEvidence] = {
                listing_id: self._evidence_from_row(row)
                for listing_id, row in in_scope.items()
            }

            # Pull in every sibling the scope did not already cover. ISIN feeds
            # the peer rules; LEI additionally feeds the sole-listing rescue,
            # which must be able to see a surviving sibling that shares an
            # issuer but not a security.
            missing_peers: Dict[int, ListingEvidence] = {}

            def _absorb(rows: Sequence[sqlite3.Row]) -> None:
                for row in rows:
                    listing_id = int(row["security_id"])
                    if listing_id in evidence_by_id or listing_id in missing_peers:
                        continue
                    missing_peers[listing_id] = self._evidence_from_row(row)

            scoped_isins = sorted(
                {
                    evidence.isin
                    for evidence in evidence_by_id.values()
                    if evidence.isin is not None
                }
            )
            for isin_chunk in _batched(scoped_isins, chunk_size):
                _absorb(self._evidence_rows(conn, isin_chunk=isin_chunk))

            scoped_leis = sorted(
                {
                    evidence.lei
                    for evidence in evidence_by_id.values()
                    if evidence.lei is not None
                }
            )
            for lei_chunk in _batched(scoped_leis, chunk_size):
                _absorb(self._evidence_rows(conn, lei_chunk=lei_chunk))

        resolved = classify_listings(
            list(evidence_by_id.values()) + list(missing_peers.values())
        )

        records: List[SecurityListingStatusRecord] = [
            self._status_record(
                evidence_by_id[listing_id],
                resolved[listing_id].status,
                resolved[listing_id].rule,
                str(in_scope[listing_id]["last_fetched_at"]),
            )
            # Sorted by provider symbol: callers and a test rely on the order,
            # and it keeps the CLI's per-rule report stable between runs.
            for listing_id in sorted(
                evidence_by_id, key=lambda key: evidence_by_id[key].provider_symbol
            )
        ]
        self.upsert_many(records)
        return records


__all__ = ["SecurityListingStatusRepository"]
