"""Security listing-status (primary/secondary) repository.

Author: Emre Tezel

The classification *rule* lives in :mod:`pyvalue.universe.listing_classification`
-- pure, DB-free domain logic. This module owns only the data movement: pulling
the evidence each rule needs out of SQLite, handing it to the resolver, and
writing the verdicts back to ``listing.primary_listing_status``.
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
)


from pyvalue.identifiers import shaped_isin, shaped_lei
from pyvalue.universe.listing_classification import (
    ListingEvidence,
    ListingStatus,
)

from .base import (
    SQLiteStore,
    _PRIMARY_LISTING_SOURCE_PROVIDER,
    _batched,
    _normalize_optional_text,
    _normalize_qualified_symbol,
    _normalize_symbol_base,
    _utc_now_iso,
)
from .records import SecurityListingStatusRecord
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
            px.country_iso2 AS venue_country_iso2,
            json_extract(fr.data, '$.General.PrimaryTicker') AS primary_ticker,
            json_extract(fr.data, '$.General.HomeCategory') AS home_category,
            json_extract(fr.data, '$.General.LEI') AS lei,
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

    def load_evidence(
        self, conn: sqlite3.Connection, listing_ids: Iterable[int]
    ) -> List[ListingEvidence]:
        """Read classification evidence for the given listings.

        The caller passes a neighbourhood -- a set closed under the peer
        relation -- so the rules see everything they need and nothing they do
        not. Scoping used to be expressed here as symbol/exchange/ISIN/LEI
        filters with a peer-expansion pass bolted on; that job moved wholesale
        to ``resolve_neighbourhood``, which does it once for both derived
        values instead of once per consumer.
        """

        wanted = sorted({int(value) for value in listing_ids})
        evidence: List[ListingEvidence] = []
        for chunk in _batched(wanted, 500):
            placeholders = ", ".join("?" for _ in chunk)
            rows = conn.execute(
                f"{self._EVIDENCE_SELECT} AND pl.listing_id IN ({placeholders})",
                [_PRIMARY_LISTING_SOURCE_PROVIDER, *chunk],
            ).fetchall()
            evidence.extend(self._evidence_from_row(row) for row in rows)
        return evidence

    def last_fetched_at(
        self, conn: sqlite3.Connection, listing_ids: Iterable[int]
    ) -> Dict[int, str]:
        """Provenance stamps for the records the CLI reports."""

        wanted = sorted({int(value) for value in listing_ids})
        stamps: Dict[int, str] = {}
        for chunk in _batched(wanted, 500):
            placeholders = ", ".join("?" for _ in chunk)
            for row in conn.execute(
                "SELECT pl.listing_id AS listing_id, fr.last_fetched_at AS stamp "
                "FROM fundamentals_raw fr "
                "JOIN provider_listing pl "
                "  ON pl.provider_listing_id = fr.provider_listing_id "
                f"WHERE pl.listing_id IN ({placeholders})",
                list(chunk),
            ):
                stamps[int(row["listing_id"])] = str(row["stamp"])
        return stamps

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

    def status_record(
        self,
        evidence: ListingEvidence,
        status: ListingStatus,
        rule: Any,
        raw_fetched_at: str,
    ) -> SecurityListingStatusRecord:
        """Public alias for the record builder the reconciler assembles."""

        return self._status_record(evidence, status, rule, raw_fetched_at)


__all__ = ["SecurityListingStatusRepository"]
