"""Neighbourhood resolution for universe reconciliation.

Author: Emre Tezel

Two derived values -- ``listing.primary_listing_status`` and issuer identity --
are not properties of a listing on its own. Both depend on what *else* the
catalog holds:

* classification's peer rules need every listing sharing an ISIN (one of them
  may name another as primary, and one security cannot have two primary lines);
* issuer grouping needs every listing sharing an ISIN or an LEI, plus the
  current members of the issuer rows involved, because repartitioning a group
  means seeing all of it.

So neither can be settled by looking at one listing. This module answers the
question both need: *given some listings, which others could change as a
result?* That set is the *neighbourhood*, and computing it is what lets a
narrow run -- a single ingest batch, or ``--symbols AAPL.US`` -- reach the same
answer a whole-universe pass would.

The closure is deliberately over stored columns and indexes only. Payload
evidence is loaded once, afterwards, for whatever the closure returns.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Union

from pyvalue.universe.issuer_identity import group_listings
from pyvalue.universe.listing_classification import classify_listings

from .base import _batched
from .issuer_identity import IssuerIdentityRepository
from .listing_status import SecurityListingStatusRepository
from .records import SecurityListingStatusRecord

logger = logging.getLogger(__name__)


# Safety valve on the fixpoint. Each round can only add listings reachable by
# one more identifier hop, and real groups are tiny -- 37,120 ISIN groups over
# 53,500 listings on the live catalog, mean 1.44 listings each, largest 15; the
# largest issuer group is 25. Eight rounds is far beyond anything observed, so
# hitting the cap means the data has a pathological chain worth knowing about
# rather than silently walking.
_MAX_ROUNDS: int = 8

_CHUNK_SIZE: int = 500


def _query_ids(conn: sqlite3.Connection, sql: str, chunk: Sequence[int]) -> Set[int]:
    """Run one closure query over a chunk of listing ids."""

    placeholders = ", ".join("?" for _ in chunk)
    rows = conn.execute(sql.format(placeholders=placeholders), list(chunk))
    return {int(row[0]) for row in rows}


# Listings quoting the same security. This is what lets classification's peer
# rules fire and what keeps a security's venues on one issuer. Seeks
# idx_listing_isin, which is partial -- listings with no ISIN have no peers by
# definition and are correctly skipped.
_SAME_ISIN_SQL = """
    SELECT other.listing_id
    FROM listing other
    WHERE other.isin IN (
        SELECT seed.isin FROM listing seed
        WHERE seed.listing_id IN ({placeholders})
          AND seed.isin IS NOT NULL
    )
"""


# Listings currently parented by the same issuer. Needed for *splits*: to
# repartition a group correctly you have to see every member, not just the one
# whose evidence changed. Seeks idx_listing_issuer.
_SAME_ISSUER_SQL = """
    SELECT other.listing_id
    FROM listing other
    WHERE other.issuer_id IN (
        SELECT seed.issuer_id FROM listing seed
        WHERE seed.listing_id IN ({placeholders})
    )
"""


# Listings of the issuer that already holds the LEI this listing's payload
# publishes -- the entity it may be about to join, which neither of the links
# above can reach. ``issuer.lei`` is UNIQUE, so finding that issuer is a single
# index seek rather than a scan of the payloads.
_PAYLOAD_LEI_SQL = """
    SELECT other.listing_id
    FROM listing other
    WHERE other.issuer_id IN (
        SELECT i.issuer_id FROM issuer i
        WHERE i.lei IN (
            SELECT json_extract(fr.data, '$.General.LEI')
            FROM fundamentals_raw fr
            JOIN provider_listing pl
              ON pl.provider_listing_id = fr.provider_listing_id
            WHERE pl.listing_id IN ({placeholders})
        )
    )
"""


def _all_listing_ids(conn: sqlite3.Connection) -> Set[int]:
    return {int(row[0]) for row in conn.execute("SELECT listing_id FROM listing")}


def resolve_neighbourhood(
    conn: sqlite3.Connection,
    listing_ids: Optional[Iterable[int]] = None,
    *,
    max_rounds: int = _MAX_ROUNDS,
) -> Set[int]:
    """Return the listings whose derived values could change with ``listing_ids``.

    Args:
        conn: Open connection. No transaction is started here -- callers run this
            inside the transaction that will apply the result.
        listing_ids: The seed. ``None`` means the whole universe, which is its
            own neighbourhood and is returned without any closure work.
        max_rounds: Fixpoint safety cap; exceeding it logs a warning and returns
            what has been reached so far.

    Returns:
        A superset of the seed, closed (up to the cap) under "shares an ISIN
        with", "shares an issuer with", and "belongs to the issuer holding the
        LEI my payload publishes".

    The closure has to be transitive: a listing joining an entity drags in that
    entity's other listings, whose ISINs may reach further listings still. Doing
    one hop would leave a partition that disagrees with what a whole-universe
    pass computes, which is exactly the order-dependence this exists to remove.
    """

    if listing_ids is None:
        return _all_listing_ids(conn)

    neighbourhood: Set[int] = {int(value) for value in listing_ids}
    if not neighbourhood:
        return neighbourhood

    frontier: Set[int] = set(neighbourhood)
    for round_number in range(max_rounds):
        discovered: Set[int] = set()
        for chunk in _batched(sorted(frontier), _CHUNK_SIZE):
            for sql in (_SAME_ISIN_SQL, _SAME_ISSUER_SQL, _PAYLOAD_LEI_SQL):
                discovered |= _query_ids(conn, sql, chunk)
        # Only the newly reached listings can reach anything further, so the
        # next round expands the frontier rather than the whole set.
        frontier = discovered - neighbourhood
        if not frontier:
            return neighbourhood
        neighbourhood |= frontier
        if round_number == max_rounds - 1:
            logger.warning(
                "neighbourhood closure hit the %d-round cap with %d listings "
                "and %d still unexplored; identifier data may contain a "
                "pathological chain",
                max_rounds,
                len(neighbourhood),
                len(frontier),
            )
    return neighbourhood


def issuer_ids_for(conn: sqlite3.Connection, listing_ids: Iterable[int]) -> Set[int]:
    """Return the issuer rows the given listings currently point at.

    The apply step needs this to scope its orphan cleanup: an issuer can only
    have been emptied by a repoint within this neighbourhood, so those are the
    only rows worth probing.
    """

    wanted: List[int] = sorted({int(value) for value in listing_ids})
    if not wanted:
        return set()
    issuers: Set[int] = set()
    for chunk in _batched(wanted, _CHUNK_SIZE):
        placeholders = ", ".join("?" for _ in chunk)
        issuers |= {
            int(row[0])
            for row in conn.execute(
                "SELECT DISTINCT issuer_id FROM listing "
                f"WHERE listing_id IN ({placeholders})",
                list(chunk),
            )
        }
    return issuers


__all__ = [
    "ReconcileResult",
    "UniverseReconciler",
    "issuer_ids_for",
    "resolve_neighbourhood",
]


@dataclass(frozen=True)
class ReconcileResult:
    """Everything one reconciliation pass settled.

    ``listing_records`` carries the classification verdicts with the rule that
    produced each, so the CLI can report *why* the universe moved -- which is
    what an operator needs when a rule change reclassifies tens of thousands of
    rows.
    """

    listings_considered: int
    listing_records: List[SecurityListingStatusRecord]
    groups_merged: int
    listings_repointed: int
    issuers_created: int
    issuers_deleted: int
    leis_assigned: int


class UniverseReconciler:
    """Settle both derived values for a scope, in one pass.

    Classification and issuer identity are not independent, so they are not
    computed independently. Grouping names a merged issuer after its *primary*
    listing, which means it needs the fresh classification; running one without
    the other leaves the catalog internally inconsistent. They were built in
    separate phases and that is the only reason they were ever separate.

    Every caller goes through here -- the two reconcile commands and, per write
    batch, fundamentals ingest -- so there is exactly one orchestration and no
    second implementation to drift from it. What differs between callers is only
    the seed:

    * ingest passes the listings whose payloads it just stored;
    * ``reconcile-listing-status`` passes its ``--symbols``/``--exchange-codes``
      scope, or None;
    * ``reconcile-issuer-identity`` passes None.

    A seed is expanded to its neighbourhood first, so a narrow run reaches the
    same verdict a whole-universe pass would. That is what makes ingestion order
    irrelevant: whichever member of a group arrives last re-evaluates all of it.
    """

    def __init__(self, db_path: Union[str, Path]) -> None:
        self._status_repo = SecurityListingStatusRepository(db_path)
        self._issuer_repo = IssuerIdentityRepository(db_path)

    def initialize_schema(self) -> None:
        self._status_repo.initialize_schema()
        self._issuer_repo.initialize_schema()

    def apply(
        self,
        conn: sqlite3.Connection,
        listing_ids: Optional[Iterable[int]] = None,
    ) -> ReconcileResult:
        """Reconcile ``listing_ids`` and their neighbourhood on ``conn``.

        Runs on the caller's connection so it joins whatever transaction is
        open. Ingest relies on that: the payloads, the statuses and the issuer
        partition commit or roll back together, so a crash never leaves a stored
        payload unclassified.

        ``None`` means the whole catalog, which is its own neighbourhood.
        """

        neighbourhood = resolve_neighbourhood(conn, listing_ids)
        if not neighbourhood:
            return ReconcileResult(0, [], 0, 0, 0, 0, 0)

        # Issuers captured *before* any repoint: the rows a listing could
        # vacate, and so the only ones a scoped orphan sweep must consider.
        # A whole-catalog pass instead sweeps everything (None) -- it is the
        # right place to collect rows that were already orphaned before this
        # run, which a scoped probe can never see because they own no listing to
        # be reached through.
        vacated: Optional[Set[int]] = (
            None if listing_ids is None else issuer_ids_for(conn, neighbourhood)
        )

        evidence = self._status_repo.load_evidence(conn, neighbourhood)
        classified = classify_listings(evidence)
        stamps = self._status_repo.last_fetched_at(
            conn, (item.listing_id for item in evidence)
        )
        records = [
            self._status_repo.status_record(
                item,
                classified[item.listing_id].status,
                classified[item.listing_id].rule,
                stamps.get(item.listing_id, ""),
            )
            # Sorted by provider symbol so the CLI's report is stable between
            # runs and a test can assert on it.
            for item in sorted(evidence, key=lambda ev: ev.provider_symbol)
        ]
        self._status_repo.upsert_many(records, connection=conn)

        # Identity reads the statuses back, so it must follow the write above.
        identities = self._issuer_repo.load_identities(conn, neighbourhood)
        groups = group_listings(identities)
        applied = self._issuer_repo.apply_groups(conn, groups, vacated)

        return ReconcileResult(
            listings_considered=len(neighbourhood),
            listing_records=records,
            groups_merged=applied.groups_merged,
            listings_repointed=applied.listings_repointed,
            issuers_created=applied.issuers_created,
            issuers_deleted=applied.issuers_deleted,
            leis_assigned=applied.leis_assigned,
        )

    def reconcile(self, listing_ids: Optional[Iterable[int]] = None) -> ReconcileResult:
        """Open a transaction and reconcile, for callers that have no connection."""

        self.initialize_schema()
        with self._status_repo._connect() as conn:
            return self.apply(conn, listing_ids)
