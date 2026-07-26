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
from typing import Iterable, List, Optional, Sequence, Set

from .base import _batched

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


__all__ = ["issuer_ids_for", "resolve_neighbourhood"]
