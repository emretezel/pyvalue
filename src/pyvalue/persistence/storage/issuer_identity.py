"""Issuer-identity reconciliation repository.

Author: Emre Tezel

The grouping *rule* lives in :mod:`pyvalue.universe.issuer_identity` -- pure,
DB-free domain logic. This module owns only the data movement: reading each
listing's stored identity evidence, handing it to the grouper, and collapsing
the ``issuer`` rows it says belong together.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, List, Tuple

from pyvalue.identifiers import shaped_isin, shaped_lei
from pyvalue.universe.issuer_identity import (
    IssuerGroup,
    ListingIdentity,
    group_listings,
)

from .base import SQLiteStore, _batched
from .migrations import apply_migrations


@dataclass(frozen=True)
class IssuerReconciliationResult:
    """What one ``reconcile-issuer-identity`` run changed."""

    listings_considered: int
    issuers_before: int
    issuers_after: int
    groups_merged: int
    listings_repointed: int
    issuers_deleted: int
    leis_assigned: int

    @property
    def issuers_removed(self) -> int:
        """Issuer rows the run collapsed away."""

        return self.issuers_before - self.issuers_after


class IssuerIdentityRepository(SQLiteStore):
    """Derive and persist which listings belong to the same legal entity."""

    def initialize_schema(self) -> None:
        apply_migrations(self.db_path)

    def _load_identities(self, conn: sqlite3.Connection) -> List[ListingIdentity]:
        """Read every listing's entity evidence.

        Deliberately unscoped: grouping is only as good as the set it sees, and
        a partial view would split entities rather than merge them.

        ``isin`` is a canonical column -- it sits at listing grain because a
        listing quotes exactly one security, and the catalog refresh populates
        it. ``lei`` is read straight out of the stored payload instead. It used
        to be cached on ``listing`` too, until that turned out to duplicate
        ``issuer.lei``: once this command converges, a listing's LEI is
        functionally determined by its issuer, which is a transitive dependency
        and a 3NF violation. Reading the payload keeps one copy of the fact, in
        the place it originates.

        That choice is what makes this a payload read rather than an indexed
        one: ~3s became ~97s on the live catalog. ``json_extract`` runs inside
        SQLite so no blob crosses into Python, and the subquery seeks rather than
        scans, which is why it costs a fraction of the ~780s the classification
        reconcile spends over the same payloads.

        The payload is reached by a scalar subquery rather than a join, so one
        listing yields exactly one row. A join would fan out if a listing ever
        carried two provider mappings, silently duplicating it inside a group.
        It also keeps listings with no stored payload: those still have an ISIN
        and a current issuer and must be grouped, not dropped -- ~4,400 catalog
        holdovers are in exactly that state.
        """

        rows = conn.execute(
            """
            SELECT
                l.listing_id AS listing_id,
                l.issuer_id AS issuer_id,
                l.isin AS isin,
                (
                    SELECT json_extract(fr.data, '$.General.LEI')
                    FROM provider_listing pl
                    JOIN fundamentals_raw fr
                      ON fr.provider_listing_id = pl.provider_listing_id
                    WHERE pl.listing_id = l.listing_id
                    LIMIT 1
                ) AS lei,
                i.name AS entity_name,
                l.primary_listing_status AS status
            FROM listing l
            JOIN issuer i ON i.issuer_id = l.issuer_id
            """
        ).fetchall()
        return [
            ListingIdentity(
                listing_id=int(row["listing_id"]),
                issuer_id=int(row["issuer_id"]),
                isin=shaped_isin(row["isin"]),
                lei=shaped_lei(row["lei"]),
                entity_name=row["entity_name"],
                is_primary=str(row["status"]) == "primary",
            )
            for row in rows
        ]

    @staticmethod
    def _issuer_count(conn: sqlite3.Connection) -> int:
        return int(conn.execute("SELECT COUNT(*) FROM issuer").fetchone()[0])

    def _resolve_target(self, conn: sqlite3.Connection, group: IssuerGroup) -> int:
        """Return the issuer row this group collapses onto, creating one if needed.

        The grouper hands back ``None`` when every issuer row the group touches
        has already been claimed by a different group -- which happens when a
        pre-existing issuer owned listings belonging to two unrelated entities.
        Splitting them is the whole point, so the group gets a fresh row rather
        than being fused into its neighbour.

        The new row is created bare. Its name and LEI are applied later, in the
        labelling pass, under the same conflict checks every other group gets.
        """

        if group.representative_issuer_id is not None:
            return group.representative_issuer_id
        # ``name`` is NOT NULL; the labelling pass promotes the real one once the
        # emptied rows are gone. A listing id is a guaranteed-unique placeholder.
        placeholder = f"issuer:{group.listing_ids[0]}"
        cursor = conn.execute(
            "INSERT INTO issuer (name) VALUES (?)",
            (placeholder,),
        )
        if cursor.lastrowid is None:
            raise RuntimeError(
                f"failed to allocate an issuer row for listings {group.listing_ids}"
            )
        return int(cursor.lastrowid)

    def _promote_and_repoint(
        self, conn: sqlite3.Connection, group: IssuerGroup
    ) -> Tuple[int, int]:
        """Move one group's listings onto its representative issuer.

        Returns ``(resolved_issuer_id, listings_repointed)``. The resolved id is
        handed back because the grouper may not have supplied one, and the
        labelling pass needs the row this group actually landed on.

        Metadata follows migration 060's never-overwrite rule: the survivor
        keeps every non-NULL value it already has and only fills its gaps from
        the rows being merged away. A merge must not lose a sector or industry
        that only the absorbed row happened to carry.

        Deleting the emptied rows is deliberately *not* done here. An issuer can
        own listings that end up in different groups -- the pre-existing catalog
        has 3,991 multi-listing issuers, and nothing guaranteed their listings
        shared an identifier -- so a per-group delete would drop a row another
        group's listings still point at. That is not hypothetical: it failed the
        FK check on the live catalog. Deletion happens once, after every group
        has been repointed, in :meth:`_delete_orphaned_issuers`.
        """

        target = self._resolve_target(conn, group)
        absorbed = [
            issuer_id for issuer_id in group.merged_issuer_ids if issuer_id != target
        ]

        if absorbed:
            placeholders = ", ".join("?" for _ in absorbed)
            conn.execute(
                f"""
                UPDATE issuer
                SET description = COALESCE(description, (
                        SELECT description FROM issuer
                        WHERE issuer_id IN ({placeholders})
                          AND description IS NOT NULL
                        ORDER BY issuer_id LIMIT 1
                    )),
                    sector = COALESCE(sector, (
                        SELECT sector FROM issuer
                        WHERE issuer_id IN ({placeholders})
                          AND sector IS NOT NULL
                        ORDER BY issuer_id LIMIT 1
                    )),
                    industry = COALESCE(industry, (
                        SELECT industry FROM issuer
                        WHERE issuer_id IN ({placeholders})
                          AND industry IS NOT NULL
                        ORDER BY issuer_id LIMIT 1
                    )),
                    country = COALESCE(country, (
                        SELECT country FROM issuer
                        WHERE issuer_id IN ({placeholders})
                          AND country IS NOT NULL
                        ORDER BY issuer_id LIMIT 1
                    ))
                WHERE issuer_id = ?
                """,
                (*absorbed, *absorbed, *absorbed, *absorbed, target),
            )

        repointed = 0
        for chunk in _batched(sorted(group.listing_ids), 500):
            placeholders = ", ".join("?" for _ in chunk)
            cursor = conn.execute(
                f"""
                UPDATE listing SET issuer_id = ?
                WHERE listing_id IN ({placeholders}) AND issuer_id != ?
                """,
                (target, *chunk, target),
            )
            repointed += cursor.rowcount if cursor.rowcount > 0 else 0

        return target, repointed

    @staticmethod
    def _delete_orphaned_issuers(
        conn: sqlite3.Connection, issuer_ids: Iterable[int]
    ) -> int:
        """Delete the given issuer rows that no listing points at any more.

        Run once, after all repointing. An issuer survives exactly when it still
        owns a listing, which is the only condition the FK cares about, so this
        cannot fail the way a per-group delete did.

        Scoped rather than global because this now runs per ingest batch as well
        as per reconcile: an unqualified ``NOT EXISTS`` walks the whole ``issuer``
        table, which is fine once but would mean ~2,900 scans of ~59k rows during
        a bootstrap. Only rows a repoint could have emptied are worth probing,
        and the caller knows which those are. A whole-universe run simply passes
        them all, so there is no second variant to keep in step.
        """

        candidates = sorted({int(value) for value in issuer_ids if value})
        deleted = 0
        for chunk in _batched(candidates, 500):
            placeholders = ", ".join("?" for _ in chunk)
            cursor = conn.execute(
                f"""
                DELETE FROM issuer
                WHERE issuer_id IN ({placeholders})
                  AND NOT EXISTS (
                      SELECT 1 FROM listing
                      WHERE listing.issuer_id = issuer.issuer_id
                  )
                """,
                list(chunk),
            )
            deleted += cursor.rowcount if cursor.rowcount > 0 else 0
        return deleted

    @staticmethod
    def _label_representative(
        conn: sqlite3.Connection, group: IssuerGroup, target: int
    ) -> bool:
        """Give a surviving representative its group's name and LEI.

        Runs after the emptied rows are gone, so the LEI a survivor is adopting
        is never still held by a row about to be deleted.

        The name is written unconditionally. Migration 089 dropped
        ``UNIQUE (name, country)`` precisely so it can be: names are descriptive
        metadata, not identity, and two unrelated entities may legitimately share
        one. The LEI probe stays because ``issuer.lei`` *is* UNIQUE -- though
        ``group_listings`` gives each LEI to exactly one group, so a collision
        can only be a stale value left by an earlier run under different
        evidence.

        A group that no longer has an LEI has its stored one **cleared**, not
        left alone. That is the state a changed LEI creates: the listing whose
        LEI justified the value has moved on, and a stale value is worse than
        none because ``issuer.lei`` is what the neighbourhood search seeks --
        it would pull the next listing publishing that LEI into this group.
        Clearing errs toward two issuers for one company, which is harmless and
        self-heals on the next pass; keeping errs toward one issuer for two
        companies, which is a wrong merge.

        Returns whether an LEI was assigned.
        """

        if group.name is not None:
            conn.execute(
                "UPDATE issuer SET name = ? WHERE issuer_id = ? AND name != ?",
                (group.name, target, group.name),
            )

        if group.lei is None:
            conn.execute(
                "UPDATE issuer SET lei = NULL WHERE issuer_id = ? AND lei IS NOT NULL",
                (target,),
            )
            return False
        held_elsewhere = conn.execute(
            "SELECT 1 FROM issuer WHERE lei = ? AND issuer_id != ?",
            (group.lei, target),
        ).fetchone()
        if held_elsewhere is not None:
            return False
        cursor = conn.execute(
            "UPDATE issuer SET lei = ? WHERE issuer_id = ? AND lei IS NOT ?",
            (group.lei, target, group.lei),
        )
        return cursor.rowcount > 0

    def reconcile(self) -> IssuerReconciliationResult:
        """Re-derive issuer identity across the whole catalog.

        Runs as one transaction: a half-merged catalog would leave listings
        pointing at issuer rows that no longer exist, and the FK would be the
        only thing to notice.

        Three ordered passes, and the order is what makes it correct:

        1. Promote metadata and repoint every group's listings.
        2. Delete the issuer rows nothing points at any more. Deferred to here
           because an issuer's listings can land in different groups, so no
           single group can know when the row is finally empty.
        3. Name the survivors and assign their LEIs, now that the rows whose
           names are being adopted are gone and UNIQUE (name, country) is free.

        Re-runnable and convergent. Groups that already match the stored shape
        issue no writes, so a second run on an unchanged catalog is a no-op, and
        a run after ISIN/LEI coverage improves merges only what newly became
        linkable.
        """

        self.initialize_schema()
        with self._connect() as conn:
            identities = self._load_identities(conn)
            issuers_before = self._issuer_count(conn)

            groups = group_listings(identities)
            merged = 0
            repointed = 0
            leis = 0
            targets: List[int] = []
            for group in groups:
                target, group_repointed = self._promote_and_repoint(conn, group)
                targets.append(target)
                repointed += group_repointed
                if group.merges:
                    merged += 1

            # Whole-catalog run: every issuer a repoint could have emptied is,
            # by definition, one of the ones these listings used to point at.
            deleted = self._delete_orphaned_issuers(
                conn, {identity.issuer_id for identity in identities}
            )

            for group, target in zip(groups, targets):
                leis += int(self._label_representative(conn, group, target))

            issuers_after = self._issuer_count(conn)

        return IssuerReconciliationResult(
            listings_considered=len(identities),
            issuers_before=issuers_before,
            issuers_after=issuers_after,
            groups_merged=merged,
            listings_repointed=repointed,
            issuers_deleted=deleted,
            leis_assigned=leis,
        )


__all__ = ["IssuerIdentityRepository", "IssuerReconciliationResult"]
