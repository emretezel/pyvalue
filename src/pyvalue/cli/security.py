"""CLI handler for refreshing security metadata from raw provider payloads.

Author: Emre Tezel
"""

from __future__ import annotations

import time
from typing import (
    List,
    Optional,
    Sequence,
)

from pyvalue.persistence.storage import (
    FundamentalsRepository,
    SecurityMetadataUpdate,
    SecurityRepository,
)

from ._common import (
    SECURITY_METADATA_CHUNK_SIZE,
    SECURITY_METADATA_PROGRESS_INTERVAL_SECONDS,
    _print_symbol_progress,
    _resolve_canonical_scope_listings,
    _resolve_database_path,
)
from ._batch import (
    _cancel_cli_command,
)


def cmd_refresh_security_metadata(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
) -> int:
    """Refresh canonical security metadata from stored raw fundamentals only."""

    db_path = _resolve_database_path(database)
    # The scope query already holds every listing_id; this command keys entirely
    # on it -- both the raw-payload metadata candidates and the canonical metadata
    # write are id-keyed -- so only the listing ids are carried into the loop.
    scope_listings, _explicit_symbols, _resolved_exchange_codes = (
        _resolve_canonical_scope_listings(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    scoped_ids: List[int] = [listing_id for listing_id, _ in scope_listings]
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    security_repo = SecurityRepository(db_path)
    security_repo.initialize_schema()

    updated = 0
    skipped_no_raw = 0
    skipped_no_metadata = 0
    unchanged = 0
    completed = 0
    total = len(scoped_ids)
    last_progress_at = time.monotonic()
    last_reported_completed = -1
    pending_updates: List[SecurityMetadataUpdate] = []

    def maybe_report_progress(force: bool = False) -> None:
        nonlocal last_progress_at, last_reported_completed
        if total <= 0:
            return
        if completed == last_reported_completed:
            return
        elapsed = time.monotonic() - last_progress_at
        if not force and elapsed < SECURITY_METADATA_PROGRESS_INTERVAL_SECONDS:
            return
        _print_symbol_progress(completed, total)
        last_reported_completed = completed
        last_progress_at = time.monotonic()

    def flush_pending() -> None:
        nonlocal updated
        if not pending_updates:
            return
        updated += security_repo.upsert_metadata_many(pending_updates)
        pending_updates.clear()

    try:
        for start in range(0, len(scoped_ids), SECURITY_METADATA_CHUNK_SIZE):
            chunk_ids = scoped_ids[start : start + SECURITY_METADATA_CHUNK_SIZE]
            existing_metadata = security_repo.fetch_many_by_id(chunk_ids)
            extracted_metadata = fund_repo.fetch_metadata_candidates(
                chunk_ids,
                chunk_size=SECURITY_METADATA_CHUNK_SIZE,
            )

            for listing_id in chunk_ids:
                metadata_candidate = extracted_metadata.get(listing_id)
                if metadata_candidate is None:
                    skipped_no_raw += 1
                    completed += 1
                    maybe_report_progress()
                    continue

                update = metadata_candidate.to_update_fields()
                if not update:
                    skipped_no_metadata += 1
                    completed += 1
                    maybe_report_progress()
                    continue

                current = existing_metadata.get(listing_id)
                if current is not None and all(
                    getattr(current, field_name) == field_value
                    for field_name, field_value in update.items()
                ):
                    unchanged += 1
                    completed += 1
                    maybe_report_progress()
                    continue

                pending_updates.append(
                    SecurityMetadataUpdate(
                        security_id=listing_id,
                        entity_name=metadata_candidate.entity_name,
                        description=metadata_candidate.description,
                        sector=metadata_candidate.sector,
                        industry=metadata_candidate.industry,
                    )
                )
                completed += 1
                maybe_report_progress()

            flush_pending()
    except KeyboardInterrupt:
        return _cancel_cli_command(
            "\nSecurity metadata refresh cancelled by user after "
            f"{completed} of {total} symbols.",
            flushers=[flush_pending, lambda: maybe_report_progress(force=True)],
        )

    flush_pending()
    maybe_report_progress(force=True)
    print(f"Scanned {total} symbols.")
    print(f"Updated metadata for {updated} symbols.")
    print(f"Skipped with no raw payload: {skipped_no_raw}")
    print(f"Skipped with no extractable metadata: {skipped_no_metadata}")
    print(f"No metadata changes needed: {unchanged}")
    return 0
