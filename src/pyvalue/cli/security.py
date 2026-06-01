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
    EntityMetadataRepository,
    FundamentalsRepository,
    SecurityRepository,
    SecurityMetadataUpdate,
)

from ._common import (
    SECURITY_METADATA_CHUNK_SIZE,
    SECURITY_METADATA_PROGRESS_INTERVAL_SECONDS,
    _print_symbol_progress,
    _resolve_canonical_scope_symbols,
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
    canonical_symbols, _, _ = _resolve_canonical_scope_symbols(
        str(db_path),
        symbols,
        exchange_codes,
        all_supported,
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    security_repo = SecurityRepository(db_path)
    security_repo.initialize_schema()
    security_ids_by_symbol = security_repo.resolve_ids_many(
        canonical_symbols,
        chunk_size=SECURITY_METADATA_CHUNK_SIZE,
    )
    scoped_rows = [
        (symbol, security_ids_by_symbol.get(symbol)) for symbol in canonical_symbols
    ]

    updated = 0
    skipped_no_raw = 0
    skipped_no_metadata = 0
    unchanged = 0
    completed_symbols = 0
    total_symbols = len(canonical_symbols)
    last_progress_at = time.monotonic()
    last_reported_completed = -1
    pending_updates: List[SecurityMetadataUpdate] = []

    def maybe_report_progress(force: bool = False) -> None:
        nonlocal last_progress_at, last_reported_completed
        if total_symbols <= 0:
            return
        if completed_symbols == last_reported_completed:
            return
        elapsed = time.monotonic() - last_progress_at
        if not force and elapsed < SECURITY_METADATA_PROGRESS_INTERVAL_SECONDS:
            return
        _print_symbol_progress(completed_symbols, total_symbols)
        last_reported_completed = completed_symbols
        last_progress_at = time.monotonic()

    def flush_pending() -> None:
        nonlocal updated
        if not pending_updates:
            return
        updated += entity_repo.upsert_many(pending_updates)
        pending_updates.clear()

    try:
        for start in range(0, len(scoped_rows), SECURITY_METADATA_CHUNK_SIZE):
            chunk = scoped_rows[start : start + SECURITY_METADATA_CHUNK_SIZE]
            chunk_symbols = [
                symbol for symbol, security_id in chunk if security_id is not None
            ]
            existing_metadata = entity_repo.fetch_many(chunk_symbols)
            extracted_metadata = fund_repo.fetch_metadata_candidates(
                [
                    int(security_id)
                    for _, security_id in chunk
                    if security_id is not None
                ],
                chunk_size=SECURITY_METADATA_CHUNK_SIZE,
            )

            for symbol, security_id in chunk:
                if security_id is None:
                    skipped_no_raw += 1
                    completed_symbols += 1
                    maybe_report_progress()
                    continue

                metadata_candidate = extracted_metadata.get(int(security_id))
                if metadata_candidate is None:
                    skipped_no_raw += 1
                    completed_symbols += 1
                    maybe_report_progress()
                    continue

                update = metadata_candidate.to_update_fields()
                if not update:
                    skipped_no_metadata += 1
                    completed_symbols += 1
                    maybe_report_progress()
                    continue

                current = existing_metadata.get(symbol)
                if current is not None and all(
                    getattr(current, field_name) == field_value
                    for field_name, field_value in update.items()
                ):
                    unchanged += 1
                    completed_symbols += 1
                    maybe_report_progress()
                    continue

                pending_updates.append(
                    SecurityMetadataUpdate(
                        security_id=int(security_id),
                        entity_name=metadata_candidate.entity_name,
                        description=metadata_candidate.description,
                        sector=metadata_candidate.sector,
                        industry=metadata_candidate.industry,
                    )
                )
                completed_symbols += 1
                maybe_report_progress()

            flush_pending()
    except KeyboardInterrupt:
        return _cancel_cli_command(
            "\nSecurity metadata refresh cancelled by user after "
            f"{completed_symbols} of {total_symbols} symbols.",
            flushers=[flush_pending, lambda: maybe_report_progress(force=True)],
        )

    flush_pending()
    maybe_report_progress(force=True)
    print(f"Scanned {len(canonical_symbols)} symbols.")
    print(f"Updated metadata for {updated} symbols.")
    print(f"Skipped with no raw payload: {skipped_no_raw}")
    print(f"Skipped with no extractable metadata: {skipped_no_metadata}")
    print(f"No metadata changes needed: {unchanged}")
    return 0
