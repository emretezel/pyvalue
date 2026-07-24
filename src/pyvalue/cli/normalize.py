"""CLI handlers for normalizing raw fundamentals into financial facts.

Author: Emre Tezel
"""

from __future__ import annotations

from concurrent.futures import (
    as_completed,
)
import os
from pathlib import Path
from typing import (
    Callable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from pyvalue.money.fx import (
    FXService,
)
from pyvalue.normalization import EODHDFactsNormalizer
from pyvalue.logging_utils import (
    current_logging_config,
    suppress_console_logging,
)
from pyvalue.persistence.storage import (
    FXRatesRepository,
    FundamentalsNormalizationStateRepository,
    FundamentalsRepository,
    FinancialFactsRepository,
    FactRecord,
    NormalizationUnit,
    SecurityMetadataUpdate,
    SecurityRepository,
    StoredFactRow,
    SupportedTickerRepository,
)

from ._common import (
    LOGGER,
    NORMALIZATION_MAX_WORKERS,
    _NormalizedFactsResult,
    _extract_entity_description_from_eodhd,
    _extract_entity_industry_from_eodhd,
    _extract_entity_name_from_eodhd,
    _extract_entity_sector_from_eodhd,
    _normalize_provider,
    _resolve_database_path,
    _resolve_provider_scope,
)
from ._batch import (
    _cancel_cli_command,
    _create_process_pool_executor,
)
from ._repos import (
    _SchemaReadyFXRatesRepository,
)


_process_local_fx_service: Optional[FXService] = None
_process_local_fx_service_db: Optional[str] = None


def cmd_normalize_fundamentals_stage(
    provider: str,
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    force: bool = False,
) -> int:
    """Unified fundamentals normalization over symbol, exchange, or full supported scope."""

    db_path = _resolve_database_path(database)
    provider_norm = _normalize_provider(provider)
    _, symbol_filters, exchange_filters = _resolve_provider_scope(
        str(db_path),
        provider_norm,
        symbols,
        exchange_codes,
        all_supported,
        primary_only=provider_norm == "EODHD",
    )
    if all_supported:
        # Whole provider: let the bulk path enumerate every unit via the id-keyed
        # full scan -- no need to hydrate the supported-ticker rows here just to
        # recover their symbols.
        return cmd_normalize_eodhd_fundamentals_bulk(
            database=str(db_path), symbols=None, force=force
        )
    # Scoped: resolve the requested scope to its (primary) provider symbols and hand
    # them to the bulk path, which converts them to id-keyed units. The has-raw
    # filter now lives in normalization_units, so the old symbols() intersection is
    # gone -- a scoped listing with no raw payload is simply absent from the units.
    ticker_repo = SupportedTickerRepository(db_path)
    scope_rows = ticker_repo.list_for_provider(
        provider_norm,
        exchange_codes=exchange_filters,
        provider_symbols=symbol_filters,
        primary_only=provider_norm == "EODHD",
    )
    selected_symbols = [row.symbol for row in scope_rows]
    if not selected_symbols:
        raise SystemExit(
            f"No {provider_norm} supported tickers in the requested scope. "
            "Run refresh-supported-tickers first."
        )
    return cmd_normalize_eodhd_fundamentals_bulk(
        database=str(db_path),
        symbols=selected_symbols,
        force=force,
    )


def _normalization_required(unit: NormalizationUnit) -> bool:
    """Return whether ``unit`` needs (re-)normalization.

    Re-normalize when nothing has been normalized yet, or when the cached raw
    payload hash differs from the hash that produced the stored facts. A former
    provider-change trigger was dropped together with
    ``financial_facts.source_provider`` now that EODHD is the only provider.
    """

    if unit.normalized_payload_hash is None:
        return True
    return unit.raw_payload_hash != unit.normalized_payload_hash


def _plan_normalization_selection(
    units: Sequence[NormalizationUnit],
    force: bool = False,
) -> Tuple[List[NormalizationUnit], int]:
    """Split ``units`` into those needing normalization and a skipped count.

    ``force`` selects every unit; otherwise a unit is skipped when the normalized
    payload hash it carries already matches its raw payload hash. The units already
    hold the freshness hashes (from the LEFT-JOINed normalization state), so this is
    pure in-memory filtering with no extra query.
    """

    if force:
        return list(units), 0
    to_normalize: List[NormalizationUnit] = []
    skipped = 0
    for unit in units:
        if _normalization_required(unit):
            to_normalize.append(unit)
        else:
            skipped += 1
    return to_normalize, skipped


def _print_normalization_up_to_date(
    provider: str,
    database: Union[str, Path],
) -> None:
    db_path = _resolve_database_path(str(database))
    print(
        f"{provider.strip().upper()} fundamentals are already up to date in {db_path}; "
        "use --force to re-normalize."
    )


def _normalization_worker_count(total_symbols: int) -> int:
    """Return an automatic worker count for bulk normalization."""

    if total_symbols <= 0:
        return 1
    cpu_bound = max(os.cpu_count() or 1, 1)
    return max(1, min(total_symbols, min(cpu_bound, NORMALIZATION_MAX_WORKERS)))


def _normalization_record_to_row(record: FactRecord) -> StoredFactRow:
    return (
        record.concept,
        record.fiscal_period,
        record.end_date,
        record.unit_kind,
        record.value,
        record.filed,
        record.currency,
    )


def _get_or_create_fx_service(database: Union[str, Path]) -> FXService:
    """Return a process-local FXService, creating it on first call.

    The cached instance is invalidated when ``database`` changes (can happen
    in test harnesses that run workers in-process with different temp DBs).
    """

    global _process_local_fx_service, _process_local_fx_service_db
    db_key = str(database)
    if _process_local_fx_service is None or _process_local_fx_service_db != db_key:
        repo = _SchemaReadyFXRatesRepository(database)
        _process_local_fx_service = FXService(
            database,
            repository=repo,
        )
        _process_local_fx_service_db = db_key
    return _process_local_fx_service


def _normalize_eodhd_payload_worker(
    database: Union[str, Path],
    provider_listing_id: int,
    currency: Optional[str],
    label: str,
) -> Optional[_NormalizedFactsResult]:
    """Normalize one stored EODHD payload, addressed by ``provider_listing_id``.

    The id-keyed worker: it reads the raw payload by its ``provider_listing_id`` PK
    and uses the ``currency`` already resolved from ``listing.currency`` by the scope
    query, so it makes a single payload read and no symbol/currency lookups. ``label``
    is the provider symbol, carried only for the normalizer's warnings and the
    progress lines -- it never addresses data.
    """

    fund_repo = FundamentalsRepository(database)
    payload_record = fund_repo.fetch_payload_with_hash_by_id(provider_listing_id)
    if payload_record is None:
        return None
    payload, payload_hash = payload_record
    if currency is None:
        raise ValueError(f"Missing listing currency for {label}")
    fx_service = _get_or_create_fx_service(database)
    normalizer = EODHDFactsNormalizer(fx_service=fx_service)
    # No console suppression here: worker processes are spawned with the quiet
    # console level inherited from the parent's suppress_console_logging scope,
    # and the in-process (workers <= 1) path runs inside that scope directly.
    rows = tuple(
        _normalization_record_to_row(record)
        for record in normalizer.normalize(
            payload, symbol=label, target_currency=currency
        )
    )
    return _NormalizedFactsResult(
        symbol=label,
        rows=rows,
        payload_hash=payload_hash,
        entity_name=_extract_entity_name_from_eodhd(payload),
        entity_description=_extract_entity_description_from_eodhd(payload),
        entity_sector=_extract_entity_sector_from_eodhd(payload),
        entity_industry=_extract_entity_industry_from_eodhd(payload),
    )


def _persist_normalization_result(
    fact_repo: FinancialFactsRepository,
    security_repo: SecurityRepository,
    state_repo: FundamentalsNormalizationStateRepository,
    unit: NormalizationUnit,
    result: _NormalizedFactsResult,
) -> int:
    """Persist one unit's metadata, facts, and watermark (all id-keyed).

    Returns the number of fact rows stored. Metadata is written only when the payload
    yielded at least one field (an empty payload mints no canonical rows); the
    watermark is always recorded so a re-run can skip the unchanged payload. Every
    unit carries a real ``listing_id`` (a NOT NULL FK on the raw row), so unlike the
    old symbol path there is no "uncatalogued, skip the writes" branch.
    """

    if (
        result.entity_name
        or result.entity_description
        or result.entity_sector
        or result.entity_industry
    ):
        security_repo.upsert_metadata_many(
            [
                SecurityMetadataUpdate(
                    security_id=unit.listing_id,
                    entity_name=result.entity_name,
                    description=result.entity_description,
                    sector=result.entity_sector,
                    industry=result.entity_industry,
                )
            ]
        )
    stored = fact_repo.replace_fact_rows(unit.listing_id, result.rows)
    state_repo.mark_success_by_id(unit.provider_listing_id, result.payload_hash)
    return stored


def _run_bulk_normalization(
    database: Union[str, Path],
    provider: str,
    units: Sequence[NormalizationUnit],
    worker: Callable[
        [Union[str, Path], int, Optional[str], str], Optional[_NormalizedFactsResult]
    ],
    requested_total: Optional[int] = None,
    skipped: int = 0,
) -> int:
    """Normalize many stored payloads while serializing SQLite writes.

    The unit of work is a :class:`NormalizationUnit`: the worker is dispatched by
    ``provider_listing_id`` and the writes are keyed by the ``listing_id`` /
    ``provider_listing_id`` the unit carries, so no symbol is resolved to an id here.
    The process-pool ``{future: unit}`` map carries the ids straight to the writes.
    """

    db_path = _resolve_database_path(str(database))
    selected_units = list(units)
    if not selected_units:
        raise SystemExit(f"No units provided for {provider} normalization.")

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    security_repo = SecurityRepository(db_path)
    security_repo.initialize_schema()
    state_repo = FundamentalsNormalizationStateRepository(db_path)
    state_repo.initialize_schema()
    # Pre-initialize the FX schema the worker processes read so the _SchemaReady
    # wrapper can skip redundant init calls. The worker no longer touches the
    # supported-ticker catalog (currency is carried on the unit), so that schema
    # init is gone.
    FXRatesRepository(db_path).initialize_schema()

    total = len(selected_units)
    requested = requested_total if requested_total is not None else total
    workers = _normalization_worker_count(total)
    processed = 0
    failed = 0
    if skipped:
        print(
            f"Normalizing {provider} fundamentals for {total} of {requested} symbols "
            f"with {workers} workers (skipped={skipped})"
        )
    else:
        print(
            f"Normalizing {provider} fundamentals for {total} symbols "
            f"with {workers} workers"
        )

    if workers <= 1:
        for idx, unit in enumerate(selected_units, 1):
            try:
                result = worker(
                    str(db_path),
                    unit.provider_listing_id,
                    unit.currency,
                    unit.provider_symbol,
                )
                if result is None:
                    LOGGER.warning(
                        "Skipping %s due to missing raw %s fundamentals",
                        unit.provider_symbol,
                        provider,
                    )
                    failed += 1
                    continue
                stored = _persist_normalization_result(
                    fact_repo, security_repo, state_repo, unit, result
                )
                processed += 1
                print(
                    f"[{idx}/{total}] Stored {stored} normalized facts for "
                    f"{unit.provider_symbol}",
                    flush=True,
                )
            except Exception as exc:
                LOGGER.error(
                    "Failed to normalize %s fundamentals for %s: %s",
                    provider,
                    unit.provider_symbol,
                    exc,
                )
                failed += 1
    else:
        executor = _create_process_pool_executor(workers)
        interrupted = False
        try:
            futures = {
                executor.submit(
                    worker,
                    str(db_path),
                    unit.provider_listing_id,
                    unit.currency,
                    unit.provider_symbol,
                ): unit
                for unit in selected_units
            }
            for idx, future in enumerate(as_completed(futures), 1):
                unit = futures[future]
                try:
                    result = future.result()
                    if result is None:
                        LOGGER.warning(
                            "Skipping %s due to missing raw %s fundamentals",
                            unit.provider_symbol,
                            provider,
                        )
                        failed += 1
                        continue
                    stored = _persist_normalization_result(
                        fact_repo, security_repo, state_repo, unit, result
                    )
                    processed += 1
                    print(
                        f"[{idx}/{total}] Stored {stored} normalized facts for "
                        f"{unit.provider_symbol}",
                        flush=True,
                    )
                except Exception as exc:
                    LOGGER.error(
                        "Failed to normalize %s fundamentals for %s: %s",
                        provider,
                        unit.provider_symbol,
                        exc,
                    )
                    failed += 1
        except KeyboardInterrupt:
            interrupted = True
            return _cancel_cli_command(
                "\nBulk normalization cancelled by user after "
                f"{processed + failed} completed symbols.",
                executors=[executor],
            )
        finally:
            if not interrupted:
                executor.shutdown(wait=True)

    summary = (
        f"Normalized {provider} fundamentals for {processed} of {requested} "
        f"requested symbols into {db_path} (skipped={skipped}, failed={failed})"
    )
    log_dir = current_logging_config()[0]
    if failed and log_dir is not None:
        # Failures are file-only while the console is quiet, so the summary
        # must say where the error details went.
        summary += f" — failure details in {log_dir / 'pyvalue.log'}"
    print(summary, flush=True)
    return 0


def cmd_normalize_eodhd_fundamentals_bulk(
    database: str,
    symbols: Optional[Sequence[str]] = None,
    force: bool = False,
) -> int:
    """Normalize stored EODHD fundamentals in parallel, keyed by listing id.

    ``symbols`` is the public selector (users type provider symbols); ``None`` means
    the whole provider. Either way the scope is resolved once to id-keyed
    :class:`NormalizationUnit`s via ``normalization_units`` -- whose INNER JOIN to
    ``fundamentals_raw`` already restricts to listings that have a raw payload, and
    whose ``primary_only`` filter drops secondary listings -- so the old has-raw
    intersection and the secondary symbol-set subtraction are both gone.

    The whole command runs with console logging suppressed: normalization
    diagnostics (missing FX, quarantined periods, per-symbol failures, ...) go
    to the log file only, and the console carries nothing but the progress
    prints. Worker processes inherit the quiet console because the process
    pool snapshots the console level while this context is active.
    """

    with suppress_console_logging():
        db_path = _resolve_database_path(database)
        fund_repo = FundamentalsRepository(db_path)
        fund_repo.initialize_schema()

        if symbols is None:
            units_by_id = fund_repo.normalization_units("EODHD", primary_only=True)
        else:
            requested = [symbol.upper() for symbol in symbols]
            if not requested:
                raise SystemExit("No symbols provided for EODHD normalization.")
            # Resolve the requested provider symbols to their listing ids, then
            # fetch the units for those ids (primary-only, has-raw enforced by
            # the query).
            listing_ids = list(
                SecurityRepository(db_path).resolve_ids_many(requested).values()
            )
            units_by_id = fund_repo.normalization_units(
                "EODHD", primary_only=True, listing_ids=listing_ids
            )

        units = list(units_by_id.values())
        if not units:
            raise SystemExit(
                "No EODHD fundamentals found. "
                "Run ingest-fundamentals --provider EODHD first."
            )

        requested_total = len(units)
        if force:
            print(
                f"Force re-normalization requested for {requested_total} EODHD "
                "symbols; skipping freshness scan",
                flush=True,
            )
            units_to_normalize = list(units)
            skipped = 0
        else:
            print(
                f"Checking EODHD normalization freshness for {requested_total} symbols",
                flush=True,
            )
            units_to_normalize, skipped = _plan_normalization_selection(
                units, force=False
            )
        if not units_to_normalize:
            _print_normalization_up_to_date("EODHD", db_path)
            return 0

        return _run_bulk_normalization(
            database=str(db_path),
            provider="EODHD",
            units=units_to_normalize,
            worker=_normalize_eodhd_payload_worker,
            requested_total=requested_total,
            skipped=skipped,
        )
