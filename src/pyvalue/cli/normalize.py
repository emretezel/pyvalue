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
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from pyvalue.money.fx import (
    FXService,
    MissingFXRateError,
)
from pyvalue.normalization import EODHDFactsNormalizer, SECFactsNormalizer
from pyvalue.logging_utils import (
    suppress_console_missing_fx_warnings,
)
from pyvalue.persistence.storage import (
    EntityMetadataRepository,
    FXRatesRepository,
    FundamentalsNormalizationCandidate,
    FundamentalsNormalizationStateRepository,
    FundamentalsRepository,
    FinancialFactsRepository,
    FactRecord,
    StoredFactRow,
    SupportedTickerRepository,
)

from ._common import (
    LOGGER,
    NORMALIZATION_MAX_WORKERS,
    _NormalizedFactsResult,
    _catalog_bootstrap_guidance,
    _extract_entity_description_from_eodhd,
    _extract_entity_industry_from_eodhd,
    _extract_entity_name_from_eodhd,
    _extract_entity_name_from_sec,
    _extract_entity_sector_from_eodhd,
    _normalize_provider,
    _qualify_symbol,
    _reconcile_eodhd_listing_scope,
    _resolve_database_path,
    _resolve_provider_scope_rows,
    _resolve_ticker_target_currency,
)
from ._batch import (
    _cancel_cli_command,
    _create_process_pool_executor,
)
from ._repos import (
    _SchemaReadyFXRatesRepository,
    _SchemaReadySupportedTickerRepository,
)


_process_local_fx_service: Optional[FXService] = None
_process_local_fx_service_db: Optional[str] = None
_process_local_ticker_repo: Optional[SupportedTickerRepository] = None
_process_local_ticker_repo_db: Optional[str] = None


def _select_listing_symbols_by_exchange(
    database: str,
    provider: str,
    exchange_code: str,
    *,
    primary_only: bool = False,
) -> List[str]:
    ticker_repo = SupportedTickerRepository(database)
    return ticker_repo.list_symbols_by_exchange(
        provider,
        exchange_code,
        primary_only=primary_only,
    )


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
    scope_rows, _, _ = _resolve_provider_scope_rows(
        str(db_path),
        provider_norm,
        symbols,
        exchange_codes,
        all_supported,
        primary_only=provider_norm == "EODHD",
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    raw_symbols = set(fund_repo.symbols(provider_norm))
    selected_symbols = [row.symbol for row in scope_rows if row.symbol in raw_symbols]
    if not selected_symbols:
        raise SystemExit(
            f"No {provider_norm} raw fundamentals found in the requested scope. "
            "Run ingest-fundamentals first."
        )
    if provider_norm == "SEC":
        return cmd_normalize_us_facts_bulk(
            database=str(db_path), symbols=selected_symbols, force=force
        )
    if provider_norm == "EODHD":
        return cmd_normalize_eodhd_fundamentals_bulk(
            database=str(db_path),
            symbols=selected_symbols,
            force=force,
        )
    raise SystemExit(f"Unsupported provider: {provider}")


def _normalization_required(
    candidate: FundamentalsNormalizationCandidate,
    provider: str,
) -> bool:
    provider_norm = provider.strip().upper()
    if candidate.normalized_payload_hash is None:
        return True
    if candidate.raw_payload_hash != candidate.normalized_payload_hash:
        return True
    if candidate.current_source_provider is None:
        return False
    return candidate.current_source_provider != provider_norm


def _plan_normalization_selection(
    database: Union[str, Path],
    provider: str,
    symbols: Sequence[str],
    force: bool = False,
) -> Tuple[List[str], Dict[str, FundamentalsNormalizationCandidate], int]:
    db_path = _resolve_database_path(str(database))
    provider_norm = _normalize_provider(provider)
    selected_symbols = [symbol.upper() for symbol in symbols]
    fund_repo = FundamentalsRepository(db_path)
    candidates = fund_repo.normalization_candidates(provider_norm, selected_symbols)
    if force:
        return (
            [symbol for symbol in selected_symbols if symbol in candidates],
            candidates,
            0,
        )

    to_normalize: List[str] = []
    skipped = 0
    for symbol in selected_symbols:
        candidate = candidates.get(symbol)
        if candidate is None:
            continue
        if _normalization_required(candidate, provider_norm):
            to_normalize.append(symbol)
        else:
            skipped += 1
    return to_normalize, candidates, skipped


def _print_normalization_up_to_date(
    provider: str,
    database: Union[str, Path],
) -> None:
    db_path = _resolve_database_path(str(database))
    print(
        f"{provider.strip().upper()} fundamentals are already up to date in {db_path}; "
        "use --force to re-normalize."
    )


def cmd_normalize_us_facts(
    symbol: str,
    database: str,
    force: bool = False,
) -> int:
    """Normalize previously ingested SEC facts for downstream metrics."""

    symbol = _qualify_symbol(symbol, exchange="US")
    fund_repo = FundamentalsRepository(database)
    fund_repo.initialize_schema()
    candidates_to_normalize, candidate_map, skipped = _plan_normalization_selection(
        database=database,
        provider="SEC",
        symbols=[symbol.upper()],
        force=force,
    )
    payload_record = fund_repo.fetch_payload_with_hash("SEC", symbol.upper())
    if payload_record is None:
        raise SystemExit(
            f"No raw SEC payload found for {symbol}. Run ingest-fundamentals --provider SEC before normalization."
        )
    if skipped and not candidates_to_normalize:
        _print_normalization_up_to_date("SEC", database)
        return 0

    payload, payload_hash = payload_record
    fx_repo = _SchemaReadyFXRatesRepository(database)
    fx_service = FXService(database, repository=fx_repo)
    normalizer = SECFactsNormalizer(fx_service=fx_service)
    try:
        records = normalizer.normalize(payload, symbol=symbol.upper())
    except MissingFXRateError as exc:
        raise SystemExit(
            f"SEC normalization failed for {symbol.upper()}: {exc}"
        ) from exc

    fact_repo = FinancialFactsRepository(database)
    fact_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()
    state_repo = FundamentalsNormalizationStateRepository(database)
    state_repo.initialize_schema()
    entity_name = payload.get("entityName")
    if entity_name:
        entity_repo.upsert(symbol.upper(), entity_name)
    stored = fact_repo.replace_facts(
        symbol.upper(),
        records,
        source_provider="SEC",
    )
    candidate = candidate_map.get(symbol.upper())
    security_id = (
        candidate.security_id
        if candidate is not None
        else fact_repo._security_repo().ensure_from_symbol(symbol.upper()).security_id
    )
    state_repo.mark_success("SEC", symbol.upper(), security_id, payload_hash)
    print(f"Stored {stored} normalized facts for {symbol.upper()} in {database}")
    return 0


def cmd_normalize_us_facts_bulk(
    database: str,
    symbols: Optional[Sequence[str]] = None,
    force: bool = False,
) -> int:
    """Normalize raw SEC facts for every stored ticker in parallel."""

    fund_repo = FundamentalsRepository(database)
    fund_repo.initialize_schema()

    if symbols is None:
        symbols = fund_repo.symbols("SEC")
        if not symbols:
            raise SystemExit(
                "No raw SEC facts found. Run ingest-fundamentals --provider SEC first."
            )
    else:
        symbols = [symbol.upper() for symbol in symbols]
        if not symbols:
            raise SystemExit("No symbols provided for SEC normalization.")

    requested_total = len(symbols)
    if force:
        print(
            f"Force re-normalization requested for {requested_total} SEC symbols; "
            "skipping freshness scan",
            flush=True,
        )
        symbols_to_normalize = list(symbols)
        candidates: Dict[str, FundamentalsNormalizationCandidate] = {}
        skipped = 0
    else:
        print(
            f"Checking SEC normalization freshness for {requested_total} symbols",
            flush=True,
        )
        symbols_to_normalize, candidates, skipped = _plan_normalization_selection(
            database=database,
            provider="SEC",
            symbols=symbols,
            force=False,
        )
    if not symbols_to_normalize:
        _print_normalization_up_to_date("SEC", database)
        return 0

    return _run_bulk_normalization(
        database=database,
        provider="SEC",
        symbols=symbols_to_normalize,
        worker=_normalize_sec_symbol_worker,
        candidate_map=candidates,
        requested_total=requested_total,
        skipped=skipped,
    )


def _normalization_worker_count(total_symbols: int) -> int:
    """Return an automatic worker count for bulk normalization."""

    if total_symbols <= 0:
        return 1
    cpu_bound = max(os.cpu_count() or 1, 1)
    return max(1, min(total_symbols, min(cpu_bound, NORMALIZATION_MAX_WORKERS)))


def _normalization_record_to_row(record: FactRecord) -> StoredFactRow:
    return (
        record.cik,
        record.concept,
        record.fiscal_period,
        record.end_date,
        record.unit_kind,
        record.value,
        record.accn,
        record.filed,
        record.frame,
        getattr(record, "start_date", None),
        getattr(record, "accounting_standard", None),
        getattr(record, "currency", None),
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


def _get_or_create_ticker_repo(
    database: Union[str, Path],
) -> SupportedTickerRepository:
    """Return a process-local SupportedTickerRepository, creating it on first call."""

    global _process_local_ticker_repo, _process_local_ticker_repo_db
    db_key = str(database)
    if _process_local_ticker_repo is None or _process_local_ticker_repo_db != db_key:
        _process_local_ticker_repo = _SchemaReadySupportedTickerRepository(database)
        _process_local_ticker_repo_db = db_key
    return _process_local_ticker_repo


def _normalize_sec_symbol_worker(
    database: Union[str, Path], symbol: str
) -> Optional[_NormalizedFactsResult]:
    """Normalize one stored SEC payload and return facts plus metadata."""

    fund_repo = FundamentalsRepository(database)
    payload_record = fund_repo.fetch_payload_with_hash("SEC", symbol)
    if payload_record is None:
        return None
    payload, payload_hash = payload_record
    fx_service = _get_or_create_fx_service(database)
    normalizer = SECFactsNormalizer(fx_service=fx_service)
    rows = tuple(
        _normalization_record_to_row(record)
        for record in normalizer.normalize(payload, symbol=symbol)
    )
    return _NormalizedFactsResult(
        symbol=symbol,
        rows=rows,
        payload_hash=payload_hash,
        entity_name=_extract_entity_name_from_sec(payload),
    )


def _normalize_eodhd_symbol_worker(
    database: Union[str, Path], symbol: str
) -> Optional[_NormalizedFactsResult]:
    """Normalize one stored EODHD payload and return facts plus metadata."""

    fund_repo = FundamentalsRepository(database)
    payload_record = fund_repo.fetch_payload_with_hash("EODHD", symbol)
    if payload_record is None:
        return None
    payload, payload_hash = payload_record
    target_currency = _resolve_ticker_target_currency(
        database, symbol, payload, ticker_repo=_get_or_create_ticker_repo(database)
    )
    if target_currency is None:
        raise ValueError(f"Missing listing/provider-listing currency for {symbol}")
    fx_service = _get_or_create_fx_service(database)
    normalizer = EODHDFactsNormalizer(fx_service=fx_service)
    with suppress_console_missing_fx_warnings(True):
        rows = tuple(
            _normalization_record_to_row(record)
            for record in normalizer.normalize(
                payload, symbol=symbol, target_currency=target_currency
            )
        )
    return _NormalizedFactsResult(
        symbol=symbol,
        rows=rows,
        payload_hash=payload_hash,
        entity_name=_extract_entity_name_from_eodhd(payload),
        entity_description=_extract_entity_description_from_eodhd(payload),
        entity_sector=_extract_entity_sector_from_eodhd(payload),
        entity_industry=_extract_entity_industry_from_eodhd(payload),
    )


def _run_bulk_normalization(
    database: Union[str, Path],
    provider: str,
    symbols: Sequence[str],
    worker: Callable[[Union[str, Path], str], Optional[_NormalizedFactsResult]],
    candidate_map: Optional[Mapping[str, FundamentalsNormalizationCandidate]] = None,
    requested_total: Optional[int] = None,
    skipped: int = 0,
) -> int:
    """Normalize many stored payloads while serializing SQLite writes."""

    db_path = _resolve_database_path(str(database))
    selected_symbols = [symbol.upper() for symbol in symbols]
    if not selected_symbols:
        raise SystemExit(f"No symbols provided for {provider} normalization.")

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    state_repo = FundamentalsNormalizationStateRepository(db_path)
    state_repo.initialize_schema()
    # Pre-initialize schemas used by worker processes so that
    # _SchemaReady* wrappers can safely skip redundant init calls.
    FXRatesRepository(db_path).initialize_schema()
    SupportedTickerRepository(db_path).initialize_schema()

    total = len(selected_symbols)
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
        for idx, symbol in enumerate(selected_symbols, 1):
            try:
                result = worker(str(db_path), symbol)
                if result is None:
                    LOGGER.warning(
                        "Skipping %s due to missing raw %s fundamentals",
                        symbol,
                        provider,
                    )
                    failed += 1
                    continue
                if (
                    result.entity_name
                    or result.entity_description
                    or result.entity_sector
                    or result.entity_industry
                ):
                    entity_repo.upsert(
                        symbol,
                        result.entity_name,
                        description=result.entity_description,
                        sector=result.entity_sector,
                        industry=result.entity_industry,
                    )
                stored = fact_repo.replace_fact_rows(
                    symbol,
                    result.rows,
                    source_provider=provider,
                )
                candidate = (
                    candidate_map.get(symbol) if candidate_map is not None else None
                )
                security_id = (
                    candidate.security_id
                    if candidate is not None
                    else fact_repo._security_repo()
                    .ensure_from_symbol(symbol)
                    .security_id
                )
                state_repo.mark_success(
                    provider,
                    symbol,
                    security_id,
                    result.payload_hash,
                )
                processed += 1
                print(
                    f"[{idx}/{total}] Stored {stored} normalized facts for {symbol}",
                    flush=True,
                )
            except Exception as exc:
                LOGGER.error(
                    "Failed to normalize %s fundamentals for %s: %s",
                    provider,
                    symbol,
                    exc,
                )
                failed += 1
    else:
        executor = _create_process_pool_executor(workers)
        interrupted = False
        try:
            futures = {
                executor.submit(worker, str(db_path), symbol): symbol
                for symbol in selected_symbols
            }
            for idx, future in enumerate(as_completed(futures), 1):
                symbol = futures[future]
                try:
                    result = future.result()
                    if result is None:
                        LOGGER.warning(
                            "Skipping %s due to missing raw %s fundamentals",
                            symbol,
                            provider,
                        )
                        failed += 1
                        continue
                    if (
                        result.entity_name
                        or result.entity_description
                        or result.entity_sector
                        or result.entity_industry
                    ):
                        entity_repo.upsert(
                            symbol,
                            result.entity_name,
                            description=result.entity_description,
                            sector=result.entity_sector,
                            industry=result.entity_industry,
                        )
                    stored = fact_repo.replace_fact_rows(
                        symbol,
                        result.rows,
                        source_provider=provider,
                    )
                    candidate = (
                        candidate_map.get(symbol) if candidate_map is not None else None
                    )
                    security_id = (
                        candidate.security_id
                        if candidate is not None
                        else fact_repo._security_repo()
                        .ensure_from_symbol(symbol)
                        .security_id
                    )
                    state_repo.mark_success(
                        provider,
                        symbol,
                        security_id,
                        result.payload_hash,
                    )
                    processed += 1
                    print(
                        f"[{idx}/{total}] Stored {stored} normalized facts for {symbol}",
                        flush=True,
                    )
                except Exception as exc:
                    LOGGER.error(
                        "Failed to normalize %s fundamentals for %s: %s",
                        provider,
                        symbol,
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

    print(
        f"Normalized {provider} fundamentals for {processed} of {requested} "
        f"requested symbols into {db_path} (skipped={skipped}, failed={failed})"
    )
    return 0


def cmd_normalize_eodhd_fundamentals(
    symbol: str,
    database: str,
    force: bool = False,
) -> int:
    """Normalize stored EODHD fundamentals for downstream metrics."""

    fund_repo = FundamentalsRepository(database)
    symbol_upper = symbol.upper()
    candidates_to_normalize, candidate_map, skipped = _plan_normalization_selection(
        database=database,
        provider="EODHD",
        symbols=[symbol_upper],
        force=force,
    )
    payload_record = fund_repo.fetch_payload_with_hash("EODHD", symbol_upper)
    if payload_record is None:
        raise SystemExit(
            f"No EODHD fundamentals found for {symbol}. Run ingest-fundamentals --provider EODHD first."
        )
    if skipped and not candidates_to_normalize:
        _print_normalization_up_to_date("EODHD", database)
        return 0

    payload, payload_hash = payload_record

    ticker_repo = _SchemaReadySupportedTickerRepository(database)
    target_currency = _resolve_ticker_target_currency(
        database, symbol_upper, payload, ticker_repo=ticker_repo
    )
    if target_currency is None:
        raise SystemExit(
            "EODHD normalization failed for "
            f"{symbol_upper}: missing listing/provider-listing currency"
        )
    fx_repo = _SchemaReadyFXRatesRepository(database)
    fx_service = FXService(database, repository=fx_repo)
    normalizer = EODHDFactsNormalizer(fx_service=fx_service)
    try:
        with suppress_console_missing_fx_warnings(True):
            records = normalizer.normalize(
                payload, symbol=symbol_upper, target_currency=target_currency
            )
    except MissingFXRateError as exc:
        raise SystemExit(
            f"EODHD normalization failed for {symbol_upper}: {exc}"
        ) from exc

    fact_repo = FinancialFactsRepository(database)
    fact_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()
    state_repo = FundamentalsNormalizationStateRepository(database)
    state_repo.initialize_schema()
    entity_name = _extract_entity_name_from_eodhd(payload)
    entity_description = _extract_entity_description_from_eodhd(payload)
    entity_sector = _extract_entity_sector_from_eodhd(payload)
    entity_industry = _extract_entity_industry_from_eodhd(payload)
    if entity_name or entity_description or entity_sector or entity_industry:
        entity_repo.upsert(
            symbol_upper,
            entity_name,
            description=entity_description,
            sector=entity_sector,
            industry=entity_industry,
        )

    stored = fact_repo.replace_facts(
        symbol_upper,
        records,
        source_provider="EODHD",
    )
    candidate = candidate_map.get(symbol_upper)
    security_id = (
        candidate.security_id
        if candidate is not None
        else fact_repo._security_repo().ensure_from_symbol(symbol_upper).security_id
    )
    state_repo.mark_success("EODHD", symbol_upper, security_id, payload_hash)
    print(f"Stored {stored} normalized facts for {symbol_upper} in {database}")
    return 0


def cmd_normalize_eodhd_fundamentals_bulk(
    database: str,
    symbols: Optional[Sequence[str]] = None,
    force: bool = False,
) -> int:
    """Normalize all stored EODHD fundamentals in parallel."""

    fund_repo = FundamentalsRepository(database)
    if symbols is None:
        symbols = fund_repo.symbols("EODHD")
        if not symbols:
            raise SystemExit(
                "No EODHD fundamentals found. Run ingest-fundamentals --provider EODHD first."
            )
    else:
        symbols = [symbol.upper() for symbol in symbols]
        if not symbols:
            raise SystemExit("No symbols provided for EODHD normalization.")

    _reconcile_eodhd_listing_scope(database, provider_symbols=symbols)
    ticker_repo = SupportedTickerRepository(database)
    supported_rows = ticker_repo.list_for_provider(
        "EODHD",
        provider_symbols=symbols,
    )
    primary_rows = ticker_repo.list_for_provider(
        "EODHD",
        provider_symbols=symbols,
        primary_only=True,
    )
    excluded_supported = {row.symbol.upper() for row in supported_rows} - {
        row.symbol.upper() for row in primary_rows
    }
    symbols = [symbol for symbol in symbols if symbol.upper() not in excluded_supported]
    if not symbols:
        raise SystemExit(
            "No primary EODHD symbols remain after secondary-listing filtering."
        )

    requested_total = len(symbols)
    if force:
        print(
            f"Force re-normalization requested for {requested_total} EODHD symbols; "
            "skipping freshness scan",
            flush=True,
        )
        symbols_to_normalize = list(symbols)
        candidates: Dict[str, FundamentalsNormalizationCandidate] = {}
        skipped = 0
    else:
        print(
            f"Checking EODHD normalization freshness for {requested_total} symbols",
            flush=True,
        )
        symbols_to_normalize, candidates, skipped = _plan_normalization_selection(
            database=database,
            provider="EODHD",
            symbols=symbols,
            force=False,
        )
    if not symbols_to_normalize:
        _print_normalization_up_to_date("EODHD", database)
        return 0

    return _run_bulk_normalization(
        database=database,
        provider="EODHD",
        symbols=symbols_to_normalize,
        worker=_normalize_eodhd_symbol_worker,
        candidate_map=candidates,
        requested_total=requested_total,
        skipped=skipped,
    )


def cmd_normalize_fundamentals(
    provider: str,
    symbol: str,
    database: str,
    exchange_code: Optional[str],
    force: bool = False,
) -> int:
    """Normalize stored fundamentals for a ticker using the provider-specific ruleset."""

    provider_norm = _normalize_provider(provider)
    if provider_norm == "SEC":
        symbol_upper = symbol.strip().upper()
        if "." in symbol_upper:
            if not symbol_upper.endswith(".US"):
                raise SystemExit(
                    "SEC normalization requires a .US suffix or an unqualified US symbol."
                )
            if exchange_code and exchange_code.upper() != "US":
                raise SystemExit("SEC normalization only supports --exchange-code US.")
        else:
            if not exchange_code:
                raise SystemExit(
                    "--exchange-code is required when SEC symbol has no suffix."
                )
            if exchange_code.upper() != "US":
                raise SystemExit("SEC normalization only supports --exchange-code US.")
        return cmd_normalize_us_facts(symbol=symbol, database=database, force=force)
    if provider_norm == "EODHD":
        symbol_upper = symbol.strip().upper()
        inferred_exchange = None
        base_symbol = symbol_upper
        if "." in symbol_upper:
            base_symbol, inferred_exchange = symbol_upper.split(".", 1)
        if not exchange_code and not inferred_exchange:
            raise SystemExit(
                "--exchange-code is required for EODHD normalization when symbol has no exchange suffix."
            )
        exch_code = inferred_exchange or exchange_code
        qualified = (
            _qualify_symbol(base_symbol, exch_code) if exch_code else symbol_upper
        )
        return cmd_normalize_eodhd_fundamentals(
            symbol=qualified,
            database=database,
            force=force,
        )
    raise SystemExit(f"Unsupported provider: {provider}")


def cmd_normalize_fundamentals_bulk(
    provider: str,
    database: str,
    exchange_code: Optional[str],
    force: bool = False,
) -> int:
    """Normalize stored fundamentals in bulk for the specified provider."""

    provider_norm = _normalize_provider(provider)
    if not exchange_code:
        if provider_norm == "SEC":
            exchange_norm = "US"
        else:
            raise SystemExit(
                "--exchange-code is required for bulk fundamentals normalization."
            )
    else:
        exchange_norm = exchange_code.upper()
    if provider_norm == "SEC" and exchange_norm != "US":
        raise SystemExit("SEC normalization only supports --exchange-code US.")
    symbols_for_exchange = _select_listing_symbols_by_exchange(
        database=database,
        provider=provider_norm,
        exchange_code=exchange_norm,
        primary_only=provider_norm == "EODHD",
    )
    if not symbols_for_exchange:
        raise SystemExit(
            f"No supported tickers found for provider {provider_norm} on exchange {exchange_norm}. "
            f"{_catalog_bootstrap_guidance(provider_norm)}"
        )
    fund_repo = FundamentalsRepository(database)
    fund_repo.initialize_schema()
    raw_symbols = set(fund_repo.symbols(provider_norm))
    symbols = [symbol for symbol in symbols_for_exchange if symbol in raw_symbols]
    if not symbols:
        raise SystemExit(
            f"No {provider_norm} fundamentals found for exchange {exchange_norm}. "
            "Run ingest-fundamentals-bulk first."
        )
    if provider_norm == "SEC":
        return cmd_normalize_us_facts_bulk(
            database=database,
            symbols=symbols,
            force=force,
        )
    if provider_norm == "EODHD":
        return cmd_normalize_eodhd_fundamentals_bulk(
            database=database,
            symbols=symbols,
            force=force,
        )
    raise SystemExit(f"Unsupported provider: {provider}")
