"""CLI handlers for ingesting raw fundamentals (EODHD) and ingest progress reports.

Author: Emre Tezel
"""

from __future__ import annotations

from concurrent.futures import (
    as_completed,
)
from datetime import datetime, timezone
from threading import local
import time
from pathlib import Path
from typing import (
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from pyvalue.config import Config
from pyvalue.ingestion import EODHDFundamentalsClient
from pyvalue.persistence.storage import (
    FundamentalsUpdate,
    FundamentalsRepository,
    FundamentalsFetchStateRepository,
    SupportedTicker,
    SupportedTickerRepository,
    canonical_json_dumps,
    fundamentals_payload_hash,
)

from ._common import (
    EODHD_FUNDAMENTALS_CALL_COST,
    EODHD_MAX_REQUESTS_PER_MINUTE,
    FUNDAMENTALS_PROGRESS_INTERVAL_SECONDS,
    FUNDAMENTALS_PROGRESS_SYMBOL_STEP,
    FUNDAMENTALS_RATE_LIMIT_BURST,
    FUNDAMENTALS_WORKERS,
    FUNDAMENTALS_WRITE_BATCH_INTERVAL_SECONDS,
    FUNDAMENTALS_WRITE_BATCH_SIZE,
    LOGGER,
    _PreparedFundamentalsRun,
    _catalog_bootstrap_guidance,
    _eodhd_request_budget,
    _normalize_provider,
    _normalize_provider_scope_symbol,
    _parse_exchange_filters,
    _reconcile_eodhd_listing_scope,
    _require_eodhd_key,
    _resolve_database_path,
    _resolve_provider_scope_rows,
    _safe_eodhd_quota_snapshot,
    _scope_label,
    _summarize_progress_breakdown,
    _validate_scope_selector,
)
from ._batch import (
    _RateLimiter,
    _cancel_cli_command,
    _create_interruptible_thread_executor,
)


_FUNDAMENTALS_CLIENT_LOCAL = local()


def cmd_reconcile_listing_status(
    provider: str,
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
) -> int:
    """Backfill cached EODHD listing classification from stored raw fundamentals."""

    provider_norm = _normalize_provider(provider)
    if provider_norm != "EODHD":
        raise SystemExit(
            "reconcile-listing-status currently only supports provider=EODHD."
        )

    db_path = _resolve_database_path(database)
    scope_rows, symbol_filters, resolved_exchange_codes = _resolve_provider_scope_rows(
        str(db_path),
        provider_norm,
        symbols,
        exchange_codes,
        all_supported,
        primary_only=False,
    )
    scope_label = _scope_label(symbol_filters, resolved_exchange_codes)
    updates = _reconcile_eodhd_listing_scope(
        str(db_path),
        provider_symbols=symbol_filters,
        exchange_codes=resolved_exchange_codes,
    )
    primary_updates = sum(1 for update in updates if update.is_primary_listing)
    secondary_updates = len(updates) - primary_updates

    print("EODHD listing-status reconciliation")
    print(f"Database: {db_path}")
    print(f"Scope: {scope_label}")
    print(f"Supported tickers in scope: {len(scope_rows)}")
    print(f"Listings classified: {len(updates)}")
    print(f"Primary listings classified: {primary_updates}")
    print(f"Secondary listings classified: {secondary_updates}")
    if not updates:
        print("No stored raw fundamentals needed reconciliation.")
    return 0


def _resolve_eodhd_fundamentals_rate(rate: Optional[float]) -> float:
    config = Config()
    configured = float(config.eodhd_fundamentals_requests_per_minute)
    rate_value = configured if rate is None else rate
    if rate_value is None or rate_value <= 0:
        raise SystemExit(
            "--rate must be greater than 0 for EODHD fundamentals ingestion."
        )
    return min(rate_value, EODHD_MAX_REQUESTS_PER_MINUTE)


def _get_thread_local_eodhd_fundamentals_client(
    api_key: str,
) -> EODHDFundamentalsClient:
    client = getattr(_FUNDAMENTALS_CLIENT_LOCAL, "client", None)
    current_key = getattr(_FUNDAMENTALS_CLIENT_LOCAL, "api_key", None)
    if client is None or current_key != api_key:
        client = EODHDFundamentalsClient(api_key=api_key)
        _FUNDAMENTALS_CLIENT_LOCAL.client = client
        _FUNDAMENTALS_CLIENT_LOCAL.api_key = api_key
    return client


def _resolve_eodhd_stage_scope(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
) -> Tuple[str, Optional[List[str]], Optional[List[str]]]:
    symbol_filters, resolved_exchange_codes = _validate_scope_selector(
        symbols, exchange_codes, all_supported
    )
    ticker_repo = SupportedTickerRepository(database)
    ticker_repo.initialize_schema()
    if symbol_filters:
        normalized_symbols = [
            _normalize_provider_scope_symbol("EODHD", symbol)
            for symbol in symbol_filters
        ]
        rows = ticker_repo.list_for_provider(
            "EODHD", provider_symbols=normalized_symbols
        )
        found = {row.symbol.upper() for row in rows}
        missing = [symbol for symbol in normalized_symbols if symbol not in found]
        if missing:
            raise SystemExit(
                f"Unsupported tickers for provider EODHD: {', '.join(missing)}. "
                f"{_catalog_bootstrap_guidance('EODHD')}"
            )
        return _scope_label(normalized_symbols, None), normalized_symbols, None

    available_exchanges = ticker_repo.available_exchanges("EODHD")
    if resolved_exchange_codes:
        missing_exchanges = [
            code for code in resolved_exchange_codes if code not in available_exchanges
        ]
        if missing_exchanges:
            raise SystemExit(
                f"No supported tickers found for provider EODHD in scope {', '.join(missing_exchanges)}. "
                f"{_catalog_bootstrap_guidance('EODHD')}"
            )
    elif not available_exchanges:
        raise SystemExit(
            "No supported tickers found for provider EODHD in scope all supported tickers. "
            f"{_catalog_bootstrap_guidance('EODHD')}"
        )
    return (
        _scope_label(None, resolved_exchange_codes),
        None,
        resolved_exchange_codes,
    )


def _prepare_eodhd_fundamentals_run(
    database: Union[str, Path],
    api_key: str,
    exchange_codes: Optional[Sequence[str]],
    provider_symbols: Optional[Sequence[str]],
    rate: Optional[float],
    max_symbols: Optional[int],
    max_age_days: Optional[int],
    respect_backoff: bool,
    missing_only: bool,
) -> _PreparedFundamentalsRun:
    eodhd_client = EODHDFundamentalsClient(api_key=api_key)
    config = Config()
    buffer_calls = max(config.eodhd_fundamentals_daily_buffer_calls, 0)
    rate_value = _resolve_eodhd_fundamentals_rate(rate)
    user_meta = eodhd_client.user_metadata()
    daily_limit, used_calls, usable_requests = _eodhd_request_budget(
        user_meta, buffer_calls, EODHD_FUNDAMENTALS_CALL_COST
    )
    request_budget = usable_requests
    if max_symbols is not None:
        request_budget = min(request_budget, max_symbols)
    if request_budget <= 0:
        return _PreparedFundamentalsRun(
            rate_value=rate_value,
            daily_limit=daily_limit,
            used_calls=used_calls,
            buffer_calls=buffer_calls,
            request_budget=request_budget,
            eligible=(),
        )

    ticker_repo = SupportedTickerRepository(database)
    eligible = ticker_repo.list_eligible_for_fundamentals(
        provider="EODHD",
        exchange_codes=exchange_codes,
        max_age_days=max_age_days,
        max_symbols=request_budget,
        respect_backoff=respect_backoff,
        missing_only=missing_only,
        provider_symbols=provider_symbols,
    )
    return _PreparedFundamentalsRun(
        rate_value=rate_value,
        daily_limit=daily_limit,
        used_calls=used_calls,
        buffer_calls=buffer_calls,
        request_budget=request_budget,
        eligible=tuple(eligible),
    )


def _build_fundamentals_update(
    ticker: SupportedTicker,
    payload: Dict[str, object],
) -> FundamentalsUpdate:
    data = canonical_json_dumps(payload)
    return FundamentalsUpdate(
        security_id=ticker.security_id,
        provider_symbol=ticker.symbol,
        provider_exchange_code=ticker.exchange_code,
        listing_currency=ticker.currency,
        data=data,
        payload_hash=fundamentals_payload_hash(data),
        last_fetched_at=datetime.now(timezone.utc).isoformat(),
    )


def _flush_fundamentals_batches(
    repo: FundamentalsRepository,
    state_repo: FundamentalsFetchStateRepository,
    success_updates: List[FundamentalsUpdate],
    failures: List[Tuple[str, str]],
) -> None:
    if success_updates:
        repo.upsert_many("EODHD", success_updates)
        success_updates.clear()
    if failures:
        state_repo.mark_failure_many("EODHD", failures)
        failures.clear()


def _fetch_symbol_fundamentals(
    api_key: str,
    limiter: _RateLimiter,
    symbol: str,
) -> Dict[str, object]:
    client = _get_thread_local_eodhd_fundamentals_client(api_key)
    limiter.acquire()
    return client.fetch_fundamentals(symbol, exchange_code=None)


def _run_eodhd_fundamentals_ingestion(
    database: Union[str, Path],
    api_key: str,
    scope_label: str,
    prepared: _PreparedFundamentalsRun,
) -> int:
    if prepared.request_budget <= 0:
        print(
            "No EODHD fundamentals request budget available for this run "
            f"(daily_limit={prepared.daily_limit}, used_calls={prepared.used_calls}, "
            f"buffer_calls={prepared.buffer_calls})."
        )
        return 0
    if not prepared.eligible:
        print(
            f"No eligible supported tickers found for {scope_label}. "
            "Refresh supported tickers first or relax freshness filters."
        )
        return 0

    db_path = _resolve_database_path(str(database))
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    state_repo = FundamentalsFetchStateRepository(db_path)
    state_repo.initialize_schema()
    limiter = _RateLimiter(prepared.rate_value, burst=FUNDAMENTALS_RATE_LIMIT_BURST)
    total = len(prepared.eligible)
    processed = 0
    failed = 0
    pending_updates: List[FundamentalsUpdate] = []
    pending_failures: List[Tuple[str, str]] = []
    last_flush = time.monotonic()
    last_report = time.monotonic()
    last_report_count = 0
    print(
        f"Fetching EODHD fundamentals for {total} supported tickers across {scope_label} "
        f"at <= {prepared.rate_value:.2f} req/min "
        f"(daily_limit={prepared.daily_limit}, used_calls={prepared.used_calls}, "
        f"buffer_calls={prepared.buffer_calls}, budget_requests={prepared.request_budget})"
    )

    def maybe_flush(force: bool = False) -> None:
        nonlocal last_flush
        if not pending_updates and not pending_failures:
            return
        if not force:
            elapsed = time.monotonic() - last_flush
            buffered = len(pending_updates) + len(pending_failures)
            if (
                buffered < FUNDAMENTALS_WRITE_BATCH_SIZE
                and elapsed < FUNDAMENTALS_WRITE_BATCH_INTERVAL_SECONDS
            ):
                return
        _flush_fundamentals_batches(repo, state_repo, pending_updates, pending_failures)
        last_flush = time.monotonic()

    def maybe_report(force: bool = False) -> None:
        nonlocal last_report, last_report_count
        completed = processed + failed
        elapsed = time.monotonic() - last_report
        if (
            not force
            and completed - last_report_count < FUNDAMENTALS_PROGRESS_SYMBOL_STEP
            and elapsed < FUNDAMENTALS_PROGRESS_INTERVAL_SECONDS
        ):
            return
        print(
            f"[progress] stored={processed} failed={failed} completed={completed}/{total}",
            flush=True,
        )
        last_report = time.monotonic()
        last_report_count = completed

    executor = _create_interruptible_thread_executor(
        max_workers=min(FUNDAMENTALS_WORKERS, total)
    )
    interrupted = False
    try:
        futures = {
            executor.submit(
                _fetch_symbol_fundamentals, api_key, limiter, ticker.symbol
            ): ticker
            for ticker in prepared.eligible
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                payload = future.result()
                pending_updates.append(_build_fundamentals_update(ticker, payload))
                processed += 1
            except Exception as exc:  # pragma: no cover - network errors
                LOGGER.error(
                    "Failed to fetch fundamentals for %s: %s", ticker.symbol, exc
                )
                pending_failures.append((ticker.symbol, str(exc)))
                failed += 1
            maybe_flush()
            maybe_report()
    except KeyboardInterrupt:
        interrupted = True
        return _cancel_cli_command(
            f"\nCancelled after {processed + failed} completed symbols.",
            executors=[executor],
            flushers=[
                lambda: maybe_flush(force=True),
                lambda: maybe_report(force=True),
            ],
        )
    finally:
        if not interrupted:
            executor.shutdown(wait=True)

    maybe_flush(force=True)
    maybe_report(force=True)
    print(
        f"Stored fundamentals for {processed} of {total} planned supported tickers in {db_path}"
    )
    return 0


def cmd_report_ingest_progress(
    provider: str,
    database: str,
    exchange_codes: Optional[Sequence[str]],
    max_age_days: Optional[int],
    missing_only: bool,
) -> int:
    """Report EODHD ingest progress across supported tickers."""

    provider_norm = provider.strip().upper()
    if provider_norm != "EODHD":
        raise SystemExit(
            "report-ingest-progress currently only supports provider=EODHD."
        )

    requested_exchange_codes = _parse_exchange_filters(exchange_codes)
    selected_exchanges = (
        sorted(requested_exchange_codes) if requested_exchange_codes else None
    )
    effective_max_age_days = None if missing_only else (max_age_days or 30)

    ticker_repo = SupportedTickerRepository(database)
    breakdown = ticker_repo.progress_by_exchange(
        provider=provider_norm,
        exchange_codes=selected_exchanges,
        max_age_days=effective_max_age_days,
        missing_only=missing_only,
    )
    summary = _summarize_progress_breakdown(breakdown)
    failures = (
        ticker_repo.recent_failures(
            provider=provider_norm,
            exchange_codes=selected_exchanges,
            limit=10,
        )
        if summary.error_rows > 0
        else []
    )
    config = Config()
    quota = _safe_eodhd_quota_snapshot(
        api_key=getattr(config, "eodhd_api_key", None),
        buffer_calls=max(
            getattr(config, "eodhd_fundamentals_daily_buffer_calls", 0), 0
        ),
        call_cost=EODHD_FUNDAMENTALS_CALL_COST,
    )

    if summary.total_supported == 0:
        status = "INCOMPLETE"
    elif summary.missing > 0 or summary.stale > 0:
        status = "INCOMPLETE"
    elif summary.blocked > 0:
        status = "BLOCKED_BY_BACKOFF"
    else:
        status = "COMPLETE"

    fresh_count = max(summary.total_supported - summary.missing - summary.stale, 0)
    percent_complete = (
        (fresh_count / summary.total_supported) * 100.0
        if summary.total_supported
        else 0.0
    )
    scope_label = (
        ", ".join(selected_exchanges) if selected_exchanges else "all exchanges"
    )
    mode_label = (
        "missing-only" if missing_only else f"freshness({effective_max_age_days}d)"
    )

    if summary.total_supported == 0:
        next_action = "Refresh supported tickers first"
    elif summary.missing > 0 or summary.stale > 0:
        if quota is not None and quota["usable_requests"] <= 0:
            next_action = "Wait for the next quota reset"
        else:
            next_action = "Run ingest-fundamentals now"
    elif summary.blocked > 0:
        next_action = "Wait for backoff to expire or rerun with --retry-failed-now"
    else:
        next_action = "Done for current scope"

    earliest_next_eligible = (
        next(
            (
                item.next_eligible_at
                for item in failures
                if item.next_eligible_at is not None
            ),
            None,
        )
        if summary.blocked > 0
        else None
    )

    print("EODHD fundamentals progress")
    print(f"Provider: {provider_norm}")
    print(f"Database: {database}")
    print(f"Scope: {scope_label}")
    print(f"Mode: {mode_label}")
    print(f"Status: {status}")
    print(f"Supported: {summary.total_supported}")
    print(f"Stored: {summary.stored}")
    print(f"Missing: {summary.missing}")
    print(f"Stale: {summary.stale}")
    print(f"Fresh: {fresh_count}")
    print(f"Blocked: {summary.blocked}")
    print(f"Error rows: {summary.error_rows}")
    print(f"Percent complete: {percent_complete:.2f}%")

    print("By exchange:")
    if breakdown:
        for row in breakdown:
            print(
                f"- {row.exchange_code}: supported={row.total_supported}, "
                f"stored={row.stored}, missing={row.missing}, stale={row.stale}, "
                f"blocked={row.blocked}, errors={row.error_rows}"
            )
    else:
        print("- none")

    print("Recent failures:")
    print(f"- error rows: {summary.error_rows}")
    print(f"- earliest next eligible: {earliest_next_eligible or 'n/a'}")
    if failures:
        for item in failures:
            print(
                f"- {item.symbol} [{item.exchange_code}] attempts={item.attempts} "
                f"next={item.next_eligible_at or 'n/a'} error={item.last_error or 'n/a'}"
            )
    else:
        print("- none")

    print("Quota:")
    if quota is None:
        print("- quota unavailable")
    else:
        print(f"- daily limit: {quota['daily_limit']}")
        print(f"- used today: {quota['used_calls']}")
        print(f"- buffer calls: {quota['buffer_calls']}")
        print(f"- usable requests left: {quota['usable_requests']}")

    print(f"Next action: {next_action}")
    return 0


def cmd_report_fundamentals_progress(
    provider: str,
    database: str,
    exchange_codes: Optional[Sequence[str]],
    max_age_days: Optional[int],
    missing_only: bool,
) -> int:
    """Public fundamentals-progress command wrapper."""

    return cmd_report_ingest_progress(
        provider=provider,
        database=database,
        exchange_codes=exchange_codes,
        max_age_days=max_age_days,
        missing_only=missing_only,
    )


def cmd_ingest_fundamentals_stage(
    provider: str,
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    rate: Optional[float],
    max_symbols: Optional[int],
    max_age_days: Optional[int],
    respect_backoff: bool,
) -> int:
    """Unified fundamentals ingestion over symbol, exchange, or full supported scope."""

    db_path = _resolve_database_path(database)
    # Validate the provider selector. EODHD is currently the only supported
    # provider; the --provider flag is retained so a future provider can be
    # re-introduced with a single change in _normalize_provider.
    _normalize_provider(provider)

    api_key = _require_eodhd_key()
    scope_label, symbol_filters, resolved_exchange_codes = _resolve_eodhd_stage_scope(
        str(db_path),
        symbols,
        exchange_codes,
        all_supported,
    )
    prepared = _prepare_eodhd_fundamentals_run(
        database=db_path,
        api_key=api_key,
        exchange_codes=resolved_exchange_codes,
        provider_symbols=symbol_filters,
        rate=rate,
        max_symbols=max_symbols,
        max_age_days=max_age_days,
        respect_backoff=respect_backoff,
        missing_only=max_age_days is None,
    )
    return _run_eodhd_fundamentals_ingestion(
        database=db_path,
        api_key=api_key,
        scope_label=scope_label,
        prepared=prepared,
    )
