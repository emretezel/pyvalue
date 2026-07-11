"""CLI handlers for updating market data and reporting market-data progress.

Author: Emre Tezel
"""

from __future__ import annotations

from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
from threading import local
import time
from typing import (
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from pyvalue.config import Config
from pyvalue.ingestion import EODHDFundamentalsClient
from pyvalue.marketdata import EODHDProvider, MarketDataUpdate, PriceData
from pyvalue.marketdata.service import MarketDataService
from pyvalue.persistence.storage import (
    MarketDataFetchStateRepository,
    SupportedTicker,
    SupportedTickerRepository,
)

from ._common import (
    EODHD_MARKET_DATA_BULK_CALL_COST,
    EODHD_MARKET_DATA_CALL_COST,
    EODHD_MAX_REQUESTS_PER_MINUTE,
    LOGGER,
    MARKET_DATA_BULK_BREAK_EVEN,
    MARKET_DATA_BULK_WORKERS,
    MARKET_DATA_PROGRESS_INTERVAL_SECONDS,
    MARKET_DATA_PROGRESS_SYMBOL_STEP,
    MARKET_DATA_SYMBOL_WORKERS,
    MARKET_DATA_WRITE_BATCH_INTERVAL_SECONDS,
    MARKET_DATA_WRITE_BATCH_SIZE,
    _MarketDataExchangeTask,
    _PlannedMarketDataRun,
    _eodhd_request_budget,
    _normalize_provider,
    _parse_exchange_filters,
    _require_eodhd_key,
    _resolve_database_path,
    _resolve_provider_scope,
    _safe_eodhd_quota_snapshot,
    _scope_label,
    _summarize_progress_breakdown,
)
from ._batch import (
    _RateLimiter,
    _cancel_cli_command,
    _create_interruptible_thread_executor,
)


_MARKET_DATA_PROVIDER_LOCAL = local()


def _resolve_eodhd_market_data_rate(rate: Optional[float]) -> float:
    config = Config()
    configured = float(config.eodhd_market_data_requests_per_minute)
    rate_value = configured if rate is None else rate
    if rate_value is None or rate_value <= 0:
        raise SystemExit(
            "--rate must be greater than 0 for EODHD global market data updates."
        )
    return min(rate_value, EODHD_MAX_REQUESTS_PER_MINUTE)


def _get_thread_local_market_data_provider(api_key: str) -> EODHDProvider:
    provider = getattr(_MARKET_DATA_PROVIDER_LOCAL, "provider", None)
    current_key = getattr(_MARKET_DATA_PROVIDER_LOCAL, "api_key", None)
    if provider is None or current_key != api_key:
        provider = EODHDProvider(api_key=api_key)
        _MARKET_DATA_PROVIDER_LOCAL.provider = provider
        _MARKET_DATA_PROVIDER_LOCAL.api_key = api_key
    return provider


def _plan_market_data_stage_run(
    eligible: Sequence[SupportedTicker],
    request_budget: int,
) -> _PlannedMarketDataRun:
    if not eligible or request_budget <= 0:
        return _PlannedMarketDataRun(
            bulk_tasks=(),
            symbol_tickers=(),
            api_call_cost=0,
            http_requests=0,
        )

    grouped: Dict[str, List[SupportedTicker]] = {}
    for ticker in eligible:
        grouped.setdefault(ticker.exchange_code, []).append(ticker)

    remaining_budget = request_budget
    exchange_mode: Dict[str, str] = {}
    bulk_tasks: List[_MarketDataExchangeTask] = []
    symbol_tickers: List[SupportedTicker] = []
    bulk_symbols: set[str] = set()

    for ticker in eligible:
        exchange_code = ticker.exchange_code
        mode = exchange_mode.get(exchange_code)
        if mode is None:
            exchange_rows = grouped[exchange_code]
            if (
                len(exchange_rows) >= MARKET_DATA_BULK_BREAK_EVEN
                and remaining_budget >= EODHD_MARKET_DATA_BULK_CALL_COST
            ):
                exchange_mode[exchange_code] = "bulk"
                bulk_tasks.append(
                    _MarketDataExchangeTask(
                        exchange_code=exchange_code,
                        tickers=tuple(exchange_rows),
                    )
                )
                bulk_symbols.update(row.symbol for row in exchange_rows)
                remaining_budget -= EODHD_MARKET_DATA_BULK_CALL_COST
                continue
            exchange_mode[exchange_code] = "symbol"
            mode = "symbol"

        if mode == "bulk" or ticker.symbol in bulk_symbols:
            continue
        if remaining_budget < EODHD_MARKET_DATA_CALL_COST:
            break
        symbol_tickers.append(ticker)
        remaining_budget -= EODHD_MARKET_DATA_CALL_COST

    api_call_cost = (
        len(bulk_tasks) * EODHD_MARKET_DATA_BULK_CALL_COST
        + len(symbol_tickers) * EODHD_MARKET_DATA_CALL_COST
    )
    return _PlannedMarketDataRun(
        bulk_tasks=tuple(bulk_tasks),
        symbol_tickers=tuple(symbol_tickers),
        api_call_cost=api_call_cost,
        http_requests=len(bulk_tasks) + len(symbol_tickers),
    )


def _build_market_data_update(
    service: MarketDataService,
    ticker: SupportedTicker,
    data: PriceData,
) -> MarketDataUpdate:
    prepared = service.prepare_price_data(
        ticker.symbol,
        data,
        currency_hint=ticker.currency,
    )
    return MarketDataUpdate(
        security_id=ticker.security_id,
        symbol=ticker.symbol,
        as_of=prepared.as_of,
        price=prepared.price,
        volume=prepared.volume,
        currency=prepared.currency,
        provider_listing_id=ticker.provider_listing_id,
    )


def _flush_market_data_batches(
    service: MarketDataService,
    state_repo: MarketDataFetchStateRepository,
    success_updates: List[MarketDataUpdate],
    failures: List[Tuple[str, str]],
) -> None:
    if success_updates:
        service.persist_updates(success_updates)
        state_repo.mark_success_many(
            "EODHD", [update.symbol for update in success_updates]
        )
        success_updates.clear()
    if failures:
        state_repo.mark_failure_many("EODHD", failures)
        failures.clear()


def _fetch_exchange_market_data(
    api_key: str,
    limiter: _RateLimiter,
    exchange_code: str,
) -> Mapping[str, PriceData]:
    provider = _get_thread_local_market_data_provider(api_key)
    limiter.acquire()
    return provider.latest_prices_for_exchange(exchange_code)


def _fetch_symbol_market_data(
    api_key: str,
    limiter: _RateLimiter,
    symbol: str,
) -> PriceData:
    provider = _get_thread_local_market_data_provider(api_key)
    limiter.acquire()
    return provider.latest_price(symbol)


def cmd_report_market_data_progress(
    provider: str,
    database: str,
    exchange_codes: Optional[Sequence[str]],
    max_age_days: int,
) -> int:
    """Report EODHD market data refresh progress across supported tickers."""

    provider_norm = provider.strip().upper()
    if provider_norm != "EODHD":
        raise SystemExit(
            "report-market-data-progress currently only supports provider=EODHD."
        )

    requested_exchange_codes = _parse_exchange_filters(exchange_codes)
    selected_exchanges = (
        sorted(requested_exchange_codes) if requested_exchange_codes else None
    )
    effective_max_age_days = max_age_days or 7

    ticker_repo = SupportedTickerRepository(database)
    # Read-only progress report: trust the cached primary_listing_status rather
    # than reconciling here (ingest / reconcile-listing-status own the writes).
    breakdown = ticker_repo.market_data_progress_by_exchange(
        provider=provider_norm,
        exchange_codes=selected_exchanges,
        max_age_days=effective_max_age_days,
        primary_only=True,
    )
    summary = _summarize_progress_breakdown(breakdown)
    failures = (
        ticker_repo.recent_market_data_failures(
            provider=provider_norm,
            exchange_codes=selected_exchanges,
            limit=10,
            primary_only=True,
        )
        if summary.error_rows > 0
        else []
    )
    config = Config()
    quota = _safe_eodhd_quota_snapshot(
        api_key=getattr(config, "eodhd_api_key", None),
        buffer_calls=max(getattr(config, "eodhd_market_data_daily_buffer_calls", 0), 0),
        call_cost=EODHD_MARKET_DATA_CALL_COST,
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
    mode_label = f"freshness({effective_max_age_days}d)"

    if summary.total_supported == 0:
        next_action = "Refresh supported tickers first"
    elif summary.missing > 0 or summary.stale > 0:
        if quota is not None and quota["usable_requests"] <= 0:
            next_action = "Wait for the next quota reset"
        else:
            next_action = "Run update-market-data now"
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

    print("EODHD market data progress")
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


def cmd_update_market_data_stage(
    provider: str,
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    rate: Optional[float],
    max_symbols: Optional[int],
    max_age_days: int,
    respect_backoff: bool,
) -> int:
    """Unified market-data refresh over symbol, exchange, or full supported scope."""

    db_path = _resolve_database_path(database)
    provider_norm = _normalize_provider(provider)
    if provider_norm != "EODHD":
        raise SystemExit("update-market-data currently only supports provider=EODHD.")

    _, symbol_filters, resolved_exchange_codes = _resolve_provider_scope(
        str(db_path),
        provider_norm,
        symbols,
        exchange_codes,
        all_supported,
        primary_only=True,
    )
    scope_label = _scope_label(symbol_filters, resolved_exchange_codes)
    config = Config()
    buffer_calls = max(config.eodhd_market_data_daily_buffer_calls, 0)
    rate_value = _resolve_eodhd_market_data_rate(rate)
    api_key = _require_eodhd_key()
    client = EODHDFundamentalsClient(api_key=api_key)
    user_meta = client.user_metadata()
    daily_limit, used_calls, usable_requests = _eodhd_request_budget(
        user_meta, buffer_calls, EODHD_MARKET_DATA_CALL_COST
    )
    request_budget = usable_requests
    if request_budget <= 0:
        print(
            "No EODHD market data request budget available for this run "
            f"(daily_limit={daily_limit}, used_calls={used_calls}, "
            f"buffer_calls={buffer_calls})."
        )
        return 0

    ticker_repo = SupportedTickerRepository(db_path)
    eligible = ticker_repo.list_eligible_for_market_data(
        provider=provider_norm,
        exchange_codes=resolved_exchange_codes,
        max_age_days=max_age_days,
        max_symbols=max_symbols,
        respect_backoff=respect_backoff,
        provider_symbols=symbol_filters,
        primary_only=True,
    )
    if not eligible:
        print(
            f"No eligible supported tickers found for {scope_label}. "
            "Refresh supported tickers first or relax freshness filters."
        )
        return 0

    plan = _plan_market_data_stage_run(eligible, request_budget)
    if plan.total_symbols == 0:
        print(
            "No EODHD market data request budget available for the current eligible "
            f"scope after planning (daily_limit={daily_limit}, used_calls={used_calls}, "
            f"buffer_calls={buffer_calls})."
        )
        return 0

    service = MarketDataService(db_path=db_path, config=config)
    state_repo = MarketDataFetchStateRepository(db_path)
    state_repo.initialize_schema()
    limiter = _RateLimiter(rate_value)
    total = plan.total_symbols
    processed = 0
    failed = 0
    pending_updates: List[MarketDataUpdate] = []
    pending_failures: List[Tuple[str, str]] = []
    fallback_symbols: List[SupportedTicker] = []
    fallback_budget = max(0, request_budget - plan.api_call_cost)
    last_flush = time.monotonic()
    last_report = time.monotonic()
    last_report_count = 0
    print(
        f"Fetching EODHD market data for {total} of {len(eligible)} eligible supported tickers across {scope_label} "
        f"at <= {rate_value:.2f} req/min "
        f"(daily_limit={daily_limit}, used_calls={used_calls}, "
        f"buffer_calls={buffer_calls}, budget_requests={request_budget}, "
        f"planned_api_calls={plan.api_call_cost}, planned_http_requests={plan.http_requests}, "
        f"bulk_exchanges={len(plan.bulk_tasks)}, symbol_requests={len(plan.symbol_tickers)})"
    )

    def maybe_flush(force: bool = False) -> None:
        nonlocal last_flush
        if not pending_updates and not pending_failures:
            return
        if not force:
            elapsed = time.monotonic() - last_flush
            buffered = len(pending_updates) + len(pending_failures)
            if (
                buffered < MARKET_DATA_WRITE_BATCH_SIZE
                and elapsed < MARKET_DATA_WRITE_BATCH_INTERVAL_SECONDS
            ):
                return
        _flush_market_data_batches(
            service, state_repo, pending_updates, pending_failures
        )
        last_flush = time.monotonic()

    def maybe_report(force: bool = False) -> None:
        nonlocal last_report, last_report_count
        completed = processed + failed
        elapsed = time.monotonic() - last_report
        if (
            not force
            and completed - last_report_count < MARKET_DATA_PROGRESS_SYMBOL_STEP
        ):
            if elapsed < MARKET_DATA_PROGRESS_INTERVAL_SECONDS:
                return
        print(
            f"[progress] stored={processed} failed={failed} completed={completed}/{total}",
            flush=True,
        )
        last_report = time.monotonic()
        last_report_count = completed

    bulk_executor: Optional[ThreadPoolExecutor] = None
    symbol_executor: Optional[ThreadPoolExecutor] = None
    interrupted = False
    try:
        if plan.bulk_tasks:
            bulk_executor = _create_interruptible_thread_executor(
                max_workers=min(MARKET_DATA_BULK_WORKERS, len(plan.bulk_tasks))
            )
            bulk_futures = {
                bulk_executor.submit(
                    _fetch_exchange_market_data,
                    api_key,
                    limiter,
                    task.exchange_code,
                ): task
                for task in plan.bulk_tasks
            }
            for future in as_completed(bulk_futures):
                task = bulk_futures[future]
                try:
                    fetched = future.result()
                except Exception as exc:  # pragma: no cover - network errors
                    LOGGER.error(
                        "Failed to refresh bulk market data for %s: %s",
                        task.exchange_code,
                        exc,
                    )
                    pending_failures.extend(
                        (ticker.symbol, str(exc)) for ticker in task.tickers
                    )
                    failed += len(task.tickers)
                    maybe_flush(force=True)
                    maybe_report()
                    continue

                stored_for_exchange = 0
                queued_fallbacks = 0
                for ticker in task.tickers:
                    bulk_data = fetched.get(ticker.symbol)
                    if bulk_data is None:
                        if fallback_budget > 0:
                            fallback_symbols.append(ticker)
                            fallback_budget -= EODHD_MARKET_DATA_CALL_COST
                            queued_fallbacks += 1
                        else:
                            pending_failures.append(
                                (
                                    ticker.symbol,
                                    f"Bulk exchange response missing {ticker.symbol}",
                                )
                            )
                            failed += 1
                        continue
                    try:
                        pending_updates.append(
                            _build_market_data_update(service, ticker, bulk_data)
                        )
                        stored_for_exchange += 1
                    except Exception as exc:
                        LOGGER.error(
                            "Failed to prepare market data for %s from bulk %s: %s",
                            ticker.symbol,
                            task.exchange_code,
                            exc,
                        )
                        pending_failures.append((ticker.symbol, str(exc)))
                        failed += 1
                processed += stored_for_exchange
                maybe_flush(force=True)
                print(
                    f"[bulk {task.exchange_code}] stored={stored_for_exchange} "
                    f"fallbacks={queued_fallbacks} symbols={len(task.tickers)}",
                    flush=True,
                )
                maybe_report()

        symbol_tickers = [*plan.symbol_tickers, *fallback_symbols]
        if symbol_tickers:
            symbol_executor = _create_interruptible_thread_executor(
                max_workers=min(MARKET_DATA_SYMBOL_WORKERS, len(symbol_tickers))
            )
            symbol_futures = {
                symbol_executor.submit(
                    _fetch_symbol_market_data,
                    api_key,
                    limiter,
                    ticker.symbol,
                ): ticker
                for ticker in symbol_tickers
            }
            for symbol_future in as_completed(symbol_futures):
                ticker = symbol_futures[symbol_future]
                try:
                    symbol_data = symbol_future.result()
                    pending_updates.append(
                        _build_market_data_update(service, ticker, symbol_data)
                    )
                    processed += 1
                except Exception as exc:  # pragma: no cover - network errors
                    LOGGER.error(
                        "Failed to refresh market data for %s: %s",
                        ticker.symbol,
                        exc,
                    )
                    pending_failures.append((ticker.symbol, str(exc)))
                    failed += 1
                maybe_flush()
                maybe_report()
    except KeyboardInterrupt:
        interrupted = True
        return _cancel_cli_command(
            f"\nCancelled after {processed + failed} completed symbols.",
            executors=[symbol_executor, bulk_executor],
            flushers=[
                lambda: maybe_flush(force=True),
                lambda: maybe_report(force=True),
            ],
        )
    finally:
        if not interrupted:
            if symbol_executor is not None:
                symbol_executor.shutdown(wait=True)
            if bulk_executor is not None:
                bulk_executor.shutdown(wait=True)

    maybe_flush(force=True)
    maybe_report(force=True)
    print(
        f"Stored market data for {processed} of {total} planned supported tickers in {db_path}"
    )
    return 0
