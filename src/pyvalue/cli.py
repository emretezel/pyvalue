"""Command line utilities for pyvalue.

Author: Emre Tezel
"""

from __future__ import annotations

import argparse
import csv
import concurrent.futures.thread as _thread_futures
from concurrent.futures import (
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import json
import logging
import os
import re
import sqlite3
from threading import Lock, Thread, local
import time
from collections import Counter
from pathlib import Path
from typing import (
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)
import weakref

from pyvalue.config import Config
from pyvalue.currency import (
    is_monetary_unit_kind,
    metric_currency_or_none,
    normalize_currency_code,
)
from pyvalue.ingestion import EODHDFundamentalsClient, SECCompanyFactsClient
from pyvalue.fx import (
    EODHDFXProvider,
    FXService,
    FrankfurterProvider,
    MissingFXRateError,
)
from pyvalue.marketdata import EODHDProvider, MarketDataUpdate, PriceData
from pyvalue.marketdata.service import MarketDataService
from pyvalue.metrics import REGISTRY
from pyvalue.metrics.base import (
    Metric,
    MetricCurrencyInvariantError,
    MetricResult,
    consume_metric_currency_invariant_error,
    metadata_for_metric,
)
from pyvalue.normalization import EODHDFactsNormalizer, SECFactsNormalizer
from pyvalue.ranking import compute_screen_ranking
from pyvalue.reporting import MetricCoverage, compute_fact_coverage
from pyvalue.screening import (
    Criterion,
    CriterionEvaluation,
    evaluate_criterion_detail,
    evaluate_criterion_verbose,
    load_screen,
    ranking_metric_ids,
    screen_metric_ids,
)
from pyvalue.logging_utils import (
    current_logging_config,
    setup_logging,
    suppress_console_metric_warnings,
    suppress_console_missing_fx_warnings,
)
from pyvalue.facts import RegionFactsRepository
from pyvalue.storage import (
    EntityMetadataRepository,
    FXRefreshStateRepository,
    FXRatesRepository,
    FXSupportedPairRecord,
    FXSupportedPairsRepository,
    FundamentalsNormalizationCandidate,
    FundamentalsNormalizationStateRepository,
    FundamentalsUpdate,
    FundamentalsRepository,
    FundamentalsFetchStateRepository,
    FinancialFactsRepository,
    FinancialFactsRefreshStateRecord,
    FinancialFactsRefreshStateRepository,
    FactRecord,
    IngestProgressExchange,
    IngestProgressSummary,
    MarketDataFetchStateRepository,
    MarketDataRepository,
    MarketSnapshotRecord,
    MetricComputeStatusRecord,
    MetricComputeStatusRepository,
    MetricRecord,
    MetricsRepository,
    SecurityRepository,
    SecurityMetadataUpdate,
    StoredFactRow,
    StoredMetricRow,
    SupportedExchangeRepository,
    SupportedTicker,
    SupportedTickerRepository,
)
from pyvalue.universe import USUniverseLoader

LOGGER = logging.getLogger(__name__)


def _resolve_ticker_target_currency(
    database: Union[str, Path],
    symbol: str,
    payload: Optional[Dict[str, object]] = None,
    *,
    ticker_repo: Optional[SupportedTickerRepository] = None,
) -> Optional[str]:
    """Resolve the trading currency for a ticker from stored market data only."""

    del payload  # Trading currency must never fall back to fundamentals payloads.
    repo = ticker_repo or SupportedTickerRepository(database)
    resolver = getattr(repo, "ticker_currency", None)
    if not callable(resolver):
        return None
    return resolver(symbol)


def _batch_values(values: Sequence[str], size: int) -> List[List[str]]:
    """Split ``values`` into stable ordered batches."""

    if size <= 0:
        raise ValueError("size must be positive")
    return [list(values[idx : idx + size]) for idx in range(0, len(values), size)]


DEFAULT_SCREEN_RESULTS_PREFIX = "data/screen_results"
EODHD_ALLOWED_TICKER_TYPES = {"COMMON STOCK", "PREFERRED STOCK", "STOCK"}
EODHD_FUNDAMENTALS_CALL_COST = 10
EODHD_MARKET_DATA_CALL_COST = 1
EODHD_MARKET_DATA_BULK_CALL_COST = 100
EODHD_MAX_REQUESTS_PER_MINUTE = 1000.0
FUNDAMENTALS_WORKERS = 16
FUNDAMENTALS_RATE_LIMIT_BURST = 2
FUNDAMENTALS_WRITE_BATCH_SIZE = 25
FUNDAMENTALS_WRITE_BATCH_INTERVAL_SECONDS = 0.25
FX_REFRESH_MAX_QUOTES_PER_REQUEST = 25
FX_REFRESH_MAX_DAYS_PER_REQUEST = 730
FX_FULL_BACKFILL_START = date(1900, 1, 1)
FUNDAMENTALS_PROGRESS_INTERVAL_SECONDS = 5.0
FUNDAMENTALS_PROGRESS_SYMBOL_STEP = 250
MARKET_DATA_BULK_BREAK_EVEN = 100
MARKET_DATA_BULK_WORKERS = 4
MARKET_DATA_SYMBOL_WORKERS = 16
MARKET_DATA_RATE_LIMIT_BURST = 2
MARKET_DATA_WRITE_BATCH_SIZE = 100
MARKET_DATA_WRITE_BATCH_INTERVAL_SECONDS = 0.25
MARKET_DATA_PROGRESS_INTERVAL_SECONDS = 5.0
MARKET_DATA_PROGRESS_SYMBOL_STEP = 250
METRICS_MAX_WORKERS = 16
METRICS_COMPUTE_BATCH_SIZE = 25
METRICS_PROGRESS_INTERVAL_SECONDS = 5.0
SECURITY_METADATA_CHUNK_SIZE = 500
SECURITY_METADATA_PROGRESS_INTERVAL_SECONDS = 5.0
SCREEN_PROGRESS_INTERVAL_SECONDS = 5.0
SCREEN_FAILURE_METRIC_LOAD_CHUNK_SIZE = 1000
SCREEN_FAILURE_PROGRESS_INTERVAL_SECONDS = 1.0
# Metric write flushes amortise SQLite fsync overhead -- batch ~200 symbols
# (~16k upsert rows at ~81 metrics each) per flush. End-of-run forced flush
# guarantees small universes still write everything; the time-based bound is
# just a liveness floor for very slow workers.
METRICS_WRITE_BATCH_SIZE = 200
METRICS_WRITE_BATCH_INTERVAL_SECONDS = 1.0
NORMALIZATION_MAX_WORKERS = 16

_MARKET_DATA_PROVIDER_LOCAL = local()
_FUNDAMENTALS_CLIENT_LOCAL = local()
_PRELOADED_MARKET_SNAPSHOT_MISSING = object()


@dataclass(frozen=True)
class _MarketDataExchangeTask:
    exchange_code: str
    tickers: Tuple[SupportedTicker, ...]


@dataclass(frozen=True)
class _PlannedMarketDataRun:
    bulk_tasks: Tuple[_MarketDataExchangeTask, ...]
    symbol_tickers: Tuple[SupportedTicker, ...]
    api_call_cost: int
    http_requests: int

    @property
    def total_symbols(self) -> int:
        return sum(len(task.tickers) for task in self.bulk_tasks) + len(
            self.symbol_tickers
        )


@dataclass(frozen=True)
class _PreparedFundamentalsRun:
    rate_value: float
    daily_limit: int
    used_calls: int
    buffer_calls: int
    request_budget: int
    eligible: Tuple[SupportedTicker, ...]


@dataclass(frozen=True)
class _NormalizedFactsResult:
    symbol: str
    rows: Tuple[StoredFactRow, ...]
    raw_fetched_at: str
    entity_name: Optional[str] = None
    entity_description: Optional[str] = None
    entity_sector: Optional[str] = None
    entity_industry: Optional[str] = None


@dataclass(frozen=True)
class _ComputedMetricsResult:
    symbol: str
    rows: Tuple[StoredMetricRow, ...]
    computed_count: int
    failures: Tuple["_MetricComputationFailure", ...] = ()
    attempts: Tuple["_MetricAttemptResult", ...] = ()


@dataclass(frozen=True)
class _ProfiledComputedMetricsBatchResult:
    """One worker batch result plus the worker-side read/compute timings."""

    results: Tuple[_ComputedMetricsResult, ...]
    read_seconds: float
    compute_seconds: float


@dataclass(frozen=True)
class _MetricComputationFailure:
    symbol: str
    metric_id: str
    reason: str
    message: str


@dataclass(frozen=True)
class _MetricAttemptResult:
    symbol: str
    metric_id: str
    status: str
    attempted_at: str
    stored_row: Optional[StoredMetricRow] = None
    reason_code: Optional[str] = None
    reason_detail: Optional[str] = None
    value_as_of: Optional[str] = None
    facts_refreshed_at: Optional[str] = None
    market_data_as_of: Optional[str] = None
    market_data_updated_at: Optional[str] = None
    persist_status: bool = True


@dataclass(frozen=True)
class _MetricAvailabilityState:
    metric_id: str
    record: Optional[MetricRecord]
    status_record: Optional[MetricComputeStatusRecord]
    stale: bool


@dataclass
class _ScreenMetricImpactSummary:
    """Aggregated NA impact for one metric referenced by a screen."""

    metric_id: str
    missing_symbols: set[str] = field(default_factory=set)
    affected_criteria: set[str] = field(default_factory=set)


@dataclass
class _CriterionFailureSummary:
    """Aggregated fallout counts for one screen criterion."""

    index: int
    criterion: Criterion
    fail_count: int = 0
    na_fail_count: int = 0
    threshold_fail_count: int = 0
    missing_metric_symbols: Dict[str, set[str]] = field(default_factory=dict)

    @property
    def label(self) -> str:
        return f"{self.index + 1}. {self.criterion.name}"


class _RateLimiter:
    """Token-bucket limiter shared across concurrent EODHD worker threads."""

    def __init__(
        self, rate_per_minute: float, burst: int = MARKET_DATA_RATE_LIMIT_BURST
    ):
        self.rate_per_second = max(rate_per_minute, 0.0) / 60.0
        self.capacity = float(max(burst, 1))
        self.tokens = self.capacity
        self.updated_at = time.monotonic()
        self._lock = Lock()

    def acquire(self) -> None:
        if self.rate_per_second <= 0:
            return
        while True:
            wait_time = 0.0
            with self._lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.tokens = min(
                    self.capacity, self.tokens + (elapsed * self.rate_per_second)
                )
                self.updated_at = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait_time = (1.0 - self.tokens) / self.rate_per_second
            if wait_time > 0:
                time.sleep(wait_time)


class _InterruptibleThreadPoolExecutor(ThreadPoolExecutor):
    """Thread pool whose workers do not block interpreter shutdown on Ctrl+C."""

    def _adjust_thread_count(self) -> None:
        if self._idle_semaphore.acquire(timeout=0):
            return

        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = "%s_%d" % (self._thread_name_prefix or self, num_threads)
            thread = Thread(
                name=thread_name,
                target=_thread_futures._worker,
                args=(
                    weakref.ref(self, weakref_cb),
                    self._work_queue,
                    self._initializer,
                    self._initargs,
                ),
                daemon=True,
            )
            thread.start()
            cast(set[Thread], self._threads).add(thread)


def _create_interruptible_thread_executor(
    max_workers: int,
) -> _InterruptibleThreadPoolExecutor:
    """Return a thread pool that can be abandoned promptly on Ctrl+C."""

    return _InterruptibleThreadPoolExecutor(max_workers=max_workers)


def _terminate_process_pool_workers(executor: ProcessPoolExecutor) -> None:
    """Best-effort terminate running process workers without waiting for them."""

    processes = getattr(executor, "_processes", None)
    if not isinstance(processes, dict):
        return
    for process in list(processes.values()):
        if process is None:
            continue
        try:
            if process.is_alive():
                process.terminate()
        except Exception:  # pragma: no cover - defensive cleanup path
            continue


def _shutdown_executor_now(executor: object) -> None:
    """Stop an executor without waiting for outstanding work to finish."""

    if isinstance(executor, ProcessPoolExecutor):
        _terminate_process_pool_workers(executor)
    shutdown = getattr(executor, "shutdown", None)
    if shutdown is None:
        return
    try:
        shutdown(wait=False, cancel_futures=True)
    except TypeError:
        shutdown(wait=False)


def _cancel_cli_command(
    message: str,
    *,
    executors: Sequence[object] = (),
    flushers: Sequence[Callable[[], None]] = (),
) -> int:
    """Flush parent state, stop workers, and exit a command cleanly."""

    for executor in executors:
        if executor is not None:
            _shutdown_executor_now(executor)
    for flusher in flushers:
        try:
            flusher()
        except Exception as exc:  # pragma: no cover - defensive cleanup path
            LOGGER.error("Failed to flush pending state during cancellation: %s", exc)
    print(message, flush=True)
    return 1


def _resolve_database_path(database: str) -> Path:
    """Resolve database path, falling back to repo data dir when using default name."""

    db_path = Path(database)
    if db_path.exists():
        return db_path
    if not db_path.is_absolute() and db_path.name == "pyvalue.db":
        repo_path = Path(__file__).resolve().parents[2] / "data" / db_path.name
        if repo_path.exists():
            return repo_path
    return db_path


def _default_screen_results_path(
    provider: str,
    exchange_code: str,
    as_of: Optional[date] = None,
) -> str:
    date_label = (as_of or date.today()).strftime("%Y%m%d")
    return (
        f"{DEFAULT_SCREEN_RESULTS_PREFIX}_{provider.upper()}_"
        f"{exchange_code.upper()}_{date_label}.csv"
    )


def _qualify_symbol(symbol: str, exchange: Optional[str] = None) -> str:
    base = symbol.strip().upper()
    if "." in base:
        return base
    if exchange:
        return f"{base}.{exchange.upper()}"
    return base


def _format_market_symbol(symbol: str, exchange: Optional[str]) -> str:
    """Format a symbol for market data providers (EODHD)."""

    normalized = symbol.upper()
    if "." in normalized:
        return normalized
    if exchange:
        exch = exchange.upper()
        # Use US suffix for common US exchange labels.
        if exch in {"US", "NYSE", "NASDAQ", "NYSE ARCA", "NYSE MKT", "CBOE BZX"}:
            return f"{normalized}.US"
        return f"{normalized}.{exch}"
    return normalized


def _normalize_provider(provider: Optional[str]) -> str:
    if not provider:
        raise SystemExit("Provider is required (SEC or EODHD).")
    normalized = provider.strip().upper()
    if normalized not in {"SEC", "EODHD"}:
        raise SystemExit(f"Unsupported provider: {provider}")
    return normalized


def _catalog_bootstrap_guidance(provider: str) -> str:
    provider_norm = provider.strip().upper()
    if provider_norm == "SEC":
        return "Run refresh-supported-exchanges --provider SEC and refresh-supported-tickers --provider SEC first."
    if provider_norm == "EODHD":
        return "Run refresh-supported-exchanges --provider EODHD and refresh-supported-tickers --provider EODHD first."
    return "Populate supported_tickers first."


def _symbols_for_exchange_or_raise(
    db_path: Path,
    provider: str,
    exchange_code: str,
) -> List[str]:
    """Return canonical catalog symbols for a provider/exchange or raise."""

    provider_norm = _normalize_provider(provider)
    exchange_norm = exchange_code.upper()
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    symbols = ticker_repo.list_symbols_by_exchange(provider_norm, exchange_norm)
    if symbols:
        return symbols

    available_exchanges = ticker_repo.available_exchanges(provider_norm)
    raise SystemExit(
        f"No supported tickers found for provider {provider_norm} on exchange {exchange_norm}. "
        f"{_catalog_bootstrap_guidance(provider_norm)} "
        f"Available exchanges: {', '.join(available_exchanges) if available_exchanges else 'none'}. "
        f"Database: {db_path}"
    )


def _select_metric_classes(metric_ids: Optional[Sequence[str]]) -> List[type]:
    """Return metric classes for requested ids, raising on unknown identifiers."""

    ids = list(metric_ids) if metric_ids else list(REGISTRY.keys())
    metric_classes: List[type] = []
    for metric_id in ids:
        metric_cls = REGISTRY.get(metric_id)
        if metric_cls is None:
            raise SystemExit(f"Unknown metric id: {metric_id}")
        metric_classes.append(metric_cls)
    return metric_classes


def _parse_currency_codes(values: Optional[Sequence[str]]) -> Optional[set[str]]:
    if not values:
        return None
    codes: set[str] = set()
    for item in values:
        if not item:
            continue
        for part in re.split(r"[,\s]+", item.strip()):
            if part:
                codes.add(part.upper())
    return codes or None


def _parse_exchange_filters(values: Optional[Sequence[str]]) -> Optional[set[str]]:
    if not values:
        return None
    filters: set[str] = set()
    for item in values:
        if not item:
            continue
        for part in item.split(","):
            part = part.strip()
            if part:
                filters.add(part.upper())
    return filters or None


def _parse_symbol_filters(values: Optional[Sequence[str]]) -> Optional[List[str]]:
    if not values:
        return None
    symbols: List[str] = []
    seen: set[str] = set()
    for item in values:
        if not item:
            continue
        for part in re.split(r"[,\s]+", item.strip()):
            if not part:
                continue
            symbol = part.upper()
            if symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)
    return symbols or None


def _validate_scope_selector(
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
) -> Tuple[Optional[List[str]], Optional[List[str]]]:
    symbol_filters = _parse_symbol_filters(symbols)
    exchange_filters = _parse_exchange_filters(exchange_codes)
    selected = sum(
        1
        for flag in (
            bool(symbol_filters),
            bool(exchange_filters),
            bool(all_supported),
        )
        if flag
    )
    if selected == 0:
        return None, None
    if selected != 1:
        raise SystemExit(
            "At most one scope selector may be provided: use one of "
            "--symbols, --exchange-codes, or --all-supported."
        )
    return symbol_filters, sorted(exchange_filters) if exchange_filters else None


def _normalize_provider_scope_symbol(provider: str, symbol: str) -> str:
    candidate = symbol.strip().upper()
    ticker, suffix = candidate.rsplit(".", 1) if "." in candidate else (candidate, None)
    provider_norm = _normalize_provider(provider)
    if provider_norm == "SEC":
        return f"{ticker}.US"
    if suffix is None:
        raise SystemExit(
            "EODHD --symbols entries must be fully qualified, for example SHEL.LSE."
        )
    return candidate


def _normalize_canonical_scope_symbol(symbol: str) -> str:
    candidate = symbol.strip().upper()
    if "." not in candidate:
        raise SystemExit(
            "--symbols entries must use canonical qualified symbols, for example AAPL.US or SHEL.LSE."
        )
    return candidate


def _resolve_provider_scope_rows(
    database: str,
    provider: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
) -> Tuple[List["SupportedTicker"], Optional[List[str]], Optional[List[str]]]:
    provider_norm = _normalize_provider(provider)
    symbol_filters, exchange_filters = _validate_scope_selector(
        symbols, exchange_codes, all_supported
    )
    ticker_repo = SupportedTickerRepository(database)
    ticker_repo.initialize_schema()

    if symbol_filters:
        normalized_symbols = [
            _normalize_provider_scope_symbol(provider_norm, symbol)
            for symbol in symbol_filters
        ]
        rows = ticker_repo.list_for_provider(
            provider_norm,
            provider_symbols=normalized_symbols,
        )
        found = {row.symbol.upper() for row in rows}
        missing = [symbol for symbol in normalized_symbols if symbol not in found]
        if missing:
            raise SystemExit(
                f"Unsupported tickers for provider {provider_norm}: {', '.join(missing)}. "
                f"{_catalog_bootstrap_guidance(provider_norm)}"
            )
        return rows, normalized_symbols, None

    rows = ticker_repo.list_for_provider(
        provider_norm,
        exchange_codes=exchange_filters,
    )
    if not rows:
        scope_label = (
            ", ".join(exchange_filters) if exchange_filters else "all supported tickers"
        )
        raise SystemExit(
            f"No supported tickers found for provider {provider_norm} in scope {scope_label}. "
            f"{_catalog_bootstrap_guidance(provider_norm)}"
        )
    return rows, None, exchange_filters


def _resolve_canonical_scope_symbols(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
) -> Tuple[List[str], Optional[List[str]], Optional[List[str]]]:
    symbol_filters, exchange_filters = _validate_scope_selector(
        symbols, exchange_codes, all_supported
    )
    ticker_repo = SupportedTickerRepository(database)
    ticker_repo.initialize_schema()

    if symbol_filters:
        normalized_symbols = [
            _normalize_canonical_scope_symbol(symbol) for symbol in symbol_filters
        ]
        supported = set(ticker_repo.list_canonical_symbols())
        missing = [symbol for symbol in normalized_symbols if symbol not in supported]
        if missing:
            raise SystemExit(
                f"Unsupported canonical tickers: {', '.join(missing)}. Populate supported_tickers first."
            )
        return normalized_symbols, normalized_symbols, None

    canonical_symbols = ticker_repo.list_canonical_symbols(exchange_filters)
    if not canonical_symbols:
        scope_label = (
            ", ".join(exchange_filters) if exchange_filters else "all supported tickers"
        )
        raise SystemExit(
            f"No canonical supported tickers found in scope {scope_label}. Populate supported_tickers first."
        )
    return canonical_symbols, None, exchange_filters


def _scope_label(
    symbol_filters: Optional[Sequence[str]],
    exchange_filters: Optional[Sequence[str]],
    default_label: str = "all supported tickers",
) -> str:
    if symbol_filters:
        return ", ".join(symbol_filters)
    if exchange_filters:
        return ", ".join(exchange_filters)
    return default_label


def _summarize_progress_breakdown(
    rows: Sequence[IngestProgressExchange],
) -> IngestProgressSummary:
    return IngestProgressSummary(
        total_supported=sum(row.total_supported for row in rows),
        stored=sum(row.stored for row in rows),
        missing=sum(row.missing for row in rows),
        stale=sum(row.stale for row in rows),
        blocked=sum(row.blocked for row in rows),
        error_rows=sum(row.error_rows for row in rows),
    )


def _refresh_supported_exchanges_for_provider(
    database: str,
    provider: str,
    client: EODHDFundamentalsClient,
) -> int:
    """Refresh and persist the supported exchange catalog for a provider."""

    provider_norm = provider.strip().upper()
    if provider_norm != "EODHD":
        raise SystemExit(
            "refresh-supported-exchanges currently only supports provider=EODHD."
        )
    repo = SupportedExchangeRepository(database)
    repo.initialize_schema()
    rows = client.list_exchanges()
    return repo.replace_for_provider(provider_norm, rows)


def _resolve_eodhd_exchange_metadata(
    database: str,
    client: EODHDFundamentalsClient,
    exchange_code: str,
) -> Optional[Dict[str, Optional[str]]]:
    """Resolve exchange metadata from the local catalog, bootstrapping on miss."""

    repo = SupportedExchangeRepository(database)
    record = repo.fetch("EODHD", exchange_code)
    if record is None:
        _refresh_supported_exchanges_for_provider(
            database=database,
            provider="EODHD",
            client=client,
        )
        record = repo.fetch("EODHD", exchange_code)
    if record is None:
        return None
    return {
        "Name": record.name,
        "Code": record.code,
        "Country": record.country,
        "Currency": record.currency,
        "OperatingMIC": record.operating_mic,
        "CountryISO2": record.country_iso2,
        "CountryISO3": record.country_iso3,
    }


def _coerce_int(value: object, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _eodhd_api_requests_used_today(user_meta: Dict[str, object]) -> int:
    request_date = str(user_meta.get("apiRequestsDate") or "").strip()
    today_utc = datetime.now(timezone.utc).date().isoformat()
    if request_date and request_date != today_utc:
        return 0
    return _coerce_int(user_meta.get("apiRequests"), 0)


def _resolve_eodhd_fundamentals_rate(rate: Optional[float]) -> float:
    config = Config()
    configured = float(config.eodhd_fundamentals_requests_per_minute)
    rate_value = configured if rate is None else rate
    if rate_value is None or rate_value <= 0:
        raise SystemExit(
            "--rate must be greater than 0 for EODHD fundamentals ingestion."
        )
    return min(rate_value, EODHD_MAX_REQUESTS_PER_MINUTE)


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
    general = payload.get("General")
    currency = ticker.currency
    if isinstance(general, Mapping):
        currency = (
            str(general.get("CurrencyCode") or ticker.currency or "").strip()
            or ticker.currency
        )
    return FundamentalsUpdate(
        security_id=ticker.security_id,
        provider_symbol=ticker.symbol,
        provider_exchange_code=ticker.exchange_code,
        currency=currency,
        data=json.dumps(payload),
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )


def _flush_fundamentals_batches(
    repo: FundamentalsRepository,
    state_repo: FundamentalsFetchStateRepository,
    success_updates: List[FundamentalsUpdate],
    failures: List[Tuple[str, str]],
) -> None:
    if success_updates:
        repo.upsert_many("EODHD", success_updates)
        state_repo.mark_success_many(
            "EODHD",
            [update.provider_symbol for update in success_updates],
        )
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
    data,
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
        market_cap=prepared.market_cap,
        currency=prepared.currency,
        source_provider="EODHD",
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


def _refresh_supported_tickers_for_exchange(
    database: str,
    provider: str,
    client: EODHDFundamentalsClient,
    exchange_code: str,
) -> Tuple[int, int]:
    """Refresh one exchange's supported tickers and prune stale fetch state."""

    provider_norm = provider.strip().upper()
    exchange_norm = exchange_code.strip().upper()
    rows = client.list_symbols(exchange_norm)
    filtered_rows: List[Dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = (row.get("Code") or "").strip()
        if not code:
            continue
        security_type = (row.get("Type") or "").strip()
        if security_type.upper() not in EODHD_ALLOWED_TICKER_TYPES:
            continue
        filtered_rows.append(row)

    ticker_repo = SupportedTickerRepository(database)
    existing = ticker_repo.list_for_exchange(provider_norm, exchange_norm)
    existing_symbols = {row.symbol for row in existing}
    stored = ticker_repo.replace_for_exchange(
        provider_norm, exchange_norm, filtered_rows
    )
    current = ticker_repo.list_for_exchange(provider_norm, exchange_norm)
    current_symbols = {row.symbol for row in current}
    removed_symbols = sorted(existing_symbols - current_symbols)

    state_repo = FundamentalsFetchStateRepository(database)
    state_repo.delete_symbols(provider_norm, removed_symbols)
    market_state_repo = MarketDataFetchStateRepository(database)
    market_state_repo.delete_symbols(provider_norm, removed_symbols)
    return stored, len(removed_symbols)


def _list_eodhd_exchange_codes(
    database: str,
    client: EODHDFundamentalsClient,
) -> List[str]:
    repo = SupportedExchangeRepository(database)
    exchanges = repo.list_all("EODHD")
    if not exchanges:
        _refresh_supported_exchanges_for_provider(database, "EODHD", client)
        exchanges = repo.list_all("EODHD")
    return [row.code for row in exchanges]


def _eodhd_request_budget(
    user_meta: Dict[str, object],
    buffer_calls: int,
    call_cost: int,
) -> Tuple[int, int, int]:
    daily_limit = _coerce_int(user_meta.get("dailyRateLimit"), 0)
    if daily_limit <= 0:
        raise SystemExit("Could not determine EODHD dailyRateLimit from the user API.")
    used_calls = _eodhd_api_requests_used_today(user_meta)
    usable_calls = max(0, daily_limit - used_calls - max(buffer_calls, 0))
    if call_cost <= 0:
        raise SystemExit("EODHD call cost must be greater than 0.")
    usable_requests = usable_calls // call_cost
    return daily_limit, used_calls, usable_requests


def _safe_eodhd_quota_snapshot(
    api_key: Optional[str],
    buffer_calls: int,
    call_cost: int,
) -> Optional[Dict[str, int]]:
    """Return EODHD quota information when available, else None."""

    if not api_key:
        return None
    try:
        client = EODHDFundamentalsClient(api_key=api_key)
        user_meta = client.user_metadata()
        daily_limit, used_calls, usable_requests = _eodhd_request_budget(
            user_meta, buffer_calls, call_cost
        )
    except SystemExit:
        return None
    except Exception as exc:  # pragma: no cover - network errors
        LOGGER.warning("Could not fetch EODHD quota snapshot: %s", exc)
        return None
    return {
        "daily_limit": daily_limit,
        "used_calls": used_calls,
        "buffer_calls": buffer_calls,
        "usable_requests": usable_requests,
    }


def _select_listing_symbols_by_exchange(
    database: str,
    provider: str,
    exchange_code: str,
) -> List[str]:
    ticker_repo = SupportedTickerRepository(database)
    return ticker_repo.list_symbols_by_exchange(provider, exchange_code)


def build_parser() -> argparse.ArgumentParser:
    """Configure the root parser with subcommands."""

    parser = argparse.ArgumentParser(description="pyvalue data utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_scope_args(command_parser: argparse.ArgumentParser) -> None:
        scope = command_parser.add_mutually_exclusive_group(required=False)
        scope.add_argument(
            "--symbols",
            nargs="+",
            default=None,
            help=(
                "Space or comma separated list of fully qualified symbols. "
                "Defaults to the full supported universe when omitted."
            ),
        )
        scope.add_argument(
            "--exchange-codes",
            nargs="+",
            default=None,
            help=(
                "Space or comma separated list of exchange codes. "
                "Defaults to the full supported universe when omitted."
            ),
        )
        scope.add_argument(
            "--all-supported",
            action="store_true",
            help="Select the full supported universe in the current catalog.",
        )

    refresh_supported_exchanges = subparsers.add_parser(
        "refresh-supported-exchanges",
        help="Refresh and persist the provider-supported exchange catalog.",
    )
    refresh_supported_exchanges.add_argument(
        "--provider",
        default="EODHD",
        choices=["SEC", "EODHD"],
        help="Supported exchange provider to refresh (default: %(default)s).",
    )
    refresh_supported_exchanges.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    refresh_supported_tickers = subparsers.add_parser(
        "refresh-supported-tickers",
        help="Refresh and persist the provider-supported ticker catalog.",
    )
    refresh_supported_tickers.add_argument(
        "--provider",
        default="EODHD",
        choices=["SEC", "EODHD"],
        help="Supported ticker provider to refresh (default: %(default)s).",
    )
    refresh_supported_tickers.add_argument(
        "--exchange-codes",
        nargs="+",
        default=None,
        help="Optional exchange-code subset (space or comma separated).",
    )
    refresh_supported_tickers.add_argument(
        "--all-supported",
        action="store_true",
        help="Refresh every supported exchange for the provider.",
    )
    refresh_supported_tickers.add_argument(
        "--include-etfs",
        action="store_true",
        help="SEC only: keep ETFs in the supported ticker catalog.",
    )
    refresh_supported_tickers.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    recalc_market_cap = subparsers.add_parser(
        "recalc-market-cap",
        help="Recompute stored market caps using latest price and share counts.",
    )
    recalc_market_cap.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(recalc_market_cap)

    clear_facts = subparsers.add_parser(
        "clear-financial-facts",
        help="Delete all normalized financial facts.",
    )
    clear_facts.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    clear_fundamentals_raw = subparsers.add_parser(
        "clear-fundamentals-raw",
        help="Delete all stored raw fundamentals.",
    )
    clear_fundamentals_raw.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    clear_metrics = subparsers.add_parser(
        "clear-metrics",
        help="Delete all computed metrics.",
    )
    clear_metrics.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    clear_market_data = subparsers.add_parser(
        "clear-market-data",
        help="Delete all stored market data snapshots.",
    )
    clear_market_data.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    ingest_fundamentals = subparsers.add_parser(
        "ingest-fundamentals",
        help="Download fundamentals for supported tickers from the chosen provider.",
    )
    ingest_fundamentals.add_argument(
        "--provider",
        default="EODHD",
        choices=["SEC", "EODHD"],
        help="Fundamentals provider to use (default: %(default)s).",
    )
    add_scope_args(ingest_fundamentals)
    ingest_fundamentals.add_argument(
        "--user-agent",
        default=None,
        help="Custom User-Agent for SEC (falls back to PYVALUE_SEC_USER_AGENT).",
    )
    ingest_fundamentals.add_argument(
        "--cik",
        default=None,
        help="Optional SEC CIK override (10-digit).",
    )
    ingest_fundamentals.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    ingest_fundamentals.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Throttle rate (SEC: req/sec, EODHD: symbols/min). Defaults depend on provider.",
    )
    ingest_fundamentals.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Maximum number of symbols to ingest in this run.",
    )
    ingest_fundamentals.add_argument(
        "--max-age-days",
        type=int,
        default=30,
        help=(
            "Only ingest symbols with older fundamentals (days) or missing "
            "data (default: %(default)s)."
        ),
    )
    ingest_fundamentals.add_argument(
        "--retry-failed-now",
        action="store_true",
        help="Ignore retry backoff and retry previously failed symbols immediately.",
    )
    fundamentals_progress = subparsers.add_parser(
        "report-fundamentals-progress",
        help="Report EODHD fundamentals ingest progress across supported tickers.",
    )
    fundamentals_progress.add_argument(
        "--provider",
        default="EODHD",
        choices=["EODHD"],
        help="Fundamentals provider to report on (default: %(default)s).",
    )
    fundamentals_progress.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    fundamentals_progress.add_argument(
        "--exchange-codes",
        nargs="+",
        default=None,
        help="Optional exchange-code filter (space or comma separated). Defaults to all stored supported tickers.",
    )
    fundamentals_progress_mode = fundamentals_progress.add_mutually_exclusive_group()
    fundamentals_progress_mode.add_argument(
        "--max-age-days",
        type=int,
        default=30,
        help="Freshness window in days (default: %(default)s).",
    )
    fundamentals_progress_mode.add_argument(
        "--missing-only",
        action="store_true",
        help="Only require that a raw fundamentals payload exists, regardless of age.",
    )

    market_data_progress = subparsers.add_parser(
        "report-market-data-progress",
        help="Report EODHD market data refresh progress across supported tickers.",
    )
    market_data_progress.add_argument(
        "--provider",
        default="EODHD",
        choices=["EODHD"],
        help="Market data provider to report on (default: %(default)s).",
    )
    market_data_progress.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    market_data_progress.add_argument(
        "--exchange-codes",
        nargs="+",
        default=None,
        help="Optional exchange-code filter (space or comma separated). Defaults to all stored supported tickers.",
    )
    market_data_progress.add_argument(
        "--max-age-days",
        type=int,
        default=30,
        help="Freshness window in days (default: %(default)s).",
    )

    market_data = subparsers.add_parser(
        "update-market-data",
        help="Fetch latest market data for supported tickers and persist it.",
    )
    market_data.add_argument(
        "--provider",
        default="EODHD",
        choices=["EODHD"],
        help="Market data provider to use (default: %(default)s).",
    )
    market_data.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(market_data)
    market_data.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Throttle rate in requests per minute (defaults to eodhd.market_data_requests_per_minute).",
    )
    market_data.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Maximum number of symbols to attempt in this run, before quota capping.",
    )
    market_data.add_argument(
        "--max-age-days",
        type=int,
        default=30,
        help=(
            "Refresh only stale or missing market data older than this many "
            "days (default: %(default)s)."
        ),
    )
    market_data.add_argument(
        "--retry-failed-now",
        action="store_true",
        help="Ignore retry backoff and retry previously failed symbols immediately.",
    )

    normalize_fundamentals = subparsers.add_parser(
        "normalize-fundamentals",
        help=(
            "Normalize stored fundamentals across the requested supported-ticker "
            "scope. Bulk runs parallelize automatically."
        ),
    )
    normalize_fundamentals.add_argument(
        "--provider",
        default="EODHD",
        choices=["SEC", "EODHD"],
        help="Fundamentals provider to normalize (default: %(default)s).",
    )
    add_scope_args(normalize_fundamentals)
    normalize_fundamentals.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    normalize_fundamentals.add_argument(
        "--force",
        action="store_true",
        help="Re-normalize even when stored raw fundamentals are already up to date.",
    )

    compute_metrics = subparsers.add_parser(
        "compute-metrics",
        help="Compute one or more metrics for the requested canonical ticker scope.",
    )
    compute_metrics.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(compute_metrics)
    compute_metrics.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to compute (default: all registered metrics).",
    )
    compute_metrics.add_argument(
        "--show-metric-warnings",
        action="store_true",
        help="Show metric/data-quality warnings on the console (default: suppressed).",
    )
    compute_metrics.add_argument(
        "--profile",
        action="store_true",
        help=(
            "Print read/compute/write/total wall-clock timings at end of run "
            "(useful for tuning compute-metrics performance)."
        ),
    )

    refresh_fx_rates = subparsers.add_parser(
        "refresh-fx-rates",
        help="Fetch and store FX rates for currencies already present in the project database.",
    )
    refresh_fx_rates.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    refresh_fx_rates.add_argument(
        "--start-date",
        default=None,
        help="Optional historical FX backfill start date (YYYY-MM-DD). Defaults to the end date.",
    )
    refresh_fx_rates.add_argument(
        "--end-date",
        default=None,
        help="Optional FX refresh end date (YYYY-MM-DD). Defaults to today.",
    )

    fact_report = subparsers.add_parser(
        "report-fact-freshness",
        help="List missing or stale financial facts required by metrics for the requested canonical scope.",
    )
    fact_report.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(fact_report)
    fact_report.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to include (default: all registered metrics)",
    )
    fact_report.add_argument(
        "--max-age-days",
        type=int,
        default=30,
        help="Fact freshness window in days (default: %(default)s)",
    )
    fact_report.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV path for detailed concept coverage.",
    )
    fact_report.add_argument(
        "--show-all",
        action="store_true",
        help="Show concepts even when all symbols are fresh.",
    )

    metric_report = subparsers.add_parser(
        "report-metric-coverage",
        help="Count how many symbols can compute all requested metrics for the requested canonical scope without writing results.",
    )
    metric_report.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(metric_report)
    metric_report.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to include (default: all registered metrics)",
    )

    failure_report = subparsers.add_parser(
        "report-metric-failures",
        help="Summarize warning reasons for metric computation failures on the requested canonical scope.",
    )
    failure_report.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(failure_report)
    failure_report.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to include (default: all registered metrics)",
    )
    failure_report.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV path for metric failure reasons.",
    )

    screen_failure_report = subparsers.add_parser(
        "report-screen-failures",
        help="Rank which screen criteria and missing metrics exclude the most symbols for the requested canonical scope.",
    )
    screen_failure_report.add_argument(
        "--config",
        required=True,
        help="Path to screening config (YAML)",
    )
    screen_failure_report.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(screen_failure_report)
    screen_failure_report.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV path for metric-level screen failure reasons.",
    )

    purge_nonfilers = subparsers.add_parser(
        "purge-us-nonfilers",
        help="Remove SEC US supported tickers that have no 10-K/10-Q filings in stored SEC company facts.",
    )
    purge_nonfilers.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    purge_nonfilers.add_argument(
        "--apply",
        action="store_true",
        help="Apply deletions instead of just printing the symbols to be removed.",
    )

    run_screen = subparsers.add_parser(
        "run-screen",
        help="Evaluate screening criteria for the requested canonical scope.",
    )
    run_screen.add_argument(
        "--config",
        required=True,
        help="Path to screening config (YAML)",
    )
    run_screen.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file (default: %(default)s)",
    )
    add_scope_args(run_screen)
    run_screen.add_argument(
        "--show-metric-warnings",
        action="store_true",
        help="Show metric/data-quality warnings on the console (default: suppressed).",
    )
    run_screen.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV path for passing results.",
    )

    refresh_security_metadata = subparsers.add_parser(
        "refresh-security-metadata",
        help="Refresh canonical security metadata from stored raw fundamentals without rewriting normalized facts.",
    )
    refresh_security_metadata.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file (default: %(default)s)",
    )
    add_scope_args(refresh_security_metadata)

    return parser


def _should_keep_listing(include_etfs: bool, listing_is_etf: bool) -> bool:
    """Return True if the listing should be kept after ETF filtering."""

    return include_etfs or not listing_is_etf


def cmd_load_universe(
    provider: str,
    database: str,
    include_etfs: bool,
    exchange_code: Optional[str],
    currencies: Optional[Sequence[str]] = None,
    include_exchanges: Optional[Sequence[str]] = None,
) -> int:
    """Load provider catalog data into the canonical supported_tickers table."""

    provider_norm = _normalize_provider(provider)
    if provider_norm == "SEC":
        if exchange_code or currencies or include_exchanges:
            raise SystemExit(
                "Flags --exchange-code, --currencies, and --include-exchanges are only valid with provider=EODHD."
            )
        return cmd_load_us_universe(database=database, include_etfs=include_etfs)

    return cmd_load_eodhd_universe(
        database=database,
        include_etfs=include_etfs,
        exchange_code=exchange_code or "",
        currencies=currencies,
        include_exchanges=include_exchanges,
    )


def cmd_load_us_universe(database: str, include_etfs: bool) -> int:
    """Load the SEC-supported US catalog into supported_tickers."""

    loader = USUniverseLoader()
    listings = loader.load()
    LOGGER.info("Fetched %s US listings", len(listings))

    # Drop ETFs unless explicitly requested in the CLI arguments.
    filtered = [
        item for item in listings if _should_keep_listing(include_etfs, item.is_etf)
    ]
    LOGGER.info("Remaining listings after ETF filter: %s", len(filtered))

    repo = SupportedTickerRepository(database)
    repo.initialize_schema()
    inserted = repo.replace_from_listings("SEC", "US", filtered)

    print(f"Stored {inserted} SEC supported tickers for US in {database}")
    return 0


def cmd_load_eodhd_universe(
    database: str,
    include_etfs: bool,
    exchange_code: str,
    currencies: Optional[Sequence[str]] = None,
    include_exchanges: Optional[Sequence[str]] = None,
) -> int:
    """Exit with guidance because EODHD now uses refresh-supported-tickers."""

    if currencies or include_exchanges or include_etfs or exchange_code:
        raise SystemExit(
            "load-universe --provider EODHD is deprecated. "
            "Use `pyvalue refresh-supported-exchanges --provider EODHD` and "
            "`pyvalue refresh-supported-tickers --provider EODHD --exchange-code <CODE>` instead."
        )
    raise SystemExit(
        "load-universe --provider EODHD is deprecated. "
        "Use `pyvalue refresh-supported-exchanges --provider EODHD` and "
        "`pyvalue refresh-supported-tickers --provider EODHD --exchange-code <CODE>` instead."
    )


def _require_eodhd_key() -> str:
    api_key = Config().eodhd_api_key
    if not api_key:
        raise SystemExit(
            "EODHD API key missing. Add [eodhd].api_key to private/config.toml."
        )
    return api_key


def cmd_refresh_supported_exchanges(provider: str, database: str) -> int:
    """Refresh the persisted supported exchange catalog."""

    provider_norm = provider.strip().upper()
    repo = SupportedExchangeRepository(database)
    repo.initialize_schema()
    if provider_norm == "SEC":
        repo.ensure_fixed_exchange(
            provider="SEC",
            provider_exchange_code="US",
            canonical_exchange_code="US",
            name="United States",
            country="US",
            currency="USD",
        )
        stored = len(repo.list_all("SEC"))
    elif provider_norm == "EODHD":
        api_key = _require_eodhd_key()
        client = EODHDFundamentalsClient(api_key=api_key)
        stored = _refresh_supported_exchanges_for_provider(
            database=database,
            provider=provider_norm,
            client=client,
        )
    else:
        raise SystemExit(f"Unsupported provider: {provider}")
    print(f"Stored {stored} supported exchanges for {provider_norm} in {database}")
    return 0


def cmd_refresh_supported_tickers(
    provider: str,
    database: str,
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    include_etfs: bool,
) -> int:
    """Refresh the persisted supported ticker catalog."""

    provider_norm = provider.strip().upper()
    requested_exchanges = _parse_exchange_filters(exchange_codes)
    if provider_norm == "SEC":
        if requested_exchanges and requested_exchanges != {"US"}:
            raise SystemExit("provider=SEC only supports --exchange-codes US.")
        exchange_list = ["US"]
        repo = SupportedExchangeRepository(database)
        repo.initialize_schema()
        repo.ensure_fixed_exchange(
            provider="SEC",
            provider_exchange_code="US",
            canonical_exchange_code="US",
            name="United States",
            country="US",
            currency="USD",
        )
        loader = USUniverseLoader()
        listings = loader.load()
        filtered = [
            item for item in listings if _should_keep_listing(include_etfs, item.is_etf)
        ]
        ticker_repo = SupportedTickerRepository(database)
        ticker_repo.initialize_schema()
        existing = ticker_repo.list_for_exchange("SEC", "US")
        existing_symbols = {row.symbol for row in existing}
        stored = ticker_repo.replace_from_listings("SEC", "US", filtered)
        current = ticker_repo.list_for_exchange("SEC", "US")
        current_symbols = {row.symbol for row in current}
        removed_symbols = sorted(existing_symbols - current_symbols)
        FundamentalsFetchStateRepository(database).delete_symbols(
            "SEC", removed_symbols
        )
        MarketDataFetchStateRepository(database).delete_symbols("SEC", removed_symbols)
        print(
            f"[1/1] Stored {stored} supported tickers for US in {database} "
            f"(removed {len(removed_symbols)} unsupported tickers)"
        )
        return 0

    if provider_norm != "EODHD":
        raise SystemExit(f"Unsupported provider: {provider}")

    api_key = _require_eodhd_key()
    eodhd_client = EODHDFundamentalsClient(api_key=api_key)
    if all_supported or not requested_exchanges:
        exchange_list = _list_eodhd_exchange_codes(database, eodhd_client)
    else:
        exchange_list = sorted(requested_exchanges)
        for exchange_norm in exchange_list:
            meta = _resolve_eodhd_exchange_metadata(
                database, eodhd_client, exchange_norm
            )
            if meta is None:
                raise SystemExit(
                    f"Exchange {exchange_norm} not found in the EODHD exchange list."
                )

    if not exchange_list:
        print("No supported exchanges available to refresh.")
        return 0

    total = len(exchange_list)
    for idx, code in enumerate(exchange_list, 1):
        stored, removed = _refresh_supported_tickers_for_exchange(
            database=database,
            provider=provider_norm,
            client=eodhd_client,
            exchange_code=code,
        )
        print(
            f"[{idx}/{total}] Stored {stored} supported tickers for {code} "
            f"in {database} (removed {removed} unsupported tickers)"
        )
    return 0


def cmd_ingest_fundamentals_global(
    provider: str,
    database: str,
    exchange_codes: Optional[Sequence[str]],
    rate: Optional[float],
    max_symbols: Optional[int],
    max_age_days: Optional[int],
    respect_backoff: bool,
) -> int:
    """Fetch EODHD fundamentals across supported tickers with quota awareness."""

    provider_norm = provider.strip().upper()
    if provider_norm != "EODHD":
        raise SystemExit(
            "ingest-fundamentals-global currently only supports provider=EODHD."
        )

    requested_exchange_codes = _parse_exchange_filters(exchange_codes)
    api_key = _require_eodhd_key()
    prepared = _prepare_eodhd_fundamentals_run(
        database=database,
        api_key=api_key,
        exchange_codes=sorted(requested_exchange_codes)
        if requested_exchange_codes
        else None,
        provider_symbols=None,
        rate=rate,
        max_symbols=max_symbols,
        max_age_days=max_age_days,
        respect_backoff=respect_backoff,
        missing_only=max_age_days is None,
    )
    scope_label = (
        ", ".join(sorted(requested_exchange_codes))
        if requested_exchange_codes
        else "all exchanges"
    )
    return _run_eodhd_fundamentals_ingestion(
        database=database,
        api_key=api_key,
        scope_label=scope_label,
        prepared=prepared,
    )


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


def cmd_update_market_data_global(
    provider: str,
    database: str,
    exchange_codes: Optional[Sequence[str]],
    rate: Optional[float],
    max_symbols: Optional[int],
    max_age_days: int,
    respect_backoff: bool,
) -> int:
    """Refresh EODHD market data across supported tickers with quota awareness."""

    provider_norm = provider.strip().upper()
    if provider_norm != "EODHD":
        raise SystemExit(
            "update-market-data-global currently only supports provider=EODHD."
        )

    api_key = _require_eodhd_key()
    client = EODHDFundamentalsClient(api_key=api_key)
    config = Config()
    buffer_calls = max(config.eodhd_market_data_daily_buffer_calls, 0)
    rate_value = _resolve_eodhd_market_data_rate(rate)
    requested_exchange_codes = _parse_exchange_filters(exchange_codes)
    user_meta = client.user_metadata()
    daily_limit, used_calls, usable_requests = _eodhd_request_budget(
        user_meta, buffer_calls, EODHD_MARKET_DATA_CALL_COST
    )
    request_budget = usable_requests
    if max_symbols is not None:
        request_budget = min(request_budget, max_symbols)
    if request_budget <= 0:
        print(
            "No EODHD market data request budget available for this run "
            f"(daily_limit={daily_limit}, used_calls={used_calls}, "
            f"buffer_calls={buffer_calls})."
        )
        return 0

    ticker_repo = SupportedTickerRepository(database)
    eligible = ticker_repo.list_eligible_for_market_data(
        provider=provider_norm,
        exchange_codes=sorted(requested_exchange_codes)
        if requested_exchange_codes
        else None,
        max_age_days=max_age_days,
        max_symbols=request_budget,
        respect_backoff=respect_backoff,
    )
    if not eligible:
        scope = (
            ", ".join(sorted(requested_exchange_codes))
            if requested_exchange_codes
            else "all exchanges"
        )
        print(
            f"No eligible supported tickers found for {scope}. "
            "Run refresh-supported-tickers first or relax freshness filters."
        )
        return 0

    service = MarketDataService(db_path=database, config=config)
    state_repo = MarketDataFetchStateRepository(database)
    state_repo.initialize_schema()
    interval = 60.0 / rate_value
    total = len(eligible)
    processed = 0
    attempted = 0
    scope_label = (
        ", ".join(sorted(requested_exchange_codes))
        if requested_exchange_codes
        else "all exchanges"
    )
    print(
        f"Fetching EODHD market data for {total} supported tickers across {scope_label} "
        f"at <= {rate_value:.2f} req/min "
        f"(daily_limit={daily_limit}, used_calls={used_calls}, "
        f"buffer_calls={buffer_calls}, budget_requests={request_budget})"
    )

    try:
        for idx, ticker in enumerate(eligible, 1):
            attempted += 1
            start = time.perf_counter()
            try:
                data = service.refresh_symbol(ticker.symbol)
                state_repo.mark_success("EODHD", ticker.symbol)
                processed += 1
                print(
                    f"[{idx}/{total}] Stored market data for {data.symbol}",
                    flush=True,
                )
            except Exception as exc:  # pragma: no cover - network errors
                LOGGER.error(
                    "Failed to refresh market data for %s: %s", ticker.symbol, exc
                )
                state_repo.mark_failure("EODHD", ticker.symbol, str(exc))

            elapsed = time.perf_counter() - start
            if elapsed < interval:
                time.sleep(interval - elapsed)
    except KeyboardInterrupt:
        return _cancel_cli_command(f"\nCancelled after {attempted} attempted symbols.")

    print(
        f"Stored market data for {processed} of {attempted} attempted symbols in {database}"
    )
    return 0


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
    breakdown = ticker_repo.market_data_progress_by_exchange(
        provider=provider_norm,
        exchange_codes=selected_exchanges,
        max_age_days=effective_max_age_days,
    )
    summary = _summarize_progress_breakdown(breakdown)
    failures = (
        ticker_repo.recent_market_data_failures(
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
    user_agent: Optional[str],
    cik: Optional[str],
) -> int:
    """Unified fundamentals ingestion over symbol, exchange, or full supported scope."""

    db_path = _resolve_database_path(database)
    provider_norm = _normalize_provider(provider)

    if provider_norm == "SEC":
        scope_rows, symbol_filters, resolved_exchange_codes = (
            _resolve_provider_scope_rows(
                str(db_path),
                provider_norm,
                symbols,
                exchange_codes,
                all_supported,
            )
        )
        ticker_repo = SupportedTickerRepository(db_path)
        scope_label = _scope_label(symbol_filters, resolved_exchange_codes)
        if cik and len(scope_rows) != 1:
            raise SystemExit(
                "--cik can only be used when ingesting exactly one SEC symbol."
            )
        rate_value = rate if rate is not None else 9.0
        sec_client = SECCompanyFactsClient(user_agent=user_agent)
        fundamentals_repo = FundamentalsRepository(db_path)
        fundamentals_repo.initialize_schema()
        eligible = ticker_repo.list_eligible_for_fundamentals(
            provider=provider_norm,
            exchange_codes=resolved_exchange_codes,
            max_age_days=max_age_days,
            max_symbols=max_symbols,
            respect_backoff=respect_backoff,
            missing_only=max_age_days is None,
            provider_symbols=symbol_filters,
        )
        if not eligible:
            print(
                f"No eligible supported tickers found for {scope_label}. "
                "Refresh supported tickers first or relax freshness filters."
            )
            return 0
        min_interval = 1.0 / rate_value if rate_value and rate_value > 0 else 0.0
        total = len(eligible)
        processed = 0
        print(
            f"Fetching SEC company facts for {total} supported tickers across {scope_label} "
            f"at <= {rate_value:.2f} req/s"
        )
        try:
            last_fetch = 0.0
            for idx, ticker in enumerate(eligible, 1):
                if min_interval > 0 and last_fetch:
                    elapsed = time.perf_counter() - last_fetch
                    if elapsed < min_interval:
                        time.sleep(min_interval - elapsed)
                try:
                    cik_value = cik
                    if cik_value is None:
                        info = sec_client.resolve_company(ticker.code)
                        cik_value = info.cik
                    payload = sec_client.fetch_company_facts(cik_value)
                except Exception as exc:  # pragma: no cover - network errors
                    LOGGER.error(
                        "Failed to fetch SEC company facts for %s: %s",
                        ticker.symbol,
                        exc,
                    )
                    last_fetch = time.perf_counter()
                    continue
                last_fetch = time.perf_counter()
                fundamentals_repo.upsert(
                    "SEC",
                    ticker.symbol,
                    payload,
                    exchange="US",
                )
                processed += 1
                print(
                    f"[{idx}/{total}] Stored company facts for {ticker.symbol}",
                    flush=True,
                )
        except KeyboardInterrupt:
            return _cancel_cli_command(
                f"\nCancelled after {processed} of {total} symbols."
            )
        print(f"Stored company facts for {processed} symbols in {db_path}")
        return 0

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

    _, symbol_filters, resolved_exchange_codes = _resolve_provider_scope_rows(
        str(db_path),
        provider_norm,
        symbols,
        exchange_codes,
        all_supported,
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
                    pending_updates.append(
                        _build_market_data_update(service, ticker, bulk_data)
                    )
                    stored_for_exchange += 1
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
    if candidate.normalized_raw_fetched_at is None:
        return True
    if candidate.raw_fetched_at > candidate.normalized_raw_fetched_at:
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


class _CachedRegionFactsRepository(RegionFactsRepository):
    """Serve one symbol's facts from memory while preserving the repo interface."""

    def __init__(
        self,
        repo: FinancialFactsRepository,
        symbol: str,
        records: Sequence[FactRecord],
    ) -> None:
        super().__init__(repo)
        self._symbol = symbol.strip().upper()
        self._ticker_currency_loaded = False
        self._ticker_currency: Optional[str] = None
        self._latest_by_concept: Dict[str, FactRecord] = {}
        self._facts_by_concept: Dict[str, Tuple[FactRecord, ...]] = {}
        facts_by_concept: Dict[str, List[FactRecord]] = {}
        facts_by_concept_period: Dict[Tuple[str, str], List[FactRecord]] = {}

        for record in records:
            facts_by_concept.setdefault(record.concept, []).append(record)
            if record.fiscal_period:
                facts_by_concept_period.setdefault(
                    (record.concept, record.fiscal_period), []
                ).append(record)
            self._latest_by_concept.setdefault(record.concept, record)

        self._facts_by_concept = {
            concept: tuple(concept_records)
            for concept, concept_records in facts_by_concept.items()
        }
        self._facts_by_concept_period = {
            key: tuple(concept_records)
            for key, concept_records in facts_by_concept_period.items()
        }

    def latest_fact(
        self,
        symbol: str,
        concept: str,
    ) -> Optional[FactRecord]:
        if symbol.strip().upper() != self._symbol:
            return super().latest_fact(symbol, concept)
        return self._latest_by_concept.get(concept)

    def facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[FactRecord]:
        if symbol.strip().upper() != self._symbol:
            return super().facts_for_concept(
                symbol,
                concept,
                fiscal_period=fiscal_period,
                limit=limit,
            )

        if fiscal_period is None:
            records = self._facts_by_concept.get(concept, ())
            # Surface a metric that asked for a concept the preload didn't
            # fetch — typically the metric under-declared its
            # ``required_concepts`` and would silently degrade to N+1 reads
            # against the live DB once we re-enable the concept filter on
            # the preload. DEBUG so production stays quiet, but tests can
            # opt in by setting the logger level.
            if not records and concept not in self._facts_by_concept:
                LOGGER.debug(
                    "preloaded fact cache miss: symbol=%s concept=%s — "
                    "metric may have under-declared required_concepts",
                    self._symbol,
                    concept,
                )
        else:
            records = self._facts_by_concept_period.get((concept, fiscal_period), ())

        selected = list(records)
        if limit is not None:
            selected = selected[:limit]
        return selected

    def ticker_currency(self, symbol: str) -> Optional[str]:
        symbol_upper = symbol.strip().upper()
        if symbol_upper != self._symbol:
            resolver = getattr(self._repo, "ticker_currency", None)
            if callable(resolver):
                return resolver(symbol)
            return None
        if not self._ticker_currency_loaded:
            resolver = getattr(self._repo, "ticker_currency", None)
            self._ticker_currency = resolver(symbol) if callable(resolver) else None
            self._ticker_currency_loaded = True
        return self._ticker_currency


class _SchemaReadySecurityRepository(SecurityRepository):
    """Read-only security repository for metric workers on an initialized DB."""

    def initialize_schema(self) -> None:
        return


class _SchemaReadyFXRatesRepository(FXRatesRepository):
    """FX rates repository that skips schema init in normalization workers."""

    def initialize_schema(self) -> None:
        return


class _SchemaReadySupportedTickerRepository(SupportedTickerRepository):
    """Supported-ticker repository that skips schema init in normalization workers."""

    def initialize_schema(self) -> None:
        return


class _SchemaReadyFinancialFactsRepository(FinancialFactsRepository):
    """Read-only facts repository that skips schema work in metric workers."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._security_repo_cache = _SchemaReadySecurityRepository(self.db_path)

    def initialize_schema(self) -> None:
        return


class _SchemaReadyMarketDataRepository(MarketDataRepository):
    """Read-only market-data repository that skips schema work in workers."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._security_repo_cache = _SchemaReadySecurityRepository(self.db_path)

    def initialize_schema(self) -> None:
        return


class _SchemaReadyMetricsRepository(MetricsRepository):
    """Metrics writer that assumes the schema is already initialized."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._security_repo_cache = _SchemaReadySecurityRepository(self.db_path)

    def initialize_schema(self) -> None:
        return


class _SchemaReadyMetricComputeStatusRepository(MetricComputeStatusRepository):
    """Metric-status repository that assumes the schema is already initialized."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._security_repo_cache = _SchemaReadySecurityRepository(self.db_path)

    def initialize_schema(self) -> None:
        return


class _SchemaReadyFinancialFactsRefreshStateRepository(
    FinancialFactsRefreshStateRepository
):
    """Facts-refresh-state repository that assumes the schema is ready."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        super().__init__(db_path)
        self._security_repo_cache = _SchemaReadySecurityRepository(self.db_path)

    def initialize_schema(self) -> None:
        return


class _PreloadedMetricsRepository(_SchemaReadyMetricsRepository):
    """Serve stored metric values from memory for a fixed symbol scope."""

    def __init__(
        self,
        db_path: Union[str, Path],
        metric_rows_by_symbol: Mapping[str, Mapping[str, MetricRecord]],
    ) -> None:
        super().__init__(db_path)
        self._metric_rows_by_symbol = {
            symbol.strip().upper(): dict(metric_rows)
            for symbol, metric_rows in metric_rows_by_symbol.items()
        }

    def fetch(self, symbol: str, metric_id: str) -> Optional[MetricRecord]:
        return self._metric_rows_by_symbol.get(symbol.strip().upper(), {}).get(
            metric_id
        )


def _metric_status_current_facts_refresh(
    record: Optional[FinancialFactsRefreshStateRecord],
) -> Optional[str]:
    return record.refreshed_at if record is not None else None


def _metric_status_current_market_watermark(
    record: Optional[MarketSnapshotRecord],
) -> Tuple[Optional[str], Optional[str]]:
    if record is None:
        return None, None
    return record.as_of, record.updated_at


def _build_metric_availability_state(
    metric_id: str,
    record: Optional[MetricRecord],
    status_record: Optional[MetricComputeStatusRecord],
    facts_refresh_record: Optional[FinancialFactsRefreshStateRecord],
    market_snapshot_record: Optional[MarketSnapshotRecord],
) -> _MetricAvailabilityState:
    if status_record is None:
        return _MetricAvailabilityState(
            metric_id=metric_id,
            record=record,
            status_record=None,
            stale=False,
        )

    metric_cls = REGISTRY.get(metric_id)
    uses_financial_facts = bool(metric_cls) and getattr(
        metric_cls, "uses_financial_facts", True
    )
    uses_market_data = bool(metric_cls) and getattr(
        metric_cls, "uses_market_data", False
    )
    stale = False
    current_facts_refreshed_at = _metric_status_current_facts_refresh(
        facts_refresh_record
    )
    current_market_data_as_of, current_market_data_updated_at = (
        _metric_status_current_market_watermark(market_snapshot_record)
    )

    if uses_financial_facts and (
        status_record.facts_refreshed_at != current_facts_refreshed_at
    ):
        stale = True
    if uses_market_data and (
        status_record.market_data_as_of != current_market_data_as_of
        or status_record.market_data_updated_at != current_market_data_updated_at
    ):
        stale = True
    if status_record.status == "success":
        if record is None:
            stale = True
        elif (
            status_record.value_as_of is not None
            and record.as_of != status_record.value_as_of
        ):
            stale = True

    if stale:
        return _MetricAvailabilityState(
            metric_id=metric_id,
            record=None,
            status_record=status_record,
            stale=True,
        )
    if status_record.status == "failure":
        return _MetricAvailabilityState(
            metric_id=metric_id,
            record=None,
            status_record=status_record,
            stale=False,
        )
    return _MetricAvailabilityState(
        metric_id=metric_id,
        record=record,
        status_record=status_record,
        stale=False,
    )


class _StatusAwareMetricsRepository(_SchemaReadyMetricsRepository):
    """Expose metric reads with persisted latest-attempt status shadowing."""

    def __init__(
        self,
        db_path: Union[str, Path],
        *,
        raw_metrics_repo: Optional[MetricsRepository] = None,
        status_repo: Optional[MetricComputeStatusRepository] = None,
        facts_refresh_repo: Optional[FinancialFactsRefreshStateRepository] = None,
        market_repo: Optional[MarketDataRepository] = None,
    ) -> None:
        super().__init__(db_path)
        self._raw_metrics_repo = raw_metrics_repo or _SchemaReadyMetricsRepository(
            db_path
        )
        self._status_repo = status_repo or _SchemaReadyMetricComputeStatusRepository(
            db_path
        )
        self._facts_refresh_repo = (
            facts_refresh_repo
            or _SchemaReadyFinancialFactsRefreshStateRepository(db_path)
        )
        self._market_repo = market_repo or _SchemaReadyMarketDataRepository(db_path)

    def state(self, symbol: str, metric_id: str) -> _MetricAvailabilityState:
        symbol_upper = symbol.strip().upper()
        record = self._raw_metrics_repo.fetch(symbol_upper, metric_id)
        status_record = self._status_repo.fetch(symbol_upper, metric_id)
        facts_refresh_record = None
        market_snapshot_record = None
        metric_cls = REGISTRY.get(metric_id)
        if metric_cls is not None and getattr(metric_cls, "uses_financial_facts", True):
            facts_refresh_record = self._facts_refresh_repo.fetch(symbol_upper)
        if metric_cls is not None and getattr(metric_cls, "uses_market_data", False):
            market_snapshot_record = self._market_repo.latest_snapshot_record(
                symbol_upper
            )
        return _build_metric_availability_state(
            metric_id,
            record,
            status_record,
            facts_refresh_record,
            market_snapshot_record,
        )

    def states_many(
        self,
        symbols: Sequence[str],
        metric_ids: Sequence[str],
        *,
        chunk_size: int = 500,
    ) -> Dict[str, Dict[str, _MetricAvailabilityState]]:
        normalized_symbols = [symbol.strip().upper() for symbol in symbols if symbol]
        requested_metric_ids = [
            metric_id.strip() for metric_id in metric_ids if str(metric_id).strip()
        ]
        if not normalized_symbols or not requested_metric_ids:
            return {}

        raw_rows = self._raw_metrics_repo.fetch_many_for_symbols(
            normalized_symbols,
            requested_metric_ids,
            chunk_size=chunk_size,
        )
        status_rows = self._status_repo.fetch_many_for_symbols(
            normalized_symbols,
            requested_metric_ids,
            chunk_size=chunk_size,
        )
        facts_refresh_symbols = sorted(
            {
                symbol
                for symbol, per_symbol_statuses in status_rows.items()
                for metric_id in per_symbol_statuses.keys()
                if getattr(REGISTRY.get(metric_id), "uses_financial_facts", True)
            }
        )
        facts_refresh_rows = (
            self._facts_refresh_repo.fetch_many_for_symbols(
                facts_refresh_symbols,
                chunk_size=chunk_size,
            )
            if facts_refresh_symbols
            else {}
        )
        market_snapshot_symbols = sorted(
            {
                symbol
                for symbol, per_symbol_statuses in status_rows.items()
                for metric_id in per_symbol_statuses.keys()
                if getattr(REGISTRY.get(metric_id), "uses_market_data", False)
            }
        )
        market_snapshot_rows = (
            self._market_repo.latest_snapshots_many(
                market_snapshot_symbols,
                chunk_size=chunk_size,
            )
            if market_snapshot_symbols
            else {}
        )

        states: Dict[str, Dict[str, _MetricAvailabilityState]] = {}
        for symbol_upper in normalized_symbols:
            per_symbol_states: Dict[str, _MetricAvailabilityState] = {}
            symbol_metric_rows = raw_rows.get(symbol_upper, {})
            symbol_status_rows = status_rows.get(symbol_upper, {})
            facts_refresh_record = facts_refresh_rows.get(symbol_upper)
            market_snapshot_record = market_snapshot_rows.get(symbol_upper)
            for metric_id in requested_metric_ids:
                per_symbol_states[metric_id] = _build_metric_availability_state(
                    metric_id,
                    symbol_metric_rows.get(metric_id),
                    symbol_status_rows.get(metric_id),
                    facts_refresh_record,
                    market_snapshot_record,
                )
            states[symbol_upper] = per_symbol_states
        return states

    def fetch(self, symbol: str, metric_id: str) -> Optional[MetricRecord]:
        return self.state(symbol, metric_id).record

    def fetch_many_for_symbols(
        self,
        symbols: Sequence[str],
        metric_ids: Sequence[str],
        chunk_size: int = 500,
    ) -> Dict[str, Dict[str, MetricRecord]]:
        states = self.states_many(symbols, metric_ids, chunk_size=chunk_size)
        rows_by_symbol: Dict[str, Dict[str, MetricRecord]] = {}
        for symbol, per_symbol_states in states.items():
            for metric_id, state in per_symbol_states.items():
                if state.record is None:
                    continue
                rows_by_symbol.setdefault(symbol, {})[metric_id] = state.record
        return rows_by_symbol


class _CachedMarketDataRepository:
    """Serve one symbol's latest market snapshot from memory."""

    def __init__(
        self,
        repo: MarketDataRepository,
        symbol: str,
        *,
        snapshot: Optional[PriceData] = None,
        snapshot_loaded: bool = False,
    ) -> None:
        self._repo = repo
        self._symbol = symbol.strip().upper()
        self._snapshot_loaded = snapshot_loaded
        self._snapshot: Optional[PriceData] = snapshot
        self._ticker_currency_loaded = False
        self._ticker_currency: Optional[str] = None

    def _load_snapshot(self) -> None:
        if self._snapshot_loaded:
            return
        self._snapshot = self._repo.latest_snapshot(self._symbol)
        self._snapshot_loaded = True

    def latest_snapshot(self, symbol: str):
        if symbol.strip().upper() != self._symbol:
            return self._repo.latest_snapshot(symbol)
        self._load_snapshot()
        return self._snapshot

    def latest_price(self, symbol: str) -> Optional[Tuple[str, float]]:
        snapshot = self.latest_snapshot(symbol)
        if snapshot is None:
            return None
        return snapshot.as_of, snapshot.price

    def ticker_currency(self, symbol: str) -> Optional[str]:
        symbol_upper = symbol.strip().upper()
        if symbol_upper != self._symbol:
            resolver = getattr(self._repo, "ticker_currency", None)
            if callable(resolver):
                return resolver(symbol)
            return None
        if not self._ticker_currency_loaded:
            resolver = getattr(self._repo, "ticker_currency", None)
            self._ticker_currency = resolver(symbol) if callable(resolver) else None
            self._ticker_currency_loaded = True
        return self._ticker_currency

    def __getattr__(self, name: str):
        return getattr(self._repo, name)


def _price_data_from_snapshot_record(record: MarketSnapshotRecord) -> PriceData:
    """Convert a stored latest-snapshot row into the PriceData interface."""

    return PriceData(
        symbol=record.symbol,
        price=record.price,
        as_of=record.as_of,
        currency=record.currency,
        volume=record.volume,
        market_cap=record.market_cap,
    )


def _make_symbol_progress_reporter(
    total_symbols: int,
    interval_seconds: float,
    printer: Optional[Callable[[int, int], None]] = None,
    start_immediately: bool = False,
) -> Callable[[int, bool], None]:
    """Return a throttled symbol-progress reporter."""

    progress_printer = printer or _print_symbol_progress
    last_progress_at = time.monotonic()
    last_reported_completed = -1
    if start_immediately and total_symbols > 0:
        progress_printer(0, total_symbols)
        last_reported_completed = 0

    def maybe_report_progress(completed_symbols: int, force: bool = False) -> None:
        nonlocal last_progress_at, last_reported_completed
        if total_symbols <= 0 or completed_symbols == last_reported_completed:
            return
        now = time.monotonic()
        elapsed = now - last_progress_at
        if not force and elapsed < interval_seconds:
            return
        progress_printer(completed_symbols, total_symbols)
        last_progress_at = now
        last_reported_completed = completed_symbols

    return maybe_report_progress


def _validated_metric_ids(metric_ids: Optional[Sequence[str]]) -> List[str]:
    ids_to_compute = list(metric_ids) if metric_ids else list(REGISTRY.keys())
    if not ids_to_compute:
        raise SystemExit("No metrics specified.")
    unknown = [metric_id for metric_id in ids_to_compute if metric_id not in REGISTRY]
    if unknown:
        if len(unknown) == 1:
            raise SystemExit(f"Unknown metric id: {unknown[0]}")
        raise SystemExit(f"Unknown metric ids: {', '.join(unknown)}")
    return ids_to_compute


def _metrics_use_market_data(metric_ids: Sequence[str]) -> bool:
    return any(
        getattr(REGISTRY[metric_id], "uses_market_data", False)
        for metric_id in metric_ids
    )


def _metrics_use_financial_facts(metric_ids: Sequence[str]) -> bool:
    return any(
        getattr(REGISTRY[metric_id], "uses_financial_facts", True)
        for metric_id in metric_ids
    )


def _prefetch_metric_facts_for_symbols(
    fact_repo: FinancialFactsRepository,
    symbols: Sequence[str],
    metric_ids: Sequence[str],
    *,
    chunk_size: int,
    security_ids_by_symbol: Optional[Mapping[str, int]] = None,
    connection: Optional[sqlite3.Connection] = None,
) -> Dict[str, List[FactRecord]]:
    """Bulk-load only the facts required by the requested metrics.

    Unknown metric ids are ignored so investigative paths can still classify
    them without aborting the whole batch. When every known metric is
    market-data-only, the preload is skipped entirely.
    """

    known_metric_ids = tuple(
        metric_id for metric_id in metric_ids if metric_id in REGISTRY
    )
    if not known_metric_ids or not _metrics_use_financial_facts(known_metric_ids):
        return {}
    required_concepts = _required_concepts_for_metric_ids(known_metric_ids)
    preload_kwargs = {"concepts": required_concepts} if required_concepts else {}
    return fact_repo.facts_for_symbols_many(
        symbols,
        chunk_size=chunk_size,
        security_ids_by_symbol=security_ids_by_symbol,
        connection=connection,
        **preload_kwargs,
    )


@lru_cache(maxsize=None)
def _required_concepts_for_metric_ids(metric_ids: Tuple[str, ...]) -> Tuple[str, ...]:
    """Return the deduplicated union of ``required_concepts`` across metrics.

    Used by ``_compute_metric_batch_results`` to restrict the fact preload to
    only the concepts the requested metric set actually consumes. The result
    is memoised because every batch in a run uses the same metric_ids tuple
    and the audit cost (resolving each metric class's declaration) is fixed.

    Returns an empty tuple if any metric in the set both uses financial facts
    AND declares an empty ``required_concepts`` — that signals a "wildcard"
    consumer, so the caller must fall back to the unfiltered preload to avoid
    silently dropping rows the metric depends on. Today the only metric with
    empty ``required_concepts`` (``MarketCapitalizationMetric``) also sets
    ``uses_financial_facts = False``, so it never triggers the fallback.
    """

    seen: set[str] = set()
    ordered: List[str] = []
    for metric_id in metric_ids:
        metric_cls = REGISTRY.get(metric_id)
        if metric_cls is None:
            continue
        if not getattr(metric_cls, "uses_financial_facts", True):
            continue
        declared = tuple(getattr(metric_cls, "required_concepts", ()) or ())
        if not declared:
            # Wildcard fact consumer — disable the filter for the whole batch.
            return ()
        for concept in declared:
            if concept and concept not in seen:
                seen.add(concept)
                ordered.append(concept)
    return tuple(ordered)


def _metric_worker_count(total_symbols: int) -> int:
    """Return an automatic worker count for bulk metric computation."""

    if total_symbols <= 0:
        return 1
    cpu_bound = max(os.cpu_count() or 1, 1)
    return max(1, min(total_symbols, min(cpu_bound, METRICS_MAX_WORKERS)))


def _initialize_metric_read_schema(
    db_path: Path,
    include_market_data: bool,
) -> None:
    """Ensure worker-read tables exist before process workers start reading."""

    FinancialFactsRepository(db_path).initialize_schema()
    FinancialFactsRefreshStateRepository(db_path).initialize_schema()
    MetricComputeStatusRepository(db_path).initialize_schema()
    if include_market_data:
        MarketDataRepository(db_path).initialize_schema()


def _ensure_metrics_wal_mode(metrics_repo: MetricsRepository) -> str:
    """Best-effort switch to WAL so metric workers can read during parent writes."""

    try:
        return metrics_repo.enable_wal_mode()
    except sqlite3.OperationalError as exc:
        LOGGER.warning(
            "Could not enable WAL mode for metric computation on %s: %s",
            metrics_repo.db_path,
            exc,
        )
        try:
            return metrics_repo.current_journal_mode()
        except sqlite3.OperationalError:
            return "unknown"


def _metric_status_rows_from_attempts(
    attempts: Sequence[_MetricAttemptResult],
) -> List[MetricComputeStatusRecord]:
    return [
        MetricComputeStatusRecord(
            symbol=attempt.symbol,
            metric_id=attempt.metric_id,
            status=cast(Literal["success", "failure"], attempt.status),
            attempted_at=attempt.attempted_at,
            reason_code=attempt.reason_code,
            reason_detail=attempt.reason_detail,
            value_as_of=attempt.value_as_of,
            facts_refreshed_at=attempt.facts_refreshed_at,
            market_data_as_of=attempt.market_data_as_of,
            market_data_updated_at=attempt.market_data_updated_at,
        )
        for attempt in attempts
        if attempt.persist_status
    ]


def _flush_metric_write_batch(
    metrics_repo: MetricsRepository,
    status_repo: MetricComputeStatusRepository,
    pending_rows: List[StoredMetricRow],
    pending_attempts: List[_MetricAttemptResult],
    profile_state: Optional["_MetricComputationProfile"] = None,
    write_connection: Optional[sqlite3.Connection] = None,
) -> None:
    """Persist one buffered metric batch."""

    if not pending_rows and not pending_attempts:
        return
    status_rows = _metric_status_rows_from_attempts(pending_attempts)
    row_count = len(pending_rows)

    def _persist_with_external_connection() -> None:
        assert write_connection is not None
        try:
            if pending_rows:
                metrics_repo.upsert_many(
                    pending_rows,
                    connection=write_connection,
                    commit=False,
                )
            if status_rows:
                status_repo.upsert_many(
                    status_rows,
                    connection=write_connection,
                    commit=False,
                )
            write_connection.commit()
        except Exception:
            write_connection.rollback()
            raise

    if profile_state is not None and profile_state.enabled:
        flush_start = time.perf_counter()
        if write_connection is not None:
            _persist_with_external_connection()
        else:
            if pending_rows:
                metrics_repo.upsert_many(pending_rows)
            if status_rows:
                status_repo.upsert_many(status_rows)
        profile_state.write_seconds += time.perf_counter() - flush_start
        profile_state.write_flush_count += 1
        profile_state.write_row_count += row_count
    else:
        if write_connection is not None:
            _persist_with_external_connection()
        else:
            if pending_rows:
                metrics_repo.upsert_many(pending_rows)
            if status_rows:
                status_repo.upsert_many(status_rows)
    pending_rows.clear()
    pending_attempts.clear()


def _persist_metric_attempts(
    metrics_repo: MetricsRepository,
    status_repo: MetricComputeStatusRepository,
    attempts: Sequence[_MetricAttemptResult],
) -> None:
    metric_rows = [
        cast(StoredMetricRow, attempt.stored_row)
        for attempt in attempts
        if attempt.stored_row is not None
    ]
    if metric_rows:
        metrics_repo.upsert_many(metric_rows)
    status_rows = _metric_status_rows_from_attempts(attempts)
    if status_rows:
        status_repo.upsert_many(status_rows)


def _print_symbol_progress(completed_symbols: int, total_symbols: int) -> None:
    """Print symbol-level percent progress for long-running CLI commands."""

    if total_symbols <= 0:
        percent = 100.0
    else:
        percent = (completed_symbols / total_symbols) * 100.0
    print(
        f"Progress: {completed_symbols}/{total_symbols} symbols complete ({percent:.1f}%)",
        flush=True,
    )


def _print_metric_progress_bar(completed_symbols: int, total_symbols: int) -> None:
    """Print a compact ASCII bar for metric-computation progress."""

    if total_symbols <= 0:
        percent = 100.0
    else:
        percent = (completed_symbols / total_symbols) * 100.0
    bar_width = 20
    filled_width = min(bar_width, max(0, round((percent / 100.0) * bar_width)))
    bar = "#" * filled_width + "-" * (bar_width - filled_width)
    print(
        f"Progress: [{bar}] {completed_symbols}/{total_symbols} symbols complete ({percent:.1f}%)",
        flush=True,
    )


def _print_screen_progress_bar(completed_symbols: int, total_symbols: int) -> None:
    """Print a compact ASCII bar for screen-evaluation progress."""

    if total_symbols <= 0:
        percent = 100.0
    else:
        percent = (completed_symbols / total_symbols) * 100.0
    bar_width = 20
    filled_width = min(bar_width, max(0, round((percent / 100.0) * bar_width)))
    bar = "#" * filled_width + "-" * (bar_width - filled_width)
    print(
        f"Progress: [{bar}] {completed_symbols}/{total_symbols} symbols screened ({percent:.1f}%)",
        flush=True,
    )


def _print_recompute_progress_bar(completed_symbols: int, total_symbols: int) -> None:
    """Print a compact ASCII bar for missing-metric root-cause analysis progress."""

    if total_symbols <= 0:
        percent = 100.0
    else:
        percent = (completed_symbols / total_symbols) * 100.0
    bar_width = 20
    filled_width = min(bar_width, max(0, round((percent / 100.0) * bar_width)))
    bar = "#" * filled_width + "-" * (bar_width - filled_width)
    print(
        "Progress: "
        f"[{bar}] {completed_symbols}/{total_symbols} missing symbols analyzed "
        f"({percent:.1f}%)",
        flush=True,
    )


def _print_fx_progress_bar(
    completed_batches: int,
    total_batches: int,
    *,
    item_label: Optional[str] = None,
) -> None:
    """Print a compact ASCII bar for FX refresh batching."""

    if total_batches <= 0:
        percent = 100.0
    else:
        percent = (completed_batches / total_batches) * 100.0
    bar_width = 20
    filled_width = min(bar_width, max(0, round((percent / 100.0) * bar_width)))
    bar = "#" * filled_width + "-" * (bar_width - filled_width)
    item_suffix = f" pair={item_label}" if item_label else ""
    print(
        f"Progress: [{bar}] {completed_batches}/{total_batches} FX batches complete ({percent:.1f}%){item_suffix}",
        flush=True,
    )


def _split_fx_refresh_ranges(
    start_date: date,
    end_date: date,
    max_days_per_request: int = FX_REFRESH_MAX_DAYS_PER_REQUEST,
) -> List[Tuple[date, date]]:
    """Split one FX refresh date range into bounded inclusive windows."""

    if max_days_per_request <= 0:
        raise ValueError("max_days_per_request must be positive")
    ranges: List[Tuple[date, date]] = []
    current_start = start_date
    window = timedelta(days=max_days_per_request - 1)
    while current_start <= end_date:
        current_end = min(current_start + window, end_date)
        ranges.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    return ranges


def _metric_attempt_success(
    metric_id: str,
    metric: Metric,
    result: MetricResult,
    *,
    symbol: str,
    attempted_at: str,
    facts_refreshed_at: Optional[str],
    market_snapshot_record: Optional[MarketSnapshotRecord],
) -> _MetricAttemptResult:
    metadata = metadata_for_metric(metric_id, metric)
    unit_kind = metadata.unit_kind if result.unit_kind == "other" else result.unit_kind
    unit_label = result.unit_label or metadata.unit_label
    currency = metric_currency_or_none(unit_kind, result.currency)
    stored_row: StoredMetricRow = (
        result.symbol,
        result.metric_id,
        result.value,
        result.as_of,
        unit_kind,
        currency,
        unit_label,
    )
    return _MetricAttemptResult(
        symbol=symbol,
        metric_id=metric_id,
        status="success",
        attempted_at=attempted_at,
        stored_row=stored_row,
        value_as_of=result.as_of,
        facts_refreshed_at=facts_refreshed_at,
        market_data_as_of=(
            market_snapshot_record.as_of if market_snapshot_record is not None else None
        ),
        market_data_updated_at=(
            market_snapshot_record.updated_at
            if market_snapshot_record is not None
            else None
        ),
    )


def _metric_attempt_failure(
    *,
    symbol: str,
    metric_id: str,
    attempted_at: str,
    reason_code: str,
    reason_detail: Optional[str],
    facts_refreshed_at: Optional[str],
    market_snapshot_record: Optional[MarketSnapshotRecord],
    persist_status: bool = True,
) -> _MetricAttemptResult:
    return _MetricAttemptResult(
        symbol=symbol,
        metric_id=metric_id,
        status="failure",
        attempted_at=attempted_at,
        reason_code=reason_code,
        reason_detail=reason_detail,
        facts_refreshed_at=facts_refreshed_at,
        market_data_as_of=(
            market_snapshot_record.as_of if market_snapshot_record is not None else None
        ),
        market_data_updated_at=(
            market_snapshot_record.updated_at
            if market_snapshot_record is not None
            else None
        ),
        persist_status=persist_status,
    )


def _compute_metrics_for_symbol(
    symbol: str,
    metric_ids: Sequence[str],
    fact_repo: FinancialFactsRepository,
    market_repo: Optional[MarketDataRepository] = None,
    *,
    preloaded_facts: Optional[Sequence[FactRecord]] = None,
    preloaded_market_snapshot: object = _PRELOADED_MARKET_SNAPSHOT_MISSING,
    preloaded_market_snapshot_record: Optional[MarketSnapshotRecord] = None,
    facts_refreshed_at: Optional[str] = None,
    warning_collector: Optional["_MetricWarningCollector"] = None,
) -> _ComputedMetricsResult:
    symbol_upper = symbol.strip().upper()
    records = (
        list(preloaded_facts)
        if preloaded_facts is not None
        else fact_repo.facts_for_symbol(symbol_upper)
    )
    cached_fact_repo = _CachedRegionFactsRepository(
        fact_repo,
        symbol_upper,
        records,
    )
    snapshot_loaded = (
        preloaded_market_snapshot is not _PRELOADED_MARKET_SNAPSHOT_MISSING
    )
    if (
        preloaded_market_snapshot is _PRELOADED_MARKET_SNAPSHOT_MISSING
        and preloaded_market_snapshot_record is not None
    ):
        preloaded_market_snapshot = _price_data_from_snapshot_record(
            preloaded_market_snapshot_record
        )
        snapshot_loaded = True
    snapshot = (
        None
        if preloaded_market_snapshot is _PRELOADED_MARKET_SNAPSHOT_MISSING
        else cast(Optional[PriceData], preloaded_market_snapshot)
    )
    cached_market_repo = (
        _CachedMarketDataRepository(
            market_repo,
            symbol_upper,
            snapshot=snapshot,
            snapshot_loaded=snapshot_loaded,
        )
        if market_repo is not None
        else None
    )

    rows: List[StoredMetricRow] = []
    failures: List[_MetricComputationFailure] = []
    attempts: List[_MetricAttemptResult] = []
    computed = 0
    for metric_id in metric_ids:
        if warning_collector is not None:
            warning_collector.clear()
        metric_cls = REGISTRY.get(metric_id)
        attempted_at = datetime.now(timezone.utc).isoformat()
        if metric_cls is None:
            attempts.append(
                _metric_attempt_failure(
                    symbol=symbol_upper,
                    metric_id=metric_id,
                    attempted_at=attempted_at,
                    reason_code="unknown_metric_id",
                    reason_detail="Metric id not found in registry",
                    facts_refreshed_at=facts_refreshed_at,
                    market_snapshot_record=preloaded_market_snapshot_record,
                    persist_status=False,
                )
            )
            continue
        metric = metric_cls()
        try:
            if getattr(metric, "uses_market_data", False):
                if cached_market_repo is None:  # pragma: no cover - defensive
                    raise RuntimeError(
                        f"Metric {metric_id} requires market data but no market repo was provided."
                    )
                result = metric.compute(
                    symbol_upper, cached_fact_repo, cached_market_repo
                )
            else:
                result = metric.compute(symbol_upper, cached_fact_repo)
        except MetricCurrencyInvariantError as exc:
            failures.append(
                _MetricComputationFailure(
                    symbol=symbol_upper,
                    metric_id=metric_id,
                    reason=exc.summary_reason,
                    message=str(exc),
                )
            )
            attempts.append(
                _metric_attempt_failure(
                    symbol=symbol_upper,
                    metric_id=metric_id,
                    attempted_at=attempted_at,
                    reason_code=exc.summary_reason,
                    reason_detail=str(exc),
                    facts_refreshed_at=facts_refreshed_at,
                    market_snapshot_record=preloaded_market_snapshot_record,
                )
            )
            continue
        except Exception as exc:  # pragma: no cover - metric errors
            LOGGER.error("Metric %s failed for %s: %s", metric_id, symbol_upper, exc)
            attempts.append(
                _metric_attempt_failure(
                    symbol=symbol_upper,
                    metric_id=metric_id,
                    attempted_at=attempted_at,
                    reason_code=_metric_failure_reason_from_exception(exc),
                    reason_detail=_metric_failure_message(exc),
                    facts_refreshed_at=facts_refreshed_at,
                    market_snapshot_record=preloaded_market_snapshot_record,
                )
            )
            continue
        invariant_error = consume_metric_currency_invariant_error(metric)
        if invariant_error is not None:
            failures.append(
                _MetricComputationFailure(
                    symbol=symbol_upper,
                    metric_id=metric_id,
                    reason=invariant_error.summary_reason,
                    message=str(invariant_error),
                )
            )
            attempts.append(
                _metric_attempt_failure(
                    symbol=symbol_upper,
                    metric_id=metric_id,
                    attempted_at=attempted_at,
                    reason_code=invariant_error.summary_reason,
                    reason_detail=str(invariant_error),
                    facts_refreshed_at=facts_refreshed_at,
                    market_snapshot_record=preloaded_market_snapshot_record,
                )
            )
            continue
        if result is None:
            LOGGER.warning(
                "Metric %s could not be computed for %s", metric_id, symbol_upper
            )
            attempts.append(
                _metric_attempt_failure(
                    symbol=symbol_upper,
                    metric_id=metric_id,
                    attempted_at=attempted_at,
                    reason_code=_format_failure_reason(
                        warning_collector.records if warning_collector else (),
                        symbol_upper,
                    ),
                    reason_detail=None,
                    facts_refreshed_at=facts_refreshed_at,
                    market_snapshot_record=preloaded_market_snapshot_record,
                )
            )
            continue
        attempt = _metric_attempt_success(
            metric_id,
            metric,
            result,
            symbol=symbol_upper,
            attempted_at=attempted_at,
            facts_refreshed_at=facts_refreshed_at,
            market_snapshot_record=preloaded_market_snapshot_record,
        )
        rows.append(cast(StoredMetricRow, attempt.stored_row))
        attempts.append(attempt)
        computed += 1

    return _ComputedMetricsResult(
        symbol=symbol_upper,
        rows=tuple(rows),
        computed_count=computed,
        failures=tuple(failures),
        attempts=tuple(attempts),
    )


def _compute_metric_batch_results(
    symbols: Sequence[str],
    metric_ids: Sequence[str],
    fact_repo: FinancialFactsRepository,
    market_repo: Optional[MarketDataRepository] = None,
    *,
    suppress_metric_warnings: bool = True,
    profile_state: Optional["_MetricComputationProfile"] = None,
    preloaded_snapshots_by_symbol: Optional[Mapping[str, MarketSnapshotRecord]] = None,
    preloaded_facts_refresh_rows: Optional[
        Mapping[str, FinancialFactsRefreshStateRecord]
    ] = None,
) -> Tuple[_ComputedMetricsResult, ...]:
    selected_symbols = [symbol.strip().upper() for symbol in symbols]
    if not selected_symbols:
        return ()

    known_metric_ids = tuple(
        metric_id for metric_id in metric_ids if metric_id in REGISTRY
    )
    uses_financial_facts = bool(known_metric_ids) and _metrics_use_financial_facts(
        known_metric_ids
    )
    uses_market_data = (
        market_repo is not None
        and bool(known_metric_ids)
        and any(
            getattr(REGISTRY[metric_id], "uses_market_data", False)
            for metric_id in known_metric_ids
        )
    )
    profile_enabled = profile_state is not None and profile_state.enabled
    facts_by_symbol: Dict[str, List[FactRecord]] = {}
    facts_refresh_rows = dict(preloaded_facts_refresh_rows or {})
    snapshots_by_symbol: Dict[str, MarketSnapshotRecord] = {}
    if preloaded_snapshots_by_symbol is not None:
        snapshots_by_symbol = {
            symbol: snapshot
            for symbol, snapshot in preloaded_snapshots_by_symbol.items()
            if symbol in selected_symbols
        }
    needs_market_snapshot_load = (
        uses_market_data and preloaded_snapshots_by_symbol is None
    )
    needs_shared_read_connection = uses_financial_facts or needs_market_snapshot_load
    if uses_financial_facts:
        facts_refresh_rows = dict(preloaded_facts_refresh_rows or {})
    if needs_shared_read_connection:
        read_start = time.perf_counter() if profile_enabled else 0.0
        with fact_repo._connect() as read_connection:
            security_ids_by_symbol = fact_repo._security_repo().resolve_ids_many(
                selected_symbols,
                chunk_size=METRICS_COMPUTE_BATCH_SIZE,
                connection=read_connection,
            )
            if uses_financial_facts:
                facts_by_symbol = _prefetch_metric_facts_for_symbols(
                    fact_repo,
                    selected_symbols,
                    known_metric_ids,
                    chunk_size=METRICS_COMPUTE_BATCH_SIZE,
                    security_ids_by_symbol=security_ids_by_symbol,
                    connection=read_connection,
                )
                if not facts_refresh_rows:
                    facts_refresh_rows = (
                        _SchemaReadyFinancialFactsRefreshStateRepository(
                            fact_repo.db_path
                        ).fetch_many_for_symbols(
                            selected_symbols,
                            chunk_size=METRICS_COMPUTE_BATCH_SIZE,
                            security_ids_by_symbol=security_ids_by_symbol,
                            connection=read_connection,
                        )
                    )
            if needs_market_snapshot_load:
                assert market_repo is not None
                snapshots_by_symbol = market_repo.latest_snapshots_many(
                    selected_symbols,
                    chunk_size=METRICS_COMPUTE_BATCH_SIZE,
                    security_ids_by_symbol=security_ids_by_symbol,
                    connection=read_connection,
                )
        if profile_enabled:
            assert profile_state is not None
            profile_state.read_seconds += time.perf_counter() - read_start

    results: List[_ComputedMetricsResult] = []
    warning_collector = _MetricWarningCollector()
    root_logger = logging.getLogger()
    root_logger.addHandler(warning_collector)
    try:
        with suppress_console_metric_warnings(suppress_metric_warnings):
            for symbol in selected_symbols:
                snapshot_record = snapshots_by_symbol.get(symbol)
                results.append(
                    _compute_metrics_for_symbol(
                        symbol,
                        metric_ids,
                        fact_repo,
                        market_repo,
                        preloaded_facts=(
                            facts_by_symbol.get(symbol, ())
                            if uses_financial_facts
                            else ()
                        ),
                        preloaded_market_snapshot_record=snapshot_record,
                        facts_refreshed_at=(
                            facts_refresh_rows[symbol].refreshed_at
                            if symbol in facts_refresh_rows
                            else None
                        ),
                        warning_collector=warning_collector,
                    )
                )
    finally:
        root_logger.removeHandler(warning_collector)
    return tuple(results)


def _compute_metrics_for_symbol_worker(
    database: Union[str, Path],
    symbol: str,
    metric_ids: Sequence[str],
    suppress_metric_warnings: bool = True,
) -> _ComputedMetricsResult:
    """Compute all requested metrics for one symbol using symbol-scoped caches."""

    fact_repo = _SchemaReadyFinancialFactsRepository(database)
    market_repo = (
        _SchemaReadyMarketDataRepository(database)
        if _metrics_use_market_data(metric_ids)
        else None
    )
    results = _compute_metric_batch_results(
        [symbol],
        metric_ids,
        fact_repo,
        market_repo,
        suppress_metric_warnings=suppress_metric_warnings,
    )
    return results[0]


def _compute_metrics_for_symbol_batch_worker(
    database: Union[str, Path],
    symbols: Sequence[str],
    metric_ids: Sequence[str],
    suppress_metric_warnings: bool = True,
) -> Tuple[_ComputedMetricsResult, ...]:
    """Compute requested metrics for a batch of symbols in one worker."""

    fact_repo = _SchemaReadyFinancialFactsRepository(database)
    market_repo = (
        _SchemaReadyMarketDataRepository(database)
        if _metrics_use_market_data(metric_ids)
        else None
    )
    return _compute_metric_batch_results(
        symbols,
        metric_ids,
        fact_repo,
        market_repo,
        suppress_metric_warnings=suppress_metric_warnings,
    )


def _compute_metrics_for_symbol_batch_worker_profiled(
    database: Union[str, Path],
    symbols: Sequence[str],
    metric_ids: Sequence[str],
    suppress_metric_warnings: bool = True,
) -> _ProfiledComputedMetricsBatchResult:
    """Compute one worker batch and return worker-side timing breakdowns."""

    profile_state = _MetricComputationProfile(enabled=True)
    results = _compute_metric_batch_results(
        symbols,
        metric_ids,
        _SchemaReadyFinancialFactsRepository(database),
        (
            _SchemaReadyMarketDataRepository(database)
            if _metrics_use_market_data(metric_ids)
            else None
        ),
        suppress_metric_warnings=suppress_metric_warnings,
        profile_state=profile_state,
    )
    return _ProfiledComputedMetricsBatchResult(
        results=results,
        read_seconds=profile_state.read_seconds,
        compute_seconds=profile_state.compute_seconds,
    )


@dataclass
class _MetricComputationProfile:
    """Wall-clock accumulator for compute-metrics phases."""

    enabled: bool = False
    read_seconds: float = 0.0
    compute_seconds: float = 0.0
    write_seconds: float = 0.0
    total_seconds: float = 0.0
    write_flush_count: int = 0
    write_row_count: int = 0


def _run_metric_computation(
    database: str,
    symbols: Sequence[str],
    metric_ids: Sequence[str],
    cancelled_message: str,
    suppress_metric_warnings: bool = True,
    profile: bool = False,
) -> int:
    db_path = _resolve_database_path(database)
    selected_symbols = [symbol.upper() for symbol in symbols]
    total_symbols = len(selected_symbols)

    profile_state = _MetricComputationProfile(enabled=profile)
    run_start_at = time.perf_counter()

    include_market_data = _metrics_use_market_data(metric_ids)
    base_metrics_repo = MetricsRepository(db_path)
    base_metrics_repo.initialize_schema()
    _initialize_metric_read_schema(db_path, include_market_data)
    journal_mode = _ensure_metrics_wal_mode(base_metrics_repo)
    metrics_repo = _SchemaReadyMetricsRepository(db_path)
    status_repo = _SchemaReadyMetricComputeStatusRepository(db_path)
    # One persistent writer connection drives every flush in this run, so
    # pragma setup happens once and the SQLite page cache stays warm across
    # the ~tens of flushes a large universe produces. Close it on every exit
    # path via the outer try/finally below.
    write_connection: sqlite3.Connection = metrics_repo.open_persistent_connection()

    print(
        f"Computing metrics for {total_symbols} symbols ({len(metric_ids)} metrics each)"
    )

    workers = _metric_worker_count(total_symbols)
    if workers > 1 and journal_mode != "wal":
        print(
            f"SQLite journal mode is {journal_mode or 'unknown'}; "
            "falling back to serial metric computation to avoid lock contention.",
            flush=True,
        )
        workers = 1

    pending_rows: List[StoredMetricRow] = []
    pending_attempts: List[_MetricAttemptResult] = []
    metric_failures: List[_MetricComputationFailure] = []
    buffered_symbols = 0
    last_flush_at = time.monotonic()
    completed_symbols = 0
    last_progress_at = time.monotonic()
    last_reported_completed = -1

    def buffer_result(result: _ComputedMetricsResult) -> None:
        nonlocal buffered_symbols, last_flush_at
        pending_rows.extend(result.rows)
        pending_attempts.extend(result.attempts)
        buffered_symbols += 1
        elapsed = time.monotonic() - last_flush_at
        if (
            buffered_symbols < METRICS_WRITE_BATCH_SIZE
            and elapsed < METRICS_WRITE_BATCH_INTERVAL_SECONDS
        ):
            return
        _flush_metric_write_batch(
            metrics_repo,
            status_repo,
            pending_rows,
            pending_attempts,
            profile_state,
            write_connection=write_connection,
        )
        buffered_symbols = 0
        last_flush_at = time.monotonic()

    def flush_pending(force: bool = False) -> None:
        nonlocal buffered_symbols, last_flush_at
        if not pending_rows and not pending_attempts:
            return
        if not force:
            elapsed = time.monotonic() - last_flush_at
            if (
                buffered_symbols < METRICS_WRITE_BATCH_SIZE
                and elapsed < METRICS_WRITE_BATCH_INTERVAL_SECONDS
            ):
                return
        _flush_metric_write_batch(
            metrics_repo,
            status_repo,
            pending_rows,
            pending_attempts,
            profile_state,
            write_connection=write_connection,
        )
        buffered_symbols = 0
        last_flush_at = time.monotonic()

    def maybe_report_progress(force: bool = False) -> None:
        nonlocal last_progress_at, last_reported_completed
        if total_symbols <= 0:
            if force and last_reported_completed != 0:
                _print_metric_progress_bar(0, 0)
                last_reported_completed = 0
                last_progress_at = time.monotonic()
            return
        if completed_symbols == last_reported_completed:
            return
        elapsed = time.monotonic() - last_progress_at
        if not force and elapsed < METRICS_PROGRESS_INTERVAL_SECONDS:
            return
        _print_metric_progress_bar(completed_symbols, total_symbols)
        last_reported_completed = completed_symbols
        last_progress_at = time.monotonic()

    if workers <= 1:
        fact_repo = _SchemaReadyFinancialFactsRepository(db_path)
        market_repo = (
            _SchemaReadyMarketDataRepository(db_path) if include_market_data else None
        )
        try:
            for symbol_batch in _batch_values(
                selected_symbols,
                METRICS_COMPUTE_BATCH_SIZE,
            ):
                if profile_state.enabled:
                    batch_wall_start = time.perf_counter()
                    read_seconds_before = profile_state.read_seconds
                else:
                    batch_wall_start = 0.0
                    read_seconds_before = 0.0
                for result in _compute_metric_batch_results(
                    symbol_batch,
                    metric_ids,
                    fact_repo,
                    market_repo,
                    suppress_metric_warnings=suppress_metric_warnings,
                    profile_state=profile_state if profile_state.enabled else None,
                ):
                    buffer_result(result)
                    metric_failures.extend(result.failures)
                    completed_symbols += 1
                    maybe_report_progress()
                if profile_state.enabled:
                    # Compute = batch wall time minus the read phase, which
                    # _compute_metric_batch_results already credited to
                    # profile_state.read_seconds.
                    batch_wall = time.perf_counter() - batch_wall_start
                    read_delta = profile_state.read_seconds - read_seconds_before
                    profile_state.compute_seconds += max(0.0, batch_wall - read_delta)
        except KeyboardInterrupt:
            return _cancel_cli_command(
                cancelled_message,
                flushers=[
                    lambda: flush_pending(force=True),
                    lambda: maybe_report_progress(force=True),
                    write_connection.close,
                ],
            )
        flush_pending(force=True)
        maybe_report_progress(force=True)
        _print_metric_invariant_failure_summary(metric_failures)
        print(f"Computed metrics for {total_symbols} symbols in {db_path}")
        if profile_state.enabled:
            profile_state.total_seconds = time.perf_counter() - run_start_at
            _print_metric_computation_profile(profile_state)
        write_connection.close()
        return 0

    executor = _create_process_pool_executor(workers)
    interrupted = False
    try:
        if total_symbols <= METRICS_COMPUTE_BATCH_SIZE:
            if profile_state.enabled:
                single_profile_futures: Dict[
                    Future[_ProfiledComputedMetricsBatchResult], str
                ] = {
                    executor.submit(
                        _compute_metrics_for_symbol_batch_worker_profiled,
                        str(db_path),
                        (symbol,),
                        tuple(metric_ids),
                        suppress_metric_warnings,
                    ): symbol
                    for symbol in selected_symbols
                }
                for profiled_future in as_completed(single_profile_futures):
                    symbol = single_profile_futures[profiled_future]
                    try:
                        profiled_result_batch = profiled_future.result()
                        profile_state.read_seconds += profiled_result_batch.read_seconds
                        profile_state.compute_seconds += (
                            profiled_result_batch.compute_seconds
                        )
                        for result in profiled_result_batch.results:
                            buffer_result(result)
                            metric_failures.extend(result.failures)
                    except Exception as exc:  # pragma: no cover - worker crashes
                        LOGGER.error(
                            "Failed to compute metrics for %s: %s", symbol, exc
                        )
                    completed_symbols += 1
                    maybe_report_progress()
            else:
                single_result_futures: Dict[Future[_ComputedMetricsResult], str] = {
                    executor.submit(
                        _compute_metrics_for_symbol_worker,
                        str(db_path),
                        symbol,
                        tuple(metric_ids),
                        suppress_metric_warnings,
                    ): symbol
                    for symbol in selected_symbols
                }
                for result_future in as_completed(single_result_futures):
                    symbol = single_result_futures[result_future]
                    try:
                        computed_result = result_future.result()
                        buffer_result(computed_result)
                        metric_failures.extend(computed_result.failures)
                    except Exception as exc:  # pragma: no cover - worker crashes
                        LOGGER.error(
                            "Failed to compute metrics for %s: %s", symbol, exc
                        )
                    completed_symbols += 1
                    maybe_report_progress()
        else:
            if profile_state.enabled:
                batch_profile_futures: Dict[
                    Future[_ProfiledComputedMetricsBatchResult],
                    Tuple[str, ...],
                ] = {
                    executor.submit(
                        _compute_metrics_for_symbol_batch_worker_profiled,
                        str(db_path),
                        tuple(symbol_batch),
                        tuple(metric_ids),
                        suppress_metric_warnings,
                    ): tuple(symbol_batch)
                    for symbol_batch in _batch_values(
                        selected_symbols,
                        METRICS_COMPUTE_BATCH_SIZE,
                    )
                }
                for profiled_batch_future in as_completed(batch_profile_futures):
                    batch_symbols = batch_profile_futures[profiled_batch_future]
                    try:
                        profiled_result_batch = profiled_batch_future.result()
                        profile_state.read_seconds += profiled_result_batch.read_seconds
                        profile_state.compute_seconds += (
                            profiled_result_batch.compute_seconds
                        )
                        for result in profiled_result_batch.results:
                            buffer_result(result)
                            metric_failures.extend(result.failures)
                            completed_symbols += 1
                            maybe_report_progress()
                    except Exception as exc:  # pragma: no cover - worker crashes
                        LOGGER.error(
                            "Failed to compute metrics for %d-symbol batch starting at %s: %s",
                            len(batch_symbols),
                            batch_symbols[0],
                            exc,
                        )
                        completed_symbols += len(batch_symbols)
                        maybe_report_progress()
            else:
                batch_result_futures: Dict[
                    Future[Tuple[_ComputedMetricsResult, ...]],
                    Tuple[str, ...],
                ] = {
                    executor.submit(
                        _compute_metrics_for_symbol_batch_worker,
                        str(db_path),
                        tuple(symbol_batch),
                        tuple(metric_ids),
                        suppress_metric_warnings,
                    ): tuple(symbol_batch)
                    for symbol_batch in _batch_values(
                        selected_symbols,
                        METRICS_COMPUTE_BATCH_SIZE,
                    )
                }
                for result_batch_future in as_completed(batch_result_futures):
                    batch_symbols = batch_result_futures[result_batch_future]
                    try:
                        computed_batch_results = result_batch_future.result()
                        for result in computed_batch_results:
                            buffer_result(result)
                            metric_failures.extend(result.failures)
                            completed_symbols += 1
                            maybe_report_progress()
                    except Exception as exc:  # pragma: no cover - worker crashes
                        LOGGER.error(
                            "Failed to compute metrics for %d-symbol batch starting at %s: %s",
                            len(batch_symbols),
                            batch_symbols[0],
                            exc,
                        )
                        completed_symbols += len(batch_symbols)
                        maybe_report_progress()
    except KeyboardInterrupt:
        interrupted = True
        return _cancel_cli_command(
            cancelled_message,
            executors=[executor],
            flushers=[
                lambda: flush_pending(force=True),
                lambda: maybe_report_progress(force=True),
                write_connection.close,
            ],
        )
    finally:
        if not interrupted:
            executor.shutdown(wait=True)

    flush_pending(force=True)
    maybe_report_progress(force=True)
    _print_metric_invariant_failure_summary(metric_failures)
    print(f"Computed metrics for {total_symbols} symbols in {db_path}")
    if profile_state.enabled:
        profile_state.total_seconds = time.perf_counter() - run_start_at
        _print_metric_computation_profile(profile_state)
    write_connection.close()
    return 0


def _print_metric_computation_profile(
    profile_state: "_MetricComputationProfile",
) -> None:
    """Emit a one-line wall-clock summary of compute-metrics phases."""

    parent_phase_seconds = (
        profile_state.read_seconds
        + profile_state.compute_seconds
        + profile_state.write_seconds
    )
    other_seconds = max(0.0, profile_state.total_seconds - parent_phase_seconds)
    print(
        "Profile: "
        f"read={profile_state.read_seconds:.2f}s "
        f"compute={profile_state.compute_seconds:.2f}s "
        f"write={profile_state.write_seconds:.2f}s "
        f"other={other_seconds:.2f}s "
        f"total={profile_state.total_seconds:.2f}s "
        f"flushes={profile_state.write_flush_count} "
        f"rows_written={profile_state.write_row_count}",
        flush=True,
    )


def cmd_compute_metrics_stage(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    metric_ids: Optional[Sequence[str]],
    show_metric_warnings: bool = False,
    profile: bool = False,
) -> int:
    """Unified metric computation over symbol, exchange, or full supported scope."""

    db_path = _resolve_database_path(database)
    canonical_symbols, _, _ = _resolve_canonical_scope_symbols(
        str(db_path),
        symbols,
        exchange_codes,
        all_supported,
    )
    ids_to_compute = _validated_metric_ids(metric_ids)
    return _run_metric_computation(
        database=str(db_path),
        symbols=canonical_symbols,
        metric_ids=ids_to_compute,
        cancelled_message="\nMetric computation cancelled by user.",
        suppress_metric_warnings=not show_metric_warnings,
        profile=profile,
    )


def _ordered_unique_metric_ids(*metric_id_lists: Sequence[str]) -> List[str]:
    ordered: List[str] = []
    seen: set[str] = set()
    for metric_ids in metric_id_lists:
        for metric_id in metric_ids:
            candidate = str(metric_id).strip()
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            ordered.append(candidate)
    return ordered


def _screen_requested_metric_ids(definition) -> List[str]:
    metric_ids = _ordered_unique_metric_ids(
        screen_metric_ids(definition),
        ranking_metric_ids(definition),
    )
    ranking = getattr(definition, "ranking", None)
    if ranking is None:
        return metric_ids
    for tie_breaker in ranking.tie_breakers:
        metric_id = str(tie_breaker.metric_id).strip()
        if metric_id in {"canonical_symbol", "symbol", "ticker", "id"}:
            continue
        if metric_id not in metric_ids:
            metric_ids.append(metric_id)
    return metric_ids


def _evaluate_screen_scope(
    definition,
    symbols: Sequence[str],
    metrics_repo: MetricsRepository,
    fact_repo: RegionFactsRepository,
    market_repo: MarketDataRepository,
    entity_repo: EntityMetadataRepository,
    universe_names: Mapping[str, Optional[str]],
    *,
    report_progress: bool,
) -> tuple[List[str], Dict[str, Dict[str, float]], Dict[str, str]]:
    entity_labels: Dict[str, str] = {}
    passed_symbols: List[str] = []
    criterion_values: Dict[str, Dict[str, float]] = {
        criterion.name: {} for criterion in definition.criteria
    }
    completed_symbols = 0
    total_symbols = len(symbols)
    last_progress_at = time.monotonic()
    last_reported_completed = -1

    def maybe_report_progress(force: bool = False) -> None:
        nonlocal last_progress_at, last_reported_completed
        if not report_progress:
            return
        if completed_symbols == last_reported_completed:
            return
        elapsed = time.monotonic() - last_progress_at
        if not force and elapsed < SCREEN_PROGRESS_INTERVAL_SECONDS:
            return
        _print_symbol_progress(completed_symbols, total_symbols)
        last_reported_completed = completed_symbols
        last_progress_at = time.monotonic()

    for symbol in symbols:
        symbol_passed = True
        per_symbol_values: Dict[str, float] = {}
        label = entity_repo.fetch(symbol) or universe_names.get(symbol) or symbol
        entity_labels[symbol] = label
        for criterion in definition.criteria:
            passed, left_value = evaluate_criterion_verbose(
                criterion, symbol, metrics_repo, fact_repo, market_repo
            )
            if not passed or left_value is None:
                symbol_passed = False
                break
            per_symbol_values[criterion.name] = left_value
        if symbol_passed:
            passed_symbols.append(symbol)
            for criterion in definition.criteria:
                criterion_values[criterion.name][symbol] = per_symbol_values[
                    criterion.name
                ]
        completed_symbols += 1
        maybe_report_progress()

    maybe_report_progress(force=True)
    return passed_symbols, criterion_values, entity_labels


def _rank_screen_passers(
    definition,
    passed_symbols: Sequence[str],
    metrics_repo: MetricsRepository,
    entity_repo: EntityMetadataRepository,
) -> tuple[List[str], List[tuple[str, Dict[str, object]]]]:
    if definition.ranking is None or not passed_symbols:
        return list(passed_symbols), []

    metric_ids = ranking_metric_ids(definition)
    for tie_breaker in definition.ranking.tie_breakers:
        if tie_breaker.metric_id in {"canonical_symbol", "symbol", "ticker", "id"}:
            continue
        if tie_breaker.metric_id not in metric_ids:
            metric_ids.append(tie_breaker.metric_id)
    ranking_metric_config = {
        metric.metric_id: metric for metric in definition.ranking.metrics
    }
    tie_breaker_config = {
        tie_breaker.metric_id: tie_breaker
        for tie_breaker in definition.ranking.tie_breakers
        if tie_breaker.metric_id not in {"canonical_symbol", "symbol", "ticker", "id"}
    }
    fx_service = FXService(metrics_repo.db_path)
    metric_values: Dict[str, Dict[str, float]] = {}
    for metric_id in metric_ids:
        records_by_symbol: Dict[str, MetricRecord] = {}
        unit_kinds = set()
        currencies = set()
        for symbol in passed_symbols:
            record = metrics_repo.fetch(symbol, metric_id)
            if record is None:
                continue
            records_by_symbol[symbol] = record
            unit_kinds.add(record.unit_kind)
            if record.currency:
                currencies.add(record.currency)

        if not records_by_symbol:
            metric_values[metric_id] = {}
            continue

        if len(unit_kinds) > 1:
            LOGGER.warning(
                "Ranking metric skipped due to mixed unit kinds | metric=%s unit_kinds=%s",
                metric_id,
                ",".join(sorted(unit_kinds)),
            )
            metric_values[metric_id] = {}
            continue

        sample = next(iter(records_by_symbol.values()))
        config_entry = ranking_metric_config.get(metric_id) or tie_breaker_config.get(
            metric_id
        )
        comparison_currency = normalize_currency_code(
            getattr(config_entry, "currency", None)
        )

        if is_monetary_unit_kind(sample.unit_kind):
            if comparison_currency is None and len(currencies) > 1:
                LOGGER.warning(
                    "Ranking metric skipped due to mixed currencies without comparison currency | metric=%s currencies=%s",
                    metric_id,
                    ",".join(sorted(currencies)),
                )
                metric_values[metric_id] = {}
                continue
            target_currency = comparison_currency or next(iter(currencies), None)
            converted_values: Dict[str, float] = {}
            for symbol, record in records_by_symbol.items():
                if target_currency is None:
                    continue
                if record.currency is None:
                    LOGGER.warning(
                        "Ranking metric missing currency | metric=%s symbol=%s",
                        metric_id,
                        symbol,
                    )
                    continue
                if record.currency == target_currency:
                    converted_values[symbol] = record.value
                    continue
                converted = fx_service.convert_amount(
                    record.value,
                    record.currency,
                    target_currency,
                    record.as_of,
                )
                if converted is None:
                    LOGGER.warning(
                        "Ranking FX conversion failed | metric=%s symbol=%s from=%s to=%s as_of=%s",
                        metric_id,
                        symbol,
                        record.currency,
                        target_currency,
                        record.as_of,
                    )
                    continue
                converted_values[symbol] = float(converted)
            metric_values[metric_id] = converted_values
            continue

        metric_values[metric_id] = {
            symbol: record.value for symbol, record in records_by_symbol.items()
        }

    metadata = entity_repo.fetch_many(passed_symbols)
    sectors = {
        symbol: security.sector if security is not None else None
        for symbol, security in (
            (symbol, metadata.get(symbol)) for symbol in passed_symbols
        )
    }
    ranking_result = compute_screen_ranking(
        passed_symbols,
        definition.ranking,
        metric_values,
        sectors,
    )
    return list(ranking_result.ordered_symbols), [
        (
            "qarp_rank",
            {
                symbol: ranking_result.ranks[symbol]
                for symbol in ranking_result.ordered_symbols
            },
        ),
        (
            "qarp_score",
            {
                symbol: ranking_result.scores[symbol]
                for symbol in ranking_result.ordered_symbols
            },
        ),
    ]


def _emit_screen_results(
    criteria: Sequence[Criterion],
    symbols: Sequence[str],
    values: Dict[str, Dict[str, float]],
    entity_labels: Mapping[str, str],
    entity_repo: EntityMetadataRepository,
    market_repo: MarketDataRepository,
    output_csv: Optional[str],
    extra_rows: Optional[Sequence[tuple[str, Dict[str, object]]]] = None,
) -> None:
    selected_names = {symbol: entity_labels.get(symbol, symbol) for symbol in symbols}
    selected_descriptions: Dict[str, str] = {}
    selected_prices: Dict[str, str] = {}
    selected_price_currencies: Dict[str, str] = {}
    for symbol in symbols:
        entity_description = entity_repo.fetch_description(symbol)
        selected_descriptions[symbol] = (
            entity_description if entity_description else "N/A"
        )
        snapshot = market_repo.latest_snapshot(symbol)
        if snapshot:
            selected_prices[symbol] = _format_value(snapshot.price)
            selected_price_currencies[symbol] = snapshot.currency or "N/A"
        else:
            selected_prices[symbol] = "N/A"
            selected_price_currencies[symbol] = "N/A"
    _print_screen_table(
        criteria,
        symbols,
        values,
        selected_names,
        selected_descriptions,
        selected_prices,
        extra_rows=extra_rows,
    )
    if output_csv:
        _write_screen_csv(
            criteria,
            symbols,
            values,
            selected_names,
            selected_descriptions,
            selected_prices,
            selected_price_currencies,
            output_csv,
            extra_rows=extra_rows,
        )


def cmd_run_screen_stage(
    config_path: str,
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    output_csv: Optional[str],
    show_metric_warnings: bool = False,
) -> int:
    """Unified screen evaluation over symbol, exchange, or full supported scope."""

    db_path = _resolve_database_path(database)
    canonical_symbols, _explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_symbols(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    definition = load_screen(config_path)
    requested_metric_ids = _screen_requested_metric_ids(definition)
    include_market_data = any(
        getattr(REGISTRY.get(metric_id), "uses_market_data", False)
        for metric_id in requested_metric_ids
        if REGISTRY.get(metric_id) is not None
    )
    MetricsRepository(db_path).initialize_schema()
    _initialize_metric_read_schema(db_path, include_market_data)
    base_fact_repo = FinancialFactsRepository(db_path)
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    metrics_repo = _StatusAwareMetricsRepository(
        db_path,
        market_repo=_SchemaReadyMarketDataRepository(db_path),
    )
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()

    with suppress_console_metric_warnings(not show_metric_warnings):
        if len(canonical_symbols) == 1:
            symbol = canonical_symbols[0]
            entity_name = entity_repo.fetch(symbol) or symbol
            description = entity_repo.fetch_description(symbol) or "N/A"
            snapshot = market_repo.latest_snapshot(symbol)
            price_label = _format_value(snapshot.price) if snapshot else "N/A"
            print(f"Entity: {entity_name}")
            print(f"Description: {description}")
            print(f"Price: {price_label}")
            results = []
            for criterion in definition.criteria:
                passed, left_value = evaluate_criterion_verbose(
                    criterion, symbol, metrics_repo, fact_repo, market_repo
                )
                results.append((criterion.name, passed, left_value))
            passed_all = all(flag for _, flag, _ in results)
            for name, passed, value in results:
                value_display = _format_value(value) if value is not None else "N/A"
                print(f"{name}: {'PASS' if passed else 'FAIL'} (value={value_display})")
            return 0 if passed_all else 1

        ticker_repo = SupportedTickerRepository(db_path)
        universe_names = dict(
            ticker_repo.list_canonical_symbol_name_pairs(resolved_exchange_codes)
        )
        evaluation_metrics_repo = _PreloadedMetricsRepository(
            db_path,
            metrics_repo.fetch_many_for_symbols(
                canonical_symbols,
                requested_metric_ids,
            ),
        )
        passed_symbols, criterion_values, entity_labels = _evaluate_screen_scope(
            definition,
            canonical_symbols,
            evaluation_metrics_repo,
            fact_repo,
            market_repo,
            entity_repo,
            universe_names,
            report_progress=True,
        )

        if not passed_symbols:
            print("No symbols satisfied all criteria.")
            if output_csv:
                _write_screen_csv(
                    definition.criteria,
                    [],
                    {},
                    {},
                    {},
                    {},
                    {},
                    output_csv,
                )
            return 1

        ordered_symbols, extra_rows = _rank_screen_passers(
            definition,
            passed_symbols,
            evaluation_metrics_repo,
            entity_repo,
        )
        _emit_screen_results(
            definition.criteria,
            ordered_symbols,
            criterion_values,
            entity_labels,
            entity_repo,
            market_repo,
            output_csv,
            extra_rows=extra_rows,
        )
        return 0


def cmd_ingest_eodhd_fundamentals(
    symbol: str,
    database: str,
    exchange_code: Optional[str],
) -> int:
    """Fetch EODHD fundamentals for a ticker and store raw payload."""

    api_key = _require_eodhd_key()
    client = EODHDFundamentalsClient(api_key=api_key)
    base_symbol = symbol.upper()
    inferred_exchange = None
    if "." in base_symbol:
        base, suffix = base_symbol.split(".", 1)
        base_symbol = base
        inferred_exchange = suffix
    if not exchange_code and not inferred_exchange:
        raise SystemExit(
            "--exchange-code is required when provider=EODHD and symbol has no exchange suffix."
        )
    exch_code = (inferred_exchange or exchange_code or "").upper() or None
    qualified_symbol = _qualify_symbol(base_symbol, exch_code)
    fetch_symbol = qualified_symbol
    payload = client.fetch_fundamentals(fetch_symbol, exchange_code=None)
    storage_symbol = qualified_symbol
    repo = FundamentalsRepository(database)
    repo.initialize_schema()
    general = payload.get("General") or {}
    repo.upsert(
        "EODHD",
        storage_symbol,
        payload,
        currency=general.get("CurrencyCode"),
        exchange=exch_code,
    )
    print(f"Stored EODHD fundamentals for {storage_symbol} in {database}")
    return 0


def cmd_ingest_eodhd_fundamentals_bulk(
    database: str,
    rate: float,
    exchange_code: Optional[str],
) -> int:
    """Fetch EODHD fundamentals for an exchange from stored supported tickers."""

    return cmd_ingest_fundamentals_bulk(
        provider="EODHD",
        database=database,
        rate=rate,
        exchange_code=exchange_code,
        user_agent=None,
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )


def cmd_ingest_fundamentals(
    provider: str,
    symbol: str,
    database: str,
    exchange_code: Optional[str],
    user_agent: Optional[str],
    cik: Optional[str],
) -> int:
    """Fetch fundamentals for a ticker from the specified provider."""

    provider_norm = _normalize_provider(provider)
    if provider_norm == "SEC":
        symbol_upper = symbol.strip().upper()
        if "." in symbol_upper:
            if not symbol_upper.endswith(".US"):
                raise SystemExit(
                    "SEC ingestion requires a .US suffix or an unqualified US symbol."
                )
            if exchange_code and exchange_code.upper() != "US":
                raise SystemExit("SEC ingestion only supports --exchange-code US.")
        else:
            if not exchange_code:
                raise SystemExit(
                    "--exchange-code is required when SEC symbol has no suffix."
                )
            if exchange_code.upper() != "US":
                raise SystemExit("SEC ingestion only supports --exchange-code US.")
        client = SECCompanyFactsClient(user_agent=user_agent)
        symbol_qualified = _qualify_symbol(symbol, exchange="US")
        if cik:
            cik_value = cik
        else:
            info = client.resolve_company(symbol_qualified.split(".")[0])
            cik_value = info.cik
            symbol_qualified = _qualify_symbol(info.symbol, exchange="US")
            LOGGER.info(
                "Resolved %s to CIK %s (%s)", symbol_qualified, cik_value, info.name
            )

        payload = client.fetch_company_facts(cik_value)
        fundamentals_repo = FundamentalsRepository(database)
        fundamentals_repo.initialize_schema()
        fundamentals_repo.upsert(
            "SEC", symbol_qualified.upper(), payload, exchange="US"
        )
        print(
            f"Stored SEC company facts for {symbol_qualified} ({cik_value}) in {database}"
        )
        return 0
    if provider_norm == "EODHD":
        if not exchange_code and "." not in symbol:
            raise SystemExit(
                "--exchange-code is required when provider=EODHD and symbol has no exchange suffix."
            )
        return cmd_ingest_eodhd_fundamentals(
            symbol=symbol, database=database, exchange_code=exchange_code
        )
    raise SystemExit(f"Unsupported provider: {provider}")


def cmd_ingest_fundamentals_bulk(
    provider: str,
    database: str,
    rate: Optional[float],
    exchange_code: Optional[str],
    user_agent: Optional[str],
    max_symbols: Optional[int],
    max_age_days: Optional[int],
    respect_backoff: bool,
) -> int:
    """Fetch fundamentals in bulk for the specified provider."""

    provider_norm = _normalize_provider(provider)
    if not exchange_code:
        raise SystemExit("--exchange-code is required for bulk fundamentals ingestion.")
    if provider_norm == "SEC":
        exchange_norm = exchange_code.upper()
        rate_value = rate if rate is not None else 9.0
        client = SECCompanyFactsClient(user_agent=user_agent)
        fundamentals_repo = FundamentalsRepository(database)
        fundamentals_repo.initialize_schema()

        ticker_repo = SupportedTickerRepository(database)
        symbols = ticker_repo.list_symbols_by_exchange(provider_norm, exchange_norm)
        if not symbols:
            raise SystemExit(
                f"No supported tickers found for provider {provider_norm} on exchange {exchange_norm}. "
                "Run load-universe --provider SEC first."
            )

        min_interval = 1.0 / rate_value if rate_value and rate_value > 0 else 0.0
        last_fetch = 0.0
        total = len(symbols)
        processed = 0
        print(
            f"Fetching SEC company facts for {total} symbols on {exchange_norm} "
            f"at <= {rate_value:.2f} req/s"
        )

        try:
            for idx, symbol in enumerate(symbols, 1):
                try:
                    info = client.resolve_company(symbol.split(".")[0])
                except Exception as exc:  # pragma: no cover - rare network errors
                    LOGGER.error("Failed to resolve CIK for %s: %s", symbol, exc)
                    continue

                if min_interval > 0 and last_fetch:
                    elapsed = time.perf_counter() - last_fetch
                    if elapsed < min_interval:
                        time.sleep(min_interval - elapsed)

                try:
                    payload = client.fetch_company_facts(info.cik)
                except Exception as exc:  # pragma: no cover - network errors
                    LOGGER.error(
                        "Failed to fetch company facts for %s: %s", info.symbol, exc
                    )
                    last_fetch = time.perf_counter()
                    continue

                last_fetch = time.perf_counter()
                qualified = _qualify_symbol(info.symbol, exchange="US")
                fundamentals_repo.upsert(
                    "SEC",
                    qualified,
                    payload,
                    exchange=exchange_norm,
                )
                processed += 1
                print(
                    f"[{idx}/{total}] Stored company facts for {qualified}", flush=True
                )
        except KeyboardInterrupt:
            return _cancel_cli_command(
                f"\nCancelled after {processed} of {total} symbols."
            )

        print(f"Stored company facts for {processed} symbols in {database}")
        return 0
    if provider_norm == "EODHD":
        exchange_norm = exchange_code.upper()
        api_key = _require_eodhd_key()
        prepared = _prepare_eodhd_fundamentals_run(
            database=database,
            api_key=api_key,
            exchange_codes=[exchange_norm],
            provider_symbols=None,
            rate=rate,
            max_symbols=max_symbols,
            max_age_days=max_age_days,
            respect_backoff=respect_backoff,
            missing_only=False,
        )
        return _run_eodhd_fundamentals_ingestion(
            database=database,
            api_key=api_key,
            scope_label=exchange_norm,
            prepared=prepared,
        )
    raise SystemExit(f"Unsupported provider: {provider}")


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
    payload_record = fund_repo.fetch_payload_with_fetched_at("SEC", symbol.upper())
    if payload_record is None:
        raise SystemExit(
            f"No raw SEC payload found for {symbol}. Run ingest-fundamentals --provider SEC before normalization."
        )
    if skipped and not candidates_to_normalize:
        _print_normalization_up_to_date("SEC", database)
        return 0

    payload, raw_fetched_at = payload_record
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
    state_repo.mark_success("SEC", symbol.upper(), security_id, raw_fetched_at)
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


def _extract_entity_name_from_eodhd(payload: Dict) -> Optional[str]:
    general = payload.get("General") or {}
    return general.get("Name") or general.get("Code")


def _extract_entity_description_from_eodhd(payload: Dict) -> Optional[str]:
    general = payload.get("General") or {}
    return general.get("Description")


def _extract_entity_sector_from_eodhd(payload: Dict) -> Optional[str]:
    general = payload.get("General") or {}
    sector = str(general.get("Sector") or "").strip()
    return sector or None


def _extract_entity_industry_from_eodhd(payload: Dict) -> Optional[str]:
    general = payload.get("General") or {}
    industry = str(general.get("Industry") or "").strip()
    return industry or None


def _extract_entity_name_from_sec(payload: Dict) -> Optional[str]:
    entity_name = str(payload.get("entityName") or "").strip()
    return entity_name or None


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
        record.unit,
        record.value,
        record.accn,
        record.filed,
        record.frame,
        getattr(record, "start_date", None),
        getattr(record, "accounting_standard", None),
        getattr(record, "currency", None),
    )


def _create_process_pool_executor(max_workers: int) -> ProcessPoolExecutor:
    """Create a process pool using the interpreter's platform default start method."""

    log_dir, console_level, file_level = current_logging_config()
    if log_dir is None:
        return ProcessPoolExecutor(max_workers=max_workers)
    return ProcessPoolExecutor(
        max_workers=max_workers,
        initializer=_initialize_worker_logging,
        initargs=(
            str(log_dir) if log_dir is not None else None,
            console_level,
            file_level,
        ),
    )


def _initialize_worker_logging(
    log_dir: Optional[str],
    console_level: int,
    file_level: int,
) -> None:
    """Mirror the parent logging configuration inside spawned worker processes."""

    setup_logging(
        log_dir=log_dir or "data/logs",
        console_level=console_level,
        file_level=file_level,
    )


# ---------------------------------------------------------------------------
# Process-local caching for normalization workers
# ---------------------------------------------------------------------------
# Each worker process reuses a single FXService and SupportedTickerRepository
# across all symbols, avoiding per-symbol config reads, schema checks, and
# repeated FX pair-history loads. The _SchemaReady* subclasses no-op
# initialize_schema() since the main process has already ensured the schema
# exists.

_process_local_fx_service: Optional[FXService] = None
_process_local_fx_service_db: Optional[str] = None
_process_local_ticker_repo: Optional[SupportedTickerRepository] = None
_process_local_ticker_repo_db: Optional[str] = None


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
    payload_record = fund_repo.fetch_payload_with_fetched_at("SEC", symbol)
    if payload_record is None:
        return None
    payload, raw_fetched_at = payload_record
    fx_service = _get_or_create_fx_service(database)
    normalizer = SECFactsNormalizer(fx_service=fx_service)
    rows = tuple(
        _normalization_record_to_row(record)
        for record in normalizer.normalize(payload, symbol=symbol)
    )
    return _NormalizedFactsResult(
        symbol=symbol,
        rows=rows,
        raw_fetched_at=raw_fetched_at,
        entity_name=_extract_entity_name_from_sec(payload),
    )


def _normalize_eodhd_symbol_worker(
    database: Union[str, Path], symbol: str
) -> Optional[_NormalizedFactsResult]:
    """Normalize one stored EODHD payload and return facts plus metadata."""

    fund_repo = FundamentalsRepository(database)
    payload_record = fund_repo.fetch_payload_with_fetched_at("EODHD", symbol)
    if payload_record is None:
        return None
    payload, raw_fetched_at = payload_record
    target_currency = _resolve_ticker_target_currency(
        database, symbol, payload, ticker_repo=_get_or_create_ticker_repo(database)
    )
    if target_currency is None:
        raise ValueError(f"Missing trading currency in market_data for {symbol}")
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
        raw_fetched_at=raw_fetched_at,
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
                    result.raw_fetched_at,
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
                        result.raw_fetched_at,
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
    payload_record = fund_repo.fetch_payload_with_fetched_at("EODHD", symbol_upper)
    if payload_record is None:
        raise SystemExit(
            f"No EODHD fundamentals found for {symbol}. Run ingest-fundamentals --provider EODHD first."
        )
    if skipped and not candidates_to_normalize:
        _print_normalization_up_to_date("EODHD", database)
        return 0

    payload, raw_fetched_at = payload_record

    ticker_repo = _SchemaReadySupportedTickerRepository(database)
    target_currency = _resolve_ticker_target_currency(
        database, symbol_upper, payload, ticker_repo=ticker_repo
    )
    if target_currency is None:
        raise SystemExit(
            "EODHD normalization failed for "
            f"{symbol_upper}: missing trading currency in market_data"
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
    state_repo.mark_success("EODHD", symbol_upper, security_id, raw_fetched_at)
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


def cmd_refresh_exchange(
    provider: str,
    exchange_code: str,
    database: str,
    include_etfs: bool,
    currencies: Optional[Sequence[str]],
    include_exchanges: Optional[Sequence[str]],
    fundamentals_rate: Optional[float],
    market_rate: float,
    max_symbols: Optional[int],
    max_age_days: Optional[int],
    respect_backoff: bool,
    user_agent: Optional[str],
    metrics: Optional[Sequence[str]],
) -> int:
    """Run catalog, fundamentals, market data, normalization, and metrics for an exchange."""

    provider_norm = _normalize_provider(provider)
    exchange_norm = exchange_code.upper()
    if provider_norm == "SEC" and exchange_norm != "US":
        raise SystemExit("provider=SEC only supports --exchange-code US.")
    if provider_norm == "SEC" and (currencies or include_exchanges):
        raise SystemExit(
            "--currencies/--include-exchanges are only valid with provider=EODHD."
        )
    if provider_norm == "EODHD" and (include_etfs or currencies or include_exchanges):
        raise SystemExit(
            "refresh-exchange --provider EODHD no longer supports universe filtering flags. "
            "The canonical EODHD catalog comes from refresh-supported-tickers."
        )

    print("Step 1/5: refresh catalog")
    if provider_norm == "SEC":
        result = cmd_load_universe(
            provider=provider_norm,
            database=database,
            include_etfs=include_etfs,
            exchange_code=None,
            currencies=None,
            include_exchanges=None,
        )
    else:
        result = cmd_refresh_supported_tickers(
            provider=provider_norm,
            database=database,
            exchange_codes=[exchange_norm],
            all_supported=False,
            include_etfs=False,
        )
    if result != 0:
        return result

    print("Step 2/5: ingest fundamentals")
    result = cmd_ingest_fundamentals_bulk(
        provider=provider_norm,
        database=database,
        rate=fundamentals_rate,
        exchange_code=exchange_norm,
        user_agent=user_agent,
        max_symbols=max_symbols,
        max_age_days=max_age_days,
        respect_backoff=respect_backoff,
    )
    if result != 0:
        return result

    print("Step 3/5: update market data")
    result = cmd_update_market_data_bulk(
        provider=provider_norm,
        database=database,
        rate=market_rate,
        exchange_code=exchange_norm,
    )
    if result != 0:
        return result

    print("Step 4/5: normalize fundamentals")
    result = cmd_normalize_fundamentals_bulk(
        provider=provider_norm,
        database=database,
        exchange_code=exchange_norm,
    )
    if result != 0:
        return result

    print("Step 5/5: compute metrics")
    return cmd_compute_metrics_bulk(
        provider=provider_norm,
        database=database,
        metric_ids=metrics,
        exchange_code=exchange_norm,
    )


def cmd_update_market_data(
    symbol: str, database: str, exchange_code: Optional[str]
) -> int:
    """Fetch latest market data for a ticker and store it."""

    symbol_clean = symbol.strip().upper()
    if "." not in symbol_clean:
        if not exchange_code:
            raise SystemExit(
                "--exchange-code is required when symbol has no exchange suffix (e.g., AAPL.US)."
            )
        symbol_clean = _format_market_symbol(symbol_clean, exchange_code)

    service = MarketDataService(db_path=database)
    data = service.refresh_symbol(symbol_clean)
    print(
        f"Stored market data for {data.symbol}: price={data.price} as_of={data.as_of} in {database}"
    )
    return 0


def cmd_update_market_data_bulk(
    provider: str,
    database: str,
    rate: float,
    exchange_code: Optional[str],
) -> int:
    """Fetch market data for every supported ticker in one provider/exchange slice."""

    if not exchange_code:
        raise SystemExit("--exchange-code is required for bulk market data updates.")

    provider_norm = _normalize_provider(provider)
    ticker_repo = SupportedTickerRepository(database)
    ticker_repo.initialize_schema()
    exchange_norm = exchange_code.upper()
    listing_rows = ticker_repo.list_symbols_by_exchange(provider_norm, exchange_norm)
    if not listing_rows:
        raise SystemExit(
            f"No supported tickers found for provider {provider_norm} on exchange {exchange_norm}. "
            f"{_catalog_bootstrap_guidance(provider_norm)}"
        )
    pairs = [(symbol, exchange_norm) for symbol in listing_rows]

    service = MarketDataService(db_path=database)
    interval = 60.0 / rate if rate and rate > 0 else 0.0
    total = len(pairs)
    processed = 0
    print(f"Updating market data for {total} symbols at <= {rate:.2f} per minute")

    try:
        for idx, (symbol, exchange) in enumerate(pairs, 1):
            start = time.perf_counter()
            try:
                fetch_symbol = _format_market_symbol(symbol, exchange)
                service.refresh_symbol(symbol, fetch_symbol=fetch_symbol)
                processed += 1
                print(f"[{idx}/{total}] Stored market data for {symbol}", flush=True)
            except Exception as exc:  # pragma: no cover - network failures
                LOGGER.error("Failed to refresh market data for %s: %s", symbol, exc)
            elapsed = time.perf_counter() - start
            if interval > 0 and elapsed < interval:
                time.sleep(interval - elapsed)
    except KeyboardInterrupt:
        return _cancel_cli_command(f"\nCancelled after {processed} of {total} symbols.")

    print(f"Stored market data for {processed} symbols in {database}")
    return 0


def cmd_compute_metrics(
    symbol: str,
    metric_ids: Sequence[str],
    database: str,
    run_all: bool,
    exchange_code: Optional[str],
    show_metric_warnings: bool = False,
) -> int:
    """Compute one or more metrics and store the results."""

    db_path = _resolve_database_path(database)
    symbol_upper = symbol.strip().upper()
    if "." not in symbol_upper:
        if not exchange_code:
            raise SystemExit(
                "--exchange-code is required when symbol has no exchange suffix (e.g., AAPL.US)."
            )
        symbol_upper = _format_market_symbol(symbol_upper, exchange_code)
    ids_to_compute = _validated_metric_ids(
        list(REGISTRY.keys()) if run_all else list(metric_ids)
    )

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    status_repo = MetricComputeStatusRepository(db_path)
    status_repo.initialize_schema()
    fact_repo = FinancialFactsRepository(db_path)
    market_repo = (
        MarketDataRepository(db_path)
        if _metrics_use_market_data(ids_to_compute)
        else None
    )
    result = _compute_metric_batch_results(
        [symbol_upper],
        ids_to_compute,
        fact_repo,
        market_repo,
        suppress_metric_warnings=not show_metric_warnings,
    )[0]
    if result.rows:
        metrics_repo.upsert_many(result.rows)
    status_rows = _metric_status_rows_from_attempts(result.attempts)
    if status_rows:
        status_repo.upsert_many(status_rows)
    _print_metric_invariant_failure_summary(result.failures, symbol=symbol_upper)
    print(f"Computed {result.computed_count} metrics for {symbol_upper} in {database}")
    return 0


def cmd_compute_metrics_bulk(
    provider: str,
    database: str,
    metric_ids: Optional[Sequence[str]],
    exchange_code: Optional[str],
    show_metric_warnings: bool = False,
) -> int:
    """Compute metrics for all supported tickers in one provider/exchange slice."""

    db_path = _resolve_database_path(database)
    provider_norm = _normalize_provider(provider)
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    if not exchange_code:
        raise SystemExit("--exchange-code is required for bulk metric computation.")

    exchange_norm = exchange_code.upper()
    symbols = ticker_repo.list_symbols_by_exchange(provider_norm, exchange_norm)
    if not symbols:
        raise SystemExit(
            f"No supported tickers found for provider {provider_norm} on exchange {exchange_norm}. "
            f"{_catalog_bootstrap_guidance(provider_norm)}"
        )

    ids_to_compute = _validated_metric_ids(metric_ids)
    return _run_metric_computation(
        database=str(db_path),
        symbols=symbols,
        metric_ids=ids_to_compute,
        cancelled_message="\nBulk metric computation cancelled by user.",
        suppress_metric_warnings=not show_metric_warnings,
    )


def cmd_report_fact_freshness(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    metric_ids: Optional[Sequence[str]],
    max_age_days: int,
    output_csv: Optional[str],
    show_all: bool,
) -> int:
    """Report missing or stale financial facts needed by metrics for a canonical scope."""

    db_path = _resolve_database_path(database)
    selected_symbols, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_symbols(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )

    metric_classes = _select_metric_classes(metric_ids)
    base_fact_repo = FinancialFactsRepository(db_path)
    fact_repo = RegionFactsRepository(base_fact_repo)
    coverage = compute_fact_coverage(
        fact_repo,
        selected_symbols,
        metric_classes,
        max_age_days=max_age_days,
    )
    scope_label = _scope_label(
        explicit_symbols,
        resolved_exchange_codes,
        "all supported tickers",
    )

    print(
        f"Fact coverage for {scope_label} "
        f"({len(selected_symbols)} symbols, max_age_days={max_age_days})"
    )
    for entry in coverage:
        missing_total = sum(c.missing for c in entry.concepts)
        stale_total = sum(c.stale for c in entry.concepts)
        print(
            f"- {entry.metric_id}: fully_fresh={entry.fully_covered}/{entry.total_symbols}, "
            f"missing={missing_total}, stale={stale_total}"
        )
        for concept in entry.concepts:
            if not show_all and concept.missing == 0 and concept.stale == 0:
                continue
            fresh = max(entry.total_symbols - concept.missing - concept.stale, 0)
            print(
                f"    {concept.concept}: fresh={fresh}, stale={concept.stale}, missing={concept.missing}"
            )
    if output_csv:
        _write_fact_report_csv(coverage, output_csv)
        print(f"Wrote concept-level coverage to {output_csv}")
    return 0


def cmd_report_metric_coverage(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    metric_ids: Optional[Sequence[str]],
) -> int:
    """Count symbols that can compute all requested metrics for a canonical scope."""

    db_path = _resolve_database_path(database)
    selected_symbols, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_symbols(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )

    metric_classes = _select_metric_classes(metric_ids)
    base_fact_repo = FinancialFactsRepository(db_path)
    base_fact_repo.initialize_schema()
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()

    per_metric_success: Dict[str, int] = {
        getattr(cls, "id", cls.__name__): 0 for cls in metric_classes
    }
    all_success = 0

    for symbol in selected_symbols:
        symbol_ok = True
        for metric_cls in metric_classes:
            metric = metric_cls()
            try:
                if getattr(metric, "uses_market_data", False):
                    result = metric.compute(symbol, fact_repo, market_repo)
                else:
                    result = metric.compute(symbol, fact_repo)
            except MetricCurrencyInvariantError:
                result = None
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.error(
                    "Metric %s failed for %s: %s",
                    getattr(metric_cls, "id", metric_cls.__name__),
                    symbol,
                    exc,
                )
                result = None
            if consume_metric_currency_invariant_error(metric) is not None:
                result = None
            if result is None:
                symbol_ok = False
                continue
            per_metric_success[getattr(metric_cls, "id", metric_cls.__name__)] += 1
        if symbol_ok and metric_classes:
            all_success += 1

    total_symbols = len(selected_symbols)
    scope_label = _scope_label(
        explicit_symbols,
        resolved_exchange_codes,
        "all supported tickers",
    )
    print(
        f"Metric coverage for {scope_label} "
        f"(symbols={total_symbols}, metrics={len(metric_classes)})"
    )
    print(f"Symbols where all metrics computed: {all_success}/{total_symbols}")
    ordered = sorted(per_metric_success.items(), key=lambda item: (item[1], item[0]))
    for metric_id, count in ordered:
        print(f"- {metric_id}: {count}/{total_symbols} symbols")
    return 0


class _MetricWarningCollector(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.WARNING:
            self.records.append(record)

    def clear(self) -> None:
        self.records.clear()


_DATE_PATTERN = re.compile(r"^\\d{4}-\\d{2}-\\d{2}$")


def _format_failure_reason(records: Sequence[logging.LogRecord], symbol: str) -> str:
    if not records:
        return "no warning emitted"

    record = records[0]
    msg = record.msg if isinstance(record.msg, str) else str(record.msg)
    args = record.args
    if not args:
        return msg

    def transform(arg: object) -> object:
        if isinstance(arg, str):
            if arg.upper() == symbol.upper():
                return "<symbol>"
            if _DATE_PATTERN.match(arg):
                return "<date>"
            return arg
        if isinstance(arg, (int, float)):
            return "<n>"
        return arg

    try:
        if isinstance(args, dict):
            transformed_map = {key: transform(value) for key, value in args.items()}
            return msg % transformed_map
        if not isinstance(args, tuple):
            args = (args,)
        transformed_args = tuple(transform(value) for value in args)
        return msg % transformed_args
    except Exception:
        return record.getMessage()


def _metric_failure_reason_from_exception(exc: Exception) -> str:
    """Return a compact failure reason for one metric exception."""

    if isinstance(exc, MetricCurrencyInvariantError):
        return exc.summary_reason
    return f"exception: {exc.__class__.__name__}"


def _metric_failure_message(exc: Exception) -> str:
    """Return the detailed user/log message for one metric exception."""

    return str(exc)


def _print_metric_invariant_failure_summary(
    failures: Sequence[_MetricComputationFailure],
    *,
    symbol: Optional[str] = None,
) -> None:
    """Print a compact grouped summary for currency-invariant metric failures."""

    invariant_failures = [
        failure
        for failure in failures
        if failure.reason.startswith("currency invariant:")
    ]
    if not invariant_failures:
        return

    grouped = Counter(
        (failure.metric_id, failure.reason) for failure in invariant_failures
    )
    examples: Dict[tuple[str, str], _MetricComputationFailure] = {}
    for failure in invariant_failures:
        examples.setdefault((failure.metric_id, failure.reason), failure)

    if symbol is not None:
        print(f"Metric currency invariant failures for {symbol}:")
    else:
        print(
            f"Metric currency invariant failures: {len(invariant_failures)} across {len(grouped)} grouped reasons"
        )

    ordered = sorted(
        grouped.items(),
        key=lambda item: (-item[1], item[0][0], item[0][1]),
    )
    for (metric_id, reason), count in ordered:
        example = examples[(metric_id, reason)]
        if symbol is not None:
            print(f"- {metric_id}: {reason} ({example.message})")
            continue
        print(f"- {metric_id}: {reason} ({count}, example={example.symbol})")


def _write_metric_failure_report_csv(
    failures: Dict[str, Counter],
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
    total_symbols: int,
    metric_order: Sequence[str],
    path: str,
) -> None:
    output_path = _prepare_output_csv_path(path)
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "metric_id",
                "reason",
                "count",
                "total_symbols",
                "failure_rate",
                "example_symbol",
                "example_market_cap",
            ]
        )
        for metric_id in metric_order:
            counter = failures.get(metric_id, Counter())
            if not counter:
                writer.writerow([metric_id, "", 0, total_symbols, 0.0, "", ""])
                continue
            for reason, count in counter.most_common():
                rate = (count / total_symbols) if total_symbols else 0.0
                example = examples.get(metric_id, {}).get(reason)
                example_symbol = example[0] if example else ""
                example_cap = example[1] if example else None
                writer.writerow(
                    [
                        metric_id,
                        reason,
                        count,
                        total_symbols,
                        rate,
                        example_symbol,
                        example_cap or "",
                    ]
                )


def _write_screen_failure_report_csv(
    impacts: Sequence[_ScreenMetricImpactSummary],
    failures: Dict[str, Counter],
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
    path: str,
) -> None:
    output_path = _prepare_output_csv_path(path)
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "metric_id",
                "missing_symbols",
                "affected_criteria_count",
                "affected_criteria",
                "root_cause",
                "root_cause_count",
                "example_symbol",
                "example_market_cap",
            ]
        )
        for impact in impacts:
            counter = failures.get(impact.metric_id, Counter())
            criteria = sorted(impact.affected_criteria)
            if not counter:
                writer.writerow(
                    [
                        impact.metric_id,
                        len(impact.missing_symbols),
                        len(criteria),
                        "; ".join(criteria),
                        "",
                        0,
                        "",
                        "",
                    ]
                )
                continue
            for reason, count in counter.most_common():
                example = examples.get(impact.metric_id, {}).get(reason)
                example_symbol = example[0] if example else ""
                example_cap = example[1] if example else None
                writer.writerow(
                    [
                        impact.metric_id,
                        len(impact.missing_symbols),
                        len(criteria),
                        "; ".join(criteria),
                        reason,
                        count,
                        example_symbol,
                        example_cap if example_cap is not None else "",
                    ]
                )


def _metric_market_cap(
    market_repo: MarketDataRepository,
    market_caps: Dict[str, Optional[float]],
    symbol: str,
) -> Optional[float]:
    cap = market_caps.get(symbol)
    if symbol in market_caps:
        return cap
    snapshot = market_repo.latest_snapshot(symbol)
    cap = snapshot.market_cap if snapshot else None
    market_caps[symbol] = cap
    return cap


def _record_failure_example(
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
    metric_id: str,
    reason: str,
    symbol: str,
    market_cap: Optional[float],
) -> None:
    current = examples[metric_id].get(reason)
    if current is None:
        examples[metric_id][reason] = (symbol, market_cap)
        return
    current_cap = current[1]
    if market_cap is not None and (current_cap is None or market_cap > current_cap):
        examples[metric_id][reason] = (symbol, market_cap)


def _record_metric_failure_reason(
    failures: Dict[str, Counter],
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
    market_repo: MarketDataRepository,
    market_caps: Dict[str, Optional[float]],
    *,
    metric_id: str,
    reason: str,
    symbol: str,
) -> None:
    failures[metric_id][reason] += 1
    cap = _metric_market_cap(market_repo, market_caps, symbol)
    _record_failure_example(examples, metric_id, reason, symbol, cap)


def _recompute_missing_screen_metrics(
    metric_impacts: Mapping[str, _ScreenMetricImpactSummary],
    fact_repo: FinancialFactsRepository,
    market_repo: MarketDataRepository,
    progress_interval_seconds: Optional[float] = None,
) -> tuple[Dict[str, Counter], Dict[str, Dict[str, tuple[str, Optional[float]]]]]:
    failures: Dict[str, Counter] = {
        metric_id: Counter() for metric_id in metric_impacts.keys()
    }
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]] = {
        metric_id: {} for metric_id in metric_impacts.keys()
    }
    metric_ids_by_symbol: Dict[str, List[str]] = {}
    for metric_id, impact in metric_impacts.items():
        for symbol in impact.missing_symbols:
            metric_ids_by_symbol.setdefault(symbol, []).append(metric_id)

    symbols_to_recompute = sorted(metric_ids_by_symbol.keys())
    if not symbols_to_recompute:
        return failures, examples

    db_path = fact_repo.db_path
    metrics_repo = _SchemaReadyMetricsRepository(db_path)
    status_repo = _SchemaReadyMetricComputeStatusRepository(db_path)
    availability_repo = _StatusAwareMetricsRepository(
        db_path,
        raw_metrics_repo=metrics_repo,
        status_repo=status_repo,
        facts_refresh_repo=_SchemaReadyFinancialFactsRefreshStateRepository(db_path),
        market_repo=market_repo,
    )
    maybe_report_progress = (
        _make_symbol_progress_reporter(
            len(symbols_to_recompute),
            progress_interval_seconds,
            printer=_print_recompute_progress_bar,
            start_immediately=True,
        )
        if progress_interval_seconds is not None
        else None
    )

    snapshots_by_symbol = market_repo.latest_snapshots_many(symbols_to_recompute)
    market_caps: Dict[str, Optional[float]] = {
        symbol: None for symbol in symbols_to_recompute
    }
    for symbol, snapshot in snapshots_by_symbol.items():
        market_caps[symbol] = snapshot.market_cap

    availability_states = availability_repo.states_many(
        symbols_to_recompute,
        tuple(metric_impacts.keys()),
        chunk_size=METRICS_COMPUTE_BATCH_SIZE,
    )
    pending_metric_ids_by_symbol: Dict[str, List[str]] = {}
    completed_symbols = 0

    for symbol in symbols_to_recompute:
        pending_metric_ids: List[str] = []
        for metric_id in metric_ids_by_symbol[symbol]:
            if metric_id not in REGISTRY:
                _record_metric_failure_reason(
                    failures,
                    examples,
                    market_repo,
                    market_caps,
                    metric_id=metric_id,
                    reason="unknown_metric_id",
                    symbol=symbol,
                )
                continue
            state = availability_states.get(symbol, {}).get(metric_id)
            if (
                state is not None
                and state.status_record is not None
                and not state.stale
                and state.status_record.status == "failure"
            ):
                _record_metric_failure_reason(
                    failures,
                    examples,
                    market_repo,
                    market_caps,
                    metric_id=metric_id,
                    reason=state.status_record.reason_code or "no warning emitted",
                    symbol=symbol,
                )
                continue
            pending_metric_ids.append(metric_id)
        if pending_metric_ids:
            pending_metric_ids_by_symbol[symbol] = pending_metric_ids
            continue
        completed_symbols += 1
        if maybe_report_progress is not None:
            maybe_report_progress(completed_symbols, False)

    symbols_by_metric_group: Dict[Tuple[str, ...], List[str]] = {}
    for symbol, pending_metric_ids in pending_metric_ids_by_symbol.items():
        metric_group = tuple(pending_metric_ids)
        symbols_by_metric_group.setdefault(metric_group, []).append(symbol)

    for metric_group, group_symbols in symbols_by_metric_group.items():
        for symbol_batch in _batch_values(
            group_symbols,
            METRICS_COMPUTE_BATCH_SIZE,
        ):
            batch_results = _compute_metric_batch_results(
                symbol_batch,
                metric_group,
                fact_repo,
                market_repo,
                suppress_metric_warnings=True,
                preloaded_snapshots_by_symbol=snapshots_by_symbol,
            )
            batch_attempts: List[_MetricAttemptResult] = []
            for result in batch_results:
                batch_attempts.extend(result.attempts)
                for attempt in result.attempts:
                    reason = (
                        "stored_missing_but_computable_now"
                        if attempt.status == "success"
                        else attempt.reason_code or "no warning emitted"
                    )
                    _record_metric_failure_reason(
                        failures,
                        examples,
                        market_repo,
                        market_caps,
                        metric_id=attempt.metric_id,
                        reason=reason,
                        symbol=attempt.symbol,
                    )
                completed_symbols += 1
                if maybe_report_progress is not None:
                    maybe_report_progress(completed_symbols, False)
            _persist_metric_attempts(metrics_repo, status_repo, batch_attempts)

    if maybe_report_progress is not None:
        maybe_report_progress(len(symbols_to_recompute), True)
    return failures, examples


def _print_screen_metric_na_impact(
    impacts: Sequence[_ScreenMetricImpactSummary],
    failures: Dict[str, Counter],
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
) -> None:
    print("Metric NA impact")
    if not impacts:
        print("- none")
        return
    for impact in impacts:
        print(
            f"- {impact.metric_id}: missing={len(impact.missing_symbols)} symbols, "
            f"affects={len(impact.affected_criteria)} criteria"
        )
        counter = failures.get(impact.metric_id, Counter())
        if not counter:
            continue
        for reason, count in counter.most_common():
            example = examples.get(impact.metric_id, {}).get(reason)
            if example:
                example_symbol, example_cap = example
                cap_display = (
                    _format_value(example_cap) if example_cap is not None else "N/A"
                )
                print(
                    f"    {reason}: {count} "
                    f"(example={example_symbol}, market_cap={cap_display})"
                )
            else:
                print(f"    {reason}: {count}")


def _print_screen_criterion_fallout(
    summaries: Sequence[_CriterionFailureSummary],
    total_symbols: int,
) -> None:
    print("Criterion fallout")
    if not summaries:
        print("- none")
        return
    for summary in summaries:
        print(
            f"- {summary.criterion.name}: fails={summary.fail_count}/{total_symbols}, "
            f"na_fails={summary.na_fail_count}, "
            f"threshold_fails={summary.threshold_fail_count}"
        )
        metric_details: List[str] = []
        if summary.criterion.left.metric:
            metric_details.append(f"left_metric={summary.criterion.left.metric}")
        if summary.criterion.right.metric:
            metric_details.append(f"right_metric={summary.criterion.right.metric}")
        if metric_details:
            print(f"    {', '.join(metric_details)}")
        if summary.missing_metric_symbols:
            missing_counts = sorted(
                (
                    (metric_id, len(symbols))
                    for metric_id, symbols in summary.missing_metric_symbols.items()
                ),
                key=lambda item: (-item[1], item[0]),
            )
            display = ", ".join(
                f"{metric_id}={count}" for metric_id, count in missing_counts
            )
            print(f"    missing_metrics: {display}")


def cmd_report_metric_failures(
    database: str,
    metric_ids: Optional[Sequence[str]],
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    output_csv: Optional[str],
) -> int:
    """Summarize warning reasons for metric computation failures in a canonical scope."""

    db_path = _resolve_database_path(database)
    selected_symbols, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_symbols(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )

    metric_classes = _select_metric_classes(metric_ids)
    metric_id_order = [getattr(cls, "id", cls.__name__) for cls in metric_classes]
    include_market_data = any(
        getattr(metric_cls, "uses_market_data", False) for metric_cls in metric_classes
    )
    MetricsRepository(db_path).initialize_schema()
    _initialize_metric_read_schema(db_path, include_market_data)
    fact_repo = _SchemaReadyFinancialFactsRepository(db_path)
    market_repo = _SchemaReadyMarketDataRepository(db_path)
    metrics_repo = _SchemaReadyMetricsRepository(db_path)
    status_repo = _SchemaReadyMetricComputeStatusRepository(db_path)
    availability_repo = _StatusAwareMetricsRepository(
        db_path,
        raw_metrics_repo=metrics_repo,
        status_repo=status_repo,
        facts_refresh_repo=_SchemaReadyFinancialFactsRefreshStateRepository(db_path),
        market_repo=market_repo,
    )

    failures: Dict[str, Counter] = {
        getattr(cls, "id", cls.__name__): Counter() for cls in metric_classes
    }
    totals: Dict[str, int] = {
        getattr(cls, "id", cls.__name__): 0 for cls in metric_classes
    }
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]] = {
        getattr(cls, "id", cls.__name__): {} for cls in metric_classes
    }
    market_caps: Dict[str, Optional[float]] = {
        symbol: None for symbol in selected_symbols
    }
    for symbol, snapshot in market_repo.latest_snapshots_many(
        selected_symbols,
        chunk_size=METRICS_COMPUTE_BATCH_SIZE,
    ).items():
        market_caps[symbol] = snapshot.market_cap

    for metric_cls in metric_classes:
        metric_id = getattr(metric_cls, "id", metric_cls.__name__)
        states_by_symbol = availability_repo.states_many(
            selected_symbols,
            [metric_id],
            chunk_size=METRICS_COMPUTE_BATCH_SIZE,
        )
        pending_symbols: List[str] = []
        for symbol in selected_symbols:
            state = states_by_symbol.get(symbol, {}).get(metric_id)
            if state is None:
                pending_symbols.append(symbol)
                continue
            if (
                state.status_record is None
                and state.record is not None
                and not state.stale
            ):
                continue
            if state.status_record is not None and not state.stale:
                if state.status_record.status == "failure":
                    reason = state.status_record.reason_code or "no warning emitted"
                    totals[metric_id] += 1
                    _record_metric_failure_reason(
                        failures,
                        examples,
                        market_repo,
                        market_caps,
                        metric_id=metric_id,
                        reason=reason,
                        symbol=symbol,
                    )
                    continue
                if state.record is not None:
                    continue
            pending_symbols.append(symbol)

        if not pending_symbols:
            continue

        batch_market_repo = (
            market_repo if getattr(metric_cls, "uses_market_data", False) else None
        )
        for symbol_batch in _batch_values(
            pending_symbols,
            METRICS_COMPUTE_BATCH_SIZE,
        ):
            batch_results = _compute_metric_batch_results(
                symbol_batch,
                [metric_id],
                fact_repo,
                batch_market_repo,
                suppress_metric_warnings=True,
            )
            batch_attempts: List[_MetricAttemptResult] = []
            for result in batch_results:
                batch_attempts.extend(result.attempts)
                for attempt in result.attempts:
                    if attempt.status != "failure":
                        continue
                    totals[metric_id] += 1
                    _record_metric_failure_reason(
                        failures,
                        examples,
                        market_repo,
                        market_caps,
                        metric_id=metric_id,
                        reason=attempt.reason_code or "no warning emitted",
                        symbol=attempt.symbol,
                    )
            _persist_metric_attempts(metrics_repo, status_repo, batch_attempts)

    total_symbols = len(selected_symbols)
    metric_order = sorted(
        metric_id_order,
        key=lambda current_metric_id: (
            -totals.get(current_metric_id, 0),
            current_metric_id,
        ),
    )
    scope_label = _scope_label(
        explicit_symbols,
        resolved_exchange_codes,
        "all supported tickers",
    )
    print(
        f"Metric failure reasons for {scope_label} (symbols={total_symbols}, metrics={len(metric_classes)})"
    )
    for metric_id in metric_order:
        total_failures = totals.get(metric_id, 0)
        print(f"- {metric_id}: failures={total_failures}/{total_symbols}")
        counter = failures.get(metric_id)
        if not counter:
            continue
        for reason, count in counter.most_common():
            example = examples.get(metric_id, {}).get(reason)
            if example:
                example_symbol, example_cap = example
                cap_display = (
                    _format_value(example_cap) if example_cap is not None else "N/A"
                )
                print(
                    f"    {reason}: {count} (example={example_symbol}, market_cap={cap_display})"
                )
            else:
                print(f"    {reason}: {count}")

    if output_csv:
        _write_metric_failure_report_csv(
            failures, examples, total_symbols, metric_order, output_csv
        )
        print(f"Wrote metric failure reasons to {output_csv}")
    return 0


def cmd_report_screen_failures(
    config_path: str,
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    output_csv: Optional[str],
) -> int:
    """Rank which criteria and missing metrics eliminate the most symbols."""

    db_path = _resolve_database_path(database)
    selected_symbols, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_symbols(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    selected_symbols = [symbol.upper() for symbol in selected_symbols]
    total_symbols = len(selected_symbols)
    completed_symbols = 0
    last_progress_at = time.monotonic()
    last_reported_completed = 0 if total_symbols > 0 else -1

    if total_symbols > 0:
        _print_screen_progress_bar(0, total_symbols)

    def maybe_report_progress(force: bool = False) -> None:
        nonlocal last_progress_at, last_reported_completed
        if total_symbols <= 0 or completed_symbols == last_reported_completed:
            return
        elapsed = time.monotonic() - last_progress_at
        if not force and elapsed < SCREEN_PROGRESS_INTERVAL_SECONDS:
            return
        _print_screen_progress_bar(completed_symbols, total_symbols)
        last_reported_completed = completed_symbols
        last_progress_at = time.monotonic()

    definition = load_screen(config_path)
    metric_ids = screen_metric_ids(definition)
    include_market_data = any(
        getattr(REGISTRY.get(metric_id), "uses_market_data", False)
        for metric_id in metric_ids
        if REGISTRY.get(metric_id) is not None
    )
    MetricsRepository(db_path).initialize_schema()
    _initialize_metric_read_schema(db_path, include_market_data)
    fact_repo = _SchemaReadyFinancialFactsRepository(db_path)
    market_repo = _SchemaReadyMarketDataRepository(db_path)
    metrics_repo = _StatusAwareMetricsRepository(
        db_path,
        market_repo=market_repo,
    )
    evaluation_metrics_repo = _PreloadedMetricsRepository(
        db_path,
        metrics_repo.fetch_many_for_symbols(selected_symbols, metric_ids),
    )

    criterion_summaries = [
        _CriterionFailureSummary(index=index, criterion=criterion)
        for index, criterion in enumerate(definition.criteria)
    ]
    metric_impacts: Dict[str, _ScreenMetricImpactSummary] = {
        metric_id: _ScreenMetricImpactSummary(metric_id=metric_id)
        for metric_id in metric_ids
    }
    passed_all = 0

    with suppress_console_metric_warnings(True):
        for symbol in selected_symbols:
            symbol_passed = True
            for summary in criterion_summaries:
                evaluation: CriterionEvaluation = evaluate_criterion_detail(
                    summary.criterion,
                    symbol,
                    evaluation_metrics_repo,
                    fact_repo,
                    market_repo,
                    log_missing_metrics=False,
                )
                if evaluation.passed:
                    continue
                symbol_passed = False
                summary.fail_count += 1
                if evaluation.failure_kind == "comparison_failed":
                    summary.threshold_fail_count += 1
                    continue
                summary.na_fail_count += 1
                for metric_id in evaluation.missing_metric_ids:
                    impact = metric_impacts.setdefault(
                        metric_id,
                        _ScreenMetricImpactSummary(metric_id=metric_id),
                    )
                    impact.missing_symbols.add(symbol)
                    impact.affected_criteria.add(summary.label)
                    summary.missing_metric_symbols.setdefault(metric_id, set()).add(
                        symbol
                    )
            if symbol_passed:
                passed_all += 1
            completed_symbols += 1
            maybe_report_progress(False)

        maybe_report_progress(True)

        failures, examples = _recompute_missing_screen_metrics(
            metric_impacts,
            fact_repo,
            market_repo,
            progress_interval_seconds=SCREEN_PROGRESS_INTERVAL_SECONDS,
        )

    ordered_impacts = sorted(
        (impact for impact in metric_impacts.values() if impact.missing_symbols),
        key=lambda impact: (-len(impact.missing_symbols), impact.metric_id),
    )
    ordered_criteria = sorted(
        criterion_summaries,
        key=lambda summary: (-summary.fail_count, summary.index),
    )

    scope_label = _scope_label(
        explicit_symbols,
        resolved_exchange_codes,
        "all supported tickers",
    )
    print(
        f"Screen failure analysis for {scope_label} "
        f"(symbols={total_symbols}, criteria={len(definition.criteria)}, "
        f"unique_metrics={len(metric_ids)})"
    )
    print(f"Passed all criteria: {passed_all}/{total_symbols}")
    print(
        f"Failed at least one criterion: {total_symbols - passed_all}/{total_symbols}"
    )
    _print_screen_metric_na_impact(ordered_impacts, failures, examples)
    _print_screen_criterion_fallout(ordered_criteria, total_symbols)

    if output_csv:
        _write_screen_failure_report_csv(
            ordered_impacts,
            failures,
            examples,
            output_csv,
        )
        print(f"Wrote screen failure reasons to {output_csv}")

    return 0


def _eligible_sec_filers(db_path: Path) -> List[str]:
    """Return symbols that have at least one 10-K/10-Q filing in SEC raw facts."""

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    allowed = {"10-K", "10-K/A", "10-Q", "10-Q/A"}
    eligible: set[str] = set()
    with repo._connect() as conn:
        rows = conn.execute(
            """
            SELECT provider_symbol, data
            FROM fundamentals_raw
            WHERE provider = 'SEC'
            """
        ).fetchall()
    for symbol, payload_json in rows:
        try:
            data = json.loads(payload_json)
        except Exception:
            continue
        facts = data.get("facts", {}).get("us-gaap", {}) or {}
        for detail in facts.values():
            units = detail.get("units", {}) if isinstance(detail, dict) else {}
            for entries in units.values():
                if not isinstance(entries, list):
                    continue
                for item in entries:
                    form = item.get("form")
                    if form in allowed:
                        eligible.add(symbol.upper())
                        break
                if symbol.upper() in eligible:
                    break
            if symbol.upper() in eligible:
                break
    return sorted(eligible)


def cmd_purge_us_nonfilers(database: str, apply: bool) -> int:
    """Remove SEC US supported tickers with no 10-K/10-Q filings stored in SEC facts."""

    db_path = _resolve_database_path(database)
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    us_symbols = ticker_repo.list_symbols_by_exchange("SEC", "US")

    eligible = set(_eligible_sec_filers(db_path))
    to_remove = sorted([sym for sym in us_symbols if sym.upper() not in eligible])
    if not to_remove:
        print("No US non-filers found to purge.")
        return 0

    print(f"Found {len(to_remove)} SEC US supported tickers without 10-K/10-Q filings.")
    for sym in to_remove:
        print(f"- {sym}")
    if not apply:
        print("Dry run only. Re-run with --apply to delete from supported_tickers.")
        return 0

    ticker_repo.delete_symbols("SEC", to_remove)
    print(f"Deleted {len(to_remove)} SEC US supported tickers from supported_tickers.")
    return 0


def cmd_recalc_market_cap(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
) -> int:
    """Recompute market cap values for stored market data."""

    db_path = _resolve_database_path(database)
    market_repo = MarketDataRepository(db_path)
    base_fact_repo = FinancialFactsRepository(db_path)
    selected_symbols, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_symbols(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    scope_label = _scope_label(
        explicit_symbols,
        resolved_exchange_codes,
        "all supported tickers",
    )
    print(
        f"Preparing market cap recalculation for {scope_label} "
        f"(selected={len(selected_symbols)})",
        flush=True,
    )
    print(
        f"Loading latest market data for {len(selected_symbols)} symbols",
        flush=True,
    )
    snapshots_by_symbol = market_repo.latest_snapshots_many(selected_symbols)
    symbols_with_market_data = [
        symbol for symbol in selected_symbols if symbol in snapshots_by_symbol
    ]
    if not symbols_with_market_data:
        print(f"No market data found to update for {scope_label}.")
        return 0

    total = len(symbols_with_market_data)
    print(
        f"Recomputing market cap for {total} symbols in {scope_label}",
        flush=True,
    )
    try:
        print(
            f"Loading latest share counts for {total} symbols",
            flush=True,
        )
        share_counts = base_fact_repo.latest_share_counts_many(
            symbols_with_market_data,
            security_ids_by_symbol={
                symbol: snapshots_by_symbol[symbol].security_id
                for symbol in symbols_with_market_data
            },
        )
        print(
            f"Loaded latest share counts for {len(share_counts)} symbols",
            flush=True,
        )
        pending_updates: List[Tuple[int, str, float]] = []
        updated_symbols: List[Tuple[int, str]] = []
        for idx, symbol in enumerate(symbols_with_market_data, 1):
            shares = share_counts.get(symbol)
            if shares is None or shares <= 0:
                LOGGER.warning("Skipping %s due to missing share count", symbol)
                continue
            snapshot = snapshots_by_symbol[symbol]
            pending_updates.append(
                (snapshot.security_id, snapshot.as_of, snapshot.price * shares)
            )
            updated_symbols.append((idx, symbol))
        print(
            f"Applying market cap updates for {len(pending_updates)} symbols",
            flush=True,
        )
        updated_rows = market_repo.update_market_caps_many(pending_updates)
        for idx, symbol in updated_symbols:
            print(f"[{idx}/{total}] Updated market cap for {symbol}", flush=True)
    except KeyboardInterrupt:
        return _cancel_cli_command("\nMarket cap recalculation cancelled by user.")

    print(f"Updated market cap for {updated_rows} rows in {db_path}")
    return 0


def cmd_clear_listings(database: str) -> int:
    """Delete the canonical supported_tickers catalog (legacy command alias)."""

    repo = SupportedTickerRepository(database)
    deleted = repo.clear()
    print(
        f"Deprecated command: cleared {deleted} supported_tickers rows in {database}. "
        "Use supported_tickers as the canonical catalog."
    )
    return 0


def cmd_clear_financial_facts(database: str) -> int:
    """Delete all normalized financial facts."""

    repo = FinancialFactsRepository(database)
    state_repo = FundamentalsNormalizationStateRepository(database)
    refresh_state_repo = FinancialFactsRefreshStateRepository(database)
    metric_status_repo = MetricComputeStatusRepository(database)
    with repo._connect() as conn:
        conn.execute("DROP TABLE IF EXISTS financial_facts")
        conn.execute("DROP TABLE IF EXISTS financial_facts_refresh_state")
        conn.execute("DROP TABLE IF EXISTS metric_compute_status")
        conn.execute("DROP TABLE IF EXISTS fundamentals_normalization_state")
    repo.initialize_schema()
    refresh_state_repo.initialize_schema()
    metric_status_repo.initialize_schema()
    state_repo.initialize_schema()
    print(f"Cleared financial_facts table in {database}")
    return 0


def cmd_clear_fundamentals_raw(database: str) -> int:
    """Delete all stored raw fundamentals."""

    repo = FundamentalsRepository(database)
    state_repo = FundamentalsNormalizationStateRepository(database)
    with repo._connect() as conn:
        conn.execute("DROP TABLE IF EXISTS fundamentals_raw")
        conn.execute("DROP TABLE IF EXISTS fundamentals_normalization_state")
    repo.initialize_schema()
    state_repo.initialize_schema()
    print(f"Cleared fundamentals_raw table in {database}")
    return 0


def cmd_clear_metrics(database: str) -> int:
    """Delete all computed metrics."""

    repo = MetricsRepository(database)
    status_repo = MetricComputeStatusRepository(database)
    with repo._connect() as conn:
        conn.execute("DROP TABLE IF EXISTS metrics")
        conn.execute("DROP TABLE IF EXISTS metric_compute_status")
    repo.initialize_schema()
    status_repo.initialize_schema()
    print(f"Cleared metrics table in {database}")
    return 0


def cmd_clear_market_data(database: str) -> int:
    """Delete all stored market data."""

    repo = MarketDataRepository(database)
    with repo._connect() as conn:
        conn.execute("DROP TABLE IF EXISTS market_data")
    repo.initialize_schema()
    print(f"Cleared market_data table in {database}")
    return 0


def cmd_run_screen(
    symbol: str,
    config_path: str,
    database: str,
    exchange_code: Optional[str],
) -> int:
    """Evaluate screening criteria against stored/derived metrics."""

    definition = load_screen(config_path)
    symbol_upper = symbol.strip().upper()
    if "." not in symbol_upper:
        if not exchange_code:
            raise SystemExit(
                "--exchange-code is required when symbol has no exchange suffix (e.g., AAPL.US)."
            )
        symbol_upper = _format_market_symbol(symbol_upper, exchange_code)
    requested_metric_ids = _screen_requested_metric_ids(definition)
    include_market_data = any(
        getattr(REGISTRY.get(metric_id), "uses_market_data", False)
        for metric_id in requested_metric_ids
        if REGISTRY.get(metric_id) is not None
    )
    MetricsRepository(database).initialize_schema()
    _initialize_metric_read_schema(
        _resolve_database_path(database), include_market_data
    )
    base_fact_repo = FinancialFactsRepository(database)
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(database)
    market_repo.initialize_schema()
    metrics_repo = _StatusAwareMetricsRepository(
        database,
        market_repo=_SchemaReadyMarketDataRepository(database),
    )
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()
    entity_name = entity_repo.fetch(symbol_upper) or symbol_upper
    description = entity_repo.fetch_description(symbol_upper) or "N/A"
    snapshot = market_repo.latest_snapshot(symbol_upper)
    price_label = _format_value(snapshot.price) if snapshot else "N/A"
    print(f"Entity: {entity_name}")
    print(f"Description: {description}")
    print(f"Price: {price_label}")
    results = []
    for criterion in definition.criteria:
        passed, left_value = evaluate_criterion_verbose(
            criterion, symbol_upper, metrics_repo, fact_repo, market_repo
        )
        results.append((criterion.name, passed, left_value))
    passed_all = all(flag for _, _, flag in results)
    for name, passed, value in results:
        value_display = _format_value(value) if value is not None else "N/A"
        print(f"{name}: {'PASS' if passed else 'FAIL'} (value={value_display})")
    return 0 if passed_all else 1


def cmd_run_screen_bulk(
    config_path: str,
    provider: str,
    database: str,
    output_csv: Optional[str],
    exchange_code: Optional[str],
) -> int:
    """Evaluate screening criteria for every canonical catalog ticker in scope."""

    definition = load_screen(config_path)
    provider_norm = _normalize_provider(provider)
    ticker_repo = SupportedTickerRepository(database)
    ticker_repo.initialize_schema()
    if not exchange_code:
        raise SystemExit("--exchange-code is required for bulk screening.")
    exchange_norm = exchange_code.upper()
    output_csv = output_csv or _default_screen_results_path(
        provider_norm, exchange_norm
    )
    symbols = ticker_repo.list_symbols_by_exchange(provider_norm, exchange_norm)
    if not symbols:
        raise SystemExit(
            f"No supported tickers found for provider {provider_norm} on exchange {exchange_norm}. "
            f"{_catalog_bootstrap_guidance(provider_norm)}"
        )

    requested_metric_ids = _screen_requested_metric_ids(definition)
    include_market_data = any(
        getattr(REGISTRY.get(metric_id), "uses_market_data", False)
        for metric_id in requested_metric_ids
        if REGISTRY.get(metric_id) is not None
    )
    MetricsRepository(database).initialize_schema()
    _initialize_metric_read_schema(
        _resolve_database_path(database), include_market_data
    )
    base_fact_repo = FinancialFactsRepository(database)
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(database)
    market_repo.initialize_schema()
    metrics_repo = _StatusAwareMetricsRepository(
        database,
        market_repo=_SchemaReadyMarketDataRepository(database),
    )
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()

    name_rows = ticker_repo.list_symbol_name_pairs_by_exchange(
        provider_norm, exchange_norm
    )
    universe_names = {row[0].upper(): (row[1] or row[0].upper()) for row in name_rows}
    evaluation_metrics_repo = _PreloadedMetricsRepository(
        database,
        metrics_repo.fetch_many_for_symbols(
            [symbol.upper() for symbol in symbols],
            requested_metric_ids,
        ),
    )
    passed_symbols, criterion_values, entity_labels = _evaluate_screen_scope(
        definition,
        [symbol.upper() for symbol in symbols],
        evaluation_metrics_repo,
        fact_repo,
        market_repo,
        entity_repo,
        universe_names,
        report_progress=False,
    )

    if not passed_symbols:
        print("No symbols satisfied all criteria.")
        if output_csv:
            _write_screen_csv(definition.criteria, [], {}, {}, {}, {}, {}, output_csv)
        return 1

    ordered_symbols, extra_rows = _rank_screen_passers(
        definition,
        passed_symbols,
        evaluation_metrics_repo,
        entity_repo,
    )
    _emit_screen_results(
        definition.criteria,
        ordered_symbols,
        criterion_values,
        entity_labels,
        entity_repo,
        market_repo,
        output_csv,
        extra_rows=extra_rows,
    )
    return 0


def _metadata_update_from_raw_payloads(
    eodhd_payload: Optional[Dict],
    sec_payload: Optional[Dict],
) -> Dict[str, Optional[str]]:
    entity_name = (
        _extract_entity_name_from_eodhd(eodhd_payload) if eodhd_payload else None
    )
    if entity_name is None and sec_payload is not None:
        entity_name = _extract_entity_name_from_sec(sec_payload)
    return {
        "entity_name": entity_name,
        "description": (
            _extract_entity_description_from_eodhd(eodhd_payload)
            if eodhd_payload
            else None
        ),
        "sector": (
            _extract_entity_sector_from_eodhd(eodhd_payload) if eodhd_payload else None
        ),
        "industry": (
            _extract_entity_industry_from_eodhd(eodhd_payload)
            if eodhd_payload
            else None
        ),
    }


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


def _print_screen_table(
    criteria: Sequence[Criterion],
    symbols: Sequence[str],
    values: Dict[str, Dict[str, float]],
    entity_names: Dict[str, str],
    descriptions: Dict[str, str],
    prices: Dict[str, str],
    extra_rows: Optional[Sequence[tuple[str, Dict[str, object]]]] = None,
) -> None:
    header = ["Criterion"] + list(symbols)
    rows: List[List[str]] = [header]
    rows.append(["Entity"] + [entity_names.get(symbol, symbol) for symbol in symbols])
    rows.append(
        ["Description"] + [descriptions.get(symbol, "N/A") for symbol in symbols]
    )
    rows.append(["Price"] + [prices.get(symbol, "N/A") for symbol in symbols])
    for row_name, row_values in extra_rows or ():
        row = [row_name]
        for symbol in symbols:
            value = row_values.get(symbol)
            row.append("N/A" if value is None else _format_output_cell(value))
        rows.append(row)
    for criterion in criteria:
        row = [criterion.name]
        for symbol in symbols:
            value = values.get(criterion.name, {}).get(symbol)
            row.append(_format_value(value) if value is not None else "N/A")
        rows.append(row)
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    for row in rows:
        print(" | ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))


def _format_value(value: float) -> str:
    formatted = f"{value:,.4f}".rstrip("0").rstrip(".")
    return formatted or "0"


def _format_output_cell(value: object) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _format_value(float(value))
    return str(value)


def _prepare_output_csv_path(path: str) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path


def _write_screen_csv(
    criteria: Sequence[Criterion],
    symbols: Sequence[str],
    values: Dict[str, Dict[str, float]],
    entity_names: Dict[str, str],
    descriptions: Dict[str, str],
    prices: Dict[str, str],
    price_currencies: Dict[str, str],
    path: str,
    extra_rows: Optional[Sequence[tuple[str, Dict[str, object]]]] = None,
) -> None:
    output_path = _prepare_output_csv_path(path)
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Criterion", *symbols])
        writer.writerow(
            ["Entity", *[entity_names.get(symbol, symbol) for symbol in symbols]]
        )
        writer.writerow(
            ["Description", *[descriptions.get(symbol, "N/A") for symbol in symbols]]
        )
        writer.writerow(["Price", *[prices.get(symbol, "N/A") for symbol in symbols]])
        writer.writerow(
            [
                "Price Currency",
                *[price_currencies.get(symbol, "N/A") for symbol in symbols],
            ]
        )
        for row_name, row_values in extra_rows or ():
            writer.writerow(
                [
                    row_name,
                    *[
                        ""
                        if row_values.get(symbol) is None
                        else _format_output_cell(row_values[symbol])
                        for symbol in symbols
                    ],
                ]
            )
        for criterion in criteria:
            row = [criterion.name]
            for symbol in symbols:
                value = values.get(criterion.name, {}).get(symbol)
                row.append("" if value is None else _format_value(value))
            writer.writerow(row)


def _write_fact_report_csv(report: Sequence[MetricCoverage], path: str) -> None:
    output_path = _prepare_output_csv_path(path)
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "metric_id",
                "concept",
                "missing",
                "stale",
                "fresh",
                "fully_covered",
                "total_symbols",
            ]
        )
        for entry in report:
            if not entry.concepts:
                writer.writerow(
                    [
                        entry.metric_id,
                        "",
                        0,
                        0,
                        entry.total_symbols,
                        entry.fully_covered,
                        entry.total_symbols,
                    ]
                )
                continue
            for concept in entry.concepts:
                fresh = max(entry.total_symbols - concept.missing - concept.stale, 0)
                writer.writerow(
                    [
                        entry.metric_id,
                        concept.concept,
                        concept.missing,
                        concept.stale,
                        fresh,
                        entry.fully_covered,
                        entry.total_symbols,
                    ]
                )


def cmd_refresh_fx_rates(
    database: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> int:
    """Refresh and store direct FX rates for the configured FX provider."""

    resolved_start, resolved_end, explicit_start_date = _resolve_fx_refresh_dates(
        start_date,
        end_date,
    )
    provider_name = Config().fx_provider
    if provider_name == "FRANKFURTER":
        return _cmd_refresh_fx_rates_frankfurter(
            database=database,
            start_date=resolved_start,
            end_date=resolved_end,
        )
    return _cmd_refresh_fx_rates_eodhd(
        database=database,
        start_date=resolved_start,
        end_date=resolved_end,
        explicit_start_date=explicit_start_date,
    )


def _resolve_fx_refresh_dates(
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple[date, date, bool]:
    try:
        resolved_end = date.fromisoformat(end_date) if end_date else date.today()
    except ValueError as exc:
        raise SystemExit(f"Invalid --end-date value: {end_date}") from exc
    try:
        resolved_start = date.fromisoformat(start_date) if start_date else resolved_end
    except ValueError as exc:
        raise SystemExit(f"Invalid --start-date value: {start_date}") from exc
    if resolved_start > resolved_end:
        raise SystemExit("--start-date must be on or before --end-date")
    return resolved_start, resolved_end, start_date is not None


def _parse_optional_rate_date(value: Optional[str]) -> Optional[date]:
    """Return a parsed ISO date or None for empty/invalid stored coverage."""

    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _describe_eodhd_fx_refresh_scope(
    *,
    start_date: date,
    end_date: date,
    explicit_start_date: bool,
) -> str:
    """Return a user-facing description of the requested EODHD refresh scope."""

    if explicit_start_date:
        return f"requested_range={start_date.isoformat()}..{end_date.isoformat()}"
    return (
        "mode=auto-full-history "
        f"requested_end={end_date.isoformat()} "
        f"first_backfill_start={FX_FULL_BACKFILL_START.isoformat()}"
    )


def _cmd_refresh_fx_rates_frankfurter(
    *,
    database: str,
    start_date: date,
    end_date: date,
) -> int:
    """Refresh direct FX rates using the legacy Frankfurter path."""

    db_path = _resolve_database_path(database)
    print(
        "Preparing FX refresh schema and indexes (the first run after an upgrade may take a while on large databases)...",
        flush=True,
    )
    repo = FXRatesRepository(db_path)
    service = FXService(db_path, repository=repo, provider_name="FRANKFURTER")
    provider = FrankfurterProvider()
    print(
        "Discovering FX currencies from supported_tickers, financial_facts, and market_data...",
        flush=True,
    )
    currencies = [
        code for code in repo.discover_currencies() if code != service.pivot_currency
    ]
    if not currencies:
        print("No non-pivot currencies found in the database.")
        return 0

    batch_plan: List[Tuple[date, date, List[str]]] = []
    requested_windows = 0
    fully_covered_currencies = set(currencies)
    for window_start, window_end in _split_fx_refresh_ranges(
        start_date,
        end_date,
        FX_REFRESH_MAX_DAYS_PER_REQUEST,
    ):
        covered_quotes = repo.fully_covered_quotes_for_window(
            service.provider_name,
            service.pivot_currency,
            currencies,
            window_start,
            window_end,
        )
        uncovered_quotes = [
            currency for currency in currencies if currency not in covered_quotes
        ]
        if not uncovered_quotes:
            continue
        requested_windows += 1
        for currency in uncovered_quotes:
            fully_covered_currencies.discard(currency)
        for batch in _batch_values(
            sorted(uncovered_quotes),
            FX_REFRESH_MAX_QUOTES_PER_REQUEST,
        ):
            batch_plan.append((window_start, window_end, batch))

    total_batches = len(batch_plan)
    skipped_currencies = len(fully_covered_currencies)
    print(
        "Refreshing FX rates: "
        f"provider={service.provider_name} "
        f"base={service.pivot_currency} "
        f"currencies={len(currencies)} "
        f"skipped_currencies={skipped_currencies} "
        f"date_windows={requested_windows} "
        f"requests={total_batches} "
        f"range={start_date.isoformat()}..{end_date.isoformat()}",
        flush=True,
    )
    _print_fx_progress_bar(0, total_batches)

    stored = 0
    failed_batches = 0
    completed_batches = 0
    for window_start, window_end, batch in batch_plan:
        try:
            rows = provider.fetch_rates(
                base_currency=service.pivot_currency,
                quote_currencies=batch,
                start_date=window_start,
                end_date=window_end,
            )
        except Exception as exc:
            LOGGER.warning(
                "FX refresh batch failed | provider=%s base=%s quotes=%s range=%s..%s exception=%s",
                service.provider_name,
                service.pivot_currency,
                ",".join(batch),
                window_start.isoformat(),
                window_end.isoformat(),
                exc,
            )
            failed_batches += 1
            completed_batches += 1
            _print_fx_progress_bar(completed_batches, total_batches)
            continue
        stored += repo.upsert_many(rows)
        completed_batches += 1
        _print_fx_progress_bar(completed_batches, total_batches)

    print(
        "Stored FX rates: "
        f"provider={service.provider_name} "
        f"base={service.pivot_currency} "
        f"currencies={len(currencies)} "
        f"rows={stored} "
        f"failed_batches={failed_batches} "
        f"range={start_date.isoformat()}..{end_date.isoformat()}"
    )
    return 0


def _plan_eodhd_fx_refresh_ranges(
    *,
    start_date: date,
    end_date: date,
    min_rate_date: Optional[str],
    max_rate_date: Optional[str],
    full_history_backfilled: bool,
    explicit_start_date: bool,
) -> tuple[list[tuple[date, date]], bool]:
    """Return the older/newer EODHD FX history ranges that need refresh."""

    min_covered = _parse_optional_rate_date(min_rate_date)
    max_covered = _parse_optional_rate_date(max_rate_date)
    if min_covered is None or max_covered is None:
        if explicit_start_date:
            return [(start_date, end_date)], False
        return [(FX_FULL_BACKFILL_START, end_date)], True

    ranges: list[tuple[date, date]] = []
    next_full = full_history_backfilled
    if explicit_start_date:
        if start_date < min_covered:
            older_end = min_covered - timedelta(days=1)
            if start_date <= older_end:
                ranges.append((start_date, older_end))
        if end_date > max_covered:
            newer_start = max_covered + timedelta(days=1)
            if newer_start <= end_date:
                ranges.append((newer_start, end_date))
        return ranges, next_full

    older_needed = False
    if not full_history_backfilled and FX_FULL_BACKFILL_START < min_covered:
        older_end = min_covered - timedelta(days=1)
        if FX_FULL_BACKFILL_START <= older_end:
            ranges.append((FX_FULL_BACKFILL_START, older_end))
            older_needed = True
    if end_date > max_covered:
        newer_start = max_covered + timedelta(days=1)
        if newer_start <= end_date:
            ranges.append((newer_start, end_date))
    next_full = full_history_backfilled or not older_needed
    return ranges, next_full


def _cmd_refresh_fx_rates_eodhd(
    *,
    database: str,
    start_date: date,
    end_date: date,
    explicit_start_date: bool,
) -> int:
    """Refresh direct FX rates from the EODHD FOREX catalog."""

    db_path = _resolve_database_path(database)
    print(
        "Preparing FX refresh schema and indexes (the first run after an upgrade may take a while on large databases)...",
        flush=True,
    )
    fx_repo = FXRatesRepository(db_path)
    catalog_repo = FXSupportedPairsRepository(db_path)
    state_repo = FXRefreshStateRepository(db_path)
    provider = EODHDFXProvider(api_key=_require_eodhd_key())

    print("Syncing EODHD FOREX catalog...", flush=True)
    catalog_entries = provider.list_catalog()
    catalog_repo.replace_provider_catalog(
        provider.provider_name,
        [
            FXSupportedPairRecord(
                provider=provider.provider_name,
                symbol=entry.symbol,
                canonical_symbol=entry.canonical_symbol,
                base_currency=entry.base_currency,
                quote_currency=entry.quote_currency,
                name=entry.name,
                is_alias=entry.is_alias,
                is_refreshable=entry.is_refreshable,
            )
            for entry in catalog_entries
        ],
    )
    refreshable_pairs = catalog_repo.list_refreshable(provider.provider_name)
    scope_description = _describe_eodhd_fx_refresh_scope(
        start_date=start_date,
        end_date=end_date,
        explicit_start_date=explicit_start_date,
    )
    print(
        "Refreshing FX rates: "
        f"provider={provider.provider_name} "
        f"canonical_pairs={len(refreshable_pairs)} "
        f"{scope_description}",
        flush=True,
    )
    _print_fx_progress_bar(0, len(refreshable_pairs))

    stored = 0
    skipped_pairs = 0
    failed_pairs = 0
    completed_pairs = 0
    for entry in refreshable_pairs:
        base_currency = normalize_currency_code(entry.base_currency)
        quote_currency = normalize_currency_code(entry.quote_currency)
        if base_currency is None or quote_currency is None:
            failed_pairs += 1
            completed_pairs += 1
            _print_fx_progress_bar(
                completed_pairs,
                len(refreshable_pairs),
                item_label=entry.canonical_symbol,
            )
            continue
        state = state_repo.fetch(provider.provider_name, entry.canonical_symbol)
        min_rate_date, max_rate_date = fx_repo.pair_coverage(
            provider.provider_name,
            base_currency,
            quote_currency,
        )
        refresh_ranges, next_full_history = _plan_eodhd_fx_refresh_ranges(
            start_date=start_date,
            end_date=end_date,
            min_rate_date=min_rate_date,
            max_rate_date=max_rate_date,
            full_history_backfilled=state.full_history_backfilled if state else False,
            explicit_start_date=explicit_start_date,
        )
        attempted_full_history_backfill = any(
            range_start == FX_FULL_BACKFILL_START for range_start, _ in refresh_ranges
        )
        if not refresh_ranges:
            skipped_pairs += 1
            if state is not None:
                state_repo.mark_success(
                    provider.provider_name,
                    entry.canonical_symbol,
                    min_rate_date=min_rate_date,
                    max_rate_date=max_rate_date,
                    full_history_backfilled=state.full_history_backfilled,
                )
            completed_pairs += 1
            _print_fx_progress_bar(
                completed_pairs,
                len(refreshable_pairs),
                item_label=entry.canonical_symbol,
            )
            continue

        pair_failed = False
        current_min = min_rate_date
        current_max = max_rate_date
        current_full = next_full_history
        for range_start, range_end in refresh_ranges:
            if range_start > range_end:
                continue
            try:
                rows = provider.fetch_history(
                    canonical_symbol=entry.canonical_symbol,
                    start_date=range_start,
                    end_date=range_end,
                )
            except Exception as exc:
                LOGGER.warning(
                    "EODHD FX refresh failed | provider=%s symbol=%s range=%s..%s exception=%s",
                    provider.provider_name,
                    entry.canonical_symbol,
                    range_start.isoformat(),
                    range_end.isoformat(),
                    exc,
                )
                state_repo.mark_failure(
                    provider.provider_name, entry.canonical_symbol, str(exc)
                )
                pair_failed = True
                break
            if not rows and current_min is None and current_max is None:
                error = (
                    "No FX history returned "
                    f"for {entry.canonical_symbol} in range {range_start.isoformat()}..{range_end.isoformat()}"
                )
                LOGGER.warning(error)
                state_repo.mark_failure(
                    provider.provider_name, entry.canonical_symbol, error
                )
                pair_failed = True
                break
            stored += fx_repo.upsert_many(rows)
            current_min, current_max = fx_repo.pair_coverage(
                provider.provider_name,
                base_currency,
                quote_currency,
            )
        if pair_failed:
            failed_pairs += 1
            completed_pairs += 1
            _print_fx_progress_bar(
                completed_pairs,
                len(refreshable_pairs),
                item_label=entry.canonical_symbol,
            )
            continue
        if attempted_full_history_backfill:
            current_full = True

        state_repo.mark_success(
            provider.provider_name,
            entry.canonical_symbol,
            min_rate_date=current_min,
            max_rate_date=current_max,
            full_history_backfilled=current_full,
        )
        completed_pairs += 1
        _print_fx_progress_bar(
            completed_pairs,
            len(refreshable_pairs),
            item_label=entry.canonical_symbol,
        )

    print(
        "Stored FX rates: "
        f"provider={provider.provider_name} "
        f"pairs={len(refreshable_pairs)} "
        f"rows={stored} "
        f"skipped_pairs={skipped_pairs} "
        f"failed_pairs={failed_pairs} "
        f"{scope_description}"
    )
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entrypoint used by console_scripts."""

    setup_logging()
    parser = build_parser()
    try:
        args = parser.parse_args(argv)

        if args.command == "refresh-supported-exchanges":
            return cmd_refresh_supported_exchanges(
                provider=args.provider,
                database=args.database,
            )
        if args.command == "refresh-supported-tickers":
            return cmd_refresh_supported_tickers(
                provider=args.provider,
                database=args.database,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                include_etfs=args.include_etfs,
            )
        if args.command == "refresh-fx-rates":
            return cmd_refresh_fx_rates(
                database=args.database,
                start_date=args.start_date,
                end_date=args.end_date,
            )
        if args.command == "ingest-fundamentals":
            return cmd_ingest_fundamentals_stage(
                provider=args.provider,
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                rate=args.rate,
                max_symbols=args.max_symbols,
                max_age_days=args.max_age_days,
                respect_backoff=not args.retry_failed_now,
                user_agent=args.user_agent,
                cik=args.cik,
            )
        if args.command == "report-fundamentals-progress":
            return cmd_report_fundamentals_progress(
                provider=args.provider,
                database=args.database,
                exchange_codes=args.exchange_codes,
                max_age_days=args.max_age_days,
                missing_only=args.missing_only,
            )
        if args.command == "report-market-data-progress":
            return cmd_report_market_data_progress(
                provider=args.provider,
                database=args.database,
                exchange_codes=args.exchange_codes,
                max_age_days=args.max_age_days,
            )
        if args.command == "update-market-data":
            return cmd_update_market_data_stage(
                provider=args.provider,
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                rate=args.rate,
                max_symbols=args.max_symbols,
                max_age_days=args.max_age_days,
                respect_backoff=not args.retry_failed_now,
            )
        if args.command == "normalize-fundamentals":
            return cmd_normalize_fundamentals_stage(
                provider=args.provider,
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                force=args.force,
            )
        if args.command == "clear-financial-facts":
            return cmd_clear_financial_facts(database=args.database)
        if args.command == "clear-fundamentals-raw":
            return cmd_clear_fundamentals_raw(database=args.database)
        if args.command == "clear-metrics":
            return cmd_clear_metrics(database=args.database)
        if args.command == "clear-market-data":
            return cmd_clear_market_data(database=args.database)
        if args.command == "compute-metrics":
            return cmd_compute_metrics_stage(
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                metric_ids=args.metrics,
                show_metric_warnings=args.show_metric_warnings,
                profile=args.profile,
            )
        if args.command == "report-fact-freshness":
            return cmd_report_fact_freshness(
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                metric_ids=args.metrics,
                max_age_days=args.max_age_days,
                output_csv=args.output_csv,
                show_all=args.show_all,
            )
        if args.command == "report-metric-coverage":
            return cmd_report_metric_coverage(
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                metric_ids=args.metrics,
            )
        if args.command == "report-metric-failures":
            return cmd_report_metric_failures(
                database=args.database,
                metric_ids=args.metrics,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                output_csv=args.output_csv,
            )
        if args.command == "report-screen-failures":
            return cmd_report_screen_failures(
                config_path=args.config,
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                output_csv=args.output_csv,
            )
        if args.command == "recalc-market-cap":
            return cmd_recalc_market_cap(
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
            )
        if args.command == "run-screen":
            return cmd_run_screen_stage(
                config_path=args.config,
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                output_csv=args.output_csv,
                show_metric_warnings=args.show_metric_warnings,
            )
        if args.command == "refresh-security-metadata":
            return cmd_refresh_security_metadata(
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
            )
        if args.command == "purge-us-nonfilers":
            return cmd_purge_us_nonfilers(database=args.database, apply=args.apply)

        parser.error(f"Unknown command: {args.command}")
        return 2
    except KeyboardInterrupt:
        return _cancel_cli_command("Cancelled by user.")


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    raise SystemExit(main())
