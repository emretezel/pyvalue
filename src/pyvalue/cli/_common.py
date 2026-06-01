"""Shared CLI constants, dataclasses, scope/symbol helpers, and the package logger.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import logging
import re
from pathlib import Path
from typing import (
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from pyvalue.config import Config
from pyvalue.ingestion import EODHDFundamentalsClient
from pyvalue.metrics import REGISTRY
from pyvalue.screening import (
    Criterion,
)
from pyvalue.persistence.storage import (
    IngestProgressExchange,
    IngestProgressSummary,
    MetricComputeStatusRecord,
    MetricRecord,
    SecurityListingStatusRecord,
    SecurityListingStatusRepository,
    StoredFactRow,
    StoredMetricRow,
    SupportedTicker,
    SupportedTickerRepository,
)

LOGGER = logging.getLogger("pyvalue.cli")


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
SCREEN_CONSOLE_PREVIEW_MAX_ROWS = 25
SCREEN_CONSOLE_MIN_ENTITY_WIDTH = 18
SCREEN_CONSOLE_MAX_ENTITY_WIDTH = 28
SCREEN_CONSOLE_MIN_DESCRIPTION_WIDTH = 24
SCREEN_CONSOLE_MAX_DESCRIPTION_WIDTH = 60
SCREEN_FAILURE_METRIC_LOAD_CHUNK_SIZE = 1000
SCREEN_FAILURE_PROGRESS_INTERVAL_SECONDS = 1.0
# Metric write flushes amortise SQLite fsync overhead -- batch ~200 symbols
# (~16k upsert rows at ~81 metrics each) per flush. End-of-run forced flush
# guarantees small universes still write everything; the time-based bound is
# just a liveness floor for very slow workers.
METRICS_WRITE_BATCH_SIZE = 200
METRICS_WRITE_BATCH_INTERVAL_SECONDS = 1.0
NORMALIZATION_MAX_WORKERS = 16


def _resolve_ticker_target_currency(
    database: Union[str, Path],
    symbol: str,
    payload: Optional[Dict[str, object]] = None,
    *,
    ticker_repo: Optional[SupportedTickerRepository] = None,
) -> Optional[str]:
    """Resolve the listing currency from provider/catalog metadata only."""

    del payload  # Listing currency must never fall back to fundamentals payloads.
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
    payload_hash: str
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


def _resolve_database_path(database: str) -> Path:
    """Resolve database path, falling back to repo data dir when using default name."""

    db_path = Path(database)
    if db_path.exists():
        return db_path
    if not db_path.is_absolute() and db_path.name == "pyvalue.db":
        # This module lives at src/pyvalue/cli/_common.py, so the repo root is
        # four parents up (cli -> pyvalue -> src -> <repo root>). When this code
        # lived in src/pyvalue/cli.py it was only three levels deep, hence the
        # historical parents[2]; the package split added the cli/ directory.
        repo_path = Path(__file__).resolve().parents[3] / "data" / db_path.name
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
    return "Populate provider_listing first."


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


def _reconcile_eodhd_listing_scope(
    database: str,
    *,
    provider_symbols: Optional[Sequence[str]] = None,
    exchange_codes: Optional[Sequence[str]] = None,
    security_ids: Optional[Sequence[int]] = None,
) -> List[SecurityListingStatusRecord]:
    repo = SecurityListingStatusRepository(database)
    updates = repo.reconcile_eodhd_fundamentals(
        provider_symbols=provider_symbols,
        exchange_codes=exchange_codes,
        security_ids=security_ids,
    )
    secondary_updates = [update for update in updates if not update.is_primary_listing]
    if secondary_updates:
        repo.purge_secondary_security_data(
            security_ids=[update.security_id for update in secondary_updates],
            provider_symbols=[update.provider_symbol for update in secondary_updates],
        )
    return updates


def _ensure_eodhd_listing_scope_cached(
    database: str,
    *,
    provider_symbols: Optional[Sequence[str]] = None,
    exchange_codes: Optional[Sequence[str]] = None,
    security_ids: Optional[Sequence[int]] = None,
) -> List[SecurityListingStatusRecord]:
    """Backfill only unknown listing-status values for an EODHD scope."""

    repo = SecurityListingStatusRepository(database)
    missing_provider_symbols = repo.list_missing_eodhd_provider_symbols(
        provider_symbols=provider_symbols,
        exchange_codes=exchange_codes,
        security_ids=security_ids,
    )
    if not missing_provider_symbols:
        return []
    updates = repo.reconcile_eodhd_fundamentals(
        provider_symbols=missing_provider_symbols,
    )
    secondary_updates = [update for update in updates if not update.is_primary_listing]
    if secondary_updates:
        repo.purge_secondary_security_data(
            security_ids=[update.security_id for update in secondary_updates],
            provider_symbols=[update.provider_symbol for update in secondary_updates],
        )
    return updates


def _sync_eodhd_listing_scope(
    database: str,
    *,
    listing_status_mode: Literal["full", "ensure_missing"] = "full",
    provider_symbols: Optional[Sequence[str]] = None,
    exchange_codes: Optional[Sequence[str]] = None,
    security_ids: Optional[Sequence[int]] = None,
) -> List[SecurityListingStatusRecord]:
    if listing_status_mode == "full":
        return _reconcile_eodhd_listing_scope(
            database,
            provider_symbols=provider_symbols,
            exchange_codes=exchange_codes,
            security_ids=security_ids,
        )
    if listing_status_mode == "ensure_missing":
        return _ensure_eodhd_listing_scope_cached(
            database,
            provider_symbols=provider_symbols,
            exchange_codes=exchange_codes,
            security_ids=security_ids,
        )
    raise ValueError(f"Unsupported listing_status_mode: {listing_status_mode}")


def _resolve_provider_scope_rows(
    database: str,
    provider: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    *,
    primary_only: bool = False,
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
        if primary_only and provider_norm == "EODHD":
            _reconcile_eodhd_listing_scope(
                database,
                provider_symbols=normalized_symbols,
            )
            rows = ticker_repo.list_for_provider(
                provider_norm,
                provider_symbols=normalized_symbols,
                primary_only=True,
            )
            primary_found = {row.symbol.upper() for row in rows}
            secondary = [
                symbol for symbol in normalized_symbols if symbol not in primary_found
            ]
            if secondary:
                raise SystemExit(
                    "Secondary listings are excluded for provider EODHD once raw "
                    f"fundamentals classification is available: {', '.join(secondary)}"
                )
        return rows, normalized_symbols, None

    if primary_only and provider_norm == "EODHD":
        _reconcile_eodhd_listing_scope(
            database,
            exchange_codes=exchange_filters,
        )
    rows = ticker_repo.list_for_provider(
        provider_norm,
        exchange_codes=exchange_filters,
        primary_only=primary_only and provider_norm == "EODHD",
    )
    if not rows:
        scope_label = (
            ", ".join(exchange_filters) if exchange_filters else "all supported tickers"
        )
        if primary_only and provider_norm == "EODHD":
            raise SystemExit(
                f"No primary supported tickers found for provider {provider_norm} in scope {scope_label}. "
                f"{_catalog_bootstrap_guidance(provider_norm)}"
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
    *,
    primary_only: bool = True,
    listing_status_mode: Literal["full", "ensure_missing"] = "full",
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
                f"Unsupported canonical tickers: {', '.join(missing)}. Populate provider_listing first."
            )
        if primary_only:
            _sync_eodhd_listing_scope(
                database,
                listing_status_mode=listing_status_mode,
                provider_symbols=normalized_symbols,
            )
            primary_supported = set(
                ticker_repo.list_canonical_symbols(primary_only=True)
            )
            secondary = [
                symbol
                for symbol in normalized_symbols
                if symbol not in primary_supported
            ]
            if secondary:
                raise SystemExit(
                    "Secondary listings are excluded once raw fundamentals "
                    f"classification is available: {', '.join(secondary)}"
                )
        return normalized_symbols, normalized_symbols, None

    if primary_only:
        _sync_eodhd_listing_scope(
            database,
            listing_status_mode=listing_status_mode,
            exchange_codes=exchange_filters,
        )
    canonical_symbols = ticker_repo.list_canonical_symbols(
        exchange_filters,
        primary_only=primary_only,
    )
    if not canonical_symbols:
        scope_label = (
            ", ".join(exchange_filters) if exchange_filters else "all supported tickers"
        )
        if primary_only:
            raise SystemExit(
                f"No primary canonical supported tickers found in scope {scope_label}. Populate provider_listing first."
            )
        raise SystemExit(
            f"No canonical supported tickers found in scope {scope_label}. Populate provider_listing first."
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


def _require_eodhd_key() -> str:
    api_key = Config().eodhd_api_key
    if not api_key:
        raise SystemExit(
            "EODHD API key missing. Add [eodhd].api_key to private/config.toml."
        )
    return api_key


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


class _MetricWarningCollector(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.WARNING:
            self.records.append(record)

    def clear(self) -> None:
        self.records.clear()


def _format_value(value: float) -> str:
    formatted = f"{value:,.4f}".rstrip("0").rstrip(".")
    return formatted or "0"


def _prepare_output_csv_path(path: str) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path
