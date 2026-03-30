"""Command line utilities for pyvalue.

Author: Emre Tezel
"""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import logging
import re
from threading import Lock, local
import time
from collections import Counter
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from pyvalue.config import Config
from pyvalue.ingestion import EODHDFundamentalsClient, SECCompanyFactsClient
from pyvalue.marketdata import EODHDProvider, MarketDataUpdate, PriceData
from pyvalue.marketdata.service import MarketDataService, latest_share_count
from pyvalue.metrics import REGISTRY
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS
from pyvalue.normalization import EODHDFactsNormalizer, SECFactsNormalizer
from pyvalue.reporting import MetricCoverage, compute_fact_coverage
from pyvalue.screening import (
    Criterion,
    load_screen,
    evaluate_criterion_verbose,
)
from pyvalue.logging_utils import setup_logging
from pyvalue.facts import RegionFactsRepository
from pyvalue.storage import (
    EntityMetadataRepository,
    FundamentalsRepository,
    FundamentalsFetchStateRepository,
    FinancialFactsRepository,
    IngestProgressExchange,
    IngestProgressSummary,
    MarketDataFetchStateRepository,
    MarketDataRepository,
    MetricsRepository,
    SupportedExchangeRepository,
    SupportedTicker,
    SupportedTickerRepository,
)
from pyvalue.universe import USUniverseLoader

LOGGER = logging.getLogger(__name__)
DEFAULT_SCREEN_RESULTS_PREFIX = "data/screen_results"
EODHD_ALLOWED_TICKER_TYPES = {"COMMON STOCK", "PREFERRED STOCK", "STOCK"}
EODHD_FUNDAMENTALS_CALL_COST = 10
EODHD_MARKET_DATA_CALL_COST = 1
EODHD_MARKET_DATA_BULK_CALL_COST = 100
EODHD_MAX_REQUESTS_PER_MINUTE = 1000.0
MARKET_DATA_BULK_BREAK_EVEN = 100
MARKET_DATA_BULK_WORKERS = 4
MARKET_DATA_SYMBOL_WORKERS = 16
MARKET_DATA_RATE_LIMIT_BURST = 2
MARKET_DATA_WRITE_BATCH_SIZE = 100
MARKET_DATA_WRITE_BATCH_INTERVAL_SECONDS = 0.25
MARKET_DATA_PROGRESS_INTERVAL_SECONDS = 5.0
MARKET_DATA_PROGRESS_SYMBOL_STEP = 250

_MARKET_DATA_PROVIDER_LOCAL = local()


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


class _RateLimiter:
    """Token-bucket limiter shared across market-data worker threads."""

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
    if selected != 1:
        raise SystemExit(
            "Exactly one scope selector is required: use one of "
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
        raise SystemExit("--rate must be greater than 0 for EODHD global ingestion.")
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
        scope = command_parser.add_mutually_exclusive_group(required=True)
        scope.add_argument(
            "--symbols",
            nargs="+",
            default=None,
            help="Space or comma separated list of fully qualified symbols.",
        )
        scope.add_argument(
            "--exchange-codes",
            nargs="+",
            default=None,
            help="Space or comma separated list of exchange codes.",
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
        required=True,
        choices=["SEC", "EODHD"],
        help="Fundamentals provider to use.",
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
        default=None,
        help="Only ingest symbols with older fundamentals (days) or missing data.",
    )
    ingest_fundamentals.add_argument(
        "--resume",
        action="store_true",
        help="Skip symbols that are still in backoff from prior failures.",
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
        default=7,
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
        default=7,
        help="Refresh only stale or missing market data older than this many days.",
    )
    market_data.add_argument(
        "--resume",
        action="store_true",
        help="Skip symbols that are still in backoff from prior failures.",
    )

    normalize_fundamentals = subparsers.add_parser(
        "normalize-fundamentals",
        help="Normalize stored fundamentals across the requested supported-ticker scope.",
    )
    normalize_fundamentals.add_argument(
        "--provider",
        required=True,
        choices=["SEC", "EODHD"],
        help="Fundamentals provider to normalize.",
    )
    add_scope_args(normalize_fundamentals)
    normalize_fundamentals.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
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
        default=MAX_FACT_AGE_DAYS,
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
    run_screen.add_argument("config", help="Path to screening config (YAML)")
    run_screen.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file (default: %(default)s)",
    )
    add_scope_args(run_screen)
    run_screen.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV path for passing results.",
    )

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
    if all_supported:
        exchange_list = _list_eodhd_exchange_codes(database, eodhd_client)
    else:
        if not requested_exchanges:
            raise SystemExit(
                "Use --exchange-codes or --all-supported with refresh-supported-tickers."
            )
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
    resume: bool,
) -> int:
    """Fetch EODHD fundamentals across supported tickers with quota awareness."""

    provider_norm = provider.strip().upper()
    if provider_norm != "EODHD":
        raise SystemExit(
            "ingest-fundamentals-global currently only supports provider=EODHD."
        )

    api_key = _require_eodhd_key()
    eodhd_client = EODHDFundamentalsClient(api_key=api_key)
    config = Config()
    buffer_calls = max(config.eodhd_fundamentals_daily_buffer_calls, 0)
    rate_value = _resolve_eodhd_fundamentals_rate(rate)
    requested_exchange_codes = _parse_exchange_filters(exchange_codes)
    user_meta = eodhd_client.user_metadata()
    daily_limit, used_calls, usable_requests = _eodhd_request_budget(
        user_meta, buffer_calls, EODHD_FUNDAMENTALS_CALL_COST
    )
    request_budget = usable_requests
    if max_symbols is not None:
        request_budget = min(request_budget, max_symbols)
    if request_budget <= 0:
        print(
            "No EODHD fundamentals request budget available for this run "
            f"(daily_limit={daily_limit}, used_calls={used_calls}, "
            f"buffer_calls={buffer_calls})."
        )
        return 0

    ticker_repo = SupportedTickerRepository(database)
    eligible = ticker_repo.list_eligible_for_fundamentals(
        provider=provider_norm,
        exchange_codes=sorted(requested_exchange_codes)
        if requested_exchange_codes
        else None,
        max_age_days=max_age_days,
        max_symbols=request_budget,
        resume=resume,
        missing_only=max_age_days is None,
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

    repo = FundamentalsRepository(database)
    repo.initialize_schema()
    state_repo = FundamentalsFetchStateRepository(database)
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
        f"Fetching EODHD fundamentals for {total} supported tickers across {scope_label} "
        f"at <= {rate_value:.2f} req/min "
        f"(daily_limit={daily_limit}, used_calls={used_calls}, "
        f"buffer_calls={buffer_calls}, budget_requests={request_budget})"
    )

    try:
        for idx, ticker in enumerate(eligible, 1):
            attempted += 1
            start = time.perf_counter()
            try:
                payload = eodhd_client.fetch_fundamentals(
                    ticker.symbol, exchange_code=None
                )
                general = payload.get("General") or {}
                repo.upsert(
                    "EODHD",
                    ticker.symbol,
                    payload,
                    currency=general.get("CurrencyCode") or ticker.currency,
                    exchange=ticker.exchange_code,
                )
                state_repo.mark_success("EODHD", ticker.symbol)
                processed += 1
                print(
                    f"[{idx}/{total}] Stored fundamentals for {ticker.symbol}",
                    flush=True,
                )
            except Exception as exc:  # pragma: no cover - network errors
                LOGGER.error(
                    "Failed to fetch fundamentals for %s: %s", ticker.symbol, exc
                )
                state_repo.mark_failure("EODHD", ticker.symbol, str(exc))

            elapsed = time.perf_counter() - start
            if elapsed < interval:
                time.sleep(interval - elapsed)
    except KeyboardInterrupt:
        print(f"\nCancelled after {attempted} attempted symbols.")
        return 1

    print(
        f"Stored fundamentals for {processed} of {attempted} attempted symbols in {database}"
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
        next_action = "Wait for backoff to expire or rerun without --resume"
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
    resume: bool,
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
        resume=resume,
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
        print(f"\nCancelled after {attempted} attempted symbols.")
        return 1

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
        next_action = "Wait for backoff to expire or rerun without --resume"
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
    resume: bool,
    user_agent: Optional[str],
    cik: Optional[str],
) -> int:
    """Unified fundamentals ingestion over symbol, exchange, or full supported scope."""

    db_path = _resolve_database_path(database)
    provider_norm = _normalize_provider(provider)
    scope_rows, symbol_filters, resolved_exchange_codes = _resolve_provider_scope_rows(
        str(db_path),
        provider_norm,
        symbols,
        exchange_codes,
        all_supported,
    )
    ticker_repo = SupportedTickerRepository(db_path)
    scope_label = _scope_label(symbol_filters, resolved_exchange_codes)

    if provider_norm == "SEC":
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
            resume=resume,
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
            print(f"\nCancelled after {processed} of {total} symbols.")
            return 1
        print(f"Stored company facts for {processed} symbols in {db_path}")
        return 0

    api_key = _require_eodhd_key()
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
        print(
            "No EODHD fundamentals request budget available for this run "
            f"(daily_limit={daily_limit}, used_calls={used_calls}, "
            f"buffer_calls={buffer_calls})."
        )
        return 0
    eligible = ticker_repo.list_eligible_for_fundamentals(
        provider=provider_norm,
        exchange_codes=resolved_exchange_codes,
        max_age_days=max_age_days,
        max_symbols=request_budget,
        resume=resume,
        missing_only=max_age_days is None,
        provider_symbols=symbol_filters,
    )
    if not eligible:
        print(
            f"No eligible supported tickers found for {scope_label}. "
            "Refresh supported tickers first or relax freshness filters."
        )
        return 0

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    state_repo = FundamentalsFetchStateRepository(db_path)
    state_repo.initialize_schema()
    interval = 60.0 / rate_value
    total = len(eligible)
    processed = 0
    attempted = 0
    print(
        f"Fetching EODHD fundamentals for {total} supported tickers across {scope_label} "
        f"at <= {rate_value:.2f} req/min "
        f"(daily_limit={daily_limit}, used_calls={used_calls}, "
        f"buffer_calls={buffer_calls}, budget_requests={request_budget})"
    )
    try:
        for idx, ticker in enumerate(eligible, 1):
            attempted += 1
            start = time.perf_counter()
            try:
                payload = eodhd_client.fetch_fundamentals(
                    ticker.symbol, exchange_code=None
                )
                general = payload.get("General") or {}
                repo.upsert(
                    "EODHD",
                    ticker.symbol,
                    payload,
                    currency=general.get("CurrencyCode") or ticker.currency,
                    exchange=ticker.exchange_code,
                )
                state_repo.mark_success("EODHD", ticker.symbol)
                processed += 1
                print(
                    f"[{idx}/{total}] Stored fundamentals for {ticker.symbol}",
                    flush=True,
                )
            except Exception as exc:  # pragma: no cover - network errors
                LOGGER.error(
                    "Failed to fetch fundamentals for %s: %s", ticker.symbol, exc
                )
                state_repo.mark_failure("EODHD", ticker.symbol, str(exc))
            elapsed = time.perf_counter() - start
            if elapsed < interval:
                time.sleep(interval - elapsed)
    except KeyboardInterrupt:
        print(f"\nCancelled after {attempted} attempted symbols.")
        return 1
    print(
        f"Stored fundamentals for {processed} of {attempted} attempted symbols in {db_path}"
    )
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
    resume: bool,
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
        resume=resume,
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

    try:
        if plan.bulk_tasks:
            with ThreadPoolExecutor(
                max_workers=min(MARKET_DATA_BULK_WORKERS, len(plan.bulk_tasks))
            ) as executor:
                bulk_futures = {
                    executor.submit(
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
            with ThreadPoolExecutor(
                max_workers=min(MARKET_DATA_SYMBOL_WORKERS, len(symbol_tickers))
            ) as executor:
                symbol_futures = {
                    executor.submit(
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
        maybe_flush(force=True)
        print(f"\nCancelled after {processed + failed} completed symbols.")
        return 1

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
            database=str(db_path), symbols=selected_symbols
        )
    if provider_norm == "EODHD":
        return cmd_normalize_eodhd_fundamentals_bulk(
            database=str(db_path),
            symbols=selected_symbols,
        )
    raise SystemExit(f"Unsupported provider: {provider}")


def cmd_compute_metrics_stage(
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    metric_ids: Optional[Sequence[str]],
) -> int:
    """Unified metric computation over symbol, exchange, or full supported scope."""

    db_path = _resolve_database_path(database)
    canonical_symbols, _, _ = _resolve_canonical_scope_symbols(
        str(db_path),
        symbols,
        exchange_codes,
        all_supported,
    )
    base_fact_repo = FinancialFactsRepository(db_path)
    fact_repo = RegionFactsRepository(base_fact_repo)
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    ids_to_compute = list(metric_ids) if metric_ids else list(REGISTRY.keys())
    if not ids_to_compute:
        raise SystemExit("No metrics specified.")

    total_symbols = len(canonical_symbols)
    print(
        f"Computing metrics for {total_symbols} symbols ({len(ids_to_compute)} metrics each)"
    )
    try:
        for idx, symbol in enumerate(canonical_symbols, 1):
            computed = 0
            for metric_id in ids_to_compute:
                metric_cls = REGISTRY.get(metric_id)
                if metric_cls is None:
                    raise SystemExit(f"Unknown metric id: {metric_id}")
                metric = metric_cls()
                try:
                    if getattr(metric, "uses_market_data", False):
                        result = metric.compute(symbol, fact_repo, market_repo)
                    else:
                        result = metric.compute(symbol, fact_repo)
                except Exception as exc:  # pragma: no cover - metric errors
                    LOGGER.error("Metric %s failed for %s: %s", metric_id, symbol, exc)
                    continue
                if result is None:
                    LOGGER.warning(
                        "Metric %s could not be computed for %s", metric_id, symbol
                    )
                    continue
                metrics_repo.upsert(
                    result.symbol, result.metric_id, result.value, result.as_of
                )
                computed += 1
            print(
                f"[{idx}/{total_symbols}] Computed {computed} metrics for {symbol}",
                flush=True,
            )
    except KeyboardInterrupt:
        print("\nMetric computation cancelled by user.")
        return 1
    print(f"Computed metrics for {total_symbols} symbols in {db_path}")
    return 0


def cmd_run_screen_stage(
    config_path: str,
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    output_csv: Optional[str],
) -> int:
    """Unified screen evaluation over symbol, exchange, or full supported scope."""

    db_path = _resolve_database_path(database)
    canonical_symbols, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_symbols(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    definition = load_screen(config_path)
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    base_fact_repo = FinancialFactsRepository(db_path)
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()

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
    entity_labels: Dict[str, str] = {}
    passed_symbols: List[str] = []
    criterion_values: Dict[str, Dict[str, float]] = {
        c.name: {} for c in definition.criteria
    }
    for symbol in canonical_symbols:
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

    if not passed_symbols:
        print("No symbols satisfied all criteria.")
        if output_csv:
            _write_screen_csv(definition.criteria, [], {}, {}, {}, {}, {}, output_csv)
        return 1

    selected_names = {
        symbol: entity_labels.get(symbol, symbol) for symbol in passed_symbols
    }
    selected_descriptions: Dict[str, str] = {}
    selected_prices: Dict[str, str] = {}
    selected_price_currencies: Dict[str, str] = {}
    for symbol in passed_symbols:
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
        definition.criteria,
        passed_symbols,
        criterion_values,
        selected_names,
        selected_descriptions,
        selected_prices,
    )
    if output_csv:
        _write_screen_csv(
            definition.criteria,
            passed_symbols,
            criterion_values,
            selected_names,
            selected_descriptions,
            selected_prices,
            selected_price_currencies,
            output_csv,
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
        resume=False,
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
    resume: bool,
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
            print(f"\\nCancelled after {processed} of {total} symbols.")
            return 1

        print(f"Stored company facts for {processed} symbols in {database}")
        return 0
    if provider_norm == "EODHD":
        rate_value = rate if rate is not None else 600.0
        exchange_norm = exchange_code.upper()
        ticker_repo = SupportedTickerRepository(database)
        tickers = ticker_repo.list_eligible_for_fundamentals(
            provider=provider_norm,
            exchange_codes=[exchange_norm],
            max_age_days=max_age_days,
            max_symbols=max_symbols,
            resume=resume,
            missing_only=False,
        )
        if not tickers:
            print(
                f"No eligible supported tickers found for exchange {exchange_norm}. "
                "Run refresh-supported-tickers first."
            )
            return 0

        api_key = _require_eodhd_key()
        eodhd_client = EODHDFundamentalsClient(api_key=api_key)
        repo = FundamentalsRepository(database)
        repo.initialize_schema()
        state_repo = FundamentalsFetchStateRepository(database)
        state_repo.initialize_schema()

        interval = 60.0 / rate_value if rate_value and rate_value > 0 else 0.0
        total = len(tickers)
        processed = 0
        print(
            f"Fetching EODHD fundamentals for {total} supported tickers on {exchange_norm} "
            f"at <= {rate_value:.2f} per minute"
        )

        try:
            for idx, ticker in enumerate(tickers, 1):
                start = time.perf_counter()
                try:
                    payload = eodhd_client.fetch_fundamentals(
                        ticker.symbol, exchange_code=None
                    )
                    general = payload.get("General") or {}
                    repo.upsert(
                        "EODHD",
                        ticker.symbol,
                        payload,
                        currency=general.get("CurrencyCode") or ticker.currency,
                        exchange=ticker.exchange_code,
                    )
                    state_repo.mark_success("EODHD", ticker.symbol)
                    processed += 1
                    print(
                        f"[{idx}/{total}] Stored fundamentals for {ticker.symbol}",
                        flush=True,
                    )
                except Exception as exc:  # pragma: no cover - network errors
                    LOGGER.error(
                        "Failed to fetch fundamentals for %s: %s",
                        ticker.symbol,
                        exc,
                    )
                    state_repo.mark_failure("EODHD", ticker.symbol, str(exc))

                elapsed = time.perf_counter() - start
                if interval > 0 and elapsed < interval:
                    time.sleep(interval - elapsed)
        except KeyboardInterrupt:
            print(f"\nCancelled after {processed} of {total} symbols.")
            return 1

        print(f"Stored fundamentals for {processed} symbols in {database}")
        return 0
    raise SystemExit(f"Unsupported provider: {provider}")


def cmd_normalize_us_facts(symbol: str, database: str) -> int:
    """Normalize previously ingested SEC facts for downstream metrics."""

    symbol = _qualify_symbol(symbol, exchange="US")
    fund_repo = FundamentalsRepository(database)
    fund_repo.initialize_schema()
    payload = fund_repo.fetch("SEC", symbol.upper())
    if payload is None:
        raise SystemExit(
            f"No raw SEC payload found for {symbol}. Run ingest-fundamentals --provider SEC before normalization."
        )
    normalizer = SECFactsNormalizer()
    records = normalizer.normalize(payload, symbol=symbol.upper())

    fact_repo = FinancialFactsRepository(database)
    fact_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()
    entity_name = payload.get("entityName")
    if entity_name:
        entity_repo.upsert(symbol.upper(), entity_name)
    stored = fact_repo.replace_facts(symbol.upper(), records)
    print(f"Stored {stored} normalized facts for {symbol.upper()} in {database}")
    return 0


def cmd_normalize_us_facts_bulk(
    database: str, symbols: Optional[Sequence[str]] = None
) -> int:
    """Normalize raw SEC facts for every stored ticker."""

    fund_repo = FundamentalsRepository(database)
    fund_repo.initialize_schema()
    normalization_repo = FinancialFactsRepository(database)
    normalization_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()

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

    normalizer = SECFactsNormalizer()
    total = len(symbols)
    print(f"Normalizing SEC facts for {total} symbols")
    try:
        for idx, symbol in enumerate(symbols, 1):
            payload = fund_repo.fetch("SEC", symbol)
            if payload is None:
                continue
            records = normalizer.normalize(payload, symbol=symbol)
            entity_name = payload.get("entityName")
            if entity_name:
                entity_repo.upsert(symbol, entity_name)
            stored = normalization_repo.replace_facts(symbol, records)
            print(
                f"[{idx}/{total}] Stored {stored} normalized facts for {symbol}",
                flush=True,
            )
    except KeyboardInterrupt:
        print("\nBulk normalization cancelled by user.")
        return 1

    print(f"Normalized SEC facts for {total} symbols into {database}")
    return 0


def _extract_entity_name_from_eodhd(payload: Dict) -> Optional[str]:
    general = payload.get("General") or {}
    return general.get("Name") or general.get("Code")


def _extract_entity_description_from_eodhd(payload: Dict) -> Optional[str]:
    general = payload.get("General") or {}
    return general.get("Description")


def cmd_normalize_eodhd_fundamentals(symbol: str, database: str) -> int:
    """Normalize stored EODHD fundamentals for downstream metrics."""

    fund_repo = FundamentalsRepository(database)
    payload = fund_repo.fetch("EODHD", symbol.upper())
    if payload is None:
        raise SystemExit(
            f"No EODHD fundamentals found for {symbol}. Run ingest-fundamentals --provider EODHD first."
        )

    normalizer = EODHDFactsNormalizer()
    records = normalizer.normalize(payload, symbol=symbol.upper())

    fact_repo = FinancialFactsRepository(database)
    fact_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()
    entity_name = _extract_entity_name_from_eodhd(payload)
    entity_description = _extract_entity_description_from_eodhd(payload)
    if entity_name or entity_description:
        entity_repo.upsert(symbol.upper(), entity_name, description=entity_description)

    stored = fact_repo.replace_facts(symbol.upper(), records)
    print(f"Stored {stored} normalized facts for {symbol.upper()} in {database}")
    return 0


def cmd_normalize_eodhd_fundamentals_bulk(
    database: str, symbols: Optional[Sequence[str]] = None
) -> int:
    """Normalize all stored EODHD fundamentals."""

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

    normalizer = EODHDFactsNormalizer()
    fact_repo = FinancialFactsRepository(database)
    fact_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()

    total = len(symbols)
    print(f"Normalizing EODHD fundamentals for {total} symbols")
    try:
        for idx, symbol in enumerate(symbols, 1):
            payload = fund_repo.fetch("EODHD", symbol)
            if payload is None:
                continue
            records = normalizer.normalize(payload, symbol=symbol)
            entity_name = _extract_entity_name_from_eodhd(payload)
            entity_description = _extract_entity_description_from_eodhd(payload)
            if entity_name or entity_description:
                entity_repo.upsert(symbol, entity_name, description=entity_description)
            stored = fact_repo.replace_facts(symbol, records)
            print(
                f"[{idx}/{total}] Stored {stored} normalized facts for {symbol}",
                flush=True,
            )
    except KeyboardInterrupt:
        print("\nBulk normalization cancelled by user.")
        return 1

    print(f"Normalized EODHD fundamentals for {total} symbols into {database}")
    return 0


def cmd_normalize_fundamentals(
    provider: str,
    symbol: str,
    database: str,
    exchange_code: Optional[str],
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
        return cmd_normalize_us_facts(symbol=symbol, database=database)
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
        return cmd_normalize_eodhd_fundamentals(symbol=qualified, database=database)
    raise SystemExit(f"Unsupported provider: {provider}")


def cmd_normalize_fundamentals_bulk(
    provider: str, database: str, exchange_code: Optional[str]
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
        return cmd_normalize_us_facts_bulk(database=database, symbols=symbols)
    if provider_norm == "EODHD":
        return cmd_normalize_eodhd_fundamentals_bulk(database=database, symbols=symbols)
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
    resume: bool,
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
        resume=resume,
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
        print(f"\nCancelled after {processed} of {total} symbols.")
        return 1

    print(f"Stored market data for {processed} symbols in {database}")
    return 0


def cmd_compute_metrics(
    symbol: str,
    metric_ids: Sequence[str],
    database: str,
    run_all: bool,
    exchange_code: Optional[str],
) -> int:
    """Compute one or more metrics and store the results."""

    db_path = _resolve_database_path(database)
    base_fact_repo = FinancialFactsRepository(db_path)
    fact_repo = RegionFactsRepository(base_fact_repo)
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    computed = 0
    symbol_upper = symbol.strip().upper()
    if "." not in symbol_upper:
        if not exchange_code:
            raise SystemExit(
                "--exchange-code is required when symbol has no exchange suffix (e.g., AAPL.US)."
            )
        symbol_upper = _format_market_symbol(symbol_upper, exchange_code)
    market_repo: Optional[MarketDataRepository] = None
    ids_to_compute = list(REGISTRY.keys()) if run_all else list(metric_ids)
    for metric_id in ids_to_compute:
        metric_cls = REGISTRY.get(metric_id)
        if metric_cls is None:
            raise SystemExit(f"Unknown metric id: {metric_id}")
        metric = metric_cls()
        if getattr(metric, "uses_market_data", False):
            if market_repo is None:
                market_repo = MarketDataRepository(db_path)
                market_repo.initialize_schema()
            result = metric.compute(symbol_upper, fact_repo, market_repo)
        else:
            result = metric.compute(symbol_upper, fact_repo)
        if result is None:
            LOGGER.warning(
                "Metric %s could not be computed for %s", metric_id, symbol_upper
            )
            continue
        metrics_repo.upsert(result.symbol, result.metric_id, result.value, result.as_of)
        computed += 1
    print(f"Computed {computed} metrics for {symbol_upper} in {database}")
    return 0


def cmd_compute_metrics_bulk(
    provider: str,
    database: str,
    metric_ids: Optional[Sequence[str]],
    exchange_code: Optional[str],
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

    base_fact_repo = FinancialFactsRepository(db_path)
    fact_repo = RegionFactsRepository(base_fact_repo)
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()

    ids_to_compute = list(metric_ids) if metric_ids else list(REGISTRY.keys())
    if not ids_to_compute:
        raise SystemExit("No metrics specified.")

    total_symbols = len(symbols)
    print(
        f"Computing metrics for {total_symbols} symbols ({len(ids_to_compute)} metrics each)"
    )

    try:
        for idx, symbol in enumerate(symbols, 1):
            symbol_upper = symbol.upper()
            computed = 0
            for metric_id in ids_to_compute:
                metric_cls = REGISTRY.get(metric_id)
                if metric_cls is None:
                    LOGGER.warning("Unknown metric id: %s", metric_id)
                    continue
                metric = metric_cls()
                try:
                    if getattr(metric, "uses_market_data", False):
                        result = metric.compute(symbol_upper, fact_repo, market_repo)
                    else:
                        result = metric.compute(symbol_upper, fact_repo)
                except Exception as exc:  # pragma: no cover - metric errors
                    LOGGER.error(
                        "Metric %s failed for %s: %s", metric_id, symbol_upper, exc
                    )
                    continue
                if result is None:
                    LOGGER.warning(
                        "Metric %s could not be computed for %s",
                        metric_id,
                        symbol_upper,
                    )
                    continue
                metrics_repo.upsert(
                    result.symbol, result.metric_id, result.value, result.as_of
                )
                computed += 1
            print(
                f"[{idx}/{total_symbols}] Computed {computed} metrics for {symbol_upper}",
                flush=True,
            )
    except KeyboardInterrupt:
        print("\nBulk metric computation cancelled by user.")
        return 1

    print(f"Computed metrics for {total_symbols} symbols in {database}")
    return 0


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
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.error(
                    "Metric %s failed for %s: %s",
                    getattr(metric_cls, "id", metric_cls.__name__),
                    symbol,
                    exc,
                )
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


def _write_metric_failure_report_csv(
    failures: Dict[str, Counter],
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]],
    total_symbols: int,
    metric_order: Sequence[str],
    path: str,
) -> None:
    with open(path, "w", newline="") as handle:
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
    base_fact_repo = FinancialFactsRepository(db_path)
    base_fact_repo.initialize_schema()
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()

    failures: Dict[str, Counter] = {
        getattr(cls, "id", cls.__name__): Counter() for cls in metric_classes
    }
    totals: Dict[str, int] = {
        getattr(cls, "id", cls.__name__): 0 for cls in metric_classes
    }
    examples: Dict[str, Dict[str, tuple[str, Optional[float]]]] = {
        getattr(cls, "id", cls.__name__): {} for cls in metric_classes
    }
    market_caps: Dict[str, Optional[float]] = {}
    handler = _MetricWarningCollector()
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    try:
        for symbol in selected_symbols:
            symbol_upper = symbol.upper()
            for metric_cls in metric_classes:
                metric_id = getattr(metric_cls, "id", metric_cls.__name__)
                handler.clear()
                metric = metric_cls()
                try:
                    if getattr(metric, "uses_market_data", False):
                        result = metric.compute(symbol_upper, fact_repo, market_repo)
                    else:
                        result = metric.compute(symbol_upper, fact_repo)
                except Exception as exc:  # pragma: no cover - defensive
                    reason = f"exception: {exc.__class__.__name__}"
                    failures[metric_id][reason] += 1
                    totals[metric_id] += 1
                    continue
                if result is None:
                    reason = _format_failure_reason(handler.records, symbol_upper)
                    failures[metric_id][reason] += 1
                    totals[metric_id] += 1
                    cap = market_caps.get(symbol_upper)
                    if symbol_upper not in market_caps:
                        snapshot = market_repo.latest_snapshot(symbol_upper)
                        cap = snapshot.market_cap if snapshot else None
                        market_caps[symbol_upper] = cap
                    current = examples[metric_id].get(reason)
                    if current is None:
                        examples[metric_id][reason] = (symbol_upper, cap)
                    else:
                        current_cap = current[1]
                        if cap is not None and (
                            current_cap is None or cap > current_cap
                        ):
                            examples[metric_id][reason] = (symbol_upper, cap)
    finally:
        root_logger.removeHandler(handler)

    total_symbols = len(selected_symbols)
    metric_order = sorted(
        totals.keys(), key=lambda metric_id: (-totals.get(metric_id, 0), metric_id)
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
    market_repo.initialize_schema()
    base_fact_repo = FinancialFactsRepository(db_path)
    base_fact_repo.initialize_schema()
    fact_repo = RegionFactsRepository(base_fact_repo)
    selected_symbols, explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_symbols(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    symbols_with_market_data = [
        symbol
        for symbol in selected_symbols
        if market_repo.latest_snapshot(symbol) is not None
    ]
    if not symbols_with_market_data:
        scope_label = _scope_label(
            explicit_symbols,
            resolved_exchange_codes,
            "all supported tickers",
        )
        print(f"No market data found to update for {scope_label}.")
        return 0

    total = len(symbols_with_market_data)
    updated_rows = 0
    print(
        f"Recomputing market cap for {total} symbols in "
        f"{_scope_label(explicit_symbols, resolved_exchange_codes, 'all supported tickers')}"
    )
    try:
        for idx, symbol in enumerate(symbols_with_market_data, 1):
            shares = latest_share_count(symbol, fact_repo)
            if shares is None or shares <= 0:
                LOGGER.warning("Skipping %s due to missing share count", symbol)
                continue
            snapshot = market_repo.latest_snapshot(symbol)
            if snapshot is None:
                continue
            updated_rows += market_repo.update_market_cap(
                symbol, snapshot.price * shares
            )
            print(f"[{idx}/{total}] Updated market cap for {symbol}", flush=True)
    except KeyboardInterrupt:
        print("\nMarket cap recalculation cancelled by user.")
        return 1

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
    with repo._connect() as conn:
        conn.execute("DROP TABLE IF EXISTS financial_facts")
    repo.initialize_schema()
    print(f"Cleared financial_facts table in {database}")
    return 0


def cmd_clear_fundamentals_raw(database: str) -> int:
    """Delete all stored raw fundamentals."""

    repo = FundamentalsRepository(database)
    with repo._connect() as conn:
        conn.execute("DROP TABLE IF EXISTS fundamentals_raw")
    repo.initialize_schema()
    print(f"Cleared fundamentals_raw table in {database}")
    return 0


def cmd_clear_metrics(database: str) -> int:
    """Delete all computed metrics."""

    repo = MetricsRepository(database)
    with repo._connect() as conn:
        conn.execute("DROP TABLE IF EXISTS metrics")
    repo.initialize_schema()
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
    metrics_repo = MetricsRepository(database)
    metrics_repo.initialize_schema()
    base_fact_repo = FinancialFactsRepository(database)
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(database)
    market_repo.initialize_schema()
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

    metrics_repo = MetricsRepository(database)
    metrics_repo.initialize_schema()
    base_fact_repo = FinancialFactsRepository(database)
    fact_repo = RegionFactsRepository(base_fact_repo)
    market_repo = MarketDataRepository(database)
    market_repo.initialize_schema()
    entity_repo = EntityMetadataRepository(database)
    entity_repo.initialize_schema()

    name_rows = ticker_repo.list_symbol_name_pairs_by_exchange(
        provider_norm, exchange_norm
    )
    universe_names = {row[0].upper(): (row[1] or row[0].upper()) for row in name_rows}
    entity_labels: Dict[str, str] = {}
    passed_symbols: List[str] = []
    criterion_values: Dict[str, Dict[str, float]] = {
        c.name: {} for c in definition.criteria
    }

    for symbol in symbols:
        symbol_upper = symbol.upper()
        symbol_passed = True
        per_symbol_values: Dict[str, float] = {}
        label = entity_labels.get(symbol_upper)
        if label is None:
            label = (
                entity_repo.fetch(symbol_upper)
                or universe_names.get(symbol_upper)
                or symbol_upper
            )
            entity_labels[symbol_upper] = label
        for criterion in definition.criteria:
            passed, left_value = evaluate_criterion_verbose(
                criterion, symbol_upper, metrics_repo, fact_repo, market_repo
            )
            if not passed or left_value is None:
                symbol_passed = False
                break
            per_symbol_values[criterion.name] = left_value
        if symbol_passed:
            passed_symbols.append(symbol_upper)
            for criterion in definition.criteria:
                criterion_values[criterion.name][symbol_upper] = per_symbol_values[
                    criterion.name
                ]

    if not passed_symbols:
        print("No symbols satisfied all criteria.")
        if output_csv:
            _write_screen_csv(definition.criteria, [], {}, {}, {}, {}, {}, output_csv)
        return 1

    selected_names = {
        symbol: entity_labels.get(symbol, symbol) for symbol in passed_symbols
    }
    selected_descriptions: Dict[str, str] = {}
    selected_prices: Dict[str, str] = {}
    selected_price_currencies: Dict[str, str] = {}
    for symbol in passed_symbols:
        description = entity_repo.fetch_description(symbol)
        selected_descriptions[symbol] = description if description else "N/A"
        snapshot = market_repo.latest_snapshot(symbol)
        if snapshot:
            selected_prices[symbol] = _format_value(snapshot.price)
            selected_price_currencies[symbol] = snapshot.currency or "N/A"
        else:
            selected_prices[symbol] = "N/A"
            selected_price_currencies[symbol] = "N/A"
    _print_screen_table(
        definition.criteria,
        passed_symbols,
        criterion_values,
        selected_names,
        selected_descriptions,
        selected_prices,
    )
    if output_csv:
        _write_screen_csv(
            definition.criteria,
            passed_symbols,
            criterion_values,
            selected_names,
            selected_descriptions,
            selected_prices,
            selected_price_currencies,
            output_csv,
        )
    return 0


def _print_screen_table(
    criteria: Sequence[Criterion],
    symbols: Sequence[str],
    values: Dict[str, Dict[str, float]],
    entity_names: Dict[str, str],
    descriptions: Dict[str, str],
    prices: Dict[str, str],
) -> None:
    header = ["Criterion"] + list(symbols)
    rows: List[List[str]] = [header]
    rows.append(["Entity"] + [entity_names.get(symbol, symbol) for symbol in symbols])
    rows.append(
        ["Description"] + [descriptions.get(symbol, "N/A") for symbol in symbols]
    )
    rows.append(["Price"] + [prices.get(symbol, "N/A") for symbol in symbols])
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


def _write_screen_csv(
    criteria: Sequence[Criterion],
    symbols: Sequence[str],
    values: Dict[str, Dict[str, float]],
    entity_names: Dict[str, str],
    descriptions: Dict[str, str],
    prices: Dict[str, str],
    price_currencies: Dict[str, str],
    path: str,
) -> None:
    with open(path, "w", newline="") as handle:
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
        for criterion in criteria:
            row = [criterion.name]
            for symbol in symbols:
                value = values.get(criterion.name, {}).get(symbol)
                row.append("" if value is None else _format_value(value))
            writer.writerow(row)


def _write_fact_report_csv(report: Sequence[MetricCoverage], path: str) -> None:
    with open(path, "w", newline="") as handle:
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


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entrypoint used by console_scripts."""

    setup_logging()
    parser = build_parser()
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
            resume=args.resume,
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
            resume=args.resume,
        )
    if args.command == "normalize-fundamentals":
        return cmd_normalize_fundamentals_stage(
            provider=args.provider,
            database=args.database,
            symbols=args.symbols,
            exchange_codes=args.exchange_codes,
            all_supported=args.all_supported,
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
        )
    if args.command == "purge-us-nonfilers":
        return cmd_purge_us_nonfilers(database=args.database, apply=args.apply)

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    raise SystemExit(main())
