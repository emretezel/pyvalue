"""Tests for CLI ingestion and metric commands.

Author: Emre Tezel
"""

import logging
import multiprocessing as mp
import sqlite3
import threading
import time
import concurrent.futures.thread as thread_futures
from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import pytest
import requests

from pyvalue import cli
from pyvalue.cli._common import _MetricWarningCollector, _PreparedFundamentalsRun
from pyvalue.cli.ingest import _run_eodhd_fundamentals_ingestion
from cli_test_helpers import patch_cli
from conftest import (
    fundamentals_payload_exists,
    normalization_state_exists,
    resolve_listing_id,
    seed_exchange,
    seed_facts,
    seed_metric,
    seed_metric_status,
    seed_normalization_success,
    seed_price,
    seed_raw_fundamentals,
    seed_security_metadata,
    seed_supported_listings,
)
from pyvalue.currency import MetricUnitKind
from pyvalue.facts import RegionFactsRepository
from pyvalue.marketdata.service import MarketDataService
from pyvalue.metrics import REGISTRY
from pyvalue.metrics.base import MetricCurrencyInvariantError, MetricResult
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS
from pyvalue.persistence.storage.base import SQLiteStore
from pyvalue.persistence.storage import (
    ExchangeProviderRepository,
    FinancialFactsRefreshStateRepository,
    FundamentalsRepository,
    FundamentalsFetchStateRepository,
    FinancialFactsRepository,
    FactRecord,
    IdKeyedStoredMetricRow,
    MarketDataFetchStateRepository,
    MarketDataRepository,
    MetricComputeStatusRecord,
    MetricComputeStatusRepository,
    MetricRecord,
    MetricsRepository,
    MetricsWriteSession,
    SecurityMetadataCandidate,
    SecurityRepository,
    SupportedTicker,
    SupportedTickerRepository,
)
from pyvalue.universe import Listing
from pyvalue.marketdata import MarketDataUpdate, PriceData


def _security_name(db_path: Path, symbol: str) -> Optional[str]:
    security = SecurityRepository(db_path).fetch(resolve_listing_id(db_path, symbol))
    return security.entity_name if security is not None else None


def _security_description(db_path: Path, symbol: str) -> Optional[str]:
    security = SecurityRepository(db_path).fetch(resolve_listing_id(db_path, symbol))
    return security.description if security is not None else None


def _security_sector(db_path: Path, symbol: str) -> Optional[str]:
    security = SecurityRepository(db_path).fetch(resolve_listing_id(db_path, symbol))
    return security.sector if security is not None else None


def _security_industry(db_path: Path, symbol: str) -> Optional[str]:
    security = SecurityRepository(db_path).fetch(resolve_listing_id(db_path, symbol))
    return security.industry if security is not None else None


# Placeholder listing id for tests whose fake fact source ignores the id it is
# given (it serves canned facts regardless): the metric layer keys on
# ``listing_id: int``, so these call sites still need *an* int to pass through.
# Tests that depend on the id matching a stored row resolve the real id via
# ``SecurityRepository.resolve_id`` instead of using this constant.
LISTING_ID = 1


@dataclass
class SymbolListingClientCalls:
    """Records construction + ``list_symbols`` calls made against a fake client.

    Several ``refresh-supported-tickers`` tests assert which exchange codes the
    fake EODHD client was asked to list. A plain ``{"listed": [], "api_key": ...}``
    dict literal infers ``dict[str, object]``, which then rejects
    ``listed.append(...)`` (``object`` has no ``append``); a typed record keeps
    the accumulator precise.
    """

    api_key: str | None = None
    listed: list[str] = field(default_factory=list)


def make_fact(
    *,
    symbol: str = "AAPL.US",
    concept: str = "",
    fiscal_period: str = "FY",
    end_date: str = "",
    unit_kind: MetricUnitKind = "monetary",
    value: float = 0.0,
    filed: str | None = None,
    currency: str | None = "USD",
) -> FactRecord:
    # Facts default to a monetary USD value; callers override ``currency`` (and,
    # for non-monetary facts, ``unit_kind`` + ``currency=None``) as needed. The
    # parameters mirror ``FactRecord`` exactly so the record is built directly,
    # rather than unpacking a ``dict[str, object]`` literal (which mypy cannot
    # reconcile with ``FactRecord``'s precisely typed fields).
    return FactRecord(
        symbol=symbol,
        concept=concept,
        fiscal_period=fiscal_period,
        end_date=end_date,
        unit_kind=unit_kind,
        value=value,
        filed=filed,
        currency=currency,
    )


def fetch_state_row(
    repo: MarketDataFetchStateRepository, provider: str, symbol: str
) -> dict[str, str | int | None]:
    """Return the persisted fetch-state row for ``(provider, symbol)``.

    ``MarketDataFetchStateRepository.fetch`` returns ``None`` for an unknown
    symbol, so callers that expect a recorded row (the common case in these
    tests) would otherwise have to narrow the optional before indexing it. This
    helper asserts the row exists and hands back the non-optional mapping.
    """

    row = repo.fetch(provider, symbol)
    assert row is not None, f"no fetch-state row for {provider}/{symbol}"
    return row


def clear_root_logging_handlers() -> None:
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()


def store_supported_exchanges(
    db_path: Path,
    rows: list[dict[str, object]] | None = None,
    provider: str = "EODHD",
) -> ExchangeProviderRepository:
    repo = ExchangeProviderRepository(db_path)
    repo.initialize_schema()
    repo.replace_for_provider(
        provider,
        rows
        or [
            {
                "Code": "LSE",
                "Name": "London Exchange",
                "Country": "UK",
                "Currency": "GBP",
                "OperatingMIC": "XLON",
                "CountryISO2": "GB",
                "CountryISO3": "GBR",
            }
        ],
    )
    return repo


def default_listing_currency(
    symbol: str, exchange_code: str | None = None
) -> str | None:
    suffix = symbol.rsplit(".", 1)[-1].upper() if "." in symbol else None
    code = (exchange_code or suffix or "").upper()
    return {
        "AS": "EUR",
        "AMS": "EUR",
        "LSE": "GBP",
        "NASDAQ": "USD",
        "NYSE": "USD",
        "US": "USD",
    }.get(code)


def store_supported_tickers(
    db_path: Path,
    exchange_code: str,
    rows: list[dict[str, object]] | None = None,
    provider: str = "EODHD",
) -> SupportedTickerRepository:
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    normalized_rows = []
    for row in rows or [
        {
            "Code": "AAA",
            "Name": "AAA plc",
            "Exchange": exchange_code,
            "Type": "Common Stock",
            "Currency": "GBP",
        }
    ]:
        normalized = dict(row)
        if not normalized.get("Currency"):
            normalized["Currency"] = default_listing_currency(
                str(normalized.get("Code") or ""),
                str(normalized.get("Exchange") or exchange_code),
            )
        normalized_rows.append(normalized)
    seed_exchange(db_path, exchange_code, provider=provider)
    repo.replace_for_exchange(
        provider,
        exchange_code,
        normalized_rows,
    )
    return repo


def store_catalog_listings(
    db_path: Path,
    exchange_code: str,
    listings: Sequence[Listing],
    provider: str = "SEC",
) -> SupportedTickerRepository:
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    listings_with_currency = [
        listing
        if listing.currency is not None
        else replace(
            listing,
            currency=default_listing_currency(listing.symbol, exchange_code),
        )
        for listing in listings
    ]
    seed_exchange(db_path, exchange_code, provider=provider)
    seed_supported_listings(db_path, provider, exchange_code, listings_with_currency)
    return repo


def _seed_listing(
    db_path: Path,
    symbol: str | Sequence[str],
    currency: str = "USD",
    provider: str = "EODHD",
) -> SupportedTickerRepository:
    """Seed cataloged listing(s) carrying ``currency`` for ``symbol``.

    ``listing.currency`` is NOT NULL with no fallback, so every listing must be
    created from a provider payload that carries a currency. Tests that drive
    ``replace_facts``/``FundamentalsRepository.upsert``/``upsert_price`` for an
    uncatalogued symbol seed the listing here first (under the same provider the
    test exercises) so the strict creation path is satisfied.

    ``symbol`` may be a single symbol or a sequence. Symbols are grouped by their
    exchange suffix and seeded with a single ``replace_for_exchange`` call per
    exchange, so seeding several same-exchange symbols does not wipe its
    siblings (``replace_for_exchange`` removes provider listings absent from the
    payload it is given).
    """

    symbols = [symbol] if isinstance(symbol, str) else list(symbol)
    rows_by_exchange: dict[str, list[dict[str, str]]] = {}
    for entry in symbols:
        ticker, suffix = entry.split(".")
        rows_by_exchange.setdefault(suffix, []).append(
            {"Code": ticker, "Type": "Common Stock", "Currency": currency}
        )
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    for suffix, rows in rows_by_exchange.items():
        seed_exchange(db_path, suffix, provider=provider, currency=currency)
        repo.replace_for_exchange(provider, suffix, rows)
    return repo


def _seed_share_count(db_path: Path, symbol: str, as_of: str, shares: float) -> None:
    """Add a co-dated shares-outstanding fact without wiping existing facts.

    Market cap is computed on demand as a share-count fact x the price as of that
    fact's date, so market-cap tests seed a share count dated with the price. The
    existing facts are read back and re-written so the (destructive)
    ``replace_facts`` does not drop facts other tests seeded for the symbol.
    """

    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    # The fact readers key on ``listing_id`` now, so resolve the symbol to its
    # listing id to read back the facts already stored for it. An unseeded symbol
    # has no listing (and therefore no facts) -- treat it as an empty carry-over.
    listing_id = resolve_listing_id(db_path, symbol)
    preserved = (
        [
            record
            for record in repo.facts_for_id(listing_id)
            if record.concept != "CommonStockSharesOutstanding"
        ]
        if listing_id is not None
        else []
    )
    seed_facts(
        db_path,
        symbol,
        preserved
        + [
            FactRecord(
                symbol=symbol,
                concept="CommonStockSharesOutstanding",
                fiscal_period="INSTANT",
                end_date=as_of,
                unit_kind="count",
                value=shares,
            )
        ],
    )


def store_market_data(
    db_path: Path,
    symbol: str,
    as_of: str,
    price: float = 10.0,
    market_cap: float | None = None,
    currency: str | None = "USD",
) -> MarketDataRepository:
    repo = MarketDataRepository(db_path)
    repo.initialize_schema()
    seed_price(
        db_path,
        symbol,
        as_of,
        price,
        currency=currency,
    )
    if market_cap is not None:
        # Reproduce the requested market cap from its on-demand inputs:
        # shares = market_cap / price, dated with the price so price_as_of pairs
        # them. Callers seed any non-share facts before calling this helper.
        _seed_share_count(db_path, symbol, as_of, market_cap / price)
    return repo


def _spawn_process_pool_executor(max_workers: int) -> ProcessPoolExecutor:
    log_dir, console_level, file_level = cli.current_logging_config()
    return ProcessPoolExecutor(
        max_workers=max_workers,
        mp_context=mp.get_context("spawn"),
        initializer=cli._initialize_worker_logging,
        initargs=(
            str(log_dir) if log_dir is not None else None,
            console_level,
            file_level,
        ),
    )


def make_supported_ticker(
    symbol: str,
    exchange_code: str,
    security_id: int,
    currency: str = "USD",
) -> SupportedTicker:
    ticker, _ = symbol.split(".")
    return SupportedTicker(
        provider="EODHD",
        provider_exchange_code=exchange_code,
        provider_symbol=symbol,
        provider_ticker=ticker,
        security_id=security_id,
        listing_exchange=exchange_code,
        security_name=symbol,
        security_type="Common Stock",
        country="US",
        currency=currency,
        isin=None,
        updated_at=None,
    )


def test_main_dispatches_report_ingest_progress_with_default_max_age_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(
        provider: str,
        database: str,
        exchange_codes: Sequence[str] | None,
        max_age_days: int | None,
        missing_only: bool,
    ) -> int:
        calls["provider"] = provider
        calls["database"] = database
        calls["exchange_codes"] = exchange_codes
        calls["max_age_days"] = max_age_days
        calls["missing_only"] = missing_only
        return 0

    patch_cli(monkeypatch, "setup_logging", lambda: None)
    patch_cli(monkeypatch, "cmd_report_fundamentals_progress", fake_cmd)

    rc = cli.main(["report-fundamentals-progress"])

    assert rc == 0
    assert calls == {
        "provider": "EODHD",
        "database": "data/pyvalue.db",
        "exchange_codes": None,
        "max_age_days": 30,
        "missing_only": False,
    }


def test_build_parser_report_ingest_progress_missing_only() -> None:
    args = cli.build_parser().parse_args(
        ["report-fundamentals-progress", "--exchange-codes", "US,LSE", "--missing-only"]
    )

    assert args.command == "report-fundamentals-progress"
    assert args.exchange_codes == ["US,LSE"]
    assert args.max_age_days == 30
    assert args.missing_only is True


def test_build_parser_refresh_tickers_allow_mass_delisting() -> None:
    args = cli.build_parser().parse_args(
        [
            "refresh-supported-tickers",
            "--exchange-codes",
            "US",
            "--allow-mass-delisting",
        ]
    )

    assert args.command == "refresh-supported-tickers"
    assert args.allow_mass_delisting is True

    default_args = cli.build_parser().parse_args(["refresh-supported-tickers"])
    assert default_args.allow_mass_delisting is False


def test_build_parser_refresh_exchanges_allow_mass_drop() -> None:
    args = cli.build_parser().parse_args(
        ["refresh-supported-exchanges", "--allow-mass-drop"]
    )

    assert args.command == "refresh-supported-exchanges"
    assert args.allow_mass_drop is True

    default_args = cli.build_parser().parse_args(["refresh-supported-exchanges"])
    assert default_args.allow_mass_drop is False


def test_main_dispatches_ingest_fundamentals_with_default_provider_and_max_age_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(
        provider: str,
        database: str,
        symbols: Sequence[str] | None,
        exchange_codes: Sequence[str] | None,
        all_supported: bool,
        rate: float | None,
        max_symbols: int | None,
        max_age_days: int | None,
        respect_backoff: bool,
    ) -> int:
        calls["provider"] = provider
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["rate"] = rate
        calls["max_symbols"] = max_symbols
        calls["max_age_days"] = max_age_days
        calls["respect_backoff"] = respect_backoff
        return 0

    patch_cli(monkeypatch, "setup_logging", lambda: None)
    patch_cli(monkeypatch, "cmd_ingest_fundamentals_stage", fake_cmd)

    rc = cli.main(["ingest-fundamentals", "--symbols", "AAPL.US"])

    assert rc == 0
    assert calls == {
        "provider": "EODHD",
        "database": "data/pyvalue.db",
        "symbols": ["AAPL.US"],
        "exchange_codes": None,
        "all_supported": False,
        "rate": None,
        "max_symbols": None,
        "max_age_days": 30,
        "respect_backoff": True,
    }

    args = cli.build_parser().parse_args(
        ["ingest-fundamentals", "--symbols", "AAPL.US", "--retry-failed-now"]
    )
    assert args.retry_failed_now is True

    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            ["ingest-fundamentals", "--symbols", "AAPL.US", "--resume"]
        )


def test_build_parser_reconcile_listing_status_defaults_provider() -> None:
    args = cli.build_parser().parse_args(["reconcile-listing-status"])

    assert args.command == "reconcile-listing-status"
    assert args.provider == "EODHD"
    assert args.symbols is None
    assert args.exchange_codes is None
    assert args.all_supported is False


def test_main_dispatches_reconcile_listing_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(
        provider: str,
        database: str,
        symbols: Sequence[str] | None,
        exchange_codes: Sequence[str] | None,
        all_supported: bool,
    ) -> int:
        calls["provider"] = provider
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        return 0

    patch_cli(monkeypatch, "setup_logging", lambda: None)
    patch_cli(monkeypatch, "cmd_reconcile_listing_status", fake_cmd)

    rc = cli.main(
        [
            "reconcile-listing-status",
            "--exchange-codes",
            "US",
            "--database",
            "data/custom.db",
        ]
    )

    assert rc == 0
    assert calls == {
        "provider": "EODHD",
        "database": "data/custom.db",
        "symbols": None,
        "exchange_codes": ["US"],
        "all_supported": False,
    }


def test_build_parser_normalize_fundamentals_defaults_provider() -> None:
    args = cli.build_parser().parse_args(["normalize-fundamentals"])

    assert args.command == "normalize-fundamentals"
    assert args.provider == "EODHD"
    assert args.symbols is None
    assert args.exchange_codes is None
    assert args.all_supported is False
    assert args.force is False

    args = cli.build_parser().parse_args(
        ["normalize-fundamentals", "--symbols", "AAPL.US"]
    )

    assert args.command == "normalize-fundamentals"
    assert args.provider == "EODHD"
    assert args.symbols == ["AAPL.US"]
    assert args.force is False

    forced = cli.build_parser().parse_args(
        ["normalize-fundamentals", "--symbols", "AAPL.US", "--force"]
    )
    assert forced.force is True


def test_main_dispatches_normalize_fundamentals_stage_with_force(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(
        provider: str,
        database: str,
        symbols: Sequence[str] | None,
        exchange_codes: Sequence[str] | None,
        all_supported: bool,
        force: bool,
    ) -> int:
        calls["provider"] = provider
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["force"] = force
        return 0

    patch_cli(monkeypatch, "setup_logging", lambda: None)
    patch_cli(monkeypatch, "cmd_normalize_fundamentals_stage", fake_cmd)

    rc = cli.main(["normalize-fundamentals", "--symbols", "AAPL.US", "--force"])

    assert rc == 0
    assert calls == {
        "provider": "EODHD",
        "database": "data/pyvalue.db",
        "symbols": ["AAPL.US"],
        "exchange_codes": None,
        "all_supported": False,
        "force": True,
    }


def test_build_parser_compute_metrics_warning_flag_defaults_to_suppressed() -> None:
    args = cli.build_parser().parse_args(["compute-metrics"])

    assert args.command == "compute-metrics"
    assert args.symbols is None
    assert args.exchange_codes is None
    assert args.all_supported is False
    assert args.show_metric_warnings is False

    args = cli.build_parser().parse_args(["compute-metrics", "--symbols", "AAPL.US"])

    assert args.command == "compute-metrics"
    assert args.show_metric_warnings is False

    args = cli.build_parser().parse_args(
        ["compute-metrics", "--symbols", "AAPL.US", "--show-metric-warnings"]
    )

    assert args.show_metric_warnings is True


def test_main_dispatches_compute_metrics_stage_with_warning_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(
        database: str,
        symbols: Sequence[str] | None,
        exchange_codes: Sequence[str] | None,
        all_supported: bool,
        metric_ids: Sequence[str],
        show_metric_warnings: bool,
        profile: bool,
    ) -> int:
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["metric_ids"] = metric_ids
        calls["show_metric_warnings"] = show_metric_warnings
        calls["profile"] = profile
        return 0

    patch_cli(monkeypatch, "setup_logging", lambda: None)
    patch_cli(monkeypatch, "cmd_compute_metrics_stage", fake_cmd)

    rc = cli.main(["compute-metrics", "--symbols", "AAPL.US", "--show-metric-warnings"])

    assert rc == 0
    assert calls == {
        "database": "data/pyvalue.db",
        "symbols": ["AAPL.US"],
        "exchange_codes": None,
        "all_supported": False,
        "metric_ids": None,
        "show_metric_warnings": True,
        "profile": False,
    }


def test_build_parser_run_screen_requires_config_and_defaults_warning_flag() -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["run-screen", "--symbols", "AAPL.US"])

    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["run-screen", "screeners/value.yml"])

    args = cli.build_parser().parse_args(
        ["run-screen", "--config", "screeners/value.yml", "--symbols", "AAPL.US"]
    )

    assert args.command == "run-screen"
    assert args.config == "screeners/value.yml"
    assert args.show_metric_warnings is False

    args = cli.build_parser().parse_args(
        [
            "run-screen",
            "--config",
            "screeners/value.yml",
            "--symbols",
            "AAPL.US",
            "--show-metric-warnings",
        ]
    )

    assert args.show_metric_warnings is True


def test_main_dispatches_run_screen_stage_with_warning_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(
        config_path: str,
        database: str,
        symbols: Sequence[str] | None,
        exchange_codes: Sequence[str] | None,
        all_supported: bool,
        output_csv: str | None,
        show_metric_warnings: bool,
    ) -> int:
        calls["config_path"] = config_path
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["output_csv"] = output_csv
        calls["show_metric_warnings"] = show_metric_warnings
        return 0

    patch_cli(monkeypatch, "setup_logging", lambda: None)
    patch_cli(monkeypatch, "cmd_run_screen_stage", fake_cmd)

    rc = cli.main(
        [
            "run-screen",
            "--config",
            "screeners/value.yml",
            "--symbols",
            "AAPL.US",
            "--output-csv",
            "data/out.csv",
            "--show-metric-warnings",
        ]
    )

    assert rc == 0
    assert calls == {
        "config_path": "screeners/value.yml",
        "database": "data/pyvalue.db",
        "symbols": ["AAPL.US"],
        "exchange_codes": None,
        "all_supported": False,
        "output_csv": "data/out.csv",
        "show_metric_warnings": True,
    }


def test_build_parser_refresh_security_metadata_uses_scope_selectors() -> None:
    args = cli.build_parser().parse_args(
        ["refresh-security-metadata", "--exchange-codes", "US"]
    )

    assert args.command == "refresh-security-metadata"
    assert args.exchange_codes == ["US"]
    assert args.database == "data/pyvalue.db"


def test_main_dispatches_refresh_security_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(
        database: str,
        symbols: Sequence[str] | None,
        exchange_codes: Sequence[str] | None,
        all_supported: bool,
    ) -> int:
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        return 0

    patch_cli(monkeypatch, "setup_logging", lambda: None)
    patch_cli(monkeypatch, "cmd_refresh_security_metadata", fake_cmd)

    rc = cli.main(
        [
            "refresh-security-metadata",
            "--exchange-codes",
            "US",
            "--database",
            "data/custom.db",
        ]
    )

    assert rc == 0
    assert calls == {
        "database": "data/custom.db",
        "symbols": None,
        "exchange_codes": ["US"],
        "all_supported": False,
    }


def test_build_parser_report_screen_failures_requires_config() -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            ["report-screen-failures", "--symbols", "AAPL.US"]
        )

    args = cli.build_parser().parse_args(
        [
            "report-screen-failures",
            "--config",
            "screeners/value.yml",
            "--symbols",
            "AAPL.US",
        ]
    )

    assert args.command == "report-screen-failures"
    assert args.config == "screeners/value.yml"
    assert args.output_csv is None


def test_main_dispatches_report_screen_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(
        config_path: str,
        database: str,
        symbols: Sequence[str] | None,
        exchange_codes: Sequence[str] | None,
        all_supported: bool,
        output_csv: str | None,
    ) -> int:
        calls["config_path"] = config_path
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["output_csv"] = output_csv
        return 0

    patch_cli(monkeypatch, "setup_logging", lambda: None)
    patch_cli(monkeypatch, "cmd_report_screen_failures", fake_cmd)

    rc = cli.main(
        [
            "report-screen-failures",
            "--config",
            "screeners/value.yml",
            "--exchange-codes",
            "US",
            "--output-csv",
            "data/out.csv",
        ]
    )

    assert rc == 0
    assert calls == {
        "config_path": "screeners/value.yml",
        "database": "data/pyvalue.db",
        "symbols": None,
        "exchange_codes": ["US"],
        "all_supported": False,
        "output_csv": "data/out.csv",
    }


def test_main_dispatches_update_market_data_global_with_default_max_age_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(
        provider: str,
        database: str,
        symbols: Sequence[str] | None,
        exchange_codes: Sequence[str] | None,
        all_supported: bool,
        rate: float | None,
        max_symbols: int | None,
        max_age_days: int | None,
        respect_backoff: bool,
    ) -> int:
        calls["provider"] = provider
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["rate"] = rate
        calls["max_symbols"] = max_symbols
        calls["max_age_days"] = max_age_days
        calls["respect_backoff"] = respect_backoff
        return 0

    patch_cli(monkeypatch, "setup_logging", lambda: None)
    patch_cli(monkeypatch, "cmd_update_market_data_stage", fake_cmd)

    rc = cli.main(["update-market-data", "--all-supported"])

    assert rc == 0
    assert calls == {
        "provider": "EODHD",
        "database": "data/pyvalue.db",
        "symbols": None,
        "exchange_codes": None,
        "all_supported": True,
        "rate": None,
        "max_symbols": None,
        "max_age_days": 30,
        "respect_backoff": True,
    }

    args = cli.build_parser().parse_args(
        ["update-market-data", "--all-supported", "--retry-failed-now"]
    )
    assert args.retry_failed_now is True

    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            ["update-market-data", "--all-supported", "--resume"]
        )


def test_main_dispatches_update_market_data_without_scope_as_default_universe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(
        provider: str,
        database: str,
        symbols: Sequence[str] | None,
        exchange_codes: Sequence[str] | None,
        all_supported: bool,
        rate: float | None,
        max_symbols: int | None,
        max_age_days: int | None,
        respect_backoff: bool,
    ) -> int:
        calls["provider"] = provider
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["rate"] = rate
        calls["max_symbols"] = max_symbols
        calls["max_age_days"] = max_age_days
        calls["respect_backoff"] = respect_backoff
        return 0

    patch_cli(monkeypatch, "setup_logging", lambda: None)
    patch_cli(monkeypatch, "cmd_update_market_data_stage", fake_cmd)

    rc = cli.main(["update-market-data"])

    assert rc == 0
    assert calls == {
        "provider": "EODHD",
        "database": "data/pyvalue.db",
        "symbols": None,
        "exchange_codes": None,
        "all_supported": False,
        "rate": None,
        "max_symbols": None,
        "max_age_days": 30,
        "respect_backoff": True,
    }


def test_main_returns_cleanly_on_uncaught_keyboard_interrupt(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    patch_cli(monkeypatch, "setup_logging", lambda: None)

    def raising_cmd(provider: str, database: str, allow_mass_drop: bool) -> None:
        raise KeyboardInterrupt

    patch_cli(monkeypatch, "cmd_refresh_supported_exchanges", raising_cmd)

    rc = cli.main(["refresh-supported-exchanges"])

    assert rc == 1
    assert capsys.readouterr().out.splitlines() == ["Cancelled by user."]


def test_main_dispatches_report_market_data_progress_with_default_max_age_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(
        provider: str,
        database: str,
        exchange_codes: Sequence[str] | None,
        max_age_days: int | None,
    ) -> int:
        calls["provider"] = provider
        calls["database"] = database
        calls["exchange_codes"] = exchange_codes
        calls["max_age_days"] = max_age_days
        return 0

    patch_cli(monkeypatch, "setup_logging", lambda: None)
    patch_cli(monkeypatch, "cmd_report_market_data_progress", fake_cmd)

    rc = cli.main(["report-market-data-progress"])

    assert rc == 0
    assert calls == {
        "provider": "EODHD",
        "database": "data/pyvalue.db",
        "exchange_codes": None,
        "max_age_days": 30,
    }


def test_build_parser_report_fact_freshness_defaults_max_age_days() -> None:
    args = cli.build_parser().parse_args(
        ["report-fact-freshness", "--symbols", "AAPL.US"]
    )

    assert args.command == "report-fact-freshness"
    assert args.max_age_days == MAX_FACT_AGE_DAYS


def test_cmd_refresh_supported_exchanges(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    @dataclass
    class ExchangeClientCalls:
        # Records the API key the client was constructed with and how many times
        # ``list_exchanges`` was invoked. A dict literal would infer
        # ``dict[str, object]`` and break the ``+ 1`` counter increment.
        api_key: str | None = None
        list_exchanges: int = 0

    calls = ExchangeClientCalls()

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            calls.api_key = api_key

        def list_exchanges(self) -> list[dict[str, object]]:
            calls.list_exchanges += 1
            return [
                {
                    "Code": " lse ",
                    "Name": " London Exchange ",
                    "Country": " UK ",
                    "Currency": " GBP ",
                    "OperatingMIC": " XLON ",
                    "CountryISO2": " GB ",
                    "CountryISO3": " GBR ",
                },
                {
                    "Code": " US ",
                    "Name": " USA Stocks ",
                    "Country": " USA ",
                    "Currency": " USD ",
                    "OperatingMIC": " XNAS, XNYS ",
                    "CountryISO2": " US ",
                    "CountryISO3": " USA ",
                },
            ]

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")

    db_path = tmp_path / "supported-exchanges.db"
    rc = cli.cmd_refresh_supported_exchanges(
        provider="EODHD",
        database=str(db_path),
    )

    assert rc == 0
    assert calls == ExchangeClientCalls(api_key="TOKEN", list_exchanges=1)

    repo = ExchangeProviderRepository(db_path)
    repo.initialize_schema()
    record = repo.fetch("eodhd", "LSE")
    assert record is not None
    assert record.code == "LSE"
    assert record.name == "London Exchange"
    assert record.country == "UK"
    assert record.currency == "GBP"
    assert record.operating_mic == "XLON"
    assert record.country_iso2 == "GB"
    assert record.country_iso3 == "GBR"
    assert [row.code for row in repo.list_all("EODHD")] == ["LSE", "US"]


def test_cmd_refresh_supported_exchanges_reports_dropped_venues(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "supported-exchanges-drop.db"
    store_supported_exchanges(
        db_path,
        rows=[
            {"Code": "LSE", "Name": "London Exchange", "Currency": "GBP"},
            {"Code": "US", "Name": "USA Stocks", "Currency": "USD"},
        ],
    )

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key

        def list_exchanges(self) -> list[dict[str, object]]:
            return [{"Code": "LSE", "Name": "London Exchange", "Currency": "GBP"}]

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_refresh_supported_exchanges(provider="EODHD", database=str(db_path))

    out = capsys.readouterr().out
    assert rc == 0
    # Destruction must be visible: each dropped venue gets its own line.
    assert (
        "Dropped US from the EODHD catalog: purged 0 provider listing(s); "
        "canonical data retained"
    ) in out
    repo = ExchangeProviderRepository(db_path)
    assert [row.code for row in repo.list_all("EODHD")] == ["LSE"]


def test_cmd_refresh_supported_exchanges_blocks_mass_drop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "supported-exchanges-mass-drop.db"
    codes = [f"E{index}" for index in range(10)]
    store_supported_exchanges(
        db_path,
        rows=[
            {"Code": code, "Name": f"{code} Exchange", "Currency": "USD"}
            for code in codes
        ],
    )

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key

        def list_exchanges(self) -> list[dict[str, object]]:
            # A truncated exchanges-list: 8 of 10 venues vanish at once.
            return [{"Code": "E0", "Name": "E0 Exchange", "Currency": "USD"}]

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_refresh_supported_exchanges(provider="EODHD", database=str(db_path))

    out = capsys.readouterr().out
    assert rc == 1
    assert "WARNING: exchange catalog refresh blocked" in out
    assert "--allow-mass-drop" in out
    repo = ExchangeProviderRepository(db_path)
    assert [row.code for row in repo.list_all("EODHD")] == codes


def test_cmd_refresh_supported_tickers_filters_types_and_cleans_catalog(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "refresh-supported-tickers.db"
    store_supported_exchanges(
        db_path,
        rows=[
            {
                "Code": "LSE",
                "Name": "London Exchange",
                "Country": "UK",
                "Currency": "GBP",
                "OperatingMIC": "XLON",
                "CountryISO2": "GB",
                "CountryISO3": "GBR",
            }
        ],
    )
    store_supported_tickers(
        db_path,
        "LSE",
        rows=[
            {
                "Code": "OLD",
                "Exchange": "LSE",
                "Name": "Old plc",
                "Type": "Common Stock",
                "Currency": "GBP",
                "ISIN": "GB00OLD",
            },
            {
                "Code": "KEEP",
                "Exchange": "LSE",
                "Name": "Keep plc",
                "Type": "Common Stock",
                "Currency": "GBP",
                "ISIN": "GB00KEEP",
            },
        ],
    )

    state_repo = FundamentalsFetchStateRepository(db_path)
    state_repo.initialize_schema()
    state_repo.mark_failure("EODHD", "OLD.LSE", "stale")
    state_repo.mark_failure("EODHD", "KEEP.LSE", "still-listed")

    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "OLD.LSE",
        {"General": {"CurrencyCode": "GBP", "Name": "Old plc"}},
        exchange="LSE",
    )

    calls = SymbolListingClientCalls()

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            calls.api_key = api_key

        def list_symbols(self, exchange_code: str) -> list[dict[str, object]]:
            calls.listed.append(exchange_code)
            return [
                {
                    "Code": "KEEP",
                    "Exchange": exchange_code,
                    "Name": "Keep plc",
                    "Type": "Common Stock",
                    "Currency": "GBP",
                    "ISIN": "GB00KEEP",
                },
                {
                    "Code": "PREF",
                    "Exchange": exchange_code,
                    "Name": "Pref plc",
                    "Type": "Preferred Stock",
                    "Currency": "GBP",
                    "ISIN": "GB00PREF",
                },
                {
                    "Code": "ETF1",
                    "Exchange": exchange_code,
                    "Name": "ETF 1",
                    "Type": "ETF",
                    "Currency": "GBP",
                    "ISIN": "GB00ETF1",
                },
            ]

        def list_exchanges(self) -> list[dict[str, object]]:
            raise AssertionError("Should not refresh supported exchanges on cache hit")

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_refresh_supported_tickers(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=["LSE"],
        all_supported=False,
    )

    assert rc == 0
    assert calls == SymbolListingClientCalls(api_key="TOKEN", listed=["LSE"])

    ticker_repo = SupportedTickerRepository(db_path)
    rows = ticker_repo.list_for_provider("EODHD", exchange_codes=["LSE"])
    assert [row.symbol for row in rows] == ["KEEP.LSE", "PREF.LSE"]
    assert [row.security_type for row in rows] == [None, None]
    with ticker_repo._connect() as conn:
        listings_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='listings'"
        ).fetchone()

    assert state_repo.fetch("EODHD", "OLD.LSE") is None
    assert state_repo.fetch("EODHD", "KEEP.LSE") is not None
    assert not fundamentals_payload_exists(db_path, "EODHD", "OLD.LSE")
    assert listings_table is None


def test_cmd_refresh_supported_tickers_all_exchanges_in_code_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "refresh-supported-tickers-all.db"
    store_supported_exchanges(
        db_path,
        rows=[
            {
                "Code": "US",
                "Name": "USA Stocks",
                "Country": "USA",
                "Currency": "USD",
                "OperatingMIC": "XNAS",
                "CountryISO2": "US",
                "CountryISO3": "USA",
            },
            {
                "Code": "LSE",
                "Name": "London Exchange",
                "Country": "UK",
                "Currency": "GBP",
                "OperatingMIC": "XLON",
                "CountryISO2": "GB",
                "CountryISO3": "GBR",
            },
            {
                "Code": "TSX",
                "Name": "Toronto Exchange",
                "Country": "Canada",
                "Currency": "CAD",
                "OperatingMIC": "XTSE",
                "CountryISO2": "CA",
                "CountryISO3": "CAN",
            },
        ],
    )
    calls = SymbolListingClientCalls()

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            calls.api_key = api_key

        def list_symbols(self, exchange_code: str) -> list[dict[str, object]]:
            calls.listed.append(exchange_code)
            return [
                {
                    "Code": f"{exchange_code}1",
                    "Exchange": exchange_code,
                    "Name": f"{exchange_code} Company",
                    "Type": "Common Stock",
                    "Currency": "USD",
                }
            ]

        def list_exchanges(self) -> list[dict[str, object]]:
            raise AssertionError("Should use cached supported exchanges")

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_refresh_supported_tickers(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        all_supported=True,
    )

    assert rc == 0
    assert calls.api_key == "TOKEN"
    assert calls.listed == ["LSE", "TSX", "US"]

    repo = SupportedTickerRepository(db_path)
    assert repo.available_exchanges("EODHD") == ["LSE", "TSX", "US"]


def test_cmd_refresh_supported_tickers_defaults_to_all_supported(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "refresh-supported-tickers-default-all.db"
    store_supported_exchanges(
        db_path,
        rows=[
            {
                "Code": "US",
                "Name": "USA Stocks",
                "Country": "USA",
                "Currency": "USD",
                "OperatingMIC": "XNAS",
                "CountryISO2": "US",
                "CountryISO3": "USA",
            },
            {
                "Code": "LSE",
                "Name": "London Exchange",
                "Country": "UK",
                "Currency": "GBP",
                "OperatingMIC": "XLON",
                "CountryISO2": "GB",
                "CountryISO3": "GBR",
            },
        ],
    )
    calls = SymbolListingClientCalls()

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            calls.api_key = api_key

        def list_symbols(self, exchange_code: str) -> list[dict[str, object]]:
            calls.listed.append(exchange_code)
            return [
                {
                    "Code": f"{exchange_code}1",
                    "Exchange": exchange_code,
                    "Name": f"{exchange_code} Company",
                    "Type": "Common Stock",
                    "Currency": "USD",
                }
            ]

        def list_exchanges(self) -> list[dict[str, object]]:
            raise AssertionError("Should use cached supported exchanges")

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_refresh_supported_tickers(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        all_supported=False,
    )

    assert rc == 0
    assert calls.api_key == "TOKEN"
    assert calls.listed == ["LSE", "US"]


def test_rate_limiter_respects_burst_and_waits(monkeypatch: pytest.MonkeyPatch) -> None:
    now: dict[str, float] = {"value": 0.0}
    sleeps = []

    def fake_monotonic() -> float:
        return now["value"]

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now["value"] += seconds

    monkeypatch.setattr(cli.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(cli.time, "sleep", fake_sleep)

    limiter = cli._RateLimiter(rate_per_minute=60.0, burst=2)
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()

    assert sleeps == [1.0, 1.0]


def test_interruptible_thread_executor_workers_skip_python_exit_registry() -> None:
    started = threading.Event()
    release = threading.Event()

    def blocking_task() -> None:
        started.set()
        release.wait(timeout=1.0)

    executor = cli._create_interruptible_thread_executor(max_workers=1)
    try:
        executor.submit(blocking_task)
        assert started.wait(timeout=1.0)
        threads = list(executor._threads)
        assert threads
        assert all(thread.daemon for thread in threads)
        assert all(thread not in thread_futures._threads_queues for thread in threads)
    finally:
        release.set()
        executor.shutdown(wait=True)


def test_cmd_report_ingest_progress_reports_complete_with_quota_snapshot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-complete.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ["AAA.US", "BBB.US"]:
        seed_raw_fundamentals(
            db_path,
            "EODHD",
            symbol,
            {"General": {"CurrencyCode": "USD"}},
            exchange="US",
        )

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            assert api_key == "TOKEN"

        def user_metadata(self) -> dict[str, object]:
            return {
                "dailyRateLimit": "100000",
                "apiRequests": "5000",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_fundamentals_daily_buffer_calls=5000,
        ),
    )

    rc = cli.cmd_report_ingest_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=30,
        missing_only=False,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Mode: freshness(30d)" in output
    assert "Status: COMPLETE" in output
    assert "Supported: 2" in output
    assert "Stale: 0" in output
    assert "Fresh: 2" in output
    assert "Quota:" in output
    assert "- usable requests left: 9000" in output
    assert "Next action: Done for current scope" in output


def test_cmd_report_ingest_progress_reports_missing_and_quota_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-missing.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key=None,
            eodhd_fundamentals_daily_buffer_calls=5000,
        ),
    )

    rc = cli.cmd_report_ingest_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=30,
        missing_only=False,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Status: INCOMPLETE" in output
    assert "Missing: 1" in output
    assert "Fresh: 0" in output
    assert "- quota unavailable" in output
    assert "Next action: Run ingest-fundamentals now" in output


def test_cmd_report_ingest_progress_default_mode_treats_old_data_as_stale(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-stale.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US"
    )
    stale_at = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
    with fund_repo._connect() as conn:
        conn.execute(
            """
            UPDATE fundamentals_raw
            SET last_fetched_at = ?
            WHERE provider_listing_id = (
                SELECT provider_listing_id
                FROM provider_listing_catalog
                WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            )
            """,
            (stale_at,),
        )
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key=None,
            eodhd_fundamentals_daily_buffer_calls=0,
        ),
    )

    rc = cli.cmd_report_ingest_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=30,
        missing_only=False,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Status: INCOMPLETE" in output
    assert "Stale: 1" in output
    assert "Fresh: 0" in output
    assert "Mode: freshness(30d)" in output


def test_cmd_report_ingest_progress_missing_only_ignores_staleness(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-missing-only.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US"
    )
    stale_at = (datetime.now(timezone.utc) - timedelta(days=120)).isoformat()
    with fund_repo._connect() as conn:
        conn.execute(
            """
            UPDATE fundamentals_raw
            SET last_fetched_at = ?
            WHERE provider_listing_id = (
                SELECT provider_listing_id
                FROM provider_listing_catalog
                WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            )
            """,
            (stale_at,),
        )
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key=None,
            eodhd_fundamentals_daily_buffer_calls=0,
        ),
    )

    rc = cli.cmd_report_ingest_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=30,
        missing_only=True,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Mode: missing-only" in output
    assert "Status: COMPLETE" in output
    assert "Stale: 0" in output
    assert "Fresh: 1" in output


def test_cmd_report_ingest_progress_reports_blocked_by_backoff(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-blocked.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path, "EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US"
    )
    state_repo = FundamentalsFetchStateRepository(db_path)
    state_repo.initialize_schema()
    state_repo.mark_failure("EODHD", "AAA.US", "boom", base_backoff_seconds=3600)
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key=None,
            eodhd_fundamentals_daily_buffer_calls=0,
        ),
    )

    rc = cli.cmd_report_ingest_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=30,
        missing_only=False,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Status: BLOCKED_BY_BACKOFF" in output
    assert "Blocked: 1" in output
    assert "earliest next eligible: " in output
    assert "AAA.US [US]" in output
    assert (
        "Next action: Wait for backoff to expire or rerun with --retry-failed-now"
        in output
    )


def test_cmd_report_ingest_progress_filters_exchanges(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-filtered.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    store_supported_tickers(
        db_path,
        "LSE",
        rows=[{"Code": "BBB", "Exchange": "LSE", "Type": "Common Stock"}],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.LSE",
        {"General": {"CurrencyCode": "GBP"}},
        exchange="LSE",
    )
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key=None,
            eodhd_fundamentals_daily_buffer_calls=0,
        ),
    )

    rc = cli.cmd_report_ingest_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=["US"],
        max_age_days=30,
        missing_only=False,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Scope: US" in output
    assert (
        "- US: supported=1, stored=0, missing=1, stale=0, blocked=0, errors=0" in output
    )
    assert "- LSE:" not in output


def test_cmd_report_ingest_progress_succeeds_when_user_api_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-user-api-fails.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            assert api_key == "TOKEN"

        def user_metadata(self) -> dict[str, object]:
            raise RuntimeError("nope")

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_fundamentals_daily_buffer_calls=10,
        ),
    )

    rc = cli.cmd_report_ingest_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=30,
        missing_only=False,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Quota:" in output
    assert "- quota unavailable" in output


def test_plan_market_data_stage_run_uses_bulk_for_large_exchange() -> None:
    eligible = [
        *(make_supported_ticker(f"U{i:03d}.US", "US", i) for i in range(100)),
        make_supported_ticker("AAA.LSE", "LSE", 1001, currency="GBP"),
        make_supported_ticker("BBB.LSE", "LSE", 1002, currency="GBP"),
    ]

    plan = cli._plan_market_data_stage_run(eligible, request_budget=1000)

    assert [task.exchange_code for task in plan.bulk_tasks] == ["US"]
    assert [ticker.symbol for ticker in plan.symbol_tickers] == ["AAA.LSE", "BBB.LSE"]
    assert plan.api_call_cost == 102
    assert plan.http_requests == 3


def test_plan_market_data_stage_run_falls_back_to_symbols_when_bulk_does_not_fit_budget() -> (
    None
):
    eligible = [
        *(make_supported_ticker(f"U{i:03d}.US", "US", i) for i in range(100)),
        make_supported_ticker("AAA.LSE", "LSE", 1001, currency="GBP"),
    ]

    plan = cli._plan_market_data_stage_run(eligible, request_budget=5)

    assert plan.bulk_tasks == ()
    assert [ticker.symbol for ticker in plan.symbol_tickers] == [
        "U000.US",
        "U001.US",
        "U002.US",
        "U003.US",
        "U004.US",
    ]
    assert plan.api_call_cost == 5


def test_cmd_update_market_data_stage_uses_bulk_for_large_exchange(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "stage-market-data-bulk.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": f"U{i:03d}", "Exchange": "US", "Type": "Common Stock"}
            for i in range(100)
        ],
    )
    store_supported_tickers(
        db_path,
        "LSE",
        rows=[{"Code": "SMALL", "Exchange": "LSE", "Type": "Common Stock"}],
    )
    calls: dict[str, list[str]] = {"bulk": [], "symbols": []}
    today = date.today().isoformat()

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key

        def user_metadata(self) -> dict[str, object]:
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeProvider:
        def __init__(
            self, api_key: str, session: requests.Session | None = None
        ) -> None:
            assert api_key == "TOKEN"

        def latest_prices_for_exchange(
            self, exchange_code: str
        ) -> dict[str, PriceData]:
            calls["bulk"].append(exchange_code)
            return {
                f"U{i:03d}.US": PriceData(
                    symbol=f"U{i:03d}.US",
                    price=10.0 + i,
                    as_of=today,
                    volume=100 + i,
                    currency="USD",
                )
                for i in range(100)
            }

        def latest_price(self, symbol: str) -> PriceData | None:
            calls["symbols"].append(symbol)
            return PriceData(
                symbol=symbol,
                price=20.0,
                as_of=today,
                volume=50,
                currency="GBP",
            )

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "EODHDProvider", FakeProvider)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )

    rc = cli.cmd_update_market_data_stage(
        provider="EODHD",
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
        rate=None,
        max_symbols=None,
        max_age_days=7,
        respect_backoff=True,
    )

    assert rc == 0
    assert calls["bulk"] == ["US"]
    assert calls["symbols"] == ["SMALL.LSE"]
    state_repo = MarketDataFetchStateRepository(db_path)
    assert fetch_state_row(state_repo, "EODHD", "U000.US")["last_status"] == "ok"
    assert fetch_state_row(state_repo, "EODHD", "SMALL.LSE")["last_status"] == "ok"


def test_cmd_update_market_data_stage_skips_secondary_listings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "stage-market-data-primary-only.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    store_supported_tickers(
        db_path,
        "LSE",
        rows=[
            {"Code": "AAA", "Exchange": "LSE", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "LSE", "Type": "Common Stock"},
        ],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"Name": "AAA", "PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.LSE",
        {"General": {"Name": "AAA plc", "PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.LSE",
        {"General": {"Name": "BBB plc"}},
        exchange="LSE",
    )
    calls = []
    today = date.today().isoformat()

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key

        def user_metadata(self) -> dict[str, object]:
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeProvider:
        def __init__(
            self, api_key: str, session: requests.Session | None = None
        ) -> None:
            assert api_key == "TOKEN"

        def latest_price(self, symbol: str) -> PriceData | None:
            calls.append(symbol)
            return PriceData(
                symbol=symbol,
                price=20.0,
                as_of=today,
                volume=50,
                currency="USD" if symbol.endswith(".US") else "GBP",
            )

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "EODHDProvider", FakeProvider)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )

    rc = cli.cmd_update_market_data_stage(
        provider="EODHD",
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
        rate=None,
        max_symbols=None,
        max_age_days=7,
        respect_backoff=True,
    )

    assert rc == 0
    assert sorted(calls) == ["AAA.US", "BBB.LSE"]
    state_repo = MarketDataFetchStateRepository(db_path)
    assert fetch_state_row(state_repo, "EODHD", "AAA.US")["last_status"] == "ok"
    assert fetch_state_row(state_repo, "EODHD", "BBB.LSE")["last_status"] == "ok"
    assert state_repo.fetch("EODHD", "AAA.LSE") is None
    market_repo = MarketDataRepository(db_path)
    id_aaa_us = resolve_listing_id(db_path, "AAA.US")
    id_bbb_lse = resolve_listing_id(db_path, "BBB.LSE")
    id_aaa_lse = resolve_listing_id(db_path, "AAA.LSE")
    assert id_aaa_us is not None
    assert id_bbb_lse is not None
    assert id_aaa_lse is not None
    assert market_repo.latest_snapshot_by_id(id_aaa_us) is not None
    assert market_repo.latest_snapshot_by_id(id_bbb_lse) is not None
    # AAA.LSE is a secondary listing (PrimaryTicker AAA.US), so it is never
    # fetched and has no market_data row even though its listing exists.
    assert market_repo.latest_snapshot_by_id(id_aaa_lse) is None


def test_cmd_update_market_data_stage_does_not_reconcile_or_mutate_listing_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """update-market-data reads classification but never (re)writes it.

    Classification is owned by ingest-fundamentals and reconcile-listing-status.
    AAA.LSE is left deliberately 'unknown' (a full reconcile would flip it to
    'secondary' via its PrimaryTicker); the stage must leave the cached status
    untouched. Patching the reconcile entrypoint to fail guards against a
    regression that re-introduces reconcile-on-read. Regression for the
    provider-scope read-only refactor.
    """
    db_path = tmp_path / "stage-market-data-readonly-status.db"
    store_supported_tickers(
        db_path,
        "LSE",
        rows=[
            {"Code": "AAA", "Exchange": "LSE", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "LSE", "Type": "Common Stock"},
        ],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.LSE",
        {"General": {"Name": "AAA plc", "PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.LSE",
        {"General": {"Name": "BBB plc"}},
        exchange="LSE",
    )
    # Deliberately stale cache after ingest classified the listings.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE listing SET primary_listing_status = "
            "CASE WHEN symbol = 'AAA' THEN 'unknown' ELSE 'primary' END"
        )

    today = date.today().isoformat()

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key

        def user_metadata(self) -> dict[str, object]:
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeProvider:
        def __init__(
            self, api_key: str, session: requests.Session | None = None
        ) -> None:
            assert api_key == "TOKEN"

        def latest_price(self, symbol: str) -> PriceData | None:
            return PriceData(
                symbol=symbol,
                price=20.0,
                as_of=today,
                volume=50,
                currency="GBP",
            )

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "EODHDProvider", FakeProvider)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )

    def fail_reconcile(*args: object, **kwargs: object) -> None:
        pytest.fail("update-market-data must not reconcile listing status")

    patch_cli(monkeypatch, "_reconcile_eodhd_listing_scope", fail_reconcile)

    rc = cli.cmd_update_market_data_stage(
        provider="EODHD",
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
        rate=None,
        max_symbols=None,
        max_age_days=7,
        respect_backoff=True,
    )

    assert rc == 0
    with sqlite3.connect(db_path) as conn:
        statuses = conn.execute(
            """
            SELECT l.symbol || '.' || e.exchange_code, l.primary_listing_status
            FROM listing l
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            ORDER BY l.symbol || '.' || e.exchange_code
            """
        ).fetchall()

    assert statuses == [
        ("AAA.LSE", "unknown"),
        ("BBB.LSE", "primary"),
    ]


def test_cmd_reconcile_listing_status_backfills_from_raw_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "reconcile-listing-status.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    store_supported_tickers(
        db_path,
        "LSE",
        rows=[
            {"Code": "AAA", "Exchange": "LSE", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "LSE", "Type": "Common Stock"},
        ],
    )

    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"Name": "AAA", "PrimaryTicker": "AAA.US"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.LSE",
        {"General": {"Name": "AAA plc", "PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.LSE",
        {"General": {"Name": "BBB plc"}},
        exchange="LSE",
    )

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    seed_facts(
        db_path,
        "AAA.LSE",
        [
            make_fact(
                symbol="AAA.LSE",
                concept="Assets",
                end_date="2024-12-31",
                value=100.0,
                currency="GBP",
            )
        ],
    )
    store_market_data(db_path, "AAA.LSE", "2025-01-02", currency="GBP")
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(
        db_path,
        "AAA.LSE",
        "market_cap",
        1000.0,
        "2025-01-02",
        unit_kind="monetary",
        currency="GBP",
    )

    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE listing SET primary_listing_status = 'unknown'")

    rc = cli.cmd_reconcile_listing_status(
        provider="EODHD",
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=False,
    )

    assert rc == 0
    with sqlite3.connect(db_path) as conn:
        statuses = conn.execute(
            """
            SELECT l.symbol || '.' || e.exchange_code, l.primary_listing_status
            FROM listing l
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            ORDER BY l.symbol || '.' || e.exchange_code
            """
        ).fetchall()
        fact_rows = conn.execute(
            """
            SELECT COUNT(*)
            FROM financial_facts ff
            JOIN securities s ON s.security_id = ff.listing_id
            WHERE s.canonical_symbol = 'AAA.LSE'
            """
        ).fetchone()[0]
        market_rows = conn.execute(
            """
            SELECT COUNT(*)
            FROM market_data md
            JOIN securities s ON s.security_id = md.listing_id
            WHERE s.canonical_symbol = 'AAA.LSE'
            """
        ).fetchone()[0]
        metric_rows = conn.execute(
            """
            SELECT COUNT(*)
            FROM metrics m
            JOIN securities s ON s.security_id = m.listing_id
            WHERE s.canonical_symbol = 'AAA.LSE'
            """
        ).fetchone()[0]

    assert statuses == [
        ("AAA.LSE", "secondary"),
        ("AAA.US", "primary"),
        ("BBB.LSE", "primary"),
    ]
    # Reconcile only rewrites the status column: the now-secondary AAA.LSE
    # keeps its seeded facts/price/metric (exclusion is scope-side only).
    assert fact_rows == 1
    assert market_rows == 1
    assert metric_rows == 1

    output = capsys.readouterr().out.splitlines()
    assert output == [
        "EODHD listing-status reconciliation",
        f"Database: {db_path}",
        "Scope: all supported tickers",
        "Supported tickers in scope: 3",
        "Listings classified: 3",
        "Primary listings classified: 2",
        "Secondary listings classified: 1",
    ]


def test_cmd_update_market_data_stage_retries_missing_bulk_symbol_individually(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "stage-market-data-fallback.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": f"U{i:03d}", "Exchange": "US", "Type": "Common Stock"}
            for i in range(100)
        ],
    )
    calls: dict[str, list[str]] = {"bulk": [], "symbols": []}
    today = date.today().isoformat()

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key

        def user_metadata(self) -> dict[str, object]:
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeProvider:
        def __init__(
            self, api_key: str, session: requests.Session | None = None
        ) -> None:
            assert api_key == "TOKEN"

        def latest_prices_for_exchange(
            self, exchange_code: str
        ) -> dict[str, PriceData]:
            calls["bulk"].append(exchange_code)
            return {
                f"U{i:03d}.US": PriceData(
                    symbol=f"U{i:03d}.US",
                    price=10.0 + i,
                    as_of=today,
                    volume=100 + i,
                    currency="USD",
                )
                for i in range(99)
            }

        def latest_price(self, symbol: str) -> PriceData | None:
            calls["symbols"].append(symbol)
            return PriceData(
                symbol=symbol,
                price=999.0,
                as_of=today,
                volume=999,
                currency="USD",
            )

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "EODHDProvider", FakeProvider)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )

    rc = cli.cmd_update_market_data_stage(
        provider="EODHD",
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
        rate=None,
        max_symbols=None,
        max_age_days=7,
        respect_backoff=True,
    )

    assert rc == 0
    assert calls["bulk"] == ["US"]
    assert calls["symbols"] == ["U099.US"]
    state_repo = MarketDataFetchStateRepository(db_path)
    assert fetch_state_row(state_repo, "EODHD", "U099.US")["last_status"] == "ok"


def test_cmd_update_market_data_stage_marks_bulk_validation_failure_per_symbol(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "stage-market-data-bulk-validation.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": f"U{i:03d}", "Exchange": "US", "Type": "Common Stock"}
            for i in range(100)
        ],
    )
    today = date.today().isoformat()

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key

        def user_metadata(self) -> dict[str, object]:
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeProvider:
        def __init__(
            self, api_key: str, session: requests.Session | None = None
        ) -> None:
            assert api_key == "TOKEN"

        def latest_prices_for_exchange(
            self, exchange_code: str
        ) -> dict[str, PriceData]:
            return {
                f"U{i:03d}.US": PriceData(
                    symbol=f"U{i:03d}.US",
                    price=10.0 + i,
                    as_of=today,
                    volume=100 + i,
                    currency="USD",
                )
                for i in range(100)
            }

        def latest_price(self, symbol: str) -> PriceData | None:
            raise AssertionError("symbol fallback should not run in this test")

    def fake_build_market_data_update(
        service: MarketDataService, ticker: SupportedTicker, data: PriceData
    ) -> MarketDataUpdate:
        if ticker.symbol == "U099.US":
            raise ValueError("suspicious market data for U099.US")
        return MarketDataUpdate(
            security_id=ticker.security_id,
            symbol=ticker.symbol,
            as_of=data.as_of,
            price=data.price,
            volume=data.volume,
            currency=data.currency,
            provider_listing_id=ticker.provider_listing_id,
        )

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "EODHDProvider", FakeProvider)
    patch_cli(monkeypatch, "_build_market_data_update", fake_build_market_data_update)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )

    rc = cli.cmd_update_market_data_stage(
        provider="EODHD",
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
        rate=None,
        max_symbols=None,
        max_age_days=7,
        respect_backoff=True,
    )

    assert rc == 0
    state_repo = MarketDataFetchStateRepository(db_path)
    assert fetch_state_row(state_repo, "EODHD", "U098.US")["last_status"] == "ok"
    failed = state_repo.fetch("EODHD", "U099.US")
    assert failed is not None
    assert failed["last_status"] == "error"
    assert failed["last_error"] == "suspicious market data for U099.US"


def test_cmd_update_market_data_stage_interrupts_cleanly_in_symbol_phase(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "stage-market-data-interrupt.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
    )

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key

        def user_metadata(self) -> dict[str, object]:
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class InlineExecutor:
        def __init__(self) -> None:
            self.shutdown_calls: list[tuple[bool, bool]] = []

        def submit(
            self, fn: Callable[..., object], *args: object, **kwargs: object
        ) -> Future[object]:
            future: Future[object] = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            self.shutdown_calls.append((wait, cancel_futures))

    def interrupting_as_completed(
        futures: Iterable[Future[object]],
    ) -> Iterator[Future[object]]:
        yielded = False
        for future in futures:
            if not yielded:
                yielded = True
                yield future
                raise KeyboardInterrupt

    executor = InlineExecutor()
    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(monkeypatch, "_require_eodhd_key", lambda: "TOKEN")
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )
    patch_cli(
        monkeypatch,
        "_create_interruptible_thread_executor",
        lambda max_workers: executor,
    )
    patch_cli(
        monkeypatch,
        "_fetch_symbol_market_data",
        lambda api_key, limiter, symbol: PriceData(
            symbol=symbol,
            price=10.0,
            as_of="2024-01-01",
            volume=100,
            currency="USD",
        ),
    )
    patch_cli(monkeypatch, "as_completed", interrupting_as_completed)

    rc = cli.cmd_update_market_data_stage(
        provider="EODHD",
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
        rate=None,
        max_symbols=None,
        max_age_days=7,
        respect_backoff=True,
    )

    assert rc == 1
    output = capsys.readouterr().out
    assert "Cancelled after 1 completed symbols." in output
    assert "Stored market data for" not in output
    assert executor.shutdown_calls == [(False, True)]
    state_repo = MarketDataFetchStateRepository(db_path)
    assert fetch_state_row(state_repo, "EODHD", "AAA.US")["last_status"] == "ok"


def test_cmd_report_market_data_progress_reports_complete_with_quota_snapshot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-market-data-complete.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    today = date.today().isoformat()
    store_market_data(db_path, "AAA.US", today)
    store_market_data(db_path, "BBB.US", today)

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            assert api_key == "TOKEN"

        def user_metadata(self) -> dict[str, object]:
            return {
                "dailyRateLimit": "100000",
                "apiRequests": "5000",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=5000,
        ),
    )

    rc = cli.cmd_report_market_data_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=7,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Mode: freshness(7d)" in output
    assert "Status: COMPLETE" in output
    assert "Supported: 2" in output
    assert "Stale: 0" in output
    assert "Fresh: 2" in output
    assert "- usable requests left: 90000" in output
    assert "Next action: Done for current scope" in output


def test_cmd_report_market_data_progress_reports_missing_and_stale(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-market-data-incomplete.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    store_market_data(
        db_path,
        "AAA.US",
        (date.today() - timedelta(days=30)).isoformat(),
    )
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key=None,
            eodhd_market_data_daily_buffer_calls=0,
        ),
    )

    rc = cli.cmd_report_market_data_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=7,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Status: INCOMPLETE" in output
    assert "Missing: 1" in output
    assert "Stale: 1" in output
    assert "Fresh: 0" in output
    assert "Next action: Run update-market-data now" in output


def test_cmd_report_market_data_progress_reports_blocked_by_backoff(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-market-data-blocked.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    store_market_data(db_path, "AAA.US", date.today().isoformat())
    state_repo = MarketDataFetchStateRepository(db_path)
    state_repo.initialize_schema()
    state_repo.mark_failure("EODHD", "AAA.US", "boom", base_backoff_seconds=3600)
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key=None,
            eodhd_market_data_daily_buffer_calls=0,
        ),
    )

    rc = cli.cmd_report_market_data_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=7,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Status: BLOCKED_BY_BACKOFF" in output
    assert "Blocked: 1" in output
    assert "AAA.US [US]" in output
    assert (
        "Next action: Wait for backoff to expire or rerun with --retry-failed-now"
        in output
    )


def test_cmd_report_market_data_progress_filters_exchanges(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-market-data-filtered.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    store_supported_tickers(
        db_path,
        "LSE",
        rows=[{"Code": "BBB", "Exchange": "LSE", "Type": "Common Stock"}],
    )
    store_market_data(db_path, "BBB.LSE", date.today().isoformat())
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key=None,
            eodhd_market_data_daily_buffer_calls=0,
        ),
    )

    rc = cli.cmd_report_market_data_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=["US"],
        max_age_days=7,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Scope: US" in output
    assert (
        "- US: supported=1, stored=0, missing=1, stale=0, blocked=0, errors=0" in output
    )
    assert "- LSE:" not in output


def test_cmd_report_market_data_progress_succeeds_when_user_api_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "report-market-data-user-api-fails.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            assert api_key == "TOKEN"

        def user_metadata(self) -> dict[str, object]:
            raise RuntimeError("nope")

    patch_cli(monkeypatch, "EODHDFundamentalsClient", FakeClient)
    patch_cli(
        monkeypatch,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=10,
        ),
    )

    rc = cli.cmd_report_market_data_progress(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        max_age_days=7,
    )

    output = capsys.readouterr().out
    assert rc == 0
    assert "Quota:" in output
    assert "- quota unavailable" in output


def test_compute_metrics_for_symbol_reuses_fact_and_market_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "metric-cache.db"
    _seed_listing(db_path, "AAA.US", currency="USD")
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    seed_facts(
        db_path,
        "AAA.US",
        [
            make_fact(concept="AssetsCurrent", end_date="2024-12-31", value=500.0),
            make_fact(
                concept="AssetsCurrent",
                end_date="2023-12-31",
                value=450.0,
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date="2024-12-31",
                value=2.0,
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date="2023-12-31",
                value=1.5,
            ),
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    seed_price(db_path, "AAA.US", "2024-12-31", price=25.0)

    # The metric layer keys on ``listing_id`` now; resolve the seeded listing's
    # id so the cached repos (constructed with this id) short-circuit the metric's
    # reads to memory -- which is exactly what the call-count assertions verify.
    listing_id = resolve_listing_id(db_path, "AAA.US")
    assert listing_id is not None

    fact_calls: dict[str, int] = {"count": 0}
    market_calls: dict[str, int] = {"count": 0}
    original_facts_for_id = FinancialFactsRepository.facts_for_id
    original_latest_snapshot_by_id = MarketDataRepository.latest_snapshot_by_id

    def counting_facts_for_id(
        self: FinancialFactsRepository, listing_id: int
    ) -> list[FactRecord]:
        fact_calls["count"] += 1
        return original_facts_for_id(self, listing_id)

    def counting_latest_snapshot_by_id(
        self: MarketDataRepository, listing_id: int
    ) -> PriceData | None:
        market_calls["count"] += 1
        return original_latest_snapshot_by_id(self, listing_id)

    monkeypatch.setattr(
        FinancialFactsRepository,
        "facts_for_id",
        counting_facts_for_id,
    )
    monkeypatch.setattr(
        MarketDataRepository,
        "latest_snapshot_by_id",
        counting_latest_snapshot_by_id,
    )

    class RepeatedFactsMetric:
        id = "repeat_facts"
        required_concepts = ("AssetsCurrent",)
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            latest_a = repo.latest_fact(listing_id, "AssetsCurrent")
            latest_b = repo.latest_fact(listing_id, "AssetsCurrent")
            series_a = repo.facts_for_concept(listing_id, "EarningsPerShare", "FY")
            series_b = repo.facts_for_concept(listing_id, "EarningsPerShare", "FY")
            # The test seeds the required facts, so both reads resolve; assert to
            # narrow the optional for the type checker.
            assert latest_a is not None and latest_b is not None
            return MetricResult(
                listing_id=listing_id,
                metric_id=self.id,
                value=latest_a.value + latest_b.value + len(series_a) + len(series_b),
                as_of=latest_a.end_date,
            )

    class RepeatedMarketMetric:
        id = "repeat_market"
        required_concepts = ()
        uses_market_data = True

        def compute(
            self,
            listing_id: int,
            repo: RegionFactsRepository,
            market_repo: MarketDataRepository,
        ) -> MetricResult | None:
            snapshot_a = market_repo.latest_snapshot_by_id(listing_id)
            snapshot_b = market_repo.latest_snapshot_by_id(listing_id)
            price = market_repo.latest_price_by_id(listing_id)
            # The test seeds a price, so every read resolves; assert to narrow the
            # optionals (snapshots and the (currency, price) tuple) for mypy.
            assert snapshot_a is not None and snapshot_b is not None
            assert price is not None
            return MetricResult(
                listing_id=listing_id,
                metric_id=self.id,
                value=snapshot_a.price + snapshot_b.price + price[1],
                as_of=snapshot_a.as_of,
            )

    patch_cli(
        monkeypatch,
        "REGISTRY",
        {
            RepeatedFactsMetric.id: RepeatedFactsMetric,
            RepeatedMarketMetric.id: RepeatedMarketMetric,
        },
    )

    result = cli._compute_metrics_for_symbol(
        "AAA.US",
        listing_id,
        [RepeatedFactsMetric.id, RepeatedMarketMetric.id],
        FinancialFactsRepository(db_path),
        MarketDataRepository(db_path),
    )

    assert result.computed_count == 2
    assert fact_calls["count"] == 1
    assert market_calls["count"] == 1


def test_compute_metrics_for_symbol_matches_real_metrics(tmp_path: Path) -> None:
    db_path = tmp_path / "metric-correctness.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(
                symbol="AAA.US",
                security_name="AAA Inc",
                exchange="NYSE",
                currency="USD",
            )
        ],
        provider="SEC",
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    recent = (date.today() - timedelta(days=15)).isoformat()
    current_year = date.today().year
    seed_facts(
        db_path,
        "AAA.US",
        [
            make_fact(concept="AssetsCurrent", end_date=recent, value=500.0),
            make_fact(
                concept="LiabilitiesCurrent",
                end_date=recent,
                value=200.0,
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{current_year - 6}-12-31",
                value=1.0,
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{current_year - 5}-12-31",
                value=1.1,
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{current_year - 4}-12-31",
                value=1.2,
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{current_year - 3}-12-31",
                value=1.3,
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{current_year - 2}-12-31",
                value=1.4,
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{current_year - 1}-12-31",
                value=1.5,
            ),
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    seed_price(db_path, "AAA.US", recent, price=25.0, currency="USD")
    _seed_share_count(db_path, "AAA.US", recent, 2500.0 / 25.0)

    # Compute the reference values directly against the real metrics using the
    # same ``listing_id`` the batch path threads, so the two must agree.
    listing_id = resolve_listing_id(db_path, "AAA.US")
    assert listing_id is not None
    metric_ids = ["working_capital", "market_cap", "eps_6y_avg"]
    expected: dict[str, tuple[float, str]] = {}
    plain_fact_repo = RegionFactsRepository(FinancialFactsRepository(db_path))
    plain_market_repo = MarketDataRepository(db_path)
    for metric_id in metric_ids:
        metric = REGISTRY[metric_id]()
        if getattr(metric, "uses_market_data", False):
            result = metric.compute(listing_id, plain_fact_repo, plain_market_repo)
        else:
            result = metric.compute(listing_id, plain_fact_repo)
        expected[metric_id] = (result.value, result.as_of)

    computed = cli._compute_metrics_for_symbol(
        "AAA.US",
        listing_id,
        metric_ids,
        FinancialFactsRepository(db_path),
        MarketDataRepository(db_path),
    )

    assert computed.computed_count == 3
    assert {
        metric_id: (value, as_of)
        for _, metric_id, value, as_of, _, _, _ in computed.rows
    } == expected


def test_compute_metrics_for_symbol_collects_currency_invariant_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class GoodMetric:
        id = "good_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            return MetricResult(
                listing_id=listing_id,
                metric_id=self.id,
                value=1.0,
                as_of="2024-01-01",
            )

    class BadMetric:
        id = "bad_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            raise MetricCurrencyInvariantError(
                metric_id=self.id,
                listing_id=listing_id,
                input_name="Assets",
                reason_code="currency_mismatch",
                expected_currency="USD",
                actual_currency="EUR",
                as_of="2024-01-01",
            )

    patch_cli(
        monkeypatch,
        "REGISTRY",
        {
            GoodMetric.id: GoodMetric,
            BadMetric.id: BadMetric,
        },
    )

    # A fact repository that returns no facts but resolves a listing currency,
    # so the invariant-raising metric is the only failure. Subclassing the real
    # repository (with a no-op ``__init__``) keeps the type contract intact
    # without opening a database.
    class _FakeFactsRepository(FinancialFactsRepository):
        def __init__(self) -> None:
            pass

        def facts_for_id(self, listing_id: int) -> list[FactRecord]:
            return []

        def latest_fact(self, listing_id: int, concept: str) -> FactRecord | None:
            return None

        def facts_for_concept(
            self,
            listing_id: int,
            concept: str,
            fiscal_period: str | None = None,
            limit: int | None = None,
        ) -> list[FactRecord]:
            return []

        def ticker_currency_by_id(self, listing_id: int) -> str | None:
            return "USD"

    result = cli._compute_metrics_for_symbol(
        "AAA.US",
        LISTING_ID,
        [GoodMetric.id, BadMetric.id],
        _FakeFactsRepository(),
    )

    assert result.computed_count == 1
    assert [row[1] for row in result.rows] == ["good_metric"]
    assert len(result.failures) == 1
    assert result.failures[0].metric_id == "bad_metric"
    assert result.failures[0].reason.startswith("currency invariant:")


def test_compute_metrics_for_symbol_populates_reason_detail_from_first_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guard failures persist both the scrubbed template and the raw warning.

    ``reason_code`` must stay the templated grouping key (numbers scrubbed to
    ``<n>``) while ``reason_detail`` carries the untemplated first warning so a
    status row says *which* year/count tripped the guard.
    """

    class GuardedMetric:
        id = "guarded_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            logging.getLogger("pyvalue.metrics.guarded").warning(
                "guarded_metric: need %s FY values for listing_id=%s, found %s",
                10,
                listing_id,
                3,
            )
            return None

    class SilentMetric:
        id = "silent_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            return None

    patch_cli(
        monkeypatch,
        "REGISTRY",
        {
            GuardedMetric.id: GuardedMetric,
            SilentMetric.id: SilentMetric,
        },
    )

    class _FakeFactsRepository(FinancialFactsRepository):
        def __init__(self) -> None:
            pass

        def facts_for_id(self, listing_id: int) -> list[FactRecord]:
            return []

        def latest_fact(self, listing_id: int, concept: str) -> FactRecord | None:
            return None

        def facts_for_concept(
            self,
            listing_id: int,
            concept: str,
            fiscal_period: str | None = None,
            limit: int | None = None,
        ) -> list[FactRecord]:
            return []

        def ticker_currency_by_id(self, listing_id: int) -> str | None:
            return "USD"

    # The collector only sees warnings while attached to the root logger — the
    # batch driver (and explain-metric) attach it the same way in production.
    collector = _MetricWarningCollector()
    root_logger = logging.getLogger()
    root_logger.addHandler(collector)
    try:
        result = cli._compute_metrics_for_symbol(
            "AAA.US",
            LISTING_ID,
            [GuardedMetric.id, SilentMetric.id],
            _FakeFactsRepository(),
            warning_collector=collector,
        )
    finally:
        root_logger.removeHandler(collector)

    attempts_by_id = {attempt.metric_id: attempt for attempt in result.attempts}
    guarded = attempts_by_id[GuardedMetric.id]
    assert guarded.status == "failure"
    assert (
        guarded.reason_code
        == "guarded_metric: need <n> FY values for listing_id=<n>, found <n>"
    )
    assert (
        guarded.reason_detail
        == f"guarded_metric: need 10 FY values for listing_id={LISTING_ID}, found 3"
    )

    # A guard that emits no warning keeps detail empty rather than inventing one.
    silent = attempts_by_id[SilentMetric.id]
    assert silent.status == "failure"
    assert silent.reason_code == "no warning emitted"
    assert silent.reason_detail is None


def test_suppress_console_metric_warnings_filters_only_metric_noise(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    log_dir = tmp_path / "logs"
    clear_root_logging_handlers()
    cli.setup_logging(log_dir=log_dir)
    try:
        with cli.suppress_console_metric_warnings(True):
            logging.getLogger("pyvalue.metrics.test").warning("metric noise")
            # Per-listing INFO diagnostics (documented-cap notices, FX
            # conversion traces) are metric noise too -- file-only.
            logging.getLogger("pyvalue.metrics.test").info("metric info noise")
            # Errors from metric loggers must still surface on the console.
            logging.getLogger("pyvalue.metrics.test").error("metric error")
            logging.getLogger("pyvalue.cli").warning(
                "Metric %s could not be computed for %s",
                "dummy_metric",
                "AAA.US",
            )
            logging.getLogger("pyvalue.cli").info("Operational info")
            logging.getLogger("pyvalue.cli").warning("Operational warning")

        captured = capsys.readouterr()
        assert "metric noise" not in captured.err
        assert "metric info noise" not in captured.err
        assert "metric error" in captured.err
        assert (
            "Metric dummy_metric could not be computed for AAA.US" not in captured.err
        )
        assert "Operational info" in captured.err
        assert "Operational warning" in captured.err

        log_text = (log_dir / "pyvalue.log").read_text(encoding="utf-8")
        assert "metric noise" in log_text
        assert "metric info noise" in log_text
        assert "metric error" in log_text
        assert "Metric dummy_metric could not be computed for AAA.US" in log_text
        assert "Operational info" in log_text
        assert "Operational warning" in log_text
    finally:
        clear_root_logging_handlers()


def test_suppress_console_missing_fx_warnings_filters_only_missing_fx_noise(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    log_dir = tmp_path / "logs"
    clear_root_logging_handlers()
    cli.setup_logging(log_dir=log_dir)
    try:
        with cli.suppress_console_missing_fx_warnings(True):
            logging.getLogger("pyvalue.money.fx").warning(
                "Missing FX rate | provider=%s base=%s quote=%s as_of=%s operation=get_fx_rate",
                "EODHD",
                "NLG",
                "EUR",
                "2000-06-30",
            )
            logging.getLogger("pyvalue.money").warning(
                "Missing FX rate for monetary conversion | operation=%s symbol=%s field=%s from=%s to=%s as_of=%s",
                "listing_currency_alignment",
                "AALB.AS",
                "Assets",
                "NLG",
                "EUR",
                "2000-06-30",
            )
            logging.getLogger("pyvalue.money.fx").warning(
                "Stale FX rate used | provider=%s base=%s quote=%s requested_as_of=%s rate_date=%s age_days=%s source_kind=%s",
                "EODHD",
                "EUR",
                "USD",
                "2024-01-10",
                "2024-01-01",
                9,
                "provider",
            )
            logging.getLogger("pyvalue.cli").warning("Operational warning")

        captured = capsys.readouterr()
        assert "Missing FX rate | provider=EODHD base=NLG quote=EUR" not in captured.err
        assert "Missing FX rate for monetary conversion" not in captured.err
        assert "Stale FX rate used" in captured.err
        assert "Operational warning" in captured.err

        log_text = (log_dir / "pyvalue.log").read_text(encoding="utf-8")
        assert "Missing FX rate | provider=EODHD base=NLG quote=EUR" in log_text
        assert "Missing FX rate for monetary conversion" in log_text
        assert "Stale FX rate used" in log_text
        assert "Operational warning" in log_text
    finally:
        clear_root_logging_handlers()


def test_cmd_compute_metrics_stage_suppresses_metric_warnings_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "metric-stage-suppressed.db"
    log_dir = tmp_path / "logs"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            return None

    patch_cli(monkeypatch, "REGISTRY", {DummyMetric.id: DummyMetric})
    patch_cli(monkeypatch, "_metric_worker_count", lambda total: 1)
    patch_cli(monkeypatch, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)
    clear_root_logging_handlers()
    cli.setup_logging(log_dir=log_dir)
    try:
        rc = cli.cmd_compute_metrics_stage(
            database=str(db_path),
            symbols=["AAA.US"],
            exchange_codes=None,
            all_supported=False,
            metric_ids=None,
        )
    finally:
        clear_root_logging_handlers()

    assert rc == 0
    captured = capsys.readouterr()
    assert "could not be computed" not in captured.err
    assert (
        "Progress: [####################] 1/1 symbols complete (100.0%)" in captured.out
    )
    log_text = (log_dir / "pyvalue.log").read_text(encoding="utf-8")
    assert "Metric dummy_metric could not be computed for AAA.US" in log_text


def test_cmd_compute_metrics_stage_can_show_metric_warnings_on_console(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "metric-stage-show-warnings.db"
    log_dir = tmp_path / "logs"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            return None

    patch_cli(monkeypatch, "REGISTRY", {DummyMetric.id: DummyMetric})
    patch_cli(monkeypatch, "_metric_worker_count", lambda total: 1)
    clear_root_logging_handlers()
    cli.setup_logging(log_dir=log_dir)
    try:
        rc = cli.cmd_compute_metrics_stage(
            database=str(db_path),
            symbols=["AAA.US"],
            exchange_codes=None,
            all_supported=False,
            metric_ids=None,
            show_metric_warnings=True,
        )
    finally:
        clear_root_logging_handlers()

    assert rc == 0
    assert (
        "Metric dummy_metric could not be computed for AAA.US"
        in capsys.readouterr().err
    )


def test_cmd_compute_metrics_stage_prints_currency_invariant_summary_when_warnings_suppressed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "metric-stage-currency-summary.db"
    log_dir = tmp_path / "logs"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )

    class BadMetric:
        id = "bad_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            raise MetricCurrencyInvariantError(
                metric_id=self.id,
                listing_id=listing_id,
                input_name="listing_currency",
                reason_code="missing_trading_currency",
            )

    patch_cli(monkeypatch, "REGISTRY", {BadMetric.id: BadMetric})
    patch_cli(monkeypatch, "_metric_worker_count", lambda total: 1)
    patch_cli(monkeypatch, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)
    clear_root_logging_handlers()
    cli.setup_logging(log_dir=log_dir)
    try:
        rc = cli.cmd_compute_metrics_stage(
            database=str(db_path),
            symbols=["AAA.US"],
            exchange_codes=None,
            all_supported=False,
            metric_ids=None,
        )
    finally:
        clear_root_logging_handlers()

    assert rc == 0
    captured = capsys.readouterr()
    assert "could not be computed" not in captured.err
    assert "Metric currency invariant failures:" in captured.out
    assert "- bad_metric: currency invariant:" in captured.out
    assert "example=AAA.US" in captured.out
    log_text = (log_dir / "pyvalue.log").read_text(encoding="utf-8")
    assert "Metric currency invariant failures:" not in log_text


def test_compute_metrics_batch_worker_suppresses_metric_warnings_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "metric-batch-worker-suppressed.db"
    log_dir = tmp_path / "logs"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            return None

    patch_cli(monkeypatch, "REGISTRY", {DummyMetric.id: DummyMetric})
    clear_root_logging_handlers()
    cli.setup_logging(log_dir=log_dir)
    try:
        cli._initialize_metric_read_schema(Path(db_path), include_market_data=False)
        # The batch worker now keys on (listing_id, display_symbol) pairs.
        ids = SecurityRepository(db_path).resolve_ids_many(["AAA.US", "BBB.US"])
        results = cli._compute_metrics_for_symbol_batch_worker(
            str(db_path),
            [(ids["AAA.US"], "AAA.US"), (ids["BBB.US"], "BBB.US")],
            [DummyMetric.id],
        )
    finally:
        clear_root_logging_handlers()

    assert [result.symbol for result in results] == ["AAA.US", "BBB.US"]
    assert "could not be computed" not in capsys.readouterr().err
    log_text = (log_dir / "pyvalue.log").read_text(encoding="utf-8")
    assert "Metric dummy_metric could not be computed for AAA.US" in log_text
    assert "Metric dummy_metric could not be computed for BBB.US" in log_text


def test_compute_metric_batch_results_uses_share_facts_for_market_cap(
    tmp_path: Path,
) -> None:
    # Market cap is derived (a share-count fact x the price as of that fact's
    # date), so the batch path preloads the share-count concepts and the metric
    # computes from them rather than from a removed stored column.
    db_path = tmp_path / "metric-batch-market-cap.db"
    recent_date = (date.today() - timedelta(days=1)).isoformat()
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    # 10 shares x 12.0 price = 120.0 market cap, co-dated at recent_date.
    store_market_data(
        db_path,
        "AAA.US",
        recent_date,
        price=12.0,
        market_cap=120.0,
        currency="USD",
    )

    # The batch driver is keyed by (listing_id, display_symbol) pairs and writes
    # id-led rows; resolve the listing id the scope would have carried.
    listing_id = resolve_listing_id(db_path, "AAA.US")
    assert listing_id is not None
    results = cli._compute_metric_batch_results(
        [(listing_id, "AAA.US")],
        ["market_cap"],
        FinancialFactsRepository(db_path),
        MarketDataRepository(db_path),
    )

    assert len(results) == 1
    assert results[0].rows == (
        (listing_id, "market_cap", 120.0, recent_date, "monetary", "USD", None),
    )


def test_compute_metric_batch_results_skips_resolution_when_ids_supplied(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # When the caller hands in the scope-resolved listing ids, the batch read
    # path must perform no symbol->id resolution at all -- the ids ride straight
    # into the facts/refresh preloads -- while still computing the same values
    # as the self-resolving path.
    db_path = tmp_path / "metric-batch-supplied-ids.db"
    recent_date = (date.today() - timedelta(days=1)).isoformat()
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    seed_facts(
        db_path,
        "AAA.US",
        [
            make_fact(
                symbol="AAA.US",
                concept="AssetsCurrent",
                end_date=recent_date,
                value=10.0,
            ),
            make_fact(
                symbol="AAA.US",
                concept="LiabilitiesCurrent",
                end_date=recent_date,
                value=3.0,
            ),
        ],
    )
    seed_facts(
        db_path,
        "BBB.US",
        [
            make_fact(
                symbol="BBB.US",
                concept="AssetsCurrent",
                end_date=recent_date,
                value=8.0,
            ),
            make_fact(
                symbol="BBB.US",
                concept="LiabilitiesCurrent",
                end_date=recent_date,
                value=2.0,
            ),
        ],
    )

    # Resolve once up front (this call is intentionally before the counter is
    # installed) to build the id map the way scope resolution would.
    ids_by_symbol = SecurityRepository(db_path).resolve_ids_many(["AAA.US", "BBB.US"])
    assert set(ids_by_symbol) == {"AAA.US", "BBB.US"}

    calls: dict[str, int] = {"resolve_ids_many": 0}
    original_resolve_ids_many = SecurityRepository.resolve_ids_many

    def counting_resolve_ids_many(
        self: SecurityRepository,
        symbols: Sequence[str],
        chunk_size: int = 500,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> dict[str, int]:
        calls["resolve_ids_many"] += 1
        return original_resolve_ids_many(
            self, symbols, chunk_size=chunk_size, connection=connection
        )

    monkeypatch.setattr(
        SecurityRepository, "resolve_ids_many", counting_resolve_ids_many
    )

    results = cli._compute_metric_batch_results(
        [(ids_by_symbol["AAA.US"], "AAA.US"), (ids_by_symbol["BBB.US"], "BBB.US")],
        ["working_capital"],
        FinancialFactsRepository(db_path),
        None,
    )

    assert calls == {"resolve_ids_many": 0}
    assert [result.computed_count for result in results] == [1, 1]
    value_by_symbol = {result.symbol: result.rows[0][2] for result in results}
    # working_capital = AssetsCurrent - LiabilitiesCurrent.
    assert value_by_symbol == {"AAA.US": 7.0, "BBB.US": 6.0}


def test_cmd_run_screen_stage_suppresses_metric_warnings_on_console_by_default(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-stage-suppressed.db"
    log_dir = tmp_path / "logs"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
"""
    )

    clear_root_logging_handlers()
    cli.setup_logging(log_dir=log_dir)
    try:
        rc = cli.cmd_run_screen_stage(
            config_path=str(screen_path),
            database=str(db_path),
            symbols=["AAA.US"],
            exchange_codes=None,
            all_supported=False,
            output_csv=None,
        )
    finally:
        clear_root_logging_handlers()

    assert rc == 1
    captured = capsys.readouterr()
    assert "run compute-metrics first" not in captured.err
    assert "Progress:" not in captured.out
    log_text = (log_dir / "pyvalue.log").read_text(encoding="utf-8")
    assert (
        "Metric working_capital missing for AAA.US; run compute-metrics first"
        in log_text
    )


def test_cmd_run_screen_stage_can_show_metric_warnings_on_console(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-stage-show-warnings.db"
    log_dir = tmp_path / "logs"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
"""
    )

    clear_root_logging_handlers()
    cli.setup_logging(log_dir=log_dir)
    try:
        rc = cli.cmd_run_screen_stage(
            config_path=str(screen_path),
            database=str(db_path),
            symbols=["AAA.US"],
            exchange_codes=None,
            all_supported=False,
            output_csv=None,
            show_metric_warnings=True,
        )
    finally:
        clear_root_logging_handlers()

    assert rc == 1
    assert "Metric working_capital missing for AAA.US; run compute-metrics first" in (
        capsys.readouterr().err
    )


def test_cmd_run_screen_stage_carries_scope_listing_ids(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """run-screen carries scope listing ids into its scope-wide metric reads.

    ``_resolve_canonical_scope_listings`` already holds every listing_id, so the
    screen's metric/fact/market reads must not re-resolve symbol->listing_id. We
    install a counter over the bulk resolver and assert a multi-symbol screen
    never calls it. This fails on the pre-fix code (which resolved the scope to
    symbols and re-resolved them inside the scope-wide metric reads).

    Author: Emre Tezel
    """
    db_path = tmp_path / "screen-stage-carry-ids.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    seed_metric(
        db_path, "AAA.US", "current_ratio", 2.0, "2026-03-29", unit_kind="ratio"
    )
    seed_metric(
        db_path, "BBB.US", "current_ratio", 1.5, "2026-03-29", unit_kind="ratio"
    )

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Liquidity"
    left:
      metric: current_ratio
    operator: ">="
    right:
      value: 1.0
"""
    )

    calls = {"resolve_ids_many": 0}
    original_resolve_ids_many = SecurityRepository.resolve_ids_many

    def counting_resolve_ids_many(
        self: SecurityRepository,
        symbols: Sequence[str],
        chunk_size: int = 500,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> dict[str, int]:
        calls["resolve_ids_many"] += 1
        return original_resolve_ids_many(
            self, symbols, chunk_size=chunk_size, connection=connection
        )

    monkeypatch.setattr(
        SecurityRepository, "resolve_ids_many", counting_resolve_ids_many
    )

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=None,
    )

    assert rc == 0
    assert calls == {"resolve_ids_many": 0}
    output = capsys.readouterr().out
    assert "AAA.US" in output
    assert "BBB.US" in output


def test_cmd_compute_metrics_stage_symbol_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "metric-stage-symbol.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            # ``compute`` keys on ``listing_id`` now and no longer sees the symbol;
            # the seeded symbols (AAA.US / BBB.US) are both length 6, so the fixed
            # 6.0 reproduces the value the prior ``len(symbol)`` produced and the
            # scope/write assertions remain meaningful.
            return MetricResult(
                listing_id=listing_id,
                metric_id=self.id,
                value=6.0,
                as_of="2024-01-01",
            )

    patch_cli(monkeypatch, "REGISTRY", {DummyMetric.id: DummyMetric})

    rc = cli.cmd_compute_metrics_stage(
        database=str(db_path),
        symbols=["BBB.US"],
        exchange_codes=None,
        all_supported=False,
        metric_ids=None,
    )

    assert rc == 0
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_bbb is not None
    assert repo.fetch_by_id(id_aaa, "dummy_metric") is None
    assert repo.fetch_by_id(id_bbb, "dummy_metric") == (6.0, "2024-01-01")


def test_cmd_compute_metrics_stage_threads_listing_ids_without_resolving(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # End-to-end guard for listing_id threading: a whole compute-metrics run
    # resolves symbol->listing_id ZERO times because the scope query already
    # carries the ids into both the read preloads and the writer. Forced serial
    # so the single-process counter observes the worker reads too (parallel
    # workers run in child processes the patch can't see); serial vs parallel
    # does not change whether resolution happens.
    db_path = tmp_path / "metric-stage-no-resolve.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            # ``compute`` keys on ``listing_id`` now and no longer sees the symbol;
            # the seeded symbols (AAA.US / BBB.US) are both length 6, so the fixed
            # 6.0 reproduces the value the prior ``len(symbol)`` produced and the
            # scope/write assertions remain meaningful.
            return MetricResult(
                listing_id=listing_id,
                metric_id=self.id,
                value=6.0,
                as_of="2024-01-01",
            )

    patch_cli(monkeypatch, "REGISTRY", {DummyMetric.id: DummyMetric})
    monkeypatch.setattr("pyvalue.cli.metrics._metric_worker_count", lambda total: 1)

    calls: dict[str, int] = {"resolve_ids_many": 0}
    original_resolve_ids_many = SecurityRepository.resolve_ids_many

    def counting_resolve_ids_many(
        self: SecurityRepository,
        symbols: Sequence[str],
        chunk_size: int = 500,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> dict[str, int]:
        calls["resolve_ids_many"] += 1
        return original_resolve_ids_many(
            self, symbols, chunk_size=chunk_size, connection=connection
        )

    monkeypatch.setattr(
        SecurityRepository, "resolve_ids_many", counting_resolve_ids_many
    )

    rc = cli.cmd_compute_metrics_stage(
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
        metric_ids=None,
    )

    assert rc == 0
    assert calls == {"resolve_ids_many": 0}
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    # Resolve ids only after the zero-resolution assertion above: these test-side
    # lookups run after the counter is checked, so they do not affect the guard.
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert repo.fetch_by_id(id_aaa, "dummy_metric") == (6.0, "2024-01-01")
    assert repo.fetch_by_id(id_bbb, "dummy_metric") == (6.0, "2024-01-01")


def test_cmd_compute_metrics_stage_exchange_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "metric-stage-exchange.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    store_catalog_listings(
        db_path,
        "LSE",
        [Listing(symbol="BBB.LSE", security_name="BBB PLC", exchange="LSE")],
        provider="EODHD",
    )

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            return MetricResult(
                listing_id=listing_id, metric_id=self.id, value=1.0, as_of="2024-01-01"
            )

    patch_cli(monkeypatch, "REGISTRY", {DummyMetric.id: DummyMetric})

    rc = cli.cmd_compute_metrics_stage(
        database=str(db_path),
        symbols=None,
        exchange_codes=["LSE"],
        all_supported=False,
        metric_ids=None,
    )

    assert rc == 0
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    id_bbb = resolve_listing_id(db_path, "BBB.LSE")
    assert id_bbb is not None
    assert repo.fetch_by_id(id_aaa, "dummy_metric") is None
    assert repo.fetch_by_id(id_bbb, "dummy_metric") == (1.0, "2024-01-01")


def test_cmd_compute_metrics_stage_all_supported_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "metric-stage-all-supported.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    store_catalog_listings(
        db_path,
        "LSE",
        [Listing(symbol="BBB.LSE", security_name="BBB PLC", exchange="LSE")],
        provider="EODHD",
    )

    store_market_data(
        db_path,
        "AAA.US",
        "2024-01-01",
        price=10.0,
        market_cap=120.0,
        currency="USD",
    )
    store_market_data(
        db_path,
        "BBB.LSE",
        "2024-01-01",
        price=20.0,
        market_cap=210.0,
        currency="GBP",
    )

    rc = cli.cmd_compute_metrics_stage(
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
        metric_ids=["market_cap"],
    )

    assert rc == 0
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    id_bbb = resolve_listing_id(db_path, "BBB.LSE")
    assert id_bbb is not None
    assert repo.fetch_by_id(id_aaa, "market_cap") == (120.0, "2024-01-01")
    assert repo.fetch_by_id(id_bbb, "market_cap") == (210.0, "2024-01-01")


def test_cmd_compute_metrics_stage_does_not_reconcile_or_mutate_listing_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """compute-metrics is a pure reader of primary_listing_status.

    It previously ran a full reconcile while resolving its canonical scope; it
    must not any longer. AAA.LSE is left deliberately 'unknown' (a reconcile
    would flip it to 'secondary' via its PrimaryTicker) and must stay untouched,
    and the reconcile entrypoint must never be called. Regression for the
    canonical-scope read-only refactor.
    """
    db_path = tmp_path / "metric-stage-readonly-status.db"
    store_catalog_listings(
        db_path,
        "LSE",
        [
            Listing(symbol="AAA.LSE", security_name="AAA PLC", exchange="LSE"),
            Listing(symbol="BBB.LSE", security_name="BBB PLC", exchange="LSE"),
        ],
        provider="EODHD",
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.LSE",
        {"General": {"Name": "AAA plc", "PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.LSE",
        {"General": {"Name": "BBB plc"}},
        exchange="LSE",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE listing SET primary_listing_status = "
            "CASE WHEN symbol = 'AAA' THEN 'unknown' ELSE 'primary' END"
        )
    store_market_data(
        db_path, "AAA.LSE", "2024-01-01", price=20.0, market_cap=210.0, currency="GBP"
    )
    store_market_data(
        db_path, "BBB.LSE", "2024-01-01", price=20.0, market_cap=210.0, currency="GBP"
    )

    def fail_reconcile(*args: object, **kwargs: object) -> None:
        pytest.fail("compute-metrics must not reconcile listing status")

    patch_cli(monkeypatch, "_reconcile_eodhd_listing_scope", fail_reconcile)

    rc = cli.cmd_compute_metrics_stage(
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
        metric_ids=["market_cap"],
    )

    assert rc == 0
    with sqlite3.connect(db_path) as conn:
        statuses = conn.execute(
            """
            SELECT l.symbol || '.' || e.exchange_code, l.primary_listing_status
            FROM listing l
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            ORDER BY l.symbol || '.' || e.exchange_code
            """
        ).fetchall()

    assert statuses == [
        ("AAA.LSE", "unknown"),
        ("BBB.LSE", "primary"),
    ]


def test_cmd_compute_metrics_stage_parallel_with_inline_executor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "metric-stage-inline.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            return MetricResult(
                listing_id=listing_id, metric_id=self.id, value=1.0, as_of="2024-01-01"
            )

    class InlineExecutor:
        def submit(
            self, fn: Callable[..., object], *args: object, **kwargs: object
        ) -> Future[object]:
            future: Future[object] = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            return None

    def reverse_as_completed(
        futures: dict[Future[object], str],
    ) -> list[Future[object]]:
        # The CLI maps each submitted future to its symbol; yielding them in
        # reverse symbol order forces out-of-submission-order completion.
        return [
            future
            for future, _ in sorted(
                futures.items(), key=lambda item: item[1], reverse=True
            )
        ]

    patch_cli(monkeypatch, "REGISTRY", {DummyMetric.id: DummyMetric})
    patch_cli(monkeypatch, "_metric_worker_count", lambda total: 2)
    patch_cli(
        monkeypatch,
        "_create_process_pool_executor",
        lambda max_workers: InlineExecutor(),
    )
    patch_cli(monkeypatch, "as_completed", reverse_as_completed)
    patch_cli(monkeypatch, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)

    rc = cli.cmd_compute_metrics_stage(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=None,
    )

    assert rc == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert [line for line in output_lines if line.startswith("Progress:")] == [
        "Progress: [##########----------] 1/2 symbols complete (50.0%)",
        "Progress: [####################] 2/2 symbols complete (100.0%)",
    ]
    assert not any(line.startswith("[") for line in output_lines)


def test_cmd_compute_metrics_stage_parallel_partial_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "metric-stage-inline-failure.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )

    class InlineExecutor:
        def submit(
            self, fn: Callable[..., object], *args: object, **kwargs: object
        ) -> Future[object]:
            future: Future[object] = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            return None

    # The worker payload is now the picklable (listing_id, display_symbol) pair.
    def fake_worker(
        database: str,
        listing: tuple[int, str],
        metric_ids: Sequence[str],
        suppress_metric_warnings: bool = True,
    ) -> cli._ComputedMetricsResult:
        listing_id, symbol = listing
        if symbol == "BBB.US":
            raise ValueError("boom")
        return cli._ComputedMetricsResult(
            symbol=symbol,
            listing_id=listing_id,
            rows=(
                (listing_id, "dummy_metric", 1.0, "2024-01-01", "ratio", None, None),
            ),
            computed_count=1,
        )

    class DummyMetric:
        id = "dummy_metric"
        uses_market_data = False

    patch_cli(monkeypatch, "_metric_worker_count", lambda total: 2)
    patch_cli(
        monkeypatch,
        "_create_process_pool_executor",
        lambda max_workers: InlineExecutor(),
    )
    patch_cli(monkeypatch, "REGISTRY", {DummyMetric.id: DummyMetric})
    patch_cli(monkeypatch, "_compute_metrics_for_symbol_worker", fake_worker)
    patch_cli(monkeypatch, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)

    ids = SecurityRepository(db_path).resolve_ids_many(["AAA.US", "BBB.US"])
    rc = cli._run_metric_computation(
        database=str(db_path),
        listings=[(ids["AAA.US"], "AAA.US"), (ids["BBB.US"], "BBB.US")],
        metric_ids=["dummy_metric"],
        cancelled_message="\nMetric computation cancelled by user.",
    )

    assert rc == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert [line for line in output_lines if line.startswith("Progress:")] == [
        "Progress: [##########----------] 1/2 symbols complete (50.0%)",
        "Progress: [####################] 2/2 symbols complete (100.0%)",
    ]
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_bbb is not None
    assert repo.fetch_by_id(id_aaa, "dummy_metric") == (1.0, "2024-01-01")
    assert repo.fetch_by_id(id_bbb, "dummy_metric") is None


def test_run_metric_computation_interrupts_cleanly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "metric-stage-interrupt.db"
    # Persisting a metric row resolves the listing; seed both symbols in ONE
    # call (listing.currency is NOT NULL, and a second same-exchange replace
    # would treat the first symbol as delisted and purge its listing).
    _seed_listing(db_path, ("AAA.US", "BBB.US"), currency="USD")

    class DummyMetric:
        id = "dummy_metric"
        uses_market_data = False

    class InlineExecutor:
        def __init__(self) -> None:
            self.shutdown_calls: list[tuple[bool, bool]] = []

        def submit(
            self, fn: Callable[..., object], *args: object, **kwargs: object
        ) -> Future[object]:
            future: Future[object] = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            self.shutdown_calls.append((wait, cancel_futures))

    def fake_worker(
        database: str,
        listing: tuple[int, str],
        metric_ids: Sequence[str],
        suppress_metric_warnings: bool = True,
    ) -> cli._ComputedMetricsResult:
        listing_id, symbol = listing
        return cli._ComputedMetricsResult(
            symbol=symbol,
            listing_id=listing_id,
            rows=(
                (listing_id, "dummy_metric", 1.0, "2024-01-01", "ratio", None, None),
            ),
            computed_count=1,
        )

    def interrupting_as_completed(
        futures: Iterable[Future[object]],
    ) -> Iterator[Future[object]]:
        yielded = False
        for future in futures:
            if not yielded:
                yielded = True
                yield future
                raise KeyboardInterrupt

    executor = InlineExecutor()
    patch_cli(monkeypatch, "REGISTRY", {DummyMetric.id: DummyMetric})
    patch_cli(monkeypatch, "_metric_worker_count", lambda total: 2)
    monkeypatch.setattr(MetricsRepository, "ensure_wal_mode", lambda self: "wal")
    patch_cli(
        monkeypatch,
        "_create_process_pool_executor",
        lambda max_workers: executor,
    )
    patch_cli(monkeypatch, "_compute_metrics_for_symbol_worker", fake_worker)
    patch_cli(monkeypatch, "as_completed", interrupting_as_completed)
    patch_cli(monkeypatch, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)

    ids = SecurityRepository(db_path).resolve_ids_many(["AAA.US", "BBB.US"])
    rc = cli._run_metric_computation(
        database=str(db_path),
        listings=[(ids["AAA.US"], "AAA.US"), (ids["BBB.US"], "BBB.US")],
        metric_ids=["dummy_metric"],
        cancelled_message="\nMetric computation cancelled by user.",
    )

    assert rc == 1
    output_lines = capsys.readouterr().out.splitlines()
    assert "Metric computation cancelled by user." in output_lines
    assert "Computed metrics for 2 symbols in" not in "\n".join(output_lines)
    assert (
        "Progress: [##########----------] 1/2 symbols complete (50.0%)" in output_lines
    )
    assert executor.shutdown_calls == [(False, True)]
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_bbb is not None
    assert repo.fetch_by_id(id_aaa, "dummy_metric") == (1.0, "2024-01-01")
    assert repo.fetch_by_id(id_bbb, "dummy_metric") is None


def test_cmd_compute_metrics_stage_falls_back_to_serial_without_wal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "metric-stage-no-wal.db"
    recent_date = (date.today() - timedelta(days=1)).isoformat()
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    seed_facts(
        db_path,
        "AAA.US",
        [
            make_fact(
                symbol="AAA.US",
                concept="AssetsCurrent",
                end_date=recent_date,
                value=10.0,
            ),
            make_fact(
                symbol="AAA.US",
                concept="LiabilitiesCurrent",
                end_date=recent_date,
                value=3.0,
            ),
        ],
    )
    seed_facts(
        db_path,
        "BBB.US",
        [
            make_fact(
                symbol="BBB.US",
                concept="AssetsCurrent",
                end_date=recent_date,
                value=8.0,
            ),
            make_fact(
                symbol="BBB.US",
                concept="LiabilitiesCurrent",
                end_date=recent_date,
                value=2.0,
            ),
        ],
    )
    store_market_data(db_path, "AAA.US", recent_date, market_cap=120.0, currency="USD")
    store_market_data(db_path, "BBB.US", recent_date, market_cap=90.0, currency="USD")

    patch_cli(monkeypatch, "_metric_worker_count", lambda total: 2)
    monkeypatch.setattr(MetricsRepository, "ensure_wal_mode", lambda self: "delete")
    patch_cli(monkeypatch, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)

    def fail_executor(max_workers: int) -> None:
        raise AssertionError("process executor should not be used without WAL")

    patch_cli(monkeypatch, "_create_process_pool_executor", fail_executor)

    rc = cli.cmd_compute_metrics_stage(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        exchange_codes=None,
        all_supported=False,
        metric_ids=["working_capital"],
    )

    assert rc == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert any(
        "falling back to serial metric computation" in line for line in output_lines
    )
    assert [line for line in output_lines if line.startswith("Progress:")] == [
        "Progress: [##########----------] 1/2 symbols complete (50.0%)",
        "Progress: [####################] 2/2 symbols complete (100.0%)",
    ]
    assert not any(line.startswith("[") for line in output_lines)
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_bbb is not None
    assert repo.fetch_by_id(id_aaa, "working_capital") == (7.0, recent_date)
    assert repo.fetch_by_id(id_bbb, "working_capital") == (6.0, recent_date)


def test_run_metric_computation_batches_metric_writes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "metric-stage-batched.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
            Listing(symbol="CCC.US", security_name="CCC Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )

    class DummyMetric:
        id = "dummy_metric"
        uses_market_data = False

    class InlineExecutor:
        def submit(
            self, fn: Callable[..., object], *args: object, **kwargs: object
        ) -> Future[object]:
            future: Future[object] = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            return None

    def fake_worker(
        database: str,
        listing: tuple[int, str],
        metric_ids: Sequence[str],
        suppress_metric_warnings: bool = True,
    ) -> cli._ComputedMetricsResult:
        listing_id, symbol = listing
        return cli._ComputedMetricsResult(
            symbol=symbol,
            listing_id=listing_id,
            rows=(
                (
                    listing_id,
                    "dummy_metric",
                    float(len(symbol)),
                    "2024-01-01",
                    "ratio",
                    None,
                    None,
                ),
            ),
            computed_count=1,
        )

    # The flush path now writes id-led rows via upsert_many_by_id; record those
    # flush sizes to assert the same batching behaviour.
    batch_sizes = []
    original_upsert_many_by_id = MetricsRepository.upsert_many_by_id

    def recording_upsert_many_by_id(
        self: MetricsRepository,
        rows: Iterable[IdKeyedStoredMetricRow],
        *,
        connection: sqlite3.Connection | None = None,
        commit: bool = True,
    ) -> int:
        materialized = list(rows)
        batch_sizes.append(len(materialized))
        return original_upsert_many_by_id(
            self,
            materialized,
            connection=connection,
            commit=commit,
        )

    patch_cli(monkeypatch, "REGISTRY", {DummyMetric.id: DummyMetric})
    patch_cli(monkeypatch, "_metric_worker_count", lambda total: 2)
    monkeypatch.setattr(MetricsRepository, "ensure_wal_mode", lambda self: "wal")
    patch_cli(
        monkeypatch,
        "_create_process_pool_executor",
        lambda max_workers: InlineExecutor(),
    )
    patch_cli(monkeypatch, "_compute_metrics_for_symbol_worker", fake_worker)
    patch_cli(monkeypatch, "METRICS_WRITE_BATCH_SIZE", 2)
    patch_cli(monkeypatch, "METRICS_WRITE_BATCH_INTERVAL_SECONDS", 999.0)
    monkeypatch.setattr(
        MetricsRepository, "upsert_many_by_id", recording_upsert_many_by_id
    )

    ids = SecurityRepository(db_path).resolve_ids_many(["AAA.US", "BBB.US", "CCC.US"])
    rc = cli._run_metric_computation(
        database=str(db_path),
        listings=[
            (ids["AAA.US"], "AAA.US"),
            (ids["BBB.US"], "BBB.US"),
            (ids["CCC.US"], "CCC.US"),
        ],
        metric_ids=["dummy_metric"],
        cancelled_message="\nMetric computation cancelled by user.",
    )

    assert rc == 0
    assert batch_sizes == [2, 1]


def test_flush_metric_write_batch_persists_metric_and_status(
    tmp_path: Path,
) -> None:
    """_flush_metric_write_batch writes the metric rows and status records (both
    keyed by listing_id) through the MetricsWriteSession in one committed
    transaction -- the connection + commit/rollback live inside the session."""
    db_path = tmp_path / "metric-flush.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    metrics_repo = MetricsRepository(db_path)
    status_repo = MetricComputeStatusRepository(db_path)
    metrics_repo.initialize_schema()
    status_repo.initialize_schema()
    listing_id = resolve_listing_id(db_path, "AAA.US")
    assert listing_id is not None

    writer = MetricsWriteSession(metrics_repo, status_repo)
    try:
        cli._flush_metric_write_batch(
            [(listing_id, "dummy_metric", 1.0, "2024-01-01", "other", None, None)],
            [
                cli._MetricAttemptResult(
                    symbol="AAA.US",
                    listing_id=listing_id,
                    metric_id="dummy_metric",
                    status="success",
                    attempted_at="2024-01-02T00:00:00+00:00",
                    value_as_of="2024-01-01",
                )
            ],
            writer,
        )
    finally:
        writer.close()

    # The flush committed: both id-keyed rows are readable.
    assert metrics_repo.fetch_by_id(listing_id, "dummy_metric") == (1.0, "2024-01-01")
    status_record = status_repo.fetch_by_id(listing_id, "dummy_metric")
    assert status_record is not None
    assert status_record.status == "success"


def test_run_metric_computation_parallel_profile_accumulates_worker_timings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "metric-stage-profiled-parallel.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
            Listing(symbol="CCC.US", security_name="CCC Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )

    class DummyMetric:
        id = "dummy_metric"
        uses_market_data = False

    class InlineExecutor:
        def submit(
            self, fn: Callable[..., object], *args: object, **kwargs: object
        ) -> Future[object]:
            future: Future[object] = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            return None

    def fake_profiled_worker(
        database: str,
        listings: Sequence[tuple[int, str]],
        metric_ids: Sequence[str],
        suppress_metric_warnings: bool = True,
    ) -> cli._ProfiledComputedMetricsBatchResult:
        assert suppress_metric_warnings is True
        return cli._ProfiledComputedMetricsBatchResult(
            results=tuple(
                cli._ComputedMetricsResult(
                    symbol=symbol,
                    listing_id=listing_id,
                    rows=(
                        (
                            listing_id,
                            "dummy_metric",
                            float(len(symbol)),
                            "2024-01-01",
                            "ratio",
                            None,
                            None,
                        ),
                    ),
                    computed_count=1,
                )
                for listing_id, symbol in listings
            ),
            read_seconds=0.25 * len(listings),
            compute_seconds=0.50 * len(listings),
        )

    patch_cli(monkeypatch, "REGISTRY", {DummyMetric.id: DummyMetric})
    patch_cli(monkeypatch, "_metric_worker_count", lambda total: 2)
    monkeypatch.setattr(MetricsRepository, "ensure_wal_mode", lambda self: "wal")
    patch_cli(
        monkeypatch,
        "_create_process_pool_executor",
        lambda max_workers: InlineExecutor(),
    )
    patch_cli(
        monkeypatch,
        "_compute_metrics_for_symbol_batch_worker_profiled",
        fake_profiled_worker,
    )
    patch_cli(monkeypatch, "METRICS_COMPUTE_BATCH_SIZE", 2)
    patch_cli(monkeypatch, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)

    ids = SecurityRepository(db_path).resolve_ids_many(["AAA.US", "BBB.US", "CCC.US"])
    rc = cli._run_metric_computation(
        database=str(db_path),
        listings=[
            (ids["AAA.US"], "AAA.US"),
            (ids["BBB.US"], "BBB.US"),
            (ids["CCC.US"], "CCC.US"),
        ],
        metric_ids=["dummy_metric"],
        cancelled_message="\nMetric computation cancelled by user.",
        profile=True,
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert "Profile: read=0.75s compute=1.50s" in output
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_bbb is not None
    id_ccc = resolve_listing_id(db_path, "CCC.US")
    assert id_ccc is not None
    assert repo.fetch_by_id(id_aaa, "dummy_metric") == (6.0, "2024-01-01")
    assert repo.fetch_by_id(id_bbb, "dummy_metric") == (6.0, "2024-01-01")
    assert repo.fetch_by_id(id_ccc, "dummy_metric") == (6.0, "2024-01-01")


def test_cmd_compute_metrics_stage_parallel_workers_skip_schema_init(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "metric-stage-worker-schema.db"
    recent_date = (date.today() - timedelta(days=1)).isoformat()
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    seed_facts(
        db_path,
        "AAA.US",
        [
            make_fact(
                symbol="AAA.US",
                concept="AssetsCurrent",
                end_date=recent_date,
                value=10.0,
            ),
            make_fact(
                symbol="AAA.US",
                concept="LiabilitiesCurrent",
                end_date=recent_date,
                value=3.0,
            ),
        ],
    )
    seed_facts(
        db_path,
        "BBB.US",
        [
            make_fact(
                symbol="BBB.US",
                concept="AssetsCurrent",
                end_date=recent_date,
                value=8.0,
            ),
            make_fact(
                symbol="BBB.US",
                concept="LiabilitiesCurrent",
                end_date=recent_date,
                value=2.0,
            ),
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    seed_price(db_path, "AAA.US", recent_date, 12.0, currency="USD")
    seed_price(db_path, "BBB.US", recent_date, 9.0, currency="USD")
    _seed_share_count(db_path, "AAA.US", recent_date, 10.0)
    _seed_share_count(db_path, "BBB.US", recent_date, 10.0)

    class InlineExecutor:
        def submit(
            self, fn: Callable[..., object], *args: object, **kwargs: object
        ) -> Future[object]:
            future: Future[object] = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            return None

    patch_cli(monkeypatch, "_metric_worker_count", lambda total: 2)
    patch_cli(
        monkeypatch,
        "_create_process_pool_executor",
        lambda max_workers: InlineExecutor(),
    )
    patch_cli(
        monkeypatch, "_initialize_metric_read_schema", lambda *args, **kwargs: None
    )

    def locked_initialize_schema(self: SQLiteStore) -> None:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(
        FinancialFactsRepository,
        "initialize_schema",
        locked_initialize_schema,
    )
    monkeypatch.setattr(
        MarketDataRepository,
        "initialize_schema",
        locked_initialize_schema,
    )

    ids = SecurityRepository(db_path).resolve_ids_many(["AAA.US", "BBB.US"])
    rc = cli._run_metric_computation(
        database=str(db_path),
        listings=[(ids["AAA.US"], "AAA.US"), (ids["BBB.US"], "BBB.US")],
        metric_ids=["working_capital", "market_cap"],
        cancelled_message="\nMetric computation cancelled by user.",
    )

    assert rc == 0
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_bbb is not None
    assert repo.fetch_by_id(id_aaa, "working_capital") == (7.0, recent_date)
    assert repo.fetch_by_id(id_bbb, "working_capital") == (6.0, recent_date)
    assert repo.fetch_by_id(id_aaa, "market_cap") == (120.0, recent_date)
    assert repo.fetch_by_id(id_bbb, "market_cap") == (90.0, recent_date)


def test_cmd_compute_metrics_stage_process_pool_smoke(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "metric-stage-process.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )

    patch_cli(monkeypatch, "_metric_worker_count", lambda total: 2)
    patch_cli(
        monkeypatch,
        "_create_process_pool_executor",
        _spawn_process_pool_executor,
    )
    store_market_data(
        db_path,
        "AAA.US",
        "2024-01-01",
        price=10.0,
        market_cap=120.0,
        currency="USD",
    )
    store_market_data(
        db_path,
        "BBB.US",
        "2024-01-01",
        price=9.0,
        market_cap=90.0,
        currency="USD",
    )

    rc = cli.cmd_compute_metrics_stage(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=["market_cap"],
    )

    assert rc == 0
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    id_bbb = resolve_listing_id(db_path, "BBB.US")
    assert id_bbb is not None
    assert repo.fetch_by_id(id_aaa, "market_cap") == (120.0, "2024-01-01")
    assert repo.fetch_by_id(id_bbb, "market_cap") == (90.0, "2024-01-01")


def test_cmd_clear_fundamentals_raw(tmp_path: Path) -> None:
    db_path = tmp_path / "clearfunds.db"
    _seed_listing(db_path, "AAA.US", currency="USD", provider="SEC")
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    seed_raw_fundamentals(db_path, "SEC", "AAA.US", {"facts": {}})
    seed_normalization_success(db_path, "AAA.US", provider="SEC")

    with repo._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM fundamentals_raw").fetchone()[0] == 1
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM fundamentals_normalization_state"
            ).fetchone()[0]
            == 1
        )

    rc = cli.cmd_clear_fundamentals_raw(str(db_path))
    assert rc == 0

    with repo._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM fundamentals_raw").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM fundamentals_normalization_state"
            ).fetchone()[0]
            == 0
        )


def test_cmd_clear_financial_facts_clears_normalization_state(tmp_path: Path) -> None:
    db_path = tmp_path / "clearfacts.db"
    _seed_listing(db_path, "AAA.US", currency="USD", provider="SEC")
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    seed_facts(
        db_path,
        "AAA.US",
        [
            make_fact(
                symbol="AAA.US",
                concept="Assets",
                end_date="2024-12-31",
                value=10.0,
            )
        ],
    )
    refresh_state_repo = FinancialFactsRefreshStateRepository(db_path)
    seed_raw_fundamentals(db_path, "SEC", "AAA.US", {"facts": {}})
    seed_normalization_success(db_path, "AAA.US", provider="SEC")
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    assert refresh_state_repo.fetch_by_id(id_aaa) is not None
    seed_metric_status(
        db_path,
        MetricComputeStatusRecord(
            symbol="AAA.US",
            metric_id="working_capital",
            status="failure",
            attempted_at="2024-01-02T00:00:00+00:00",
            reason_code="missing_data",
        ),
    )

    rc = cli.cmd_clear_financial_facts(str(db_path))
    assert rc == 0

    with fact_repo._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM financial_facts").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM financial_facts_refresh_state"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM metric_compute_status").fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM fundamentals_normalization_state"
            ).fetchone()[0]
            == 0
        )


def test_cmd_clear_metrics_clears_metric_compute_status(tmp_path: Path) -> None:
    db_path = tmp_path / "clearmetrics.db"
    _seed_listing(db_path, "AAA.US", currency="USD")
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 10.0, "2024-12-31")
    status_repo = MetricComputeStatusRepository(db_path)
    status_repo.initialize_schema()
    seed_metric_status(
        db_path,
        MetricComputeStatusRecord(
            symbol="AAA.US",
            metric_id="working_capital",
            status="success",
            attempted_at="2025-01-01T00:00:00+00:00",
            value_as_of="2024-12-31",
        ),
    )

    rc = cli.cmd_clear_metrics(str(db_path))

    assert rc == 0
    with metrics_repo._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0] == 0
        assert (
            conn.execute("SELECT COUNT(*) FROM metric_compute_status").fetchone()[0]
            == 0
        )


def test_cmd_normalize_fundamentals_stage_all_supported_normalizes_primary_with_raw(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "normalize-stage-all.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
        provider="EODHD",
    )
    store_supported_tickers(
        db_path,
        "LSE",
        rows=[
            {"Code": "CCC", "Exchange": "LSE", "Type": "Common Stock"},
            {"Code": "DDD", "Exchange": "LSE", "Type": "Common Stock"},
        ],
        provider="EODHD",
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {
            "General": {"Name": "AAA", "PrimaryTicker": "AAA.US"},
            "Financials": {},
        },
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "CCC.LSE",
        {
            "General": {"Name": "CCC", "PrimaryTicker": "AAA.US"},
            "Financials": {},
        },
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "DDD.LSE",
        {"General": {"Name": "DDD"}, "Financials": {}},
        exchange="LSE",
    )

    # The has-raw + primary filtering now lives in normalization_units (reached via
    # the bulk path), so this exercises the real stage->bulk->units pipeline rather
    # than a mocked handoff. A trivial normalizer keeps the run inline and FX-free.
    class FakeNormalizer:
        def normalize(
            self,
            payload: dict[str, object],
            symbol: str,
            accounting_standard: str | None = None,
            **kwargs: object,
        ) -> list[FactRecord]:
            return [
                make_fact(
                    symbol=symbol, concept="Dummy", end_date="2023-12-31", value=1.0
                )
            ]

    patch_cli(
        monkeypatch, "EODHDFactsNormalizer", lambda fx_service=None: FakeNormalizer()
    )
    patch_cli(monkeypatch, "_normalization_worker_count", lambda total: 1)

    rc = cli.cmd_normalize_fundamentals_stage(
        provider="EODHD",
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
    )

    assert rc == 0
    # AAA.US + DDD.LSE are primary and have raw; BBB.US has no raw payload and
    # CCC.LSE is a secondary listing -- both are excluded, so only the first two
    # carry a normalization watermark.
    units = FundamentalsRepository(db_path).normalization_units(
        "EODHD", primary_only=True
    )
    normalized = {
        unit.provider_symbol
        for unit in units.values()
        if unit.normalized_payload_hash is not None
    }
    assert normalized == {"AAA.US", "DDD.LSE"}


def test_ingest_run_reports_secondary_reclassification_and_retains_data(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "ingest-secondary.db"
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    seed_exchange(db_path, "US", "LSE")
    ticker_repo.replace_for_exchange(
        "EODHD",
        "US",
        [{"Code": "AAA", "Name": "AAA Inc", "Type": "Common Stock", "Currency": "USD"}],
    )
    ticker_repo.replace_for_exchange(
        "EODHD",
        "LSE",
        [{"Code": "AAA", "Name": "AAA plc", "Type": "Common Stock", "Currency": "GBX"}],
    )
    by_symbol = {row.symbol: row for row in ticker_repo.list_for_provider("EODHD")}
    aaa_lse_id = by_symbol["AAA.LSE"].security_id

    # Seed downstream facts on the listing ingest will reclassify secondary.
    seed_facts(
        db_path,
        "AAA.LSE",
        [
            FactRecord(
                symbol="AAA.LSE",
                concept="Assets",
                fiscal_period="FY",
                end_date="2024-12-31",
                unit_kind="monetary",
                value=100.0,
                currency="GBP",
            )
        ],
    )

    eligible = tuple(ticker_repo.list_for_provider("EODHD"))
    prepared = _PreparedFundamentalsRun(
        rate_value=100000.0,
        daily_limit=1000,
        used_calls=0,
        buffer_calls=0,
        request_budget=len(eligible),
        eligible=eligible,
    )

    def fake_fetch(api_key: str, limiter: object, symbol: str) -> dict[str, object]:
        # PrimaryTicker points at AAA.US, so AAA.US is primary and AAA.LSE
        # secondary -- the reclassification is reported but AAA.LSE's seeded
        # facts stay put (retention policy: exclusion is scope-side only).
        return {"General": {"Name": symbol, "PrimaryTicker": "AAA.US"}}

    patch_cli(monkeypatch, "_fetch_symbol_fundamentals", fake_fetch)

    rc = _run_eodhd_fundamentals_ingestion(
        database=db_path,
        api_key="key",
        scope_label="test scope",
        prepared=prepared,
    )

    assert rc == 0
    out = capsys.readouterr().out
    assert "Reclassified 1 listing(s) to secondary" in out
    assert "retained" in out
    with sqlite3.connect(db_path) as conn:
        remaining_facts = conn.execute(
            "SELECT COUNT(*) FROM financial_facts WHERE listing_id = ?",
            (aaa_lse_id,),
        ).fetchone()[0]
        status = conn.execute(
            "SELECT primary_listing_status FROM listing WHERE listing_id = ?",
            (aaa_lse_id,),
        ).fetchone()[0]
    assert remaining_facts == 1
    assert status == "secondary"


def test_cmd_normalize_eodhd_fundamentals_bulk_reports_freshness_scan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "normalize-eodhd-status.db"
    # Seed both US (USD) listings up front so the payload upserts can resolve
    # them (listing.currency is NOT NULL, no fallback).
    _seed_listing(db_path, ("AAA.US", "BBB.US"), currency="USD", provider="EODHD")
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US"):
        seed_raw_fundamentals(
            db_path,
            "EODHD",
            symbol,
            {"General": {"Name": symbol}, "Financials": {}},
            exchange="US",
        )
        store_market_data(db_path, symbol, "2024-12-31", currency="USD")
        store_market_data(db_path, symbol, "2024-12-31", currency="USD")

    patch_cli(
        monkeypatch,
        "_plan_normalization_selection",
        lambda units, force=False: ([], 0),
    )

    rc = cli.cmd_normalize_eodhd_fundamentals_bulk(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
    )

    assert rc == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert output_lines[0] == "Checking EODHD normalization freshness for 2 symbols"
    assert "already up to date" in output_lines[-1]


def test_cmd_normalize_eodhd_fundamentals_bulk_force_skips_freshness_scan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "normalize-eodhd-force.db"
    _seed_listing(db_path, ("AAA.US", "BBB.US"), currency="USD", provider="EODHD")
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US"):
        seed_raw_fundamentals(
            db_path,
            "EODHD",
            symbol,
            {"General": {"Name": symbol}, "Financials": {}},
            exchange="US",
        )
        store_market_data(db_path, symbol, "2024-12-31", currency="USD")

    def fail_plan(**kwargs: object) -> None:
        raise AssertionError("freshness planning should be skipped for --force")

    class FakeNormalizer:
        def normalize(
            self,
            payload: dict[str, object],
            symbol: str,
            accounting_standard: str | None = None,
            **kwargs: object,
        ) -> list[FactRecord]:
            return [
                make_fact(
                    symbol=symbol,
                    concept="Dummy",
                    end_date="2023-12-31",
                    value=1.0,
                )
            ]

    patch_cli(monkeypatch, "_plan_normalization_selection", fail_plan)
    patch_cli(
        monkeypatch, "EODHDFactsNormalizer", lambda fx_service=None: FakeNormalizer()
    )
    patch_cli(monkeypatch, "_normalization_worker_count", lambda total: 1)

    rc = cli.cmd_normalize_eodhd_fundamentals_bulk(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        force=True,
    )

    assert rc == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert (
        output_lines[0]
        == "Force re-normalization requested for 2 EODHD symbols; skipping freshness scan"
    )
    assert normalization_state_exists(db_path, "EODHD", "AAA.US")
    assert normalization_state_exists(db_path, "EODHD", "BBB.US")


def test_cmd_normalize_eodhd_fundamentals_bulk_suppresses_missing_fx_warnings_on_console(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "normalize-eodhd-missing-fx.db"
    log_dir = tmp_path / "logs"
    # AALB.AS (Amsterdam) quotes in EUR; seed the listing so the payload upsert
    # can resolve it (listing.currency is NOT NULL, no fallback).
    _seed_listing(db_path, "AALB.AS", currency="EUR", provider="EODHD")
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AALB.AS",
        {
            "General": {
                "Name": "Aalberts",
                "CurrencyCode": "EUR",
            },
            "Financials": {
                "Balance_Sheet": {
                    "yearly": [
                        {
                            "date": "2000-06-30",
                            "totalAssets": 1000.0,
                            "currency_symbol": "NLG",
                        },
                        {
                            "date": "2001-12-31",
                            "totalAssets": 1200.0,
                            "currency_symbol": "EUR",
                        },
                    ]
                }
            },
        },
        exchange="AS",
    )
    store_market_data(db_path, "AALB.AS", "2024-12-31", currency="EUR")

    patch_cli(monkeypatch, "_normalization_worker_count", lambda total: 2)
    patch_cli(monkeypatch, "_process_local_fx_service", None)
    patch_cli(monkeypatch, "_process_local_fx_service_db", None)
    patch_cli(
        monkeypatch,
        "_create_process_pool_executor",
        _spawn_process_pool_executor,
    )
    clear_root_logging_handlers()
    cli.setup_logging(log_dir=log_dir)
    try:
        rc = cli.cmd_normalize_eodhd_fundamentals_bulk(
            database=str(db_path),
            symbols=["AALB.AS"],
            force=True,
        )
        captured = capsys.readouterr()
    finally:
        clear_root_logging_handlers()

    assert rc == 0
    assert "Missing FX rate" not in captured.err
    assert "Missing FX rate for monetary conversion" not in captured.err
    log_text = (log_dir / "pyvalue.log").read_text(encoding="utf-8")
    assert "Missing FX rate | provider=EODHD base=NLG quote=EUR" in log_text
    assert "Missing FX rate for monetary conversion" in log_text


def test_cmd_normalize_eodhd_fundamentals_bulk_continues_after_symbol_failure_with_inline_executor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "normalize-eodhd-failure.db"
    _seed_listing(
        db_path, ("AAA.US", "BBB.US", "CCC.US"), currency="USD", provider="EODHD"
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US", "CCC.US"):
        seed_raw_fundamentals(
            db_path,
            "EODHD",
            symbol,
            {"General": {"Name": symbol}, "Financials": {}},
            exchange="US",
        )
        store_market_data(db_path, symbol, "2024-12-31", currency="USD")

    class FakeNormalizer:
        def __init__(self, **kwargs: object) -> None:
            pass

        def normalize(
            self,
            payload: dict[str, object],
            symbol: str,
            accounting_standard: str | None = None,
            **kwargs: object,
        ) -> list[FactRecord]:
            if symbol == "BBB.US":
                raise ValueError("boom")
            return [
                make_fact(
                    symbol=symbol,
                    concept="Dummy",
                    end_date="2023-12-31",
                    value=1.0,
                )
            ]

    patch_cli(monkeypatch, "EODHDFactsNormalizer", FakeNormalizer)
    patch_cli(monkeypatch, "_normalization_worker_count", lambda total: 3)

    class InlineExecutor:
        def submit(
            self, fn: Callable[..., object], *args: object, **kwargs: object
        ) -> Future[object]:
            future: Future[object] = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            return None

    patch_cli(
        monkeypatch,
        "_create_process_pool_executor",
        lambda max_workers: InlineExecutor(),
    )

    rc = cli.cmd_normalize_eodhd_fundamentals_bulk(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US", "CCC.US"],
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert "failed=1" in output
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    rows = (
        fact_repo._connect()
        .execute(
            """
            SELECT s.canonical_symbol
            FROM financial_facts ff
            JOIN securities s ON s.security_id = ff.listing_id
            ORDER BY s.canonical_symbol
            """
        )
        .fetchall()
    )
    assert [row[0] for row in rows] == ["AAA.US", "CCC.US"]


def test_cmd_normalize_eodhd_fundamentals_bulk_interrupts_cleanly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "normalize-eodhd-interrupt.db"
    _seed_listing(db_path, ("AAA.US", "BBB.US"), currency="USD", provider="EODHD")
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US"):
        seed_raw_fundamentals(
            db_path,
            "EODHD",
            symbol,
            {"General": {"Name": symbol}, "Financials": {}},
            exchange="US",
        )
        store_market_data(db_path, symbol, "2024-12-31", currency="USD")

    class FakeNormalizer:
        def __init__(self, **kwargs: object) -> None:
            pass

        def normalize(
            self,
            payload: dict[str, object],
            symbol: str,
            accounting_standard: str | None = None,
            **kwargs: object,
        ) -> list[FactRecord]:
            return [
                make_fact(
                    symbol=symbol,
                    concept="Dummy",
                    end_date="2023-12-31",
                    value=1.0,
                )
            ]

    class InlineExecutor:
        def __init__(self) -> None:
            self.shutdown_calls: list[tuple[bool, bool]] = []

        def submit(
            self, fn: Callable[..., object], *args: object, **kwargs: object
        ) -> Future[object]:
            future: Future[object] = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            self.shutdown_calls.append((wait, cancel_futures))

    def interrupting_as_completed(
        futures: Iterable[Future[object]],
    ) -> Iterator[Future[object]]:
        yielded = False
        for future in futures:
            if not yielded:
                yielded = True
                yield future
                raise KeyboardInterrupt

    executor = InlineExecutor()
    patch_cli(monkeypatch, "EODHDFactsNormalizer", FakeNormalizer)
    patch_cli(monkeypatch, "_normalization_worker_count", lambda total: 2)
    patch_cli(
        monkeypatch,
        "_create_process_pool_executor",
        lambda max_workers: executor,
    )
    patch_cli(monkeypatch, "as_completed", interrupting_as_completed)

    rc = cli.cmd_normalize_eodhd_fundamentals_bulk(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
    )

    assert rc == 1
    output = capsys.readouterr().out
    assert "Bulk normalization cancelled by user after 1 completed symbols." in output
    assert "Normalized EODHD fundamentals for" not in output
    assert executor.shutdown_calls == [(False, True)]
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    rows = (
        fact_repo._connect()
        .execute(
            """
            SELECT s.canonical_symbol
            FROM financial_facts ff
            JOIN securities s ON s.security_id = ff.listing_id
            ORDER BY s.canonical_symbol
            """
        )
        .fetchall()
    )
    assert [row[0] for row in rows] == ["AAA.US"]


def test_cmd_normalize_eodhd_fundamentals_bulk_process_pool_smoke(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "normalize-eodhd-process.db"
    _seed_listing(db_path, ("AAA.US", "BBB.US"), currency="USD", provider="EODHD")
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US"):
        seed_raw_fundamentals(
            db_path,
            "EODHD",
            symbol,
            {
                "General": {"Name": symbol, "CurrencyCode": "USD"},
                "Financials": {
                    "Balance_Sheet": {
                        "yearly": [
                            {
                                "date": "2024-12-31",
                                "totalAssets": 100.0,
                                "currency_symbol": "USD",
                            }
                        ]
                    }
                },
            },
            exchange="US",
        )
        store_market_data(db_path, symbol, "2024-12-31", currency="USD")

    patch_cli(monkeypatch, "_normalization_worker_count", lambda total: 2)
    patch_cli(
        monkeypatch,
        "_create_process_pool_executor",
        _spawn_process_pool_executor,
    )

    rc = cli.cmd_normalize_eodhd_fundamentals_bulk(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
    )

    assert rc == 0
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    rows = (
        fact_repo._connect()
        .execute(
            """
            SELECT s.canonical_symbol, ff.concept, ff.value
            FROM financial_facts ff
            JOIN securities s ON s.security_id = ff.listing_id
            WHERE ff.concept = 'Assets'
            ORDER BY s.canonical_symbol
            """
        )
        .fetchall()
    )
    assert [(row[0], row[1], row[2]) for row in rows] == [
        ("AAA.US", "Assets", 100.0),
        ("BBB.US", "Assets", 100.0),
    ]


def test_cmd_refresh_security_metadata_backfills_eodhd_fields_and_sec_name_fallback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "refresh-security-metadata.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
            Listing(symbol="CCC.US", security_name="CCC Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    # AAA.US carries EODHD fundamentals, so it also needs an EODHD provider
    # listing whose listing has a currency (NOT NULL, no fallback) for the
    # payload to store.
    _seed_listing(db_path, "AAA.US", currency="USD", provider="EODHD")
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {
            "General": {
                "Name": "AAA Holdings",
                "Description": "AAA business",
                "Sector": "Technology",
                "Industry": "Software",
            }
        },
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path, "SEC", "BBB.US", {"entityName": "BBB SEC Name", "facts": {}}
    )

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    seed_facts(
        db_path,
        "AAA.US",
        [
            make_fact(
                symbol="AAA.US", concept="Assets", end_date="2024-12-31", value=1.0
            )
        ],
    )
    fact_count_before = (
        fact_repo._connect()
        .execute("SELECT COUNT(*) FROM financial_facts")
        .fetchone()[0]
    )

    rc = cli.cmd_refresh_security_metadata(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
    )

    assert rc == 0
    assert _security_name(db_path, "AAA.US") == "AAA Holdings"
    assert _security_description(db_path, "AAA.US") == "AAA business"
    assert _security_sector(db_path, "AAA.US") == "Technology"
    assert _security_industry(db_path, "AAA.US") == "Software"
    assert _security_name(db_path, "BBB.US") == "BBB SEC Name"
    assert _security_sector(db_path, "BBB.US") is None
    fact_count_after = (
        fact_repo._connect()
        .execute("SELECT COUNT(*) FROM financial_facts")
        .fetchone()[0]
    )
    assert fact_count_after == fact_count_before

    output = capsys.readouterr().out.splitlines()
    assert output == [
        "Progress: 3/3 symbols complete (100.0%)",
        "Scanned 3 symbols.",
        "Updated metadata for 2 symbols.",
        "Skipped with no raw payload: 1",
        "Skipped with no extractable metadata: 0",
        "No metadata changes needed: 0",
    ]

    rc = cli.cmd_refresh_security_metadata(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
    )

    assert rc == 0
    output = capsys.readouterr().out.splitlines()
    assert output == [
        "Progress: 3/3 symbols complete (100.0%)",
        "Scanned 3 symbols.",
        "Updated metadata for 0 symbols.",
        "Skipped with no raw payload: 1",
        "Skipped with no extractable metadata: 0",
        "No metadata changes needed: 2",
    ]


def test_cmd_refresh_security_metadata_respects_symbol_scope(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "refresh-security-metadata-scope.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    # Both symbols carry EODHD fundamentals, so seed their EODHD provider
    # listings (USD) so the payloads can store.
    _seed_listing(db_path, ("AAA.US", "BBB.US"), currency="USD", provider="EODHD")
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"Sector": "Technology", "Industry": "Software"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.US",
        {"General": {"Sector": "Industrials", "Industry": "Machinery"}},
        exchange="US",
    )

    rc = cli.cmd_refresh_security_metadata(
        database=str(db_path),
        symbols=["AAA.US"],
        exchange_codes=None,
        all_supported=False,
    )

    assert rc == 0
    assert _security_sector(db_path, "AAA.US") == "Technology"
    assert _security_sector(db_path, "BBB.US") is None
    assert capsys.readouterr().out.splitlines() == [
        "Progress: 1/1 symbols complete (100.0%)",
        "Scanned 1 symbols.",
        "Updated metadata for 1 symbols.",
        "Skipped with no raw payload: 0",
        "Skipped with no extractable metadata: 0",
        "No metadata changes needed: 0",
    ]


def test_cmd_refresh_security_metadata_carries_scope_listing_ids(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The command carries scope listing ids instead of re-resolving symbols.

    refresh-security-metadata now resolves its scope to (listing_id, symbol)
    pairs via ``_resolve_canonical_scope_listings`` and threads those ids into
    the raw / metadata reads. Re-running ``resolve_ids_many`` would be a second
    pass over the listing table for ids the scope already produced, so we make
    it raise to prove the command never calls it.

    Author: Emre Tezel
    """
    db_path = tmp_path / "refresh-security-metadata-carry-ids.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    _seed_listing(db_path, ("AAA.US", "BBB.US"), currency="USD", provider="EODHD")
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"Sector": "Technology", "Industry": "Software"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.US",
        {"General": {"Sector": "Industrials", "Industry": "Machinery"}},
        exchange="US",
    )

    monkeypatch.setattr(
        SecurityRepository,
        "resolve_ids_many",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError(
                "refresh-security-metadata must carry scope listing ids, "
                "not re-resolve symbols"
            )
        ),
    )

    rc = cli.cmd_refresh_security_metadata(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
    )

    assert rc == 0
    # The no-re-resolution guard above scopes to the command run; lift it before
    # the test-side metadata reads, which legitimately resolve symbols to ids.
    monkeypatch.undo()
    assert _security_sector(db_path, "AAA.US") == "Technology"
    assert _security_sector(db_path, "BBB.US") == "Industrials"
    assert "Updated metadata for 2 symbols." in capsys.readouterr().out


def test_cmd_refresh_security_metadata_reports_progress(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "refresh-security-metadata-progress.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    _seed_listing(db_path, ("AAA.US", "BBB.US"), currency="USD", provider="EODHD")
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"Sector": "Technology", "Industry": "Software"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.US",
        {"General": {"Sector": "Industrials", "Industry": "Machinery"}},
        exchange="US",
    )
    patch_cli(monkeypatch, "SECURITY_METADATA_PROGRESS_INTERVAL_SECONDS", 0.0)

    rc = cli.cmd_refresh_security_metadata(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        exchange_codes=None,
        all_supported=False,
    )

    assert rc == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert [line for line in output_lines if line.startswith("Progress:")] == [
        "Progress: 1/2 symbols complete (50.0%)",
        "Progress: 2/2 symbols complete (100.0%)",
    ]
    assert output_lines[-5:] == [
        "Scanned 2 symbols.",
        "Updated metadata for 2 symbols.",
        "Skipped with no raw payload: 0",
        "Skipped with no extractable metadata: 0",
        "No metadata changes needed: 0",
    ]


def test_cmd_refresh_security_metadata_cancels_cleanly(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "refresh-security-metadata-cancel.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    _seed_listing(db_path, ("AAA.US", "BBB.US"), currency="USD", provider="EODHD")
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.US",
        {"General": {"Sector": "Technology", "Industry": "Software"}},
        exchange="US",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.US",
        {"General": {"Sector": "Industrials", "Industry": "Machinery"}},
        exchange="US",
    )

    call_count = 0

    real_fetch_metadata_candidates = FundamentalsRepository.fetch_metadata_candidates

    def interrupting_fetch_metadata_candidates(
        self: FundamentalsRepository, security_ids: Sequence[int], chunk_size: int = 500
    ) -> dict[int, SecurityMetadataCandidate]:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise KeyboardInterrupt
        return real_fetch_metadata_candidates(self, security_ids, chunk_size)

    monkeypatch.setattr(
        FundamentalsRepository,
        "fetch_metadata_candidates",
        interrupting_fetch_metadata_candidates,
    )
    patch_cli(monkeypatch, "SECURITY_METADATA_PROGRESS_INTERVAL_SECONDS", 0.0)
    patch_cli(monkeypatch, "SECURITY_METADATA_CHUNK_SIZE", 1)

    rc = cli.cmd_refresh_security_metadata(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        exchange_codes=None,
        all_supported=False,
    )

    assert rc == 1
    assert _security_sector(db_path, "AAA.US") == "Technology"
    assert _security_sector(db_path, "BBB.US") is None
    output_lines = capsys.readouterr().out.splitlines()
    assert (
        "Security metadata refresh cancelled by user after 1 of 2 symbols."
        in output_lines
    )
    assert [line for line in output_lines if line.startswith("Progress:")] == [
        "Progress: 1/2 symbols complete (50.0%)",
    ]
    assert "Scanned 2 symbols." not in output_lines


def test_cmd_run_screen_stage_reports_progress_for_multi_symbol_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-stage-progress.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 100.0, "2023-12-31")
    security_repo = SecurityRepository(db_path)
    security_repo.initialize_schema()
    seed_security_metadata(db_path, "AAA.US", "AAA Inc", description="AAA description")
    seed_security_metadata(db_path, "BBB.US", "BBB Inc", description="BBB description")

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
"""
    )

    patch_cli(monkeypatch, "SCREEN_PROGRESS_INTERVAL_SECONDS", 0.0)

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=None,
    )

    assert rc == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert [line for line in output_lines if line.startswith("Progress:")] == [
        "Progress: 1/2 symbols complete (50.0%)",
        "Progress: 2/2 symbols complete (100.0%)",
    ]
    assert any("AAA.US" in line for line in output_lines)


def test_cmd_run_screen_stage_creates_output_csv_parent_dirs_for_passing_results(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-stage-output-pass.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 100.0, "2023-12-31")
    seed_metric(db_path, "BBB.US", "working_capital", 50.0, "2023-12-31")
    security_repo = SecurityRepository(db_path)
    security_repo.initialize_schema()
    seed_security_metadata(db_path, "AAA.US", "AAA Inc", description="AAA description")
    seed_security_metadata(db_path, "BBB.US", "BBB Inc", description="BBB description")

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
"""
    )

    csv_path = tmp_path / "nested" / "output" / "results.csv"

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=str(csv_path),
    )

    assert rc == 0
    assert csv_path.exists()
    csv_contents = csv_path.read_text().strip().splitlines()
    assert (
        csv_contents[0]
        == "symbol,entity,description,price,price_currency,Working capital minimum"
    )
    assert csv_contents[1] == "AAA.US,AAA Inc,AAA description,N/A,N/A,100"
    output = capsys.readouterr().out
    assert "Passing symbols: 1" in output
    assert "CSV output:" in output
    assert "AAA.US" in output


def test_cmd_run_screen_stage_adds_ranked_output_rows_and_sorts_passers(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-stage-ranked.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
            Listing(symbol="CCC.US", security_name="CCC Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US", "CCC.US"):
        seed_metric(db_path, symbol, "working_capital", 100.0, "2023-12-31")
    seed_metric(db_path, "AAA.US", "primary_score", 10.0, "2023-12-31")
    seed_metric(db_path, "BBB.US", "primary_score", 10.0, "2023-12-31")
    seed_metric(db_path, "CCC.US", "primary_score", 5.0, "2023-12-31")
    seed_metric(db_path, "AAA.US", "oey_ev_norm", 0.05, "2023-12-31")
    seed_metric(db_path, "BBB.US", "oey_ev_norm", 0.07, "2023-12-31")
    seed_metric(db_path, "CCC.US", "oey_ev_norm", 0.09, "2023-12-31")
    seed_metric(db_path, "AAA.US", "net_debt_to_ebitda", 1.5, "2023-12-31")
    seed_metric(db_path, "BBB.US", "net_debt_to_ebitda", 1.5, "2023-12-31")
    seed_metric(db_path, "CCC.US", "net_debt_to_ebitda", 0.5, "2023-12-31")
    security_repo = SecurityRepository(db_path)
    security_repo.initialize_schema()
    seed_security_metadata(db_path, "AAA.US", "AAA Inc", description="AAA description")
    seed_security_metadata(db_path, "BBB.US", "BBB Inc", description="BBB description")
    seed_security_metadata(db_path, "CCC.US", "CCC Inc", description="CCC description")

    screen_path = tmp_path / "ranked-screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
ranking:
  peer_group: sector
  min_sector_peers: 10
  winsorize:
    lower_percentile: 5
    upper_percentile: 95
  metrics:
    - metric: primary_score
      weight: 1.0
      direction: higher
  tie_breakers:
    - metric: oey_ev_norm
      direction: descending
    - metric: net_debt_to_ebitda
      direction: ascending
    - metric: canonical_symbol
      direction: ascending
"""
    )
    csv_path = tmp_path / "ranked-results.csv"

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=str(csv_path),
    )

    assert rc == 0
    csv_contents = csv_path.read_text().strip().splitlines()
    assert (
        csv_contents[0]
        == "symbol,entity,description,price,price_currency,qarp_rank,qarp_score,Working capital minimum"
    )
    assert csv_contents[1].startswith(
        "BBB.US,BBB Inc,BBB description,N/A,N/A,1,66.6667,100"
    )
    assert csv_contents[2].startswith(
        "AAA.US,AAA Inc,AAA description,N/A,N/A,2,66.6667,100"
    )
    assert csv_contents[3].startswith(
        "CCC.US,CCC Inc,CCC description,N/A,N/A,3,16.6667,100"
    )
    output = capsys.readouterr().out.splitlines()
    assert "Passing symbols: 3" in output
    assert any(
        line.lstrip().startswith("Rank") and "BBB.US" not in line for line in output
    )
    assert any("BBB.US" in line for line in output)


def test_cmd_run_screen_stage_any_of_group_column_and_or_coverage(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-or-group.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    # AAA clears the group on interest coverage; BBB has no interest line at all
    # but clears it on leverage -- the OR keeps a debt-free issuer in.
    seed_metric(db_path, "AAA.US", "interest_coverage", 8.0, "2023-12-31")
    seed_metric(db_path, "BBB.US", "net_debt_to_ebitda", 1.0, "2023-12-31")
    security_repo = SecurityRepository(db_path)
    security_repo.initialize_schema()
    seed_security_metadata(db_path, "AAA.US", "AAA Inc", description="AAA description")
    seed_security_metadata(db_path, "BBB.US", "BBB Inc", description="BBB description")

    screen_path = tmp_path / "or-group-screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Debt-service capacity"
    any_of:
      - name: "Interest coverage >= 6x"
        left:
          metric: interest_coverage
        operator: ">="
        right:
          value: 6
      - name: "Net debt / EBITDA <= 2.5x"
        left:
          metric: net_debt_to_ebitda
        operator: "<="
        right:
          value: 2.5
"""
    )
    csv_path = tmp_path / "or-group-results.csv"

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=str(csv_path),
    )

    assert rc == 0
    csv_contents = csv_path.read_text().strip().splitlines()
    # The GROUP name is the trailing column -- not the member criteria names.
    assert (
        csv_contents[0]
        == "symbol,entity,description,price,price_currency,Debt-service capacity"
    )
    rows = {line.split(",", 1)[0]: line for line in csv_contents[1:]}
    assert set(rows) == {"AAA.US", "BBB.US"}
    # The reported value is the left value of the arm that carried each issuer.
    assert rows["AAA.US"].endswith(",8")  # interest coverage arm
    assert rows["BBB.US"].endswith(",1")  # leverage arm
    assert "Passing symbols: 2" in capsys.readouterr().out


def test_cmd_run_screen_stage_defers_ranking_metric_loads_until_after_filtering(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "screen-stage-ranked-loads.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
            Listing(symbol="CCC.US", security_name="CCC Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 100.0, "2023-12-31")
    seed_metric(db_path, "BBB.US", "working_capital", 100.0, "2023-12-31")
    seed_metric(db_path, "CCC.US", "working_capital", 50.0, "2023-12-31")
    seed_metric(db_path, "AAA.US", "primary_score", 10.0, "2023-12-31")
    seed_metric(db_path, "BBB.US", "primary_score", 20.0, "2023-12-31")
    seed_metric(db_path, "CCC.US", "primary_score", 30.0, "2023-12-31")
    seed_metric(db_path, "AAA.US", "oey_ev_norm", 0.05, "2023-12-31")
    seed_metric(db_path, "BBB.US", "oey_ev_norm", 0.07, "2023-12-31")
    seed_metric(db_path, "CCC.US", "oey_ev_norm", 0.09, "2023-12-31")
    seed_metric(db_path, "AAA.US", "net_debt_to_ebitda", 1.5, "2023-12-31")
    seed_metric(db_path, "BBB.US", "net_debt_to_ebitda", 0.5, "2023-12-31")
    seed_metric(db_path, "CCC.US", "net_debt_to_ebitda", 0.1, "2023-12-31")

    screen_path = tmp_path / "ranked-screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
ranking:
  peer_group: sector
  min_sector_peers: 10
  winsorize:
    lower_percentile: 5
    upper_percentile: 95
  metrics:
    - metric: primary_score
      weight: 1.0
      direction: higher
  tie_breakers:
    - metric: oey_ev_norm
      direction: descending
    - metric: net_debt_to_ebitda
      direction: ascending
    - metric: canonical_symbol
      direction: ascending
"""
    )

    aaa = resolve_listing_id(db_path, "AAA.US")
    bbb = resolve_listing_id(db_path, "BBB.US")
    ccc = resolve_listing_id(db_path, "CCC.US")
    assert aaa is not None and bbb is not None and ccc is not None

    calls = []
    original_fetch_many = cli._StatusAwareMetricsRepository.fetch_many_by_ids

    def wrapped_fetch_many(
        self: cli._StatusAwareMetricsRepository,
        listing_ids: Sequence[int],
        metric_ids: Sequence[str],
        chunk_size: int = 500,
    ) -> dict[int, dict[str, MetricRecord]]:
        calls.append((tuple(listing_ids), tuple(metric_ids)))
        return original_fetch_many(
            self,
            listing_ids,
            metric_ids,
            chunk_size=chunk_size,
        )

    monkeypatch.setattr(
        cli._StatusAwareMetricsRepository,
        "fetch_many_by_ids",
        wrapped_fetch_many,
    )

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=None,
    )

    assert rc == 0
    # Filter metrics load for the whole scope first; the ranking-extra metrics
    # load only for the two passers (AAA, BBB; CCC fails working_capital) after
    # filtering -- now keyed by listing_id.
    assert calls == [
        ((aaa, bbb, ccc), ("working_capital",)),
        ((aaa, bbb), ("primary_score", "oey_ev_norm", "net_debt_to_ebitda")),
    ]


def test_cmd_run_screen_stage_limits_console_preview_and_truncates_description(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-stage-preview.db"
    listings = [
        Listing(symbol=f"{symbol}.US", security_name=f"{symbol} Inc", exchange="NYSE")
        for symbol in ("AAA", "BBB", "CCC")
    ]
    store_catalog_listings(db_path, "US", listings, provider="SEC")

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US", "CCC.US"):
        seed_metric(db_path, symbol, "working_capital", 100.0, "2023-12-31")

    security_repo = SecurityRepository(db_path)
    security_repo.initialize_schema()
    seed_security_metadata(
        db_path,
        "AAA.US",
        "AAA Incorporated",
        description=(
            "AAA makes precision industrial components for regulated and safety-"
            "critical end markets across aerospace, energy, and medical devices."
        ),
    )
    seed_security_metadata(
        db_path, "BBB.US", "BBB Incorporated", description="BBB description"
    )
    seed_security_metadata(
        db_path, "CCC.US", "CCC Incorporated", description="CCC description"
    )

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
"""
    )

    patch_cli(monkeypatch, "SCREEN_CONSOLE_PREVIEW_MAX_ROWS", 2)
    patch_cli(monkeypatch, "SCREEN_CONSOLE_MAX_DESCRIPTION_WIDTH", 36)

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=None,
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert "Passing symbols: 3" in output
    assert "Showing top 2 of 3 passing symbols." in output
    assert "Use --output-csv to save the full result set." in output
    assert "AAA makes precision industrial..." in output
    assert "CCC.US" not in output


def test_cmd_run_screen_stage_reports_progress_when_no_symbols_pass(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-stage-no-pass.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 50.0, "2023-12-31")
    seed_metric(db_path, "BBB.US", "working_capital", 60.0, "2023-12-31")

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
"""
    )

    patch_cli(monkeypatch, "SCREEN_PROGRESS_INTERVAL_SECONDS", 0.0)

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=None,
    )

    assert rc == 1
    output_lines = capsys.readouterr().out.splitlines()
    assert [line for line in output_lines if line.startswith("Progress:")] == [
        "Progress: 1/2 symbols complete (50.0%)",
        "Progress: 2/2 symbols complete (100.0%)",
    ]
    assert output_lines[-1] == "No symbols satisfied all criteria."


def test_cmd_run_screen_stage_missing_status_falls_back_to_raw_metric_value(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "screen-stage-missing-status.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 100.0, "2024-12-31")

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
"""
    )

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=["AAA.US"],
        exchange_codes=None,
        all_supported=False,
        output_csv=None,
    )

    assert rc == 0


def test_cmd_run_screen_stage_does_not_reconcile_or_mutate_listing_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run-screen is a pure reader of ``primary_listing_status``.

    Classification is written only by ingest-fundamentals and
    reconcile-listing-status (with migration 078 as the one-time backstop), so a
    read-only screen must not write classification. To prove no write happens,
    AAA.LSE is left deliberately ``'unknown'`` -- a value a full reconcile WOULD
    flip to ``'secondary'`` because its ``PrimaryTicker`` points at AAA.US -- and
    the test asserts it stays ``'unknown'`` with its metrics intact. Regression
    for the removal of the reconcile-on-read behaviour.
    """
    db_path = tmp_path / "screen-stage-readonly-listing-status.db"
    store_catalog_listings(
        db_path,
        "LSE",
        [
            Listing(symbol="AAA.LSE", security_name="AAA PLC", exchange="LSE"),
            Listing(symbol="BBB.LSE", security_name="BBB PLC", exchange="LSE"),
        ],
        provider="EODHD",
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "AAA.LSE",
        {"General": {"Name": "AAA plc", "PrimaryTicker": "AAA.US"}},
        exchange="LSE",
    )
    seed_raw_fundamentals(
        db_path,
        "EODHD",
        "BBB.LSE",
        {"General": {"Name": "BBB plc"}},
        exchange="LSE",
    )
    # Deliberately stale cache: AAA.LSE left 'unknown' (a reconcile would flip it
    # to 'secondary'), BBB.LSE already 'primary'.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE listing SET primary_listing_status = "
            "CASE WHEN symbol = 'AAA' THEN 'unknown' ELSE 'primary' END"
        )

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.LSE", "working_capital", 100.0, "2024-12-31")
    seed_metric(db_path, "BBB.LSE", "working_capital", 100.0, "2024-12-31")

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
"""
    )

    def fail_reconcile(*args: object, **kwargs: object) -> None:
        pytest.fail("run-screen must not reconcile listing status")

    patch_cli(monkeypatch, "_reconcile_eodhd_listing_scope", fail_reconcile)

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=False,
        output_csv=None,
    )

    assert rc == 0
    with sqlite3.connect(db_path) as conn:
        statuses = conn.execute(
            """
            SELECT l.symbol || '.' || e.exchange_code, l.primary_listing_status
            FROM listing l
            JOIN "exchange" e ON e.exchange_id = l.exchange_id
            ORDER BY l.symbol || '.' || e.exchange_code
            """
        ).fetchall()
        aaa_metric_rows = conn.execute(
            """
            SELECT COUNT(*) FROM metrics
            WHERE listing_id = (
                SELECT security_id FROM provider_listing_catalog
                WHERE provider = 'EODHD' AND provider_symbol = 'AAA.LSE'
            )
            """
        ).fetchone()[0]

    # No reconcile-on-read: the deliberately-stale 'unknown' is left untouched
    # (a reconcile would have flipped it to 'secondary') and metrics are intact.
    assert statuses == [
        ("AAA.LSE", "unknown"),
        ("BBB.LSE", "primary"),
    ]
    assert aaa_metric_rows == 1


def test_cmd_run_screen_stage_failure_status_shadows_stored_metric_value(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "screen-stage-failed-status.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 100.0, "2024-12-31")
    status_repo = MetricComputeStatusRepository(db_path)
    status_repo.initialize_schema()
    seed_metric_status(
        db_path,
        MetricComputeStatusRecord(
            symbol="AAA.US",
            metric_id="working_capital",
            status="failure",
            attempted_at="2025-01-01T00:00:00+00:00",
            reason_code="missing_data",
        ),
    )

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
"""
    )

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=["AAA.US"],
        exchange_codes=None,
        all_supported=False,
        output_csv=None,
    )

    assert rc == 1


def test_cmd_run_screen_stage_stale_success_status_hides_stored_metric_value(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "screen-stage-stale-status.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    seed_facts(
        db_path,
        "AAA.US",
        [
            make_fact(concept="AssetsCurrent", end_date="2024-12-31", value=150.0),
            make_fact(
                concept="LiabilitiesCurrent",
                end_date="2024-12-31",
                value=50.0,
            ),
        ],
    )
    refresh_repo = FinancialFactsRefreshStateRepository(db_path)
    id_aaa = resolve_listing_id(db_path, "AAA.US")
    assert id_aaa is not None
    initial_refresh = refresh_repo.fetch_by_id(id_aaa)
    assert initial_refresh is not None

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 100.0, "2024-12-31")
    status_repo = MetricComputeStatusRepository(db_path)
    status_repo.initialize_schema()
    seed_metric_status(
        db_path,
        MetricComputeStatusRecord(
            symbol="AAA.US",
            metric_id="working_capital",
            status="success",
            attempted_at="2025-01-01T00:00:00+00:00",
            value_as_of="2024-12-31",
            facts_refreshed_at=initial_refresh.refreshed_at,
        ),
    )

    time.sleep(0.01)
    seed_facts(
        db_path,
        "AAA.US",
        [
            make_fact(concept="AssetsCurrent", end_date="2025-03-31", value=80.0),
            make_fact(
                concept="LiabilitiesCurrent",
                end_date="2025-03-31",
                value=70.0,
            ),
        ],
    )

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
"""
    )

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=["AAA.US"],
        exchange_codes=None,
        all_supported=False,
        output_csv=None,
    )

    assert rc == 1


def test_cmd_run_screen_stage_creates_output_csv_parent_dirs_when_no_symbols_pass(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-stage-output-empty.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 50.0, "2023-12-31")
    seed_metric(db_path, "BBB.US", "working_capital", 60.0, "2023-12-31")

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital minimum"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 75
"""
    )

    csv_path = tmp_path / "nested" / "output" / "empty-results.csv"

    rc = cli.cmd_run_screen_stage(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        output_csv=str(csv_path),
    )

    assert rc == 1
    assert csv_path.exists()
    csv_contents = csv_path.read_text().strip().splitlines()
    assert (
        csv_contents[0]
        == "symbol,entity,description,price,price_currency,Working capital minimum"
    )
    assert len(csv_contents) == 1
    assert "No symbols satisfied all criteria." in capsys.readouterr().out


def test_cmd_report_screen_failures_dedupes_metric_na_counts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen_failures.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    as_of = (date.today() - timedelta(days=5)).isoformat()
    seed_facts(
        db_path,
        "BBB.US",
        [
            make_fact(
                symbol="BBB.US",
                concept="AssetsCurrent",
                end_date=as_of,
                value=100.0,
            ),
            make_fact(
                symbol="BBB.US",
                concept="LiabilitiesCurrent",
                end_date=as_of,
                value=20.0,
            ),
        ],
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 10.0, as_of)
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    seed_price(db_path, "BBB.US", as_of, price=10.0, currency="USD")
    _seed_share_count(db_path, "BBB.US", as_of, 25.0)

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital >= 20"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 20

  - name: "Working capital >= 50"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 50
"""
    )
    csv_path = tmp_path / "reports" / "screen_failures.csv"

    rc = cli.cmd_report_screen_failures(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        exchange_codes=None,
        all_supported=False,
        output_csv=str(csv_path),
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert "Progress: [--------------------] 0/2 symbols screened (0.0%)" in output
    assert "Progress: [####################] 2/2 symbols screened (100.0%)" in output
    assert "Passed all criteria: 0/2" in output
    assert "Metric NA impact" in output
    # BBB's missing metric counts once even though it blocks both criteria.
    assert "- working_capital: missing=1 symbols, affects=2 criteria" in output
    # Root causes moved to report-metric-status --reasons; the report hints there.
    assert (
        f"hint: pyvalue report-metric-status --config {screen_path} --reasons" in output
    )
    assert "Criterion fallout" in output
    assert "Working capital >= 20: fails=2/2, na_fails=1, threshold_fails=1" in output
    assert "Working capital >= 50: fails=2/2, na_fails=1, threshold_fails=1" in output
    assert "missing_metrics: working_capital=1" in output
    csv_lines = csv_path.read_text().strip().splitlines()
    assert csv_lines[0] == (
        "metric_id,missing_symbols,affected_criteria_count,affected_criteria"
    )
    assert csv_lines[1] == (
        "working_capital,1,2,1. Working capital >= 20; 2. Working capital >= 50"
    )


def test_cmd_report_screen_failures_or_group_na_coverage(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen_failures_or.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    # AAA has leverage data and misses the bar (a real threshold fail); its missing
    # interest_coverage does NOT block the group. BBB is NA on both arms, so both
    # metrics genuinely block its group.
    seed_metric(db_path, "AAA.US", "net_debt_to_ebitda", 4.0, "2023-12-31")

    screen_path = tmp_path / "or-fallout-screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Debt-service capacity"
    any_of:
      - name: "Interest coverage >= 6x"
        left:
          metric: interest_coverage
        operator: ">="
        right:
          value: 6
      - name: "Net debt / EBITDA <= 2.5x"
        left:
          metric: net_debt_to_ebitda
        operator: "<="
        right:
          value: 2.5
"""
    )

    rc = cli.cmd_report_screen_failures(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        exchange_codes=None,
        all_supported=False,
        output_csv=None,
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert "Passed all criteria: 0/2" in output
    # AAA fails on a real threshold miss (leverage arm); BBB is NA-blocked (both
    # arms missing). One group, so both symbols land on the same fallout line.
    assert "Debt-service capacity: fails=2/2, na_fails=1, threshold_fails=1" in output
    # Coverage payoff: interest_coverage is blamed only for BBB (missing=1), NOT
    # for AAA -- even though AAA is also missing it -- because AAA's group got a
    # real answer from the leverage arm.
    assert "- interest_coverage: missing=1 symbols, affects=1 criteria" in output
    assert "- net_debt_to_ebitda: missing=1 symbols, affects=1 criteria" in output


def test_cmd_report_screen_failures_never_recomputes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "screen-failure-status.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    status_repo = MetricComputeStatusRepository(db_path)
    status_repo.initialize_schema()
    seed_metric_status(
        db_path,
        MetricComputeStatusRecord(
            symbol="AAA.US",
            metric_id="cached_metric",
            status="failure",
            attempted_at="2025-01-01T00:00:00+00:00",
            reason_code="cached_failure",
        ),
    )

    class CachedMetric:
        id = "cached_metric"
        required_concepts = ()
        uses_market_data = False
        uses_financial_facts = False

        def compute(
            self, listing_id: int, repo: RegionFactsRepository
        ) -> MetricResult | None:
            raise AssertionError("report-screen-failures must never compute metrics")

    patch_cli(monkeypatch, "REGISTRY", {CachedMetric.id: CachedMetric})

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Cached metric > 0"
    left:
      metric: cached_metric
    operator: ">"
    right:
      value: 0
"""
    )

    rc = cli.cmd_report_screen_failures(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=["AAA.US"],
        exchange_codes=None,
        all_supported=False,
        output_csv=None,
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert "- cached_metric: missing=1 symbols, affects=1 criteria" in output
    # Reason buckets no longer render here; the drill-down hint replaces them.
    assert "cached_failure" not in output
    assert (
        f"hint: pyvalue report-metric-status --config {screen_path} --reasons" in output
    )


def test_cmd_report_screen_failures_reports_progress_by_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "screen_failures_progress.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    as_of = (date.today() - timedelta(days=5)).isoformat()
    seed_facts(
        db_path,
        "BBB.US",
        [
            make_fact(
                symbol="BBB.US",
                concept="AssetsCurrent",
                end_date=as_of,
                value=100.0,
            ),
            make_fact(
                symbol="BBB.US",
                concept="LiabilitiesCurrent",
                end_date=as_of,
                value=20.0,
            ),
        ],
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 10.0, as_of)
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    seed_price(db_path, "BBB.US", as_of, price=10.0)
    _seed_share_count(db_path, "BBB.US", as_of, 25.0)

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital >= 20"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 20

  - name: "Working capital >= 50"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 50
"""
    )
    patch_cli(monkeypatch, "SCREEN_PROGRESS_INTERVAL_SECONDS", 0.0)

    rc = cli.cmd_report_screen_failures(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        exchange_codes=None,
        all_supported=False,
        output_csv=None,
    )

    assert rc == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert (
        "Progress: [--------------------] 0/2 symbols screened (0.0%)" in output_lines
    )
    assert (
        "Progress: [##########----------] 1/2 symbols screened (50.0%)" in output_lines
    )
    assert (
        "Progress: [####################] 2/2 symbols screened (100.0%)" in output_lines
    )
    # The recompute phase is gone: screening progress is the only progress bar.
    assert not any("missing symbols analyzed" in line for line in output_lines)


def test_cmd_report_screen_failures_avoids_point_metric_fetches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "screen_failures_preloaded_metrics.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    as_of = (date.today() - timedelta(days=5)).isoformat()
    seed_facts(
        db_path,
        "BBB.US",
        [
            make_fact(
                symbol="BBB.US",
                concept="AssetsCurrent",
                end_date=as_of,
                value=100.0,
            ),
            make_fact(
                symbol="BBB.US",
                concept="LiabilitiesCurrent",
                end_date=as_of,
                value=20.0,
            ),
        ],
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    seed_metric(db_path, "AAA.US", "working_capital", 10.0, as_of)
    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Working capital >= 20"
    left:
      metric: working_capital
    operator: ">="
    right:
      value: 20
"""
    )

    def fail_point_fetch(
        self: MetricsRepository, listing_id: int, metric_id: str
    ) -> None:
        raise AssertionError("point metric fetch should not be used")

    monkeypatch.setattr(MetricsRepository, "fetch_by_id", fail_point_fetch)

    rc = cli.cmd_report_screen_failures(
        config_path=str(screen_path),
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        exchange_codes=None,
        all_supported=False,
        output_csv=None,
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert "- working_capital: missing=1 symbols, affects=1 criteria" in output


def test_report_skipped_no_currency_prints_count_and_preview(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """refresh-supported-tickers surfaces skipped (no-currency) tickers on screen."""

    # Nothing is printed when no tickers were skipped.
    cli._report_skipped_no_currency("LSE", [])
    assert capsys.readouterr().out == ""

    # A small skip list is enumerated in full so the operator can chase them.
    cli._report_skipped_no_currency("LSE", ["AAA", "BBB"])
    out = capsys.readouterr().out
    assert "2 ticker(s) on LSE skipped" in out
    assert "AAA, BBB" in out
    assert "chase with the provider" in out

    # A large skip list is previewed (first 20) with the remainder summarized.
    cli._report_skipped_no_currency("US", [f"T{i}" for i in range(25)])
    out = capsys.readouterr().out
    assert "25 ticker(s) on US skipped" in out
    assert "(+5 more)" in out
