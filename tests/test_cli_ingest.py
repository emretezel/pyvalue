"""Tests for CLI ingestion and metric commands.

Author: Emre Tezel
"""

import logging
import sqlite3
import threading
import time
import concurrent.futures.thread as thread_futures
from concurrent.futures import Future
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from pyvalue import cli
from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics import REGISTRY
from pyvalue.metrics.base import MetricResult
from pyvalue.storage import (
    EntityMetadataRepository,
    FundamentalsNormalizationStateRepository,
    FundamentalsRepository,
    FundamentalsFetchStateRepository,
    FinancialFactsRepository,
    FactRecord,
    MarketDataFetchStateRepository,
    MarketDataRepository,
    MetricsRepository,
    SupportedTicker,
    SupportedExchangeRepository,
    SupportedTickerRepository,
)
from pyvalue.universe import Listing
from pyvalue.marketdata import PriceData


def make_fact(**kwargs):
    base = {
        "symbol": "AAPL.US",
        "cik": "CIK",
        "concept": "",
        "fiscal_period": "FY",
        "end_date": "",
        "unit": "USD",
        "value": 0.0,
        "accn": None,
        "filed": None,
        "frame": None,
        "start_date": None,
    }
    base.update(kwargs)
    return FactRecord(**base)


def clear_root_logging_handlers():
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()


def store_supported_exchanges(
    db_path,
    rows=None,
    provider: str = "EODHD",
):
    repo = SupportedExchangeRepository(db_path)
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


def store_supported_tickers(
    db_path,
    exchange_code: str,
    rows=None,
    provider: str = "EODHD",
):
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    repo.replace_for_exchange(
        provider,
        exchange_code,
        rows
        or [
            {
                "Code": "AAA",
                "Name": "AAA plc",
                "Exchange": exchange_code,
                "Type": "Common Stock",
                "Currency": "GBP",
            }
        ],
    )
    return repo


def store_catalog_listings(
    db_path,
    exchange_code: str,
    listings,
    provider: str = "SEC",
):
    repo = SupportedTickerRepository(db_path)
    repo.initialize_schema()
    repo.replace_from_listings(provider, exchange_code, listings)
    return repo


def store_market_data(db_path, symbol: str, as_of: str, price: float = 10.0):
    repo = MarketDataRepository(db_path)
    repo.initialize_schema()
    repo.upsert_price(symbol, as_of, price)
    return repo


def make_supported_ticker(
    symbol: str,
    exchange_code: str,
    security_id: int,
    currency: str = "USD",
):
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
    monkeypatch,
):
    calls = {}

    def fake_cmd(provider, database, exchange_codes, max_age_days, missing_only):
        calls["provider"] = provider
        calls["database"] = database
        calls["exchange_codes"] = exchange_codes
        calls["max_age_days"] = max_age_days
        calls["missing_only"] = missing_only
        return 0

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "cmd_report_fundamentals_progress", fake_cmd)

    rc = cli.main(["report-fundamentals-progress"])

    assert rc == 0
    assert calls == {
        "provider": "EODHD",
        "database": "data/pyvalue.db",
        "exchange_codes": None,
        "max_age_days": 30,
        "missing_only": False,
    }


def test_build_parser_report_ingest_progress_missing_only():
    args = cli.build_parser().parse_args(
        ["report-fundamentals-progress", "--exchange-codes", "US,LSE", "--missing-only"]
    )

    assert args.command == "report-fundamentals-progress"
    assert args.exchange_codes == ["US,LSE"]
    assert args.max_age_days == 30
    assert args.missing_only is True


def test_main_dispatches_ingest_fundamentals_with_default_provider_and_max_age_days(
    monkeypatch,
):
    calls = {}

    def fake_cmd(
        provider,
        database,
        symbols,
        exchange_codes,
        all_supported,
        rate,
        max_symbols,
        max_age_days,
        respect_backoff,
        user_agent,
        cik,
    ):
        calls["provider"] = provider
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["rate"] = rate
        calls["max_symbols"] = max_symbols
        calls["max_age_days"] = max_age_days
        calls["respect_backoff"] = respect_backoff
        calls["user_agent"] = user_agent
        calls["cik"] = cik
        return 0

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "cmd_ingest_fundamentals_stage", fake_cmd)

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
        "user_agent": None,
        "cik": None,
    }

    args = cli.build_parser().parse_args(
        ["ingest-fundamentals", "--symbols", "AAPL.US", "--retry-failed-now"]
    )
    assert args.retry_failed_now is True

    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            ["ingest-fundamentals", "--symbols", "AAPL.US", "--resume"]
        )


def test_build_parser_normalize_fundamentals_defaults_provider():
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


def test_main_dispatches_normalize_fundamentals_stage_with_force(monkeypatch):
    calls = {}

    def fake_cmd(provider, database, symbols, exchange_codes, all_supported, force):
        calls["provider"] = provider
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["force"] = force
        return 0

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "cmd_normalize_fundamentals_stage", fake_cmd)

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


def test_build_parser_compute_metrics_warning_flag_defaults_to_suppressed():
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


def test_main_dispatches_compute_metrics_stage_with_warning_flag(monkeypatch):
    calls = {}

    def fake_cmd(
        database,
        symbols,
        exchange_codes,
        all_supported,
        metric_ids,
        show_metric_warnings,
    ):
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["metric_ids"] = metric_ids
        calls["show_metric_warnings"] = show_metric_warnings
        return 0

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "cmd_compute_metrics_stage", fake_cmd)

    rc = cli.main(["compute-metrics", "--symbols", "AAPL.US", "--show-metric-warnings"])

    assert rc == 0
    assert calls == {
        "database": "data/pyvalue.db",
        "symbols": ["AAPL.US"],
        "exchange_codes": None,
        "all_supported": False,
        "metric_ids": None,
        "show_metric_warnings": True,
    }


def test_build_parser_run_screen_requires_config_and_defaults_warning_flag():
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


def test_main_dispatches_run_screen_stage_with_warning_flag(monkeypatch):
    calls = {}

    def fake_cmd(
        config_path,
        database,
        symbols,
        exchange_codes,
        all_supported,
        output_csv,
        show_metric_warnings,
    ):
        calls["config_path"] = config_path
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["output_csv"] = output_csv
        calls["show_metric_warnings"] = show_metric_warnings
        return 0

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "cmd_run_screen_stage", fake_cmd)

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


def test_build_parser_refresh_security_metadata_uses_scope_selectors():
    args = cli.build_parser().parse_args(
        ["refresh-security-metadata", "--exchange-codes", "US"]
    )

    assert args.command == "refresh-security-metadata"
    assert args.exchange_codes == ["US"]
    assert args.database == "data/pyvalue.db"


def test_main_dispatches_refresh_security_metadata(monkeypatch):
    calls = {}

    def fake_cmd(database, symbols, exchange_codes, all_supported):
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        return 0

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "cmd_refresh_security_metadata", fake_cmd)

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


def test_build_parser_report_screen_failures_requires_config():
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


def test_main_dispatches_report_screen_failures(monkeypatch):
    calls = {}

    def fake_cmd(
        config_path,
        database,
        symbols,
        exchange_codes,
        all_supported,
        output_csv,
    ):
        calls["config_path"] = config_path
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["output_csv"] = output_csv
        return 0

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "cmd_report_screen_failures", fake_cmd)

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
    monkeypatch,
):
    calls = {}

    def fake_cmd(
        provider,
        database,
        symbols,
        exchange_codes,
        all_supported,
        rate,
        max_symbols,
        max_age_days,
        respect_backoff,
    ):
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

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "cmd_update_market_data_stage", fake_cmd)

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
    monkeypatch,
):
    calls = {}

    def fake_cmd(
        provider,
        database,
        symbols,
        exchange_codes,
        all_supported,
        rate,
        max_symbols,
        max_age_days,
        respect_backoff,
    ):
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

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "cmd_update_market_data_stage", fake_cmd)

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


def test_main_returns_cleanly_on_uncaught_keyboard_interrupt(monkeypatch, capsys):
    monkeypatch.setattr(cli, "setup_logging", lambda: None)

    def raising_cmd(provider, database):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "cmd_refresh_supported_exchanges", raising_cmd)

    rc = cli.main(["refresh-supported-exchanges"])

    assert rc == 1
    assert capsys.readouterr().out.splitlines() == ["Cancelled by user."]


def test_main_dispatches_report_market_data_progress_with_default_max_age_days(
    monkeypatch,
):
    calls = {}

    def fake_cmd(provider, database, exchange_codes, max_age_days):
        calls["provider"] = provider
        calls["database"] = database
        calls["exchange_codes"] = exchange_codes
        calls["max_age_days"] = max_age_days
        return 0

    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(cli, "cmd_report_market_data_progress", fake_cmd)

    rc = cli.main(["report-market-data-progress"])

    assert rc == 0
    assert calls == {
        "provider": "EODHD",
        "database": "data/pyvalue.db",
        "exchange_codes": None,
        "max_age_days": 30,
    }


def test_build_parser_report_fact_freshness_defaults_max_age_days():
    args = cli.build_parser().parse_args(
        ["report-fact-freshness", "--symbols", "AAPL.US"]
    )

    assert args.command == "report-fact-freshness"
    assert args.max_age_days == 30


def test_cmd_ingest_fundamentals_sec(monkeypatch, tmp_path):
    calls = {}

    class FakeClient:
        def __init__(self, user_agent=None):
            calls["ua"] = user_agent

        def resolve_company(self, symbol):
            return SimpleNamespace(
                symbol=symbol.upper(), cik="CIK0000320193", name="Apple"
            )

        def fetch_company_facts(self, cik):
            calls["cik"] = cik
            return {"cik": cik, "data": []}

    monkeypatch.setattr(cli, "SECCompanyFactsClient", FakeClient)

    db_path = tmp_path / "facts.db"
    rc = cli.cmd_ingest_fundamentals(
        provider="SEC",
        symbol="AAPL",
        database=str(db_path),
        exchange_code="US",
        user_agent="UA",
        cik=None,
    )
    assert rc == 0
    assert calls["ua"] == "UA"
    assert calls["cik"] == "CIK0000320193"

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    stored = repo.fetch("SEC", "AAPL.US")
    assert stored["cik"] == "CIK0000320193"


def test_cmd_ingest_fundamentals_eodhd(monkeypatch, tmp_path):
    db_path = tmp_path / "funds.db"
    calls = {}

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["symbol"] = symbol
            calls["exchange_code"] = exchange_code
            return {"General": {"CurrencyCode": "USD", "Name": "Shell PLC"}}

        def list_symbols(self, exchange_code):
            raise AssertionError("Should not list symbols in single ingest")

        def exchange_metadata(self, exchange_code):
            return {"Name": "London", "Country": "UK", "Currency": "GBP"}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_ingest_fundamentals(
        provider="EODHD",
        symbol="SHEL.LSE",
        database=str(db_path),
        exchange_code="LSE",
        user_agent=None,
        cik=None,
    )
    assert rc == 0
    assert calls == {"api_key": "TOKEN", "symbol": "SHEL.LSE", "exchange_code": None}

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    payload = repo.fetch("EODHD", "SHEL.LSE")
    assert payload["General"]["CurrencyCode"] == "USD"
    with repo._connect() as conn:
        row = conn.execute(
            """
            SELECT currency, provider_exchange_code
            FROM fundamentals_raw
            WHERE provider='EODHD' AND provider_symbol='SHEL.LSE'
            """
        ).fetchone()
    assert row[0] == "USD"
    assert row[1] == "LSE"


def test_cmd_ingest_fundamentals_bulk_eodhd_with_exchange(monkeypatch, tmp_path):
    db_path = tmp_path / "bulkfunds.db"
    calls = {"fetched": []}
    store_supported_tickers(
        db_path,
        "LSE",
        rows=[
            {"Code": "AAA", "Exchange": "LSE", "Type": "Common Stock"},
            {"Code": "CCC", "Exchange": "LSE", "Type": "Preferred Stock"},
        ],
    )

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def list_symbols(self, exchange_code):
            raise AssertionError(
                "Bulk EODHD fundamentals should use stored supported_tickers."
            )

        def user_metadata(self):
            return {
                "dailyRateLimit": "100000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append((symbol, exchange_code))
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_ingest_fundamentals_bulk(
        provider="EODHD",
        database=str(db_path),
        rate=None,
        exchange_code="LSE",
        user_agent=None,
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )
    assert rc == 0
    assert set(calls["fetched"]) == {("AAA.LSE", None), ("CCC.LSE", None)}

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    assert repo.fetch("EODHD", "AAA.LSE")["General"]["Name"] == "AAA.LSE"
    assert repo.fetch("EODHD", "CCC.LSE")["General"]["Name"] == "CCC.LSE"


def test_cmd_ingest_fundamentals_bulk_eodhd_with_exchange_symbols(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "bulkfunds_region.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Stock"},
        ],
    )

    calls = {"fetched": []}

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def list_symbols(self, exchange_code):
            raise AssertionError(
                "Bulk EODHD fundamentals should use stored supported_tickers."
            )

        def user_metadata(self):
            return {
                "dailyRateLimit": "100000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append((symbol, exchange_code))
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_ingest_fundamentals_bulk(
        provider="EODHD",
        database=str(db_path),
        rate=None,
        exchange_code="US",
        user_agent=None,
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )
    assert rc == 0
    assert set(calls["fetched"]) == {("AAA.US", None), ("BBB.US", None)}

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    assert repo.fetch("EODHD", "AAA.US")["General"]["Name"] == "AAA.US"
    assert repo.fetch("EODHD", "BBB.US")["General"]["Name"] == "BBB.US"


def test_cmd_ingest_fundamentals_bulk_sec(monkeypatch, tmp_path):
    db_path = tmp_path / "bulk.db"
    listings = [
        Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
        Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
    ]
    store_catalog_listings(db_path, "US", listings, provider="SEC")

    class FakeClient:
        def __init__(self, user_agent=None):
            self.user_agent = user_agent
            self.calls = []

        def resolve_company(self, symbol):
            return SimpleNamespace(symbol=symbol, cik=f"CIK{symbol}", name=symbol)

        def fetch_company_facts(self, cik):
            self.calls.append(cik)
            return {"cik": cik}

    fake_client = FakeClient()
    monkeypatch.setattr(
        cli, "SECCompanyFactsClient", lambda user_agent=None: fake_client
    )

    rc = cli.cmd_ingest_fundamentals_bulk(
        provider="SEC",
        database=str(db_path),
        rate=0,
        exchange_code="US",
        user_agent="UA",
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )
    assert rc == 0
    assert fake_client.calls == ["CIKAAA", "CIKBBB"]
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    assert fund_repo.fetch("SEC", "AAA.US") == {"cik": "CIKAAA"}


def test_cmd_load_universe_eodhd(monkeypatch, tmp_path):
    db_path = tmp_path / "uk.db"
    store_supported_exchanges(
        db_path,
        rows=[
            {
                "Code": "LSE",
                "Name": "London",
                "Country": "UK",
                "Currency": "GBP",
                "OperatingMIC": "XLON",
                "CountryISO2": "GB",
                "CountryISO3": "GBR",
            }
        ],
    )

    with pytest.raises(SystemExit) as exc:
        cli.cmd_load_universe(
            provider="EODHD",
            database=str(db_path),
            include_etfs=False,
            exchange_code="LSE",
            currencies=["GBP"],
            include_exchanges=["LSE"],
        )

    assert "load-universe --provider EODHD is deprecated" in str(exc.value)


def test_cmd_load_universe_sec_stores_supported_tickers(monkeypatch, tmp_path):
    class FakeLoader:
        def load(self):
            return [
                Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NASDAQ"),
                Listing(
                    symbol="ETF1.US",
                    security_name="ETF One",
                    exchange="NYSE Arca",
                    is_etf=True,
                ),
            ]

    monkeypatch.setattr(cli, "USUniverseLoader", lambda: FakeLoader())

    db_path = tmp_path / "sec-universe.db"
    rc = cli.cmd_load_universe(
        provider="SEC",
        database=str(db_path),
        include_etfs=False,
        exchange_code=None,
        currencies=None,
        include_exchanges=None,
    )

    assert rc == 0
    repo = SupportedTickerRepository(db_path)
    rows = repo.list_for_exchange("SEC", "US")
    assert [(row.symbol, row.listing_exchange, row.security_type) for row in rows] == [
        ("AAA.US", "NASDAQ", "Common Stock")
    ]


def test_cmd_refresh_supported_exchanges(monkeypatch, tmp_path):
    calls = {}

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def list_exchanges(self):
            calls["list_exchanges"] = calls.get("list_exchanges", 0) + 1
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

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")

    db_path = tmp_path / "supported-exchanges.db"
    rc = cli.cmd_refresh_supported_exchanges(
        provider="EODHD",
        database=str(db_path),
    )

    assert rc == 0
    assert calls == {"api_key": "TOKEN", "list_exchanges": 1}

    repo = SupportedExchangeRepository(db_path)
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


def test_cmd_refresh_supported_tickers_filters_types_and_cleans_catalog(
    monkeypatch, tmp_path
):
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

    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD",
        "OLD.LSE",
        {"General": {"CurrencyCode": "GBP", "Name": "Old plc"}},
        exchange="LSE",
    )

    calls = {"listed": []}

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def list_symbols(self, exchange_code):
            calls["listed"].append(exchange_code)
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

        def list_exchanges(self):
            raise AssertionError("Should not refresh supported exchanges on cache hit")

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_refresh_supported_tickers(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=["LSE"],
        all_supported=False,
        include_etfs=False,
    )

    assert rc == 0
    assert calls == {"api_key": "TOKEN", "listed": ["LSE"]}

    ticker_repo = SupportedTickerRepository(db_path)
    rows = ticker_repo.list_for_exchange("EODHD", "LSE")
    assert [row.symbol for row in rows] == ["KEEP.LSE", "PREF.LSE"]
    assert [row.security_type for row in rows] == ["Common Stock", "Preferred Stock"]
    with ticker_repo._connect() as conn:
        listings_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='listings'"
        ).fetchone()

    assert state_repo.fetch("EODHD", "OLD.LSE") is None
    assert state_repo.fetch("EODHD", "KEEP.LSE") is not None
    assert fund_repo.fetch("EODHD", "OLD.LSE") is not None
    assert listings_table is None


def test_cmd_refresh_supported_tickers_all_exchanges_in_code_order(
    monkeypatch, tmp_path
):
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
    calls = {"listed": []}

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def list_symbols(self, exchange_code):
            calls["listed"].append(exchange_code)
            return [
                {
                    "Code": f"{exchange_code}1",
                    "Exchange": exchange_code,
                    "Name": f"{exchange_code} Company",
                    "Type": "Common Stock",
                    "Currency": "USD",
                }
            ]

        def list_exchanges(self):
            raise AssertionError("Should use cached supported exchanges")

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_refresh_supported_tickers(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        all_supported=True,
        include_etfs=False,
    )

    assert rc == 0
    assert calls["api_key"] == "TOKEN"
    assert calls["listed"] == ["LSE", "TSX", "US"]

    repo = SupportedTickerRepository(db_path)
    assert repo.list_all_exchanges("EODHD") == ["LSE", "TSX", "US"]


def test_cmd_refresh_supported_tickers_defaults_to_all_supported(monkeypatch, tmp_path):
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
    calls = {"listed": []}

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def list_symbols(self, exchange_code):
            calls["listed"].append(exchange_code)
            return [
                {
                    "Code": f"{exchange_code}1",
                    "Exchange": exchange_code,
                    "Name": f"{exchange_code} Company",
                    "Type": "Common Stock",
                    "Currency": "USD",
                }
            ]

        def list_exchanges(self):
            raise AssertionError("Should use cached supported exchanges")

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_refresh_supported_tickers(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        all_supported=False,
        include_etfs=False,
    )

    assert rc == 0
    assert calls["api_key"] == "TOKEN"
    assert calls["listed"] == ["LSE", "US"]


def test_cmd_load_universe_eodhd_bootstraps_supported_exchanges(monkeypatch, tmp_path):
    db_path = tmp_path / "bootstrap-universe.db"
    with pytest.raises(SystemExit) as exc:
        cli.cmd_load_universe(
            provider="EODHD",
            database=str(db_path),
            include_etfs=False,
            exchange_code="LSE",
            currencies=None,
            include_exchanges=None,
        )

    assert "deprecated" in str(exc.value)


def test_cmd_ingest_fundamentals_global_respects_budget_from_user_metadata(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "global-budget.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Stock"},
        ],
    )
    calls = {"fetched": []}

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "25",
                "apiRequests": "10",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append(symbol)
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_fundamentals_daily_buffer_calls=5,
            eodhd_fundamentals_requests_per_minute=900,
        ),
    )

    rc = cli.cmd_ingest_fundamentals_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )

    assert rc == 0
    assert calls["api_key"] == "TOKEN"
    assert calls["fetched"] == ["AAA.US"]

    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    assert fund_repo.fetch("EODHD", "AAA.US") is not None
    assert fund_repo.fetch("EODHD", "BBB.US") is None


def test_cmd_ingest_fundamentals_global_exits_cleanly_when_budget_exhausted(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "global-no-budget.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "100",
                "apiRequests": "100",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            raise AssertionError("No fetch should happen when the daily budget is 0.")

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_fundamentals_daily_buffer_calls=0,
            eodhd_fundamentals_requests_per_minute=600,
        ),
    )

    rc = cli.cmd_ingest_fundamentals_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )

    assert rc == 0
    assert "No EODHD fundamentals request budget available" in capsys.readouterr().out


def test_cmd_ingest_fundamentals_global_rerun_fetches_remaining_missing_symbols(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "global-rerun.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    calls = {"fetched": []}

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append(symbol)
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_fundamentals_daily_buffer_calls=0,
            eodhd_fundamentals_requests_per_minute=600,
        ),
    )

    rc = cli.cmd_ingest_fundamentals_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=1,
        max_age_days=None,
        respect_backoff=True,
    )
    assert rc == 0
    assert calls["fetched"] == ["AAA.US"]

    calls["fetched"].clear()
    rc = cli.cmd_ingest_fundamentals_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )
    assert rc == 0
    assert calls["fetched"] == ["BBB.US"]


def test_cmd_ingest_fundamentals_global_max_age_days_refreshes_only_stale_or_missing(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "global-stale.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "CCC", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US"
    )
    fund_repo.upsert(
        "EODHD", "BBB.US", {"General": {"CurrencyCode": "USD"}}, exchange="US"
    )
    fresh_at = datetime.now(timezone.utc).isoformat()
    stale_at = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
    with fund_repo._connect() as conn:
        conn.execute(
            """
            UPDATE fundamentals_raw
            SET fetched_at = CASE provider_symbol
                WHEN 'AAA.US' THEN ?
                WHEN 'BBB.US' THEN ?
                ELSE fetched_at
            END
            WHERE provider = 'EODHD'
            """,
            (fresh_at, stale_at),
        )
        conn.execute(
            """
            UPDATE fundamentals_fetch_state
            SET last_fetched_at = CASE provider_symbol
                WHEN 'AAA.US' THEN ?
                WHEN 'BBB.US' THEN ?
                ELSE last_fetched_at
            END
            WHERE provider = 'EODHD'
            """,
            (fresh_at, stale_at),
        )

    calls = {"fetched": []}

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append(symbol)
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_fundamentals_daily_buffer_calls=0,
            eodhd_fundamentals_requests_per_minute=600,
        ),
    )

    rc = cli.cmd_ingest_fundamentals_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=30,
        respect_backoff=True,
    )

    assert rc == 0
    assert set(calls["fetched"]) == {"BBB.US", "CCC.US"}


def test_cmd_ingest_fundamentals_global_respects_backoff_by_default(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "global-respect-backoff.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    state_repo = FundamentalsFetchStateRepository(db_path)
    state_repo.initialize_schema()
    state_repo.mark_failure("EODHD", "BBB.US", "boom", base_backoff_seconds=3600)
    calls = {"fetched": []}

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append(symbol)
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_fundamentals_daily_buffer_calls=0,
            eodhd_fundamentals_requests_per_minute=600,
        ),
    )

    rc = cli.cmd_ingest_fundamentals_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )

    assert rc == 0
    assert calls["fetched"] == ["AAA.US"]


def test_cmd_ingest_fundamentals_global_default_mode_remains_missing_only(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "global-bootstrap-regression.db"
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
    fund_repo.upsert(
        "EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US"
    )
    stale_at = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
    with fund_repo._connect() as conn:
        conn.execute(
            """
            UPDATE fundamentals_raw
            SET fetched_at = ?
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            """,
            (stale_at,),
        )
        conn.execute(
            """
            UPDATE fundamentals_fetch_state
            SET last_fetched_at = ?
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            """,
            (stale_at,),
        )

    calls = {"fetched": []}

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append(symbol)
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_fundamentals_daily_buffer_calls=0,
            eodhd_fundamentals_requests_per_minute=600,
        ),
    )

    rc = cli.cmd_ingest_fundamentals_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )

    assert rc == 0
    assert calls["fetched"] == ["BBB.US"]


def test_rate_limiter_respects_burst_and_waits(monkeypatch):
    now = {"value": 0.0}
    sleeps = []

    def fake_monotonic():
        return now["value"]

    def fake_sleep(seconds):
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


def test_cmd_ingest_fundamentals_global_uses_concurrent_workers(monkeypatch, tmp_path):
    db_path = tmp_path / "global-concurrent.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "CCC", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "DDD", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    calls = {"in_flight": 0, "max_in_flight": 0, "fetched": []}
    lock = threading.Lock()

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "100000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            with lock:
                calls["in_flight"] += 1
                calls["max_in_flight"] = max(calls["max_in_flight"], calls["in_flight"])
            try:
                time.sleep(0.05)
                calls["fetched"].append(symbol)
                return {"General": {"CurrencyCode": "USD", "Name": symbol}}
            finally:
                with lock:
                    calls["in_flight"] -= 1

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_fundamentals_daily_buffer_calls=0,
            eodhd_fundamentals_requests_per_minute=950,
        ),
    )

    rc = cli.cmd_ingest_fundamentals_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )

    assert rc == 0
    assert set(calls["fetched"]) == {"AAA.US", "BBB.US", "CCC.US", "DDD.US"}
    assert calls["max_in_flight"] > 1


def test_cmd_ingest_fundamentals_global_batches_success_state_updates(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "global-batch-state.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    success_batches = []
    original_mark_success_many = FundamentalsFetchStateRepository.mark_success_many

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "100000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

    def fail_mark_success(self, provider, symbol, fetched_at=None):
        raise AssertionError("multi-symbol ingestion should not call mark_success")

    def track_mark_success_many(self, provider, symbols, fetched_at=None):
        success_batches.append(list(symbols))
        return original_mark_success_many(
            self, provider, symbols, fetched_at=fetched_at
        )

    def fail_single_upsert(
        self, provider, symbol, payload, currency=None, exchange=None
    ):
        raise AssertionError("multi-symbol ingestion should not call upsert")

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_fundamentals_daily_buffer_calls=0,
            eodhd_fundamentals_requests_per_minute=950,
        ),
    )
    monkeypatch.setattr(
        FundamentalsFetchStateRepository, "mark_success", fail_mark_success
    )
    monkeypatch.setattr(
        FundamentalsFetchStateRepository,
        "mark_success_many",
        track_mark_success_many,
    )
    monkeypatch.setattr(FundamentalsRepository, "upsert", fail_single_upsert)

    rc = cli.cmd_ingest_fundamentals_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )

    assert rc == 0
    assert len(success_batches) == 1
    assert set(success_batches[0]) == {"AAA.US", "BBB.US"}

    state_repo = FundamentalsFetchStateRepository(db_path)
    assert state_repo.fetch("EODHD", "AAA.US")["last_status"] == "ok"
    assert state_repo.fetch("EODHD", "BBB.US")["last_status"] == "ok"


def test_cmd_ingest_fundamentals_global_flushes_batches_on_keyboard_interrupt(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "global-interrupt.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
    )

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "100000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

    def interrupting_as_completed(futures):
        yielded = False
        for future in futures:
            if not yielded:
                yielded = True
                yield future
                raise KeyboardInterrupt

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_fundamentals_daily_buffer_calls=0,
            eodhd_fundamentals_requests_per_minute=950,
        ),
    )
    monkeypatch.setattr(cli, "as_completed", interrupting_as_completed)
    shutdown_calls = []
    monkeypatch.setattr(
        cli,
        "_shutdown_executor_now",
        lambda executor: shutdown_calls.append(executor),
    )

    rc = cli.cmd_ingest_fundamentals_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=None,
        respect_backoff=True,
    )

    assert rc == 1
    output = capsys.readouterr().out
    assert "Cancelled after 1 completed symbols." in output
    assert "Stored fundamentals for" not in output
    assert len(shutdown_calls) == 1
    fund_repo = FundamentalsRepository(db_path)
    assert fund_repo.fetch("EODHD", "AAA.US") is not None


def test_interruptible_thread_executor_workers_skip_python_exit_registry():
    started = threading.Event()
    release = threading.Event()

    def blocking_task():
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
    monkeypatch, tmp_path, capsys
):
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
        fund_repo.upsert(
            "EODHD", symbol, {"General": {"CurrencyCode": "USD"}}, exchange="US"
        )

    class FakeClient:
        def __init__(self, api_key):
            assert api_key == "TOKEN"

        def user_metadata(self):
            return {
                "dailyRateLimit": "100000",
                "apiRequests": "5000",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(
        cli,
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
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "report-missing.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    monkeypatch.setattr(
        cli,
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
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "report-stale.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US"
    )
    stale_at = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
    with fund_repo._connect() as conn:
        conn.execute(
            """
            UPDATE fundamentals_raw
            SET fetched_at = ?
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            """,
            (stale_at,),
        )
        conn.execute(
            """
            UPDATE fundamentals_fetch_state
            SET last_fetched_at = ?
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            """,
            (stale_at,),
        )
    monkeypatch.setattr(
        cli,
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
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "report-missing-only.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US"
    )
    stale_at = (datetime.now(timezone.utc) - timedelta(days=120)).isoformat()
    with fund_repo._connect() as conn:
        conn.execute(
            """
            UPDATE fundamentals_raw
            SET fetched_at = ?
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            """,
            (stale_at,),
        )
        conn.execute(
            """
            UPDATE fundamentals_fetch_state
            SET last_fetched_at = ?
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            """,
            (stale_at,),
        )
    monkeypatch.setattr(
        cli,
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
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "report-blocked.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US"
    )
    state_repo = FundamentalsFetchStateRepository(db_path)
    state_repo.initialize_schema()
    state_repo.mark_failure("EODHD", "AAA.US", "boom", base_backoff_seconds=3600)
    monkeypatch.setattr(
        cli,
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


def test_cmd_report_ingest_progress_filters_exchanges(monkeypatch, tmp_path, capsys):
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
    fund_repo.upsert(
        "EODHD", "BBB.LSE", {"General": {"CurrencyCode": "GBP"}}, exchange="LSE"
    )
    monkeypatch.setattr(
        cli,
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
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "report-user-api-fails.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )

    class FakeClient:
        def __init__(self, api_key):
            assert api_key == "TOKEN"

        def user_metadata(self):
            raise RuntimeError("nope")

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(
        cli,
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


def test_cmd_update_market_data_global_uses_supported_tickers(monkeypatch, tmp_path):
    db_path = tmp_path / "global-market-data.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    calls = {"refreshed": []}
    today = date.today().isoformat()

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeService:
        def __init__(self, db_path, config=None):
            self.repo = MarketDataRepository(db_path)
            self.repo.initialize_schema()
            calls["config"] = config

        def refresh_symbol(self, symbol):
            calls["refreshed"].append(symbol)
            self.repo.upsert_price(symbol, today, 10.0)
            return SimpleNamespace(symbol=symbol, as_of=today, price=10.0)

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "MarketDataService", FakeService)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )

    rc = cli.cmd_update_market_data_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=7,
        respect_backoff=True,
    )

    assert rc == 0
    assert calls["api_key"] == "TOKEN"
    assert calls["refreshed"] == ["AAA.US", "BBB.US"]
    state_repo = MarketDataFetchStateRepository(db_path)
    assert state_repo.fetch("EODHD", "AAA.US")["last_status"] == "ok"
    assert state_repo.fetch("EODHD", "BBB.US")["last_status"] == "ok"


def test_plan_market_data_stage_run_uses_bulk_for_large_exchange():
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


def test_plan_market_data_stage_run_falls_back_to_symbols_when_bulk_does_not_fit_budget():
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
    monkeypatch, tmp_path
):
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
    calls = {"bulk": [], "symbols": []}
    today = date.today().isoformat()

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeProvider:
        def __init__(self, api_key: str, session=None):
            assert api_key == "TOKEN"

        def latest_prices_for_exchange(self, exchange_code: str):
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

        def latest_price(self, symbol: str):
            calls["symbols"].append(symbol)
            return PriceData(
                symbol=symbol,
                price=20.0,
                as_of=today,
                volume=50,
                currency="GBP",
            )

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "EODHDProvider", FakeProvider)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
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
    assert state_repo.fetch("EODHD", "U000.US")["last_status"] == "ok"
    assert state_repo.fetch("EODHD", "SMALL.LSE")["last_status"] == "ok"


def test_cmd_update_market_data_stage_retries_missing_bulk_symbol_individually(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "stage-market-data-fallback.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": f"U{i:03d}", "Exchange": "US", "Type": "Common Stock"}
            for i in range(100)
        ],
    )
    calls = {"bulk": [], "symbols": []}
    today = date.today().isoformat()

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeProvider:
        def __init__(self, api_key: str, session=None):
            assert api_key == "TOKEN"

        def latest_prices_for_exchange(self, exchange_code: str):
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

        def latest_price(self, symbol: str):
            calls["symbols"].append(symbol)
            return PriceData(
                symbol=symbol,
                price=999.0,
                as_of=today,
                volume=999,
                currency="USD",
            )

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "EODHDProvider", FakeProvider)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
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
    assert state_repo.fetch("EODHD", "U099.US")["last_status"] == "ok"


def test_cmd_update_market_data_stage_interrupts_cleanly_in_symbol_phase(
    monkeypatch, tmp_path, capsys
):
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
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class InlineExecutor:
        def __init__(self):
            self.shutdown_calls = []

        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait=True, cancel_futures=False):
            self.shutdown_calls.append((wait, cancel_futures))

    def interrupting_as_completed(futures):
        yielded = False
        for future in futures:
            if not yielded:
                yielded = True
                yield future
                raise KeyboardInterrupt

    executor = InlineExecutor()
    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )
    monkeypatch.setattr(
        cli,
        "_create_interruptible_thread_executor",
        lambda max_workers: executor,
    )
    monkeypatch.setattr(
        cli,
        "_fetch_symbol_market_data",
        lambda api_key, limiter, symbol: PriceData(
            symbol=symbol,
            price=10.0,
            as_of="2024-01-01",
            volume=100,
            currency="USD",
        ),
    )
    monkeypatch.setattr(cli, "as_completed", interrupting_as_completed)

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
    assert state_repo.fetch("EODHD", "AAA.US")["last_status"] == "ok"


def test_cmd_update_market_data_global_prefers_missing_then_oldest_stale(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "global-market-data-order.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "CCC", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "DDD", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    store_market_data(
        db_path,
        "BBB.US",
        (date.today() - timedelta(days=30)).isoformat(),
    )
    store_market_data(
        db_path,
        "CCC.US",
        (date.today() - timedelta(days=12)).isoformat(),
    )
    store_market_data(
        db_path,
        "DDD.US",
        (date.today() - timedelta(days=1)).isoformat(),
    )
    calls = {"refreshed": []}
    today = date.today().isoformat()

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeService:
        def __init__(self, db_path, config=None):
            self.repo = MarketDataRepository(db_path)
            self.repo.initialize_schema()

        def refresh_symbol(self, symbol):
            calls["refreshed"].append(symbol)
            self.repo.upsert_price(symbol, today, 10.0)
            return SimpleNamespace(symbol=symbol, as_of=today, price=10.0)

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "MarketDataService", FakeService)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )

    rc = cli.cmd_update_market_data_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=7,
        respect_backoff=True,
    )

    assert rc == 0
    assert calls["refreshed"] == ["AAA.US", "BBB.US", "CCC.US"]


def test_cmd_update_market_data_global_exits_cleanly_when_budget_exhausted(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "global-market-data-no-budget.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "100",
                "apiRequests": "100",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeService:
        def __init__(self, db_path, config=None):
            raise AssertionError(
                "No service should be created when the daily budget is 0."
            )

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "MarketDataService", FakeService)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )

    rc = cli.cmd_update_market_data_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=7,
        respect_backoff=True,
    )

    assert rc == 0
    assert "No EODHD market data request budget available" in capsys.readouterr().out


def test_cmd_update_market_data_global_rerun_fetches_remaining_missing_or_stale(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "global-market-data-rerun.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    calls = {"refreshed": []}
    today = date.today().isoformat()

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeService:
        def __init__(self, db_path, config=None):
            self.repo = MarketDataRepository(db_path)
            self.repo.initialize_schema()

        def refresh_symbol(self, symbol):
            calls["refreshed"].append(symbol)
            self.repo.upsert_price(symbol, today, 10.0)
            return SimpleNamespace(symbol=symbol, as_of=today, price=10.0)

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "MarketDataService", FakeService)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )

    rc = cli.cmd_update_market_data_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=1,
        max_age_days=7,
        respect_backoff=True,
    )
    assert rc == 0
    assert calls["refreshed"] == ["AAA.US"]

    calls["refreshed"].clear()
    rc = cli.cmd_update_market_data_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=7,
        respect_backoff=True,
    )
    assert rc == 0
    assert calls["refreshed"] == ["BBB.US"]


def test_cmd_update_market_data_global_respects_backoff_by_default(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "global-market-data-respect-backoff.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[
            {"Code": "AAA", "Exchange": "US", "Type": "Common Stock"},
            {"Code": "BBB", "Exchange": "US", "Type": "Common Stock"},
        ],
    )
    state_repo = MarketDataFetchStateRepository(db_path)
    state_repo.initialize_schema()
    state_repo.mark_failure("EODHD", "BBB.US", "boom", base_backoff_seconds=3600)
    calls = {"refreshed": []}
    today = date.today().isoformat()

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "1000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    class FakeService:
        def __init__(self, db_path, config=None):
            self.repo = MarketDataRepository(db_path)
            self.repo.initialize_schema()

        def refresh_symbol(self, symbol):
            calls["refreshed"].append(symbol)
            self.repo.upsert_price(symbol, today, 10.0)
            return SimpleNamespace(symbol=symbol, as_of=today, price=10.0)

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "MarketDataService", FakeService)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")
    monkeypatch.setattr(
        cli,
        "Config",
        lambda: SimpleNamespace(
            eodhd_api_key="TOKEN",
            eodhd_market_data_daily_buffer_calls=0,
            eodhd_market_data_requests_per_minute=950,
        ),
    )

    rc = cli.cmd_update_market_data_global(
        provider="EODHD",
        database=str(db_path),
        exchange_codes=None,
        rate=None,
        max_symbols=None,
        max_age_days=7,
        respect_backoff=True,
    )

    assert rc == 0
    assert calls["refreshed"] == ["AAA.US"]


def test_cmd_report_market_data_progress_reports_complete_with_quota_snapshot(
    monkeypatch, tmp_path, capsys
):
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
        def __init__(self, api_key):
            assert api_key == "TOKEN"

        def user_metadata(self):
            return {
                "dailyRateLimit": "100000",
                "apiRequests": "5000",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(
        cli,
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
    monkeypatch, tmp_path, capsys
):
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
    monkeypatch.setattr(
        cli,
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
    monkeypatch, tmp_path, capsys
):
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
    monkeypatch.setattr(
        cli,
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
    monkeypatch, tmp_path, capsys
):
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
    monkeypatch.setattr(
        cli,
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
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "report-market-data-user-api-fails.db"
    store_supported_tickers(
        db_path,
        "US",
        rows=[{"Code": "AAA", "Exchange": "US", "Type": "Common Stock"}],
    )

    class FakeClient:
        def __init__(self, api_key):
            assert api_key == "TOKEN"

        def user_metadata(self):
            raise RuntimeError("nope")

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(
        cli,
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


def test_cmd_ingest_fundamentals_bulk_skips_fresh_and_backoff(monkeypatch, tmp_path):
    calls = {"fetched": []}

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def user_metadata(self):
            return {
                "dailyRateLimit": "100000",
                "apiRequests": "0",
                "apiRequestsDate": datetime.now(timezone.utc).date().isoformat(),
            }

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append((symbol, exchange_code))
            return {"General": {"CurrencyCode": "USD"}}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")

    db_path = tmp_path / "eodhd-resume.db"
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
    fund_repo.upsert(
        "EODHD", "AAA.US", {"General": {"CurrencyCode": "USD"}}, exchange="US"
    )

    state_repo = FundamentalsFetchStateRepository(db_path)
    state_repo.initialize_schema()
    state_repo.mark_failure("EODHD", "BBB.US", "boom", base_backoff_seconds=3600)

    rc = cli.cmd_ingest_fundamentals_bulk(
        provider="EODHD",
        database=str(db_path),
        rate=None,
        exchange_code="US",
        user_agent=None,
        max_symbols=None,
        max_age_days=30,
        respect_backoff=True,
    )

    assert rc == 0
    assert calls["fetched"] == []

    calls["fetched"].clear()
    rc = cli.cmd_ingest_fundamentals_bulk(
        provider="EODHD",
        database=str(db_path),
        rate=None,
        exchange_code="US",
        user_agent=None,
        max_symbols=None,
        max_age_days=30,
        respect_backoff=False,
    )

    assert rc == 0
    assert calls["fetched"] == [("BBB.US", None)]


def test_cmd_update_market_data_bulk(monkeypatch, tmp_path):
    db_path = tmp_path / "marketbulk.db"
    listings = [
        Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
        Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
    ]
    store_catalog_listings(db_path, "US", listings, provider="SEC")

    calls = []

    class DummyService:
        def __init__(self, db_path):
            self.db_path = db_path

        def refresh_symbol(self, symbol, fetch_symbol=None):
            calls.append(symbol)

    monkeypatch.setattr(cli, "MarketDataService", lambda db_path: DummyService(db_path))

    rc = cli.cmd_update_market_data_bulk(
        provider="SEC",
        database=str(db_path),
        rate=0,
        exchange_code="US",
    )
    assert rc == 0
    assert calls == ["AAA.US", "BBB.US"]


def test_cmd_update_market_data_bulk_with_exchange(monkeypatch, tmp_path):
    db_path = tmp_path / "marketbulk_exchange.db"
    store_catalog_listings(
        db_path,
        "LSE",
        [Listing(symbol="AAA", security_name="AAA PLC", exchange="LSE")],
        provider="EODHD",
    )
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE")],
        provider="SEC",
    )

    calls = []

    class DummyService:
        def __init__(self, db_path):
            self.db_path = db_path

        def refresh_symbol(self, symbol, fetch_symbol=None):
            calls.append((symbol, fetch_symbol))

    monkeypatch.setattr(cli, "MarketDataService", lambda db_path: DummyService(db_path))

    rc = cli.cmd_update_market_data_bulk(
        provider="EODHD",
        database=str(db_path),
        rate=0,
        exchange_code="LSE",
    )
    assert rc == 0
    assert calls == [("AAA.LSE", "AAA.LSE")]


def test_cmd_compute_metrics_bulk(monkeypatch, tmp_path):
    db_path = tmp_path / "metricsbulk.db"
    listings = [
        Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
        Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
    ]
    store_catalog_listings(db_path, "US", listings, provider="SEC")

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    for symbol in ["AAA.US", "BBB.US"]:
        fact_repo.replace_facts(symbol, [])

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(self, symbol, repo):
            return MetricResult(
                symbol=symbol, metric_id=self.id, value=len(symbol), as_of="2024-01-01"
            )

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})

    rc = cli.cmd_compute_metrics_bulk(
        provider="SEC",
        database=str(db_path),
        metric_ids=None,
        exchange_code="US",
    )
    assert rc == 0

    assert metrics_repo.fetch("AAA.US", "dummy_metric")[0] == len("AAA.US")
    assert metrics_repo.fetch("BBB.US", "dummy_metric")[0] == len("BBB.US")


def test_cmd_compute_metrics_bulk_with_exchange(monkeypatch, tmp_path):
    db_path = tmp_path / "metrics_exchange.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="US"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="US"),
        ],
        provider="SEC",
    )
    store_catalog_listings(
        db_path,
        "LSE",
        [Listing(symbol="CCC.LSE", security_name="CCC PLC", exchange="LSE")],
        provider="EODHD",
    )

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(self, symbol, repo):
            return MetricResult(
                symbol=symbol, metric_id=self.id, value=1.0, as_of="2024-01-01"
            )

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})

    rc = cli.cmd_compute_metrics_bulk(
        provider="EODHD",
        database=str(db_path),
        metric_ids=None,
        exchange_code="LSE",
    )
    assert rc == 0

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    rows = (
        metrics_repo._connect()
        .execute(
            """
            SELECT s.canonical_symbol
            FROM metrics m
            JOIN securities s ON s.security_id = m.security_id
            ORDER BY s.canonical_symbol
            """
        )
        .fetchall()
    )
    assert [row[0] for row in rows] == ["CCC.LSE"]


def test_cmd_compute_metrics_bulk_requires_supported_tickers(monkeypatch, tmp_path):
    db_path = tmp_path / "fundmetrics.db"
    # No listings stored; only fundamentals exist.
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert("EODHD", "AAA.LSE", {"dummy": True})

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAA.LSE",
        [
            make_fact(
                symbol="AAA.LSE",
                concept="NetIncomeLoss",
                end_date="2023-12-31",
                value=10.0,
            )
        ],
    )

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(self, symbol, repo):
            return MetricResult(
                symbol=symbol, metric_id=self.id, value=len(symbol), as_of="2024-01-01"
            )

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})

    with pytest.raises(SystemExit) as exc:
        cli.cmd_compute_metrics_bulk(
            provider="EODHD",
            database=str(db_path),
            metric_ids=None,
            exchange_code="LSE",
        )
    assert "No supported tickers found for provider EODHD on exchange LSE" in str(
        exc.value
    )


def test_compute_metrics_for_symbol_reuses_fact_and_market_cache(monkeypatch, tmp_path):
    db_path = tmp_path / "metric-cache.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
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
    market_repo.upsert_price("AAA.US", "2024-12-31", price=25.0)

    fact_calls = {"count": 0}
    market_calls = {"count": 0}
    original_facts_for_symbol = FinancialFactsRepository.facts_for_symbol
    original_latest_snapshot = MarketDataRepository.latest_snapshot

    def counting_facts_for_symbol(self, symbol):
        fact_calls["count"] += 1
        return original_facts_for_symbol(self, symbol)

    def counting_latest_snapshot(self, symbol):
        market_calls["count"] += 1
        return original_latest_snapshot(self, symbol)

    monkeypatch.setattr(
        FinancialFactsRepository,
        "facts_for_symbol",
        counting_facts_for_symbol,
    )
    monkeypatch.setattr(
        MarketDataRepository,
        "latest_snapshot",
        counting_latest_snapshot,
    )

    class RepeatedFactsMetric:
        id = "repeat_facts"
        required_concepts = ("AssetsCurrent",)
        uses_market_data = False

        def compute(self, symbol, repo):
            latest_a = repo.latest_fact(symbol, "AssetsCurrent")
            latest_b = repo.latest_fact(symbol, "AssetsCurrent")
            series_a = repo.facts_for_concept(symbol, "EarningsPerShare", "FY")
            series_b = repo.facts_for_concept(symbol, "EarningsPerShare", "FY")
            return MetricResult(
                symbol=symbol,
                metric_id=self.id,
                value=latest_a.value + latest_b.value + len(series_a) + len(series_b),
                as_of=latest_a.end_date,
            )

    class RepeatedMarketMetric:
        id = "repeat_market"
        required_concepts = ()
        uses_market_data = True

        def compute(self, symbol, repo, market_repo):
            snapshot_a = market_repo.latest_snapshot(symbol)
            snapshot_b = market_repo.latest_snapshot(symbol)
            price = market_repo.latest_price(symbol)
            return MetricResult(
                symbol=symbol,
                metric_id=self.id,
                value=snapshot_a.price + snapshot_b.price + price[1],
                as_of=snapshot_a.as_of,
            )

    monkeypatch.setattr(
        cli,
        "REGISTRY",
        {
            RepeatedFactsMetric.id: RepeatedFactsMetric,
            RepeatedMarketMetric.id: RepeatedMarketMetric,
        },
    )

    result = cli._compute_metrics_for_symbol(
        "AAA.US",
        [RepeatedFactsMetric.id, RepeatedMarketMetric.id],
        FinancialFactsRepository(db_path),
        MarketDataRepository(db_path),
    )

    assert result.computed_count == 2
    assert fact_calls["count"] == 1
    assert market_calls["count"] == 1


def test_compute_metrics_for_symbol_matches_real_metrics(tmp_path):
    db_path = tmp_path / "metric-correctness.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    recent = (date.today() - timedelta(days=15)).isoformat()
    current_year = date.today().year
    fact_repo.replace_facts(
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
                frame=f"CY{current_year - 6}",
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{current_year - 5}-12-31",
                value=1.1,
                frame=f"CY{current_year - 5}",
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{current_year - 4}-12-31",
                value=1.2,
                frame=f"CY{current_year - 4}",
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{current_year - 3}-12-31",
                value=1.3,
                frame=f"CY{current_year - 3}",
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{current_year - 2}-12-31",
                value=1.4,
                frame=f"CY{current_year - 2}",
            ),
            make_fact(
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{current_year - 1}-12-31",
                value=1.5,
                frame=f"CY{current_year - 1}",
            ),
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAA.US", recent, price=25.0, market_cap=2500.0)

    metric_ids = ["working_capital", "market_cap", "eps_6y_avg"]
    expected = {}
    plain_fact_repo = RegionFactsRepository(FinancialFactsRepository(db_path))
    plain_market_repo = MarketDataRepository(db_path)
    for metric_id in metric_ids:
        metric = REGISTRY[metric_id]()
        if getattr(metric, "uses_market_data", False):
            result = metric.compute("AAA.US", plain_fact_repo, plain_market_repo)
        else:
            result = metric.compute("AAA.US", plain_fact_repo)
        expected[metric_id] = (result.value, result.as_of)

    computed = cli._compute_metrics_for_symbol(
        "AAA.US",
        metric_ids,
        FinancialFactsRepository(db_path),
        MarketDataRepository(db_path),
    )

    assert computed.computed_count == 3
    assert {
        metric_id: (value, as_of) for _, metric_id, value, as_of in computed.rows
    } == expected


def test_suppress_console_metric_warnings_filters_only_metric_noise(tmp_path, capsys):
    log_dir = tmp_path / "logs"
    clear_root_logging_handlers()
    cli.setup_logging(log_dir=log_dir)
    try:
        with cli.suppress_console_metric_warnings(True):
            logging.getLogger("pyvalue.metrics.test").warning("metric noise")
            logging.getLogger("pyvalue.cli").warning(
                "Metric %s could not be computed for %s",
                "dummy_metric",
                "AAA.US",
            )
            logging.getLogger("pyvalue.cli").warning("Operational warning")

        captured = capsys.readouterr()
        assert "metric noise" not in captured.err
        assert (
            "Metric dummy_metric could not be computed for AAA.US" not in captured.err
        )
        assert "Operational warning" in captured.err

        log_text = (log_dir / "pyvalue.log").read_text(encoding="utf-8")
        assert "metric noise" in log_text
        assert "Metric dummy_metric could not be computed for AAA.US" in log_text
        assert "Operational warning" in log_text
    finally:
        clear_root_logging_handlers()


def test_cmd_compute_metrics_stage_suppresses_metric_warnings_by_default(
    monkeypatch, tmp_path, capsys
):
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

        def compute(self, symbol, repo):
            return None

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})
    monkeypatch.setattr(cli, "_metric_worker_count", lambda total: 1)
    monkeypatch.setattr(cli, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)
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
    assert "Progress: 1/1 symbols complete (100.0%)" in captured.out
    log_text = (log_dir / "pyvalue.log").read_text(encoding="utf-8")
    assert "Metric dummy_metric could not be computed for AAA.US" in log_text


def test_cmd_compute_metrics_stage_can_show_metric_warnings_on_console(
    monkeypatch, tmp_path, capsys
):
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

        def compute(self, symbol, repo):
            return None

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})
    monkeypatch.setattr(cli, "_metric_worker_count", lambda total: 1)
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


def test_cmd_run_screen_stage_suppresses_metric_warnings_on_console_by_default(
    tmp_path, capsys
):
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


def test_cmd_run_screen_stage_can_show_metric_warnings_on_console(tmp_path, capsys):
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


def test_cmd_compute_metrics_stage_symbol_scope(monkeypatch, tmp_path):
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

        def compute(self, symbol, repo):
            return MetricResult(
                symbol=symbol,
                metric_id=self.id,
                value=float(len(symbol)),
                as_of="2024-01-01",
            )

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})

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
    assert repo.fetch("AAA.US", "dummy_metric") is None
    assert repo.fetch("BBB.US", "dummy_metric") == (6.0, "2024-01-01")


def test_cmd_compute_metrics_stage_exchange_scope(monkeypatch, tmp_path):
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

        def compute(self, symbol, repo):
            return MetricResult(
                symbol=symbol, metric_id=self.id, value=1.0, as_of="2024-01-01"
            )

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})

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
    assert repo.fetch("AAA.US", "dummy_metric") is None
    assert repo.fetch("BBB.LSE", "dummy_metric") == (1.0, "2024-01-01")


def test_cmd_compute_metrics_stage_all_supported_scope(monkeypatch, tmp_path):
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

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(self, symbol, repo):
            return MetricResult(
                symbol=symbol, metric_id=self.id, value=2.0, as_of="2024-01-01"
            )

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})

    rc = cli.cmd_compute_metrics_stage(
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
        metric_ids=None,
    )

    assert rc == 0
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    assert repo.fetch("AAA.US", "dummy_metric") == (2.0, "2024-01-01")
    assert repo.fetch("BBB.LSE", "dummy_metric") == (2.0, "2024-01-01")


def test_cmd_compute_metrics_stage_parallel_with_inline_executor(
    monkeypatch, tmp_path, capsys
):
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

        def compute(self, symbol, repo):
            return MetricResult(
                symbol=symbol, metric_id=self.id, value=1.0, as_of="2024-01-01"
            )

    class InlineExecutor:
        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait=True, cancel_futures=False):
            return None

    def reverse_as_completed(futures):
        return [
            future
            for future, _ in sorted(
                futures.items(), key=lambda item: item[1], reverse=True
            )
        ]

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})
    monkeypatch.setattr(cli, "_metric_worker_count", lambda total: 2)
    monkeypatch.setattr(
        cli,
        "_create_normalization_executor",
        lambda max_workers: InlineExecutor(),
    )
    monkeypatch.setattr(cli, "as_completed", reverse_as_completed)
    monkeypatch.setattr(cli, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)

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
        "Progress: 1/2 symbols complete (50.0%)",
        "Progress: 2/2 symbols complete (100.0%)",
    ]
    assert not any(line.startswith("[") for line in output_lines)


def test_cmd_compute_metrics_stage_parallel_partial_failure(
    monkeypatch, tmp_path, capsys
):
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
        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait=True, cancel_futures=False):
            return None

    def fake_worker(database, symbol, metric_ids):
        if symbol == "BBB.US":
            raise ValueError("boom")
        return cli._ComputedMetricsResult(
            symbol=symbol,
            rows=((symbol, "dummy_metric", 1.0, "2024-01-01"),),
            computed_count=1,
        )

    class DummyMetric:
        id = "dummy_metric"
        uses_market_data = False

    monkeypatch.setattr(cli, "_metric_worker_count", lambda total: 2)
    monkeypatch.setattr(
        cli,
        "_create_normalization_executor",
        lambda max_workers: InlineExecutor(),
    )
    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})
    monkeypatch.setattr(cli, "_compute_metrics_for_symbol_worker", fake_worker)
    monkeypatch.setattr(cli, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)

    rc = cli._run_metric_computation(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        metric_ids=["dummy_metric"],
        cancelled_message="\nMetric computation cancelled by user.",
    )

    assert rc == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert [line for line in output_lines if line.startswith("Progress:")] == [
        "Progress: 1/2 symbols complete (50.0%)",
        "Progress: 2/2 symbols complete (100.0%)",
    ]
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    assert repo.fetch("AAA.US", "dummy_metric") == (1.0, "2024-01-01")
    assert repo.fetch("BBB.US", "dummy_metric") is None


def test_run_metric_computation_interrupts_cleanly(monkeypatch, tmp_path, capsys):
    db_path = tmp_path / "metric-stage-interrupt.db"

    class DummyMetric:
        id = "dummy_metric"
        uses_market_data = False

    class InlineExecutor:
        def __init__(self):
            self.shutdown_calls = []

        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait=True, cancel_futures=False):
            self.shutdown_calls.append((wait, cancel_futures))

    def fake_worker(database, symbol, metric_ids):
        return cli._ComputedMetricsResult(
            symbol=symbol,
            rows=((symbol, "dummy_metric", 1.0, "2024-01-01"),),
            computed_count=1,
        )

    def interrupting_as_completed(futures):
        yielded = False
        for future in futures:
            if not yielded:
                yielded = True
                yield future
                raise KeyboardInterrupt

    executor = InlineExecutor()
    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})
    monkeypatch.setattr(cli, "_metric_worker_count", lambda total: 2)
    monkeypatch.setattr(cli, "_ensure_metrics_wal_mode", lambda repo: "wal")
    monkeypatch.setattr(
        cli,
        "_create_normalization_executor",
        lambda max_workers: executor,
    )
    monkeypatch.setattr(cli, "_compute_metrics_for_symbol_worker", fake_worker)
    monkeypatch.setattr(cli, "as_completed", interrupting_as_completed)
    monkeypatch.setattr(cli, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)

    rc = cli._run_metric_computation(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        metric_ids=["dummy_metric"],
        cancelled_message="\nMetric computation cancelled by user.",
    )

    assert rc == 1
    output_lines = capsys.readouterr().out.splitlines()
    assert "Metric computation cancelled by user." in output_lines
    assert "Computed metrics for 2 symbols in" not in "\n".join(output_lines)
    assert any(line.startswith("Progress: 1/2") for line in output_lines)
    assert executor.shutdown_calls == [(False, True)]
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    assert repo.fetch("AAA.US", "dummy_metric") == (1.0, "2024-01-01")
    assert repo.fetch("BBB.US", "dummy_metric") is None


def test_cmd_compute_metrics_stage_falls_back_to_serial_without_wal(
    monkeypatch, tmp_path, capsys
):
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
    fact_repo.replace_facts(
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
    fact_repo.replace_facts(
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

    monkeypatch.setattr(cli, "_metric_worker_count", lambda total: 2)
    monkeypatch.setattr(cli, "_ensure_metrics_wal_mode", lambda repo: "delete")
    monkeypatch.setattr(cli, "METRICS_PROGRESS_INTERVAL_SECONDS", 0.0)

    def fail_executor(max_workers):
        raise AssertionError("process executor should not be used without WAL")

    monkeypatch.setattr(cli, "_create_normalization_executor", fail_executor)

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
        "Progress: 1/2 symbols complete (50.0%)",
        "Progress: 2/2 symbols complete (100.0%)",
    ]
    assert not any(line.startswith("[") for line in output_lines)
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    assert repo.fetch("AAA.US", "working_capital") == (7.0, recent_date)
    assert repo.fetch("BBB.US", "working_capital") == (6.0, recent_date)


def test_run_metric_computation_batches_metric_writes(monkeypatch, tmp_path):
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
        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait=True, cancel_futures=False):
            return None

    def fake_worker(database, symbol, metric_ids):
        return cli._ComputedMetricsResult(
            symbol=symbol,
            rows=((symbol, "dummy_metric", float(len(symbol)), "2024-01-01"),),
            computed_count=1,
        )

    batch_sizes = []
    original_upsert_many = MetricsRepository.upsert_many

    def recording_upsert_many(self, rows):
        materialized = list(rows)
        batch_sizes.append(len(materialized))
        return original_upsert_many(self, materialized)

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})
    monkeypatch.setattr(cli, "_metric_worker_count", lambda total: 2)
    monkeypatch.setattr(cli, "_ensure_metrics_wal_mode", lambda repo: "wal")
    monkeypatch.setattr(
        cli,
        "_create_normalization_executor",
        lambda max_workers: InlineExecutor(),
    )
    monkeypatch.setattr(cli, "_compute_metrics_for_symbol_worker", fake_worker)
    monkeypatch.setattr(cli, "METRICS_WRITE_BATCH_SIZE", 2)
    monkeypatch.setattr(cli, "METRICS_WRITE_BATCH_INTERVAL_SECONDS", 999.0)
    monkeypatch.setattr(MetricsRepository, "upsert_many", recording_upsert_many)

    rc = cli._run_metric_computation(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US", "CCC.US"],
        metric_ids=["dummy_metric"],
        cancelled_message="\nMetric computation cancelled by user.",
    )

    assert rc == 0
    assert batch_sizes == [2, 1]
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    assert repo.fetch("AAA.US", "dummy_metric") == (6.0, "2024-01-01")
    assert repo.fetch("BBB.US", "dummy_metric") == (6.0, "2024-01-01")
    assert repo.fetch("CCC.US", "dummy_metric") == (6.0, "2024-01-01")


def test_cmd_compute_metrics_stage_parallel_workers_skip_schema_init(
    monkeypatch, tmp_path
):
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
    fact_repo.replace_facts(
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
    fact_repo.replace_facts(
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
    market_repo.upsert_price("AAA.US", recent_date, 12.0, market_cap=120.0)
    market_repo.upsert_price("BBB.US", recent_date, 9.0, market_cap=90.0)

    class InlineExecutor:
        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait=True, cancel_futures=False):
            return None

    monkeypatch.setattr(cli, "_metric_worker_count", lambda total: 2)
    monkeypatch.setattr(
        cli,
        "_create_normalization_executor",
        lambda max_workers: InlineExecutor(),
    )
    monkeypatch.setattr(
        cli, "_initialize_metric_read_schema", lambda *args, **kwargs: None
    )

    def locked_initialize_schema(self):
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

    rc = cli._run_metric_computation(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        metric_ids=["working_capital", "market_cap"],
        cancelled_message="\nMetric computation cancelled by user.",
    )

    assert rc == 0
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    assert repo.fetch("AAA.US", "working_capital") == (7.0, recent_date)
    assert repo.fetch("BBB.US", "working_capital") == (6.0, recent_date)
    assert repo.fetch("AAA.US", "market_cap") == (120.0, recent_date)
    assert repo.fetch("BBB.US", "market_cap") == (90.0, recent_date)


def test_cmd_compute_metrics_stage_process_pool_smoke(monkeypatch, tmp_path):
    if cli.os.name != "posix":
        pytest.skip("requires POSIX fork support")

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

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(self, symbol, repo):
            return MetricResult(
                symbol=symbol,
                metric_id=self.id,
                value=float(len(symbol)),
                as_of="2024-01-01",
            )

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})
    monkeypatch.setattr(cli, "_metric_worker_count", lambda total: 2)

    rc = cli.cmd_compute_metrics_stage(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
        metric_ids=None,
    )

    assert rc == 0
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    assert repo.fetch("AAA.US", "dummy_metric") == (6.0, "2024-01-01")
    assert repo.fetch("BBB.US", "dummy_metric") == (6.0, "2024-01-01")


def test_cmd_clear_fundamentals_raw(tmp_path):
    db_path = tmp_path / "clearfunds.db"
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    repo.upsert("SEC", "AAA.US", {"facts": {}})
    state_repo = FundamentalsNormalizationStateRepository(db_path)
    security_id = repo._security_repo().ensure_from_symbol("AAA.US").security_id
    state_repo.mark_success(
        "SEC",
        "AAA.US",
        security_id=security_id,
        raw_fetched_at="2024-01-01T00:00:00+00:00",
    )

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


def test_cmd_clear_financial_facts_clears_normalization_state(tmp_path):
    db_path = tmp_path / "clearfacts.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAA.US",
        [
            make_fact(
                symbol="AAA.US",
                concept="Assets",
                end_date="2024-12-31",
                unit="USD",
                value=10.0,
            )
        ],
        source_provider="SEC",
    )
    state_repo = FundamentalsNormalizationStateRepository(db_path)
    security_id = fact_repo._security_repo().ensure_from_symbol("AAA.US").security_id
    state_repo.mark_success(
        "SEC",
        "AAA.US",
        security_id=security_id,
        raw_fetched_at="2024-01-01T00:00:00+00:00",
    )

    rc = cli.cmd_clear_financial_facts(str(db_path))
    assert rc == 0

    with fact_repo._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM financial_facts").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM fundamentals_normalization_state"
            ).fetchone()[0]
            == 0
        )


def test_cmd_normalize_fundamentals_sec(monkeypatch, tmp_path):
    db_path = tmp_path / "facts.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert("SEC", "AAPL.US", {"entityName": "Apple Inc", "facts": {}})

    class FakeNormalizer:
        def __init__(self):
            self.calls = []

        def normalize(self, payload, symbol, cik=None):
            self.calls.append((payload, symbol, cik))

            return [
                make_fact(
                    symbol=symbol,
                    cik=cik,
                    concept="NetIncomeLoss",
                    end_date="2023-09-30",
                    value=123.0,
                )
            ]

    fake_normalizer = FakeNormalizer()
    monkeypatch.setattr(cli, "SECFactsNormalizer", lambda: fake_normalizer)

    rc = cli.cmd_normalize_fundamentals(
        provider="SEC",
        symbol="AAPL",
        database=str(db_path),
        exchange_code="US",
    )
    assert rc == 0

    result_repo = FinancialFactsRepository(db_path)
    result_repo.initialize_schema()
    rows = (
        result_repo._connect()
        .execute(
            """
            SELECT ff.concept, ff.value, ff.source_provider
            FROM financial_facts ff
            JOIN securities s ON s.security_id = ff.security_id
            WHERE s.canonical_symbol = 'AAPL.US'
            """
        )
        .fetchall()
    )
    assert [(row[0], row[1], row[2]) for row in rows] == [
        ("NetIncomeLoss", 123.0, "SEC")
    ]
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch("AAPL.US") == "Apple Inc"
    state_repo = FundamentalsNormalizationStateRepository(db_path)
    state = state_repo.fetch("SEC", "AAPL.US")
    assert state is not None
    assert state["raw_fetched_at"] is not None
    assert state["last_normalized_at"] is not None


def test_cmd_normalize_fundamentals_sec_skips_when_up_to_date(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "facts-skip.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert("SEC", "AAPL.US", {"entityName": "Apple Inc", "facts": {}})
    calls = []

    class FakeNormalizer:
        def normalize(self, payload, symbol, cik=None):
            calls.append(symbol)
            return [
                make_fact(
                    symbol=symbol,
                    cik=cik,
                    concept="NetIncomeLoss",
                    end_date="2023-09-30",
                    value=123.0,
                )
            ]

    monkeypatch.setattr(cli, "SECFactsNormalizer", lambda: FakeNormalizer())

    assert (
        cli.cmd_normalize_fundamentals(
            provider="SEC",
            symbol="AAPL",
            database=str(db_path),
            exchange_code="US",
        )
        == 0
    )
    assert (
        cli.cmd_normalize_fundamentals(
            provider="SEC",
            symbol="AAPL",
            database=str(db_path),
            exchange_code="US",
        )
        == 0
    )

    assert calls == ["AAPL.US"]
    assert "already up to date" in capsys.readouterr().out


def test_cmd_normalize_fundamentals_sec_force_reprocesses_up_to_date_symbol(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "facts-force.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert("SEC", "AAPL.US", {"entityName": "Apple Inc", "facts": {}})
    calls = []

    class FakeNormalizer:
        def normalize(self, payload, symbol, cik=None):
            calls.append(symbol)
            return [
                make_fact(
                    symbol=symbol,
                    cik=cik,
                    concept="NetIncomeLoss",
                    end_date="2023-09-30",
                    value=float(len(calls)),
                )
            ]

    monkeypatch.setattr(cli, "SECFactsNormalizer", lambda: FakeNormalizer())

    assert (
        cli.cmd_normalize_fundamentals(
            provider="SEC",
            symbol="AAPL",
            database=str(db_path),
            exchange_code="US",
        )
        == 0
    )
    assert (
        cli.cmd_normalize_fundamentals(
            provider="SEC",
            symbol="AAPL",
            database=str(db_path),
            exchange_code="US",
            force=True,
        )
        == 0
    )

    assert calls == ["AAPL.US", "AAPL.US"]


def test_cmd_normalize_fundamentals_bulk_sec(monkeypatch, tmp_path):
    db_path = tmp_path / "facts.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Corp", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Corp", exchange="NYSE"),
        ],
        provider="SEC",
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert("SEC", "AAA.US", {"entityName": "AAA Corp", "facts": {}})
    fund_repo.upsert("SEC", "BBB.US", {"entityName": "BBB Corp", "facts": {}})

    class DummyNormalizer:
        def normalize(self, payload, symbol, cik=None):
            return [
                make_fact(
                    symbol=symbol,
                    cik=cik,
                    concept="Dummy",
                    end_date="2023-12-31",
                    value=len(symbol),
                )
            ]

    normalization_repo = FinancialFactsRepository(db_path)
    normalization_repo.initialize_schema()
    monkeypatch.setattr(cli, "_normalization_worker_count", lambda total: 1)

    normalizer = DummyNormalizer()
    monkeypatch.setattr(cli, "SECFactsNormalizer", lambda: normalizer)

    rc = cli.cmd_normalize_fundamentals_bulk(
        provider="SEC",
        database=str(db_path),
        exchange_code="US",
    )
    assert rc == 0
    cursor = normalization_repo._connect().execute(
        """
        SELECT s.canonical_symbol, ff.value
        FROM financial_facts ff
        JOIN securities s ON s.security_id = ff.security_id
        ORDER BY s.canonical_symbol
        """
    )
    facts = [(row[0], row[1]) for row in cursor.fetchall()]
    assert facts == [("AAA.US", 6.0), ("BBB.US", 6.0)]
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch("AAA.US") == "AAA Corp"
    assert entity_repo.fetch("BBB.US") == "BBB Corp"


def test_cmd_normalize_fundamentals_bulk_sec_reprocesses_only_stale_symbols(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "facts-bulk-stale.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Corp", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Corp", exchange="NYSE"),
            Listing(symbol="CCC.US", security_name="CCC Corp", exchange="NYSE"),
        ],
        provider="SEC",
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US", "CCC.US"):
        fund_repo.upsert("SEC", symbol, {"entityName": symbol, "facts": {}})

    calls = []

    class DummyNormalizer:
        def normalize(self, payload, symbol, cik=None):
            calls.append(symbol)
            return [
                make_fact(
                    symbol=symbol,
                    cik=cik,
                    concept="Dummy",
                    end_date="2023-12-31",
                    value=len(symbol),
                )
            ]

    monkeypatch.setattr(cli, "_normalization_worker_count", lambda total: 1)
    monkeypatch.setattr(cli, "SECFactsNormalizer", lambda: DummyNormalizer())

    assert (
        cli.cmd_normalize_fundamentals_bulk(
            provider="SEC",
            database=str(db_path),
            exchange_code="US",
        )
        == 0
    )
    assert calls == ["AAA.US", "BBB.US", "CCC.US"]

    assert (
        cli.cmd_normalize_fundamentals_bulk(
            provider="SEC",
            database=str(db_path),
            exchange_code="US",
        )
        == 0
    )
    assert calls == ["AAA.US", "BBB.US", "CCC.US"]

    fund_repo.upsert("SEC", "BBB.US", {"entityName": "BBB.US", "facts": {}})

    assert (
        cli.cmd_normalize_fundamentals_bulk(
            provider="SEC",
            database=str(db_path),
            exchange_code="US",
        )
        == 0
    )
    assert calls == ["AAA.US", "BBB.US", "CCC.US", "BBB.US"]
    output = capsys.readouterr().out
    assert "already up to date" in output
    assert "skipped=2" in output


def test_cmd_normalize_fundamentals_bulk_with_exchange(monkeypatch, tmp_path):
    db_path = tmp_path / "fundamentals_exchange.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="US"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="US"),
            Listing(symbol="CCC.US", security_name="CCC Inc", exchange="US"),
        ],
        provider="SEC",
    )
    store_catalog_listings(
        db_path,
        "LSE",
        [Listing(symbol="DDD.LSE", security_name="DDD PLC", exchange="LSE")],
        provider="EODHD",
    )

    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert("SEC", "AAA.US", {"entityName": "AAA Corp", "facts": {}})
    fund_repo.upsert("SEC", "BBB.US", {"entityName": "BBB Corp", "facts": {}})

    class DummyNormalizer:
        def normalize(self, payload, symbol, cik=None):
            return [
                make_fact(
                    symbol=symbol, concept="Dummy", end_date="2023-12-31", value=1.0
                )
            ]

    monkeypatch.setattr(cli, "SECFactsNormalizer", lambda: DummyNormalizer())

    rc = cli.cmd_normalize_fundamentals_bulk(
        provider="SEC",
        database=str(db_path),
        exchange_code="US",
    )
    assert rc == 0

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    rows = (
        fact_repo._connect()
        .execute(
            """
            SELECT s.canonical_symbol
            FROM financial_facts ff
            JOIN securities s ON s.security_id = ff.security_id
            ORDER BY s.canonical_symbol
            """
        )
        .fetchall()
    )
    assert [row[0] for row in rows] == ["AAA.US", "BBB.US"]


def test_cmd_normalize_fundamentals_eodhd(monkeypatch, tmp_path):
    db_path = tmp_path / "funds.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD",
        "SHEL.LSE",
        {
            "General": {
                "Name": "Shell PLC",
                "Sector": "Energy",
                "Industry": "Oil & Gas Integrated",
            },
            "Financials": {},
        },
    )

    class FakeNormalizer:
        def normalize(self, payload, symbol, accounting_standard=None):
            return [
                make_fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    end_date="2023-12-31",
                    value=10.0,
                )
            ]

    monkeypatch.setattr(cli, "EODHDFactsNormalizer", lambda: FakeNormalizer())

    rc = cli.cmd_normalize_fundamentals(
        provider="EODHD",
        symbol="SHEL.LSE",
        database=str(db_path),
        exchange_code=None,
    )
    assert rc == 0

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    rows = (
        fact_repo._connect()
        .execute(
            """
            SELECT ff.concept, ff.value, ff.source_provider
            FROM financial_facts ff
            JOIN securities s ON s.security_id = ff.security_id
            WHERE s.canonical_symbol = 'SHEL.LSE'
            """
        )
        .fetchall()
    )
    assert [(row[0], row[1], row[2]) for row in rows] == [
        ("NetIncomeLoss", 10.0, "EODHD")
    ]
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch("SHEL.LSE") == "Shell PLC"
    assert entity_repo.fetch_sector("SHEL.LSE") == "Energy"
    assert entity_repo.fetch_industry("SHEL.LSE") == "Oil & Gas Integrated"


def test_cmd_normalize_fundamentals_eodhd_zero_row_normalization_records_state(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "funds-zero.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD",
        "SHEL.LSE",
        {"General": {"Name": "Shell PLC"}, "Financials": {}},
    )
    calls = []

    class FakeNormalizer:
        def normalize(self, payload, symbol, accounting_standard=None):
            calls.append(symbol)
            return []

    monkeypatch.setattr(cli, "EODHDFactsNormalizer", lambda: FakeNormalizer())

    assert (
        cli.cmd_normalize_fundamentals(
            provider="EODHD",
            symbol="SHEL.LSE",
            database=str(db_path),
            exchange_code=None,
        )
        == 0
    )
    assert (
        cli.cmd_normalize_fundamentals(
            provider="EODHD",
            symbol="SHEL.LSE",
            database=str(db_path),
            exchange_code=None,
        )
        == 0
    )

    state_repo = FundamentalsNormalizationStateRepository(db_path)
    assert state_repo.fetch("EODHD", "SHEL.LSE") is not None
    assert calls == ["SHEL.LSE"]
    assert "already up to date" in capsys.readouterr().out


def test_cmd_normalize_fundamentals_cross_provider_reruns_when_facts_owned_by_other_provider(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "funds-cross-provider.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD",
        "AAA.US",
        {"General": {"Name": "AAA"}, "Financials": {}},
        exchange="US",
    )
    fund_repo.upsert("SEC", "AAA.US", {"entityName": "AAA", "facts": {}})
    eodhd_calls = []
    sec_calls = []

    class FakeEODHDNormalizer:
        def normalize(self, payload, symbol, accounting_standard=None):
            eodhd_calls.append(symbol)
            return [
                make_fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    end_date="2023-12-31",
                    value=10.0,
                )
            ]

    class FakeSECNormalizer:
        def normalize(self, payload, symbol, cik=None):
            sec_calls.append(symbol)
            return [
                make_fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    end_date="2024-12-31",
                    value=20.0,
                )
            ]

    monkeypatch.setattr(cli, "EODHDFactsNormalizer", lambda: FakeEODHDNormalizer())
    monkeypatch.setattr(cli, "SECFactsNormalizer", lambda: FakeSECNormalizer())

    assert (
        cli.cmd_normalize_fundamentals(
            provider="EODHD",
            symbol="AAA.US",
            database=str(db_path),
            exchange_code=None,
        )
        == 0
    )
    assert (
        cli.cmd_normalize_fundamentals(
            provider="SEC",
            symbol="AAA.US",
            database=str(db_path),
            exchange_code="US",
        )
        == 0
    )
    assert (
        cli.cmd_normalize_fundamentals(
            provider="EODHD",
            symbol="AAA.US",
            database=str(db_path),
            exchange_code=None,
        )
        == 0
    )

    assert eodhd_calls == ["AAA.US", "AAA.US"]
    assert sec_calls == ["AAA.US"]


def test_cmd_normalize_fundamentals_stage_all_supported_filters_to_raw_symbols(
    monkeypatch, tmp_path
):
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
    fund_repo.upsert(
        "EODHD",
        "AAA.US",
        {"General": {"Name": "AAA"}, "Financials": {}},
        exchange="US",
    )
    fund_repo.upsert(
        "EODHD",
        "CCC.LSE",
        {"General": {"Name": "CCC"}, "Financials": {}},
        exchange="LSE",
    )
    captured = {}

    def fake_bulk(database, symbols=None, force=False):
        captured["database"] = database
        captured["symbols"] = list(symbols or [])
        captured["force"] = force
        return 0

    monkeypatch.setattr(cli, "cmd_normalize_eodhd_fundamentals_bulk", fake_bulk)

    rc = cli.cmd_normalize_fundamentals_stage(
        provider="EODHD",
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
    )

    assert rc == 0
    assert captured["database"] == str(db_path)
    assert sorted(captured["symbols"]) == ["AAA.US", "CCC.LSE"]
    assert captured["force"] is False


def test_cmd_normalize_eodhd_fundamentals_bulk_reports_freshness_scan(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "normalize-eodhd-status.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US"):
        fund_repo.upsert(
            "EODHD",
            symbol,
            {"General": {"Name": symbol}, "Financials": {}},
            exchange="US",
        )

    monkeypatch.setattr(
        cli,
        "_plan_normalization_selection",
        lambda **kwargs: ([], {}, len(kwargs["symbols"])),
    )

    rc = cli.cmd_normalize_eodhd_fundamentals_bulk(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
    )

    assert rc == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert output_lines[0] == "Checking EODHD normalization freshness for 2 symbols"
    assert "already up to date" in output_lines[-1]


def test_cmd_normalize_eodhd_fundamentals_bulk_continues_after_symbol_failure_with_inline_executor(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "normalize-eodhd-failure.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US", "CCC.US"):
        fund_repo.upsert(
            "EODHD",
            symbol,
            {"General": {"Name": symbol}, "Financials": {}},
            exchange="US",
        )

    class FakeNormalizer:
        def normalize(self, payload, symbol, accounting_standard=None):
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

    monkeypatch.setattr(cli, "EODHDFactsNormalizer", FakeNormalizer)
    monkeypatch.setattr(cli, "_normalization_worker_count", lambda total: 3)

    class InlineExecutor:
        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait=True, cancel_futures=False):
            return None

    monkeypatch.setattr(
        cli,
        "_create_normalization_executor",
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
            JOIN securities s ON s.security_id = ff.security_id
            ORDER BY s.canonical_symbol
            """
        )
        .fetchall()
    )
    assert [row[0] for row in rows] == ["AAA.US", "CCC.US"]


def test_cmd_normalize_eodhd_fundamentals_bulk_interrupts_cleanly(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "normalize-eodhd-interrupt.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US"):
        fund_repo.upsert(
            "EODHD",
            symbol,
            {"General": {"Name": symbol}, "Financials": {}},
            exchange="US",
        )

    class FakeNormalizer:
        def normalize(self, payload, symbol, accounting_standard=None):
            return [
                make_fact(
                    symbol=symbol,
                    concept="Dummy",
                    end_date="2023-12-31",
                    value=1.0,
                )
            ]

    class InlineExecutor:
        def __init__(self):
            self.shutdown_calls = []

        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait=True, cancel_futures=False):
            self.shutdown_calls.append((wait, cancel_futures))

    def interrupting_as_completed(futures):
        yielded = False
        for future in futures:
            if not yielded:
                yielded = True
                yield future
                raise KeyboardInterrupt

    executor = InlineExecutor()
    monkeypatch.setattr(cli, "EODHDFactsNormalizer", FakeNormalizer)
    monkeypatch.setattr(cli, "_normalization_worker_count", lambda total: 2)
    monkeypatch.setattr(
        cli,
        "_create_normalization_executor",
        lambda max_workers: executor,
    )
    monkeypatch.setattr(cli, "as_completed", interrupting_as_completed)

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
            JOIN securities s ON s.security_id = ff.security_id
            ORDER BY s.canonical_symbol
            """
        )
        .fetchall()
    )
    assert [row[0] for row in rows] == ["AAA.US"]


def test_cmd_normalize_sec_facts_bulk_with_inline_executor(monkeypatch, tmp_path):
    db_path = tmp_path / "normalize-sec-inline.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US"):
        fund_repo.upsert("SEC", symbol, {"entityName": symbol, "facts": {}})

    class FakeNormalizer:
        def normalize(self, payload, symbol, cik=None):
            return [
                make_fact(
                    symbol=symbol,
                    cik=cik,
                    concept="Dummy",
                    end_date="2023-12-31",
                    value=len(symbol),
                )
            ]

    class InlineExecutor:
        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:
                future.set_exception(exc)
            return future

        def shutdown(self, wait=True, cancel_futures=False):
            return None

    monkeypatch.setattr(cli, "SECFactsNormalizer", FakeNormalizer)
    monkeypatch.setattr(cli, "_normalization_worker_count", lambda total: 2)
    monkeypatch.setattr(
        cli,
        "_create_normalization_executor",
        lambda max_workers: InlineExecutor(),
    )

    rc = cli.cmd_normalize_us_facts_bulk(
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
            SELECT s.canonical_symbol, ff.value
            FROM financial_facts ff
            JOIN securities s ON s.security_id = ff.security_id
            ORDER BY s.canonical_symbol
            """
        )
        .fetchall()
    )
    assert [(row[0], row[1]) for row in rows] == [
        ("AAA.US", 6.0),
        ("BBB.US", 6.0),
    ]


def test_cmd_normalize_eodhd_fundamentals_bulk_process_pool_smoke(
    monkeypatch, tmp_path
):
    if cli.os.name != "posix":
        pytest.skip("requires POSIX fork support")

    db_path = tmp_path / "normalize-eodhd-process.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    for symbol in ("AAA.US", "BBB.US"):
        fund_repo.upsert(
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

    monkeypatch.setattr(cli, "_normalization_worker_count", lambda total: 2)

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
            JOIN securities s ON s.security_id = ff.security_id
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


def test_cmd_normalize_sec_facts_bulk_process_pool_smoke(monkeypatch, tmp_path):
    if cli.os.name != "posix":
        pytest.skip("requires POSIX fork support")

    db_path = tmp_path / "normalize-sec-process.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    payload = {
        "entityName": "Test Corp",
        "facts": {
            "us-gaap": {
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            {
                                "val": 123.0,
                                "fy": 2024,
                                "fp": "FY",
                                "end": "2024-12-31",
                                "filed": "2025-02-01",
                                "form": "10-K",
                            }
                        ]
                    }
                }
            }
        },
    }
    for symbol in ("AAA.US", "BBB.US"):
        fund_repo.upsert("SEC", symbol, payload)

    monkeypatch.setattr(cli, "_normalization_worker_count", lambda total: 2)

    rc = cli.cmd_normalize_us_facts_bulk(
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
            JOIN securities s ON s.security_id = ff.security_id
            WHERE ff.concept = 'NetIncomeLoss'
            ORDER BY s.canonical_symbol
            """
        )
        .fetchall()
    )
    assert [(row[0], row[1], row[2]) for row in rows] == [
        ("AAA.US", "NetIncomeLoss", 123.0),
        ("BBB.US", "NetIncomeLoss", 123.0),
    ]


def test_cmd_recalc_market_cap(tmp_path):
    db_path = tmp_path / "marketcap.db"
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
    fact_repo.replace_facts(
        "AAA.US",
        [
            make_fact(
                concept="CommonStockSharesOutstanding",
                end_date="2023-12-31",
                value=100,
                symbol="AAA.US",
            )
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAA.US", "2023-12-31", price=40.0, market_cap=4000.0)
    market_repo.upsert_price("AAA.US", "2024-01-01", price=50.0)
    market_repo.upsert_price("BBB.US", "2024-01-01", price=70.0)

    rc = cli.cmd_recalc_market_cap(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
    )
    assert rc == 0
    snapshot = market_repo.latest_snapshot("AAA.US")
    assert snapshot.market_cap == 5000.0
    with market_repo._connect() as conn:
        historical_cap = conn.execute(
            """
            SELECT market_cap
            FROM market_data md
            JOIN securities s ON s.security_id = md.security_id
            WHERE s.canonical_symbol = 'AAA.US' AND md.as_of = '2023-12-31'
            """
        ).fetchone()[0]
    assert historical_cap == 4000.0
    snapshot_b = market_repo.latest_snapshot("BBB.US")
    assert snapshot_b.market_cap is None


def test_cmd_recalc_market_cap_prints_status_before_market_data_scan(
    monkeypatch, tmp_path, capsys
):
    db_path = tmp_path / "marketcap-status.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )

    def fake_latest_snapshots_many(self, symbols, chunk_size=500):
        output = capsys.readouterr().out
        assert "Preparing market cap recalculation for US (selected=1)" in output
        assert "Loading latest market data for 1 symbols" in output
        return {}

    monkeypatch.setattr(
        MarketDataRepository,
        "latest_snapshots_many",
        fake_latest_snapshots_many,
    )

    rc = cli.cmd_recalc_market_cap(
        database=str(db_path),
        symbols=None,
        exchange_codes=["US"],
        all_supported=False,
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert "No market data found to update for US." in output


def test_cmd_recalc_market_cap_symbol_scope(tmp_path):
    db_path = tmp_path / "marketcap-symbol-scope.db"
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
    fact_repo.replace_facts(
        "AAA.US",
        [
            make_fact(
                concept="CommonStockSharesOutstanding",
                end_date="2023-12-31",
                value=100,
                symbol="AAA.US",
            )
        ],
    )
    fact_repo.replace_facts(
        "BBB.US",
        [
            make_fact(
                concept="CommonStockSharesOutstanding",
                end_date="2023-12-31",
                value=200,
                symbol="BBB.US",
            )
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAA.US", "2024-01-01", price=50.0)
    market_repo.upsert_price("BBB.US", "2024-01-01", price=70.0)

    rc = cli.cmd_recalc_market_cap(
        database=str(db_path),
        symbols=["BBB.US"],
        exchange_codes=None,
        all_supported=False,
    )

    assert rc == 0
    assert market_repo.latest_snapshot("AAA.US").market_cap is None
    assert market_repo.latest_snapshot("BBB.US").market_cap == 14000.0


def test_cmd_recalc_market_cap_all_supported_scope(tmp_path):
    db_path = tmp_path / "marketcap-all-supported.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    store_catalog_listings(
        db_path,
        "LSE",
        [Listing(symbol="SHEL.LSE", security_name="Shell PLC", exchange="LSE")],
        provider="EODHD",
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAA.US",
        [
            make_fact(
                concept="CommonStockSharesOutstanding",
                end_date="2023-12-31",
                value=100,
                symbol="AAA.US",
            )
        ],
    )
    fact_repo.replace_facts(
        "SHEL.LSE",
        [
            make_fact(
                concept="CommonStockSharesOutstanding",
                end_date="2023-12-31",
                value=50,
                symbol="SHEL.LSE",
            )
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAA.US", "2024-01-01", price=50.0)
    market_repo.upsert_price("SHEL.LSE", "2024-01-01", price=25.0)

    rc = cli.cmd_recalc_market_cap(
        database=str(db_path),
        symbols=None,
        exchange_codes=None,
        all_supported=True,
    )

    assert rc == 0
    assert market_repo.latest_snapshot("AAA.US").market_cap == 5000.0
    assert market_repo.latest_snapshot("SHEL.LSE").market_cap == 1250.0


def test_cmd_recalc_market_cap_reports_loaded_share_counts(tmp_path, capsys):
    db_path = tmp_path / "marketcap-share-count-status.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAA.US",
        [
            make_fact(
                concept="CommonStockSharesOutstanding",
                end_date="2023-12-31",
                value=100,
                symbol="AAA.US",
            )
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAA.US", "2024-01-01", price=50.0)

    rc = cli.cmd_recalc_market_cap(
        database=str(db_path),
        symbols=["AAA.US"],
        exchange_codes=None,
        all_supported=False,
    )

    assert rc == 0
    output = capsys.readouterr().out
    assert "Loaded latest share counts for 1 symbols" in output


def test_cmd_recalc_market_cap_interrupts_cleanly(monkeypatch, tmp_path, capsys):
    db_path = tmp_path / "recalc-market-cap-interrupt.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAA.US", "2024-01-01", 10.0, market_cap=100.0)

    def interrupting_latest_share_counts_many(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(
        FinancialFactsRepository,
        "latest_share_counts_many",
        interrupting_latest_share_counts_many,
    )

    rc = cli.cmd_recalc_market_cap(
        database=str(db_path),
        symbols=["AAA.US"],
        exchange_codes=None,
        all_supported=False,
    )

    assert rc == 1
    output = capsys.readouterr().out
    assert "Market cap recalculation cancelled by user." in output
    assert "Updated market cap for" not in output


def test_cmd_refresh_security_metadata_backfills_eodhd_fields_and_sec_name_fallback(
    tmp_path, capsys
):
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
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
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
    fund_repo.upsert("SEC", "BBB.US", {"entityName": "BBB SEC Name", "facts": {}})

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAA.US",
        [
            make_fact(
                symbol="AAA.US", concept="Assets", end_date="2024-12-31", value=1.0
            )
        ],
        source_provider="EODHD",
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
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch("AAA.US") == "AAA Holdings"
    assert entity_repo.fetch_description("AAA.US") == "AAA business"
    assert entity_repo.fetch_sector("AAA.US") == "Technology"
    assert entity_repo.fetch_industry("AAA.US") == "Software"
    assert entity_repo.fetch("BBB.US") == "BBB SEC Name"
    assert entity_repo.fetch_sector("BBB.US") is None
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


def test_cmd_refresh_security_metadata_respects_symbol_scope(tmp_path, capsys):
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
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD",
        "AAA.US",
        {"General": {"Sector": "Technology", "Industry": "Software"}},
        exchange="US",
    )
    fund_repo.upsert(
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
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch_sector("AAA.US") == "Technology"
    assert entity_repo.fetch_sector("BBB.US") is None
    assert capsys.readouterr().out.splitlines() == [
        "Progress: 1/1 symbols complete (100.0%)",
        "Scanned 1 symbols.",
        "Updated metadata for 1 symbols.",
        "Skipped with no raw payload: 0",
        "Skipped with no extractable metadata: 0",
        "No metadata changes needed: 0",
    ]


def test_cmd_refresh_security_metadata_does_not_use_full_payload_fetch(
    tmp_path, capsys, monkeypatch
):
    db_path = tmp_path / "refresh-security-metadata-no-fetch-many.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD",
        "AAA.US",
        {"General": {"Sector": "Technology", "Industry": "Software"}},
        exchange="US",
    )

    monkeypatch.setattr(
        FundamentalsRepository,
        "fetch_many",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("refresh-security-metadata should not call fetch_many")
        ),
    )

    rc = cli.cmd_refresh_security_metadata(
        database=str(db_path),
        symbols=["AAA.US"],
        exchange_codes=None,
        all_supported=False,
    )

    assert rc == 0
    assert "Updated metadata for 1 symbols." in capsys.readouterr().out


def test_cmd_refresh_security_metadata_reports_progress(tmp_path, capsys, monkeypatch):
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
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD",
        "AAA.US",
        {"General": {"Sector": "Technology", "Industry": "Software"}},
        exchange="US",
    )
    fund_repo.upsert(
        "EODHD",
        "BBB.US",
        {"General": {"Sector": "Industrials", "Industry": "Machinery"}},
        exchange="US",
    )
    monkeypatch.setattr(cli, "SECURITY_METADATA_PROGRESS_INTERVAL_SECONDS", 0.0)

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


def test_cmd_refresh_security_metadata_cancels_cleanly(tmp_path, capsys, monkeypatch):
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
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD",
        "AAA.US",
        {"General": {"Sector": "Technology", "Industry": "Software"}},
        exchange="US",
    )
    fund_repo.upsert(
        "EODHD",
        "BBB.US",
        {"General": {"Sector": "Industrials", "Industry": "Machinery"}},
        exchange="US",
    )

    call_count = 0

    real_fetch_metadata_candidates = FundamentalsRepository.fetch_metadata_candidates

    def interrupting_fetch_metadata_candidates(self, security_ids, chunk_size=500):
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
    monkeypatch.setattr(cli, "SECURITY_METADATA_PROGRESS_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(cli, "SECURITY_METADATA_CHUNK_SIZE", 1)

    rc = cli.cmd_refresh_security_metadata(
        database=str(db_path),
        symbols=["AAA.US", "BBB.US"],
        exchange_codes=None,
        all_supported=False,
    )

    assert rc == 1
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch_sector("AAA.US") == "Technology"
    assert entity_repo.fetch_sector("BBB.US") is None
    output_lines = capsys.readouterr().out.splitlines()
    assert (
        "Security metadata refresh cancelled by user after 1 of 2 symbols."
        in output_lines
    )
    assert [line for line in output_lines if line.startswith("Progress:")] == [
        "Progress: 1/2 symbols complete (50.0%)",
    ]
    assert "Scanned 2 symbols." not in output_lines


def test_cmd_run_screen_bulk(tmp_path, capsys):
    db_path = tmp_path / "screen.db"
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
    metrics_repo.upsert("AAA.US", "working_capital", 100.0, "2023-12-31")
    metrics_repo.upsert("BBB.US", "working_capital", 50.0, "2023-12-31")
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    entity_repo.upsert("AAA.US", "AAA Inc", description="AAA description")
    entity_repo.upsert("BBB.US", "BBB Inc", description="BBB description")

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

    csv_path = tmp_path / "results.csv"

    rc = cli.cmd_run_screen_bulk(
        config_path=str(screen_path),
        provider="SEC",
        database=str(db_path),
        output_csv=str(csv_path),
        exchange_code="US",
    )
    assert rc == 0
    output = capsys.readouterr().out
    assert "AAA.US" in output
    assert "BBB.US" not in output
    csv_contents = csv_path.read_text().strip().splitlines()
    assert csv_contents[0] == "Criterion,AAA.US"
    assert csv_contents[1].startswith("Entity,AAA Inc")
    assert csv_contents[2].startswith("Description,AAA description")
    assert csv_contents[3] == "Price,N/A"


def test_cmd_run_screen_bulk_with_exchange(tmp_path, capsys):
    db_path = tmp_path / "screen_exchange.db"
    store_catalog_listings(
        db_path,
        "LSE",
        [Listing(symbol="AAA.LSE", security_name="AAA PLC", exchange="LSE")],
        provider="EODHD",
    )
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE")],
        provider="SEC",
    )

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    metrics_repo.upsert("AAA.LSE", "working_capital", 100.0, "2023-12-31")
    metrics_repo.upsert("BBB.US", "working_capital", 100.0, "2023-12-31")

    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    entity_repo.upsert("AAA.LSE", "AAA PLC", description="AAA description")
    entity_repo.upsert("BBB.US", "BBB Inc", description="BBB description")

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

    csv_path = tmp_path / "results.csv"

    rc = cli.cmd_run_screen_bulk(
        config_path=str(screen_path),
        provider="EODHD",
        database=str(db_path),
        output_csv=str(csv_path),
        exchange_code="LSE",
    )
    assert rc == 0
    output = capsys.readouterr().out
    assert "AAA.LSE" in output
    assert "BBB.US" not in output
    csv_contents = csv_path.read_text().strip().splitlines()
    assert csv_contents[0] == "Criterion,AAA.LSE"
    assert csv_contents[1].startswith("Entity,AAA PLC")
    assert csv_contents[2].startswith("Description,AAA description")
    assert csv_contents[3] == "Price,N/A"


def test_cmd_run_screen_stage_reports_progress_for_multi_symbol_scope(
    monkeypatch, tmp_path, capsys
):
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
    metrics_repo.upsert("AAA.US", "working_capital", 100.0, "2023-12-31")
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    entity_repo.upsert("AAA.US", "AAA Inc", description="AAA description")
    entity_repo.upsert("BBB.US", "BBB Inc", description="BBB description")

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

    monkeypatch.setattr(cli, "SCREEN_PROGRESS_INTERVAL_SECONDS", 0.0)

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
    tmp_path, capsys
):
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
    metrics_repo.upsert("AAA.US", "working_capital", 100.0, "2023-12-31")
    metrics_repo.upsert("BBB.US", "working_capital", 50.0, "2023-12-31")
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    entity_repo.upsert("AAA.US", "AAA Inc", description="AAA description")
    entity_repo.upsert("BBB.US", "BBB Inc", description="BBB description")

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
    assert csv_contents[0] == "Criterion,AAA.US"
    assert csv_contents[1].startswith("Entity,AAA Inc")
    assert csv_contents[2].startswith("Description,AAA description")
    assert csv_contents[3] == "Price,N/A"
    assert "AAA.US" in capsys.readouterr().out


def test_cmd_run_screen_stage_adds_ranked_output_rows_and_sorts_passers(
    tmp_path, capsys
):
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
        metrics_repo.upsert(symbol, "working_capital", 100.0, "2023-12-31")
    metrics_repo.upsert("AAA.US", "primary_score", 10.0, "2023-12-31")
    metrics_repo.upsert("BBB.US", "primary_score", 10.0, "2023-12-31")
    metrics_repo.upsert("CCC.US", "primary_score", 5.0, "2023-12-31")
    metrics_repo.upsert("AAA.US", "oey_ev_norm", 0.05, "2023-12-31")
    metrics_repo.upsert("BBB.US", "oey_ev_norm", 0.07, "2023-12-31")
    metrics_repo.upsert("CCC.US", "oey_ev_norm", 0.09, "2023-12-31")
    metrics_repo.upsert("AAA.US", "net_debt_to_ebitda", 1.5, "2023-12-31")
    metrics_repo.upsert("BBB.US", "net_debt_to_ebitda", 1.5, "2023-12-31")
    metrics_repo.upsert("CCC.US", "net_debt_to_ebitda", 0.5, "2023-12-31")
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    entity_repo.upsert("AAA.US", "AAA Inc", description="AAA description")
    entity_repo.upsert("BBB.US", "BBB Inc", description="BBB description")
    entity_repo.upsert("CCC.US", "CCC Inc", description="CCC description")

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
    assert csv_contents[0] == "Criterion,BBB.US,AAA.US,CCC.US"
    assert csv_contents[5] == "qarp_rank,1,2,3"
    assert csv_contents[6].startswith("qarp_score,66.6667,66.6667,16.6667")
    output = capsys.readouterr().out.splitlines()
    assert any(
        line.lstrip().startswith("Criterion") and "BBB.US" in line for line in output
    )


def test_cmd_run_screen_stage_reports_progress_when_no_symbols_pass(
    monkeypatch, tmp_path, capsys
):
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
    metrics_repo.upsert("AAA.US", "working_capital", 50.0, "2023-12-31")
    metrics_repo.upsert("BBB.US", "working_capital", 60.0, "2023-12-31")

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

    monkeypatch.setattr(cli, "SCREEN_PROGRESS_INTERVAL_SECONDS", 0.0)

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


def test_cmd_run_screen_stage_creates_output_csv_parent_dirs_when_no_symbols_pass(
    tmp_path, capsys
):
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
    metrics_repo.upsert("AAA.US", "working_capital", 50.0, "2023-12-31")
    metrics_repo.upsert("BBB.US", "working_capital", 60.0, "2023-12-31")

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
    assert csv_contents[0] == "Criterion"
    assert csv_contents[1] == "Entity"
    assert csv_contents[2] == "Description"
    assert csv_contents[3] == "Price"
    assert "No symbols satisfied all criteria." in capsys.readouterr().out


def test_cmd_report_metric_failures_uses_highest_market_cap_example(tmp_path, capsys):
    db_path = tmp_path / "failures.db"
    store_catalog_listings(
        db_path,
        "US",
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ],
        provider="SEC",
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAA.US", "2024-01-01", price=10.0, market_cap=100.0)
    market_repo.upsert_price("BBB.US", "2024-01-01", price=10.0, market_cap=200.0)

    rc = cli.cmd_report_metric_failures(
        database=str(db_path),
        metric_ids=["working_capital"],
        symbols=["AAA.US", "BBB.US"],
        exchange_codes=None,
        all_supported=False,
        output_csv=None,
    )
    assert rc == 0
    output = capsys.readouterr().out
    assert "working_capital" in output
    assert "example=BBB.US" in output


def test_cmd_report_metric_failures_with_exchange(tmp_path, capsys):
    db_path = tmp_path / "failures_exchange.db"
    store_catalog_listings(
        db_path,
        "LSE",
        [Listing(symbol="AAA.LSE", security_name="AAA PLC", exchange="LSE")],
        provider="EODHD",
    )
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE")],
        provider="SEC",
    )

    rc = cli.cmd_report_metric_failures(
        database=str(db_path),
        metric_ids=["working_capital"],
        symbols=None,
        exchange_codes=["LSE"],
        all_supported=False,
        output_csv=None,
    )
    assert rc == 0
    output = capsys.readouterr().out
    assert "symbols=1" in output
    assert "example=AAA.LSE" in output
    assert "BBB.US" not in output


def test_cmd_report_screen_failures_dedupes_metric_na_counts(tmp_path, capsys):
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
    fact_repo.replace_facts(
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
    metrics_repo.upsert("AAA.US", "working_capital", 10.0, as_of)
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("BBB.US", as_of, price=10.0, market_cap=250.0)

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
    assert (
        "Progress: [--------------------] 0/1 missing symbols analyzed (0.0%)" in output
    )
    assert (
        "Progress: [####################] 1/1 missing symbols analyzed (100.0%)"
        in output
    )
    assert "Passed all criteria: 0/2" in output
    assert "Metric NA impact" in output
    assert "- working_capital: missing=1 symbols, affects=2 criteria" in output
    assert "stored_missing_but_computable_now: 1 (example=BBB.US" in output
    assert "Criterion fallout" in output
    assert "Working capital >= 20: fails=2/2, na_fails=1, threshold_fails=1" in output
    assert "Working capital >= 50: fails=2/2, na_fails=1, threshold_fails=1" in output
    assert "missing_metrics: working_capital=1" in output
    csv_lines = csv_path.read_text().strip().splitlines()
    assert csv_lines[0] == (
        "metric_id,missing_symbols,affected_criteria_count,"
        "affected_criteria,root_cause,root_cause_count,example_symbol,example_market_cap"
    )
    assert "working_capital,1,2," in csv_lines[1]
    assert "stored_missing_but_computable_now,1,BBB.US,250.0" in csv_lines[1]


def test_cmd_report_screen_failures_reports_progress_by_phase(
    tmp_path,
    monkeypatch,
    capsys,
):
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
    fact_repo.replace_facts(
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
    metrics_repo.upsert("AAA.US", "working_capital", 10.0, as_of)
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("BBB.US", as_of, price=10.0, market_cap=250.0)

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
    monkeypatch.setattr(cli, "SCREEN_PROGRESS_INTERVAL_SECONDS", 0.0)

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
    assert (
        "Progress: [--------------------] 0/1 missing symbols analyzed (0.0%)"
        in output_lines
    )
    assert (
        "Progress: [####################] 1/1 missing symbols analyzed (100.0%)"
        in output_lines
    )


def test_cmd_report_screen_failures_avoids_point_metric_fetches(
    tmp_path,
    monkeypatch,
    capsys,
):
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
    fact_repo.replace_facts(
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
    metrics_repo.upsert("AAA.US", "working_capital", 10.0, as_of)
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

    def fail_point_fetch(self, symbol, metric_id):
        raise AssertionError("point metric fetch should not be used")

    monkeypatch.setattr(MetricsRepository, "fetch", fail_point_fetch)

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


def test_cmd_report_screen_failures_recompute_uses_symbol_caches(
    tmp_path,
    monkeypatch,
    capsys,
):
    db_path = tmp_path / "screen_failures_symbol_cache.db"
    store_catalog_listings(
        db_path,
        "US",
        [Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE")],
        provider="SEC",
    )
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAA.US",
        [
            make_fact(concept="AssetsCurrent", end_date="2024-12-31", value=500.0),
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
    market_repo.upsert_price("AAA.US", "2024-12-31", price=25.0, market_cap=1000.0)

    fact_calls = {"count": 0}
    snapshot_batch_calls = {"count": 0}
    original_facts_for_symbol = FinancialFactsRepository.facts_for_symbol
    original_latest_snapshots_many = MarketDataRepository.latest_snapshots_many

    def counting_facts_for_symbol(self, symbol):
        fact_calls["count"] += 1
        return original_facts_for_symbol(self, symbol)

    def counting_latest_snapshots_many(self, symbols, chunk_size=500):
        snapshot_batch_calls["count"] += 1
        return original_latest_snapshots_many(self, symbols, chunk_size=chunk_size)

    def fail_latest_snapshot(self, symbol):
        raise AssertionError("expected report-screen-failures to use bulk snapshots")

    monkeypatch.setattr(
        FinancialFactsRepository,
        "facts_for_symbol",
        counting_facts_for_symbol,
    )
    monkeypatch.setattr(
        MarketDataRepository,
        "latest_snapshots_many",
        counting_latest_snapshots_many,
    )
    monkeypatch.setattr(MarketDataRepository, "latest_snapshot", fail_latest_snapshot)

    class RepeatedFactsMetric:
        id = "repeat_facts"
        required_concepts = ("AssetsCurrent",)
        uses_market_data = False

        def compute(self, symbol, repo):
            latest_a = repo.latest_fact(symbol, "AssetsCurrent")
            latest_b = repo.latest_fact(symbol, "AssetsCurrent")
            series_a = repo.facts_for_concept(symbol, "EarningsPerShare", "FY")
            series_b = repo.facts_for_concept(symbol, "EarningsPerShare", "FY")
            return MetricResult(
                symbol=symbol,
                metric_id=self.id,
                value=latest_a.value + latest_b.value + len(series_a) + len(series_b),
                as_of=latest_a.end_date,
            )

    class RepeatedMarketMetric:
        id = "repeat_market"
        required_concepts = ()
        uses_market_data = True

        def compute(self, symbol, repo, market_repo):
            snapshot_a = market_repo.latest_snapshot(symbol)
            snapshot_b = market_repo.latest_snapshot(symbol)
            latest_price = market_repo.latest_price(symbol)
            return MetricResult(
                symbol=symbol,
                metric_id=self.id,
                value=snapshot_a.price + snapshot_b.price + latest_price[1],
                as_of=snapshot_a.as_of,
            )

    monkeypatch.setattr(
        cli,
        "REGISTRY",
        {
            RepeatedFactsMetric.id: RepeatedFactsMetric,
            RepeatedMarketMetric.id: RepeatedMarketMetric,
        },
    )

    screen_path = tmp_path / "screen.yml"
    screen_path.write_text(
        """
criteria:
  - name: "Repeated facts > 0"
    left:
      metric: repeat_facts
    operator: ">"
    right:
      value: 0

  - name: "Repeated market > 0"
    left:
      metric: repeat_market
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
    assert "- repeat_facts: missing=1 symbols, affects=1 criteria" in output
    assert "- repeat_market: missing=1 symbols, affects=1 criteria" in output
    assert "stored_missing_but_computable_now: 1 (example=AAA.US" in output
    assert fact_calls["count"] == 1
    assert snapshot_batch_calls["count"] == 1


def test_cmd_report_screen_failures_suppresses_console_metric_warnings(
    tmp_path,
    monkeypatch,
    capsys,
):
    clear_root_logging_handlers()
    cli.setup_logging(log_dir=tmp_path / "logs")
    try:
        db_path = tmp_path / "screen_failures_warning_suppression.db"
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
  - name: "Noisy metric"
    left:
      metric: noisy_metric
    operator: ">"
    right:
      value: 0
"""
        )

        class NoisyMetric:
            id = "noisy_metric"
            required_concepts = ()

            def compute(self, symbol, repo):
                logging.getLogger("pyvalue.metrics.noisy").warning(
                    "Console-only warning for %s", symbol
                )
                return None

        monkeypatch.setitem(cli.REGISTRY, "noisy_metric", NoisyMetric)

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
        assert "WARNING Console-only warning for AAA.US" not in output
        assert "Console-only warning for <symbol>: 1" in output
        log_text = (tmp_path / "logs" / "pyvalue.log").read_text()
        assert "Console-only warning for AAA.US" in log_text
    finally:
        clear_root_logging_handlers()


def test_cmd_report_screen_failures_reports_metric_exceptions(
    tmp_path,
    monkeypatch,
    capsys,
):
    db_path = tmp_path / "screen_failure_exception.db"
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
  - name: "Exploding metric"
    left:
      metric: exploding_metric
    operator: ">"
    right:
      value: 0
"""
    )

    class ExplodingMetric:
        id = "exploding_metric"
        required_concepts = ()

        def compute(self, symbol, repo):
            raise RuntimeError("boom")

    monkeypatch.setitem(cli.REGISTRY, "exploding_metric", ExplodingMetric)

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
    assert "- exploding_metric: missing=1 symbols, affects=1 criteria" in output
    assert "exception: RuntimeError: 1" in output


def test_cmd_compute_metrics(tmp_path):
    db_path = tmp_path / "facts.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    recent = (date.today() - timedelta(days=15)).isoformat()
    q3 = (date.today() - timedelta(days=105)).isoformat()
    q2 = (date.today() - timedelta(days=195)).isoformat()
    q1 = (date.today() - timedelta(days=285)).isoformat()
    fact_repo.replace_facts(
        "AAPL.US",
        [
            make_fact(concept="AssetsCurrent", end_date=recent, value=500),
            make_fact(concept="LiabilitiesCurrent", end_date=recent, value=200),
            make_fact(
                concept="EarningsPerShare",
                end_date=recent,
                value=2.5,
                fiscal_period="Q4",
            ),
            make_fact(
                concept="EarningsPerShare", end_date=q3, value=2.0, fiscal_period="Q3"
            ),
            make_fact(
                concept="EarningsPerShare", end_date=q2, value=1.5, fiscal_period="Q2"
            ),
            make_fact(
                concept="EarningsPerShare", end_date=q1, value=1.0, fiscal_period="Q1"
            ),
            make_fact(concept="StockholdersEquity", end_date=recent, value=1000),
            make_fact(
                concept="CommonStockSharesOutstanding", end_date=recent, value=100
            ),
            make_fact(concept="Goodwill", end_date=recent, value=50),
            make_fact(
                concept="IntangibleAssetsNetExcludingGoodwill",
                end_date=recent,
                value=25,
            ),
        ],
    )
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAPL.US", recent, 150.0)

    rc = cli.cmd_compute_metrics(
        "AAPL.US",
        ["working_capital", "graham_multiplier"],
        str(db_path),
        run_all=False,
        exchange_code=None,
    )
    assert rc == 0
    value = repo.fetch("AAPL.US", "working_capital")
    assert value[0] == 300
    graham_value = repo.fetch("AAPL.US", "graham_multiplier")
    assert graham_value[0] > 0


def test_cmd_compute_metrics_all(tmp_path):
    db_path = tmp_path / "runall.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    records = []
    current_year = date.today().year
    start_year = current_year - 14
    for year in range(start_year, current_year + 1):
        frame = f"CY{year}"
        records.append(
            make_fact(
                concept="EarningsPerShare",
                end_date=f"{year}-09-30",
                value=2.0 + 0.1 * (year - 2010),
                frame=frame,
                fiscal_period="FY",
            )
        )
    for year in range(current_year - 10, current_year + 1):
        end_date = f"{year}-09-30"
        records.extend(
            [
                make_fact(concept="AssetsCurrent", end_date=end_date, value=400 + year),
                make_fact(
                    concept="LiabilitiesCurrent", end_date=end_date, value=200 + year
                ),
                make_fact(
                    concept="OperatingIncomeLoss", end_date=end_date, value=150 + year
                ),
                make_fact(
                    concept="NetCashProvidedByUsedInOperatingActivities",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=220 + year,
                ),
                make_fact(
                    concept="NetIncomeLoss",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=110 + (year - (current_year - 10)),
                ),
                make_fact(
                    concept="Revenues",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=2000 + year,
                ),
                make_fact(
                    concept="GrossProfit",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=900 + (year / 2),
                ),
                make_fact(
                    concept="PropertyPlantAndEquipmentNet",
                    end_date=end_date,
                    value=500 + year,
                ),
                make_fact(
                    concept="CapitalExpenditures",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=80 + year,
                ),
                make_fact(
                    concept="DepreciationDepletionAndAmortization",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=70 + year,
                ),
                make_fact(
                    concept="ShortTermDebt",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=60 + (year - (current_year - 9)),
                ),
                make_fact(
                    concept="CashAndShortTermInvestments",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=100 + (year - (current_year - 9)),
                ),
                make_fact(
                    concept="StockholdersEquity",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=1500 + (year - (current_year - 10)),
                ),
                make_fact(
                    concept="CommonStockholdersEquity",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=1500 + (year - (current_year - 10)),
                ),
                make_fact(
                    concept="CommonStockSharesOutstanding",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=500 + 10 * (year - (current_year - 10)),
                ),
                make_fact(
                    concept="WeightedAverageNumberOfDilutedSharesOutstanding",
                    end_date=end_date,
                    fiscal_period="FY",
                    value=480 + 8 * (year - (current_year - 10)),
                ),
            ]
        )
        records.extend(
            [
                make_fact(
                    concept="StockholdersEquity",
                    end_date=f"{current_year}-09-30",
                    value=2000,
                ),
                make_fact(
                    concept="StockholdersEquity",
                    end_date=f"{current_year - 1}-09-30",
                    value=1800,
                ),
                make_fact(
                    concept="StockholdersEquity",
                    end_date=f"{current_year - 2}-09-30",
                    value=1600,
                ),
                make_fact(
                    concept="StockholdersEquity",
                    end_date=f"{current_year - 3}-09-30",
                    value=1400,
                ),
                make_fact(
                    concept="StockholdersEquity",
                    end_date=f"{current_year - 4}-09-30",
                    value=1200,
                ),
                make_fact(
                    concept="StockholdersEquity",
                    end_date=f"{current_year - 5}-09-30",
                    value=1000,
                ),
                make_fact(
                    concept="CommonStockholdersEquity",
                    end_date=f"{current_year}-09-30",
                    value=2000,
                ),
                make_fact(
                    concept="CommonStockholdersEquity",
                    end_date=f"{current_year - 1}-09-30",
                    value=1800,
                ),
                make_fact(
                    concept="CommonStockholdersEquity",
                    end_date=f"{current_year - 2}-09-30",
                    value=1600,
                ),
                make_fact(
                    concept="CommonStockholdersEquity",
                    end_date=f"{current_year - 3}-09-30",
                    value=1400,
                ),
                make_fact(
                    concept="CommonStockholdersEquity",
                    end_date=f"{current_year - 4}-09-30",
                    value=1200,
                ),
                make_fact(
                    concept="CommonStockholdersEquity",
                    end_date=f"{current_year - 5}-09-30",
                    value=1000,
                ),
                make_fact(
                    concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                    end_date=f"{current_year}-09-30",
                    value=250,
                ),
                make_fact(
                    concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                    end_date=f"{current_year - 1}-09-30",
                    value=230,
                ),
                make_fact(
                    concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                    end_date=f"{current_year - 2}-09-30",
                    value=210,
                ),
                make_fact(
                    concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                    end_date=f"{current_year - 3}-09-30",
                    value=190,
                ),
                make_fact(
                    concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                    end_date=f"{current_year - 4}-09-30",
                    value=170,
                ),
                make_fact(
                    concept="PreferredStock", end_date=f"{current_year}-09-30", value=0
                ),
                make_fact(
                    concept="Goodwill", end_date=f"{current_year}-09-30", value=100
                ),
                make_fact(
                    concept="IntangibleAssetsNetExcludingGoodwill",
                    end_date=f"{current_year}-09-30",
                    value=50,
                ),
                make_fact(
                    concept="LongTermDebt", end_date=f"{current_year}-09-30", value=300
                ),
                make_fact(
                    concept="LongTermDebt",
                    end_date=f"{current_year - 1}-09-30",
                    value=280,
                ),
                make_fact(
                    concept="ShortTermDebt", end_date=f"{current_year}-09-30", value=75
                ),
                make_fact(
                    concept="ShortTermDebt",
                    end_date=f"{current_year - 1}-09-30",
                    value=70,
                ),
                make_fact(
                    concept="CashAndShortTermInvestments",
                    end_date=f"{current_year}-09-30",
                    value=125,
                ),
                make_fact(
                    concept="CashAndShortTermInvestments",
                    end_date=f"{current_year - 1}-09-30",
                    value=120,
                ),
            ]
        )
    q4 = (date.today() - timedelta(days=20)).isoformat()
    q3 = (date.today() - timedelta(days=110)).isoformat()
    q2 = (date.today() - timedelta(days=200)).isoformat()
    q1 = (date.today() - timedelta(days=290)).isoformat()
    q4_prev = (date.today() - timedelta(days=380)).isoformat()
    quarterly_nwc_points = [
        (q4, "Q4", 620.0, 360.0, 130.0, 55.0),
        (q3, "Q3", 600.0, 350.0, 125.0, 50.0),
        (q2, "Q2", 590.0, 345.0, 120.0, 48.0),
        (q1, "Q1", 580.0, 340.0, 115.0, 46.0),
        (q4_prev, "Q4", 560.0, 350.0, 140.0, 65.0),
    ]
    for end_date, period, assets, liabilities, cash, short_debt in quarterly_nwc_points:
        records.append(
            make_fact(
                concept="AssetsCurrent",
                end_date=end_date,
                fiscal_period=period,
                value=assets,
            )
        )
        records.append(
            make_fact(
                concept="LiabilitiesCurrent",
                end_date=end_date,
                fiscal_period=period,
                value=liabilities,
            )
        )
        records.append(
            make_fact(
                concept="CashAndShortTermInvestments",
                end_date=end_date,
                fiscal_period=period,
                value=cash,
            )
        )
        records.append(
            make_fact(
                concept="ShortTermDebt",
                end_date=end_date,
                fiscal_period=period,
                value=short_debt,
            )
        )
        records.append(
            make_fact(
                concept="StockholdersEquity",
                end_date=end_date,
                fiscal_period=period,
                value=assets + 400.0,
            )
        )
    quarterly_assets = [
        (q4, "Q4", 1600.0),
        (q3, "Q3", 1550.0),
        (q2, "Q2", 1500.0),
        (q1, "Q1", 1450.0),
        (q4_prev, "Q4", 1400.0),
    ]
    for end_date, period, value in quarterly_assets:
        records.append(
            make_fact(
                concept="Assets",
                end_date=end_date,
                fiscal_period=period,
                value=value,
            )
        )
    quarterly_cash_flows = [
        (q4, "Q4", 130.0, 40.0, -60.0, 18.0),
        (q3, "Q3", 120.0, 35.0, -55.0, 17.0),
        (q2, "Q2", 110.0, 30.0, -50.0, 16.0),
        (q1, "Q1", 100.0, 25.0, -45.0, 15.0),
        (q4_prev, "Q4", 90.0, 20.0, -40.0, 14.0),
    ]
    for end_date, period, ocf, capex, sale_purchase, sbc in quarterly_cash_flows:
        records.append(
            make_fact(
                concept="NetCashProvidedByUsedInOperatingActivities",
                end_date=end_date,
                fiscal_period=period,
                value=ocf,
            )
        )
        records.append(
            make_fact(
                concept="CapitalExpenditures",
                end_date=end_date,
                fiscal_period=period,
                value=capex,
            )
        )
        records.append(
            make_fact(
                concept="DepreciationDepletionAndAmortization",
                end_date=end_date,
                fiscal_period=period,
                value=capex * 0.8,
            )
        )
        records.append(
            make_fact(
                concept="SalePurchaseOfStock",
                end_date=end_date,
                fiscal_period=period,
                value=sale_purchase,
            )
        )
        records.append(
            make_fact(
                concept="StockBasedCompensation",
                end_date=end_date,
                fiscal_period=period,
                value=sbc,
            )
        )
    quarterly_net_income = [
        (q4, "Q4", 220.0),
        (q3, "Q3", 210.0),
        (q2, "Q2", 200.0),
        (q1, "Q1", 190.0),
        (q4_prev, "Q4", 180.0),
    ]
    for end_date, period, value in quarterly_net_income:
        records.append(
            make_fact(
                concept="NetIncomeLoss",
                end_date=end_date,
                fiscal_period=period,
                value=value,
            )
        )
    quarterly_revenues = [
        (q4, "Q4", 600.0),
        (q3, "Q3", 580.0),
        (q2, "Q2", 560.0),
        (q1, "Q1", 540.0),
    ]
    for end_date, period, value in quarterly_revenues:
        records.append(
            make_fact(
                concept="Revenues",
                end_date=end_date,
                fiscal_period=period,
                value=value,
            )
        )
        records.append(
            make_fact(
                concept="GrossProfit",
                end_date=end_date,
                fiscal_period=period,
                value=value * 0.6,
            )
        )
        records.append(
            make_fact(
                concept="CostOfRevenue",
                end_date=end_date,
                fiscal_period=period,
                value=value * 0.4,
            )
        )
        records.append(
            make_fact(
                concept="CommonStockDividendsPaid",
                end_date=end_date,
                fiscal_period=period,
                value=-12.5,
            )
        )
    quarterly_eps = [
        (q4, "Q4", 2.5),
        (q3, "Q3", 2.0),
        (q2, "Q2", 1.5),
        (q1, "Q1", 1.0),
        (q4_prev, "Q4", 0.5),
    ]
    for end_date, period, value in quarterly_eps:
        records.append(
            make_fact(
                concept="EarningsPerShare",
                end_date=end_date,
                fiscal_period=period,
                value=value,
                frame=f"CY{end_date[:4]}{period}",
            )
        )
    quarterly_ebitda = [
        (q4, "Q4", 400.0),
        (q3, "Q3", 350.0),
        (q2, "Q2", 300.0),
        (q1, "Q1", 250.0),
    ]
    for end_date, period, value in quarterly_ebitda:
        records.append(
            make_fact(
                concept="EBITDA",
                end_date=end_date,
                fiscal_period=period,
                value=value,
            )
        )
    quarterly_ebit = [
        (q4, "Q4", 300.0),
        (q3, "Q3", 250.0),
        (q2, "Q2", 200.0),
        (q1, "Q1", 150.0),
    ]
    for end_date, period, value in quarterly_ebit:
        records.append(
            make_fact(
                concept="OperatingIncomeLoss",
                end_date=end_date,
                fiscal_period=period,
                value=value,
            )
        )
    quarterly_pretax = [
        (q4, "Q4", 320.0),
        (q3, "Q3", 270.0),
        (q2, "Q2", 220.0),
        (q1, "Q1", 170.0),
    ]
    for end_date, period, value in quarterly_pretax:
        records.append(
            make_fact(
                concept="IncomeBeforeIncomeTaxes",
                end_date=end_date,
                fiscal_period=period,
                value=value,
            )
        )
    quarterly_tax = [
        (q4, "Q4", 64.0),
        (q3, "Q3", 54.0),
        (q2, "Q2", 44.0),
        (q1, "Q1", 34.0),
    ]
    for end_date, period, value in quarterly_tax:
        records.append(
            make_fact(
                concept="IncomeTaxExpense",
                end_date=end_date,
                fiscal_period=period,
                value=value,
            )
        )
    quarterly_interest = [
        (q4, "Q4", 12.0),
        (q3, "Q3", 11.0),
        (q2, "Q2", 10.0),
        (q1, "Q1", 9.0),
    ]
    for end_date, period, value in quarterly_interest:
        records.append(
            make_fact(
                concept="InterestExpense",
                end_date=end_date,
                fiscal_period=period,
                value=value,
            )
        )
    fact_repo.replace_facts("AAPL.US", records)

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAPL.US", q3, 150.0, market_cap=50000.0)
    market_repo.upsert_price(
        "AAPL.US", f"{current_year - 5}-09-30", 100.0, market_cap=30000.0
    )

    rc = cli.cmd_compute_metrics(
        "AAPL.US",
        ["placeholder"],
        str(db_path),
        run_all=True,
        exchange_code=None,
    )
    assert rc == 0
    for metric_id in REGISTRY.keys():
        row = metrics_repo.fetch("AAPL.US", metric_id)
        assert row is not None, f"{metric_id} missing"
    assert metrics_repo.fetch("AAPL.US", "oey_equity") is not None
    assert metrics_repo.fetch("AAPL.US", "oey_equity_5y") is not None
    assert metrics_repo.fetch("AAPL.US", "oey_ev") is not None
    assert metrics_repo.fetch("AAPL.US", "oey_ev_norm") is not None
    assert metrics_repo.fetch("AAPL.US", "oe_ev_ttm") is not None
    assert metrics_repo.fetch("AAPL.US", "oe_ev_5y_avg") is not None
    assert metrics_repo.fetch("AAPL.US", "oe_ev_fy_median_5y") is not None
    assert metrics_repo.fetch("AAPL.US", "worst_oe_ev_fy_10y") is not None
    assert metrics_repo.fetch("AAPL.US", "fcf_fy_median_5y") is not None
    assert metrics_repo.fetch("AAPL.US", "ni_loss_years_10y") is not None
    assert metrics_repo.fetch("AAPL.US", "fcf_neg_years_10y") is not None
