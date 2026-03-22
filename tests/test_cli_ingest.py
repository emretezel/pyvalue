"""Tests for CLI ingestion and metric commands.

Author: Emre Tezel
"""

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from pyvalue import cli
from pyvalue.metrics import REGISTRY
from pyvalue.metrics.base import MetricResult
from pyvalue.storage import (
    EntityMetadataRepository,
    FundamentalsRepository,
    FundamentalsFetchStateRepository,
    FinancialFactsRepository,
    FactRecord,
    MarketDataFetchStateRepository,
    MarketDataRepository,
    MetricsRepository,
    SupportedExchangeRepository,
    SupportedTickerRepository,
)
from pyvalue.universe import Listing


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
        resume,
    ):
        calls["provider"] = provider
        calls["database"] = database
        calls["symbols"] = symbols
        calls["exchange_codes"] = exchange_codes
        calls["all_supported"] = all_supported
        calls["rate"] = rate
        calls["max_symbols"] = max_symbols
        calls["max_age_days"] = max_age_days
        calls["resume"] = resume
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
        "max_age_days": 7,
        "resume": False,
    }


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
        "max_age_days": 7,
    }


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

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append((symbol, exchange_code))
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_ingest_fundamentals_bulk(
        provider="EODHD",
        database=str(db_path),
        rate=0,
        exchange_code="LSE",
        user_agent=None,
        max_symbols=None,
        max_age_days=None,
        resume=False,
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

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append((symbol, exchange_code))
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")

    rc = cli.cmd_ingest_fundamentals_bulk(
        provider="EODHD",
        database=str(db_path),
        rate=0,
        exchange_code="US",
        user_agent=None,
        max_symbols=None,
        max_age_days=None,
        resume=False,
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
        resume=False,
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
        resume=False,
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
        resume=False,
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
        resume=False,
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
        resume=False,
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
            (
                datetime.now(timezone.utc).isoformat(),
                (datetime.now(timezone.utc) - timedelta(days=45)).isoformat(),
            ),
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
        resume=False,
    )

    assert rc == 0
    assert calls["fetched"] == ["CCC.US", "BBB.US"]


def test_cmd_ingest_fundamentals_global_resume_respects_backoff(monkeypatch, tmp_path):
    db_path = tmp_path / "global-resume.db"
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
        resume=True,
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
    with fund_repo._connect() as conn:
        conn.execute(
            """
            UPDATE fundamentals_raw
            SET fetched_at = ?
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            """,
            ((datetime.now(timezone.utc) - timedelta(days=45)).isoformat(),),
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
        resume=False,
    )

    assert rc == 0
    assert calls["fetched"] == ["BBB.US"]


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
    with fund_repo._connect() as conn:
        conn.execute(
            """
            UPDATE fundamentals_raw
            SET fetched_at = ?
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            """,
            ((datetime.now(timezone.utc) - timedelta(days=45)).isoformat(),),
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
    with fund_repo._connect() as conn:
        conn.execute(
            """
            UPDATE fundamentals_raw
            SET fetched_at = ?
            WHERE provider = 'EODHD' AND provider_symbol = 'AAA.US'
            """,
            ((datetime.now(timezone.utc) - timedelta(days=120)).isoformat(),),
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
    assert "Next action: Wait for backoff to expire or rerun without --resume" in output


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
        resume=False,
    )

    assert rc == 0
    assert calls["api_key"] == "TOKEN"
    assert calls["refreshed"] == ["AAA.US", "BBB.US"]
    state_repo = MarketDataFetchStateRepository(db_path)
    assert state_repo.fetch("EODHD", "AAA.US")["last_status"] == "ok"
    assert state_repo.fetch("EODHD", "BBB.US")["last_status"] == "ok"


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
        resume=False,
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
        resume=False,
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
        resume=False,
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
        resume=False,
    )
    assert rc == 0
    assert calls["refreshed"] == ["BBB.US"]


def test_cmd_update_market_data_global_resume_respects_backoff(monkeypatch, tmp_path):
    db_path = tmp_path / "global-market-data-resume.db"
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
        resume=True,
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
    assert "Next action: Wait for backoff to expire or rerun without --resume" in output


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
        rate=0,
        exchange_code="US",
        user_agent=None,
        max_symbols=None,
        max_age_days=30,
        resume=True,
    )

    assert rc == 0
    assert calls["fetched"] == []

    calls["fetched"].clear()
    rc = cli.cmd_ingest_fundamentals_bulk(
        provider="EODHD",
        database=str(db_path),
        rate=0,
        exchange_code="US",
        user_agent=None,
        max_symbols=None,
        max_age_days=30,
        resume=False,
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


def test_cmd_clear_fundamentals_raw(tmp_path):
    db_path = tmp_path / "clearfunds.db"
    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    repo.upsert("SEC", "AAA.US", {"facts": {}})

    with repo._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM fundamentals_raw").fetchone()[0] == 1

    rc = cli.cmd_clear_fundamentals_raw(str(db_path))
    assert rc == 0

    with repo._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM fundamentals_raw").fetchone()[0] == 0


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
            SELECT ff.concept, ff.value
            FROM financial_facts ff
            JOIN securities s ON s.security_id = ff.security_id
            WHERE s.canonical_symbol = 'AAPL.US'
            """
        )
        .fetchall()
    )
    assert [(row[0], row[1]) for row in rows] == [("NetIncomeLoss", 123.0)]
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch("AAPL.US") == "Apple Inc"


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
        {"General": {"Name": "Shell PLC"}, "Financials": {}},
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
            SELECT ff.concept, ff.value
            FROM financial_facts ff
            JOIN securities s ON s.security_id = ff.security_id
            WHERE s.canonical_symbol = 'SHEL.LSE'
            """
        )
        .fetchall()
    )
    assert [(row[0], row[1]) for row in rows] == [("NetIncomeLoss", 10.0)]
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch("SHEL.LSE") == "Shell PLC"


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
    snapshot_b = market_repo.latest_snapshot("BBB.US")
    assert snapshot_b.market_cap is None


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
