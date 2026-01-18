"""Tests for CLI ingestion and metric commands.

Author: Emre Tezel
"""

from datetime import date, timedelta
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
    MarketDataRepository,
    MetricsRepository,
    UniverseRepository,
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
            "SELECT currency, exchange FROM fundamentals_raw WHERE provider='EODHD' AND symbol='SHEL.LSE'"
        ).fetchone()
    assert row[0] == "USD"
    assert row[1] == "LSE"


def test_cmd_ingest_fundamentals_bulk_eodhd_with_exchange(monkeypatch, tmp_path):
    db_path = tmp_path / "bulkfunds.db"
    calls = {"listed": False, "fetched": []}

    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [
            Listing(symbol="AAA.LSE", security_name="AAA plc", exchange="LSE"),
            Listing(symbol="CCC.LSE", security_name="CCC plc", exchange="LSE"),
        ]
    )

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def list_symbols(self, exchange_code):
            calls["listed"] = exchange_code
            return [
                {"Code": "AAA", "Exchange": exchange_code, "Type": "Common Stock"},
                {"Code": "BBB", "Exchange": exchange_code, "Type": "ETF"},
                {"Code": "CCC", "Exchange": exchange_code, "Type": "Preferred Stock"},
            ]

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append((symbol, exchange_code))
            return {"General": {"CurrencyCode": "USD", "Name": symbol}}

        def exchange_metadata(self, exchange_code):
            return {"Name": "London", "Country": "UK", "Currency": "GBP"}

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
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    listings = [
        Listing(symbol="AAA.US", security_name="AAA Inc", exchange="US"),
        Listing(symbol="BBB.US", security_name="BBB Inc", exchange="US"),
    ]
    universe_repo.replace_universe(listings)

    calls = {"fetched": []}

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

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
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    listings = [
        Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
        Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
    ]
    universe_repo.replace_universe(listings)

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
        exchange_code="NYSE",
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
    calls = {}

    class FakeLoader:
        def __init__(
            self,
            api_key,
            exchange_code,
            include_etfs=False,
            allowed_currencies=None,
            include_exchanges=None,
            fetcher=None,
            session=None,
        ):
            calls["api_key"] = api_key
            calls["exchange_code"] = exchange_code
            calls["include_etfs"] = include_etfs
            calls["allowed_currencies"] = allowed_currencies
            calls["include_exchanges"] = include_exchanges

        def load(self):
            return [
                Listing(
                    symbol="AAA.LSE",
                    security_name="AAA plc",
                    exchange="LSE",
                    currency="GBX",
                ),
                Listing(
                    symbol="ETF1.LSE",
                    security_name="ETF",
                    exchange="LSE",
                    is_etf=True,
                    currency="GBX",
                ),
            ]

    monkeypatch.setattr(cli, "UKUniverseLoader", FakeLoader)
    monkeypatch.setattr(cli, "Config", lambda: SimpleNamespace(eodhd_api_key="KEY"))

    db_path = tmp_path / "uk.db"

    class FakeClient:
        def __init__(self, api_key):
            calls["meta_api_key"] = api_key

        def exchange_metadata(self, code):
            return {"Name": "London", "Country": "UK", "Currency": "GBP"}

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)

    db_path = tmp_path / "uk.db"
    rc = cli.cmd_load_universe(
        provider="EODHD",
        database=str(db_path),
        include_etfs=False,
        exchange_code="LSE",
        currencies=["GBP"],
        include_exchanges=["LSE"],
    )

    assert rc == 0
    assert calls["api_key"] == "KEY"
    assert calls["exchange_code"] == "LSE"
    assert calls["include_etfs"] is False
    assert calls["allowed_currencies"] == ["GBP"]
    assert calls["include_exchanges"] == ["LSE"]


def test_cmd_ingest_fundamentals_bulk_uses_exchange_listings(monkeypatch, tmp_path):
    calls = {"fetched": []}

    class FakeClient:
        def __init__(self, api_key):
            calls["api_key"] = api_key

        def fetch_fundamentals(self, symbol, exchange_code=None):
            calls["fetched"].append((symbol, exchange_code))
            return {"General": {"CurrencyCode": "USD"}}

        def list_symbols(self, exchange_code):
            raise AssertionError(
                "Should not call list_symbols for exchange listings path"
            )

    monkeypatch.setattr(cli, "EODHDFundamentalsClient", FakeClient)
    monkeypatch.setattr(cli, "_require_eodhd_key", lambda: "TOKEN")

    db_path = tmp_path / "eodhd-exchange.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [Listing(symbol="AAA.US", security_name="AAA", exchange="US")]
    )
    universe_repo.replace_universe(
        [Listing(symbol="BBB.LSE", security_name="BBB", exchange="LSE")]
    )

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
    assert calls["api_key"] == "TOKEN"
    assert calls["fetched"] == [("AAA.US", None)]

    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    assert fund_repo.fetch("EODHD", "AAA.US") is not None
    with fund_repo._connect() as conn:
        row = conn.execute(
            "SELECT currency, exchange FROM fundamentals_raw WHERE provider='EODHD' AND symbol='AAA.US'"
        ).fetchone()
    assert row[0] == "USD"
    assert row[1] == "US"


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
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [
            Listing(symbol="AAA.US", security_name="AAA", exchange="US"),
            Listing(symbol="BBB.US", security_name="BBB", exchange="US"),
        ]
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
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    listings = [
        Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
        Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
    ]
    universe_repo.replace_universe(listings)

    calls = []

    class DummyService:
        def __init__(self, db_path):
            self.db_path = db_path

        def refresh_symbol(self, symbol, fetch_symbol=None):
            calls.append(symbol)

    monkeypatch.setattr(cli, "MarketDataService", lambda db_path: DummyService(db_path))

    rc = cli.cmd_update_market_data_bulk(
        database=str(db_path),
        rate=0,
        exchange_code="NYSE",
    )
    assert rc == 0
    assert calls == ["AAA.US", "BBB.US"]


def test_cmd_update_market_data_bulk_with_exchange(monkeypatch, tmp_path):
    db_path = tmp_path / "marketbulk_exchange.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [Listing(symbol="AAA", security_name="AAA PLC", exchange="LSE")]
    )
    universe_repo.replace_universe(
        [Listing(symbol="BBB", security_name="BBB Inc", exchange="NYSE")]
    )

    calls = []

    class DummyService:
        def __init__(self, db_path):
            self.db_path = db_path

        def refresh_symbol(self, symbol, fetch_symbol=None):
            calls.append((symbol, fetch_symbol))

    monkeypatch.setattr(cli, "MarketDataService", lambda db_path: DummyService(db_path))

    rc = cli.cmd_update_market_data_bulk(
        database=str(db_path),
        rate=0,
        exchange_code="LSE",
    )
    assert rc == 0
    assert calls == [("AAA", "AAA.LSE")]


def test_cmd_compute_metrics_bulk(monkeypatch, tmp_path):
    db_path = tmp_path / "metricsbulk.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    listings = [
        Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
        Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
    ]
    universe_repo.replace_universe(listings)

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
        database=str(db_path),
        metric_ids=None,
        exchange_code="NYSE",
    )
    assert rc == 0

    assert metrics_repo.fetch("AAA.US", "dummy_metric")[0] == len("AAA.US")
    assert metrics_repo.fetch("BBB.US", "dummy_metric")[0] == len("BBB.US")


def test_cmd_compute_metrics_bulk_with_exchange(monkeypatch, tmp_path):
    db_path = tmp_path / "metrics_exchange.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="US"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="US"),
        ]
    )
    universe_repo.replace_universe(
        [Listing(symbol="CCC.LSE", security_name="CCC PLC", exchange="LSE")]
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
        database=str(db_path),
        metric_ids=None,
        exchange_code="LSE",
    )
    assert rc == 0

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    rows = (
        metrics_repo._connect()
        .execute("SELECT symbol FROM metrics ORDER BY symbol")
        .fetchall()
    )
    assert [row[0] for row in rows] == ["CCC.LSE"]


def test_cmd_compute_metrics_bulk_requires_listings(monkeypatch, tmp_path):
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
            database=str(db_path),
            metric_ids=None,
            exchange_code="LSE",
        )
    assert "No listings found for exchange LSE" in str(exc.value)


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
        .execute("SELECT concept, value FROM financial_facts WHERE symbol='AAPL.US'")
        .fetchall()
    )
    assert [(row[0], row[1]) for row in rows] == [("NetIncomeLoss", 123.0)]
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch("AAPL.US") == "Apple Inc"


def test_cmd_normalize_fundamentals_bulk_sec(monkeypatch, tmp_path):
    db_path = tmp_path / "facts.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [
            Listing(symbol="AAA.US", security_name="AAA Corp", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Corp", exchange="NYSE"),
        ]
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
        "SELECT symbol, value FROM financial_facts ORDER BY symbol"
    )
    facts = [(row[0], row[1]) for row in cursor.fetchall()]
    assert facts == [("AAA.US", 6.0), ("BBB.US", 6.0)]
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch("AAA.US") == "AAA Corp"
    assert entity_repo.fetch("BBB.US") == "BBB Corp"


def test_cmd_normalize_fundamentals_bulk_with_exchange(monkeypatch, tmp_path):
    db_path = tmp_path / "fundamentals_exchange.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="US"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="US"),
            Listing(symbol="CCC.US", security_name="CCC Inc", exchange="US"),
        ]
    )
    universe_repo.replace_universe(
        [Listing(symbol="DDD.LSE", security_name="DDD PLC", exchange="LSE")]
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
        .execute("SELECT symbol FROM financial_facts ORDER BY symbol")
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
        .execute("SELECT concept, value FROM financial_facts WHERE symbol='SHEL.LSE'")
        .fetchall()
    )
    assert [(row[0], row[1]) for row in rows] == [("NetIncomeLoss", 10.0)]
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch("SHEL.LSE") == "Shell PLC"


def test_cmd_recalc_market_cap(tmp_path):
    db_path = tmp_path / "marketcap.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ]
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

    rc = cli.cmd_recalc_market_cap(database=str(db_path), exchange_code="NYSE")
    assert rc == 0
    snapshot = market_repo.latest_snapshot("AAA.US")
    assert snapshot.market_cap == 5000.0
    snapshot_b = market_repo.latest_snapshot("BBB.US")
    assert snapshot_b.market_cap is None


def test_cmd_run_screen_bulk(tmp_path, capsys):
    db_path = tmp_path / "screen.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ]
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
        database=str(db_path),
        output_csv=str(csv_path),
        exchange_code="NYSE",
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
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [Listing(symbol="AAA.LSE", security_name="AAA PLC", exchange="LSE")]
    )
    universe_repo.replace_universe(
        [Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE")]
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
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [
            Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
        ]
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAA.US", "2024-01-01", price=10.0, market_cap=100.0)
    market_repo.upsert_price("BBB.US", "2024-01-01", price=10.0, market_cap=200.0)

    rc = cli.cmd_report_metric_failures(
        database=str(db_path),
        metric_ids=["working_capital"],
        symbols=["AAA.US", "BBB.US"],
        output_csv=None,
        exchange_code="NYSE",
    )
    assert rc == 0
    output = capsys.readouterr().out
    assert "working_capital" in output
    assert "example=BBB.US" in output


def test_cmd_report_metric_failures_with_exchange(tmp_path, capsys):
    db_path = tmp_path / "failures_exchange.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    universe_repo.replace_universe(
        [Listing(symbol="AAA.LSE", security_name="AAA PLC", exchange="LSE")]
    )
    universe_repo.replace_universe(
        [Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE")]
    )

    rc = cli.cmd_report_metric_failures(
        database=str(db_path),
        metric_ids=["working_capital"],
        symbols=None,
        output_csv=None,
        exchange_code="LSE",
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
    for year in range(current_year - 9, current_year + 1):
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
                    concept="PropertyPlantAndEquipmentNet",
                    end_date=end_date,
                    value=500 + year,
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
                    concept="CommonStockSharesOutstanding",
                    end_date=f"{current_year}-09-30",
                    value=500,
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
                    concept="ShortTermDebt", end_date=f"{current_year}-09-30", value=75
                ),
                make_fact(
                    concept="CashAndShortTermInvestments",
                    end_date=f"{current_year}-09-30",
                    value=125,
                ),
            ]
        )
    q4 = (date.today() - timedelta(days=20)).isoformat()
    q3 = (date.today() - timedelta(days=110)).isoformat()
    q2 = (date.today() - timedelta(days=200)).isoformat()
    q1 = (date.today() - timedelta(days=290)).isoformat()
    q4_prev = (date.today() - timedelta(days=380)).isoformat()
    quarterly_cash_flows = [
        (q4, "Q4", 130.0, 40.0),
        (q3, "Q3", 120.0, 35.0),
        (q2, "Q2", 110.0, 30.0),
        (q1, "Q1", 100.0, 25.0),
        (q4_prev, "Q4", 90.0, 20.0),
    ]
    for end_date, period, ocf, capex in quarterly_cash_flows:
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
