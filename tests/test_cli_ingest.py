"""Tests for CLI ingestion and metric commands.

Author: Emre Tezel
"""
from types import SimpleNamespace

from pyvalue import cli
from pyvalue.metrics import REGISTRY
from pyvalue.metrics.base import MetricResult
from pyvalue.storage import (
    CompanyFactsRepository,
    EntityMetadataRepository,
    FundamentalsRepository,
    FinancialFactsRepository,
    FactRecord,
    MarketDataRepository,
    MetricsRepository,
    UKCompanyFactsRepository,
    UKFilingRepository,
    UKSymbolMapRepository,
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


def test_cmd_ingest_us_facts(monkeypatch, tmp_path):
    calls = {}

    class FakeClient:
        def __init__(self, user_agent=None):
            calls["ua"] = user_agent

        def resolve_company(self, symbol):
            return SimpleNamespace(symbol=symbol.upper(), cik="CIK0000320193", name="Apple")

        def fetch_company_facts(self, cik):
            calls["cik"] = cik
            return {"cik": cik, "data": []}

    monkeypatch.setattr(cli, "SECCompanyFactsClient", FakeClient)

    db_path = tmp_path / "facts.db"
    rc = cli.cmd_ingest_us_facts("AAPL", str(db_path), user_agent="UA", cik=None)
    assert rc == 0
    assert calls["ua"] == "UA"
    assert calls["cik"] == "CIK0000320193"

    repo = CompanyFactsRepository(db_path)
    stored = repo.fetch_fact("AAPL.US")
    assert stored["cik"] == "CIK0000320193"


def test_cmd_ingest_eodhd_fundamentals(monkeypatch, tmp_path):
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

    rc = cli.cmd_ingest_eodhd_fundamentals(
        symbol="SHEL.LSE", database=str(db_path), exchange_code=None
    )
    assert rc == 0
    assert calls == {"api_key": "TOKEN", "symbol": "SHEL.LSE", "exchange_code": None}

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    payload = repo.fetch("EODHD", "SHEL.LSE")
    assert payload["General"]["CurrencyCode"] == "USD"
    with repo._connect() as conn:
        row = conn.execute(
            "SELECT region, exchange FROM fundamentals_raw WHERE provider='EODHD' AND symbol='SHEL.LSE'"
        ).fetchone()
    assert row[0] == "UK"
    assert row[1] == "LSE"


def test_cmd_ingest_eodhd_fundamentals_bulk_with_exchange(monkeypatch, tmp_path):
    db_path = tmp_path / "bulkfunds.db"
    calls = {"listed": False, "fetched": []}

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

    rc = cli.cmd_ingest_eodhd_fundamentals_bulk(
        database=str(db_path), rate=0, exchange_code="LSE"
    )
    assert rc == 0
    assert calls["listed"] == "LSE"
    # ETF filtered out; only AAA and CCC stored
    assert set(calls["fetched"]) == {("AAA.LSE", None), ("CCC.LSE", None)}

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    assert repo.fetch("EODHD", "AAA.LSE")["General"]["Name"] == "AAA.LSE"
    assert repo.fetch("EODHD", "CCC.LSE")["General"]["Name"] == "CCC.LSE"

def test_cmd_ingest_us_facts_bulk(monkeypatch, tmp_path):
    db_path = tmp_path / "bulk.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    listings = [
        Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
        Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
    ]
    universe_repo.replace_universe(listings, region="US")

    company_repo = CompanyFactsRepository(db_path)
    company_repo.initialize_schema()

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
    monkeypatch.setattr(cli, "SECCompanyFactsClient", lambda user_agent=None: fake_client)

    rc = cli.cmd_ingest_us_facts_bulk(
        database=str(db_path),
        region="US",
        rate=0,
        user_agent="UA",
    )
    assert rc == 0
    assert fake_client.calls == ["CIKAAA", "CIKBBB"]
    assert company_repo.fetch_fact("AAA.US") == {"cik": "CIKAAA"}


def test_cmd_load_eodhd_universe(monkeypatch, tmp_path):
    calls = {}

    class FakeLoader:
        def __init__(self, api_key, exchange_code, fetcher=None, session=None):
            calls["api_key"] = api_key
            calls["exchange_code"] = exchange_code

        def load(self):
            return [
                Listing(symbol="AAA.LSE", security_name="AAA plc", exchange="LSE", currency="GBX"),
                Listing(symbol="ETF1.LSE", security_name="ETF", exchange="LSE", is_etf=True, currency="GBX"),
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
    rc = cli.cmd_load_eodhd_universe(str(db_path), include_etfs=False, exchange_code="LSE")

    assert rc == 0
    assert calls["api_key"] == "KEY"
    assert calls["exchange_code"] == "LSE"

    repo = UniverseRepository(db_path)
    assert repo.fetch_symbols("UK") == ["AAA.LSE"]


def test_cmd_ingest_uk_facts(monkeypatch, tmp_path):
    calls = {}

    class FakeClient:
        def __init__(self, api_key=None):
            calls["api_key"] = api_key

        def fetch_company_profile(self, company_number):
            calls["company_number"] = company_number
            return {"company_number": company_number, "name": "Example"}

    monkeypatch.setattr(cli, "CompaniesHouseClient", FakeClient)
    monkeypatch.setattr(cli, "Config", lambda: SimpleNamespace(companies_house_api_key="KEY"))

    db_path = tmp_path / "ukfacts.db"
    rc = cli.cmd_ingest_uk_facts("00000000", str(db_path), symbol="AAA.LSE")

    assert rc == 0
    assert calls == {"api_key": "KEY", "company_number": "00000000"}

    repo = UKCompanyFactsRepository(db_path)
    stored = repo.fetch_fact("00000000")
    assert stored["company_number"] == "00000000"


def test_cmd_ingest_uk_facts_by_symbol(monkeypatch, tmp_path):
    calls = {}

    class FakeClient:
        def __init__(self, api_key=None):
            calls["api_key"] = api_key

        def fetch_company_profile(self, company_number):
            calls["company_number"] = company_number
            return {"company_number": company_number, "name": "Example"}

    monkeypatch.setattr(cli, "CompaniesHouseClient", FakeClient)
    monkeypatch.setattr(cli, "Config", lambda: SimpleNamespace(companies_house_api_key="KEY"))

    db_path = tmp_path / "ukfacts.db"
    mapper = UKSymbolMapRepository(db_path)
    mapper.initialize_schema()
    mapper.upsert_mapping("AAA.LSE", company_number="00000000")

    rc = cli.cmd_ingest_uk_facts(None, str(db_path), symbol="AAA.LSE")

    assert rc == 0
    assert calls == {"api_key": "KEY", "company_number": "00000000"}


def test_cmd_ingest_uk_filings(monkeypatch, tmp_path):
    calls = {}

    class FakeClient:
        def __init__(self, api_key=None):
            calls["api_key"] = api_key

        def fetch_filing_history(self, company_number, category="accounts", items=100):
            calls["company_number"] = company_number
            return {
                "items": [
                    {
                        "transaction_id": "tx1",
                        "links": {"document_metadata": "http://meta"},
                    }
                ]
            }

        def fetch_document_metadata(self, url):
            calls["meta_url"] = url
            return {"resources": {"application/xhtml+xml": {"url": "http://doc"}}}

        def fetch_document(self, url):
            calls["doc_url"] = url
            return b"<html>ixbrl</html>"

    monkeypatch.setattr(cli, "CompaniesHouseClient", FakeClient)
    monkeypatch.setattr(cli, "Config", lambda: SimpleNamespace(companies_house_api_key="KEY"))

    db_path = tmp_path / "ukfiling.db"
    mapper = UKSymbolMapRepository(db_path)
    mapper.initialize_schema()
    mapper.upsert_mapping("AAA.LSE", company_number="00000000")

    rc = cli.cmd_ingest_uk_filings("AAA.LSE", str(db_path))
    assert rc == 0

    assert calls["company_number"] == "00000000"
    assert calls["doc_url"] == "http://doc"

    repo = UKFilingRepository(db_path)
    repo.initialize_schema()
    latest = repo.latest_for_company("00000000")
    assert latest == b"<html>ixbrl</html>"

def test_cmd_update_market_data_bulk(monkeypatch, tmp_path):
    db_path = tmp_path / "marketbulk.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    listings = [
        Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
        Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
    ]
    universe_repo.replace_universe(listings, region="US")

    calls = []

    class DummyService:
        def __init__(self, db_path):
            self.db_path = db_path

        def refresh_symbol(self, symbol, fetch_symbol=None):
            calls.append(symbol)

    monkeypatch.setattr(cli, "MarketDataService", lambda db_path: DummyService(db_path))

    rc = cli.cmd_update_market_data_bulk(database=str(db_path), region="US", rate=0)
    assert rc == 0
    assert calls == ["AAA.US", "BBB.US"]

def test_cmd_compute_metrics_bulk(monkeypatch, tmp_path):
    db_path = tmp_path / "metricsbulk.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    listings = [
        Listing(symbol="AAA.US", security_name="AAA Inc", exchange="NYSE"),
        Listing(symbol="BBB.US", security_name="BBB Inc", exchange="NYSE"),
    ]
    universe_repo.replace_universe(listings, region="US")

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
            return MetricResult(symbol=symbol, metric_id=self.id, value=len(symbol), as_of="2024-01-01")

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})

    rc = cli.cmd_compute_metrics_bulk(database=str(db_path), region="US", metric_ids=None)
    assert rc == 0

    assert metrics_repo.fetch("AAA.US", "dummy_metric")[0] == len("AAA.US")
    assert metrics_repo.fetch("BBB.US", "dummy_metric")[0] == len("BBB.US")


def test_cmd_compute_metrics_bulk_fallback_to_fundamentals(monkeypatch, tmp_path):
    db_path = tmp_path / "fundmetrics.db"
    universe_repo = UniverseRepository(db_path)
    universe_repo.initialize_schema()
    # No listings stored; only fundamentals with region
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert("EODHD", "AAA.LSE", {"dummy": True}, region="UK")

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAA.LSE",
        [
            make_fact(symbol="AAA.LSE", concept="NetIncomeLoss", end_date="2023-12-31", value=10.0, provider="EODHD")
        ],
        provider="EODHD",
    )

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()

    class DummyMetric:
        id = "dummy_metric"
        required_concepts = ()
        uses_market_data = False

        def compute(self, symbol, repo):
            return MetricResult(symbol=symbol, metric_id=self.id, value=len(symbol), as_of="2024-01-01")

    monkeypatch.setattr(cli, "REGISTRY", {DummyMetric.id: DummyMetric})

    rc = cli.cmd_compute_metrics_bulk(database=str(db_path), region="UK", metric_ids=None)
    assert rc == 0
    assert metrics_repo.fetch("AAA.LSE", "dummy_metric")[0] == 7  # len("AAA.LSE")

def test_cmd_normalize_us_facts(monkeypatch, tmp_path):
    db_path = tmp_path / "facts.db"
    company_repo = CompanyFactsRepository(db_path)
    company_repo.initialize_schema()
    company_repo.upsert_company_facts("AAPL.US", "CIK0000320193", {"entityName": "Apple Inc", "facts": {}})

    class FakeNormalizer:
        def __init__(self):
            self.calls = []

        def normalize(self, payload, symbol, cik):
            self.calls.append((payload, symbol, cik))
            from pyvalue.storage import FactRecord

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

    rc = cli.cmd_normalize_us_facts("AAPL", str(db_path))
    assert rc == 0

    result_repo = FinancialFactsRepository(db_path)
    result_repo.initialize_schema()
    rows = result_repo._connect().execute(
        "SELECT concept, value FROM financial_facts WHERE symbol='AAPL.US'"
    ).fetchall()
    assert [(row[0], row[1]) for row in rows] == [("NetIncomeLoss", 123.0)]
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch("AAPL.US") == "Apple Inc"

def test_cmd_normalize_us_facts_bulk(monkeypatch, tmp_path):
    db_path = tmp_path / "facts.db"
    company_repo = CompanyFactsRepository(db_path)
    company_repo.initialize_schema()
    company_repo.upsert_company_facts("AAA.US", "CIK00001", {"entityName": "AAA Corp", "facts": {}})
    company_repo.upsert_company_facts("BBB.US", "CIK00002", {"entityName": "BBB Corp", "facts": {}})

    class DummyNormalizer:
        def normalize(self, payload, symbol, cik):
            return [
                make_fact(symbol=symbol, cik=cik, concept="Dummy", end_date="2023-12-31", value=len(symbol))
            ]

    normalization_repo = FinancialFactsRepository(db_path)
    normalization_repo.initialize_schema()

    normalizer = DummyNormalizer()
    monkeypatch.setattr(cli, "SECFactsNormalizer", lambda: normalizer)

    rc = cli.cmd_normalize_us_facts_bulk(database=str(db_path))
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


def test_cmd_normalize_eodhd_fundamentals(monkeypatch, tmp_path):
    db_path = tmp_path / "funds.db"
    fund_repo = FundamentalsRepository(db_path)
    fund_repo.initialize_schema()
    fund_repo.upsert(
        "EODHD",
        "SHEL",
        {"General": {"Name": "Shell PLC"}, "Financials": {}},
        region="UK",
    )

    class FakeNormalizer:
        def normalize(self, payload, symbol, accounting_standard=None):
            return [
                make_fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    end_date="2023-12-31",
                    value=10.0,
                    provider="EODHD",
                )
            ]

    monkeypatch.setattr(cli, "EODHDFactsNormalizer", lambda: FakeNormalizer())

    rc = cli.cmd_normalize_eodhd_fundamentals("SHEL", str(db_path))
    assert rc == 0

    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    rows = fact_repo._connect().execute(
        "SELECT provider, concept, value FROM financial_facts WHERE symbol='SHEL'"
    ).fetchall()
    assert [(row[0], row[1], row[2]) for row in rows] == [("EODHD", "NetIncomeLoss", 10.0)]
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    assert entity_repo.fetch("SHEL") == "Shell PLC"

def test_cmd_recalc_market_cap(tmp_path):
    db_path = tmp_path / "marketcap.db"
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

    rc = cli.cmd_recalc_market_cap(database=str(db_path))
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
        ],
        region="US",
    )
    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    metrics_repo.upsert("AAA.US", "working_capital", 100.0, "2023-12-31")
    metrics_repo.upsert("BBB.US", "working_capital", 50.0, "2023-12-31")
    entity_repo = EntityMetadataRepository(db_path)
    entity_repo.initialize_schema()
    entity_repo.upsert("AAA.US", "AAA Inc")
    entity_repo.upsert("BBB.US", "BBB Inc")

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
        region="US",
        output_csv=str(csv_path),
    )
    assert rc == 0
    output = capsys.readouterr().out
    assert "AAA.US" in output
    assert "BBB.US" not in output
    csv_contents = csv_path.read_text().strip().splitlines()
    assert csv_contents[0] == "Criterion,AAA.US"
    assert csv_contents[1].startswith("Entity,AAA Inc")

def test_cmd_compute_metrics(tmp_path):
    db_path = tmp_path / "facts.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAPL.US",
        [
            make_fact(concept="AssetsCurrent", end_date="2023-09-30", value=500),
            make_fact(concept="LiabilitiesCurrent", end_date="2023-09-30", value=200),
            make_fact(concept="EarningsPerShareDiluted", end_date="2023-12-31", value=2.5, fiscal_period="Q4"),
            make_fact(concept="EarningsPerShareDiluted", end_date="2023-09-30", value=2.0, fiscal_period="Q3"),
            make_fact(concept="EarningsPerShareDiluted", end_date="2023-06-30", value=1.5, fiscal_period="Q2"),
            make_fact(concept="EarningsPerShareDiluted", end_date="2023-03-31", value=1.0, fiscal_period="Q1"),
            make_fact(concept="StockholdersEquity", end_date="2023-09-30", value=1000),
            make_fact(concept="CommonStockSharesOutstanding", end_date="2023-09-30", value=100),
            make_fact(concept="Goodwill", end_date="2023-09-30", value=50),
            make_fact(concept="IntangibleAssetsNet", end_date="2023-09-30", value=25),
        ],
    )
    repo = MetricsRepository(db_path)
    repo.initialize_schema()
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAPL.US", "2023-09-30", 150.0)

    rc = cli.cmd_compute_metrics("AAPL.US", ["working_capital", "graham_multiplier"], str(db_path), run_all=False)
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
    for year in range(2010, 2025):
        frame = f"CY{year}"
        records.append(
            make_fact(
                concept="EarningsPerShareDiluted",
                end_date=f"{year}-09-30",
                value=2.0 + 0.1 * (year - 2010),
                frame=frame,
                fiscal_period="FY",
            )
        )
    for year in range(2015, 2025):
        end_date = f"{year}-09-30"
        records.extend(
            [
                make_fact(concept="AssetsCurrent", end_date=end_date, value=400 + year),
                make_fact(concept="LiabilitiesCurrent", end_date=end_date, value=200 + year),
                make_fact(concept="OperatingIncomeLoss", end_date=end_date, value=150 + year),
                make_fact(
                    concept="PropertyPlantAndEquipmentNet",
                    end_date=end_date,
                    value=500 + year,
                ),
            ]
        )
    records.extend(
            [
                make_fact(concept="StockholdersEquity", end_date="2024-09-30", value=2000),
                make_fact(concept="StockholdersEquity", end_date="2023-09-30", value=1800),
                make_fact(concept="StockholdersEquity", end_date="2022-09-30", value=1600),
                make_fact(concept="StockholdersEquity", end_date="2021-09-30", value=1400),
                make_fact(concept="StockholdersEquity", end_date="2020-09-30", value=1200),
                make_fact(concept="StockholdersEquity", end_date="2019-09-30", value=1000),
                make_fact(concept="NetIncomeLossAvailableToCommonStockholdersBasic", end_date="2024-09-30", value=250),
                make_fact(concept="NetIncomeLossAvailableToCommonStockholdersBasic", end_date="2023-09-30", value=230),
                make_fact(concept="NetIncomeLossAvailableToCommonStockholdersBasic", end_date="2022-09-30", value=210),
                make_fact(concept="NetIncomeLossAvailableToCommonStockholdersBasic", end_date="2021-09-30", value=190),
                make_fact(concept="NetIncomeLossAvailableToCommonStockholdersBasic", end_date="2020-09-30", value=170),
                make_fact(concept="PreferredStock", end_date="2024-09-30", value=0),
                make_fact(concept="CommonStockSharesOutstanding", end_date="2024-09-30", value=500),
                make_fact(concept="Goodwill", end_date="2024-09-30", value=100),
                make_fact(concept="IntangibleAssetsNet", end_date="2024-09-30", value=50),
                make_fact(concept="LongTermDebtNoncurrent", end_date="2024-09-30", value=300),
            ]
    )
    quarterly_cash_flows = [
        ("2024-12-31", "Q4", 130.0, 40.0),
        ("2024-09-30", "Q3", 120.0, 35.0),
        ("2024-06-30", "Q2", 110.0, 30.0),
        ("2024-03-31", "Q1", 100.0, 25.0),
        ("2023-12-31", "Q4", 90.0, 20.0),
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
                concept="PaymentsToAcquirePropertyPlantAndEquipment",
                end_date=end_date,
                fiscal_period=period,
                value=capex,
            )
        )
    quarterly_eps = [
        ("2024-12-31", "Q4", 2.5),
        ("2024-09-30", "Q3", 2.0),
        ("2024-06-30", "Q2", 1.5),
        ("2024-03-31", "Q1", 1.0),
        ("2023-12-31", "Q4", 0.5),
    ]
    for end_date, period, value in quarterly_eps:
        records.append(
            make_fact(
                concept="EarningsPerShareDiluted",
                end_date=end_date,
                fiscal_period=period,
                value=value,
                frame=f"CY{end_date[:4]}{period}",
            )
        )
    fact_repo.replace_facts("AAPL.US", records)

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAPL.US", "2024-09-30", 150.0, market_cap=50000.0)
    market_repo.upsert_price("AAPL.US", "2019-09-30", 100.0, market_cap=30000.0)

    rc = cli.cmd_compute_metrics("AAPL.US", ["placeholder"], str(db_path), run_all=True)
    assert rc == 0
    for metric_id in REGISTRY.keys():
        row = metrics_repo.fetch("AAPL.US", metric_id)
        assert row is not None, f"{metric_id} missing"
