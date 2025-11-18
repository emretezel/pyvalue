"""Tests for CLI ingestion and metric commands.

Author: Emre Tezel
"""
from types import SimpleNamespace

from pyvalue import cli
from pyvalue.metrics import REGISTRY
from pyvalue.storage import (
    CompanyFactsRepository,
    FinancialFactsRepository,
    FactRecord,
    MarketDataRepository,
    MetricsRepository,
)


def make_fact(**kwargs):
    base = {
        "symbol": "AAPL",
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
    stored = repo.fetch_fact("AAPL")
    assert stored["cik"] == "CIK0000320193"

def test_cmd_normalize_us_facts(monkeypatch, tmp_path):
    db_path = tmp_path / "facts.db"
    company_repo = CompanyFactsRepository(db_path)
    company_repo.initialize_schema()
    company_repo.upsert_company_facts("AAPL", "CIK0000320193", {"facts": {}})

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

    fact_repo = CompanyFactsRepository(db_path)
    result_repo = FinancialFactsRepository(db_path)
    result_repo.initialize_schema()
    rows = result_repo._connect().execute(
        "SELECT concept, value FROM financial_facts WHERE symbol='AAPL'"
    ).fetchall()
    assert [(row[0], row[1]) for row in rows] == [("NetIncomeLoss", 123.0)]

def test_cmd_compute_metrics(tmp_path):
    db_path = tmp_path / "facts.db"
    fact_repo = FinancialFactsRepository(db_path)
    fact_repo.initialize_schema()
    fact_repo.replace_facts(
        "AAPL",
        [
            make_fact(concept="AssetsCurrent", end_date="2023-09-30", value=500),
            make_fact(concept="LiabilitiesCurrent", end_date="2023-09-30", value=200),
            make_fact(concept="EarningsPerShareDiluted", end_date="2023-09-30", value=5.0, frame="CY2023"),
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
    market_repo.upsert_price("AAPL", "2023-09-30", 150.0)

    rc = cli.cmd_compute_metrics("AAPL", ["working_capital", "graham_multiplier"], str(db_path), run_all=False)
    assert rc == 0
    value = repo.fetch("AAPL", "working_capital")
    assert value[0] == 300
    graham_value = repo.fetch("AAPL", "graham_multiplier")
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
    fact_repo.replace_facts("AAPL", records)

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAPL", "2024-09-30", 150.0, market_cap=50000.0)
    market_repo.upsert_price("AAPL", "2019-09-30", 100.0, market_cap=30000.0)

    rc = cli.cmd_compute_metrics("AAPL", ["placeholder"], str(db_path), run_all=True)
    assert rc == 0
    for metric_id in REGISTRY.keys():
        row = metrics_repo.fetch("AAPL", metric_id)
        assert row is not None, f"{metric_id} missing"
