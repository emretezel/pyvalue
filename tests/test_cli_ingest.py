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
                FactRecord(
                    symbol=symbol,
                    cik=cik,
                    concept="NetIncomeLoss",
                    fiscal_year=2023,
                    fiscal_period="FY",
                    end_date="2023-09-30",
                    unit="USD",
                    value=123.0,
                    accn=None,
                    filed=None,
                    frame=None,
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
            FactRecord("AAPL", "CIK", "AssetsCurrent", 2023, "FY", "2023-09-30", "USD", 500, None, None, None),
            FactRecord("AAPL", "CIK", "LiabilitiesCurrent", 2023, "FY", "2023-09-30", "USD", 200, None, None, None),
            FactRecord("AAPL", "CIK", "EarningsPerShareDiluted", 2023, "FY", "2023-09-30", "USD", 5.0, None, None, "CY2023"),
            FactRecord("AAPL", "CIK", "StockholdersEquity", 2023, "FY", "2023-09-30", "USD", 1000, None, None, None),
            FactRecord("AAPL", "CIK", "CommonStockSharesOutstanding", 2023, "FY", "2023-09-30", "USD", 100, None, None, None),
            FactRecord("AAPL", "CIK", "Goodwill", 2023, "FY", "2023-09-30", "USD", 50, None, None, None),
            FactRecord("AAPL", "CIK", "IntangibleAssetsNet", 2023, "FY", "2023-09-30", "USD", 25, None, None, None),
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
            FactRecord(
                symbol="AAPL",
                cik="CIK",
                concept="EarningsPerShareDiluted",
                fiscal_year=year,
                fiscal_period="FY",
                end_date=f"{year}-09-30",
                unit="USD",
                value=2.0 + 0.1 * (year - 2010),
                accn=None,
                filed=None,
                frame=frame,
            )
        )
    for year in range(2020, 2025):
        end_date = f"{year}-09-30"
        records.extend(
            [
                FactRecord("AAPL", "CIK", "AssetsCurrent", year, "FY", end_date, "USD", 400 + year, None, None, None),
                FactRecord("AAPL", "CIK", "LiabilitiesCurrent", year, "FY", end_date, "USD", 200 + year, None, None, None),
                FactRecord("AAPL", "CIK", "OperatingIncomeLoss", year, "FY", end_date, "USD", 150 + year, None, None, None),
                FactRecord(
                    "AAPL",
                    "CIK",
                    "PropertyPlantAndEquipmentNet",
                    year,
                    "FY",
                    end_date,
                    "USD",
                    500 + year,
                    None,
                    None,
                    None,
                ),
            ]
        )
    records.extend(
        [
            FactRecord("AAPL", "CIK", "StockholdersEquity", 2024, "FY", "2024-09-30", "USD", 2000, None, None, None),
            FactRecord("AAPL", "CIK", "CommonStockSharesOutstanding", 2024, "FY", "2024-09-30", "USD", 500, None, None, None),
            FactRecord("AAPL", "CIK", "Goodwill", 2024, "FY", "2024-09-30", "USD", 100, None, None, None),
            FactRecord("AAPL", "CIK", "IntangibleAssetsNet", 2024, "FY", "2024-09-30", "USD", 50, None, None, None),
            FactRecord("AAPL", "CIK", "LongTermDebtNoncurrent", 2024, "FY", "2024-09-30", "USD", 300, None, None, None),
        ]
    )
    fact_repo.replace_facts("AAPL", records)

    metrics_repo = MetricsRepository(db_path)
    metrics_repo.initialize_schema()
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price("AAPL", "2024-09-30", 150.0)

    rc = cli.cmd_compute_metrics("AAPL", ["placeholder"], str(db_path), run_all=True)
    assert rc == 0
    for metric_id in REGISTRY.keys():
        row = metrics_repo.fetch("AAPL", metric_id)
        assert row is not None, f"{metric_id} missing"
