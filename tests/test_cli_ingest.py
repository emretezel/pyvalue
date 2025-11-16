# Author: Emre Tezel
from types import SimpleNamespace

from pyvalue import cli
from pyvalue.storage import CompanyFactsRepository, FinancialFactsRepository, FactRecord, MetricsRepository


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
            FactRecord(
                symbol="AAPL",
                cik="CIK",
                concept="AssetsCurrent",
                fiscal_year=2023,
                fiscal_period="FY",
                end_date="2023-09-30",
                unit="USD",
                value=500,
                accn=None,
                filed=None,
                frame=None,
            ),
            FactRecord(
                symbol="AAPL",
                cik="CIK",
                concept="LiabilitiesCurrent",
                fiscal_year=2023,
                fiscal_period="FY",
                end_date="2023-09-30",
                unit="USD",
                value=200,
                accn=None,
                filed=None,
                frame=None,
            ),
        ],
    )
    repo = MetricsRepository(db_path)
    repo.initialize_schema()

    rc = cli.cmd_compute_metrics("AAPL", ["working_capital"], str(db_path))
    assert rc == 0
    value = repo.fetch("AAPL", "working_capital")
    assert value[0] == 300
