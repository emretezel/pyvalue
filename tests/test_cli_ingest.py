# Author: Emre Tezel
from types import SimpleNamespace

from pyvalue import cli
from pyvalue.storage import CompanyFactsRepository


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
