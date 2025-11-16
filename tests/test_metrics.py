# Author: Emre Tezel
from pyvalue.metrics.working_capital import WorkingCapitalMetric
from pyvalue.storage import FactRecord


def test_working_capital_metric_computes_difference(monkeypatch):
    metric = WorkingCapitalMetric()

    class DummyRepo:
        def __init__(self):
            self.calls = []

        def latest_fact(self, symbol, concept):
            if concept == "AssetsCurrent":
                return FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 200.0, None, None, None)
            if concept == "LiabilitiesCurrent":
                return FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 50.0, None, None, None)
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL", repo)

    assert result is not None
    assert result.value == 150.0
    assert result.as_of == "2023-09-30"
