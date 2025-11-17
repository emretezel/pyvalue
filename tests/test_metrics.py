# Author: Emre Tezel
from pyvalue.metrics.working_capital import WorkingCapitalMetric
from pyvalue.metrics.eps_streak import EPSStreakMetric
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


def test_eps_streak_counts_consecutive_positive_years():
    metric = EPSStreakMetric()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShareDiluted":
                return [
                    FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 2.0, None, None, "CY2023"),
                    FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 2.1, None, None, "CY2023Q4"),
                    FactRecord(symbol, "CIK", concept, 2022, "FY", "2022-09-30", "USD", 1.5, None, None, "CY2022"),
                    FactRecord(symbol, "CIK", concept, 2021, "FY", "2021-09-30", "USD", -0.5, None, None, "CY2021"),
                ]
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL", repo)
    assert result is not None
    assert result.value == 2
    assert result.as_of == "2023-09-30"
