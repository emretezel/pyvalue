# Author: Emre Tezel
from pyvalue.metrics.working_capital import WorkingCapitalMetric
from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.metrics.eps_streak import EPSStreakMetric
from pyvalue.metrics.graham_eps_cagr import GrahamEPSCAGRMetric
from pyvalue.metrics.graham_multiplier import GrahamMultiplierMetric
from pyvalue.metrics.earnings_yield import EarningsYieldMetric
from pyvalue.metrics.roc_greenblatt import ROCGreenblattMetric
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


def test_current_ratio_metric(monkeypatch):
    metric = CurrentRatioMetric()

    class DummyRepo:
        def latest_fact(self, symbol, concept):
            if concept == "AssetsCurrent":
                return FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 400.0, None, None, None)
            if concept == "LiabilitiesCurrent":
                return FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 200.0, None, None, None)
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL", repo)
    assert result is not None
    assert result.value == 2.0


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

def test_graham_eps_cagr_metric(monkeypatch):
    metric = GrahamEPSCAGRMetric()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShareDiluted":
                records = []
                for year in range(2000, 2015):
                    value = 1.0 + (year - 2000) * 0.1
                    records.append(
                        FactRecord(
                            symbol,
                            "CIK",
                            concept,
                            year,
                            "FY",
                            f"{year}-09-30",
                            "USD",
                            value,
                            None,
                            None,
                            "CY" + str(year),
                        )
                    )
                return records
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL", repo)
    assert result is not None
    assert result.value > 0

def test_graham_multiplier_metric(monkeypatch):
    metric = GrahamMultiplierMetric()

    class DummyRepo:
        def __init__(self):
            self.values = {
                "StockholdersEquity": 1000,
                "CommonStockSharesOutstanding": 100,
                "Goodwill": 50,
                "IntangibleAssetsNet": 25,
            }

        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShareDiluted":
                return [
                    FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 5.0, None, None, "CY2023")
                ]
            return []

        def latest_fact(self, symbol, concept):
            value = self.values.get(concept)
            if value is None:
                return None
            return FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", value, None, None, None)

    class DummyMarketRepo:
        def latest_price(self, symbol):
            return ("2023-09-30", 150.0)

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL", repo, market_repo)
    assert result is not None
    assert result.value > 0

def test_earnings_yield_metric(monkeypatch):
    metric = EarningsYieldMetric()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShareDiluted":
                return [FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 5.0, None, None, "CY2023")]
            return []

    class DummyMarketRepo:
        def latest_price(self, symbol):
            return ("2023-09-30", 50.0)

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL", repo, market_repo)
    assert result is not None
    assert result.value == 0.1

def test_roc_greenblatt_metric(monkeypatch):
    metric = ROCGreenblattMetric()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "OperatingIncomeLoss":
                return [
                    FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 200, None, None, None),
                    FactRecord(symbol, "CIK", concept, 2022, "FY", "2022-09-30", "USD", 150, None, None, None),
                ]
            if concept == "PropertyPlantAndEquipmentNet":
                return [
                    FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 500, None, None, None),
                    FactRecord(symbol, "CIK", concept, 2022, "FY", "2022-09-30", "USD", 450, None, None, None),
                ]
            if concept == "AssetsCurrent":
                return [
                    FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 400, None, None, None),
                    FactRecord(symbol, "CIK", concept, 2022, "FY", "2022-09-30", "USD", 350, None, None, None),
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    FactRecord(symbol, "CIK", concept, 2023, "FY", "2023-09-30", "USD", 300, None, None, None),
                    FactRecord(symbol, "CIK", concept, 2022, "FY", "2022-09-30", "USD", 250, None, None, None),
                ]
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL", repo)
    assert result is not None
    assert result.value > 0
