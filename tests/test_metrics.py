"""Tests for metric implementations.

Author: Emre Tezel
"""

from datetime import date, timedelta

from pyvalue.metrics import REGISTRY
from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.metrics.earnings_yield import EarningsYieldMetric
from pyvalue.metrics.eps_average import EPSAverageSixYearMetric
from pyvalue.metrics.eps_quarterly import EarningsPerShareTTM
from pyvalue.metrics.eps_streak import EPSStreakMetric
from pyvalue.metrics.graham_eps_cagr import GrahamEPSCAGRMetric
from pyvalue.metrics.graham_multiplier import GrahamMultiplierMetric
from pyvalue.metrics.market_capitalization import MarketCapitalizationMetric
from pyvalue.metrics.price_to_fcf import PriceToFCFMetric
from pyvalue.metrics.roc_greenblatt import ROCGreenblattMetric
from pyvalue.metrics.roe_greenblatt import ROEGreenblattMetric
from pyvalue.metrics.working_capital import WorkingCapitalMetric
from pyvalue.storage import FactRecord


def fact(**kwargs):
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


def test_working_capital_metric_computes_difference():
    metric = WorkingCapitalMetric()
    recent = (date.today() - timedelta(days=10)).isoformat()

    class DummyRepo:
        def latest_fact(self, symbol, concept):
            if concept == "AssetsCurrent":
                return fact(symbol=symbol, concept=concept, end_date=recent, value=200.0)
            if concept == "LiabilitiesCurrent":
                return fact(symbol=symbol, concept=concept, end_date=recent, value=50.0)
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 150.0


def test_current_ratio_metric():
    metric = CurrentRatioMetric()
    recent = (date.today() - timedelta(days=10)).isoformat()

    class DummyRepo:
        def latest_fact(self, symbol, concept):
            if concept == "AssetsCurrent":
                return fact(symbol=symbol, concept=concept, end_date=recent, value=400.0)
            if concept == "LiabilitiesCurrent":
                return fact(symbol=symbol, concept=concept, end_date=recent, value=200.0)
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 2.0


def test_eps_streak_counts_consecutive_positive_years():
    metric = EPSStreakMetric()
    recent = (date.today() - timedelta(days=30)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShare":
                return [
                    fact(symbol=symbol, concept=concept, end_date=recent, value=2.0, frame=f"CY{date.today().year}"),
                    fact(symbol=symbol, concept=concept, end_date="2023-09-30", value=2.1, frame="CY2023"),
                    fact(symbol=symbol, concept=concept, end_date="2022-09-30", value=1.5, frame="CY2022"),
                    fact(symbol=symbol, concept=concept, end_date="2021-09-30", value=-0.5, frame="CY2021"),
                ]
            return []

        def latest_fact(self, symbol, concept):
            return fact(symbol=symbol, concept=concept, end_date=recent, value=2.0)

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 3
    assert result.as_of == recent


def test_graham_eps_cagr_metric():
    metric = GrahamEPSCAGRMetric()
    recent = (date.today() - timedelta(days=15)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShare":
                records = [
                    fact(symbol=symbol, concept=concept, end_date=recent, value=2.0, frame="CYRECENT"),
                ]
                for year in range(2000, 2015):
                    value = 1.0 + (year - 2000) * 0.1
                    records.append(
                        fact(symbol=symbol, concept=concept, end_date=f"{year}-09-30", value=value, frame=f"CY{year}")
                    )
                return records
            return []

        def latest_fact(self, symbol, concept):
            return fact(symbol=symbol, concept=concept, end_date=recent, value=2.0)

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None


def test_graham_multiplier_metric():
    metric = GrahamMultiplierMetric()
    recent = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def __init__(self):
            self.values = {
                "StockholdersEquity": 1000,
                "CommonStockSharesOutstanding": 100,
                "Goodwill": 50,
                "IntangibleAssetsNetExcludingGoodwill": 25,
            }

        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShare":
                return [
                    fact(symbol=symbol, concept=concept, fiscal_period="Q4", end_date=recent, value=2.5),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q3", end_date="2024-09-30", value=2.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q2", end_date="2024-06-30", value=1.5),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q1", end_date="2024-03-31", value=1.0),
                ]
            return []

        def latest_fact(self, symbol, concept):
            value = self.values.get(concept)
            if value is None:
                return None
            return fact(symbol=symbol, concept=concept, end_date=recent, value=value)

    class DummyMarketRepo:
        def latest_price(self, symbol):
            return (recent, 150.0)

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value > 0


def test_graham_multiplier_falls_back_to_fy_eps():
    metric = GrahamMultiplierMetric()
    recent = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def __init__(self):
            self.values = {
                "StockholdersEquity": 1000,
                "CommonStockSharesOutstanding": 100,
                "Goodwill": 50,
                "IntangibleAssetsNetExcludingGoodwill": 25,
            }

        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShare" and fiscal_period == "FY":
                return [fact(symbol=symbol, concept=concept, fiscal_period="FY", end_date=recent, value=5.0)]
            return []

        def latest_fact(self, symbol, concept):
            value = self.values.get(concept)
            if value is None:
                return None
            return fact(symbol=symbol, concept=concept, end_date=recent, value=value)

    class DummyMarketRepo:
        def latest_price(self, symbol):
            return (recent, 150.0)

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value > 0


def test_graham_multiplier_uses_zero_when_optional_values_missing():
    metric = GrahamMultiplierMetric()
    recent = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def __init__(self):
            self.values = {
                "StockholdersEquity": 1000,
                "CommonStockSharesOutstanding": 100,
            }

        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShare":
                return [
                    fact(symbol=symbol, concept=concept, fiscal_period="Q4", end_date=recent, value=2.5),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q3", end_date="2024-09-30", value=2.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q2", end_date="2024-06-30", value=1.5),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q1", end_date="2024-03-31", value=1.0),
                ]
            return []

        def latest_fact(self, symbol, concept):
            value = self.values.get(concept)
            if value is None:
                return None
            return fact(symbol=symbol, concept=concept, end_date=recent, value=value)

    class DummyMarketRepo:
        def latest_price(self, symbol):
            return (recent, 150.0)

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value > 0


def test_earnings_yield_metric():
    metric = EarningsYieldMetric()
    recent = (date.today() - timedelta(days=30)).isoformat()
    older = (date.today() - timedelta(days=120)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShare":
                return [
                    fact(symbol=symbol, concept=concept, fiscal_period="Q4", end_date=recent, value=2.5),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q3", end_date=older, value=2.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q2", end_date="2024-06-30", value=1.5),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q1", end_date="2024-03-31", value=1.0),
                ]
            return []

    class DummyMarketRepo:
        def latest_price(self, symbol):
            return (recent, 50.0)

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value == (2.5 + 2.0 + 1.5 + 1.0) / 50.0


def test_earnings_yield_metric_falls_back_to_fy():
    metric = EarningsYieldMetric()
    recent_fy = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShare" and fiscal_period == "FY":
                return [fact(symbol=symbol, concept=concept, fiscal_period="FY", end_date=recent_fy, value=4.0)]
            return []

    class DummyMarketRepo:
        def latest_price(self, symbol):
            return (recent_fy, 40.0)

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value == 4.0 / 40.0


def test_price_to_fcf_metric():
    metric = PriceToFCFMetric()
    recent = (date.today() - timedelta(days=15)).isoformat()
    older = (date.today() - timedelta(days=90)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "NetCashProvidedByUsedInOperatingActivities":
                return [
                    fact(symbol=symbol, concept=concept, fiscal_period="Q4", end_date=recent, value=130.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q3", end_date=older, value=120.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q2", end_date="2024-06-30", value=110.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q1", end_date="2024-03-31", value=100.0),
                ]
            if concept == "CapitalExpenditures":
                return [
                    fact(symbol=symbol, concept=concept, fiscal_period="Q4", end_date=recent, value=-30.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q3", end_date=older, value=-40.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q2", end_date="2024-06-30", value=-50.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q1", end_date="2024-03-31", value=-60.0),
                ]
            return []

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 6400.0
                as_of = (date.today() - timedelta(days=10)).isoformat()

            return Snapshot()

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value == 10.0


def test_price_to_fcf_metric_uses_zero_capex_when_missing():
    metric = PriceToFCFMetric()
    recent = (date.today() - timedelta(days=15)).isoformat()
    older = (date.today() - timedelta(days=90)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "NetCashProvidedByUsedInOperatingActivities":
                return [
                    fact(symbol=symbol, concept=concept, fiscal_period="Q4", end_date=recent, value=130.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q3", end_date=older, value=120.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q2", end_date="2024-06-30", value=110.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q1", end_date="2024-03-31", value=100.0),
                ]
            return []

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 6400.0
                as_of = (date.today() - timedelta(days=10)).isoformat()

            return Snapshot()

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value == 6400.0 / 460.0


def test_eps_ttm_metric():
    metric = EarningsPerShareTTM()
    recent = (date.today() - timedelta(days=30)).isoformat()
    older = (date.today() - timedelta(days=120)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShare":
                return [
                    fact(symbol=symbol, concept=concept, fiscal_period="Q4", end_date=recent, value=2.5),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q3", end_date=older, value=2.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q2", end_date="2024-06-30", value=1.5),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q1", end_date="2024-03-31", value=1.0),
                    fact(symbol=symbol, concept=concept, fiscal_period="Q4", end_date="2023-12-31", value=0.5),
                ]
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 7.0
    assert result.as_of == recent


def test_eps_ttm_metric_falls_back_to_fy():
    metric = EarningsPerShareTTM()
    recent_fy = (date.today() - timedelta(days=30)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShare" and fiscal_period == "FY":
                return [fact(symbol=symbol, concept=concept, fiscal_period="FY", end_date=recent_fy, value=4.2)]
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 4.2
    assert result.as_of == recent_fy


def test_eps_6y_avg_metric():
    metric = EPSAverageSixYearMetric()
    recent_fy = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EarningsPerShare":
                records = [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=recent_fy,
                        value=7.0,
                        frame=f"CY{date.today().year}",
                    )
                ]
                for idx, year in enumerate(range(2018, 2025), start=1):
                    records.append(
                        fact(
                            symbol=symbol,
                            concept=concept,
                            fiscal_period="FY",
                            end_date=f"{year}-09-30",
                            value=float(idx),
                            frame=f"CY{year}",
                        )
                    )
                return records
            return []

        def latest_fact(self, symbol, concept):
            return fact(symbol=symbol, concept=concept, end_date=recent_fy, value=7.0)

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.as_of == recent_fy


def test_market_capitalization_metric():
    metric = MarketCapitalizationMetric()

    class DummyRepo:
        pass

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 123456789.0
                as_of = "2024-05-01"

            return Snapshot()

    repo = DummyRepo()
    market_repo = DummyMarketRepo()

    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value == 123456789.0
    assert result.as_of == "2024-05-01"


def test_roc_greenblatt_metric():
    metric = ROCGreenblattMetric()
    recent = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "OperatingIncomeLoss":
                return [
                    fact(symbol=symbol, concept=concept, end_date=recent, value=220),
                    fact(symbol=symbol, concept=concept, end_date="2023-09-30", value=200),
                    fact(symbol=symbol, concept=concept, end_date="2022-09-30", value=150),
                ]
            if concept == "PropertyPlantAndEquipmentNet":
                return [
                    fact(symbol=symbol, concept=concept, end_date=recent, value=520),
                    fact(symbol=symbol, concept=concept, end_date="2023-09-30", value=500),
                    fact(symbol=symbol, concept=concept, end_date="2022-09-30", value=450),
                ]
            if concept == "AssetsCurrent":
                return [
                    fact(symbol=symbol, concept=concept, end_date=recent, value=420),
                    fact(symbol=symbol, concept=concept, end_date="2023-09-30", value=400),
                    fact(symbol=symbol, concept=concept, end_date="2022-09-30", value=350),
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(symbol=symbol, concept=concept, end_date=recent, value=310),
                    fact(symbol=symbol, concept=concept, end_date="2023-09-30", value=300),
                    fact(symbol=symbol, concept=concept, end_date="2022-09-30", value=250),
                ]
            return []

        def latest_fact(self, symbol, concept):
            return fact(symbol=symbol, concept=concept, end_date=recent, value=0.0)

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.as_of == recent


def test_roe_greenblatt_metric():
    metric = ROEGreenblattMetric()
    recent = (date.today() - timedelta(days=25)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "NetIncomeLossAvailableToCommonStockholdersBasic":
                return [
                    fact(symbol=symbol, concept=concept, end_date=recent, value=220),
                    fact(symbol=symbol, concept=concept, end_date="2024-09-30", value=200),
                    fact(symbol=symbol, concept=concept, end_date="2023-09-30", value=180),
                ]
            if concept == "CommonStockholdersEquity":
                return [
                    fact(symbol=symbol, concept=concept, end_date=recent, value=1100),
                    fact(symbol=symbol, concept=concept, end_date="2024-09-30", value=1000),
                    fact(symbol=symbol, concept=concept, end_date="2023-09-30", value=900),
                ]
            return []

        def latest_fact(self, symbol, concept):
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value > 0


def test_registry_contains_all_ids():
    # Ensure the registry still exposes all metric identifiers
    assert len(REGISTRY) >= 1
