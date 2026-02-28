"""Tests for metric implementations.

Author: Emre Tezel
"""

from datetime import date, timedelta

from pyvalue.metrics import REGISTRY
from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.metrics.debt_paydown_years import DebtPaydownYearsMetric
from pyvalue.metrics.earnings_yield import EarningsYieldMetric
from pyvalue.metrics.eps_average import EPSAverageSixYearMetric
from pyvalue.metrics.eps_quarterly import EarningsPerShareTTM
from pyvalue.metrics.eps_streak import EPSStreakMetric
from pyvalue.metrics.graham_eps_cagr import GrahamEPSCAGRMetric
from pyvalue.metrics.graham_multiplier import GrahamMultiplierMetric
from pyvalue.metrics.interest_coverage import InterestCoverageMetric
from pyvalue.metrics.market_capitalization import MarketCapitalizationMetric
from pyvalue.metrics.mcapex import (
    MCapexFYMetric,
    MCapexFiveYearMetric,
    MCapexTTMMetric,
)
from pyvalue.metrics.net_debt_to_ebitda import NetDebtToEBITDAMetric
from pyvalue.metrics.nwc import (
    DeltaNWCFYMetric,
    DeltaNWCMaintMetric,
    DeltaNWCTTMMetric,
    NWCFYMetric,
    NWCMostRecentQuarterMetric,
)
from pyvalue.metrics.owner_earnings_equity import (
    OwnerEarningsEquityFiveYearAverageMetric,
    OwnerEarningsEquityTTMMetric,
)
from pyvalue.metrics.owner_earnings_yield import (
    OwnerEarningsYieldEquityFiveYearMetric,
    OwnerEarningsYieldEquityMetric,
)
from pyvalue.metrics.price_to_fcf import PriceToFCFMetric
from pyvalue.metrics.roc_greenblatt import ROCGreenblattMetric
from pyvalue.metrics.roe_greenblatt import ROEGreenblattMetric
from pyvalue.metrics.return_on_invested_capital import ReturnOnInvestedCapitalMetric
from pyvalue.metrics.short_term_debt_share import ShortTermDebtShareMetric
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
                return fact(
                    symbol=symbol, concept=concept, end_date=recent, value=200.0
                )
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
                return fact(
                    symbol=symbol, concept=concept, end_date=recent, value=400.0
                )
            if concept == "LiabilitiesCurrent":
                return fact(
                    symbol=symbol, concept=concept, end_date=recent, value=200.0
                )
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
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date=recent,
                        value=2.0,
                        frame=f"CY{date.today().year}",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2023-09-30",
                        value=2.1,
                        frame="CY2023",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2022-09-30",
                        value=1.5,
                        frame="CY2022",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2021-09-30",
                        value=-0.5,
                        frame="CY2021",
                    ),
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
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date=recent,
                        value=2.0,
                        frame="CYRECENT",
                    ),
                ]
                for year in range(2000, 2015):
                    value = 1.0 + (year - 2000) * 0.1
                    records.append(
                        fact(
                            symbol=symbol,
                            concept=concept,
                            end_date=f"{year}-09-30",
                            value=value,
                            frame=f"CY{year}",
                        )
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
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=recent,
                        value=2.5,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date="2024-09-30",
                        value=2.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date="2024-06-30",
                        value=1.5,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date="2024-03-31",
                        value=1.0,
                    ),
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
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=recent,
                        value=5.0,
                    )
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


def test_net_debt_to_ebitda_metric():
    metric = NetDebtToEBITDAMetric()
    today = date.today()
    q4 = (today - timedelta(days=30)).isoformat()
    q3 = (today - timedelta(days=120)).isoformat()
    q2 = (today - timedelta(days=210)).isoformat()
    q1 = (today - timedelta(days=300)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EBITDA":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=40.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=30.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=20.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=10.0,
                        currency="USD",
                    ),
                ]
            return []

        def latest_fact(self, symbol, concept):
            if concept == "ShortTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=q4,
                    value=10.0,
                    currency="USD",
                )
            if concept == "LongTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=q4,
                    value=90.0,
                    currency="USD",
                )
            if concept == "CashAndShortTermInvestments":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=q4,
                    value=20.0,
                    currency="USD",
                )
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 0.8


def test_debt_paydown_years_metric():
    metric = DebtPaydownYearsMetric()
    today = date.today()
    q4 = (today - timedelta(days=30)).isoformat()
    q3 = (today - timedelta(days=120)).isoformat()
    q2 = (today - timedelta(days=210)).isoformat()
    q1 = (today - timedelta(days=300)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "NetCashProvidedByUsedInOperatingActivities":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=100.0,
                        currency="USD",
                    ),
                ]
            if concept == "CapitalExpenditures":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=50.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=50.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=50.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=50.0,
                        currency="USD",
                    ),
                ]
            return []

        def latest_fact(self, symbol, concept):
            if concept == "ShortTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=q4,
                    value=50.0,
                    currency="USD",
                )
            if concept == "LongTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=q4,
                    value=150.0,
                    currency="USD",
                )
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 1.0


def test_short_term_debt_share_metric():
    metric = ShortTermDebtShareMetric()
    recent = (date.today() - timedelta(days=10)).isoformat()

    class DummyRepo:
        def latest_fact(self, symbol, concept):
            if concept == "ShortTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=25.0,
                )
            if concept == "LongTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=75.0,
                )
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 0.25


def test_short_term_debt_share_skips_non_positive_total():
    metric = ShortTermDebtShareMetric()
    recent = (date.today() - timedelta(days=10)).isoformat()

    class DummyRepo:
        def latest_fact(self, symbol, concept):
            if concept == "ShortTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=0.0,
                )
            if concept == "LongTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=0.0,
                )
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_return_on_invested_capital_metric():
    metric = ReturnOnInvestedCapitalMetric()
    today = date.today()
    q4 = (today - timedelta(days=30)).isoformat()
    q3 = (today - timedelta(days=120)).isoformat()
    q2 = (today - timedelta(days=210)).isoformat()
    q1 = (today - timedelta(days=300)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "OperatingIncomeLoss":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=100.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=100.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=100.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=100.0,
                    ),
                ]
            if concept == "IncomeBeforeIncomeTaxes":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=125.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=125.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=125.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=125.0,
                    ),
                ]
            if concept == "IncomeTaxExpense":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=25.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=25.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=25.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=25.0,
                    ),
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=50.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=40.0,
                    ),
                ]
            if concept == "LongTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=150.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=140.0,
                    ),
                ]
            if concept == "StockholdersEquity":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=600.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=600.0,
                    ),
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=150.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=150.0,
                    ),
                ]
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 0.5


def test_return_on_invested_capital_uses_fallback_tax_rate():
    metric = ReturnOnInvestedCapitalMetric()
    today = date.today()
    q4 = (today - timedelta(days=30)).isoformat()
    q3 = (today - timedelta(days=120)).isoformat()
    q2 = (today - timedelta(days=210)).isoformat()
    q1 = (today - timedelta(days=300)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "OperatingIncomeLoss":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=100.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=100.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=100.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=100.0,
                    ),
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=50.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=40.0,
                    ),
                ]
            if concept == "LongTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=150.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=140.0,
                    ),
                ]
            if concept == "StockholdersEquity":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=600.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=600.0,
                    ),
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=150.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=150.0,
                    ),
                ]
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert round(result.value, 4) == round(316.0 / 640.0, 4)


def test_debt_paydown_years_skips_non_positive_fcf():
    metric = DebtPaydownYearsMetric()
    today = date.today()
    q4 = (today - timedelta(days=30)).isoformat()
    q3 = (today - timedelta(days=120)).isoformat()
    q2 = (today - timedelta(days=210)).isoformat()
    q1 = (today - timedelta(days=300)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "NetCashProvidedByUsedInOperatingActivities":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=50.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=50.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=50.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=50.0,
                        currency="USD",
                    ),
                ]
            if concept == "CapitalExpenditures":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=60.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=60.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=60.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=60.0,
                        currency="USD",
                    ),
                ]
            return []

        def latest_fact(self, symbol, concept):
            if concept == "ShortTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=q4,
                    value=50.0,
                    currency="USD",
                )
            if concept == "LongTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=q4,
                    value=150.0,
                    currency="USD",
                )
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_interest_coverage_metric():
    metric = InterestCoverageMetric()
    today = date.today()
    q4 = (today - timedelta(days=30)).isoformat()
    q3 = (today - timedelta(days=120)).isoformat()
    q2 = (today - timedelta(days=210)).isoformat()
    q1 = (today - timedelta(days=300)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "OperatingIncomeLoss":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=40.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=30.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=20.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=10.0,
                        currency="USD",
                    ),
                ]
            if concept == "InterestExpense":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=4.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=3.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=2.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=1.0,
                        currency="USD",
                    ),
                ]
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 10.0


def test_interest_coverage_skips_non_positive_interest():
    metric = InterestCoverageMetric()
    today = date.today()
    q4 = (today - timedelta(days=30)).isoformat()
    q3 = (today - timedelta(days=120)).isoformat()
    q2 = (today - timedelta(days=210)).isoformat()
    q1 = (today - timedelta(days=300)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "OperatingIncomeLoss":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=40.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=30.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=20.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=10.0,
                        currency="USD",
                    ),
                ]
            if concept == "InterestExpense":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=0.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=0.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=0.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=0.0,
                        currency="USD",
                    ),
                ]
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_net_debt_to_ebitda_skips_non_positive_ebitda():
    metric = NetDebtToEBITDAMetric()
    today = date.today()
    q4 = (today - timedelta(days=30)).isoformat()
    q3 = (today - timedelta(days=120)).isoformat()
    q2 = (today - timedelta(days=210)).isoformat()
    q1 = (today - timedelta(days=300)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "EBITDA":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=0.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=0.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=0.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=0.0,
                        currency="USD",
                    ),
                ]
            return []

        def latest_fact(self, symbol, concept):
            if concept == "ShortTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=q4,
                    value=10.0,
                    currency="USD",
                )
            if concept == "LongTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=q4,
                    value=90.0,
                    currency="USD",
                )
            if concept == "CashAndShortTermInvestments":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=q4,
                    value=20.0,
                    currency="USD",
                )
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is None


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
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=recent,
                        value=2.5,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date="2024-09-30",
                        value=2.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date="2024-06-30",
                        value=1.5,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date="2024-03-31",
                        value=1.0,
                    ),
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
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=recent,
                        value=2.5,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=older,
                        value=2.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date="2024-06-30",
                        value=1.5,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date="2024-03-31",
                        value=1.0,
                    ),
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
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=recent_fy,
                        value=4.0,
                    )
                ]
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
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=recent,
                        value=130.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=older,
                        value=120.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date="2024-06-30",
                        value=110.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date="2024-03-31",
                        value=100.0,
                    ),
                ]
            if concept == "CapitalExpenditures":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=recent,
                        value=-30.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=older,
                        value=-40.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date="2024-06-30",
                        value=-50.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date="2024-03-31",
                        value=-60.0,
                    ),
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
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=recent,
                        value=130.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=older,
                        value=120.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date="2024-06-30",
                        value=110.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date="2024-03-31",
                        value=100.0,
                    ),
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
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=recent,
                        value=2.5,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=older,
                        value=2.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date="2024-06-30",
                        value=1.5,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date="2024-03-31",
                        value=1.0,
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date="2023-12-31",
                        value=0.5,
                    ),
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
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=recent_fy,
                        value=4.2,
                    )
                ]
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
    recent_quarter = (date.today() - timedelta(days=20)).isoformat()
    recent_fy = (date.today() - timedelta(days=200)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            records = []
            if concept == "OperatingIncomeLoss":
                records = [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date=recent_fy,
                        value=220,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2023-09-30",
                        value=200,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2022-09-30",
                        value=150,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date=recent_quarter,
                        value=999,
                        fiscal_period="Q1",
                    ),
                ]
            if concept == "PropertyPlantAndEquipmentNet":
                records = [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date=recent_fy,
                        value=520,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2023-09-30",
                        value=500,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2022-09-30",
                        value=450,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date=recent_quarter,
                        value=777,
                        fiscal_period="Q1",
                    ),
                ]
            if concept == "AssetsCurrent":
                records = [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date=recent_fy,
                        value=420,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2023-09-30",
                        value=400,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2022-09-30",
                        value=350,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date=recent_quarter,
                        value=888,
                        fiscal_period="Q1",
                    ),
                ]
            if concept == "LiabilitiesCurrent":
                records = [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date=recent_fy,
                        value=310,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2023-09-30",
                        value=300,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2022-09-30",
                        value=250,
                        fiscal_period="FY",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date=recent_quarter,
                        value=444,
                        fiscal_period="Q1",
                    ),
                ]
            if fiscal_period:
                return [
                    record
                    for record in records
                    if (record.fiscal_period or "").upper() == fiscal_period.upper()
                ]
            return records

        def latest_fact(self, symbol, concept):
            return fact(
                symbol=symbol,
                concept=concept,
                end_date=recent_quarter,
                value=0.0,
                fiscal_period="Q1",
            )

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.as_of == recent_fy


def test_roe_greenblatt_metric():
    metric = ROEGreenblattMetric()
    recent = (date.today() - timedelta(days=25)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "NetIncomeLossAvailableToCommonStockholdersBasic":
                return [
                    fact(symbol=symbol, concept=concept, end_date=recent, value=220),
                    fact(
                        symbol=symbol, concept=concept, end_date="2024-09-30", value=200
                    ),
                    fact(
                        symbol=symbol, concept=concept, end_date="2023-09-30", value=180
                    ),
                ]
            if concept == "CommonStockholdersEquity":
                return [
                    fact(symbol=symbol, concept=concept, end_date=recent, value=1100),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        end_date="2024-09-30",
                        value=1000,
                    ),
                    fact(
                        symbol=symbol, concept=concept, end_date="2023-09-30", value=900
                    ),
                ]
            return []

        def latest_fact(self, symbol, concept):
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value > 0


def test_mcapex_fy_metric_uses_min_formula():
    metric = MCapexFYMetric()
    recent = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "CapitalExpenditures":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=recent,
                        value=100.0,
                        currency="USD",
                    )
                ]
            if concept == "DepreciationDepletionAndAmortization":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=recent,
                        value=80.0,
                        currency="USD",
                    )
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 88.0


def test_mcapex_fy_metric_falls_back_to_capex_when_da_missing():
    metric = MCapexFYMetric()
    recent = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "CapitalExpenditures":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=recent,
                        value=120.0,
                        currency="USD",
                    )
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 120.0


def test_mcapex_fy_metric_falls_back_to_da_when_capex_missing():
    metric = MCapexFYMetric()
    recent = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "DepreciationDepletionAndAmortization":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=recent,
                        value=50.0,
                        currency="USD",
                    )
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert round(result.value, 6) == 55.0


def test_mcapex_fy_metric_uses_absolute_values():
    metric = MCapexFYMetric()
    recent = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "CapitalExpenditures":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=recent,
                        value=-120.0,
                        currency="USD",
                    )
                ]
            if concept == "DepreciationDepletionAndAmortization":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=recent,
                        value=-80.0,
                        currency="USD",
                    )
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 88.0


def test_mcapex_ttm_metric_uses_quarterly_formula():
    metric = MCapexTTMMetric()
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "CapitalExpenditures":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=100.0,
                        currency="USD",
                    ),
                ]
            if concept == "DepreciationDepletionAndAmortization":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=80.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=80.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=80.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=80.0,
                        currency="USD",
                    ),
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 352.0


def test_mcapex_ttm_metric_falls_back_to_cash_flow_da():
    metric = MCapexTTMMetric()
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "CapitalExpenditures":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=100.0,
                        currency="USD",
                    ),
                ]
            if concept == "DepreciationFromCashFlow":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=70.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3,
                        value=70.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q2",
                        end_date=q2,
                        value=70.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q1",
                        end_date=q1,
                        value=70.0,
                        currency="USD",
                    ),
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 308.0


def test_mcapex_5y_metric_requires_exactly_five_values():
    metric = MCapexFiveYearMetric()
    d0 = (date.today() - timedelta(days=20)).isoformat()
    d1 = (date.today() - timedelta(days=390)).isoformat()
    d2 = (date.today() - timedelta(days=760)).isoformat()
    d3 = (date.today() - timedelta(days=1130)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept in {
                "CapitalExpenditures",
                "DepreciationDepletionAndAmortization",
            }:
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d0,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d1,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d2,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d3,
                        value=100.0,
                        currency="USD",
                    ),
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is None


def test_mcapex_5y_metric_allows_year_gaps():
    metric = MCapexFiveYearMetric()
    d0 = (date.today() - timedelta(days=20)).isoformat()
    d1 = (date.today() - timedelta(days=760)).isoformat()
    d2 = (date.today() - timedelta(days=1130)).isoformat()
    d3 = (date.today() - timedelta(days=1860)).isoformat()
    d4 = (date.today() - timedelta(days=2230)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "CapitalExpenditures":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d0,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d1,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d2,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d3,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d4,
                        value=100.0,
                        currency="USD",
                    ),
                ]
            if concept == "DepreciationDepletionAndAmortization":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d0,
                        value=200.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d1,
                        value=200.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d2,
                        value=200.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d3,
                        value=200.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=d4,
                        value=200.0,
                        currency="USD",
                    ),
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 100.0


def test_nwc_mqr_metric_base_formula():
    metric = NWCMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=500.0,
                        currency="USD",
                    )
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=300.0,
                        currency="USD",
                    )
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=100.0,
                        currency="USD",
                    )
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=50.0,
                        currency="USD",
                    )
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 150.0


def test_nwc_mqr_metric_short_term_debt_fallback():
    metric = NWCMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=500.0,
                        currency="USD",
                    )
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=300.0,
                        currency="USD",
                    )
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=100.0,
                        currency="USD",
                    )
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 100.0


def test_nwc_mqr_metric_cash_fallback_uses_components():
    metric = NWCMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=500.0,
                        currency="USD",
                    )
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=300.0,
                        currency="USD",
                    )
                ]
            if concept == "CashAndCashEquivalents":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=80.0,
                        currency="USD",
                    )
                ]
            if concept == "ShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=20.0,
                        currency="USD",
                    )
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=50.0,
                        currency="USD",
                    )
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 150.0


def test_nwc_mqr_metric_returns_none_without_cash_source():
    metric = NWCMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=500.0,
                        currency="USD",
                    )
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=300.0,
                        currency="USD",
                    )
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is None


def test_nwc_mqr_metric_floors_adjusted_liabilities():
    metric = NWCMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=300.0,
                        currency="USD",
                    )
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=100.0,
                        currency="USD",
                    )
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=50.0,
                        currency="USD",
                    )
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=150.0,
                        currency="USD",
                    )
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 250.0


def test_nwc_fy_metric():
    metric = NWCFYMetric()
    fy = (date.today() - timedelta(days=100)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=fy,
                        value=500.0,
                        currency="USD",
                    )
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=fy,
                        value=300.0,
                        currency="USD",
                    )
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=fy,
                        value=100.0,
                        currency="USD",
                    )
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=fy,
                        value=50.0,
                        currency="USD",
                    )
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 150.0


def test_delta_nwc_ttm_metric():
    metric = DeltaNWCTTMMetric()
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=500.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4_prev,
                        value=450.0,
                        currency="USD",
                    ),
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=300.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4_prev,
                        value=310.0,
                        currency="USD",
                    ),
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4_prev,
                        value=120.0,
                        currency="USD",
                    ),
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=50.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4_prev,
                        value=60.0,
                        currency="USD",
                    ),
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 70.0


def test_delta_nwc_ttm_metric_requires_same_quarter_last_year():
    metric = DeltaNWCTTMMetric()
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3_prev = (today - timedelta(days=470)).isoformat()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=500.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3_prev,
                        value=450.0,
                        currency="USD",
                    ),
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=300.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3_prev,
                        value=310.0,
                        currency="USD",
                    ),
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3_prev,
                        value=120.0,
                        currency="USD",
                    ),
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q4",
                        end_date=q4,
                        value=50.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="Q3",
                        end_date=q3_prev,
                        value=60.0,
                        currency="USD",
                    ),
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is None


def test_delta_nwc_fy_metric():
    metric = DeltaNWCFYMetric()
    y0 = f"{date.today().year - 1}-09-30"
    y1 = f"{date.today().year - 2}-09-30"

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=500.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=450.0,
                        currency="USD",
                    ),
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=300.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=310.0,
                        currency="USD",
                    ),
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=120.0,
                        currency="USD",
                    ),
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=50.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=60.0,
                        currency="USD",
                    ),
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 70.0


def test_delta_nwc_maint_metric():
    metric = DeltaNWCMaintMetric()
    current_year = date.today().year
    y0 = f"{current_year - 1}-09-30"
    y1 = f"{current_year - 2}-09-30"
    y2 = f"{current_year - 3}-09-30"
    y3 = f"{current_year - 4}-09-30"

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=560.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=520.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y2,
                        value=500.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=470.0,
                        currency="USD",
                    ),
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=320.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=300.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y2,
                        value=290.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=280.0,
                        currency="USD",
                    ),
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=90.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y2,
                        value=70.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=85.0,
                        currency="USD",
                    ),
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=40.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=35.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y2,
                        value=30.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=30.0,
                        currency="USD",
                    ),
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert round(result.value, 4) == round((35.0 - 15.0 + 35.0) / 3.0, 4)


def test_delta_nwc_maint_metric_floors_negative_average_to_zero():
    metric = DeltaNWCMaintMetric()
    current_year = date.today().year
    y0 = f"{current_year - 1}-09-30"
    y1 = f"{current_year - 2}-09-30"
    y2 = f"{current_year - 3}-09-30"
    y3 = f"{current_year - 4}-09-30"

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=600.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=550.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y2,
                        value=500.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=450.0,
                        currency="USD",
                    ),
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=450.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=350.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y2,
                        value=280.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=200.0,
                        currency="USD",
                    ),
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=60.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=70.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y2,
                        value=80.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=90.0,
                        currency="USD",
                    ),
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=40.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=35.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y2,
                        value=30.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=25.0,
                        currency="USD",
                    ),
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is not None
    assert result.value == 0.0


def test_delta_nwc_maint_metric_requires_consecutive_deltas():
    metric = DeltaNWCMaintMetric()
    current_year = date.today().year
    y0 = f"{current_year - 1}-09-30"
    y1 = f"{current_year - 2}-09-30"
    y3 = f"{current_year - 4}-09-30"

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "AssetsCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=560.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=520.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=470.0,
                        currency="USD",
                    ),
                ]
            if concept == "LiabilitiesCurrent":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=320.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=300.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=280.0,
                        currency="USD",
                    ),
                ]
            if concept == "CashAndShortTermInvestments":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=90.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=100.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=85.0,
                        currency="USD",
                    ),
                ]
            if concept == "ShortTermDebt":
                return [
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y0,
                        value=40.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y1,
                        value=35.0,
                        currency="USD",
                    ),
                    fact(
                        symbol=symbol,
                        concept=concept,
                        fiscal_period="FY",
                        end_date=y3,
                        value=30.0,
                        currency="USD",
                    ),
                ]
            return []

    result = metric.compute("AAPL.US", DummyRepo())
    assert result is None


def _build_nwc_fy_records(
    symbol: str,
    latest_year: int,
    nwc_values: list[float],
) -> dict[str, list[FactRecord]]:
    assets_records: list[FactRecord] = []
    liabilities_records: list[FactRecord] = []
    cash_records: list[FactRecord] = []
    short_debt_records: list[FactRecord] = []
    for offset, nwc in enumerate(nwc_values):
        year = latest_year - offset
        end_date = f"{year}-09-30"
        assets = nwc + 350.0
        liabilities = 300.0
        cash = 100.0
        short_debt = 50.0
        assets_records.append(
            fact(
                symbol=symbol,
                concept="AssetsCurrent",
                fiscal_period="FY",
                end_date=end_date,
                value=assets,
                currency="USD",
            )
        )
        liabilities_records.append(
            fact(
                symbol=symbol,
                concept="LiabilitiesCurrent",
                fiscal_period="FY",
                end_date=end_date,
                value=liabilities,
                currency="USD",
            )
        )
        cash_records.append(
            fact(
                symbol=symbol,
                concept="CashAndShortTermInvestments",
                fiscal_period="FY",
                end_date=end_date,
                value=cash,
                currency="USD",
            )
        )
        short_debt_records.append(
            fact(
                symbol=symbol,
                concept="ShortTermDebt",
                fiscal_period="FY",
                end_date=end_date,
                value=short_debt,
                currency="USD",
            )
        )
    return {
        "AssetsCurrent": assets_records,
        "LiabilitiesCurrent": liabilities_records,
        "CashAndShortTermInvestments": cash_records,
        "ShortTermDebt": short_debt_records,
    }


class _OwnerEarningsRepo:
    def __init__(self, records_by_concept):
        self.records_by_concept = records_by_concept

    def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
        records = list(self.records_by_concept.get(concept, []))
        if fiscal_period:
            period = fiscal_period.upper()
            records = [
                record
                for record in records
                if (record.fiscal_period or "").upper() == period
            ]
        if limit is not None:
            return records[:limit]
        return records


def test_oe_equity_ttm_metric_computes_formula():
    metric = OwnerEarningsEquityTTMMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=200.0,
                    currency="USD",
                ),
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=90.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=90.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=90.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=90.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=100.0,
                    currency="USD",
                ),
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.as_of == q4
    assert result.value == 744.0


def test_oe_equity_ttm_metric_net_income_fallback():
    metric = OwnerEarningsEquityTTMMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLossAvailableToCommonStockholdersBasic": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=150.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=150.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=150.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLossAvailableToCommonStockholdersBasic",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=150.0,
                    currency="USD",
                ),
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=50.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=50.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=50.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=50.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=40.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=40.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=40.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=40.0,
                    currency="USD",
                ),
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 620.0


def test_oe_equity_ttm_metric_da_fallback_to_cash_flow():
    metric = OwnerEarningsEquityTTMMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=100.0,
                    currency="USD",
                ),
            ],
            "DepreciationFromCashFlow": [
                fact(
                    symbol=symbol,
                    concept="DepreciationFromCashFlow",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=30.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationFromCashFlow",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=30.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationFromCashFlow",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=30.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationFromCashFlow",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=30.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=50.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=50.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=50.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=50.0,
                    currency="USD",
                ),
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 368.0


def test_oe_equity_ttm_metric_treats_missing_da_as_zero():
    metric = OwnerEarningsEquityTTMMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=120.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=120.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=120.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=120.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=40.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=40.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=40.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=40.0,
                    currency="USD",
                ),
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 300.0


def test_oe_equity_ttm_metric_requires_delta_nwc_maint():
    metric = OwnerEarningsEquityTTMMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=100.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=50.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=50.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=50.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=50.0,
                    currency="USD",
                ),
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is None


def test_oe_equity_ttm_metric_currency_mismatch_returns_none():
    metric = OwnerEarningsEquityTTMMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=100.0,
                    currency="USD",
                ),
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=30.0,
                    currency="EUR",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=30.0,
                    currency="EUR",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=30.0,
                    currency="EUR",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=30.0,
                    currency="EUR",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=50.0,
                    currency="EUR",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=50.0,
                    currency="EUR",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=50.0,
                    currency="EUR",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=50.0,
                    currency="EUR",
                ),
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is None


def test_oe_equity_5y_avg_metric_computes_expected_average():
    metric = OwnerEarningsEquityFiveYearAverageMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0, 70.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{years[0]}-09-30",
                    value=500.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{years[1]}-09-30",
                    value=450.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{years[2]}-09-30",
                    value=400.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{years[3]}-09-30",
                    value=350.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{years[4]}-09-30",
                    value=300.0,
                    currency="USD",
                ),
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=100.0,
                    currency="USD",
                )
                for year in years
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=90.0,
                    currency="USD",
                )
                for year in years
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 390.0
    assert result.as_of == f"{years[0]}-09-30"


def test_oe_equity_5y_avg_metric_requires_five_points():
    metric = OwnerEarningsEquityFiveYearAverageMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(4)]

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=300.0,
                    currency="USD",
                )
                for year in years
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=90.0,
                    currency="USD",
                )
                for year in years
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is None


def test_oe_equity_5y_avg_metric_allows_year_gaps():
    metric = OwnerEarningsEquityFiveYearAverageMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    fy_years = [
        latest_year,
        latest_year - 2,
        latest_year - 3,
        latest_year - 5,
        latest_year - 6,
    ]

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0, 70.0, 60.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=value,
                    currency="USD",
                )
                for year, value in zip(fy_years, [500.0, 400.0, 350.0, 250.0, 200.0])
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=100.0,
                    currency="USD",
                )
                for year in fy_years
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=90.0,
                    currency="USD",
                )
                for year in fy_years
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 330.0


def test_oe_equity_5y_avg_metric_uses_latest_delta_nwc_maint_for_all_years():
    metric = OwnerEarningsEquityFiveYearAverageMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]

    # NWC deltas: +50, +20, +20 => delta_nwc_maint = 30 from latest FY.
    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [300.0, 250.0, 230.0, 210.0, 190.0, 170.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=300.0,
                    currency="USD",
                )
                for year in years
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=100.0,
                    currency="USD",
                )
                for year in years
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=90.0,
                    currency="USD",
                )
                for year in years
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 280.0


def test_oe_equity_5y_avg_metric_requires_consistent_currency_across_years():
    metric = OwnerEarningsEquityFiveYearAverageMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]
    currencies = ["USD", "USD", "USD", "EUR", "EUR"]

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0, 70.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=400.0,
                    currency=currency,
                )
                for year, currency in zip(years, currencies)
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=100.0,
                    currency=currency,
                )
                for year, currency in zip(years, currencies)
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=90.0,
                    currency=currency,
                )
                for year, currency in zip(years, currencies)
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is None


def test_oey_equity_metric_computes_ratio_from_ttm_numerator():
    metric = OwnerEarningsYieldEquityMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=200.0,
                    currency="USD",
                ),
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=90.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=90.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=90.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=90.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=100.0,
                    currency="USD",
                ),
            ],
        }
    )

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 7440.0
                as_of = q3
                currency = "USD"

            return Snapshot()

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is not None
    assert result.as_of == q4
    assert result.value == 0.1


def test_oey_equity_5y_metric_computes_ratio_from_5y_numerator():
    metric = OwnerEarningsYieldEquityFiveYearMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0, 70.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{years[0]}-09-30",
                    value=500.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{years[1]}-09-30",
                    value=450.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{years[2]}-09-30",
                    value=400.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{years[3]}-09-30",
                    value=350.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{years[4]}-09-30",
                    value=300.0,
                    currency="USD",
                ),
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=100.0,
                    currency="USD",
                )
                for year in years
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=90.0,
                    currency="USD",
                )
                for year in years
            ],
        }
    )

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 3900.0
                as_of = "2026-01-01"
                currency = "USD"

            return Snapshot()

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is not None
    assert result.value == 0.1
    assert result.as_of == f"{years[0]}-09-30"


def test_oey_equity_metric_returns_none_when_market_cap_missing():
    metric = OwnerEarningsYieldEquityMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=200.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=100.0,
                    currency="USD",
                ),
            ],
        }
    )

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = None
                as_of = q3
                currency = "USD"

            return Snapshot()

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is None


def test_oey_equity_metric_returns_none_when_market_cap_non_positive():
    metric = OwnerEarningsYieldEquityMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=120.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=120.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=120.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=120.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=40.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=40.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=40.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=40.0,
                    currency="USD",
                ),
            ],
        }
    )

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 0.0
                as_of = q3
                currency = "USD"

            return Snapshot()

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is None


def test_oey_equity_metric_returns_none_when_numerator_missing():
    metric = OwnerEarningsYieldEquityMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 1000.0
                as_of = "2026-01-01"
                currency = "USD"

            return Snapshot()

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is None


def test_oey_equity_metric_applies_fx_conversion(monkeypatch):
    metric = OwnerEarningsYieldEquityMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=200.0,
                    currency="USD",
                ),
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=90.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=90.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=90.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=90.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=100.0,
                    currency="USD",
                ),
            ],
        }
    )

    monkeypatch.setattr(
        "pyvalue.metrics.owner_earnings_yield.FXRateStore.convert",
        lambda self, amount, from_currency, to_currency, as_of: amount * 2.0,
    )

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 100.0
                as_of = q3
                currency = "EUR"

            return Snapshot()

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is not None
    assert result.value == 744.0 / 200.0


def test_oey_equity_metric_returns_none_when_fx_conversion_fails(monkeypatch):
    metric = OwnerEarningsYieldEquityMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=200.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=100.0,
                    currency="USD",
                ),
            ],
        }
    )

    monkeypatch.setattr(
        "pyvalue.metrics.owner_earnings_yield.FXRateStore.convert",
        lambda self, amount, from_currency, to_currency, as_of: None,
    )

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 100.0
                as_of = q3
                currency = "EUR"

            return Snapshot()

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is None


def test_oey_equity_metric_allows_negative_values():
    metric = OwnerEarningsYieldEquityMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    records_by_concept.update(
        {
            "NetIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=-100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=-100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=-100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetIncomeLoss",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=-100.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=20.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=20.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=20.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=20.0,
                    currency="USD",
                ),
            ],
        }
    )

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 4920.0
                as_of = q3
                currency = "USD"

            return Snapshot()

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is not None
    assert result.value == -500.0 / 4920.0


def test_registry_contains_all_ids():
    # Ensure the registry still exposes all metric identifiers
    assert len(REGISTRY) >= 1
    assert "mcapex_fy" in REGISTRY
    assert "mcapex_5y" in REGISTRY
    assert "mcapex_ttm" in REGISTRY
    assert "nwc_mqr" in REGISTRY
    assert "nwc_fy" in REGISTRY
    assert "delta_nwc_ttm" in REGISTRY
    assert "delta_nwc_fy" in REGISTRY
    assert "delta_nwc_maint" in REGISTRY
    assert "oe_equity_ttm" in REGISTRY
    assert "oe_equity_5y_avg" in REGISTRY
    assert "oey_equity" in REGISTRY
    assert "oey_equity_5y" in REGISTRY
