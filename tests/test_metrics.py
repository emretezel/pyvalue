"""Tests for metric implementations.

Author: Emre Tezel
"""

from datetime import date, timedelta

from pyvalue.metrics import REGISTRY
from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.metrics.debt_paydown_years import DebtPaydownYearsMetric, FCFToDebtMetric
from pyvalue.metrics.earnings_yield import EarningsYieldMetric
from pyvalue.metrics.eps_average import EPSAverageSixYearMetric
from pyvalue.metrics.eps_quarterly import EarningsPerShareTTM
from pyvalue.metrics.eps_streak import EPSStreakMetric
from pyvalue.metrics.graham_eps_cagr import GrahamEPSCAGRMetric
from pyvalue.metrics.graham_multiplier import GrahamMultiplierMetric
from pyvalue.metrics.interest_coverage import InterestCoverageMetric
from pyvalue.metrics.invested_capital import (
    AvgICMetric,
    ICFYMetric,
    ICMostRecentQuarterMetric,
)
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
    OwnerEarningsYieldEVMetric,
)
from pyvalue.metrics.owner_earnings_enterprise import (
    OwnerEarningsEnterpriseFiveYearAverageMetric,
    OwnerEarningsEnterpriseTTMMetric,
)
from pyvalue.metrics.price_to_fcf import PriceToFCFMetric
from pyvalue.metrics.roc_greenblatt import ROCGreenblattMetric
from pyvalue.metrics.roic_fy_series import (
    ROIC10YMedianMetric,
    ROIC10YMinMetric,
    ROICYearsAbove12PctMetric,
)
from pyvalue.metrics.roic_ttm import RoicTTMMetric
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


def _net_debt_quarter_dates():
    today = date.today()
    return (
        (today - timedelta(days=30)).isoformat(),
        (today - timedelta(days=120)).isoformat(),
        (today - timedelta(days=210)).isoformat(),
        (today - timedelta(days=300)).isoformat(),
    )


def _build_net_debt_repo(*, concept_records=None, latest_records=None):
    concept_records = concept_records or {}
    latest_records = latest_records or {}

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            return concept_records.get(concept, [])

        def latest_fact(self, symbol, concept):
            return latest_records.get(concept)

    return DummyRepo()


def _quarterly_records(concept, quarter_dates, values, *, currency="USD"):
    periods = ("Q4", "Q3", "Q2", "Q1")[: len(quarter_dates)]
    return [
        fact(
            concept=concept,
            fiscal_period=period,
            end_date=end_date,
            value=value,
            currency=currency,
        )
        for period, end_date, value in zip(periods, quarter_dates, values, strict=True)
    ]


def _base_ebit_da_concepts(
    quarter_dates,
    *,
    ebit_values=(20.0, 20.0, 20.0, 20.0),
    ebit_currency="USD",
    da_values=(5.0, 5.0, 5.0, 5.0),
    da_currency="USD",
    da_concept="DepreciationDepletionAndAmortization",
):
    return {
        "OperatingIncomeLoss": _quarterly_records(
            "OperatingIncomeLoss",
            quarter_dates,
            ebit_values,
            currency=ebit_currency,
        ),
        da_concept: _quarterly_records(
            da_concept,
            quarter_dates,
            da_values,
            currency=da_currency,
        ),
    }


def _default_net_debt_latest_records(q4):
    return {
        "ShortTermDebt": fact(
            concept="ShortTermDebt",
            end_date=q4,
            value=10.0,
            currency="USD",
        ),
        "LongTermDebt": fact(
            concept="LongTermDebt",
            end_date=q4,
            value=90.0,
            currency="USD",
        ),
        "CashAndShortTermInvestments": fact(
            concept="CashAndShortTermInvestments",
            end_date=q4,
            value=20.0,
            currency="USD",
        ),
    }


def _base_debt_paydown_concepts(quarter_dates):
    return {
        "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
            "NetCashProvidedByUsedInOperatingActivities",
            quarter_dates,
            (100.0, 100.0, 100.0, 100.0),
        ),
        "CapitalExpenditures": _quarterly_records(
            "CapitalExpenditures",
            quarter_dates,
            (50.0, 50.0, 50.0, 50.0),
        ),
    }


def _default_debt_paydown_latest_records(q4):
    return {
        "ShortTermDebt": fact(
            concept="ShortTermDebt",
            end_date=q4,
            value=50.0,
            currency="USD",
        ),
        "LongTermDebt": fact(
            concept="LongTermDebt",
            end_date=q4,
            value=150.0,
            currency="USD",
        ),
    }


def _build_fcf_debt_repo(*, concept_records=None, latest_records=None):
    concept_records = concept_records or {}
    latest_records = latest_records or {}

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            return concept_records.get(concept, [])

        def latest_fact(self, symbol, concept):
            return latest_records.get(concept)

    return DummyRepo()


def _build_ic_repo(*, concept_records=None):
    concept_records = concept_records or {}

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            return concept_records.get(concept, [])

    return DummyRepo()


def _roic_dates():
    today = date.today()
    return {
        "q4": (today - timedelta(days=20)).isoformat(),
        "q3": (today - timedelta(days=110)).isoformat(),
        "q2": (today - timedelta(days=200)).isoformat(),
        "q1": (today - timedelta(days=290)).isoformat(),
        "q4_prev": (today - timedelta(days=380)).isoformat(),
        "fy_latest": (today - timedelta(days=45)).isoformat(),
        "fy_prior": (today - timedelta(days=410)).isoformat(),
    }


def _base_roic_concepts(
    *,
    ebit_currency="USD",
    tax_currency="USD",
    pretax_currency="USD",
    avg_currency="USD",
    include_ttm_tax=True,
    include_ttm_pretax=True,
    include_fy_tax_proxy=True,
    include_avg_ic=True,
    quarterly_ebit_values=(100.0, 100.0, 100.0, 100.0),
    quarterly_tax_values=(25.0, 25.0, 25.0, 25.0),
    quarterly_pretax_values=(125.0, 125.0, 125.0, 125.0),
    avg_latest=(60.0, 140.0, 500.0, 100.0),
    avg_prior=(50.0, 100.0, 450.0, 90.0),
):
    dates = _roic_dates()
    q_dates = [dates["q4"], dates["q3"], dates["q2"], dates["q1"]]
    concepts = {
        "OperatingIncomeLoss": [
            fact(
                concept="OperatingIncomeLoss",
                fiscal_period=period,
                end_date=end_date,
                value=value,
                currency=ebit_currency,
            )
            for period, end_date, value in zip(
                ("Q4", "Q3", "Q2", "Q1"), q_dates, quarterly_ebit_values, strict=True
            )
        ],
    }

    if include_ttm_tax:
        concepts["IncomeTaxExpense"] = [
            fact(
                concept="IncomeTaxExpense",
                fiscal_period=period,
                end_date=end_date,
                value=value,
                currency=tax_currency,
            )
            for period, end_date, value in zip(
                ("Q4", "Q3", "Q2", "Q1"), q_dates, quarterly_tax_values, strict=True
            )
        ]
    if include_ttm_pretax:
        concepts["IncomeBeforeIncomeTaxes"] = [
            fact(
                concept="IncomeBeforeIncomeTaxes",
                fiscal_period=period,
                end_date=end_date,
                value=value,
                currency=pretax_currency,
            )
            for period, end_date, value in zip(
                ("Q4", "Q3", "Q2", "Q1"),
                q_dates,
                quarterly_pretax_values,
                strict=True,
            )
        ]

    if include_fy_tax_proxy:
        concepts.setdefault("IncomeTaxExpense", []).extend(
            [
                fact(
                    concept="IncomeTaxExpense",
                    fiscal_period="FY",
                    end_date=dates["fy_latest"],
                    value=90.0,
                    currency=tax_currency,
                ),
                fact(
                    concept="IncomeTaxExpense",
                    fiscal_period="FY",
                    end_date=dates["fy_prior"],
                    value=80.0,
                    currency=tax_currency,
                ),
            ]
        )
        concepts.setdefault("IncomeBeforeIncomeTaxes", []).extend(
            [
                fact(
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="FY",
                    end_date=dates["fy_latest"],
                    value=300.0,
                    currency=pretax_currency,
                ),
                fact(
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="FY",
                    end_date=dates["fy_prior"],
                    value=280.0,
                    currency=pretax_currency,
                ),
            ]
        )

    if include_avg_ic:
        short_latest, long_latest, equity_latest, cash_latest = avg_latest
        short_prior, long_prior, equity_prior, cash_prior = avg_prior
        concepts["ShortTermDebt"] = [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=dates["q4"],
                value=short_latest,
                currency=avg_currency,
            ),
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=dates["q4_prev"],
                value=short_prior,
                currency=avg_currency,
            ),
        ]
        concepts["LongTermDebt"] = [
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=dates["q4"],
                value=long_latest,
                currency=avg_currency,
            ),
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=dates["q4_prev"],
                value=long_prior,
                currency=avg_currency,
            ),
        ]
        concepts["StockholdersEquity"] = [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=dates["q4"],
                value=equity_latest,
                currency=avg_currency,
            ),
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=dates["q4_prev"],
                value=equity_prior,
                currency=avg_currency,
            ),
        ]
        concepts["CashAndCashEquivalents"] = [
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=dates["q4"],
                value=cash_latest,
                currency=avg_currency,
            ),
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=dates["q4_prev"],
                value=cash_prior,
                currency=avg_currency,
            ),
        ]

    return concepts


def _base_roic_10y_concepts(
    *,
    latest_year=None,
    ebit_by_year=None,
    tax_by_year=None,
    pretax_by_year=None,
    ic_short_by_year=None,
    ic_long_by_year=None,
    ic_equity_by_year=None,
    ic_cash_by_year=None,
    currency_by_year=None,
):
    if latest_year is None:
        latest_year = date.today().year - 1

    # Need 11 IC points (Y..Y-10) to compute 10 ROIC points (Y..Y-9).
    ic_years = list(range(latest_year - 10, latest_year + 1))
    roic_years = list(range(latest_year - 9, latest_year + 1))

    if ebit_by_year is None:
        ebit_values = [
            300.0,
            275.0,
            250.0,
            225.0,
            200.0,
            175.0,
            150.0,
            125.0,
            100.0,
            75.0,
        ]
        ebit_by_year = {
            year: value
            for year, value in zip(reversed(roic_years), ebit_values, strict=True)
        }
    if tax_by_year is None:
        tax_by_year = {year: 40.0 for year in roic_years}
    if pretax_by_year is None:
        pretax_by_year = {year: 200.0 for year in roic_years}
    if ic_short_by_year is None:
        ic_short_by_year = {year: 100.0 for year in ic_years}
    if ic_long_by_year is None:
        ic_long_by_year = {year: 300.0 for year in ic_years}
    if ic_equity_by_year is None:
        ic_equity_by_year = {year: 900.0 for year in ic_years}
    if ic_cash_by_year is None:
        ic_cash_by_year = {year: 300.0 for year in ic_years}
    if currency_by_year is None:
        currency_by_year = {}

    concept_records = {
        "OperatingIncomeLoss": [],
        "IncomeTaxExpense": [],
        "IncomeBeforeIncomeTaxes": [],
        "ShortTermDebt": [],
        "LongTermDebt": [],
        "StockholdersEquity": [],
        "CashAndCashEquivalents": [],
    }

    for year in roic_years:
        currency = currency_by_year.get(year, "USD")
        end_date = f"{year}-09-30"
        if year in ebit_by_year:
            concept_records["OperatingIncomeLoss"].append(
                fact(
                    concept="OperatingIncomeLoss",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=ebit_by_year[year],
                    currency=currency,
                )
            )
        if year in tax_by_year:
            concept_records["IncomeTaxExpense"].append(
                fact(
                    concept="IncomeTaxExpense",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=tax_by_year[year],
                    currency=currency,
                )
            )
        if year in pretax_by_year:
            concept_records["IncomeBeforeIncomeTaxes"].append(
                fact(
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=pretax_by_year[year],
                    currency=currency,
                )
            )

    for year in ic_years:
        currency = currency_by_year.get(year, "USD")
        end_date = f"{year}-09-30"
        if year in ic_short_by_year:
            concept_records["ShortTermDebt"].append(
                fact(
                    concept="ShortTermDebt",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=ic_short_by_year[year],
                    currency=currency,
                )
            )
        if year in ic_long_by_year:
            concept_records["LongTermDebt"].append(
                fact(
                    concept="LongTermDebt",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=ic_long_by_year[year],
                    currency=currency,
                )
            )
        if year in ic_equity_by_year:
            concept_records["StockholdersEquity"].append(
                fact(
                    concept="StockholdersEquity",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=ic_equity_by_year[year],
                    currency=currency,
                )
            )
        if year in ic_cash_by_year:
            concept_records["CashAndCashEquivalents"].append(
                fact(
                    concept="CashAndCashEquivalents",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=ic_cash_by_year[year],
                    currency=currency,
                )
            )

    return concept_records


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
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    repo = _build_net_debt_repo(
        concept_records=_base_ebit_da_concepts(quarter_dates),
        latest_records=_default_net_debt_latest_records(q4),
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 0.8


def test_net_debt_to_ebitda_uses_da_fallback_per_quarter():
    metric = NetDebtToEBITDAMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    concept_records = _base_ebit_da_concepts(quarter_dates)
    concept_records["DepreciationDepletionAndAmortization"] = concept_records[
        "DepreciationDepletionAndAmortization"
    ][:2]
    concept_records["DepreciationFromCashFlow"] = _quarterly_records(
        "DepreciationFromCashFlow", quarter_dates, (5.0, 5.0, 5.0, 5.0)
    )[2:]
    repo = _build_net_debt_repo(
        concept_records=concept_records,
        latest_records=_default_net_debt_latest_records(q4),
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 0.8


def test_net_debt_to_ebitda_requires_four_quarters_of_ebit():
    metric = NetDebtToEBITDAMetric()
    quarter_dates = _net_debt_quarter_dates()
    concept_records = _base_ebit_da_concepts(quarter_dates)
    concept_records["OperatingIncomeLoss"] = concept_records["OperatingIncomeLoss"][:3]
    repo = _build_net_debt_repo(
        concept_records=concept_records,
        latest_records={},
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_debt_paydown_years_metric():
    metric = DebtPaydownYearsMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    repo = _build_fcf_debt_repo(
        concept_records=_base_debt_paydown_concepts(quarter_dates),
        latest_records=_default_debt_paydown_latest_records(q4),
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 1.0


def test_fcf_to_debt_metric():
    metric = FCFToDebtMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    repo = _build_fcf_debt_repo(
        concept_records=_base_debt_paydown_concepts(quarter_dates),
        latest_records=_default_debt_paydown_latest_records(q4),
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 1.0


def test_debt_paydown_years_uses_total_debt_fallback():
    metric = DebtPaydownYearsMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    latest = {
        "LongTermDebt": fact(
            concept="LongTermDebt",
            end_date=q4,
            value=999.0,
            currency="USD",
        ),
        "TotalDebtFromBalanceSheet": fact(
            concept="TotalDebtFromBalanceSheet",
            end_date=q4,
            value=200.0,
            currency="USD",
        ),
    }
    repo = _build_fcf_debt_repo(
        concept_records=_base_debt_paydown_concepts(quarter_dates),
        latest_records=latest,
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 1.0


def test_debt_paydown_years_uses_one_side_debt_fallback():
    metric = DebtPaydownYearsMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    latest = {
        "LongTermDebt": fact(
            concept="LongTermDebt",
            end_date=q4,
            value=150.0,
            currency="USD",
        ),
    }
    repo = _build_fcf_debt_repo(
        concept_records=_base_debt_paydown_concepts(quarter_dates),
        latest_records=latest,
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 0.75


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


def test_short_term_debt_share_uses_total_debt_fallback_when_long_missing():
    metric = ShortTermDebtShareMetric()
    recent = (date.today() - timedelta(days=10)).isoformat()

    class DummyRepo:
        def latest_fact(self, symbol, concept):
            if concept == "ShortTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=30.0,
                    currency="USD",
                )
            if concept == "LongTermDebt":
                return None
            if concept == "TotalDebtFromBalanceSheet":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=120.0,
                    currency="USD",
                )
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 0.25


def test_short_term_debt_share_requires_short_term_debt():
    metric = ShortTermDebtShareMetric()
    recent = (date.today() - timedelta(days=10)).isoformat()

    class DummyRepo:
        def latest_fact(self, symbol, concept):
            if concept == "LongTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=100.0,
                    currency="USD",
                )
            if concept == "TotalDebtFromBalanceSheet":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=140.0,
                    currency="USD",
                )
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is None


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


def test_short_term_debt_share_skips_ratio_out_of_bounds():
    metric = ShortTermDebtShareMetric()
    recent = (date.today() - timedelta(days=10)).isoformat()

    class DummyRepo:
        def latest_fact(self, symbol, concept):
            if concept == "ShortTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=120.0,
                    currency="USD",
                )
            if concept == "TotalDebtFromBalanceSheet":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=100.0,
                    currency="USD",
                )
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_short_term_debt_share_skips_currency_mismatch():
    metric = ShortTermDebtShareMetric()
    recent = (date.today() - timedelta(days=10)).isoformat()

    class DummyRepo:
        def latest_fact(self, symbol, concept):
            if concept == "ShortTermDebt":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=30.0,
                    currency="USD",
                )
            if concept == "LongTermDebt":
                return None
            if concept == "TotalDebtFromBalanceSheet":
                return fact(
                    symbol=symbol,
                    concept=concept,
                    end_date=recent,
                    value=120.0,
                    currency="EUR",
                )
            return None

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_ic_mqr_metric():
    metric = ICMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=50.0,
                currency="USD",
            )
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=150.0,
                currency="USD",
            )
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=500.0,
                currency="USD",
            )
        ],
        "CashAndCashEquivalents": [
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=q4,
                value=100.0,
                currency="USD",
            )
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 600.0


def test_ic_mqr_uses_total_debt_fallback_when_long_missing():
    metric = ICMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=50.0,
                currency="USD",
            )
        ],
        "TotalDebtFromBalanceSheet": [
            fact(
                concept="TotalDebtFromBalanceSheet",
                fiscal_period="Q4",
                end_date=q4,
                value=260.0,
                currency="USD",
            )
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=500.0,
                currency="USD",
            )
        ],
        "CashAndCashEquivalents": [
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=q4,
                value=100.0,
                currency="USD",
            )
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 660.0


def test_ic_mqr_uses_one_side_debt_fallback():
    metric = ICMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    concept_records = {
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=180.0,
                currency="USD",
            )
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=500.0,
                currency="USD",
            )
        ],
        "CashAndCashEquivalents": [
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=q4,
                value=100.0,
                currency="USD",
            )
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 580.0


def test_ic_mqr_uses_cash_fallback_when_primary_missing():
    metric = ICMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=50.0,
                currency="USD",
            )
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=150.0,
                currency="USD",
            )
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=500.0,
                currency="USD",
            )
        ],
        "CashAndShortTermInvestments": [
            fact(
                concept="CashAndShortTermInvestments",
                fiscal_period="Q4",
                end_date=q4,
                value=120.0,
                currency="USD",
            )
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 580.0


def test_ic_mqr_returns_none_when_missing_required_inputs():
    metric = ICMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=50.0,
                currency="USD",
            )
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=150.0,
                currency="USD",
            )
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=500.0,
                currency="USD",
            )
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_ic_mqr_returns_none_on_currency_mismatch():
    metric = ICMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=50.0,
                currency="USD",
            )
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=150.0,
                currency="USD",
            )
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=500.0,
                currency="USD",
            )
        ],
        "CashAndCashEquivalents": [
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=q4,
                value=100.0,
                currency="EUR",
            )
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_ic_mqr_emits_signed_negative_value():
    metric = ICMostRecentQuarterMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=50.0,
                currency="USD",
            )
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=50.0,
                currency="USD",
            )
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=100.0,
                currency="USD",
            )
        ],
        "CashAndCashEquivalents": [
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=q4,
                value=300.0,
                currency="USD",
            )
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == -100.0


def test_ic_mqr_returns_none_when_latest_quarter_is_stale():
    metric = ICMostRecentQuarterMetric()
    stale_q4 = (date.today() - timedelta(days=500)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=stale_q4,
                value=50.0,
                currency="USD",
            )
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=stale_q4,
                value=150.0,
                currency="USD",
            )
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=stale_q4,
                value=500.0,
                currency="USD",
            )
        ],
        "CashAndCashEquivalents": [
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=stale_q4,
                value=100.0,
                currency="USD",
            )
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_ic_fy_metric():
    metric = ICFYMetric()
    fy = (date.today() - timedelta(days=30)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="FY",
                end_date=fy,
                value=80.0,
                currency="USD",
            )
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="FY",
                end_date=fy,
                value=220.0,
                currency="USD",
            )
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="FY",
                end_date=fy,
                value=1000.0,
                currency="USD",
            )
        ],
        "CashAndCashEquivalents": [
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="FY",
                end_date=fy,
                value=200.0,
                currency="USD",
            )
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 1100.0


def test_ic_fy_returns_none_when_latest_fy_is_stale():
    metric = ICFYMetric()
    stale_fy = (date.today() - timedelta(days=500)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="FY",
                end_date=stale_fy,
                value=80.0,
                currency="USD",
            )
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="FY",
                end_date=stale_fy,
                value=220.0,
                currency="USD",
            )
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="FY",
                end_date=stale_fy,
                value=1000.0,
                currency="USD",
            )
        ],
        "CashAndCashEquivalents": [
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="FY",
                end_date=stale_fy,
                value=200.0,
                currency="USD",
            )
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_avg_ic_uses_same_quarter_yoy_when_available():
    metric = AvgICMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    q4_prev = (date.today() - timedelta(days=380)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=60.0,
                currency="USD",
            ),
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4_prev,
                value=50.0,
                currency="USD",
            ),
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=140.0,
                currency="USD",
            ),
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=q4_prev,
                value=100.0,
                currency="USD",
            ),
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=500.0,
                currency="USD",
            ),
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4_prev,
                value=450.0,
                currency="USD",
            ),
        ],
        "CashAndCashEquivalents": [
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=q4,
                value=100.0,
                currency="USD",
            ),
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=q4_prev,
                value=90.0,
                currency="USD",
            ),
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 555.0
    assert result.as_of == q4


def test_avg_ic_falls_back_to_fy_when_quarterly_pair_missing():
    metric = AvgICMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    fy_latest = (date.today() - timedelta(days=45)).isoformat()
    fy_prior = (date.today() - timedelta(days=400)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=60.0,
                currency="USD",
            ),
            fact(
                concept="ShortTermDebt",
                fiscal_period="FY",
                end_date=fy_latest,
                value=90.0,
                currency="USD",
            ),
            fact(
                concept="ShortTermDebt",
                fiscal_period="FY",
                end_date=fy_prior,
                value=80.0,
                currency="USD",
            ),
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="FY",
                end_date=fy_latest,
                value=210.0,
                currency="USD",
            ),
            fact(
                concept="LongTermDebt",
                fiscal_period="FY",
                end_date=fy_prior,
                value=200.0,
                currency="USD",
            ),
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=500.0,
                currency="USD",
            ),
            fact(
                concept="StockholdersEquity",
                fiscal_period="FY",
                end_date=fy_latest,
                value=1000.0,
                currency="USD",
            ),
            fact(
                concept="StockholdersEquity",
                fiscal_period="FY",
                end_date=fy_prior,
                value=900.0,
                currency="USD",
            ),
        ],
        "CashAndShortTermInvestments": [
            fact(
                concept="CashAndShortTermInvestments",
                fiscal_period="Q4",
                end_date=q4,
                value=100.0,
                currency="USD",
            ),
            fact(
                concept="CashAndShortTermInvestments",
                fiscal_period="FY",
                end_date=fy_latest,
                value=200.0,
                currency="USD",
            ),
            fact(
                concept="CashAndShortTermInvestments",
                fiscal_period="FY",
                end_date=fy_prior,
                value=180.0,
                currency="USD",
            ),
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 1050.0
    assert result.as_of == fy_latest


def test_avg_ic_requires_strict_prior_year_for_fy_fallback():
    metric = AvgICMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    fy_latest = (date.today() - timedelta(days=45)).isoformat()
    fy_gap = (date.today() - timedelta(days=800)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=60.0,
                currency="USD",
            ),
            fact(
                concept="ShortTermDebt",
                fiscal_period="FY",
                end_date=fy_latest,
                value=90.0,
                currency="USD",
            ),
            fact(
                concept="ShortTermDebt",
                fiscal_period="FY",
                end_date=fy_gap,
                value=80.0,
                currency="USD",
            ),
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="FY",
                end_date=fy_latest,
                value=210.0,
                currency="USD",
            ),
            fact(
                concept="LongTermDebt",
                fiscal_period="FY",
                end_date=fy_gap,
                value=200.0,
                currency="USD",
            ),
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=500.0,
                currency="USD",
            ),
            fact(
                concept="StockholdersEquity",
                fiscal_period="FY",
                end_date=fy_latest,
                value=1000.0,
                currency="USD",
            ),
            fact(
                concept="StockholdersEquity",
                fiscal_period="FY",
                end_date=fy_gap,
                value=900.0,
                currency="USD",
            ),
        ],
        "CashAndShortTermInvestments": [
            fact(
                concept="CashAndShortTermInvestments",
                fiscal_period="Q4",
                end_date=q4,
                value=100.0,
                currency="USD",
            ),
            fact(
                concept="CashAndShortTermInvestments",
                fiscal_period="FY",
                end_date=fy_latest,
                value=200.0,
                currency="USD",
            ),
            fact(
                concept="CashAndShortTermInvestments",
                fiscal_period="FY",
                end_date=fy_gap,
                value=180.0,
                currency="USD",
            ),
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_avg_ic_returns_none_when_no_quarterly_or_fy_pairs():
    metric = AvgICMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=60.0,
                currency="USD",
            )
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=500.0,
                currency="USD",
            )
        ],
        "CashAndShortTermInvestments": [
            fact(
                concept="CashAndShortTermInvestments",
                fiscal_period="Q4",
                end_date=q4,
                value=100.0,
                currency="USD",
            )
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_avg_ic_returns_none_on_cross_point_currency_mismatch():
    metric = AvgICMetric()
    q4 = (date.today() - timedelta(days=20)).isoformat()
    q4_prev = (date.today() - timedelta(days=380)).isoformat()
    concept_records = {
        "ShortTermDebt": [
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=60.0,
                currency="USD",
            ),
            fact(
                concept="ShortTermDebt",
                fiscal_period="Q4",
                end_date=q4_prev,
                value=50.0,
                currency="EUR",
            ),
        ],
        "LongTermDebt": [
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=q4,
                value=140.0,
                currency="USD",
            ),
            fact(
                concept="LongTermDebt",
                fiscal_period="Q4",
                end_date=q4_prev,
                value=100.0,
                currency="EUR",
            ),
        ],
        "StockholdersEquity": [
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4,
                value=500.0,
                currency="USD",
            ),
            fact(
                concept="StockholdersEquity",
                fiscal_period="Q4",
                end_date=q4_prev,
                value=450.0,
                currency="EUR",
            ),
        ],
        "CashAndCashEquivalents": [
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=q4,
                value=100.0,
                currency="USD",
            ),
            fact(
                concept="CashAndCashEquivalents",
                fiscal_period="Q4",
                end_date=q4_prev,
                value=90.0,
                currency="EUR",
            ),
        ],
    }
    repo = _build_ic_repo(concept_records=concept_records)
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


def test_roic_ttm_metric():
    metric = RoicTTMMetric()
    repo = _build_ic_repo(concept_records=_base_roic_concepts())
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert round(result.value, 6) == round(320.0 / 555.0, 6)


def test_roic_ttm_uses_fy_tax_proxy_when_ttm_rate_invalid():
    metric = RoicTTMMetric()
    repo = _build_ic_repo(
        concept_records=_base_roic_concepts(
            quarterly_pretax_values=(0.0, 0.0, 0.0, 0.0),
        )
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert round(result.value, 6) == round(280.0 / 555.0, 6)


def test_roic_ttm_uses_default_tax_rate_when_no_valid_tax_inputs():
    metric = RoicTTMMetric()
    repo = _build_ic_repo(
        concept_records=_base_roic_concepts(
            include_ttm_tax=False,
            include_ttm_pretax=False,
            include_fy_tax_proxy=False,
        )
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert round(result.value, 6) == round(316.0 / 555.0, 6)


def test_roic_ttm_returns_none_when_ebit_missing():
    metric = RoicTTMMetric()
    concepts = _base_roic_concepts()
    concepts["OperatingIncomeLoss"] = concepts["OperatingIncomeLoss"][:3]
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_roic_ttm_returns_none_when_ebit_stale():
    metric = RoicTTMMetric()
    stale_dates = [
        (date.today() - timedelta(days=500)).isoformat(),
        (date.today() - timedelta(days=590)).isoformat(),
        (date.today() - timedelta(days=680)).isoformat(),
        (date.today() - timedelta(days=770)).isoformat(),
    ]
    concepts = _base_roic_concepts()
    concepts["OperatingIncomeLoss"] = [
        fact(
            concept="OperatingIncomeLoss",
            fiscal_period=period,
            end_date=end_date,
            value=100.0,
            currency="USD",
        )
        for period, end_date in zip(("Q4", "Q3", "Q2", "Q1"), stale_dates, strict=True)
    ]
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_roic_ttm_returns_none_when_avg_ic_missing():
    metric = RoicTTMMetric()
    repo = _build_ic_repo(
        concept_records=_base_roic_concepts(
            include_avg_ic=False,
        )
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_roic_ttm_returns_none_when_nopat_non_positive():
    metric = RoicTTMMetric()
    repo = _build_ic_repo(
        concept_records=_base_roic_concepts(
            include_ttm_tax=False,
            include_ttm_pretax=False,
            include_fy_tax_proxy=False,
            quarterly_ebit_values=(-100.0, -100.0, -100.0, -100.0),
        )
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_roic_ttm_returns_none_when_avg_ic_non_positive():
    metric = RoicTTMMetric()
    repo = _build_ic_repo(
        concept_records=_base_roic_concepts(
            avg_latest=(60.0, 140.0, 100.0, 500.0),
            avg_prior=(50.0, 100.0, 100.0, 450.0),
        )
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_roic_ttm_returns_none_on_numerator_currency_mismatch():
    metric = RoicTTMMetric()
    concepts = _base_roic_concepts()
    concepts["OperatingIncomeLoss"][1] = fact(
        concept="OperatingIncomeLoss",
        fiscal_period="Q3",
        end_date=concepts["OperatingIncomeLoss"][1].end_date,
        value=100.0,
        currency="EUR",
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_roic_ttm_returns_none_on_numerator_vs_avg_ic_currency_mismatch():
    metric = RoicTTMMetric()
    repo = _build_ic_repo(
        concept_records=_base_roic_concepts(
            avg_currency="EUR",
        )
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_roic_10y_metrics_happy_path():
    median_metric = ROIC10YMedianMetric()
    count_metric = ROICYearsAbove12PctMetric()
    min_metric = ROIC10YMinMetric()
    repo = _build_ic_repo(concept_records=_base_roic_10y_concepts())

    median_result = median_metric.compute("AAPL.US", repo)
    count_result = count_metric.compute("AAPL.US", repo)
    min_result = min_metric.compute("AAPL.US", repo)

    assert median_result is not None
    assert count_result is not None
    assert min_result is not None
    assert round(median_result.value, 6) == 0.15
    assert count_result.value == 6.0
    assert round(min_result.value, 6) == 0.06


def test_roic_10y_returns_none_when_strict_window_missing_year():
    metric = ROIC10YMedianMetric()
    latest_year = date.today().year - 1
    concepts = _base_roic_10y_concepts()
    concepts["OperatingIncomeLoss"] = [
        rec
        for rec in concepts["OperatingIncomeLoss"]
        if rec.end_date != f"{latest_year - 5}-09-30"
    ]
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_roic_10y_tax_fallback_uses_latest_valid_fy_proxy():
    metric = ROICYearsAbove12PctMetric()
    latest_year = date.today().year - 1
    roic_years = range(latest_year - 9, latest_year + 1)
    ebit = {year: 200.0 for year in roic_years}
    tax = {year: 80.0 for year in roic_years}
    pretax = {year: 200.0 for year in roic_years}
    pretax[latest_year] = 0.0
    repo = _build_ic_repo(
        concept_records=_base_roic_10y_concepts(
            latest_year=latest_year,
            ebit_by_year=ebit,
            tax_by_year=tax,
            pretax_by_year=pretax,
        )
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 0.0


def test_roic_10y_tax_fallback_uses_default_when_no_valid_proxy():
    metric = ROIC10YMedianMetric()
    latest_year = date.today().year - 1
    roic_years = range(latest_year - 9, latest_year + 1)
    ebit = {year: 200.0 for year in roic_years}
    tax = {year: 80.0 for year in roic_years}
    pretax = {year: 0.0 for year in roic_years}
    repo = _build_ic_repo(
        concept_records=_base_roic_10y_concepts(
            latest_year=latest_year,
            ebit_by_year=ebit,
            tax_by_year=tax,
            pretax_by_year=pretax,
        )
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert round(result.value, 6) == round(0.158, 6)


def test_roic_10y_min_keeps_signed_negative_year():
    metric = ROIC10YMinMetric()
    latest_year = date.today().year - 1
    concepts = _base_roic_10y_concepts()
    concepts["OperatingIncomeLoss"] = [
        fact(
            concept=rec.concept,
            fiscal_period=rec.fiscal_period,
            end_date=rec.end_date,
            value=-50.0 if rec.end_date == f"{latest_year - 9}-09-30" else rec.value,
            currency=rec.currency,
        )
        for rec in concepts["OperatingIncomeLoss"]
    ]
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert round(result.value, 6) == -0.04


def test_roic_10y_returns_none_when_avg_ic_year_pair_is_zero():
    metric = ROIC10YMedianMetric()
    latest_year = date.today().year - 1
    concepts = _base_roic_10y_concepts(
        ic_cash_by_year={
            year: (2300.0 if year == latest_year - 1 else 300.0)
            for year in range(latest_year - 10, latest_year + 1)
        }
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_roic_10y_returns_none_on_series_currency_conflict():
    metric = ROIC10YMedianMetric()
    latest_year = date.today().year - 1
    concepts = _base_roic_10y_concepts(
        currency_by_year={latest_year - 3: "EUR"},
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_roic_10y_returns_none_when_latest_fy_stale():
    metric = ROIC10YMedianMetric()
    stale_latest_year = date.today().year - 3
    repo = _build_ic_repo(
        concept_records=_base_roic_10y_concepts(latest_year=stale_latest_year)
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


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


def test_fcf_to_debt_skips_non_positive_fcf():
    metric = FCFToDebtMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    concept_records = {
        "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
            "NetCashProvidedByUsedInOperatingActivities",
            quarter_dates,
            (50.0, 50.0, 50.0, 50.0),
        ),
        "CapitalExpenditures": _quarterly_records(
            "CapitalExpenditures",
            quarter_dates,
            (60.0, 60.0, 60.0, 60.0),
        ),
    }
    repo = _build_fcf_debt_repo(
        concept_records=concept_records,
        latest_records=_default_debt_paydown_latest_records(q4),
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_fcf_and_debt_paydown_skip_non_positive_debt():
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    latest = _default_debt_paydown_latest_records(q4)
    latest["ShortTermDebt"] = fact(
        concept="ShortTermDebt",
        end_date=q4,
        value=0.0,
        currency="USD",
    )
    latest["LongTermDebt"] = fact(
        concept="LongTermDebt",
        end_date=q4,
        value=0.0,
        currency="USD",
    )
    repo = _build_fcf_debt_repo(
        concept_records=_base_debt_paydown_concepts(quarter_dates),
        latest_records=latest,
    )

    assert DebtPaydownYearsMetric().compute("AAPL.US", repo) is None
    assert FCFToDebtMetric().compute("AAPL.US", repo) is None


def test_fcf_to_debt_uses_capex_zero_when_missing():
    metric = FCFToDebtMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    concept_records = {
        "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
            "NetCashProvidedByUsedInOperatingActivities",
            quarter_dates,
            (100.0, 100.0, 100.0, 100.0),
        )
    }
    repo = _build_fcf_debt_repo(
        concept_records=concept_records,
        latest_records=_default_debt_paydown_latest_records(q4),
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 2.0


def test_fcf_and_debt_paydown_return_none_on_currency_mismatch():
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    concept_records = {
        "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
            "NetCashProvidedByUsedInOperatingActivities",
            quarter_dates,
            (100.0, 100.0, 100.0, 100.0),
            currency="GBP",
        ),
        "CapitalExpenditures": _quarterly_records(
            "CapitalExpenditures",
            quarter_dates,
            (50.0, 50.0, 50.0, 50.0),
            currency="GBP",
        ),
    }
    repo = _build_fcf_debt_repo(
        concept_records=concept_records,
        latest_records=_default_debt_paydown_latest_records(q4),
    )

    assert DebtPaydownYearsMetric().compute("AAPL.US", repo) is None
    assert FCFToDebtMetric().compute("AAPL.US", repo) is None


def test_registry_includes_fcf_to_debt_metric():
    assert "fcf_to_debt" in REGISTRY


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


def test_interest_coverage_uses_derived_interest_fallback():
    metric = InterestCoverageMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "OperatingIncomeLoss":
                return _quarterly_records(
                    concept, (q4, q3, q2, q1), (40.0, 30.0, 20.0, 10.0)
                )
            if concept == "InterestExpense":
                return _quarterly_records(concept, (q4, q3), (4.0, 3.0))
            if concept == "InterestExpenseFromNetInterestIncome":
                return _quarterly_records(concept, (q2, q1), (2.0, 1.0))
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 10.0


def test_interest_coverage_keeps_direct_path_when_valid():
    metric = InterestCoverageMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "OperatingIncomeLoss":
                return _quarterly_records(
                    concept, (q4, q3, q2, q1), (40.0, 30.0, 20.0, 10.0)
                )
            if concept == "InterestExpense":
                return _quarterly_records(
                    concept, (q4, q3, q2, q1), (4.0, 3.0, 2.0, 1.0)
                )
            if concept == "InterestExpenseFromNetInterestIncome":
                return _quarterly_records(
                    concept, (q4, q3, q2, q1), (40.0, 30.0, 20.0, 10.0)
                )
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 10.0


def test_interest_coverage_returns_none_when_fallback_insufficient():
    metric = InterestCoverageMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "OperatingIncomeLoss":
                return _quarterly_records(
                    concept, (q4, q3, q2, q1), (40.0, 30.0, 20.0, 10.0)
                )
            if concept == "InterestExpense":
                return _quarterly_records(concept, (q4, q3), (4.0, 3.0))
            if concept == "InterestExpenseFromNetInterestIncome":
                return _quarterly_records(concept, (q2,), (2.0,))
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_interest_coverage_returns_none_on_fallback_currency_mismatch():
    metric = InterestCoverageMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "OperatingIncomeLoss":
                return _quarterly_records(
                    concept, (q4, q3, q2, q1), (40.0, 30.0, 20.0, 10.0)
                )
            if concept == "InterestExpense":
                return _quarterly_records(concept, (q4, q3), (4.0, 3.0))
            if concept == "InterestExpenseFromNetInterestIncome":
                return _quarterly_records(concept, (q2, q1), (2.0, 1.0), currency="EUR")
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_interest_coverage_normalizes_gbx_to_gbp():
    metric = InterestCoverageMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()

    class DummyRepo:
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            if concept == "OperatingIncomeLoss":
                return _quarterly_records(
                    concept, (q4, q3, q2, q1), (4.0, 3.0, 2.0, 1.0), currency="GBP"
                )
            if concept == "InterestExpense":
                return _quarterly_records(
                    concept,
                    (q4, q3, q2, q1),
                    (40.0, 30.0, 20.0, 10.0),
                    currency="GBX",
                )
            return []

    repo = DummyRepo()
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 10.0


def test_net_debt_to_ebitda_skips_non_positive_ebitda():
    metric = NetDebtToEBITDAMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    repo = _build_net_debt_repo(
        concept_records=_base_ebit_da_concepts(
            quarter_dates,
            ebit_values=(0.0, 0.0, 0.0, 0.0),
            da_values=(0.0, 0.0, 0.0, 0.0),
        ),
        latest_records=_default_net_debt_latest_records(q4),
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_net_debt_to_ebitda_allows_single_debt_side():
    metric = NetDebtToEBITDAMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    latest_records = _default_net_debt_latest_records(q4)
    latest_records.pop("ShortTermDebt")
    repo = _build_net_debt_repo(
        concept_records=_base_ebit_da_concepts(quarter_dates),
        latest_records=latest_records,
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 0.7


def test_net_debt_to_ebitda_requires_at_least_one_debt_component():
    metric = NetDebtToEBITDAMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    repo = _build_net_debt_repo(
        concept_records=_base_ebit_da_concepts(quarter_dates),
        latest_records={
            "CashAndShortTermInvestments": fact(
                concept="CashAndShortTermInvestments",
                end_date=q4,
                value=20.0,
                currency="USD",
            )
        },
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_net_debt_to_ebitda_uses_cash_component_fallback():
    metric = NetDebtToEBITDAMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    latest_records = _default_net_debt_latest_records(q4)
    latest_records.pop("CashAndShortTermInvestments")
    latest_records["CashAndCashEquivalents"] = fact(
        concept="CashAndCashEquivalents",
        end_date=q4,
        value=15.0,
        currency="USD",
    )
    latest_records["ShortTermInvestments"] = fact(
        concept="ShortTermInvestments",
        end_date=q4,
        value=5.0,
        currency="USD",
    )
    repo = _build_net_debt_repo(
        concept_records=_base_ebit_da_concepts(quarter_dates),
        latest_records=latest_records,
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 0.8


def test_net_debt_to_ebitda_cash_component_fallback_allows_missing_sti():
    metric = NetDebtToEBITDAMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    latest_records = _default_net_debt_latest_records(q4)
    latest_records.pop("CashAndShortTermInvestments")
    latest_records["CashAndCashEquivalents"] = fact(
        concept="CashAndCashEquivalents",
        end_date=q4,
        value=20.0,
        currency="USD",
    )
    repo = _build_net_debt_repo(
        concept_records=_base_ebit_da_concepts(quarter_dates),
        latest_records=latest_records,
    )
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 0.8


def test_net_debt_to_ebitda_requires_cash_source():
    metric = NetDebtToEBITDAMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    latest_records = _default_net_debt_latest_records(q4)
    latest_records.pop("CashAndShortTermInvestments")
    repo = _build_net_debt_repo(
        concept_records=_base_ebit_da_concepts(quarter_dates),
        latest_records=latest_records,
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_net_debt_to_ebitda_returns_none_on_denominator_currency_mismatch():
    metric = NetDebtToEBITDAMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    repo = _build_net_debt_repo(
        concept_records=_base_ebit_da_concepts(quarter_dates, da_currency="EUR"),
        latest_records=_default_net_debt_latest_records(q4),
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_net_debt_to_ebitda_returns_none_on_net_debt_currency_mismatch():
    metric = NetDebtToEBITDAMetric()
    quarter_dates = _net_debt_quarter_dates()
    q4 = quarter_dates[0]
    latest_records = _default_net_debt_latest_records(q4)
    latest_records["CashAndShortTermInvestments"] = fact(
        concept="CashAndShortTermInvestments",
        end_date=q4,
        value=20.0,
        currency="EUR",
    )
    repo = _build_net_debt_repo(
        concept_records=_base_ebit_da_concepts(quarter_dates),
        latest_records=latest_records,
    )
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

    def latest_fact(self, symbol, concept):
        records = self.facts_for_concept(symbol, concept)
        if not records:
            return None
        return max(records, key=lambda record: record.end_date)


def _build_oe_ev_ttm_input_records(
    *,
    symbol: str,
    q4: str,
    q3: str,
    q2: str,
    q1: str,
    latest_year: int,
    ebit: float = 200.0,
    tax: float = 40.0,
    pretax: float = 200.0,
    capex: float = 100.0,
    da: float | None = 90.0,
    base_currency: str = "USD",
) -> dict[str, list[FactRecord]]:
    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [150.0, 130.0, 110.0, 90.0]
    )
    periods = [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
    records_by_concept.update(
        {
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period=period,
                    end_date=end_date,
                    value=ebit,
                    currency=base_currency,
                )
                for end_date, period in periods
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period=period,
                    end_date=end_date,
                    value=tax,
                    currency=base_currency,
                )
                for end_date, period in periods
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period=period,
                    end_date=end_date,
                    value=pretax,
                    currency=base_currency,
                )
                for end_date, period in periods
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period=period,
                    end_date=end_date,
                    value=capex,
                    currency=base_currency,
                )
                for end_date, period in periods
            ],
        }
    )
    if da is not None:
        records_by_concept["DepreciationDepletionAndAmortization"] = [
            fact(
                symbol=symbol,
                concept="DepreciationDepletionAndAmortization",
                fiscal_period=period,
                end_date=end_date,
                value=da,
                currency=base_currency,
            )
            for end_date, period in periods
        ]
    return records_by_concept


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


def test_oe_ev_ttm_metric_computes_formula():
    metric = OwnerEarningsEnterpriseTTMMetric()
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
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period=period,
                    end_date=end_date,
                    value=200.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period=period,
                    end_date=end_date,
                    value=40.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period=period,
                    end_date=end_date,
                    value=200.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period=period,
                    end_date=end_date,
                    value=90.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period=period,
                    end_date=end_date,
                    value=100.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.as_of == q4
    assert result.value == 584.0


def test_oe_ev_ttm_metric_uses_fy_tax_rate_fallback():
    metric = OwnerEarningsEnterpriseTTMMetric()
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
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period=period,
                    end_date=end_date,
                    value=100.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=5.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=5.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=5.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=5.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period="FY",
                    end_date=f"{latest_year}-09-30",
                    value=30.0,
                    currency="USD",
                ),
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=-10.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=-10.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=-10.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=-10.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="FY",
                    end_date=f"{latest_year}-09-30",
                    value=100.0,
                    currency="USD",
                ),
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period=period,
                    end_date=end_date,
                    value=20.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period=period,
                    end_date=end_date,
                    value=15.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 280.0


def test_oe_ev_ttm_metric_uses_default_tax_rate_when_no_valid_proxy():
    metric = OwnerEarningsEnterpriseTTMMetric()
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
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period=period,
                    end_date=end_date,
                    value=100.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period=period,
                    end_date=end_date,
                    value=5.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period=period,
                    end_date=end_date,
                    value=-10.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period=period,
                    end_date=end_date,
                    value=30.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 176.0


def test_oe_ev_ttm_metric_treats_missing_da_as_zero():
    metric = OwnerEarningsEnterpriseTTMMetric()
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
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period=period,
                    end_date=end_date,
                    value=80.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period=period,
                    end_date=end_date,
                    value=16.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period=period,
                    end_date=end_date,
                    value=80.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period=period,
                    end_date=end_date,
                    value=20.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 156.0


def test_oe_ev_ttm_metric_requires_delta_nwc_maint():
    metric = OwnerEarningsEnterpriseTTMMetric()
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
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period=period,
                    end_date=end_date,
                    value=100.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period=period,
                    end_date=end_date,
                    value=20.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period=period,
                    end_date=end_date,
                    value=100.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period=period,
                    end_date=end_date,
                    value=40.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is None


def test_oe_ev_ttm_metric_currency_mismatch_returns_none():
    metric = OwnerEarningsEnterpriseTTMMetric()
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
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period=period,
                    end_date=end_date,
                    value=100.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period=period,
                    end_date=end_date,
                    value=20.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period=period,
                    end_date=end_date,
                    value=100.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "DepreciationDepletionAndAmortization": [
                fact(
                    symbol=symbol,
                    concept="DepreciationDepletionAndAmortization",
                    fiscal_period=period,
                    end_date=end_date,
                    value=30.0,
                    currency="EUR",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period=period,
                    end_date=end_date,
                    value=30.0,
                    currency="EUR",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is None


def test_oe_ev_ttm_metric_allows_negative_values():
    metric = OwnerEarningsEnterpriseTTMMetric()
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
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period=period,
                    end_date=end_date,
                    value=10.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period=period,
                    end_date=end_date,
                    value=2.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period=period,
                    end_date=end_date,
                    value=10.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period=period,
                    end_date=end_date,
                    value=30.0,
                    currency="USD",
                )
                for end_date, period in [(q4, "Q4"), (q3, "Q3"), (q2, "Q2"), (q1, "Q1")]
            ],
        }
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == -108.0


def test_oe_ev_5y_avg_metric_computes_expected_average():
    metric = OwnerEarningsEnterpriseFiveYearAverageMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [300.0, 250.0, 230.0, 210.0, 190.0, 170.0]
    )
    records_by_concept.update(
        {
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=value,
                    currency="USD",
                )
                for year, value in zip(years, [500.0, 450.0, 400.0, 350.0, 300.0])
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=value,
                    currency="USD",
                )
                for year, value in zip(years, [100.0, 90.0, 80.0, 70.0, 60.0])
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=value,
                    currency="USD",
                )
                for year, value in zip(years, [500.0, 450.0, 400.0, 350.0, 300.0])
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
    assert result.as_of == f"{years[0]}-09-30"
    assert result.value == 300.0


def test_oe_ev_5y_avg_metric_requires_five_points():
    metric = OwnerEarningsEnterpriseFiveYearAverageMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(4)]

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [300.0, 250.0, 230.0, 210.0, 190.0, 170.0]
    )
    records_by_concept.update(
        {
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=400.0,
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


def test_oe_ev_5y_avg_metric_allows_year_gaps():
    metric = OwnerEarningsEnterpriseFiveYearAverageMetric()
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
        symbol, latest_year, [300.0, 250.0, 230.0, 210.0, 190.0, 170.0, 150.0]
    )
    records_by_concept.update(
        {
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=value,
                    currency="USD",
                )
                for year, value in zip(fy_years, [500.0, 420.0, 380.0, 320.0, 280.0])
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=value,
                    currency="USD",
                )
                for year, value in zip(fy_years, [100.0, 84.0, 76.0, 64.0, 56.0])
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=value,
                    currency="USD",
                )
                for year, value in zip(fy_years, [500.0, 420.0, 380.0, 320.0, 280.0])
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
    assert result.value == 284.0


def test_oe_ev_5y_avg_metric_uses_latest_delta_nwc_maint_for_all_years():
    metric = OwnerEarningsEnterpriseFiveYearAverageMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [300.0, 250.0, 230.0, 210.0, 190.0, 170.0]
    )
    records_by_concept.update(
        {
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=200.0,
                    currency="USD",
                )
                for year in years
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=40.0,
                    currency="USD",
                )
                for year in years
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=200.0,
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
    assert result.value == 140.0


def test_oe_ev_5y_avg_metric_requires_consistent_currency_across_years():
    metric = OwnerEarningsEnterpriseFiveYearAverageMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]
    currencies = ["USD", "USD", "USD", "EUR", "EUR"]

    records_by_concept = _build_nwc_fy_records(
        symbol, latest_year, [300.0, 250.0, 230.0, 210.0, 190.0, 170.0]
    )
    records_by_concept.update(
        {
            "OperatingIncomeLoss": [
                fact(
                    symbol=symbol,
                    concept="OperatingIncomeLoss",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=300.0,
                    currency=currency,
                )
                for year, currency in zip(years, currencies)
            ],
            "IncomeTaxExpense": [
                fact(
                    symbol=symbol,
                    concept="IncomeTaxExpense",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=60.0,
                    currency=currency,
                )
                for year, currency in zip(years, currencies)
            ],
            "IncomeBeforeIncomeTaxes": [
                fact(
                    symbol=symbol,
                    concept="IncomeBeforeIncomeTaxes",
                    fiscal_period="FY",
                    end_date=f"{year}-09-30",
                    value=300.0,
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


def test_oey_ev_metric_uses_normalized_enterprise_value_denominator():
    metric = OwnerEarningsYieldEVMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_oe_ev_ttm_input_records(
        symbol=symbol,
        q4=q4,
        q3=q3,
        q2=q2,
        q1=q1,
        latest_year=latest_year,
    )
    records_by_concept["EnterpriseValue"] = [
        fact(
            symbol=symbol,
            concept="EnterpriseValue",
            end_date=q4,
            value=5840.0,
            currency="USD",
            fiscal_period="",
        )
    ]

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            return None

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is not None
    assert result.as_of == q4
    assert result.value == 0.1


def test_oey_ev_metric_falls_back_to_derived_ev_when_primary_missing():
    metric = OwnerEarningsYieldEVMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_oe_ev_ttm_input_records(
        symbol=symbol,
        q4=q4,
        q3=q3,
        q2=q2,
        q1=q1,
        latest_year=latest_year,
    )
    records_by_concept["LongTermDebt"] = [
        fact(
            symbol=symbol,
            concept="LongTermDebt",
            fiscal_period="FY",
            end_date=f"{latest_year}-09-30",
            value=300.0,
            currency="USD",
        )
    ]

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 1000.0
                as_of = q4
                currency = "USD"

            return Snapshot()

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is not None
    assert result.value == 584.0 / 1250.0


def test_oey_ev_metric_falls_back_to_derived_ev_when_primary_non_positive():
    metric = OwnerEarningsYieldEVMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_oe_ev_ttm_input_records(
        symbol=symbol,
        q4=q4,
        q3=q3,
        q2=q2,
        q1=q1,
        latest_year=latest_year,
    )
    records_by_concept["EnterpriseValue"] = [
        fact(
            symbol=symbol,
            concept="EnterpriseValue",
            end_date=q4,
            value=0.0,
            currency="USD",
            fiscal_period="",
        )
    ]
    records_by_concept["LongTermDebt"] = [
        fact(
            symbol=symbol,
            concept="LongTermDebt",
            fiscal_period="FY",
            end_date=f"{latest_year}-09-30",
            value=300.0,
            currency="USD",
        )
    ]

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 1000.0
                as_of = q4
                currency = "USD"

            return Snapshot()

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is not None
    assert result.value == 584.0 / 1250.0


def test_oey_ev_metric_returns_none_when_ev_primary_and_fallback_unavailable():
    metric = OwnerEarningsYieldEVMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_oe_ev_ttm_input_records(
        symbol=symbol,
        q4=q4,
        q3=q3,
        q2=q2,
        q1=q1,
        latest_year=latest_year,
    )

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = None
                as_of = q4
                currency = "USD"

            return Snapshot()

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is None


def test_oey_ev_metric_applies_fx_conversion(monkeypatch):
    metric = OwnerEarningsYieldEVMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_oe_ev_ttm_input_records(
        symbol=symbol,
        q4=q4,
        q3=q3,
        q2=q2,
        q1=q1,
        latest_year=latest_year,
    )
    records_by_concept["EnterpriseValue"] = [
        fact(
            symbol=symbol,
            concept="EnterpriseValue",
            end_date=q4,
            value=100.0,
            currency="EUR",
            fiscal_period="",
        )
    ]

    monkeypatch.setattr(
        "pyvalue.metrics.owner_earnings_yield.FXRateStore.convert",
        lambda self, amount, from_currency, to_currency, as_of: amount * 2.0,
    )

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            return None

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is not None
    assert result.value == 584.0 / 200.0


def test_oey_ev_metric_returns_none_when_fx_conversion_fails(monkeypatch):
    metric = OwnerEarningsYieldEVMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_oe_ev_ttm_input_records(
        symbol=symbol,
        q4=q4,
        q3=q3,
        q2=q2,
        q1=q1,
        latest_year=latest_year,
    )
    records_by_concept["EnterpriseValue"] = [
        fact(
            symbol=symbol,
            concept="EnterpriseValue",
            end_date=q4,
            value=100.0,
            currency="EUR",
            fiscal_period="",
        )
    ]

    monkeypatch.setattr(
        "pyvalue.metrics.owner_earnings_yield.FXRateStore.convert",
        lambda self, amount, from_currency, to_currency, as_of: None,
    )

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            return None

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is None


def test_oey_ev_metric_allows_negative_values():
    metric = OwnerEarningsYieldEVMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    latest_year = date.today().year - 1

    records_by_concept = _build_oe_ev_ttm_input_records(
        symbol=symbol,
        q4=q4,
        q3=q3,
        q2=q2,
        q1=q1,
        latest_year=latest_year,
        ebit=10.0,
        tax=2.0,
        pretax=10.0,
        capex=30.0,
        da=None,
    )
    records_by_concept["EnterpriseValue"] = [
        fact(
            symbol=symbol,
            concept="EnterpriseValue",
            end_date=q4,
            value=1080.0,
            currency="USD",
            fiscal_period="",
        )
    ]

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            return None

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is not None
    assert result.value == -0.1


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
    assert "oey_ev" in REGISTRY
    assert "oe_ev_ttm" in REGISTRY
    assert "oe_ev_5y_avg" in REGISTRY
    assert "short_term_debt_share" in REGISTRY
    assert "ic_mqr" in REGISTRY
    assert "ic_fy" in REGISTRY
    assert "avg_ic" in REGISTRY
    assert "roic_ttm" in REGISTRY
    assert "roic_10y_median" in REGISTRY
    assert "roic_years_above_12pct" in REGISTRY
    assert "roic_10y_min" in REGISTRY
