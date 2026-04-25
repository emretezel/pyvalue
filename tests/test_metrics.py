"""Tests for metric implementations.

Author: Emre Tezel
"""

from datetime import date, timedelta
from pathlib import Path

import pytest

from pyvalue.currency import normalize_currency_code
from pyvalue.metrics import REGISTRY
from pyvalue.metrics.accruals_ratio import AccrualsRatioMetric
from pyvalue.metrics.base import MetricCurrencyInvariantError, metadata_for_metric
from pyvalue.metrics.buyback_yield import NetBuybackYieldMetric
from pyvalue.metrics.cash_conversion import (
    CFOToNITenYearMedianMetric,
    CFOToNITTMMetric,
)
from pyvalue.metrics.fundamental_consistency import (
    FCFFiveYearMedianMetric,
    FCFNegativeYearsTenYearMetric,
    NetIncomeLossYearsTenYearMetric,
)
from pyvalue.metrics.sbc_load import SBCToFCFMetric, SBCToRevenueMetric
from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.metrics.debt_paydown_years import DebtPaydownYearsMetric, FCFToDebtMetric
from pyvalue.metrics.earnings_yield import EarningsYieldMetric
from pyvalue.metrics.enterprise_value_ratios import (
    EBITYieldEVMetric,
    EVToEBITDAMetric,
    EVToEBITMetric,
    FCFYieldEVMetric,
)
from pyvalue.metrics.eps_average import EPSAverageSixYearMetric
from pyvalue.metrics.eps_quarterly import EarningsPerShareTTM
from pyvalue.metrics.eps_streak import EPSStreakMetric
from pyvalue.metrics.graham_eps_cagr import GrahamEPSCAGRMetric
from pyvalue.metrics.graham_multiplier import GrahamMultiplierMetric
from pyvalue.metrics.gross_margin_stability import GrossMarginTenYearStdMetric
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
    OwnerEarningsYieldEVNormalizedMetric,
)
from pyvalue.metrics.owner_earnings_enterprise import (
    OwnerEarningsEnterpriseFiveYearAverageMetric,
    OwnerEarningsEnterpriseFiveYearMedianMetric,
    OwnerEarningsEnterpriseTTMMetric,
    WorstOwnerEarningsEnterpriseTenYearMetric,
)
from pyvalue.metrics.operating_margin_stability import (
    OperatingMarginSevenYearMinMetric,
    OperatingMarginTenYearMinMetric,
    OperatingMarginTenYearStdMetric,
)
from pyvalue.metrics.price_to_fcf import PriceToFCFMetric
from pyvalue.metrics.profitability_returns_growth import (
    DividendPayoutRatioTTMMetric,
    DividendYieldTTMMetric,
    FCFMarginTTMMetric,
    FCFPerShareCAGR10YMetric,
    GrossMarginTTMMetric,
    GrossProfitToAssetsTTMMetric,
    OperatingMarginTTMMetric,
    OwnerEarningsCAGR10YMetric,
    ROATTMMetric,
    ROETTMMetric,
    ROETangibleCommonEquityTTMMetric,
    RevenueCAGR10YMetric,
    ShareholderYieldTTMMetric,
)
from pyvalue.metrics.roc_greenblatt import ROCGreenblattMetric
from pyvalue.metrics.roic_fy_series import (
    IncrementalROICFiveYearMetric,
    ROIC10YMedianMetric,
    ROIC10YMinMetric,
    ROIC7YMedianMetric,
    ROIC7YMinMetric,
    ROICFYSeriesCalculator,
    ROICYearsAbove12PctMetric,
)
from pyvalue.metrics.roic_ttm import RoicTTMMetric
from pyvalue.metrics.roe_greenblatt import ROEGreenblattMetric
from pyvalue.metrics.return_on_invested_capital import ReturnOnInvestedCapitalMetric
from pyvalue.metrics.share_count_change import (
    ShareCountCAGR5YMetric,
    ShareCountCAGR10YMetric,
    Shares10YPctChangeMetric,
)
from pyvalue.metrics.short_term_debt_share import ShortTermDebtShareMetric
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, MAX_FY_FACT_AGE_DAYS
from pyvalue.metrics.working_capital import WorkingCapitalMetric
from pyvalue.storage import FactRecord


class _USDTickerCurrencyRepo:
    def ticker_currency(self, symbol):
        return "USD"


class _GBPTickerCurrencyRepo:
    def ticker_currency(self, symbol):
        return "GBP"


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
    if "currency" not in kwargs:
        inferred_currency = normalize_currency_code(base.get("unit"))
        if inferred_currency is not None:
            base["currency"] = inferred_currency
    return FactRecord(**base)


def test_metric_modules_do_not_use_metric_side_fx_helpers():
    metrics_dir = Path("src/pyvalue/metrics")
    banned_tokens = (
        "fx_service_for_context",
        "fx_converter_for_context",
        "align_money_values",
        "convert_denominator_amount",
        "from pyvalue.fx import FXService",
        ".convert_amount(",
    )
    allowed_files = {"utils.py"}

    offending: list[tuple[str, str]] = []
    for path in sorted(metrics_dir.glob("*.py")):
        if path.name in allowed_files:
            continue
        text = path.read_text(encoding="utf-8")
        for token in banned_tokens:
            if token in text:
                offending.append((path.name, token))

    assert offending == []


def test_current_ratio_metric_returns_none_for_fact_currency_mismatch():
    metric = CurrentRatioMetric()
    symbol = "AAPL.US"
    today = date.today().isoformat()
    repo = _OwnerEarningsRepo(
        {
            "AssetsCurrent": [
                fact(
                    symbol=symbol,
                    concept="AssetsCurrent",
                    fiscal_period="Q4",
                    end_date=today,
                    value=150.0,
                    currency="EUR",
                )
            ],
            "LiabilitiesCurrent": [
                fact(
                    symbol=symbol,
                    concept="LiabilitiesCurrent",
                    fiscal_period="Q4",
                    end_date=today,
                    value=100.0,
                    currency="USD",
                )
            ],
        },
        ticker_currency="USD",
    )

    assert metric.compute(symbol, repo) is None


def test_market_capitalization_metric_uses_listing_currency_for_market_cap():
    metric = MarketCapitalizationMetric()
    symbol = "AAPL.US"

    result = metric.compute(
        symbol,
        _OwnerEarningsRepo({}, ticker_currency="USD"),
        _build_market_repo(
            market_cap=100.0,
            as_of=date.today().isoformat(),
            currency="EUR",
            ticker_currency="USD",
        ),
    )
    assert result is not None
    assert result.value == 100.0
    assert result.currency == "USD"


def test_fx_rate_store_removed_from_public_api():
    import pyvalue.fx as fx

    assert not hasattr(fx, "FXRateStore")


def _net_debt_quarter_dates():
    today = date.today()
    return (
        (today - timedelta(days=30)).isoformat(),
        (today - timedelta(days=120)).isoformat(),
        (today - timedelta(days=210)).isoformat(),
        (today - timedelta(days=300)).isoformat(),
    )


def _build_net_debt_repo(
    *, concept_records=None, latest_records=None, ticker_currency="USD"
):
    concept_records = concept_records or {}
    latest_records = latest_records or {}

    class DummyRepo(_GBPTickerCurrencyRepo):
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            return concept_records.get(concept, [])

        def latest_fact(self, symbol, concept):
            return latest_records.get(concept)

        def ticker_currency(self, symbol):
            return ticker_currency

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


def _build_fcf_debt_repo(
    *, concept_records=None, latest_records=None, ticker_currency="USD"
):
    concept_records = concept_records or {}
    latest_records = latest_records or {}

    class DummyRepo(_GBPTickerCurrencyRepo):
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            return concept_records.get(concept, [])

        def latest_fact(self, symbol, concept):
            return latest_records.get(concept)

        def ticker_currency(self, symbol):
            return ticker_currency

    return DummyRepo()


def _build_ic_repo(*, concept_records=None, ticker_currency="USD"):
    concept_records = concept_records or {}

    class DummyRepo(_GBPTickerCurrencyRepo):
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            return concept_records.get(concept, [])

        def ticker_currency(self, symbol):
            return ticker_currency

    return DummyRepo()


def _build_metric_repo(
    *, concept_records=None, latest_records=None, ticker_currency="USD"
):
    concept_records = concept_records or {}
    latest_records = latest_records or {}

    class DummyRepo(_GBPTickerCurrencyRepo):
        def facts_for_concept(self, symbol, concept, fiscal_period=None, limit=None):
            records = concept_records.get(concept, [])
            if fiscal_period is None:
                return records
            return [
                record
                for record in records
                if (record.fiscal_period or "").upper() == fiscal_period.upper()
            ]

        def latest_fact(self, symbol, concept):
            if concept in latest_records:
                return latest_records[concept]
            records = concept_records.get(concept, [])
            return records[0] if records else None

        def ticker_currency(self, symbol):
            return ticker_currency

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


def _iroic_short_debt_ramp(latest_year, *, base=100.0, step=10.0):
    return {
        year: base + (year - (latest_year - 10)) * step
        for year in range(latest_year - 10, latest_year + 1)
    }


def _base_gm_10y_concepts(
    *,
    latest_year=None,
    revenue_by_year=None,
    gross_profit_by_year=None,
    cost_of_revenue_by_year=None,
    currency_by_year=None,
):
    if latest_year is None:
        latest_year = date.today().year - 1
    years = list(range(latest_year - 9, latest_year + 1))

    if revenue_by_year is None:
        revenue_by_year = {year: 1000.0 + 10.0 * idx for idx, year in enumerate(years)}
    if gross_profit_by_year is None:
        gross_profit_by_year = {
            year: revenue_by_year[year] * (0.10 + 0.01 * idx)
            for idx, year in enumerate(years)
            if year in revenue_by_year
        }
    if cost_of_revenue_by_year is None:
        cost_of_revenue_by_year = {
            year: revenue_by_year[year] - gross_profit_by_year[year]
            for year in years
            if year in revenue_by_year and year in gross_profit_by_year
        }
    if currency_by_year is None:
        currency_by_year = {}

    concepts = {
        "Revenues": [],
        "GrossProfit": [],
        "CostOfRevenue": [],
    }
    for year in years:
        currency = currency_by_year.get(year, "USD")
        end_date = f"{year}-09-30"
        revenue = revenue_by_year.get(year)
        if revenue is not None:
            concepts["Revenues"].append(
                fact(
                    concept="Revenues",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=revenue,
                    currency=currency,
                )
            )
        gross_profit = gross_profit_by_year.get(year)
        if gross_profit is not None:
            concepts["GrossProfit"].append(
                fact(
                    concept="GrossProfit",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=gross_profit,
                    currency=currency,
                )
            )
        cost_of_revenue = cost_of_revenue_by_year.get(year)
        if cost_of_revenue is not None:
            concepts["CostOfRevenue"].append(
                fact(
                    concept="CostOfRevenue",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=cost_of_revenue,
                    currency=currency,
                )
            )
    return concepts


def _base_opm_10y_concepts(
    *,
    latest_year=None,
    revenue_by_year=None,
    operating_income_by_year=None,
    currency_by_year=None,
):
    if latest_year is None:
        latest_year = date.today().year - 1
    years = list(range(latest_year - 9, latest_year + 1))

    if revenue_by_year is None:
        revenue_by_year = {year: 1000.0 + 10.0 * idx for idx, year in enumerate(years)}
    if operating_income_by_year is None:
        operating_income_by_year = {
            year: revenue_by_year[year] * (0.05 + 0.01 * idx)
            for idx, year in enumerate(years)
            if year in revenue_by_year
        }
    if currency_by_year is None:
        currency_by_year = {}

    concepts = {
        "Revenues": [],
        "OperatingIncomeLoss": [],
    }
    for year in years:
        currency = currency_by_year.get(year, "USD")
        end_date = f"{year}-09-30"
        revenue = revenue_by_year.get(year)
        if revenue is not None:
            concepts["Revenues"].append(
                fact(
                    concept="Revenues",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=revenue,
                    currency=currency,
                )
            )
        operating_income = operating_income_by_year.get(year)
        if operating_income is not None:
            concepts["OperatingIncomeLoss"].append(
                fact(
                    concept="OperatingIncomeLoss",
                    fiscal_period="FY",
                    end_date=end_date,
                    value=operating_income,
                    currency=currency,
                )
            )
    return concepts


def test_working_capital_metric_computes_difference():
    metric = WorkingCapitalMetric()
    recent = (date.today() - timedelta(days=10)).isoformat()

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

        def ticker_currency(self, symbol):
            return "USD"

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                price = 150.0
                as_of = recent
                currency = "USD"

            return Snapshot()

        def ticker_currency(self, symbol):
            return "USD"

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value > 0


def test_graham_multiplier_falls_back_to_fy_eps():
    metric = GrahamMultiplierMetric()
    recent = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo(_GBPTickerCurrencyRepo):
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

        def ticker_currency(self, symbol):
            return "USD"

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                price = 150.0
                as_of = recent
                currency = "USD"

            return Snapshot()

        def ticker_currency(self, symbol):
            return "USD"

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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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


def test_roic_10y_diagnostics_reports_missing_prior_ic_year():
    calculator = ROICFYSeriesCalculator()
    metric = ROIC10YMedianMetric()
    latest_year = date.today().year - 1
    ic_years = range(latest_year - 9, latest_year + 1)
    repo = _build_ic_repo(
        concept_records=_base_roic_10y_concepts(
            latest_year=latest_year,
            ic_short_by_year={year: 100.0 for year in ic_years},
            ic_long_by_year={year: 300.0 for year in ic_years},
            ic_equity_by_year={year: 900.0 for year in ic_years},
            ic_cash_by_year={year: 300.0 for year in ic_years},
        )
    )

    diagnostic = calculator.diagnose_series("AAPL.US", repo)

    assert diagnostic.snapshot is None
    assert diagnostic.failure_reason == "missing prior FY invested capital"
    assert diagnostic.latest_valid_roic_year == latest_year
    assert diagnostic.missing_window_years == (latest_year - 9,)
    oldest_year = next(
        item for item in diagnostic.year_diagnostics if item.year == latest_year - 9
    )
    assert oldest_year.roic_failure_reason == "missing prior FY invested capital"
    assert metric.compute("AAPL.US", repo) is None


def test_roic_10y_diagnostics_reports_missing_debt_input_on_latest_year():
    calculator = ROICFYSeriesCalculator()
    latest_year = date.today().year - 1
    concepts = _base_roic_10y_concepts(latest_year=latest_year)
    concepts["ShortTermDebt"] = [
        record
        for record in concepts["ShortTermDebt"]
        if record.end_date != f"{latest_year}-09-30"
    ]
    concepts["LongTermDebt"] = [
        record
        for record in concepts["LongTermDebt"]
        if record.end_date != f"{latest_year}-09-30"
    ]
    repo = _build_ic_repo(concept_records=concepts)

    diagnostic = calculator.diagnose_series("AAPL.US", repo)

    assert diagnostic.snapshot is None
    assert diagnostic.failure_reason == "missing invested capital debt input"
    latest = next(
        item for item in diagnostic.year_diagnostics if item.year == latest_year
    )
    assert (
        latest.invested_capital_failure_reason == "missing invested capital debt input"
    )
    assert latest.roic_failure_reason == "missing current FY invested capital"


def test_roic_10y_diagnostics_raises_for_currency_conflict_on_latest_year():
    calculator = ROICFYSeriesCalculator()
    latest_year = date.today().year - 1
    repo = _build_ic_repo(
        concept_records=_base_roic_10y_concepts(
            latest_year=latest_year,
            currency_by_year={latest_year - 1: "EUR"},
        )
    )

    with pytest.raises(MetricCurrencyInvariantError):
        calculator.diagnose_series("AAPL.US", repo)


def test_roic_10y_diagnostics_records_tax_fallback_without_failing():
    calculator = ROICFYSeriesCalculator()
    latest_year = date.today().year - 1
    roic_years = range(latest_year - 9, latest_year + 1)
    pretax = {year: 200.0 for year in roic_years}
    pretax[latest_year] = 0.0
    repo = _build_ic_repo(
        concept_records=_base_roic_10y_concepts(
            latest_year=latest_year,
            pretax_by_year=pretax,
        )
    )

    diagnostic = calculator.diagnose_series("AAPL.US", repo)

    assert diagnostic.snapshot is not None
    assert diagnostic.failure_reason is None
    latest = next(
        item for item in diagnostic.year_diagnostics if item.year == latest_year
    )
    assert latest.tax_rate_source == "latest_valid_fy"
    assert latest.roic_available is True


def test_roic_7y_metrics_pass_when_10y_fails_on_missing_eleventh_ic_year():
    latest_year = date.today().year - 1
    ic_years = range(latest_year - 9, latest_year + 1)
    repo = _build_ic_repo(
        concept_records=_base_roic_10y_concepts(
            latest_year=latest_year,
            ic_short_by_year={year: 100.0 for year in ic_years},
            ic_long_by_year={year: 300.0 for year in ic_years},
            ic_equity_by_year={year: 900.0 for year in ic_years},
            ic_cash_by_year={year: 300.0 for year in ic_years},
        )
    )

    assert ROIC10YMedianMetric().compute("AAPL.US", repo) is None

    median_result = ROIC7YMedianMetric().compute("AAPL.US", repo)
    min_result = ROIC7YMinMetric().compute("AAPL.US", repo)

    assert median_result is not None
    assert min_result is not None
    assert round(median_result.value, 6) == 0.18
    assert round(min_result.value, 6) == 0.12


def test_iroic_5y_metric_happy_path():
    metric = IncrementalROICFiveYearMetric()
    latest_year = date.today().year - 1
    concepts = _base_roic_10y_concepts(
        latest_year=latest_year,
        ic_short_by_year=_iroic_short_debt_ramp(latest_year, step=10.0),
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert round(result.value, 6) == 2.0


def test_iroic_5y_returns_none_when_strict_t_minus_5_missing():
    metric = IncrementalROICFiveYearMetric()
    latest_year = date.today().year - 1
    concepts = _base_roic_10y_concepts(
        latest_year=latest_year,
        ic_short_by_year=_iroic_short_debt_ramp(latest_year, step=10.0),
    )
    concepts["OperatingIncomeLoss"] = [
        record
        for record in concepts["OperatingIncomeLoss"]
        if record.end_date != f"{latest_year - 5}-09-30"
    ]
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_iroic_5y_tax_fallback_uses_latest_valid_fy_proxy():
    metric = IncrementalROICFiveYearMetric()
    latest_year = date.today().year - 1
    roic_years = range(latest_year - 9, latest_year + 1)
    tax = {year: 40.0 for year in roic_years}
    pretax = {year: 200.0 for year in roic_years}
    tax[latest_year - 1] = 60.0
    pretax[latest_year] = 0.0
    concepts = _base_roic_10y_concepts(
        latest_year=latest_year,
        tax_by_year=tax,
        pretax_by_year=pretax,
        ic_short_by_year=_iroic_short_debt_ramp(latest_year, step=10.0),
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert round(result.value, 6) == 1.4


def test_iroic_5y_tax_fallback_uses_default_when_no_valid_proxy():
    metric = IncrementalROICFiveYearMetric()
    latest_year = date.today().year - 1
    roic_years = range(latest_year - 9, latest_year + 1)
    tax = {year: 40.0 for year in roic_years}
    pretax = {year: 0.0 for year in roic_years}
    concepts = _base_roic_10y_concepts(
        latest_year=latest_year,
        tax_by_year=tax,
        pretax_by_year=pretax,
        ic_short_by_year=_iroic_short_debt_ramp(latest_year, step=10.0),
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert round(result.value, 6) == 1.975


def test_iroic_5y_returns_none_when_delta_ic_non_positive():
    metric = IncrementalROICFiveYearMetric()
    repo = _build_ic_repo(concept_records=_base_roic_10y_concepts())
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_iroic_5y_returns_none_when_relative_delta_ic_is_tiny():
    metric = IncrementalROICFiveYearMetric()
    latest_year = date.today().year - 1
    concepts = _base_roic_10y_concepts(
        latest_year=latest_year,
        ic_short_by_year=_iroic_short_debt_ramp(latest_year, step=1.0),
        ic_equity_by_year={
            year: 900_000.0 for year in range(latest_year - 10, latest_year + 1)
        },
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_iroic_5y_returns_none_on_currency_conflict():
    metric = IncrementalROICFiveYearMetric()
    latest_year = date.today().year - 1
    concepts = _base_roic_10y_concepts(
        latest_year=latest_year,
        ic_short_by_year=_iroic_short_debt_ramp(latest_year, step=10.0),
        currency_by_year={latest_year - 5: "EUR"},
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_iroic_5y_returns_none_when_latest_fy_stale():
    metric = IncrementalROICFiveYearMetric()
    stale_latest_year = date.today().year - 3
    concepts = _base_roic_10y_concepts(
        latest_year=stale_latest_year,
        ic_short_by_year=_iroic_short_debt_ramp(stale_latest_year, step=10.0),
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_iroic_5y_keeps_signed_negative_delta_nopat():
    metric = IncrementalROICFiveYearMetric()
    latest_year = date.today().year - 1
    concepts = _base_roic_10y_concepts(
        latest_year=latest_year,
        ic_short_by_year=_iroic_short_debt_ramp(latest_year, step=10.0),
    )
    concepts["OperatingIncomeLoss"] = [
        fact(
            concept=record.concept,
            fiscal_period=record.fiscal_period,
            end_date=record.end_date,
            value=80.0 if record.end_date == f"{latest_year}-09-30" else record.value,
            currency=record.currency,
        )
        for record in concepts["OperatingIncomeLoss"]
    ]
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert round(result.value, 6) == -1.52


def test_gm_10y_std_metric_happy_path():
    metric = GrossMarginTenYearStdMetric()
    repo = _build_ic_repo(concept_records=_base_gm_10y_concepts())
    result = metric.compute("AAPL.US", repo)
    assert result is not None

    margins = [0.10 + 0.01 * idx for idx in range(10)]
    mean = sum(margins) / len(margins)
    expected = (sum((value - mean) ** 2 for value in margins) / len(margins)) ** 0.5
    assert round(result.value, 12) == round(expected, 12)


def test_gm_10y_std_uses_revenue_minus_cost_fallback_when_gross_missing():
    metric = GrossMarginTenYearStdMetric()
    latest_year = date.today().year - 1
    concepts = _base_gm_10y_concepts(latest_year=latest_year)
    concepts["GrossProfit"] = [
        record
        for record in concepts["GrossProfit"]
        if record.end_date != f"{latest_year - 4}-09-30"
    ]
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is not None


def test_gm_10y_std_returns_none_when_strict_window_missing_year():
    metric = GrossMarginTenYearStdMetric()
    latest_year = date.today().year - 1
    concepts = _base_gm_10y_concepts(latest_year=latest_year)
    concepts["Revenues"] = [
        record
        for record in concepts["Revenues"]
        if record.end_date != f"{latest_year - 5}-09-30"
    ]
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_gm_10y_std_returns_none_when_revenue_non_positive():
    metric = GrossMarginTenYearStdMetric()
    latest_year = date.today().year - 1
    concepts = _base_gm_10y_concepts(
        latest_year=latest_year,
        revenue_by_year={
            year: (0.0 if year == latest_year else 1000.0 + idx * 10.0)
            for idx, year in enumerate(range(latest_year - 9, latest_year + 1))
        },
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_gm_10y_std_allows_mixed_series_currencies_when_yearly_margins_align():
    metric = GrossMarginTenYearStdMetric()
    latest_year = date.today().year - 1
    concepts = _base_gm_10y_concepts(
        latest_year=latest_year,
        currency_by_year={latest_year - 2: "EUR"},
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_gm_10y_std_returns_none_when_latest_fy_stale():
    metric = GrossMarginTenYearStdMetric()
    stale_latest_year = date.today().year - 3
    repo = _build_ic_repo(
        concept_records=_base_gm_10y_concepts(latest_year=stale_latest_year)
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_opm_10y_metrics_happy_path():
    std_metric = OperatingMarginTenYearStdMetric()
    min_metric = OperatingMarginTenYearMinMetric()
    repo = _build_ic_repo(concept_records=_base_opm_10y_concepts())

    std_result = std_metric.compute("AAPL.US", repo)
    min_result = min_metric.compute("AAPL.US", repo)

    assert std_result is not None
    assert min_result is not None

    margins = [0.05 + 0.01 * idx for idx in range(10)]
    mean = sum(margins) / len(margins)
    expected_std = (sum((value - mean) ** 2 for value in margins) / len(margins)) ** 0.5
    assert round(std_result.value, 12) == round(expected_std, 12)
    assert round(min_result.value, 12) == round(min(margins), 12)


def test_opm_10y_returns_none_when_strict_window_missing_year():
    metric = OperatingMarginTenYearStdMetric()
    latest_year = date.today().year - 1
    concepts = _base_opm_10y_concepts(latest_year=latest_year)
    concepts["OperatingIncomeLoss"] = [
        record
        for record in concepts["OperatingIncomeLoss"]
        if record.end_date != f"{latest_year - 5}-09-30"
    ]
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_opm_10y_returns_none_when_revenue_non_positive():
    metric = OperatingMarginTenYearStdMetric()
    latest_year = date.today().year - 1
    concepts = _base_opm_10y_concepts(
        latest_year=latest_year,
        revenue_by_year={
            year: (0.0 if year == latest_year else 1000.0 + idx * 10.0)
            for idx, year in enumerate(range(latest_year - 9, latest_year + 1))
        },
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_opm_10y_allows_mixed_series_currencies_when_yearly_margins_align():
    metric = OperatingMarginTenYearStdMetric()
    latest_year = date.today().year - 1
    concepts = _base_opm_10y_concepts(
        latest_year=latest_year,
        currency_by_year={latest_year - 2: "EUR"},
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_opm_10y_returns_none_when_latest_fy_stale():
    metric = OperatingMarginTenYearStdMetric()
    stale_latest_year = date.today().year - 3
    repo = _build_ic_repo(
        concept_records=_base_opm_10y_concepts(latest_year=stale_latest_year)
    )
    result = metric.compute("AAPL.US", repo)
    assert result is None


def test_opm_10y_min_keeps_signed_negative_margin():
    metric = OperatingMarginTenYearMinMetric()
    latest_year = date.today().year - 1
    concepts = _base_opm_10y_concepts(
        latest_year=latest_year,
        operating_income_by_year={
            year: (
                -20.0
                if year == latest_year - 4
                else (1000.0 + 10.0 * idx) * (0.05 + 0.01 * idx)
            )
            for idx, year in enumerate(range(latest_year - 9, latest_year + 1))
        },
    )
    repo = _build_ic_repo(concept_records=concepts)
    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert round(result.value, 12) == round(-20.0 / (1000.0 + 10.0 * 5), 12)


def test_opm_7y_min_happy_path():
    metric = OperatingMarginSevenYearMinMetric()
    latest_year = date.today().year - 1
    repo = _build_ic_repo(
        concept_records=_base_opm_10y_concepts(latest_year=latest_year)
    )

    result = metric.compute("AAPL.US", repo)

    assert result is not None
    margins = [0.08 + 0.01 * idx for idx in range(7)]
    assert round(result.value, 12) == round(min(margins), 12)


def test_opm_7y_min_returns_none_when_strict_window_missing_year():
    metric = OperatingMarginSevenYearMinMetric()
    latest_year = date.today().year - 1
    concepts = _base_opm_10y_concepts(latest_year=latest_year)
    concepts["OperatingIncomeLoss"] = [
        record
        for record in concepts["OperatingIncomeLoss"]
        if record.end_date != f"{latest_year - 3}-09-30"
    ]
    repo = _build_ic_repo(concept_records=concepts)

    result = metric.compute("AAPL.US", repo)

    assert result is None


def test_opm_7y_min_returns_none_when_revenue_non_positive():
    metric = OperatingMarginSevenYearMinMetric()
    latest_year = date.today().year - 1
    concepts = _base_opm_10y_concepts(
        latest_year=latest_year,
        revenue_by_year={
            year: (0.0 if year == latest_year - 2 else 1000.0 + 10.0 * idx)
            for idx, year in enumerate(range(latest_year - 9, latest_year + 1))
        },
    )
    repo = _build_ic_repo(concept_records=concepts)

    result = metric.compute("AAPL.US", repo)

    assert result is None


def test_opm_7y_min_keeps_signed_negative_margin():
    metric = OperatingMarginSevenYearMinMetric()
    latest_year = date.today().year - 1
    concepts = _base_opm_10y_concepts(
        latest_year=latest_year,
        operating_income_by_year={
            year: (
                -20.0
                if year == latest_year - 2
                else (1000.0 + 10.0 * idx) * (0.05 + 0.01 * idx)
            )
            for idx, year in enumerate(range(latest_year - 9, latest_year + 1))
        },
    )
    repo = _build_ic_repo(concept_records=concepts)

    result = metric.compute("AAPL.US", repo)

    assert result is not None
    expected_revenue = 1000.0 + 10.0 * 7
    assert round(result.value, 12) == round(-20.0 / expected_revenue, 12)


def test_debt_paydown_years_skips_non_positive_fcf():
    metric = DebtPaydownYearsMetric()
    today = date.today()
    q4 = (today - timedelta(days=30)).isoformat()
    q3 = (today - timedelta(days=120)).isoformat()
    q2 = (today - timedelta(days=210)).isoformat()
    q1 = (today - timedelta(days=300)).isoformat()

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_GBPTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

        def ticker_currency(self, symbol):
            return "USD"

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                price = 150.0
                as_of = recent
                currency = "USD"

            return Snapshot()

        def ticker_currency(self, symbol):
            return "USD"

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value > 0


def test_earnings_yield_metric():
    metric = EarningsYieldMetric()
    recent = (date.today() - timedelta(days=30)).isoformat()
    older = (date.today() - timedelta(days=120)).isoformat()

    class DummyRepo(_USDTickerCurrencyRepo):
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

        def ticker_currency(self, symbol):
            return "USD"

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                price = 50.0
                as_of = recent
                currency = "USD"

            return Snapshot()

        def ticker_currency(self, symbol):
            return "USD"

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value == (2.5 + 2.0 + 1.5 + 1.0) / 50.0


def test_earnings_yield_metric_falls_back_to_fy():
    metric = EarningsYieldMetric()
    recent_fy = (date.today() - timedelta(days=20)).isoformat()

    class DummyRepo(_USDTickerCurrencyRepo):
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

        def ticker_currency(self, symbol):
            return "USD"

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                price = 40.0
                as_of = recent_fy
                currency = "USD"

            return Snapshot()

        def ticker_currency(self, symbol):
            return "USD"

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value == 4.0 / 40.0


def test_price_to_fcf_metric():
    metric = PriceToFCFMetric()
    recent = (date.today() - timedelta(days=15)).isoformat()
    older = (date.today() - timedelta(days=90)).isoformat()

    class DummyRepo(_USDTickerCurrencyRepo):
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

        def ticker_currency(self, symbol):
            return "USD"

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 6400.0
                as_of = (date.today() - timedelta(days=10)).isoformat()
                currency = "USD"

            return Snapshot()

        def ticker_currency(self, symbol):
            return "USD"

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value == 10.0


def test_price_to_fcf_metric_uses_zero_capex_when_missing():
    metric = PriceToFCFMetric()
    recent = (date.today() - timedelta(days=15)).isoformat()
    older = (date.today() - timedelta(days=90)).isoformat()

    class DummyRepo(_USDTickerCurrencyRepo):
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

        def ticker_currency(self, symbol):
            return "USD"

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 6400.0
                as_of = (date.today() - timedelta(days=10)).isoformat()
                currency = "USD"

            return Snapshot()

        def ticker_currency(self, symbol):
            return "USD"

    repo = DummyRepo()
    market_repo = DummyMarketRepo()
    result = metric.compute("AAPL.US", repo, market_repo)
    assert result is not None
    assert result.value == 6400.0 / 460.0


def test_eps_ttm_metric():
    metric = EarningsPerShareTTM()
    recent = (date.today() - timedelta(days=30)).isoformat()
    older = (date.today() - timedelta(days=120)).isoformat()

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
        pass

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                market_cap = 123456789.0
                as_of = "2024-05-01"
                currency = "USD"

            return Snapshot()

        def ticker_currency(self, symbol):
            return "USD"

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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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

    class DummyRepo(_USDTickerCurrencyRepo):
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


def _build_cash_conversion_fy_records(
    symbol: str,
    latest_year: int,
    cfo_values: list[float],
    ni_values: list[float],
    *,
    ni_concept: str = "NetIncomeLoss",
    cfo_currency: str = "USD",
    ni_currency: str = "USD",
) -> dict[str, list[FactRecord]]:
    cfo_records: list[FactRecord] = []
    net_income_records: list[FactRecord] = []
    for offset, (cfo_value, ni_value) in enumerate(
        zip(cfo_values, ni_values, strict=True)
    ):
        year = latest_year - offset
        end_date = f"{year}-09-30"
        cfo_records.append(
            fact(
                symbol=symbol,
                concept="NetCashProvidedByUsedInOperatingActivities",
                fiscal_period="FY",
                end_date=end_date,
                value=cfo_value,
                currency=cfo_currency,
            )
        )
        net_income_records.append(
            fact(
                symbol=symbol,
                concept=ni_concept,
                fiscal_period="FY",
                end_date=end_date,
                value=ni_value,
                currency=ni_currency,
            )
        )
    return {
        "NetCashProvidedByUsedInOperatingActivities": cfo_records,
        ni_concept: net_income_records,
    }


def _build_fcf_fy_records(
    symbol: str,
    latest_year: int,
    ocf_values: list[float],
    *,
    capex_values: list[float] | None = None,
    ocf_currency: str = "USD",
    capex_currency: str = "USD",
) -> dict[str, list[FactRecord]]:
    records: dict[str, list[FactRecord]] = {
        "NetCashProvidedByUsedInOperatingActivities": [
            fact(
                symbol=symbol,
                concept="NetCashProvidedByUsedInOperatingActivities",
                fiscal_period="FY",
                end_date=f"{latest_year - offset}-09-30",
                value=value,
                currency=ocf_currency,
            )
            for offset, value in enumerate(ocf_values)
        ]
    }
    if capex_values is not None:
        records["CapitalExpenditures"] = [
            fact(
                symbol=symbol,
                concept="CapitalExpenditures",
                fiscal_period="FY",
                end_date=f"{latest_year - offset}-09-30",
                value=value,
                currency=capex_currency,
            )
            for offset, value in enumerate(capex_values)
        ]
    return records


def _build_net_income_fy_records(
    symbol: str,
    latest_year: int,
    values: list[float],
    *,
    concept: str = "NetIncomeLoss",
    currency: str = "USD",
) -> dict[str, list[FactRecord]]:
    return {
        concept: [
            fact(
                symbol=symbol,
                concept=concept,
                fiscal_period="FY",
                end_date=f"{latest_year - offset}-09-30",
                value=value,
                currency=currency,
            )
            for offset, value in enumerate(values)
        ]
    }


def _build_assets_quarter_records(
    *,
    symbol: str,
    q4: str,
    q3: str,
    q2: str,
    q1: str,
    q4_prev: str,
    values: tuple[float, float, float, float, float] = (
        1000.0,
        980.0,
        960.0,
        940.0,
        800.0,
    ),
    currency: str = "USD",
) -> list[FactRecord]:
    return [
        fact(
            symbol=symbol,
            concept="Assets",
            fiscal_period="Q4",
            end_date=q4,
            value=values[0],
            currency=currency,
        ),
        fact(
            symbol=symbol,
            concept="Assets",
            fiscal_period="Q3",
            end_date=q3,
            value=values[1],
            currency=currency,
        ),
        fact(
            symbol=symbol,
            concept="Assets",
            fiscal_period="Q2",
            end_date=q2,
            value=values[2],
            currency=currency,
        ),
        fact(
            symbol=symbol,
            concept="Assets",
            fiscal_period="Q1",
            end_date=q1,
            value=values[3],
            currency=currency,
        ),
        fact(
            symbol=symbol,
            concept="Assets",
            fiscal_period="Q4",
            end_date=q4_prev,
            value=values[4],
            currency=currency,
        ),
    ]


def _build_share_count_records(
    *,
    symbol: str,
    points: list[tuple[str, str, float]],
    concept: str = "CommonStockSharesOutstanding",
) -> dict[str, list[FactRecord]]:
    return {
        concept: [
            fact(
                symbol=symbol,
                concept=concept,
                fiscal_period=fiscal_period,
                end_date=end_date,
                value=value,
            )
            for fiscal_period, end_date, value in points
        ]
    }


def _build_market_repo(
    *,
    market_cap,
    as_of,
    currency="USD",
    price=100.0,
    ticker_currency="USD",
):
    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            class Snapshot:
                pass

            snapshot = Snapshot()
            snapshot.market_cap = market_cap
            snapshot.as_of = as_of
            snapshot.currency = currency
            snapshot.price = price
            return snapshot

        def ticker_currency(self, symbol):
            return ticker_currency

    return DummyMarketRepo()


def _build_ev_ratio_records(
    *,
    symbol: str,
    q4: str,
    q3: str,
    q2: str,
    q1: str,
    ebit_values: tuple[float, float, float, float] = (100.0, 100.0, 100.0, 100.0),
    ebit_currency: str = "USD",
    ocf_values: tuple[float, float, float, float] | None = (
        125.0,
        125.0,
        125.0,
        125.0,
    ),
    capex_values: tuple[float, float, float, float] | None = (
        25.0,
        25.0,
        25.0,
        25.0,
    ),
    da_primary_values: tuple[float, float, float, float] | None = (
        25.0,
        25.0,
        25.0,
        25.0,
    ),
    da_fallback_values: tuple[float, float, float, float] | None = None,
    enterprise_value: float | None = 1000.0,
    enterprise_currency: str = "USD",
    short_debt: float = 50.0,
    long_debt: float = 150.0,
    cash: float = 20.0,
    balance_currency: str = "USD",
) -> dict[str, list[FactRecord]]:
    records: dict[str, list[FactRecord]] = {
        "OperatingIncomeLoss": _quarterly_records(
            "OperatingIncomeLoss",
            (q4, q3, q2, q1),
            ebit_values,
            currency=ebit_currency,
        )
    }
    if ocf_values is not None:
        records["NetCashProvidedByUsedInOperatingActivities"] = _quarterly_records(
            "NetCashProvidedByUsedInOperatingActivities",
            (q4, q3, q2, q1),
            ocf_values,
            currency=ebit_currency,
        )
    if capex_values is not None:
        records["CapitalExpenditures"] = _quarterly_records(
            "CapitalExpenditures",
            (q4, q3, q2, q1),
            capex_values,
            currency=ebit_currency,
        )
    if da_primary_values is not None:
        records["DepreciationDepletionAndAmortization"] = _quarterly_records(
            "DepreciationDepletionAndAmortization",
            (q4, q3, q2, q1),
            da_primary_values,
            currency=ebit_currency,
        )
    if da_fallback_values is not None:
        records["DepreciationFromCashFlow"] = _quarterly_records(
            "DepreciationFromCashFlow",
            (q4, q3, q2, q1),
            da_fallback_values,
            currency=ebit_currency,
        )
    if enterprise_value is not None:
        records["EnterpriseValue"] = [
            fact(
                symbol=symbol,
                concept="EnterpriseValue",
                end_date=q4,
                fiscal_period="",
                value=enterprise_value,
                currency=enterprise_currency,
            )
        ]
    records["ShortTermDebt"] = [
        fact(
            symbol=symbol,
            concept="ShortTermDebt",
            end_date=q4,
            fiscal_period="Q4",
            value=short_debt,
            currency=balance_currency,
        )
    ]
    records["LongTermDebt"] = [
        fact(
            symbol=symbol,
            concept="LongTermDebt",
            end_date=q4,
            fiscal_period="Q4",
            value=long_debt,
            currency=balance_currency,
        )
    ]
    records["CashAndShortTermInvestments"] = [
        fact(
            symbol=symbol,
            concept="CashAndShortTermInvestments",
            end_date=q4,
            fiscal_period="Q4",
            value=cash,
            currency=balance_currency,
        )
    ]
    return records


class _OwnerEarningsRepo:
    def __init__(self, records_by_concept, *, ticker_currency="USD"):
        self.records_by_concept = records_by_concept
        self._ticker_currency = ticker_currency

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

    def ticker_currency(self, symbol):
        return self._ticker_currency


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


def _build_oe_ev_fy_input_records(
    *,
    symbol: str,
    latest_year: int,
    years: list[int],
    ebit_values: list[float],
    tax_values: list[float] | None = None,
    pretax_values: list[float] | None = None,
    da_values: list[float] | None = None,
    capex_values: list[float] | None = None,
    currency: str = "USD",
    nwc_values: list[float] | None = None,
) -> dict[str, list[FactRecord]]:
    if nwc_values is None:
        nwc_values = [
            300.0,
            250.0,
            230.0,
            210.0,
            190.0,
            170.0,
            150.0,
            130.0,
            110.0,
            90.0,
            70.0,
        ]
    records_by_concept = _build_nwc_fy_records(symbol, latest_year, nwc_values)
    records_by_concept["OperatingIncomeLoss"] = [
        fact(
            symbol=symbol,
            concept="OperatingIncomeLoss",
            fiscal_period="FY",
            end_date=f"{year}-09-30",
            value=value,
            currency=currency,
        )
        for year, value in zip(years, ebit_values, strict=True)
    ]
    if tax_values is not None:
        records_by_concept["IncomeTaxExpense"] = [
            fact(
                symbol=symbol,
                concept="IncomeTaxExpense",
                fiscal_period="FY",
                end_date=f"{year}-09-30",
                value=value,
                currency=currency,
            )
            for year, value in zip(years, tax_values, strict=True)
        ]
    if pretax_values is not None:
        records_by_concept["IncomeBeforeIncomeTaxes"] = [
            fact(
                symbol=symbol,
                concept="IncomeBeforeIncomeTaxes",
                fiscal_period="FY",
                end_date=f"{year}-09-30",
                value=value,
                currency=currency,
            )
            for year, value in zip(years, pretax_values, strict=True)
        ]
    if da_values is not None:
        records_by_concept["DepreciationDepletionAndAmortization"] = [
            fact(
                symbol=symbol,
                concept="DepreciationDepletionAndAmortization",
                fiscal_period="FY",
                end_date=f"{year}-09-30",
                value=value,
                currency=currency,
            )
            for year, value in zip(years, da_values, strict=True)
        ]
    if capex_values is not None:
        records_by_concept["CapitalExpenditures"] = [
            fact(
                symbol=symbol,
                concept="CapitalExpenditures",
                fiscal_period="FY",
                end_date=f"{year}-09-30",
                value=value,
                currency=currency,
            )
            for year, value in zip(years, capex_values, strict=True)
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


def test_oey_equity_metric_uses_listing_currency_for_market_cap():
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
                market_cap = 100.0
                as_of = q3
                currency = "EUR"

            return Snapshot()

        def ticker_currency(self, symbol):
            return "USD"

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is not None
    assert result.value == 7.44


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


def test_oey_ev_metric_returns_none_for_enterprise_value_currency_mismatch():
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

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            return None

        def ticker_currency(self, symbol):
            return "USD"

    result = metric.compute(
        symbol, _OwnerEarningsRepo(records_by_concept), DummyMarketRepo()
    )
    assert result is None


def test_oey_ev_metric_returns_none_when_enterprise_value_missing():
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

    records_by_concept.pop("EnterpriseValue")

    class DummyMarketRepo:
        def latest_snapshot(self, symbol):
            return None

        def ticker_currency(self, symbol):
            return "USD"

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


def test_ebit_yield_ev_metric_uses_normalized_enterprise_value():
    metric = EBITYieldEVMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(symbol=symbol, q4=q4, q3=q3, q2=q2, q1=q1)
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=999.0, as_of=q4)
    )

    assert result is not None
    assert result.as_of == q4
    assert result.value == 0.4


def test_ebit_yield_ev_metric_falls_back_to_derived_ev_when_primary_missing():
    metric = EBITYieldEVMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(
            symbol=symbol,
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
            enterprise_value=None,
        )
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=820.0, as_of=q4)
    )

    assert result is not None
    assert result.value == 0.4


def test_ebit_yield_ev_metric_allows_negative_values():
    metric = EBITYieldEVMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(
            symbol=symbol,
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
            ebit_values=(-50.0, -50.0, -50.0, -50.0),
        )
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=999.0, as_of=q4)
    )

    assert result is not None
    assert result.value == -0.2


def test_fcf_yield_ev_metric_uses_existing_fcf_policy():
    metric = FCFYieldEVMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(symbol=symbol, q4=q4, q3=q3, q2=q2, q1=q1)
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=999.0, as_of=q4)
    )

    assert result is not None
    assert result.value == 0.4


def test_fcf_yield_ev_metric_uses_zero_capex_when_missing():
    metric = FCFYieldEVMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(
            symbol=symbol,
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
            capex_values=None,
        )
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=999.0, as_of=q4)
    )

    assert result is not None
    assert result.value == 0.5


def test_fcf_yield_ev_metric_allows_negative_values():
    metric = FCFYieldEVMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(
            symbol=symbol,
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
            ocf_values=(20.0, 20.0, 20.0, 20.0),
            capex_values=(30.0, 30.0, 30.0, 30.0),
        )
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=999.0, as_of=q4)
    )

    assert result is not None
    assert result.value == -0.04


def test_ev_to_ebit_metric_computes_with_positive_ebit():
    metric = EVToEBITMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(symbol=symbol, q4=q4, q3=q3, q2=q2, q1=q1)
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=999.0, as_of=q4)
    )

    assert result is not None
    assert result.value == 2.5


def test_ev_to_ebit_metric_returns_none_when_ebit_non_positive():
    metric = EVToEBITMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(
            symbol=symbol,
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
            ebit_values=(0.0, 0.0, 0.0, 0.0),
        )
    )

    assert (
        metric.compute(symbol, repo, _build_market_repo(market_cap=999.0, as_of=q4))
        is None
    )


def test_ev_to_ebitda_metric_uses_component_ebitda_and_da_fallback():
    metric = EVToEBITDAMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(
            symbol=symbol,
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
            da_primary_values=None,
            da_fallback_values=(25.0, 25.0, 25.0, 25.0),
        )
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=999.0, as_of=q4)
    )

    assert result is not None
    assert result.value == 2.0


def test_ev_to_ebitda_metric_returns_none_when_ebitda_non_positive():
    metric = EVToEBITDAMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(
            symbol=symbol,
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
            ebit_values=(-20.0, -20.0, -20.0, -20.0),
            da_primary_values=(10.0, 10.0, 10.0, 10.0),
        )
    )

    assert (
        metric.compute(symbol, repo, _build_market_repo(market_cap=999.0, as_of=q4))
        is None
    )


def test_ebit_yield_ev_metric_returns_none_for_ev_currency_mismatch():
    metric = EBITYieldEVMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(
            symbol=symbol,
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
            enterprise_value=500.0,
            enterprise_currency="EUR",
        )
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=999.0, as_of=q4)
    )
    assert result is not None
    assert result.value == 400.0 / 1179.0


def test_ebit_yield_ev_metric_uses_market_cap_fallback_when_ev_missing():
    metric = EBITYieldEVMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(
            symbol=symbol,
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
            enterprise_value=500.0,
            enterprise_currency="EUR",
        )
    )

    repo = _OwnerEarningsRepo(
        _build_ev_ratio_records(
            symbol=symbol,
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
        )
    )

    result = metric.compute(
        symbol,
        repo,
        _build_market_repo(market_cap=999.0, as_of=q4),
    )
    assert result is not None
    assert result.value == 0.4


def test_cfo_to_ni_ttm_metric():
    metric = CFOToNITTMMetric()
    quarter_dates = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                quarter_dates,
                (120.0, 110.0, 100.0, 90.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                quarter_dates,
                (60.0, 55.0, 50.0, 45.0),
            ),
        }
    )

    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.as_of == quarter_dates[0]
    assert result.value == 2.0


def test_cfo_to_ni_ttm_metric_net_income_fallback():
    metric = CFOToNITTMMetric()
    quarter_dates = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                quarter_dates,
                (100.0, 100.0, 100.0, 100.0),
            ),
            "NetIncomeLossAvailableToCommonStockholdersBasic": _quarterly_records(
                "NetIncomeLossAvailableToCommonStockholdersBasic",
                quarter_dates,
                (50.0, 50.0, 50.0, 50.0),
            ),
        }
    )

    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.value == 2.0


def test_cfo_to_ni_ttm_metric_requires_four_quarters():
    metric = CFOToNITTMMetric()
    quarter_dates = _net_debt_quarter_dates()[:3]
    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                quarter_dates,
                (120.0, 110.0, 100.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                quarter_dates,
                (60.0, 55.0, 50.0),
            ),
        }
    )

    assert metric.compute("AAPL.US", repo) is None


def test_cfo_to_ni_ttm_metric_rejects_stale_latest_quarter():
    metric = CFOToNITTMMetric()
    today = date.today()
    quarter_dates = (
        (today - timedelta(days=420)).isoformat(),
        (today - timedelta(days=510)).isoformat(),
        (today - timedelta(days=600)).isoformat(),
        (today - timedelta(days=690)).isoformat(),
    )
    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                quarter_dates,
                (120.0, 110.0, 100.0, 90.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                quarter_dates,
                (60.0, 55.0, 50.0, 45.0),
            ),
        }
    )

    assert metric.compute("AAPL.US", repo) is None


def test_cfo_to_ni_ttm_metric_rejects_non_positive_net_income():
    metric = CFOToNITTMMetric()
    quarter_dates = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                quarter_dates,
                (120.0, 110.0, 100.0, 90.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                quarter_dates,
                (10.0, -10.0, 0.0, 0.0),
            ),
        }
    )

    assert metric.compute("AAPL.US", repo) is None


def test_cfo_to_ni_ttm_metric_rejects_currency_mismatch():
    metric = CFOToNITTMMetric()
    quarter_dates = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                quarter_dates,
                (120.0, 110.0, 100.0, 90.0),
                currency="USD",
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                quarter_dates,
                (60.0, 55.0, 50.0, 45.0),
                currency="EUR",
            ),
        }
    )

    assert metric.compute("AAPL.US", repo) is None


def test_cfo_to_ni_10y_median_metric():
    metric = CFOToNITenYearMedianMetric()
    latest_year = date.today().year - 1
    repo = _OwnerEarningsRepo(
        _build_cash_conversion_fy_records(
            "AAPL.US",
            latest_year,
            [100.0 * value for value in range(1, 11)],
            [100.0] * 10,
        )
    )

    result = metric.compute("AAPL.US", repo)
    assert result is not None
    assert result.as_of == f"{latest_year}-09-30"
    assert result.value == 5.5


def test_cfo_to_ni_10y_median_metric_requires_strict_consecutive_years():
    metric = CFOToNITenYearMedianMetric()
    latest_year = date.today().year - 1
    records_by_concept = _build_cash_conversion_fy_records(
        "AAPL.US",
        latest_year,
        [100.0 * value for value in range(1, 11)],
        [100.0] * 10,
    )
    records_by_concept["NetCashProvidedByUsedInOperatingActivities"] = [
        record
        for record in records_by_concept["NetCashProvidedByUsedInOperatingActivities"]
        if record.end_date != f"{latest_year - 4}-09-30"
    ]

    assert metric.compute("AAPL.US", _OwnerEarningsRepo(records_by_concept)) is None


def test_cfo_to_ni_10y_median_metric_rejects_non_positive_net_income_year():
    metric = CFOToNITenYearMedianMetric()
    latest_year = date.today().year - 1
    ni_values = [100.0] * 10
    ni_values[3] = 0.0
    repo = _OwnerEarningsRepo(
        _build_cash_conversion_fy_records(
            "AAPL.US",
            latest_year,
            [150.0] * 10,
            ni_values,
        )
    )

    assert metric.compute("AAPL.US", repo) is None


def test_cfo_to_ni_10y_median_metric_rejects_stale_latest_fy():
    metric = CFOToNITenYearMedianMetric()
    latest_year = date.today().year - 2
    repo = _OwnerEarningsRepo(
        _build_cash_conversion_fy_records(
            "AAPL.US",
            latest_year,
            [150.0] * 10,
            [100.0] * 10,
        )
    )

    assert metric.compute("AAPL.US", repo) is None


def test_cfo_to_ni_10y_median_metric_rejects_currency_conflict():
    metric = CFOToNITenYearMedianMetric()
    latest_year = date.today().year - 1
    records_by_concept = _build_cash_conversion_fy_records(
        "AAPL.US",
        latest_year,
        [150.0] * 10,
        [100.0] * 10,
    )
    records_by_concept["NetIncomeLoss"][5] = fact(
        symbol="AAPL.US",
        concept="NetIncomeLoss",
        fiscal_period="FY",
        end_date=f"{latest_year - 5}-09-30",
        value=100.0,
        currency="EUR",
    )

    assert metric.compute("AAPL.US", _OwnerEarningsRepo(records_by_concept)) is None


def test_accruals_ratio_metric():
    metric = AccrualsRatioMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (120.0, 110.0, 100.0, 90.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                (q4, q3, q2, q1),
                (150.0, 140.0, 130.0, 120.0),
            ),
            "Assets": _build_assets_quarter_records(
                symbol=symbol,
                q4=q4,
                q3=q3,
                q2=q2,
                q1=q1,
                q4_prev=q4_prev,
                values=(1000.0, 980.0, 960.0, 940.0, 800.0),
            ),
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert result.as_of == q4
    assert result.value == 120.0 / 900.0


def test_accruals_ratio_metric_net_income_fallback():
    metric = AccrualsRatioMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
            ),
            "NetIncomeLossAvailableToCommonStockholdersBasic": _quarterly_records(
                "NetIncomeLossAvailableToCommonStockholdersBasic",
                (q4, q3, q2, q1),
                (150.0, 150.0, 150.0, 150.0),
            ),
            "Assets": _build_assets_quarter_records(
                symbol=symbol,
                q4=q4,
                q3=q3,
                q2=q2,
                q1=q1,
                q4_prev=q4_prev,
            ),
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert result.value == 200.0 / 900.0


def test_accruals_ratio_metric_allows_negative_values():
    metric = AccrualsRatioMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (150.0, 150.0, 150.0, 150.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
            ),
            "Assets": _build_assets_quarter_records(
                symbol=symbol,
                q4=q4,
                q3=q3,
                q2=q2,
                q1=q1,
                q4_prev=q4_prev,
            ),
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert result.value == -200.0 / 900.0


def test_accruals_ratio_metric_requires_four_quarters():
    metric = AccrualsRatioMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2),
                (120.0, 110.0, 100.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                (q4, q3, q2),
                (150.0, 140.0, 130.0),
            ),
            "Assets": [
                fact(
                    symbol=symbol,
                    concept="Assets",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=1000.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="Assets",
                    fiscal_period="Q4",
                    end_date=q4_prev,
                    value=800.0,
                    currency="USD",
                ),
            ],
        }
    )

    assert metric.compute(symbol, repo) is None


def test_accruals_ratio_metric_rejects_stale_numerator_quarter():
    metric = AccrualsRatioMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=420)).isoformat()
    q3 = (today - timedelta(days=510)).isoformat()
    q2 = (today - timedelta(days=600)).isoformat()
    q1 = (today - timedelta(days=690)).isoformat()
    q4_prev = (today - timedelta(days=780)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (120.0, 110.0, 100.0, 90.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                (q4, q3, q2, q1),
                (150.0, 140.0, 130.0, 120.0),
            ),
            "Assets": _build_assets_quarter_records(
                symbol=symbol,
                q4=q4,
                q3=q3,
                q2=q2,
                q1=q1,
                q4_prev=q4_prev,
            ),
        }
    )

    assert metric.compute(symbol, repo) is None


def test_accruals_ratio_metric_rejects_stale_latest_assets_quarter():
    metric = AccrualsRatioMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()
    stale_assets_q4 = (today - timedelta(days=420)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (120.0, 110.0, 100.0, 90.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                (q4, q3, q2, q1),
                (150.0, 140.0, 130.0, 120.0),
            ),
            "Assets": _build_assets_quarter_records(
                symbol=symbol,
                q4=stale_assets_q4,
                q3=q3,
                q2=q2,
                q1=q1,
                q4_prev=q4_prev,
            ),
        }
    )

    assert metric.compute(symbol, repo) is None


def test_accruals_ratio_metric_requires_same_quarter_last_year_assets():
    metric = AccrualsRatioMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (120.0, 110.0, 100.0, 90.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                (q4, q3, q2, q1),
                (150.0, 140.0, 130.0, 120.0),
            ),
            "Assets": [
                fact(
                    symbol=symbol,
                    concept="Assets",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=1000.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="Assets",
                    fiscal_period="Q3",
                    end_date=q3,
                    value=980.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="Assets",
                    fiscal_period="Q2",
                    end_date=q2,
                    value=960.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="Assets",
                    fiscal_period="Q1",
                    end_date=q1,
                    value=940.0,
                    currency="USD",
                ),
            ],
        }
    )

    assert metric.compute(symbol, repo) is None


def test_accruals_ratio_metric_rejects_non_positive_avg_assets():
    metric = AccrualsRatioMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (120.0, 110.0, 100.0, 90.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                (q4, q3, q2, q1),
                (150.0, 140.0, 130.0, 120.0),
            ),
            "Assets": _build_assets_quarter_records(
                symbol=symbol,
                q4=q4,
                q3=q3,
                q2=q2,
                q1=q1,
                q4_prev=q4_prev,
                values=(-100.0, 980.0, 960.0, 940.0, 100.0),
            ),
        }
    )

    assert metric.compute(symbol, repo) is None


def test_accruals_ratio_metric_rejects_numerator_currency_mismatch():
    metric = AccrualsRatioMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (120.0, 110.0, 100.0, 90.0),
                currency="USD",
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                (q4, q3, q2, q1),
                (150.0, 140.0, 130.0, 120.0),
                currency="EUR",
            ),
            "Assets": _build_assets_quarter_records(
                symbol=symbol,
                q4=q4,
                q3=q3,
                q2=q2,
                q1=q1,
                q4_prev=q4_prev,
            ),
        }
    )

    assert metric.compute(symbol, repo) is None


def test_accruals_ratio_metric_rejects_assets_currency_mismatch():
    metric = AccrualsRatioMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()

    assets_records = _build_assets_quarter_records(
        symbol=symbol,
        q4=q4,
        q3=q3,
        q2=q2,
        q1=q1,
        q4_prev=q4_prev,
    )
    assets_records[-1] = fact(
        symbol=symbol,
        concept="Assets",
        fiscal_period="Q4",
        end_date=q4_prev,
        value=800.0,
        currency="EUR",
    )

    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (120.0, 110.0, 100.0, 90.0),
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                (q4, q3, q2, q1),
                (150.0, 140.0, 130.0, 120.0),
            ),
            "Assets": assets_records,
        }
    )

    assert metric.compute(symbol, repo) is None


def test_accruals_ratio_metric_rejects_numerator_denominator_currency_mismatch():
    metric = AccrualsRatioMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (120.0, 110.0, 100.0, 90.0),
                currency="USD",
            ),
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss",
                (q4, q3, q2, q1),
                (150.0, 140.0, 130.0, 120.0),
                currency="USD",
            ),
            "Assets": _build_assets_quarter_records(
                symbol=symbol,
                q4=q4,
                q3=q3,
                q2=q2,
                q1=q1,
                q4_prev=q4_prev,
                currency="EUR",
            ),
        }
    )

    assert metric.compute(symbol, repo) is None


def test_share_count_change_metrics_prefer_quarterly_path():
    symbol = "AAPL.US"
    today = date.today()
    q_latest = (today - timedelta(days=20)).isoformat()
    latest_year = int(q_latest[:4])
    q_prior = f"{latest_year - 10}-03-31"
    fy_latest = f"{latest_year}-09-30"
    fy_prior = f"{latest_year - 10}-09-30"

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", q_latest, 80.0),
                ("FY", fy_latest, 140.0),
                ("FY", fy_prior, 100.0),
                ("Q4", q_prior, 100.0),
            ],
        )
    )

    cagr = ShareCountCAGR10YMetric().compute(symbol, repo)
    pct_change = Shares10YPctChangeMetric().compute(symbol, repo)

    assert cagr is not None
    assert pct_change is not None
    assert cagr.as_of == q_latest
    assert pct_change.as_of == q_latest
    assert round(cagr.value, 10) == round((80.0 / 100.0) ** 0.1 - 1.0, 10)
    assert round(pct_change.value, 10) == -0.2


def test_share_count_cagr_5y_metric_prefers_quarterly_path():
    symbol = "AAPL.US"
    today = date.today()
    q_latest = (today - timedelta(days=20)).isoformat()
    latest_year = int(q_latest[:4])
    q_prior = f"{latest_year - 5}-03-31"
    fy_latest = f"{latest_year}-09-30"
    fy_prior = f"{latest_year - 5}-09-30"

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", q_latest, 80.0),
                ("FY", fy_latest, 140.0),
                ("FY", fy_prior, 100.0),
                ("Q4", q_prior, 100.0),
            ],
        )
    )

    result = ShareCountCAGR5YMetric().compute(symbol, repo)

    assert result is not None
    assert result.as_of == q_latest
    assert round(result.value, 10) == round((80.0 / 100.0) ** 0.2 - 1.0, 10)


def test_share_count_change_metrics_fallback_to_fy_path():
    symbol = "AAPL.US"
    today = date.today()
    q_latest = (today - timedelta(days=20)).isoformat()
    latest_year = int(q_latest[:4])
    fy_latest = f"{latest_year}-09-30"
    fy_prior = f"{latest_year - 10}-09-30"

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", q_latest, 90.0),
                ("FY", fy_latest, 150.0),
                ("FY", fy_prior, 100.0),
            ],
        )
    )

    cagr = ShareCountCAGR10YMetric().compute(symbol, repo)
    pct_change = Shares10YPctChangeMetric().compute(symbol, repo)

    assert cagr is not None
    assert pct_change is not None
    assert cagr.as_of == fy_latest
    assert pct_change.as_of == fy_latest
    assert round(cagr.value, 10) == round((150.0 / 100.0) ** 0.1 - 1.0, 10)
    assert round(pct_change.value, 10) == 0.5


def test_share_count_cagr_5y_metric_fallbacks_to_fy_path():
    symbol = "AAPL.US"
    today = date.today()
    q_latest = (today - timedelta(days=20)).isoformat()
    latest_year = int(q_latest[:4])
    fy_latest = f"{latest_year}-09-30"
    fy_prior = f"{latest_year - 5}-09-30"

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", q_latest, 90.0),
                ("FY", fy_latest, 150.0),
                ("FY", fy_prior, 100.0),
            ],
        )
    )

    result = ShareCountCAGR5YMetric().compute(symbol, repo)

    assert result is not None
    assert result.as_of == fy_latest
    assert round(result.value, 10) == round((150.0 / 100.0) ** 0.2 - 1.0, 10)


def test_share_count_change_metrics_require_exact_10_year_match():
    symbol = "AAPL.US"
    today = date.today()
    q_latest = (today - timedelta(days=20)).isoformat()
    latest_year = int(q_latest[:4])

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", q_latest, 80.0),
                ("FY", f"{latest_year}-09-30", 150.0),
                ("FY", f"{latest_year - 9}-09-30", 100.0),
            ],
        )
    )

    assert ShareCountCAGR10YMetric().compute(symbol, repo) is None
    assert Shares10YPctChangeMetric().compute(symbol, repo) is None


def test_share_count_cagr_5y_metric_requires_exact_5_year_match():
    symbol = "AAPL.US"
    today = date.today()
    q_latest = (today - timedelta(days=20)).isoformat()
    latest_year = int(q_latest[:4])

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", q_latest, 80.0),
                ("FY", f"{latest_year}-09-30", 150.0),
                ("FY", f"{latest_year - 4}-09-30", 100.0),
            ],
        )
    )

    assert ShareCountCAGR5YMetric().compute(symbol, repo) is None


def test_share_count_change_metrics_require_positive_share_counts():
    symbol = "AAPL.US"
    today = date.today()
    q_latest = (today - timedelta(days=20)).isoformat()
    latest_year = int(q_latest[:4])

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", q_latest, 80.0),
                ("FY", f"{latest_year}-09-30", 150.0),
                ("FY", f"{latest_year - 10}-09-30", 0.0),
            ],
        )
    )

    assert ShareCountCAGR10YMetric().compute(symbol, repo) is None
    assert Shares10YPctChangeMetric().compute(symbol, repo) is None


def test_share_count_cagr_5y_metric_requires_positive_share_counts():
    symbol = "AAPL.US"
    today = date.today()
    q_latest = (today - timedelta(days=20)).isoformat()
    latest_year = int(q_latest[:4])

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", q_latest, 80.0),
                ("FY", f"{latest_year}-09-30", 150.0),
                ("FY", f"{latest_year - 5}-09-30", 0.0),
            ],
        )
    )

    assert ShareCountCAGR5YMetric().compute(symbol, repo) is None


def test_share_count_change_metrics_reject_stale_latest_quarter_without_fallback():
    symbol = "AAPL.US"
    stale_latest = (date.today() - timedelta(days=MAX_FACT_AGE_DAYS + 10)).isoformat()
    latest_year = int(stale_latest[:4])

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", stale_latest, 100.0),
                ("Q4", f"{latest_year - 10}-03-31", 90.0),
            ],
        )
    )

    assert ShareCountCAGR10YMetric().compute(symbol, repo) is None
    assert Shares10YPctChangeMetric().compute(symbol, repo) is None


def test_share_count_cagr_5y_metric_rejects_stale_latest_quarter_without_fallback():
    symbol = "AAPL.US"
    stale_latest = (date.today() - timedelta(days=MAX_FACT_AGE_DAYS + 10)).isoformat()
    latest_year = int(stale_latest[:4])

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", stale_latest, 100.0),
                ("Q4", f"{latest_year - 5}-03-31", 90.0),
            ],
        )
    )

    assert ShareCountCAGR5YMetric().compute(symbol, repo) is None


def test_share_count_change_metrics_reject_stale_latest_fy_fallback():
    symbol = "AAPL.US"
    latest_fy = (date.today() - timedelta(days=MAX_FY_FACT_AGE_DAYS + 10)).isoformat()
    latest_year = int(latest_fy[:4])

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", (date.today() - timedelta(days=20)).isoformat(), 100.0),
                ("FY", latest_fy, 150.0),
                ("FY", f"{latest_year - 10}-09-30", 90.0),
            ],
        )
    )

    assert ShareCountCAGR10YMetric().compute(symbol, repo) is None
    assert Shares10YPctChangeMetric().compute(symbol, repo) is None


def test_share_count_cagr_5y_metric_rejects_stale_latest_fy_fallback():
    symbol = "AAPL.US"
    latest_fy = (date.today() - timedelta(days=MAX_FY_FACT_AGE_DAYS + 10)).isoformat()
    latest_year = int(latest_fy[:4])

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", (date.today() - timedelta(days=20)).isoformat(), 100.0),
                ("FY", latest_fy, 150.0),
                ("FY", f"{latest_year - 5}-09-30", 90.0),
            ],
        )
    )

    assert ShareCountCAGR5YMetric().compute(symbol, repo) is None


def test_share_count_change_metrics_ignore_weighted_average_share_concepts():
    symbol = "AAPL.US"
    today = date.today()
    latest_fy = f"{today.year}-09-30"
    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            concept="WeightedAverageNumberOfSharesOutstandingBasic",
            points=[
                ("FY", latest_fy, 150.0),
                ("FY", f"{today.year - 10}-09-30", 100.0),
            ],
        )
    )

    assert ShareCountCAGR10YMetric().compute(symbol, repo) is None
    assert Shares10YPctChangeMetric().compute(symbol, repo) is None
    assert ShareCountCAGR5YMetric().compute(symbol, repo) is None


def test_share_count_cagr_5y_metric_declares_percent_metadata():
    assert metadata_for_metric("share_count_cagr_5y").unit_kind == "percent"


def test_net_buyback_yield_metric_computes_from_sale_purchase_ttm():
    metric = NetBuybackYieldMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "SalePurchaseOfStock": _quarterly_records(
                "SalePurchaseOfStock",
                (q4, q3, q2, q1),
                (-40.0, -30.0, -20.0, -10.0),
                currency="USD",
            )
        }
    )

    result = metric.compute(
        symbol,
        repo,
        _build_market_repo(market_cap=1000.0, as_of=q3),
    )

    assert result is not None
    assert result.as_of == q4
    assert result.value == 0.1


def test_net_buyback_yield_metric_allows_negative_values_for_net_issuance():
    metric = NetBuybackYieldMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "SalePurchaseOfStock": _quarterly_records(
                "SalePurchaseOfStock",
                (q4, q3, q2, q1),
                (40.0, 30.0, 20.0, 10.0),
                currency="USD",
            )
        }
    )

    result = metric.compute(
        symbol,
        repo,
        _build_market_repo(market_cap=1000.0, as_of=q3),
    )

    assert result is not None
    assert result.value == -0.1


def test_net_buyback_yield_metric_falls_back_to_issuance_cash_flow():
    metric = NetBuybackYieldMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "IssuanceOfCapitalStock": _quarterly_records(
                "IssuanceOfCapitalStock",
                (q4, q3, q2, q1),
                (10.0, 10.0, 10.0, 10.0),
                currency="USD",
            )
        }
    )

    result = metric.compute(
        symbol,
        repo,
        _build_market_repo(market_cap=200.0, as_of=q3),
    )

    assert result is not None
    assert result.value == -0.2


def test_net_buyback_yield_metric_falls_back_to_share_count_when_market_cap_missing():
    metric = NetBuybackYieldMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()

    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", q4, 90.0),
                ("Q4", q4_prev, 100.0),
            ],
        )
    )

    result = metric.compute(
        symbol,
        repo,
        _build_market_repo(market_cap=None, as_of=q4),
    )

    assert result is not None
    assert result.as_of == q4
    assert round(result.value, 10) == 0.1


def test_net_buyback_yield_metric_returns_none_when_market_cap_missing_and_no_share_fallback():
    metric = NetBuybackYieldMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "SalePurchaseOfStock": _quarterly_records(
                "SalePurchaseOfStock",
                (q4, q3, q2, q1),
                (-20.0, -20.0, -20.0, -20.0),
                currency="USD",
            )
        }
    )

    assert (
        metric.compute(symbol, repo, _build_market_repo(market_cap=None, as_of=q4))
        is None
    )


def test_net_buyback_yield_metric_uses_listing_currency_for_market_cap():
    metric = NetBuybackYieldMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "SalePurchaseOfStock": _quarterly_records(
                "SalePurchaseOfStock",
                (q4, q3, q2, q1),
                (-40.0, -30.0, -20.0, -10.0),
                currency="USD",
            )
        }
    )

    result = metric.compute(
        symbol,
        repo,
        _build_market_repo(
            market_cap=500.0,
            as_of=q3,
            currency="EUR",
            ticker_currency="USD",
        ),
    )
    assert result is not None
    assert result.value == 0.2


def test_net_buyback_yield_metric_uses_share_count_fallback_when_market_cap_missing():
    metric = NetBuybackYieldMetric()
    symbol = "AAPL.US"
    today = date.today()
    q4 = (today - timedelta(days=20)).isoformat()
    q3 = (today - timedelta(days=110)).isoformat()
    q2 = (today - timedelta(days=200)).isoformat()
    q1 = (today - timedelta(days=290)).isoformat()
    q4_prev = (today - timedelta(days=380)).isoformat()

    records = {
        "SalePurchaseOfStock": _quarterly_records(
            "SalePurchaseOfStock",
            (q4, q3, q2, q1),
            (-40.0, -30.0, -20.0, -10.0),
            currency="USD",
        )
    }
    records.update(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("Q4", q4, 90.0),
                ("Q4", q4_prev, 100.0),
            ],
        )
    )
    repo = _OwnerEarningsRepo(records)

    result = metric.compute(
        symbol,
        repo,
        _build_market_repo(market_cap=None, as_of=q3, currency="USD"),
    )

    assert result is not None
    assert round(result.value, 10) == 0.1
    assert result.as_of == q4


def test_net_buyback_yield_metric_falls_back_to_fy_share_change():
    metric = NetBuybackYieldMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("FY", f"{latest_year}-09-30", 120.0),
                ("FY", f"{latest_year - 1}-09-30", 100.0),
            ],
        )
    )

    result = metric.compute(
        symbol,
        repo,
        _build_market_repo(market_cap=None, as_of=f"{latest_year}-09-30"),
    )

    assert result is not None
    assert round(result.value, 10) == -0.2


def test_net_buyback_yield_metric_ignores_weighted_average_shares_in_fallback():
    metric = NetBuybackYieldMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            concept="WeightedAverageNumberOfSharesOutstandingBasic",
            points=[
                ("FY", f"{latest_year}-09-30", 120.0),
                ("FY", f"{latest_year - 1}-09-30", 100.0),
            ],
        )
    )

    assert (
        metric.compute(
            symbol,
            repo,
            _build_market_repo(market_cap=None, as_of=f"{latest_year}-09-30"),
        )
        is None
    )


def test_net_buyback_yield_metric_requires_strict_prior_year_share_pair():
    metric = NetBuybackYieldMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("FY", f"{latest_year}-09-30", 120.0),
                ("FY", f"{latest_year - 2}-09-30", 100.0),
            ],
        )
    )

    assert (
        metric.compute(
            symbol,
            repo,
            _build_market_repo(market_cap=None, as_of=f"{latest_year}-09-30"),
        )
        is None
    )


def test_net_buyback_yield_metric_requires_positive_share_counts_in_fallback():
    metric = NetBuybackYieldMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    repo = _OwnerEarningsRepo(
        _build_share_count_records(
            symbol=symbol,
            points=[
                ("FY", f"{latest_year}-09-30", 120.0),
                ("FY", f"{latest_year - 1}-09-30", 0.0),
            ],
        )
    )

    assert (
        metric.compute(
            symbol,
            repo,
            _build_market_repo(market_cap=None, as_of=f"{latest_year}-09-30"),
        )
        is None
    )


def test_sbc_to_revenue_metric_computes_ttm_ratio():
    metric = SBCToRevenueMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()

    repo = _OwnerEarningsRepo(
        {
            "StockBasedCompensation": _quarterly_records(
                "StockBasedCompensation",
                (q4, q3, q2, q1),
                (10.0, 9.0, 8.0, 7.0),
                currency="USD",
            ),
            "Revenues": _quarterly_records(
                "Revenues",
                (q4, q3, q2, q1),
                (100.0, 110.0, 120.0, 130.0),
                currency="USD",
            ),
        }
    )

    result = metric.compute(symbol, repo)

    assert result is not None
    assert result.as_of == q4
    assert round(result.value, 10) == round(34.0 / 460.0, 10)


def test_sbc_to_fcf_metric_computes_ttm_ratio():
    metric = SBCToFCFMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()

    repo = _OwnerEarningsRepo(
        {
            "StockBasedCompensation": _quarterly_records(
                "StockBasedCompensation",
                (q4, q3, q2, q1),
                (10.0, 10.0, 10.0, 10.0),
                currency="USD",
            ),
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
                currency="USD",
            ),
            "CapitalExpenditures": _quarterly_records(
                "CapitalExpenditures",
                (q4, q3, q2, q1),
                (20.0, 20.0, 20.0, 20.0),
                currency="USD",
            ),
        }
    )

    result = metric.compute(symbol, repo)

    assert result is not None
    assert result.as_of == q4
    assert result.value == 0.125


def test_sbc_to_fcf_metric_uses_zero_capex_when_capex_missing():
    metric = SBCToFCFMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()

    repo = _OwnerEarningsRepo(
        {
            "StockBasedCompensation": _quarterly_records(
                "StockBasedCompensation",
                (q4, q3, q2, q1),
                (10.0, 10.0, 10.0, 10.0),
                currency="USD",
            ),
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
                currency="USD",
            ),
        }
    )

    result = metric.compute(symbol, repo)

    assert result is not None
    assert result.value == 0.1


def test_sbc_load_metrics_return_none_when_sbc_missing():
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        {
            "Revenues": _quarterly_records(
                "Revenues",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
            ),
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
            ),
        }
    )

    assert SBCToRevenueMetric().compute(symbol, repo) is None
    assert SBCToFCFMetric().compute(symbol, repo) is None


def test_sbc_load_metrics_require_four_quarters_of_sbc():
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        {
            "StockBasedCompensation": _quarterly_records(
                "StockBasedCompensation",
                (q4, q3, q2),
                (10.0, 9.0, 8.0),
            ),
            "Revenues": _quarterly_records(
                "Revenues",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
            ),
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
            ),
        }
    )

    assert SBCToRevenueMetric().compute(symbol, repo) is None
    assert SBCToFCFMetric().compute(symbol, repo) is None


def test_sbc_load_metrics_reject_stale_latest_quarter():
    symbol = "AAPL.US"
    q4 = (date.today() - timedelta(days=MAX_FACT_AGE_DAYS + 10)).isoformat()
    q3 = (date.today() - timedelta(days=MAX_FACT_AGE_DAYS + 100)).isoformat()
    q2 = (date.today() - timedelta(days=MAX_FACT_AGE_DAYS + 190)).isoformat()
    q1 = (date.today() - timedelta(days=MAX_FACT_AGE_DAYS + 280)).isoformat()

    repo = _OwnerEarningsRepo(
        {
            "StockBasedCompensation": _quarterly_records(
                "StockBasedCompensation",
                (q4, q3, q2, q1),
                (10.0, 9.0, 8.0, 7.0),
            ),
            "Revenues": _quarterly_records(
                "Revenues",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
            ),
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
            ),
        }
    )

    assert SBCToRevenueMetric().compute(symbol, repo) is None
    assert SBCToFCFMetric().compute(symbol, repo) is None


def test_sbc_to_revenue_metric_requires_positive_revenue():
    metric = SBCToRevenueMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()

    repo = _OwnerEarningsRepo(
        {
            "StockBasedCompensation": _quarterly_records(
                "StockBasedCompensation",
                (q4, q3, q2, q1),
                (10.0, 10.0, 10.0, 10.0),
            ),
            "Revenues": _quarterly_records(
                "Revenues",
                (q4, q3, q2, q1),
                (0.0, 0.0, 0.0, 0.0),
            ),
        }
    )

    assert metric.compute(symbol, repo) is None


def test_sbc_to_fcf_metric_requires_positive_fcf():
    metric = SBCToFCFMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()

    repo = _OwnerEarningsRepo(
        {
            "StockBasedCompensation": _quarterly_records(
                "StockBasedCompensation",
                (q4, q3, q2, q1),
                (10.0, 10.0, 10.0, 10.0),
            ),
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (20.0, 20.0, 20.0, 20.0),
            ),
            "CapitalExpenditures": _quarterly_records(
                "CapitalExpenditures",
                (q4, q3, q2, q1),
                (20.0, 20.0, 20.0, 20.0),
            ),
        }
    )

    assert metric.compute(symbol, repo) is None


def test_sbc_load_metrics_reject_currency_mismatch():
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    sbc_repo = _OwnerEarningsRepo(
        {
            "StockBasedCompensation": _quarterly_records(
                "StockBasedCompensation",
                (q4, q3, q2, q1),
                (10.0, 9.0, 8.0, 7.0),
                currency="USD",
            ),
            "Revenues": _quarterly_records(
                "Revenues",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
                currency="EUR",
            ),
        }
    )
    fcf_repo = _OwnerEarningsRepo(
        {
            "StockBasedCompensation": _quarterly_records(
                "StockBasedCompensation",
                (q4, q3, q2, q1),
                (10.0, 9.0, 8.0, 7.0),
                currency="USD",
            ),
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
                currency="EUR",
            ),
        }
    )

    assert SBCToRevenueMetric().compute(symbol, sbc_repo) is None
    assert SBCToFCFMetric().compute(symbol, fcf_repo) is None


def test_sbc_load_metrics_reject_currency_conflict_within_sbc():
    symbol = "AAPL.US"
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        {
            "StockBasedCompensation": _quarterly_records(
                "StockBasedCompensation",
                (q4, q3, q2, q1),
                (10.0, 9.0, 8.0, 7.0),
                currency="USD",
            ),
            "Revenues": _quarterly_records(
                "Revenues",
                (q4, q3, q2, q1),
                (100.0, 100.0, 100.0, 100.0),
                currency="USD",
            ),
        }
    )
    repo.records_by_concept["StockBasedCompensation"][2] = fact(
        concept="StockBasedCompensation",
        fiscal_period="Q2",
        end_date=q2,
        value=8.0,
        currency="EUR",
    )

    assert SBCToRevenueMetric().compute(symbol, repo) is None


def test_oe_ev_fy_median_5y_metric_computes_expected_median():
    metric = OwnerEarningsEnterpriseFiveYearMedianMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]

    records_by_concept = _build_oe_ev_fy_input_records(
        symbol=symbol,
        latest_year=latest_year,
        years=years,
        ebit_values=[500.0, 450.0, 400.0, 350.0, 300.0],
        tax_values=[100.0, 90.0, 80.0, 70.0, 60.0],
        pretax_values=[500.0, 450.0, 400.0, 350.0, 300.0],
        da_values=[100.0, 100.0, 100.0, 100.0, 100.0],
        capex_values=[90.0, 90.0, 90.0, 90.0, 90.0],
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.as_of == f"{years[0]}-09-30"
    assert result.value == 300.0


def test_oe_ev_fy_median_5y_metric_requires_five_points():
    metric = OwnerEarningsEnterpriseFiveYearMedianMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(4)]

    records_by_concept = _build_oe_ev_fy_input_records(
        symbol=symbol,
        latest_year=latest_year,
        years=years,
        ebit_values=[400.0, 380.0, 360.0, 340.0],
        capex_values=[90.0, 90.0, 90.0, 90.0],
    )

    assert metric.compute(symbol, _OwnerEarningsRepo(records_by_concept)) is None


def test_oe_ev_fy_median_5y_metric_allows_year_gaps():
    metric = OwnerEarningsEnterpriseFiveYearMedianMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [
        latest_year,
        latest_year - 2,
        latest_year - 3,
        latest_year - 5,
        latest_year - 6,
    ]

    records_by_concept = _build_oe_ev_fy_input_records(
        symbol=symbol,
        latest_year=latest_year,
        years=years,
        ebit_values=[500.0, 420.0, 380.0, 320.0, 280.0],
        tax_values=[100.0, 84.0, 76.0, 64.0, 56.0],
        pretax_values=[500.0, 420.0, 380.0, 320.0, 280.0],
        da_values=[100.0, 100.0, 100.0, 100.0, 100.0],
        capex_values=[90.0, 90.0, 90.0, 90.0, 90.0],
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 284.0


def test_oe_ev_fy_median_5y_metric_returns_none_when_delta_nwc_maint_missing():
    metric = OwnerEarningsEnterpriseFiveYearMedianMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]

    records_by_concept = _build_oe_ev_fy_input_records(
        symbol=symbol,
        latest_year=latest_year,
        years=years,
        ebit_values=[500.0, 450.0, 400.0, 350.0, 300.0],
        tax_values=[100.0, 90.0, 80.0, 70.0, 60.0],
        pretax_values=[500.0, 450.0, 400.0, 350.0, 300.0],
        da_values=[100.0, 100.0, 100.0, 100.0, 100.0],
        capex_values=[90.0, 90.0, 90.0, 90.0, 90.0],
    )
    records_by_concept.pop("AssetsCurrent")

    assert metric.compute(symbol, _OwnerEarningsRepo(records_by_concept)) is None


def test_worst_oe_ev_fy_10y_metric_preserves_negative_worst_year():
    metric = WorstOwnerEarningsEnterpriseTenYearMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(10)]

    records_by_concept = _build_oe_ev_fy_input_records(
        symbol=symbol,
        latest_year=latest_year,
        years=years,
        ebit_values=[
            500.0,
            450.0,
            400.0,
            350.0,
            300.0,
            250.0,
            200.0,
            150.0,
            100.0,
            0.0,
        ],
        tax_values=[100.0, 90.0, 80.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0, 0.0],
        pretax_values=[
            500.0,
            450.0,
            400.0,
            350.0,
            300.0,
            250.0,
            200.0,
            150.0,
            100.0,
            10.0,
        ],
        da_values=[100.0] * 10,
        capex_values=[90.0] * 10,
        nwc_values=[
            300.0,
            250.0,
            230.0,
            210.0,
            190.0,
            170.0,
            150.0,
            130.0,
            110.0,
            90.0,
            70.0,
        ],
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == -20.0


def test_worst_oe_ev_fy_10y_metric_requires_strict_consecutive_years():
    metric = WorstOwnerEarningsEnterpriseTenYearMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [
        latest_year,
        latest_year - 1,
        latest_year - 2,
        latest_year - 3,
        latest_year - 4,
        latest_year - 5,
        latest_year - 6,
        latest_year - 8,
        latest_year - 9,
        latest_year - 10,
    ]

    records_by_concept = _build_oe_ev_fy_input_records(
        symbol=symbol,
        latest_year=latest_year,
        years=years,
        ebit_values=[
            500.0,
            450.0,
            400.0,
            350.0,
            300.0,
            250.0,
            200.0,
            150.0,
            100.0,
            50.0,
        ],
        tax_values=[100.0, 90.0, 80.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0, 10.0],
        pretax_values=[
            500.0,
            450.0,
            400.0,
            350.0,
            300.0,
            250.0,
            200.0,
            150.0,
            100.0,
            50.0,
        ],
        da_values=[100.0] * 10,
        capex_values=[90.0] * 10,
        nwc_values=[
            300.0,
            250.0,
            230.0,
            210.0,
            190.0,
            170.0,
            150.0,
            130.0,
            110.0,
            90.0,
            70.0,
        ],
    )

    assert metric.compute(symbol, _OwnerEarningsRepo(records_by_concept)) is None


def test_fcf_fy_median_5y_metric_computes_expected_median():
    metric = FCFFiveYearMedianMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1

    records_by_concept = _build_fcf_fy_records(
        symbol,
        latest_year,
        [200.0, 180.0, 160.0, 140.0, 120.0],
        capex_values=[50.0, 50.0, 50.0, 50.0, 50.0],
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 110.0


def test_fcf_fy_median_5y_metric_uses_zero_capex_when_missing():
    metric = FCFFiveYearMedianMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1

    records_by_concept = _build_fcf_fy_records(
        symbol,
        latest_year,
        [150.0, 140.0, 130.0, 120.0, 110.0],
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 130.0


def test_fcf_fy_median_5y_metric_allows_year_gaps():
    metric = FCFFiveYearMedianMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [
        latest_year,
        latest_year - 2,
        latest_year - 3,
        latest_year - 5,
        latest_year - 6,
    ]

    records_by_concept = {
        "NetCashProvidedByUsedInOperatingActivities": [
            fact(
                symbol=symbol,
                concept="NetCashProvidedByUsedInOperatingActivities",
                fiscal_period="FY",
                end_date=f"{year}-09-30",
                value=value,
                currency="USD",
            )
            for year, value in zip(
                years, [200.0, 180.0, 160.0, 140.0, 120.0], strict=True
            )
        ],
        "CapitalExpenditures": [
            fact(
                symbol=symbol,
                concept="CapitalExpenditures",
                fiscal_period="FY",
                end_date=f"{year}-09-30",
                value=50.0,
                currency="USD",
            )
            for year in years
        ],
    }

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 110.0


def test_fcf_fy_median_5y_metric_requires_five_points():
    metric = FCFFiveYearMedianMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1

    records_by_concept = _build_fcf_fy_records(
        symbol,
        latest_year,
        [200.0, 180.0, 160.0, 140.0],
        capex_values=[50.0, 50.0, 50.0, 50.0],
    )

    assert metric.compute(symbol, _OwnerEarningsRepo(records_by_concept)) is None


def test_fcf_neg_years_10y_metric_counts_negative_values():
    metric = FCFNegativeYearsTenYearMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1

    records_by_concept = _build_fcf_fy_records(
        symbol,
        latest_year,
        [100.0, 90.0, 80.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0, 10.0],
        capex_values=[20.0, 20.0, 20.0, 80.0, 80.0, 20.0, 20.0, 40.0, 30.0, 20.0],
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 5.0


def test_fcf_neg_years_10y_metric_requires_strict_consecutive_years():
    metric = FCFNegativeYearsTenYearMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1

    records_by_concept = _build_fcf_fy_records(
        symbol,
        latest_year,
        [100.0, 90.0, 80.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0],
        capex_values=[20.0] * 9,
    )

    assert metric.compute(symbol, _OwnerEarningsRepo(records_by_concept)) is None


def test_ni_loss_years_10y_metric_uses_fallback_and_counts_negative_values():
    metric = NetIncomeLossYearsTenYearMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1

    records_by_concept = _build_net_income_fy_records(
        symbol,
        latest_year,
        [10.0, -5.0, 20.0, -1.0, 30.0, -2.0, 40.0, 50.0, -3.0, 60.0],
        concept="NetIncomeLossAvailableToCommonStockholdersBasic",
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    assert result.value == 4.0


def test_ni_loss_years_10y_metric_requires_strict_consecutive_years():
    metric = NetIncomeLossYearsTenYearMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1

    records_by_concept = _build_net_income_fy_records(
        symbol,
        latest_year,
        [10.0, -5.0, 20.0, -1.0, 30.0, -2.0, 40.0, 50.0, -3.0],
    )

    assert metric.compute(symbol, _OwnerEarningsRepo(records_by_concept)) is None


def test_oey_ev_norm_metric_uses_oe_fy_median_and_normalized_ev():
    metric = OwnerEarningsYieldEVNormalizedMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]
    records_by_concept = _build_oe_ev_fy_input_records(
        symbol=symbol,
        latest_year=latest_year,
        years=years,
        ebit_values=[500.0, 450.0, 400.0, 350.0, 300.0],
        tax_values=[100.0, 90.0, 80.0, 70.0, 60.0],
        pretax_values=[500.0, 450.0, 400.0, 350.0, 300.0],
        da_values=[100.0, 100.0, 100.0, 100.0, 100.0],
        capex_values=[90.0, 90.0, 90.0, 90.0, 90.0],
    )
    records_by_concept["EnterpriseValue"] = [
        fact(
            symbol=symbol,
            concept="EnterpriseValue",
            end_date=f"{latest_year}-09-30",
            fiscal_period="",
            value=3200.0,
            currency="USD",
        )
    ]

    result = metric.compute(
        symbol,
        _OwnerEarningsRepo(records_by_concept),
        _build_market_repo(market_cap=1.0, as_of=f"{latest_year}-09-30"),
    )
    assert result is not None
    assert result.value == 300.0 / 3200.0


def test_oey_ev_norm_metric_falls_back_to_derived_ev():
    metric = OwnerEarningsYieldEVNormalizedMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]
    records_by_concept = _build_oe_ev_fy_input_records(
        symbol=symbol,
        latest_year=latest_year,
        years=years,
        ebit_values=[500.0, 450.0, 400.0, 350.0, 300.0],
        tax_values=[100.0, 90.0, 80.0, 70.0, 60.0],
        pretax_values=[500.0, 450.0, 400.0, 350.0, 300.0],
        da_values=[100.0, 100.0, 100.0, 100.0, 100.0],
        capex_values=[90.0, 90.0, 90.0, 90.0, 90.0],
    )
    records_by_concept["LongTermDebt"] = [
        fact(
            symbol=symbol,
            concept="LongTermDebt",
            end_date=f"{latest_year}-09-30",
            fiscal_period="FY",
            value=350.0,
            currency="USD",
        )
    ]

    result = metric.compute(
        symbol,
        _OwnerEarningsRepo(records_by_concept),
        _build_market_repo(
            market_cap=2950.0,
            as_of=f"{latest_year}-09-30",
            currency="USD",
        ),
    )
    assert result is not None
    assert result.value == 300.0 / 3250.0


def test_oey_ev_norm_metric_returns_none_for_enterprise_value_currency_mismatch():
    metric = OwnerEarningsYieldEVNormalizedMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(5)]
    records_by_concept = _build_oe_ev_fy_input_records(
        symbol=symbol,
        latest_year=latest_year,
        years=years,
        ebit_values=[500.0, 450.0, 400.0, 350.0, 300.0],
        tax_values=[100.0, 90.0, 80.0, 70.0, 60.0],
        pretax_values=[500.0, 450.0, 400.0, 350.0, 300.0],
        da_values=[100.0, 100.0, 100.0, 100.0, 100.0],
        capex_values=[90.0, 90.0, 90.0, 90.0, 90.0],
    )
    records_by_concept["EnterpriseValue"] = [
        fact(
            symbol=symbol,
            concept="EnterpriseValue",
            end_date=f"{latest_year}-09-30",
            fiscal_period="",
            value=100.0,
            currency="EUR",
        )
    ]

    result = metric.compute(
        symbol,
        _OwnerEarningsRepo(records_by_concept),
        _build_market_repo(market_cap=1.0, as_of=f"{latest_year}-09-30"),
    )
    assert result is None


def _new_metric_quarter_dates():
    today = date.today()
    return (
        (today - timedelta(days=20)).isoformat(),
        (today - timedelta(days=110)).isoformat(),
        (today - timedelta(days=200)).isoformat(),
        (today - timedelta(days=290)).isoformat(),
        (today - timedelta(days=380)).isoformat(),
    )


def test_gross_margin_ttm_metric_clamps_and_uses_gross_profit_fallback():
    metric = GrossMarginTTMMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1, _ = _new_metric_quarter_dates()
    repo = _build_metric_repo(
        concept_records={
            "Revenues": _quarterly_records(
                "Revenues", (q4, q3, q2, q1), (100.0, 100.0, 100.0, 100.0)
            ),
            "GrossProfit": _quarterly_records(
                "GrossProfit", (q4, q3, q2, q1), (250.0, 250.0, 250.0, 250.0)
            ),
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert result.value == 1.0


def test_gross_margin_ttm_metric_returns_none_when_revenue_non_positive():
    metric = GrossMarginTTMMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1, _ = _new_metric_quarter_dates()
    repo = _build_metric_repo(
        concept_records={
            "Revenues": _quarterly_records(
                "Revenues", (q4, q3, q2, q1), (-10.0, -10.0, -10.0, -10.0)
            ),
            "CostOfRevenue": _quarterly_records(
                "CostOfRevenue", (q4, q3, q2, q1), (5.0, 5.0, 5.0, 5.0)
            ),
        }
    )

    assert metric.compute(symbol, repo) is None


def test_operating_margin_ttm_metric_allows_negative_values():
    metric = OperatingMarginTTMMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1, _ = _new_metric_quarter_dates()
    repo = _build_metric_repo(
        concept_records={
            "Revenues": _quarterly_records(
                "Revenues", (q4, q3, q2, q1), (100.0, 100.0, 100.0, 100.0)
            ),
            "OperatingIncomeLoss": _quarterly_records(
                "OperatingIncomeLoss", (q4, q3, q2, q1), (-10.0, -10.0, -10.0, -10.0)
            ),
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert result.value == -0.1


def test_fcf_margin_ttm_metric_allows_negative_values():
    metric = FCFMarginTTMMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1, _ = _new_metric_quarter_dates()
    repo = _build_metric_repo(
        concept_records={
            "Revenues": _quarterly_records(
                "Revenues", (q4, q3, q2, q1), (100.0, 100.0, 100.0, 100.0)
            ),
            "NetCashProvidedByUsedInOperatingActivities": _quarterly_records(
                "NetCashProvidedByUsedInOperatingActivities",
                (q4, q3, q2, q1),
                (5.0, 5.0, 5.0, 5.0),
            ),
            "CapitalExpenditures": _quarterly_records(
                "CapitalExpenditures", (q4, q3, q2, q1), (10.0, 10.0, 10.0, 10.0)
            ),
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert result.value == -0.05


def test_gross_profit_to_assets_ttm_metric_uses_avg_assets():
    metric = GrossProfitToAssetsTTMMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1, q4_prev = _new_metric_quarter_dates()
    repo = _build_metric_repo(
        concept_records={
            "Revenues": _quarterly_records(
                "Revenues", (q4, q3, q2, q1), (100.0, 100.0, 100.0, 100.0)
            ),
            "CostOfRevenue": _quarterly_records(
                "CostOfRevenue", (q4, q3, q2, q1), (40.0, 40.0, 40.0, 40.0)
            ),
            "Assets": [
                fact(
                    symbol=symbol,
                    concept="Assets",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="Assets",
                    fiscal_period="Q4",
                    end_date=q4_prev,
                    value=100.0,
                    currency="USD",
                ),
            ],
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert result.value == 240.0 / 150.0


def test_roe_ttm_metric_falls_back_to_fy_average():
    metric = ROETTMMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1, _ = _new_metric_quarter_dates()
    current_year = date.today().year - 1
    repo = _build_metric_repo(
        concept_records={
            "NetIncomeLossAvailableToCommonStockholdersBasic": _quarterly_records(
                "NetIncomeLossAvailableToCommonStockholdersBasic",
                (q4, q3, q2, q1),
                (25.0, 25.0, 25.0, 25.0),
            ),
            "CommonStockholdersEquity": [
                fact(
                    symbol=symbol,
                    concept="CommonStockholdersEquity",
                    fiscal_period="FY",
                    end_date=f"{current_year}-09-30",
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CommonStockholdersEquity",
                    fiscal_period="FY",
                    end_date=f"{current_year - 1}-09-30",
                    value=100.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CommonStockholdersEquity",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=190.0,
                    currency="USD",
                ),
            ],
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert result.value == 100.0 / 150.0


def test_roa_ttm_metric_uses_same_quarter_average_assets():
    metric = ROATTMMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1, q4_prev = _new_metric_quarter_dates()
    repo = _build_metric_repo(
        concept_records={
            "NetIncomeLoss": _quarterly_records(
                "NetIncomeLoss", (q4, q3, q2, q1), (10.0, 10.0, 10.0, 10.0)
            ),
            "Assets": [
                fact(
                    symbol=symbol,
                    concept="Assets",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=250.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="Assets",
                    fiscal_period="Q4",
                    end_date=q4_prev,
                    value=150.0,
                    currency="USD",
                ),
            ],
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert result.value == 40.0 / 200.0


def test_roetce_ttm_metric_treats_missing_goodwill_and_intangibles_as_zero():
    metric = ROETangibleCommonEquityTTMMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1, q4_prev = _new_metric_quarter_dates()
    repo = _build_metric_repo(
        concept_records={
            "NetIncomeLossAvailableToCommonStockholdersBasic": _quarterly_records(
                "NetIncomeLossAvailableToCommonStockholdersBasic",
                (q4, q3, q2, q1),
                (10.0, 10.0, 10.0, 10.0),
            ),
            "CommonStockholdersEquity": [
                fact(
                    symbol=symbol,
                    concept="CommonStockholdersEquity",
                    fiscal_period="Q4",
                    end_date=q4,
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CommonStockholdersEquity",
                    fiscal_period="Q4",
                    end_date=q4_prev,
                    value=100.0,
                    currency="USD",
                ),
            ],
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert result.value == 40.0 / 150.0


def test_dividend_yield_ttm_metric_uses_cash_dividends_and_abs_sign():
    metric = DividendYieldTTMMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1, _ = _new_metric_quarter_dates()
    repo = _build_metric_repo(
        concept_records={
            "CommonStockDividendsPaid": _quarterly_records(
                "CommonStockDividendsPaid",
                (q4, q3, q2, q1),
                (-5.0, -5.0, -5.0, -5.0),
            )
        }
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=200.0, as_of=q4, price=100.0)
    )
    assert result is not None
    assert result.value == 20.0 / 200.0


def test_dividend_yield_ttm_metric_falls_back_to_dps_and_price():
    metric = DividendYieldTTMMetric()
    symbol = "AAPL.US"
    q4, _, _, _, _ = _new_metric_quarter_dates()
    repo = _build_metric_repo(
        concept_records={
            "CommonStockDividendsPerShareCashPaid": [
                fact(
                    symbol=symbol,
                    concept="CommonStockDividendsPerShareCashPaid",
                    fiscal_period="",
                    end_date=q4,
                    value=2.5,
                    currency="USD",
                )
            ]
        }
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=None, as_of=q4, price=50.0)
    )
    assert result is not None
    assert result.value == 2.5 / 50.0


def test_shareholder_yield_ttm_metric_sums_dividend_and_buyback_yields():
    metric = ShareholderYieldTTMMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1, _ = _new_metric_quarter_dates()
    repo = _build_metric_repo(
        concept_records={
            "CommonStockDividendsPaid": _quarterly_records(
                "CommonStockDividendsPaid",
                (q4, q3, q2, q1),
                (-5.0, -5.0, -5.0, -5.0),
            ),
            "SalePurchaseOfStock": _quarterly_records(
                "SalePurchaseOfStock",
                (q4, q3, q2, q1),
                (-10.0, -10.0, -10.0, -10.0),
            ),
        }
    )

    result = metric.compute(
        symbol, repo, _build_market_repo(market_cap=200.0, as_of=q4, price=100.0)
    )
    assert result is not None
    assert result.value == (20.0 / 200.0) + (40.0 / 200.0)


def test_dividend_payout_ratio_ttm_metric_requires_positive_net_income():
    metric = DividendPayoutRatioTTMMetric()
    symbol = "AAPL.US"
    q4, q3, q2, q1, _ = _new_metric_quarter_dates()
    repo = _build_metric_repo(
        concept_records={
            "CommonStockDividendsPaid": _quarterly_records(
                "CommonStockDividendsPaid",
                (q4, q3, q2, q1),
                (-5.0, -5.0, -5.0, -5.0),
            ),
            "NetIncomeLossAvailableToCommonStockholdersBasic": _quarterly_records(
                "NetIncomeLossAvailableToCommonStockholdersBasic",
                (q4, q3, q2, q1),
                (-10.0, -10.0, -10.0, -10.0),
            ),
        }
    )

    assert metric.compute(symbol, repo) is None


def test_revenue_cagr_10y_metric_uses_strict_fy_pair():
    metric = RevenueCAGR10YMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    repo = _build_metric_repo(
        concept_records={
            "Revenues": [
                fact(
                    symbol=symbol,
                    concept="Revenues",
                    fiscal_period="FY",
                    end_date=f"{latest_year}-09-30",
                    value=200.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="Revenues",
                    fiscal_period="FY",
                    end_date=f"{latest_year - 10}-09-30",
                    value=100.0,
                    currency="USD",
                ),
            ]
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert round(result.value, 8) == round((2.0**0.1) - 1.0, 8)


def test_fcf_per_share_cagr_10y_metric_computes_happy_path():
    metric = FCFPerShareCAGR10YMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    repo = _build_metric_repo(
        concept_records={
            "NetCashProvidedByUsedInOperatingActivities": [
                fact(
                    symbol=symbol,
                    concept="NetCashProvidedByUsedInOperatingActivities",
                    fiscal_period="FY",
                    end_date=f"{latest_year}-09-30",
                    value=120.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetCashProvidedByUsedInOperatingActivities",
                    fiscal_period="FY",
                    end_date=f"{latest_year - 10}-09-30",
                    value=60.0,
                    currency="USD",
                ),
            ],
            "CapitalExpenditures": [
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="FY",
                    end_date=f"{latest_year}-09-30",
                    value=20.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="CapitalExpenditures",
                    fiscal_period="FY",
                    end_date=f"{latest_year - 10}-09-30",
                    value=10.0,
                    currency="USD",
                ),
            ],
            "WeightedAverageNumberOfDilutedSharesOutstanding": [
                fact(
                    symbol=symbol,
                    concept="WeightedAverageNumberOfDilutedSharesOutstanding",
                    fiscal_period="FY",
                    end_date=f"{latest_year}-09-30",
                    value=10.0,
                    currency=None,
                ),
                fact(
                    symbol=symbol,
                    concept="WeightedAverageNumberOfDilutedSharesOutstanding",
                    fiscal_period="FY",
                    end_date=f"{latest_year - 10}-09-30",
                    value=10.0,
                    currency=None,
                ),
            ],
        }
    )

    result = metric.compute(symbol, repo)
    assert result is not None
    assert round(result.value, 8) == round((2.0**0.1) - 1.0, 8)


def test_fcf_per_share_cagr_10y_metric_requires_diluted_shares():
    metric = FCFPerShareCAGR10YMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    repo = _build_metric_repo(
        concept_records={
            "NetCashProvidedByUsedInOperatingActivities": [
                fact(
                    symbol=symbol,
                    concept="NetCashProvidedByUsedInOperatingActivities",
                    fiscal_period="FY",
                    end_date=f"{latest_year}-09-30",
                    value=120.0,
                    currency="USD",
                ),
                fact(
                    symbol=symbol,
                    concept="NetCashProvidedByUsedInOperatingActivities",
                    fiscal_period="FY",
                    end_date=f"{latest_year - 10}-09-30",
                    value=60.0,
                    currency="USD",
                ),
            ]
        }
    )

    assert metric.compute(symbol, repo) is None


def test_owner_earnings_cagr_10y_metric_uses_three_year_endpoint_averages():
    metric = OwnerEarningsCAGR10YMetric()
    symbol = "AAPL.US"
    latest_year = date.today().year - 1
    years = [latest_year - offset for offset in range(10)]
    records_by_concept = _build_oe_ev_fy_input_records(
        symbol=symbol,
        latest_year=latest_year,
        years=years,
        ebit_values=[
            530.0,
            500.0,
            470.0,
            440.0,
            410.0,
            380.0,
            350.0,
            320.0,
            290.0,
            260.0,
        ],
        tax_values=[106.0, 100.0, 94.0, 88.0, 82.0, 76.0, 70.0, 64.0, 58.0, 52.0],
        pretax_values=[
            530.0,
            500.0,
            470.0,
            440.0,
            410.0,
            380.0,
            350.0,
            320.0,
            290.0,
            260.0,
        ],
        da_values=[100.0] * 10,
        capex_values=[90.0] * 10,
        nwc_values=[
            300.0,
            280.0,
            260.0,
            240.0,
            220.0,
            200.0,
            180.0,
            160.0,
            140.0,
            120.0,
            100.0,
        ],
    )

    result = metric.compute(symbol, _OwnerEarningsRepo(records_by_concept))
    assert result is not None
    start_avg = (246.0 + 222.0 + 198.0) / 3.0
    end_avg = (414.0 + 390.0 + 366.0) / 3.0
    assert round(result.value, 8) == round(
        (end_avg / start_avg) ** (1.0 / 7.0) - 1.0, 8
    )


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
    assert "roic_7y_median" in REGISTRY
    assert "roic_years_above_12pct" in REGISTRY
    assert "roic_10y_min" in REGISTRY
    assert "roic_7y_min" in REGISTRY
    assert "iroic_5y" in REGISTRY
    assert "gm_10y_std" in REGISTRY
    assert "opm_10y_std" in REGISTRY
    assert "opm_10y_min" in REGISTRY
    assert "opm_7y_min" in REGISTRY
    assert "cfo_to_ni_ttm" in REGISTRY
    assert "cfo_to_ni_10y_median" in REGISTRY
    assert "fcf_fy_median_5y" in REGISTRY
    assert "ni_loss_years_10y" in REGISTRY
    assert "fcf_neg_years_10y" in REGISTRY
    assert "accruals_ratio" in REGISTRY
    assert "share_count_cagr_5y" in REGISTRY
    assert "share_count_cagr_10y" in REGISTRY
    assert "shares_10y_pct_change" in REGISTRY
    assert "net_buyback_yield" in REGISTRY
    assert "ebit_yield_ev" in REGISTRY
    assert "fcf_yield_ev" in REGISTRY
    assert "ev_to_ebit" in REGISTRY
    assert "ev_to_ebitda" in REGISTRY
    assert "sbc_to_revenue" in REGISTRY
    assert "sbc_to_fcf" in REGISTRY
    assert "gross_margin_ttm" in REGISTRY
    assert "operating_margin_ttm" in REGISTRY
    assert "fcf_margin_ttm" in REGISTRY
    assert "roe_ttm" in REGISTRY
    assert "roa_ttm" in REGISTRY
    assert "roetce_ttm" in REGISTRY
    assert "dividend_yield_ttm" in REGISTRY
    assert "shareholder_yield_ttm" in REGISTRY
    assert "dividend_payout_ratio_ttm" in REGISTRY
    assert "revenue_cagr_10y" in REGISTRY
    assert "fcf_per_share_cagr_10y" in REGISTRY
    assert "owner_earnings_cagr_10y" in REGISTRY
    assert "gross_profit_to_assets_ttm" in REGISTRY
    assert "oe_ev_fy_median_5y" in REGISTRY
    assert "worst_oe_ev_fy_10y" in REGISTRY
    assert "oey_ev_norm" in REGISTRY
