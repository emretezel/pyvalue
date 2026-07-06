"""Regression: TTM-flow metrics measure annual-only filers via the annual cadence.

Annual-only issuers (companies that file only an annual/FY income statement,
no quarterly) used to NA on every trailing-twelve-month flow metric, because
``resolve_ttm_window`` builds from quarterly rows. Each metric here opts into
the resolver's annual cadence (a single fresh FY row), so an annual-only filer
becomes computable. The cases fail on the pre-opt-in code (NA) and pass with
the annual cadence.

One case per metric also pins the *cadence-matched balance-sheet freshness*:
an annual filer's balance sheet is filed on the same once-a-year cadence, so
when the income flow resolves annual the point-in-time legs widen to the
480-day FY window (a fresh annual EBITDA over a 430-day-old balance sheet must
not be a false NA).

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.marketdata.base import PriceData
from pyvalue.metrics.cash_conversion import CFOToNITTMMetric
from pyvalue.metrics.enterprise_value_ratios import EVToEBITMetric, EVToSalesMetric
from pyvalue.metrics.fcf_to_ebitda import FCFToEBITDAMetric
from pyvalue.metrics.net_debt_to_ebitda import NetDebtToEBITDAMetric
from pyvalue.metrics.profitability_returns_growth import (
    GrossMarginTTMMetric,
    GrossProfitToAssetsTTMMetric,
    ShareholderYieldTTMMetric,
)
from pyvalue.persistence.storage import MarketDataRepository

LISTING_ID = 1
_TODAY = date.today()

# A fresh FY end_date (well inside 400d) and one in the 400-480d band: stale
# under the standard window, fresh under the FY window.
FRESH_FY = (_TODAY - timedelta(days=180)).isoformat()
BAND_FY = (_TODAY - timedelta(days=430)).isoformat()
# Prior-year FY end_dates (one calendar year before the latest), for the
# two-point average-assets denominator. Subtracting 365 days always lands in
# the previous calendar year, which is what the same-period prior-year match
# keys on; only the latest point is freshness-checked, so the prior may be old.
PRIOR_FRESH_FY = (_TODAY - timedelta(days=180 + 365)).isoformat()
PRIOR_BAND_FY = (_TODAY - timedelta(days=430 + 365)).isoformat()


class _FakeFactsRepo(RegionFactsRepository):
    """In-memory fact source keyed by concept, mirroring the read path."""

    def __init__(self, records_by_concept: dict[str, list[FactRecord]]) -> None:
        super().__init__(self)
        self._records_by_concept = records_by_concept

    def facts_for_concept(
        self,
        listing_id: int,
        concept: str,
        fiscal_period: str | None = None,
        limit: int | None = None,
    ) -> list[FactRecord]:
        records = list(self._records_by_concept.get(concept, []))
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

    def latest_fact(self, listing_id: int, concept: str) -> FactRecord | None:
        records = self.facts_for_concept(listing_id, concept)
        if not records:
            return None
        return max(records, key=lambda record: record.end_date)

    def ticker_currency_by_id(self, listing_id: int) -> str | None:
        return "USD"


def _fy(concept: str, value: float, *, end_date: str = FRESH_FY) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period="FY",
        end_date=end_date,
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _net_debt_repo(*, end_date: str) -> _FakeFactsRepo:
    # EBITDA = FY EBIT 80 + FY D&A 20 = 100; net debt = (30 + 70) - 50 = 50;
    # ratio = 0.5.
    return _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [
                _fy("OperatingIncomeLoss", 80.0, end_date=end_date)
            ],
            "DepreciationDepletionAndAmortization": [
                _fy("DepreciationDepletionAndAmortization", 20.0, end_date=end_date)
            ],
            "ShortTermDebt": [_fy("ShortTermDebt", 30.0, end_date=end_date)],
            "LongTermDebt": [_fy("LongTermDebt", 70.0, end_date=end_date)],
            "CashAndShortTermInvestments": [
                _fy("CashAndShortTermInvestments", 50.0, end_date=end_date)
            ],
        }
    )


def test_net_debt_to_ebitda_measures_an_annual_only_filer() -> None:
    result = NetDebtToEBITDAMetric().compute(
        LISTING_ID, _net_debt_repo(end_date=FRESH_FY)
    )
    assert result is not None
    assert result.value == 0.5


def test_net_debt_to_ebitda_cadence_matched_freshness_in_post_fye_band() -> None:
    # FY data 430 days old: the income leg resolves on the 480-day annual
    # window, and the net-debt legs must follow it (not the 400-day default).
    result = NetDebtToEBITDAMetric().compute(
        LISTING_ID, _net_debt_repo(end_date=BAND_FY)
    )
    assert result is not None
    assert result.value == 0.5


class _FakeMarketRepo(MarketDataRepository):
    # Nominal MarketDataRepository subtype whose __init__ skips super() so no
    # SQLite DB is opened; serves one fixed fresh in-memory snapshot.
    def __init__(self, price: float) -> None:
        self._price = price

    def latest_snapshot_by_id(self, listing_id: int) -> PriceData | None:
        return PriceData(
            symbol="TEST.US",
            price=self._price,
            as_of=(_TODAY - timedelta(days=1)).isoformat(),
            currency="USD",
        )

    def ticker_currency_by_id(self, listing_id: int) -> str | None:
        return "USD"


def _shares(value: float) -> list[FactRecord]:
    return [
        FactRecord(
            symbol="TEST.US",
            concept="CommonStockSharesOutstanding",
            fiscal_period="FY",
            end_date=FRESH_FY,
            unit_kind="count",
            value=value,
            filed=None,
            currency=None,
        )
    ]


def _ev_balance_sheet() -> dict[str, list[FactRecord]]:
    # Shares 100 x price 5 = 500 market cap; net debt (30 + 70) - 50 = 50;
    # EV = 550.
    return {
        "ShortTermDebt": [_fy("ShortTermDebt", 30.0)],
        "LongTermDebt": [_fy("LongTermDebt", 70.0)],
        "CashAndShortTermInvestments": [_fy("CashAndShortTermInvestments", 50.0)],
        "CommonStockSharesOutstanding": _shares(100.0),
    }


def test_ev_to_ebit_measures_an_annual_only_filer() -> None:
    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [_fy("OperatingIncomeLoss", 80.0)],
            **_ev_balance_sheet(),
        }
    )
    result = EVToEBITMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=5.0))
    assert result is not None
    # EV 550 / FY EBIT 80.
    assert abs(result.value - 550.0 / 80.0) < 1e-12


def test_ev_to_sales_measures_an_annual_only_filer() -> None:
    repo = _FakeFactsRepo(
        {
            "Revenues": [_fy("Revenues", 200.0)],
            **_ev_balance_sheet(),
        }
    )
    result = EVToSalesMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=5.0))
    assert result is not None
    # EV 550 / FY revenue 200.
    assert abs(result.value - 550.0 / 200.0) < 1e-12


def test_fcf_to_ebitda_measures_an_annual_only_filer() -> None:
    # Ratio of two annual flows, no EV denominator. Capex omitted (assumed
    # zero), so FCF = FY OCF 60; EBITDA = FY EBIT 80 + FY D&A 20 = 100.
    repo = _FakeFactsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": [
                _fy("NetCashProvidedByUsedInOperatingActivities", 60.0)
            ],
            "OperatingIncomeLoss": [_fy("OperatingIncomeLoss", 80.0)],
            "DepreciationDepletionAndAmortization": [
                _fy("DepreciationDepletionAndAmortization", 20.0)
            ],
        }
    )
    result = FCFToEBITDAMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 0.6) < 1e-12


def test_cfo_to_ni_ttm_measures_an_annual_only_filer() -> None:
    # Ratio of two annual flows: FY CFO 90 / FY net income 100 = 0.9.
    repo = _FakeFactsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": [
                _fy("NetCashProvidedByUsedInOperatingActivities", 90.0)
            ],
            "NetIncomeLoss": [_fy("NetIncomeLoss", 100.0)],
        }
    )
    result = CFOToNITTMMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 0.9) < 1e-12


def test_gross_margin_ttm_measures_an_annual_only_filer() -> None:
    # Self-normalizing ratio of two annual flows: (FY revenue 200 - FY COGS
    # 120) / FY revenue 200 = 0.4. The COGS companion is paired on the FY key.
    repo = _FakeFactsRepo(
        {
            "Revenues": [_fy("Revenues", 200.0)],
            "CostOfRevenue": [_fy("CostOfRevenue", 120.0)],
        }
    )
    result = GrossMarginTTMMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 0.4) < 1e-12


def _gross_profit_to_assets_repo(*, latest_fy: str, prior_fy: str) -> _FakeFactsRepo:
    # FY gross profit = 200 - 120 = 80; average assets = (1000 + 800) / 2 = 900.
    return _FakeFactsRepo(
        {
            "Revenues": [_fy("Revenues", 200.0, end_date=latest_fy)],
            "CostOfRevenue": [_fy("CostOfRevenue", 120.0, end_date=latest_fy)],
            "Assets": [
                _fy("Assets", 1000.0, end_date=latest_fy),
                _fy("Assets", 800.0, end_date=prior_fy),
            ],
        }
    )


def test_gross_profit_to_assets_ttm_measures_an_annual_only_filer() -> None:
    result = GrossProfitToAssetsTTMMetric().compute(
        LISTING_ID,
        _gross_profit_to_assets_repo(latest_fy=FRESH_FY, prior_fy=PRIOR_FRESH_FY),
    )
    assert result is not None
    # gross profit 80 / average assets 900.
    assert abs(result.value - 80.0 / 900.0) < 1e-12


def test_gross_profit_to_assets_ttm_annual_assets_in_post_fye_band() -> None:
    # Latest FY balance sheet 430 days old: no quarterly assets, so the two-
    # point average resolves on the FY cadence's 480-day window (not the 400-
    # day default that would reject it).
    result = GrossProfitToAssetsTTMMetric().compute(
        LISTING_ID,
        _gross_profit_to_assets_repo(latest_fy=BAND_FY, prior_fy=PRIOR_BAND_FY),
    )
    assert result is not None
    assert abs(result.value - 80.0 / 900.0) < 1e-12


def test_shareholder_yield_ttm_measures_an_annual_only_filer() -> None:
    # Both legs are annual flows over a live market cap (shares 100 x price 5 =
    # 500). Dividend leg: |FY dividends 20| / 500 = 0.04. Buyback leg:
    # -(FY sale/purchase of stock -30) / 500 = 0.06. Shareholder yield = 0.10.
    repo = _FakeFactsRepo(
        {
            "CommonStockDividendsPaid": [_fy("CommonStockDividendsPaid", -20.0)],
            "SalePurchaseOfStock": [_fy("SalePurchaseOfStock", -30.0)],
            "CommonStockSharesOutstanding": _shares(100.0),
        }
    )
    result = ShareholderYieldTTMMetric().compute(
        LISTING_ID, repo, _FakeMarketRepo(price=5.0)
    )
    assert result is not None
    assert abs(result.value - 0.10) < 1e-12
