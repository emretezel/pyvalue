"""Regression: dividend_payout_ratio_ttm resolves annually-reported dividends.

EODHD's quarterly financing block is only intermittently populated. SK Hynix
(``000660.KO``) carries ``dividendsPaid`` in 37 of 61 quarters and none at all
in Q1/Q2 since 2024, while every operating line in those same quarters is
complete -- so the dividend end dates run Q4, Q3, then a 273-day jump to the
prior Q4. That spacing matches neither the quarterly (70-110 day) nor the
semi-annual (150-220 day) band, the window is correctly refused, and the metric
went NA even though the FY dividends row is present and correct.

Two things are pinned here:

- the dividends leg opts into the resolver's annual cadence, so a filer whose
  dividends survive only as an FY row measures a payout (this is what
  ``dividend_yield_ttm`` already did while the payout ratio did not);
- on that annual path the *denominator* is pinned to the same fiscal year.
  Letting net income resolve its own quarterly trailing window would divide a
  calendar-2025 dividend total by a window ending 2026-03-31 -- two different
  twelve-month spans. With Hynix's real figures that reads 2.24% against a
  coherent 3.92%, a 43% understatement.

The first case fails on the pre-fix code (NA) and passes after. The quarterly
cases pin that nothing on the existing path moved.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.profitability_returns_growth import (
    DividendPayoutRatioTTMMetric,
)

LISTING_ID = 1
_TODAY = date.today()

# Four consecutive quarter-ends at the resolver's expected ~91-day cadence,
# newest one comfortably fresh. Index 0 is the newest.
_QUARTER_ENDS = tuple(
    (_TODAY - timedelta(days=30 + 91 * offset)).isoformat() for offset in range(8)
)
# The FY row sits on the newest quarter end, exactly as EODHD stamps a Q4/FY
# pair, and stays inside the 480-day FY freshness window.
_FY_END = _QUARTER_ENDS[0]

DIVIDENDS = "CommonStockDividendsPaid"
NET_INCOME_COMMON = "NetIncomeLossAvailableToCommonStockholdersBasic"

# SK Hynix FY2025, in KRW billions to keep the arithmetic readable. The real
# rows are -1,681,166,000,000 dividends over 42,919,287,000,000 net income.
HYNIX_FY_DIVIDENDS = -1_681_166.0
HYNIX_FY_NET_INCOME = 42_919_287.0
HYNIX_PAYOUT = abs(HYNIX_FY_DIVIDENDS) / HYNIX_FY_NET_INCOME  # 3.9170%


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
        return "KRW"


def _monetary(
    concept: str, value: float, *, end_date: str, fiscal_period: str
) -> FactRecord:
    return FactRecord(
        symbol="000660.KO",
        concept=concept,
        fiscal_period=fiscal_period,
        end_date=end_date,
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="KRW",
    )


def _quarters(concept: str, value: float, *, count: int = 4) -> list[FactRecord]:
    """``count`` consecutive quarterly rows, newest first."""

    periods = ("Q4", "Q3", "Q2", "Q1")
    return [
        _monetary(
            concept,
            value,
            end_date=_QUARTER_ENDS[offset],
            fiscal_period=periods[offset % 4],
        )
        for offset in range(count)
    ]


def _hynix_dividend_rows() -> list[FactRecord]:
    """The Q3/Q4-only shape: a 273-day hole where Q1/Q2 should be.

    Indices 0 and 1 are the newest two quarters; index 4 is four quarters back,
    so the gap from index 1 to index 4 spans three quarters -- the same
    "two half-years, sixteen months apart" spacing the live payload produces.
    """

    return [
        _monetary(DIVIDENDS, -263_131.0, end_date=_QUARTER_ENDS[0], fiscal_period="Q4"),
        _monetary(DIVIDENDS, -258_921.0, end_date=_QUARTER_ENDS[1], fiscal_period="Q3"),
        _monetary(DIVIDENDS, -206_712.0, end_date=_QUARTER_ENDS[4], fiscal_period="Q4"),
        _monetary(DIVIDENDS, -206_585.0, end_date=_QUARTER_ENDS[5], fiscal_period="Q3"),
    ]


def test_annual_dividends_resolve_through_the_fy_row() -> None:
    # The regression: quarterly dividends form no window, but the FY row does.
    # Pre-fix this was NA; the fallback now measures the payout, and the
    # denominator is the *same* fiscal year rather than a quarterly window
    # ending on a different date.
    repo = _FakeFactsRepo(
        {
            DIVIDENDS: [
                *_hynix_dividend_rows(),
                _monetary(
                    DIVIDENDS,
                    HYNIX_FY_DIVIDENDS,
                    end_date=_FY_END,
                    fiscal_period="FY",
                ),
            ],
            NET_INCOME_COMMON: [
                # A complete quarterly net-income history -- exactly Hynix's
                # shape, and the trap: it resolves a window of its own that the
                # dividend numerator does not cover.
                *_quarters(NET_INCOME_COMMON, HYNIX_FY_NET_INCOME / 4.0, count=8),
                _monetary(
                    NET_INCOME_COMMON,
                    HYNIX_FY_NET_INCOME,
                    end_date=_FY_END,
                    fiscal_period="FY",
                ),
            ],
        }
    )

    result = DividendPayoutRatioTTMMetric().compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == pytest.approx(HYNIX_PAYOUT)
    assert result.as_of == _FY_END


def test_annual_dividends_without_an_aligned_fy_net_income_stay_na() -> None:
    # An FY dividend total with no FY net-income companion must not fall back
    # to the quarterly window: that is the cross-cadence ratio this guards.
    repo = _FakeFactsRepo(
        {
            DIVIDENDS: [
                *_hynix_dividend_rows(),
                _monetary(
                    DIVIDENDS,
                    HYNIX_FY_DIVIDENDS,
                    end_date=_FY_END,
                    fiscal_period="FY",
                ),
            ],
            NET_INCOME_COMMON: _quarters(
                NET_INCOME_COMMON, HYNIX_FY_NET_INCOME / 4.0, count=8
            ),
        }
    )

    assert DividendPayoutRatioTTMMetric().compute(LISTING_ID, repo) is None


def test_broken_dividend_window_without_any_fy_row_stays_na() -> None:
    # No FY row to rescue the window, and a fresh nonzero dividends row rules
    # out the zero-payout inference: an honest NA, as before the change.
    repo = _FakeFactsRepo(
        {
            DIVIDENDS: _hynix_dividend_rows(),
            NET_INCOME_COMMON: _quarters(
                NET_INCOME_COMMON, HYNIX_FY_NET_INCOME / 4.0, count=8
            ),
        }
    )

    assert DividendPayoutRatioTTMMetric().compute(LISTING_ID, repo) is None


def test_clean_quarterly_filer_is_unchanged_by_the_fallback() -> None:
    # The no-regression case that covers the 15k listings computing today: a
    # complete quarterly history never reaches the annual branch, and net
    # income keeps resolving its own window.
    repo = _FakeFactsRepo(
        {
            DIVIDENDS: _quarters(DIVIDENDS, -25.0),
            NET_INCOME_COMMON: _quarters(NET_INCOME_COMMON, 100.0),
        }
    )

    result = DividendPayoutRatioTTMMetric().compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == pytest.approx(100.0 / 400.0)
    assert result.as_of == _QUARTER_ENDS[0]


def test_loss_making_filer_stays_na_on_the_annual_path() -> None:
    # A payout ratio is undefined without positive earnings, and the guard must
    # survive the reordering that put the dividends leg first.
    repo = _FakeFactsRepo(
        {
            DIVIDENDS: [
                *_hynix_dividend_rows(),
                _monetary(
                    DIVIDENDS,
                    HYNIX_FY_DIVIDENDS,
                    end_date=_FY_END,
                    fiscal_period="FY",
                ),
            ],
            NET_INCOME_COMMON: [
                _monetary(
                    NET_INCOME_COMMON,
                    -5_000.0,
                    end_date=_FY_END,
                    fiscal_period="FY",
                )
            ],
        }
    )

    assert DividendPayoutRatioTTMMetric().compute(LISTING_ID, repo) is None
