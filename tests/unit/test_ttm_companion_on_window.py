"""Unit tests for summing a companion concept over an anchor TTM window.

``_compute_ttm_amount_on_window`` is the second half of the two-flow-ratio
rule: where one leg resolves the window, the other is summed over exactly that
window's periods instead of resolving one of its own. It is the reusable
primitive behind ``dividend_payout_ratio_ttm``'s annual path, so its contract
is pinned directly rather than only through the metric.

Three properties matter:

- an annual window pairs only with the ``FY`` companion, never a same-dated
  quarter (an FY row routinely shares Q4's end date);
- a quarterly window pairs on every quarter and sums all four;
- a window period with no companion fails the whole pairing rather than
  summing short, which would silently compare unequal spans.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pytest

from pyvalue.facts import FactRecord, MonetaryFact, RegionFactsRepository
from pyvalue.metrics.profitability_returns_growth import (
    ProfitabilityReturnsGrowthCalculator,
    _MoneySnapshot,
)
from pyvalue.metrics.ttm import TTMWindow, resolve_ttm_window
from pyvalue.metrics.utils import MAX_FY_FACT_AGE_DAYS

LISTING_ID = 1
_TODAY = date.today()
_QUARTER_ENDS = tuple(
    (_TODAY - timedelta(days=30 + 91 * offset)).isoformat() for offset in range(4)
)

ANCHOR = "CommonStockDividendsPaid"
COMPANION = "NetIncomeLossAvailableToCommonStockholdersBasic"


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


def _monetary(
    concept: str, value: float, *, end_date: str, fiscal_period: str
) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period=fiscal_period,
        end_date=end_date,
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _quarters(concept: str, values: tuple[float, ...]) -> list[FactRecord]:
    periods = ("Q4", "Q3", "Q2", "Q1")
    return [
        _monetary(concept, value, end_date=end_date, fiscal_period=period)
        for value, end_date, period in zip(values, _QUARTER_ENDS, periods)
    ]


def _sum_companion(
    repo: _FakeFactsRepo,
) -> tuple[TTMWindow[MonetaryFact], Optional[_MoneySnapshot]]:
    """Resolve the anchor window, then sum the companion over its periods."""

    window = resolve_ttm_window(
        repo.monetary_facts_for_concept(LISTING_ID, ANCHOR),
        annual_max_age_days=MAX_FY_FACT_AGE_DAYS,
    ).window
    assert window is not None
    companion = ProfitabilityReturnsGrowthCalculator()._compute_ttm_amount_on_window(
        LISTING_ID, repo, (COMPANION,), window=window, context="unit_test"
    )
    return window, companion


def test_annual_window_pairs_the_fy_companion_not_a_same_dated_quarter() -> None:
    # The FY companion shares Q4's end date. Pairing must pick the annual row;
    # taking the quarter would divide a year's flow by a quarter's.
    fy_end = _QUARTER_ENDS[0]
    repo = _FakeFactsRepo(
        {
            # Only two quarters, a year apart: no sub-annual window forms, so
            # the resolver falls through to the FY row.
            ANCHOR: [
                _monetary(ANCHOR, -10.0, end_date=fy_end, fiscal_period="Q4"),
                _monetary(ANCHOR, -40.0, end_date=fy_end, fiscal_period="FY"),
            ],
            COMPANION: [
                _monetary(COMPANION, 25.0, end_date=fy_end, fiscal_period="Q4"),
                _monetary(COMPANION, 400.0, end_date=fy_end, fiscal_period="FY"),
            ],
        }
    )

    window, result = _sum_companion(repo)

    assert window.cadence == "annual"
    assert result is not None
    assert result.money.amount == pytest.approx(400.0)
    assert result.as_of == fy_end


def test_quarterly_window_sums_every_quarter() -> None:
    repo = _FakeFactsRepo(
        {
            ANCHOR: _quarters(ANCHOR, (-1.0, -2.0, -3.0, -4.0)),
            COMPANION: _quarters(COMPANION, (10.0, 20.0, 30.0, 40.0)),
        }
    )

    window, result = _sum_companion(repo)

    assert window.cadence == "quarterly"
    assert result is not None
    assert result.money.amount == pytest.approx(100.0)
    assert result.as_of == _QUARTER_ENDS[0]


def test_missing_companion_for_one_period_fails_the_whole_pairing() -> None:
    # Three of four quarters have a companion. Summing those three would cover
    # nine months against a twelve-month numerator, so the pairing must fail.
    repo = _FakeFactsRepo(
        {
            ANCHOR: _quarters(ANCHOR, (-1.0, -2.0, -3.0, -4.0)),
            COMPANION: _quarters(COMPANION, (10.0, 20.0, 30.0))[:3],
        }
    )

    _window, result = _sum_companion(repo)

    assert result is None
