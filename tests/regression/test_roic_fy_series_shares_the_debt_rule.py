"""Regression: the ROIC FY series reads debt through the shared rule.

The 2026-08-10 debt audit collapsed five private copies of the debt decision
into ``balance_sheet.broader_debt_reading`` -- and missed a sixth, in
``roic_fy_series._resolve_invested_capital_debt``. That chain kept the retired
components-preferred precedence, so the ROIC persistence family
(``roic_years_above_12pct``, ``roic_10y_min``, the medians, ``iroic_5y``) --
four of QARP's fourteen gates -- went on valuing invested capital on a
lease-blind figure while every other metric had moved to the broadest measured
reading. Nothing caught it because the audit's cross-metric test compared
``interest_coverage`` against ``enterprise_value``, neither of which routes
through this module.

The same chain also needs the ``LiabilitiesNoncurrent`` last resort that
``invested_capital`` gained, and for the same one-way-safe reason: a year whose
provider nulls all three debt fields is otherwise dropped, and one dropped year
voids the whole strict decade. Kweichow Moutai (FY2025 debt fields all null,
non-current liabilities and cash both reported) is the worked case.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date

import pytest

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.roic_fy_series import ROIC10YMinMetric

LISTING_ID = 1
LATEST_YEAR = date.today().year - 1
OLDEST_YEAR = LATEST_YEAR - 9

# EBIT 100 with no tax facts -> the documented 21% default -> NOPAT 79.
NOPAT = 79.0
EQUITY = 500.0
CASH = 100.0


class _FakeFactsRepo(RegionFactsRepository):
    """Minimal in-memory fact source mirroring the production read path."""

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


def _fy(concept: str, year: int, value: float) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period="FY",
        end_date=f"{year}-12-31",
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _records(
    debt_by_concept: dict[str, float],
    *,
    hole_year: int | None = None,
    hole_extras: dict[str, float] | None = None,
) -> dict[str, list[FactRecord]]:
    """Ten uniform FY years, optionally with one year's debt fields removed.

    Every year is identical, so invested capital is flat and the 10-year
    minimum ROIC is simply ``NOPAT / IC`` — no averaging or history-boundary
    convention can move it, which keeps the assertions about the debt leg alone.
    """

    records: dict[str, list[FactRecord]] = {"OperatingIncomeLoss": []}
    for year in range(OLDEST_YEAR, LATEST_YEAR + 1):
        records["OperatingIncomeLoss"].append(_fy("OperatingIncomeLoss", year, 100.0))
        records.setdefault("StockholdersEquity", []).append(
            _fy("StockholdersEquity", year, EQUITY)
        )
        records.setdefault("CashAndCashEquivalents", []).append(
            _fy("CashAndCashEquivalents", year, CASH)
        )
        if year == hole_year:
            for concept, value in (hole_extras or {}).items():
                records.setdefault(concept, []).append(_fy(concept, year, value))
            continue
        for concept, value in debt_by_concept.items():
            records.setdefault(concept, []).append(_fy(concept, year, value))
    return records


def test_broader_rollup_wins_in_the_roic_chain() -> None:
    # Fails on the pre-fix code with 79 / 500: the components (30 + 70) were
    # preferred and the 999 rollup — the representation that carries finance
    # leases — was never consulted. IC = 999 + 500 - 100 = 1399.
    repo = _FakeFactsRepo(
        _records(
            {
                "ShortTermDebt": 30.0,
                "LongTermDebt": 70.0,
                "TotalDebtFromBalanceSheet": 999.0,
            }
        )
    )
    result = ROIC10YMinMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert result.value == pytest.approx(NOPAT / 1399.0)


def test_broader_components_win_in_the_roic_chain() -> None:
    # Broadest-measured, not rollup-preferred: components 100 beat a 60 rollup.
    # IC = 100 + 500 - 100 = 500.
    repo = _FakeFactsRepo(
        _records(
            {
                "ShortTermDebt": 30.0,
                "LongTermDebt": 70.0,
                "TotalDebtFromBalanceSheet": 60.0,
            }
        )
    )
    result = ROIC10YMinMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert result.value == pytest.approx(NOPAT / 500.0)


def test_a_debt_less_year_falls_back_to_noncurrent_liabilities() -> None:
    # One year has no debt row of any kind but does report non-current
    # liabilities — the Kweichow Moutai FY2025 shape. Pre-fix that year was
    # dropped and the strict decade collapsed to None; now it contributes an
    # IC of 100 + 500 - 100 = 500, identical to its neighbours.
    repo = _FakeFactsRepo(
        _records(
            {"LongTermDebt": 100.0},
            hole_year=LATEST_YEAR,
            hole_extras={"LiabilitiesNoncurrent": 100.0},
        )
    )
    result = ROIC10YMinMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert result.value == pytest.approx(NOPAT / 500.0)


def test_a_debt_less_year_without_the_bound_still_fails() -> None:
    # No debt row and no non-current-liabilities row: nothing to stand in with,
    # so the year is still dropped and the strict decade is still NA.
    repo = _FakeFactsRepo(_records({"LongTermDebt": 100.0}, hole_year=LATEST_YEAR - 4))
    assert ROIC10YMinMetric().compute(LISTING_ID, repo) is None
