"""Regression: a 10-FY history must support the full 10-year ROIC window.

Every ROIC year averages current and prior FY invested capital, so the
strict 10-year series silently demanded an *11th* FY balance-sheet year for
the oldest window year ("missing prior FY invested capital" — 4,573 listings
in persisted state; see ``docs/research/screener-na-investigation.md``,
2026-07-05 findings). That was one year above the 10-FY maturity bar the
other strict-10y screen gates impose.

The history boundary — the oldest observable IC year, which cannot have a
prior balance sheet by definition — now falls back to its own end-of-FY
invested capital (conservative: end >= average for a growing business, so
the boundary ROIC is biased low). Mid-chain IC holes still fail the year.
The first test fails on the prior-IC-demanding code (the metric returns
``None`` for an exactly-10-FY history) and passes with the boundary
convention.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date

import pytest

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.roic_fy_series import (
    FAILURE_MISSING_PRIOR_FY_INVESTED_CAPITAL,
    ROIC10YMinMetric,
    ROICFYSeriesCalculator,
)

LISTING_ID = 1
LATEST_YEAR = date.today().year - 1
OLDEST_EBIT_YEAR = LATEST_YEAR - 9


class _FakeFactsRepo(RegionFactsRepository):
    """Minimal in-memory fact source mirroring the production read path."""

    def __init__(self, records_by_concept: dict[str, list[FactRecord]]) -> None:
        # Wire the RegionFactsRepository wrapper to read raw facts back through
        # this same object, as the SQLite-backed repo does in production.
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


def _fy_fact(concept: str, year: int, value: float) -> FactRecord:
    return FactRecord(
        symbol="PLTR.US",
        concept=concept,
        fiscal_period="FY",
        end_date=f"{year}-12-31",
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _build_records(
    ic_years: range, *, skip_ic_year: int | None = None
) -> dict[str, list[FactRecord]]:
    """EBIT for the latest 10 FY years; IC components for ``ic_years``.

    Every year carries EBIT = 100 with no tax facts (documented 21% default,
    NOPAT = 79). IC = 100 (debt) + 500 (equity) - 100 (cash) = 500, except the
    oldest EBIT year, whose equity of 790 makes its end-of-FY IC 790 — chosen
    so a boundary ROIC of exactly 79 / 790 = 0.10 proves the year's *own*
    level (not an average with a neighbour) was used.
    """

    records: dict[str, list[FactRecord]] = {
        "OperatingIncomeLoss": [],
        "LongTermDebt": [],
        "StockholdersEquity": [],
        "CashAndCashEquivalents": [],
    }
    for year in ic_years:
        if year == skip_ic_year:
            continue
        equity = 790.0 if year == OLDEST_EBIT_YEAR else 500.0
        records["LongTermDebt"].append(_fy_fact("LongTermDebt", year, 100.0))
        records["StockholdersEquity"].append(
            _fy_fact("StockholdersEquity", year, equity)
        )
        records["CashAndCashEquivalents"].append(
            _fy_fact("CashAndCashEquivalents", year, 100.0)
        )
    for year in range(OLDEST_EBIT_YEAR, LATEST_YEAR + 1):
        records["OperatingIncomeLoss"].append(
            _fy_fact("OperatingIncomeLoss", year, 100.0)
        )
    return records


def test_exactly_ten_fy_years_compute_the_full_window() -> None:
    """Ten FY years must suffice; the boundary year uses its end-of-FY IC."""

    repo = _FakeFactsRepo(_build_records(range(OLDEST_EBIT_YEAR, LATEST_YEAR + 1)))

    result = ROIC10YMinMetric().compute(LISTING_ID, repo)

    # On the prior-IC-demanding code this was None (the oldest window year had
    # no prior balance sheet). The minimum is the boundary year itself:
    # 79 / 790 = 0.10 against 79 / 500-ish averages everywhere else.
    assert result is not None
    assert result.value == pytest.approx(0.10)


def test_mid_chain_ic_hole_still_fails() -> None:
    """The boundary convention must not paper over holes inside the history."""

    hole_year = LATEST_YEAR - 5
    repo = _FakeFactsRepo(
        _build_records(range(OLDEST_EBIT_YEAR, LATEST_YEAR + 1), skip_ic_year=hole_year)
    )

    assert ROIC10YMinMetric().compute(LISTING_ID, repo) is None

    diagnostic = ROICFYSeriesCalculator().diagnose_series(LISTING_ID, repo)
    by_year = {entry.year: entry for entry in diagnostic.year_diagnostics}
    # The year after the hole is missing its *prior* IC and sits inside the
    # observable history, so it must keep failing rather than fall back.
    assert (
        by_year[hole_year + 1].roic_failure_reason
        == FAILURE_MISSING_PRIOR_FY_INVESTED_CAPITAL
    )


def test_eleven_fy_years_keep_the_averaged_values() -> None:
    """Histories that already satisfied the old rule are bit-identical."""

    repo = _FakeFactsRepo(_build_records(range(OLDEST_EBIT_YEAR - 1, LATEST_YEAR + 1)))

    result = ROIC10YMinMetric().compute(LISTING_ID, repo)

    # With an 11th IC year present, every window year (the boundary EBIT year
    # included) averages current and prior FY IC exactly as before: the oldest
    # EBIT year averages (790 + 500) / 2 = 645 -> 79 / 645, which is also the
    # window minimum.
    assert result is not None
    assert result.value == pytest.approx(79.0 / 645.0)
