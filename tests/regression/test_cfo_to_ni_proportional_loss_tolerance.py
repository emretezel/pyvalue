"""Regression: short chains must get the loss tolerance DVG advertises.

``cfo_to_ni_10y_median`` required a flat six positive-NI points regardless of
chain length, capping DVG's effective loss tolerance at
``min(floor(0.4 x n), n - 6)``: a 6-year chain allowed *zero* loss years even
though the paired ``ni_loss_year_share <= 0.40`` gate advertises two (see
``docs/research/screener-na-investigation.md``, 2026-07-05 findings, B1).

The floor is now proportional — ``ceil(3/5 x chain length)`` positive points,
the exact complement of the 0.40 loss-share gate — so both DVG criteria
tolerate the same loss count at every window length. At the full 10-year
window the floor is still six points, so deep histories are bit-identical.
The first test fails on the flat-floor code (``None``) and passes with the
proportional floor.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.cash_conversion import CFOToNITenYearMedianMetric

LISTING_ID = 1
LATEST_YEAR = date.today().year - 1


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
        symbol="YNG.US",
        concept=concept,
        fiscal_period="FY",
        end_date=f"{year}-12-31",
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _build_records(ni_by_age_desc: list[float]) -> dict[str, list[FactRecord]]:
    """Joint CFO+NI chain, newest first; CFO is 1.5x NI's magnitude."""

    records: dict[str, list[FactRecord]] = {
        "NetCashProvidedByUsedInOperatingActivities": [],
        "NetIncomeLoss": [],
    }
    for age, net_income in enumerate(ni_by_age_desc):
        year = LATEST_YEAR - age
        records["NetCashProvidedByUsedInOperatingActivities"].append(
            _fy_fact(
                "NetCashProvidedByUsedInOperatingActivities",
                year,
                1.5 * abs(net_income),
            )
        )
        records["NetIncomeLoss"].append(_fy_fact("NetIncomeLoss", year, net_income))
    return records


def test_six_year_chain_tolerates_two_loss_years() -> None:
    """A 6-year history with 2 losses (share 0.33 <= 0.40) must compute."""

    # Four profitable years at CFO/NI = 1.5, two loss years mid-chain — the
    # exact shape ni_loss_year_share admits (2/6 = 0.33) but the flat
    # six-point floor rejected.
    ni_by_age_desc = [100.0, 100.0, -60.0, 100.0, -40.0, 100.0]
    repo = _FakeFactsRepo(_build_records(ni_by_age_desc))

    result = CFOToNITenYearMedianMetric().compute(LISTING_ID, repo)

    # On the flat-floor code this was None ("too few positive-NI FY years:
    # 4 of 6 in chain"); ceil(3/5 x 6) = 4 admits it.
    assert result is not None
    assert result.value == 1.5
    assert result.as_of == f"{LATEST_YEAR}-12-31"


def test_ten_year_chain_keeps_the_six_point_floor() -> None:
    """ceil(3/5 x 10) = 6: five positive years in ten still fail."""

    ni_by_age_desc = [
        100.0,
        -10.0,
        100.0,
        -10.0,
        100.0,
        -10.0,
        100.0,
        -10.0,
        100.0,
        -10.0,
    ]
    repo = _FakeFactsRepo(_build_records(ni_by_age_desc))

    assert CFOToNITenYearMedianMetric().compute(LISTING_ID, repo) is None
