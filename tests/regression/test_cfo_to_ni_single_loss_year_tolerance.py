"""Regression: one loss year must not void the 10y cash-conversion median.

``cfo_to_ni_10y_median`` used to demand a strict 10-consecutive-FY window in
which *every* year had positive net income: a single NI <= 0 year anywhere in
the window returned ``None`` ("non-positive FY net income"). One bad year — an
AMD-shaped FY2016 loss at the tail of an otherwise healthy decade — therefore
voided the whole metric until the loss aged out of the window. That guard
accounted for 87.6% of this metric's failures across the universe (see
``docs/research/screener-na-investigation.md``), conflating earnings
*stability* (``ni_loss_years_10y`` territory) with earnings *quality*.

Loss years are now skipped instead of fatal: the median is taken over the
positive-NI years of the latest consecutive joint CFO+NI chain (capped at 10
years, minimum 6 valid points). This test fails on the any-loss-is-fatal code
(the metric returns ``None``) and passes once loss years are skipped.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.cash_conversion import CFOToNITenYearMedianMetric

LISTING_ID = 1
LATEST_YEAR = date.today().year - 1

# Ten consecutive joint FY years, newest -> oldest, mirroring AMD's real
# 2016-2025 window: nine profitable years with NI = 100 and the ratio ladder
# below, plus one loss year (NI = -487) as the *oldest* chain year — exactly
# the shape the strict guard used to kill after clearing nine good years.
CFO_BY_AGE_DESC = [178.0, 185.0, 195.0, 270.0, 111.0, 43.0, 145.0, 10.0, 158.0, 90.0]
NI_BY_AGE_DESC = [100.0] * 9 + [-487.0]


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
        symbol="AMD.US",
        concept=concept,
        fiscal_period="FY",
        end_date=f"{year}-12-31",
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _build_records() -> dict[str, list[FactRecord]]:
    records: dict[str, list[FactRecord]] = {
        "NetCashProvidedByUsedInOperatingActivities": [],
        "NetIncomeLoss": [],
    }
    for age, (cfo, net_income) in enumerate(
        zip(CFO_BY_AGE_DESC, NI_BY_AGE_DESC, strict=True)
    ):
        year = LATEST_YEAR - age
        records["NetCashProvidedByUsedInOperatingActivities"].append(
            _fy_fact("NetCashProvidedByUsedInOperatingActivities", year, cfo)
        )
        records["NetIncomeLoss"].append(_fy_fact("NetIncomeLoss", year, net_income))
    return records


def test_single_loss_year_no_longer_voids_the_decade() -> None:
    """Nine valid ratios must produce their median despite the one loss year."""

    metric = CFOToNITenYearMedianMetric()

    result = metric.compute(LISTING_ID, _FakeFactsRepo(_build_records()))

    assert result is not None
    # Valid ratios (newest -> oldest): 1.78, 1.85, 1.95, 2.70, 1.11, 0.43,
    # 1.45, 0.10, 1.58 — the loss year contributes no point. Median = 1.58,
    # matching AMD's real relaxed value of ~1.581 over FY2017-FY2025.
    assert result.value == 1.58
    assert result.as_of == f"{LATEST_YEAR}-12-31"
