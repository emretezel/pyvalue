"""Regression: owner-earnings FY series subtracts per-year maintenance NWC.

The FY owner-earnings builder used to compute ``delta_nwc_maint`` once (the
*current* trailing-3-FY-delta average) and subtract that constant from every
historical FY point. For a company whose working capital exploded with recent
growth (NVDA-shaped), today's maintenance NWC dwarfs decade-old earnings, so
the oldest points went negative even though owner earnings were genuinely
positive in every year — and ``owner_earnings_cagr_10y`` refused the series
("non-positive endpoint averages"). Each FY point must instead subtract *that
year's own* maintenance NWC delta: max(avg of the 3 trailing FY NWC deltas, 0),
mirroring the standalone ``delta_nwc_maint`` convention.

This test fails on the constant-subtrahend code (the metric returns ``None``)
and passes once the per-year rule is in place.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics import OwnerEarningsCAGR10YMetric

LISTING_ID = 1
LATEST_YEAR = date.today().year - 1

# Ten consecutive FY EBIT values, oldest -> newest. Tax is seeded as zero with
# pretax == EBIT (effective rate 0), and D&A == capex, so each pre-NWC owner
# earnings point equals its EBIT — the NWC subtrahend is the only moving part.
EBIT_BY_AGE_ASC = [
    100.0,
    120.0,
    140.0,
    160.0,
    180.0,
    200.0,
    300.0,
    500.0,
    800.0,
    1200.0,
]
DA_VALUE = 50.0

# Thirteen consecutive FY NWC levels, oldest -> newest: flat while the company
# was small (all trailing deltas zero), then ballooning over the last three
# years. Current trailing-3 average = (300 + 600 + 1000) / 3 = 633.33 — larger
# than the six oldest EBIT values, which is exactly the artifact scenario.
NWC_BY_AGE_ASC = [100.0] * 10 + [400.0, 1000.0, 2000.0]


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
        symbol="NVDA.US",
        concept=concept,
        fiscal_period="FY",
        end_date=f"{year}-09-30",
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _build_records() -> dict[str, list[FactRecord]]:
    records: dict[str, list[FactRecord]] = {
        "OperatingIncomeLoss": [],
        "IncomeTaxExpense": [],
        "IncomeBeforeIncomeTaxes": [],
        "DepreciationDepletionAndAmortization": [],
        "CapitalExpenditures": [],
        "AssetsCurrent": [],
        "LiabilitiesCurrent": [],
        "CashAndShortTermInvestments": [],
        "ShortTermDebt": [],
    }

    for age, ebit in enumerate(reversed(EBIT_BY_AGE_ASC)):
        year = LATEST_YEAR - age
        records["OperatingIncomeLoss"].append(
            _fy_fact("OperatingIncomeLoss", year, ebit)
        )
        records["IncomeTaxExpense"].append(_fy_fact("IncomeTaxExpense", year, 0.0))
        records["IncomeBeforeIncomeTaxes"].append(
            _fy_fact("IncomeBeforeIncomeTaxes", year, ebit)
        )
        records["DepreciationDepletionAndAmortization"].append(
            _fy_fact("DepreciationDepletionAndAmortization", year, DA_VALUE)
        )
        records["CapitalExpenditures"].append(
            _fy_fact("CapitalExpenditures", year, DA_VALUE)
        )

    # NWC components use the same composition as the production formula:
    # NWC = (AssetsCurrent - cash) - (LiabilitiesCurrent - ShortTermDebt).
    for age, nwc in enumerate(reversed(NWC_BY_AGE_ASC)):
        year = LATEST_YEAR - age
        records["AssetsCurrent"].append(_fy_fact("AssetsCurrent", year, nwc + 350.0))
        records["LiabilitiesCurrent"].append(
            _fy_fact("LiabilitiesCurrent", year, 300.0)
        )
        records["CashAndShortTermInvestments"].append(
            _fy_fact("CashAndShortTermInvestments", year, 100.0)
        )
        records["ShortTermDebt"].append(_fy_fact("ShortTermDebt", year, 50.0))
    return records


def test_cagr_survives_recent_working_capital_explosion() -> None:
    """Positive per-year owner earnings must yield a CAGR despite today's NWC."""

    metric = OwnerEarningsCAGR10YMetric()

    result = metric.compute(LISTING_ID, _FakeFactsRepo(_build_records()))

    assert result is not None
    # Per-year maintenance NWC deltas: 0 for the seven oldest points (flat NWC),
    # then avg(300,0,0)=100, avg(600,300,0)=300, avg(1000,600,300)=1900/3.
    start_avg = (100.0 + 120.0 + 140.0) / 3.0
    end_avg = ((500.0 - 100.0) + (800.0 - 300.0) + (1200.0 - 1900.0 / 3.0)) / 3.0
    expected = (end_avg / start_avg) ** (1.0 / 7.0) - 1.0
    assert round(result.value, 8) == round(expected, 8)
    assert result.as_of == f"{LATEST_YEAR}-09-30"
