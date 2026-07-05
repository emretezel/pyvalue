"""Regression: owner-earnings CAGR endpoint guard applies to window averages.

The 10y owner-earnings CAGR used to refuse the series when *any single* value
inside either 3-year endpoint window was non-positive. For a company with one
operating-loss year at the start of the chain and a strongly positive window
average (AMD.US-shaped: FY2016 EBIT -372M inside a FY2016-18 window averaging
~+76M), the metric reported "non-positive endpoint averages" even though the
compound's actual base — the window *average* — was positive. Loss-year
exclusion belongs to the explicit screen criteria (``ni_loss_years_10y`` /
``ni_loss_year_share``), not to this guard; only a window whose average is
non-positive has no real compound-growth solution.

This test fails on the per-value guard (the metric returns ``None``) and
passes once the guard checks the endpoint averages.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics import OwnerEarningsCAGR10YMetric

LISTING_ID = 1
LATEST_YEAR = date.today().year - 1

# Ten consecutive FY EBIT values, oldest -> newest: a single loss year at the
# chain start, then a steady recovery. Tax is seeded as zero with
# pretax == EBIT (effective rate 0; the loss year falls back to that same
# latest valid rate), and D&A == capex with flat NWC, so each owner-earnings
# point equals its EBIT — the endpoint guard is the only moving part.
EBIT_BY_AGE_ASC = [
    -250.0,
    290.0,
    320.0,
    350.0,
    380.0,
    410.0,
    440.0,
    470.0,
    500.0,
    530.0,
]
DA_VALUE = 50.0

# Thirteen flat FY NWC levels: every trailing delta is zero, so the per-year
# maintenance NWC is zero for all ten owner-earnings points.
NWC_BY_AGE_ASC = [100.0] * 13


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


def test_cagr_survives_single_loss_year_inside_positive_start_window() -> None:
    """One loss year must not void a window whose 3-year average is positive."""

    metric = OwnerEarningsCAGR10YMetric()

    result = metric.compute(LISTING_ID, _FakeFactsRepo(_build_records()))

    assert result is not None
    # Start window: (-250 + 290 + 320) / 3 = +120 — positive despite the loss
    # year, so the compound has a real base. End window: (470 + 500 + 530) / 3.
    start_avg = (-250.0 + 290.0 + 320.0) / 3.0
    end_avg = (470.0 + 500.0 + 530.0) / 3.0
    expected = (end_avg / start_avg) ** (1.0 / 7.0) - 1.0
    assert round(result.value, 8) == round(expected, 8)
    assert result.as_of == f"{LATEST_YEAR}-09-30"
