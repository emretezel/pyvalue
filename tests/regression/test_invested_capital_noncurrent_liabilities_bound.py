"""Regression: a debt-less fiscal period no longer voids invested capital.

Removing the fabricated ``LongTermDebt`` on 2026-08-10 (commit ``d5d8b34``)
left ``invested_capital`` with no debt leg for any period where the provider
nulls all three of its debt fields. Because ``_build_points`` drops such a
period outright, a single gap anywhere in the decade voided the strict 10-year
ROIC chain -- ``roic_years_above_12pct`` and ``roic_10y_min`` went NA on ~6,000
listings, taking PayPal (no borrowings at all in FY2015-FY2016), Kweichow
Moutai, Evolution AB and Cirrus Logic out of the QARP universe.

The gap is filled with ``LiabilitiesNoncurrent`` as an explicit **bound**, not
as a measurement. Reading the missing row as zero was rejected on evidence:
EODHD reports explicit zeroes when a company genuinely has no debt (141k
``LongTermDebt`` and 88k ``ShortTermDebt`` rows are exactly 0), so an absent
row means unreported -- and on a 1,457-period sample the nearest reported debt
exceeded 20% of that period's total liabilities in 48.8% of cases.

The bound is safe *here and only here*: it can only inflate invested capital,
which can only depress a return, so it can never lift a listing over a ROIC
gate. The last two tests pin that containment -- the metrics that report a
debt figure to a reader must stay NA.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from pyvalue.currency import MetricUnitKind
from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.invested_capital import ICFYMetric
from pyvalue.metrics.net_debt_to_ebitda import NetDebtToEBITDAMetric

LISTING_ID = 1
_TODAY = date.today()
FRESH_FY = (_TODAY - timedelta(days=120)).isoformat()


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


def _fy(
    concept: str, value: float, *, unit_kind: MetricUnitKind = "monetary"
) -> list[FactRecord]:
    return [
        FactRecord(
            symbol="TEST.US",
            concept=concept,
            fiscal_period="FY",
            end_date=FRESH_FY,
            unit_kind=unit_kind,
            value=value,
            filed=None,
            currency="USD" if unit_kind == "monetary" else None,
        )
    ]


def _balance_sheet_without_debt() -> dict[str, list[FactRecord]]:
    """A complete FY balance sheet whose three debt fields are all nulled."""

    return {
        "StockholdersEquity": _fy("StockholdersEquity", 1_000.0),
        "CashAndCashEquivalents": _fy("CashAndCashEquivalents", 200.0),
    }


def test_noncurrent_liabilities_stand_in_for_a_missing_debt_leg() -> None:
    # Fails on the pre-fix code: the period carries no ShortTermDebt,
    # LongTermDebt or TotalDebtFromBalanceSheet, so it was skipped and ic_fy
    # came back None. Invested capital = bound 150 + equity 1000 - cash 200.
    repo = _FakeFactsRepo(
        {
            **_balance_sheet_without_debt(),
            "LiabilitiesNoncurrent": _fy("LiabilitiesNoncurrent", 150.0),
        }
    )
    result = ICFYMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 950.0) < 1e-9


def test_measured_debt_wins_over_the_bound() -> None:
    # The bound is last-resort only. With a measured rollup of 40 present, the
    # much larger non-current-liabilities figure must not be substituted:
    # invested capital = 40 + 1000 - 200, not 150 + 1000 - 200.
    repo = _FakeFactsRepo(
        {
            **_balance_sheet_without_debt(),
            "TotalDebtFromBalanceSheet": _fy("TotalDebtFromBalanceSheet", 40.0),
            "LiabilitiesNoncurrent": _fy("LiabilitiesNoncurrent", 150.0),
        }
    )
    result = ICFYMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 840.0) < 1e-9


def test_a_single_reported_component_still_wins_over_the_bound() -> None:
    # One component side reported alone is a real balance-sheet shape, so it is
    # a measurement and outranks the bound even when it is the smaller number.
    repo = _FakeFactsRepo(
        {
            **_balance_sheet_without_debt(),
            "ShortTermDebt": _fy("ShortTermDebt", 10.0),
            "LiabilitiesNoncurrent": _fy("LiabilitiesNoncurrent", 150.0),
        }
    )
    result = ICFYMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 810.0) < 1e-9


def test_no_debt_and_no_bound_stays_na() -> None:
    # Without either, the period is still dropped and the metric is still NA.
    repo = _FakeFactsRepo(dict(_balance_sheet_without_debt()))
    assert ICFYMetric().compute(LISTING_ID, repo) is None


def test_net_debt_to_ebitda_does_not_borrow_the_bound() -> None:
    # Containment. net_debt_to_ebitda reports a debt figure a reader acts on,
    # and overstating debt there is not wrong-way-safe the way it is inside
    # invested capital, so the same balance sheet must stay NA.
    repo = _FakeFactsRepo(
        {
            **_balance_sheet_without_debt(),
            "LiabilitiesNoncurrent": _fy("LiabilitiesNoncurrent", 150.0),
            "OperatingIncomeLoss": _fy("OperatingIncomeLoss", 80.0),
            "DepreciationDepletionAndAmortization": _fy(
                "DepreciationDepletionAndAmortization", 20.0
            ),
        }
    )
    assert NetDebtToEBITDAMetric().compute(LISTING_ID, repo) is None
