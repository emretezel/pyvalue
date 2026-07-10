"""Regression: a negative quarterly interest-expense fact must not distort TTM
interest coverage (2026-07 metric-verification audit, item A3/P5).

EODHD ships sign-flipped and scale-garbage ``interestExpense`` rows (ADBE.US
2026-02-28 raw ``-63,000,000`` against +62/+68/+67/+66M for the four prior
quarters; worst universe row ``-262.5B``). ``interest_coverage`` summed the
aligned TTM window *signed* and guarded only the total, so a lone flipped
quarter halved the denominator: ADBE read 8,961 / (-63+66+67+68=138) = 64.93x
instead of ~34x. 2,679 such quarterly rows across 1,644 listings; 416 live
metrics were computed off a contaminated window.

The fix treats a negative interest fact as *absent* at read time (the
``fact_guards`` seam, same policy as negative D&A): the window then anchors on
the last clean quarter and measures honestly. ``abs()`` would be wrong -- it
would bless the scale-garbage rows -- so the scale case pins drop-not-abs.
Both tests fail on the unguarded code (64.93x / ~0x) and pass with the guard.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.interest_coverage import InterestCoverageMetric

LISTING_ID = 1

_TODAY = date.today()
# Five fresh quarter-ends, 90 days apart (all inside the quarterly gap band).
# Dropping the newest interest row must leave a clean four-quarter chain whose
# anchor (day 120) is still well inside the 400-day freshness window.
FRESH_QUARTERS = tuple(
    (_TODAY - timedelta(days=days)).isoformat() for days in (30, 120, 210, 300, 390)
)
QUARTER_PERIODS = ("Q1", "Q4", "Q3", "Q2", "Q1")

# The ADBE.US shape, in USD millions (newest first). EBIT and interest values
# are the real audited figures; only the dates are synthesized fresh.
EBIT_VALUES = (2418.0, 2261.0, 2173.0, 2109.0, 2163.0)
INTEREST_SIGN_FLIP = (-63.0, 66.0, 67.0, 68.0, 62.0)
# Worst negative interest row in the 2026-07 universe (listing 57664): a scale
# blow-up, not a sign flip. abs() would turn it into a crushing denominator.
INTEREST_SCALE_GARBAGE = (-262_507_845.0, 66.0, 67.0, 68.0, 62.0)

# With the newest (negative) quarter treated as absent, the window is the four
# clean quarters below it: EBIT 2261+2173+2109+2163 over interest 66+67+68+62.
EXPECTED_COVERAGE = (2261.0 + 2173.0 + 2109.0 + 2163.0) / (66.0 + 67.0 + 68.0 + 62.0)


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


def _quarterly(concept: str, values: tuple[float, ...]) -> list[FactRecord]:
    return [
        FactRecord(
            symbol="TEST.US",
            concept=concept,
            fiscal_period=period,
            end_date=end_date,
            unit_kind="monetary",
            value=value,
            filed=None,
            currency="USD",
        )
        for period, end_date, value in zip(
            QUARTER_PERIODS, FRESH_QUARTERS, values, strict=True
        )
    ]


def _repo(interest_values: tuple[float, ...]) -> _FakeFactsRepo:
    return _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _quarterly("OperatingIncomeLoss", EBIT_VALUES),
            "InterestExpense": _quarterly("InterestExpense", interest_values),
        }
    )


def test_sign_flipped_quarter_is_dropped_not_summed() -> None:
    """The ADBE shape: the lone negative quarter must not halve the denominator.

    On the unguarded code this read 8,961/138 = 64.93x anchored on the flipped
    quarter; the guard drops the row so the window recedes one quarter and
    measures 8,706/263 = 33.10x.
    """

    result = InterestCoverageMetric().compute(LISTING_ID, _repo(INTEREST_SIGN_FLIP))

    assert result is not None
    assert round(result.value, 3) == round(EXPECTED_COVERAGE, 3)
    assert result.as_of == FRESH_QUARTERS[1]


def test_scale_garbage_quarter_is_dropped_not_abs() -> None:
    """A scale-blow-up row gets the same treat-as-absent outcome.

    ``abs()`` would keep the corrupted row and crush coverage toward zero;
    dropping it must yield the identical clean-window reading as the sign-flip
    case.
    """

    result = InterestCoverageMetric().compute(LISTING_ID, _repo(INTEREST_SCALE_GARBAGE))

    assert result is not None
    assert round(result.value, 3) == round(EXPECTED_COVERAGE, 3)
    assert result.as_of == FRESH_QUARTERS[1]
