"""Regression: debt-free issuers must score the coverage cap, not NA.

``interest_coverage`` (TTM EBIT / TTM interest) structurally NA'd exactly the
strongest balance sheets: an issuer that repays its debt stops reporting an
interest-expense line, so the aligned EBIT+interest series goes stale (the
PLTR.US shape — interest line ended Q4-2023 while EBIT stayed fresh) or never
exists at all. ~27k listings sat in the missing/stale/non-positive interest
buckets, and every ``>=`` screen gate excluded them (see
``docs/research/screener-na-investigation.md``, queue item 5 / 2026-07-05
findings).

With fresh, positive TTM EBIT and no measurable interest expense the metric
now emits the documented ``INTEREST_COVERAGE_CAP`` (100x) instead of ``None``.
Loss-making or stale-EBIT issuers are not rescued. The capped tests fail on
the ratio-only code (``None``) and pass with the cap.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.interest_coverage import (
    INTEREST_COVERAGE_CAP,
    InterestCoverageMetric,
)

LISTING_ID = 1

_TODAY = date.today()
# Four fresh quarter-ends (newest ~1 month old) and four stale ones roughly
# two years back — safely outside the 400-day freshness window.
FRESH_QUARTERS = tuple(
    (_TODAY - timedelta(days=days)).isoformat() for days in (30, 120, 210, 300)
)
STALE_QUARTERS = tuple(
    (_TODAY - timedelta(days=days)).isoformat() for days in (760, 850, 940, 1030)
)
QUARTER_PERIODS = ("Q4", "Q3", "Q2", "Q1")


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


def _quarterly(
    concept: str, dates: tuple[str, ...], values: tuple[float, ...]
) -> list[FactRecord]:
    return [
        FactRecord(
            symbol="PLTR.US",
            concept=concept,
            fiscal_period=period,
            end_date=end_date,
            unit_kind="monetary",
            value=value,
            filed=None,
            currency="USD",
        )
        for period, end_date, value in zip(QUARTER_PERIODS, dates, values, strict=True)
    ]


def test_stale_interest_line_with_fresh_ebit_scores_the_cap() -> None:
    """The PLTR shape: interest reporting ended years ago, EBIT is current."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _quarterly(
                "OperatingIncomeLoss", FRESH_QUARTERS, (40.0, 30.0, 20.0, 10.0)
            ),
            "InterestExpense": _quarterly(
                "InterestExpense", STALE_QUARTERS, (4.0, 3.0, 2.0, 1.0)
            ),
        }
    )

    result = InterestCoverageMetric().compute(LISTING_ID, repo)

    # On the ratio-only code this was None ("latest quarter too old").
    assert result is not None
    assert result.value == INTEREST_COVERAGE_CAP
    assert result.as_of == FRESH_QUARTERS[0]


def test_no_interest_facts_at_all_scores_the_cap() -> None:
    """An issuer that never reported an interest line is debt-free, not NA."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _quarterly(
                "OperatingIncomeLoss", FRESH_QUARTERS, (40.0, 30.0, 20.0, 10.0)
            ),
        }
    )

    result = InterestCoverageMetric().compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == INTEREST_COVERAGE_CAP


def test_loss_maker_without_interest_stays_na() -> None:
    """Negative TTM EBIT must not be rescued by the cap."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _quarterly(
                "OperatingIncomeLoss", FRESH_QUARTERS, (-40.0, -30.0, 20.0, 10.0)
            ),
        }
    )

    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None


def test_stale_ebit_without_interest_stays_na() -> None:
    """A dormant filer (stale EBIT, no interest) is a data gap, not debt-free."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _quarterly(
                "OperatingIncomeLoss", STALE_QUARTERS, (40.0, 30.0, 20.0, 10.0)
            ),
        }
    )

    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None


def test_levered_issuer_keeps_the_measured_ratio() -> None:
    """Fresh aligned interest still produces the plain ratio, never the cap."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _quarterly(
                "OperatingIncomeLoss", FRESH_QUARTERS, (40.0, 30.0, 20.0, 10.0)
            ),
            "InterestExpense": _quarterly(
                "InterestExpense", FRESH_QUARTERS, (4.0, 3.0, 2.0, 1.0)
            ),
        }
    )

    result = InterestCoverageMetric().compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == 10.0
