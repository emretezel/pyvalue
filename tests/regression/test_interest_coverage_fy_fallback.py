"""Regression: annual (FY) fallback ratio for levered issuers with no
quarterly interest line.

``interest_coverage`` builds its ratio from an aligned *quarterly* window, so
an issuer that reports operating income quarterly but interest only in its
annual filing (common for Korean conglomerates — Hanwha 000880.KO is the
archetype) never forms a quarterly window. The 2026-07-06 evidence-gated cap
correctly refuses to cap such a levered issuer, but that left it NA. The
2026-07-06 FY fallback measures it honestly instead: ``FY EBIT / FY
InterestExpense`` for the same fiscal year (480-day window), e.g. Hanwha's
₩4.15T / ₩1.57T = 2.64x.

The fallback runs *only* in the material/absent-debt-evidence branches of the
cap decision, never where an issuer earns the cap. That guard is load-bearing:
annual interest is noise-dominated when debt is small — Aperam SA (APMSF.US)
shows a contaminated 0.25x against debt of only 1x EBIT (an implied ~400%
rate) — so a proven-immaterial-debt issuer must keep its cap and never be
re-scored on that unreliable line. The measured/keeper tests fail on the
pre-fallback code (NA / re-scored) and pass with the guarded fallback.

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
# Four fresh quarter-ends: the quarterly TTM EBIT sums to 100.0 in every case,
# so debt/EBIT multiples read directly. Interest is never reported quarterly
# here — that is the whole point of the FY fallback.
FRESH_QUARTERS = tuple(
    (_TODAY - timedelta(days=days)).isoformat() for days in (30, 120, 210, 300)
)
QUARTER_PERIODS = ("Q4", "Q3", "Q2", "Q1")
EBIT_VALUES = (40.0, 30.0, 20.0, 10.0)

# A fresh annual date (inside the 480-day FY window) and a stale one beyond it.
FRESH_FY = (_TODAY - timedelta(days=120)).isoformat()
FRESH_FY_PRIOR = (_TODAY - timedelta(days=480)).isoformat()
STALE_FY = (_TODAY - timedelta(days=900)).isoformat()


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


def _quarterly_ebit() -> list[FactRecord]:
    return [
        FactRecord(
            symbol="TEST.US",
            concept="OperatingIncomeLoss",
            fiscal_period=period,
            end_date=end_date,
            unit_kind="monetary",
            value=value,
            filed=None,
            currency="USD",
        )
        for period, end_date, value in zip(
            QUARTER_PERIODS, FRESH_QUARTERS, EBIT_VALUES, strict=True
        )
    ]


def _fy(concept: str, value: float, *, end_date: str = FRESH_FY) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period="FY",
        end_date=end_date,
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _balance_row(concept: str, value: float) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period="Q4",
        end_date=FRESH_QUARTERS[0],
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def test_material_debt_with_fresh_fy_interest_is_measured() -> None:
    """The Hanwha shape: material debt, no quarterly interest, fresh FY pair."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [
                *_quarterly_ebit(),
                _fy("OperatingIncomeLoss", 100.0),
            ],
            "InterestExpense": [_fy("InterestExpense", 38.0)],
            "LongTermDebt": [_balance_row("LongTermDebt", 1500.0)],
        }
    )

    result = InterestCoverageMetric().compute(LISTING_ID, repo)

    # On the pre-fallback code this was None (material-debt NA).
    assert result is not None
    assert result.value != INTEREST_COVERAGE_CAP
    assert round(result.value, 3) == round(100.0 / 38.0, 3)
    assert result.as_of == FRESH_FY


def test_immaterial_debt_keeper_is_not_rescored_by_fy() -> None:
    """A proven debt-free/immaterial-debt issuer keeps the cap even when its
    FY interest line is contaminated (the Aperam shape: 0.25x)."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [
                *_quarterly_ebit(),
                _fy("OperatingIncomeLoss", 100.0),
            ],
            # Implied 0.25x coverage — an absurd ~400% rate on tiny debt.
            "InterestExpense": [_fy("InterestExpense", 400.0)],
            "ShortTermDebt": [_balance_row("ShortTermDebt", 3.0)],
            "LongTermDebt": [_balance_row("LongTermDebt", 17.0)],
        }
    )

    result = InterestCoverageMetric().compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == INTEREST_COVERAGE_CAP


def test_levered_loss_maker_stays_na() -> None:
    """Material debt + fresh FY interest but a non-positive FY EBIT is a
    levered loss year, not a coverage reading."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [
                *_quarterly_ebit(),
                _fy("OperatingIncomeLoss", -50.0),
            ],
            "InterestExpense": [_fy("InterestExpense", 38.0)],
            "LongTermDebt": [_balance_row("LongTermDebt", 1500.0)],
        }
    )

    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None


def test_stale_fy_interest_is_not_measured() -> None:
    """An FY interest line beyond the 480-day window is a data gap; material
    debt then stays NA (the fallback declines)."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [
                *_quarterly_ebit(),
                _fy("OperatingIncomeLoss", 100.0, end_date=STALE_FY),
            ],
            "InterestExpense": [_fy("InterestExpense", 38.0, end_date=STALE_FY)],
            "LongTermDebt": [_balance_row("LongTermDebt", 1500.0)],
        }
    )

    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None


def test_misaligned_fy_years_stay_na() -> None:
    """FY interest and FY EBIT from different fiscal years cannot form a
    coherent same-year ratio."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [
                *_quarterly_ebit(),
                _fy("OperatingIncomeLoss", 100.0, end_date=FRESH_FY_PRIOR),
            ],
            "InterestExpense": [_fy("InterestExpense", 38.0, end_date=FRESH_FY)],
            "LongTermDebt": [_balance_row("LongTermDebt", 1500.0)],
        }
    )

    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None


def test_no_balance_sheet_with_fresh_fy_pair_is_measured() -> None:
    """The third NA branch (no balance-sheet evidence) also measures when a
    fresh FY pair exists — a real ratio beats a data-gap NA."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [
                *_quarterly_ebit(),
                _fy("OperatingIncomeLoss", 100.0),
            ],
            "InterestExpense": [_fy("InterestExpense", 20.0)],
        }
    )

    result = InterestCoverageMetric().compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == 5.0
    assert result.as_of == FRESH_FY


def test_material_debt_without_any_fy_interest_stays_na() -> None:
    """Guard: with no FY interest at all, the material-debt branch still NAs
    (the fallback must not invent a ratio)."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [
                *_quarterly_ebit(),
                _fy("OperatingIncomeLoss", 100.0),
            ],
            "LongTermDebt": [_balance_row("LongTermDebt", 1500.0)],
        }
    )

    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None


def test_direct_fy_interest_wins_over_derived_fallback() -> None:
    """When both a direct and a derived FY interest row exist for the same
    year, the direct line is used (parity with the quarterly merge rule)."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [
                *_quarterly_ebit(),
                _fy("OperatingIncomeLoss", 100.0),
            ],
            "InterestExpense": [_fy("InterestExpense", 25.0)],
            "InterestExpenseFromNetInterestIncome": [
                _fy("InterestExpenseFromNetInterestIncome", 50.0)
            ],
            "LongTermDebt": [_balance_row("LongTermDebt", 1500.0)],
        }
    )

    result = InterestCoverageMetric().compute(LISTING_ID, repo)

    assert result is not None
    # 100 / 25 (direct), not 100 / 50 (derived).
    assert result.value == 4.0
