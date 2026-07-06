"""Regression: the coverage cap must require no-material-debt evidence.

The 2026-07-05 cap keyed on fresh positive TTM EBIT alone (the
"missing-interest-alone trigger"), so any issuer whose provider feed lacked a
fresh quarterly interest line scored 100x — including genuinely levered ones.
The 2026-07-06 audit of the ~2,107 cap-path listings found 1,160 (55%) with
fresh balance-sheet debt above 1x TTM EBIT (580 above 5x), all falsely
passing the ``>= 6`` / ``>= 1.5`` screen gates. Web-verified examples:
Hanwha 000880.KO (~15x EBIT of real debt), S-Oil 010950.KO (~32x), and
SIGA.US (a x10^6 EODHD unit error storing $2.65T of long-term debt against a
debt-free reality). Only 157 listings showed explicit zero-debt facts, and
PLTR.US — the shape the cap was built for — carries lease/derived "debt" of
0.29x EBIT, so a strict zero test would break it.

The cap now fires only when fresh balance-sheet evidence bounds debt at
``CAP_MAX_DEBT_TO_EBIT`` (1.0x) times TTM EBIT: the larger of
ShortTermDebt+LongTermDebt and TotalDebtFromBalanceSheet, falling back to
total Liabilities (an upper bound on debt) when no debt concept is fresh.
Material evidence — or no fresh balance-sheet facts at all — stays NA. The
NA-asserting tests fail on the EBIT-only trigger (which returned the cap)
and pass with the evidence gate.

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
# Four fresh quarter-ends (newest ~1 month old): TTM EBIT sums to 100.0 in
# every case below, so debt materiality thresholds read directly as
# multiples of EBIT.
FRESH_QUARTERS = tuple(
    (_TODAY - timedelta(days=days)).isoformat() for days in (30, 120, 210, 300)
)
QUARTER_PERIODS = ("Q4", "Q3", "Q2", "Q1")
EBIT_VALUES = (40.0, 30.0, 20.0, 10.0)


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
            symbol="TEST.US",
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


def _balance_row(concept: str, value: float) -> list[FactRecord]:
    """One fresh balance-sheet row — enough for latest_monetary_fact."""

    return [
        FactRecord(
            symbol="TEST.US",
            concept=concept,
            fiscal_period="Q4",
            end_date=FRESH_QUARTERS[0],
            unit_kind="monetary",
            value=value,
            filed=None,
            currency="USD",
        )
    ]


def _repo(**balance_values: float) -> _FakeFactsRepo:
    """Fresh 100.0 TTM EBIT, no interest line, plus the given balance rows."""

    records: dict[str, list[FactRecord]] = {
        "OperatingIncomeLoss": _quarterly(
            "OperatingIncomeLoss", FRESH_QUARTERS, EBIT_VALUES
        ),
    }
    for concept, value in balance_values.items():
        records[concept] = _balance_row(concept, value)
    return _FakeFactsRepo(records)


def test_material_debt_without_interest_is_na_not_capped() -> None:
    """The Hanwha shape: 15x EBIT of fresh debt, no fresh interest line."""

    repo = _repo(LongTermDebt=1500.0)

    # On the EBIT-only trigger this scored the 100x cap and passed the gates.
    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None


def test_junk_magnitude_debt_is_na_not_capped() -> None:
    """The SIGA shape: a provider unit error storing debt at ~10^10 x EBIT."""

    repo = _repo(LongTermDebt=2.65e12)

    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None


def test_lease_contaminated_small_debt_still_scores_the_cap() -> None:
    """The PLTR shape: lease/derived "debt" well under 1x EBIT keeps the cap."""

    repo = _repo(ShortTermDebt=3.0, LongTermDebt=17.0)

    result = InterestCoverageMetric().compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == INTEREST_COVERAGE_CAP
    assert result.as_of == FRESH_QUARTERS[0]


def test_large_total_debt_rollup_vetoes_despite_small_components() -> None:
    """Evidence is the *larger* of components and rollup — max(), not either."""

    repo = _repo(
        ShortTermDebt=3.0,
        LongTermDebt=17.0,
        TotalDebtFromBalanceSheet=2000.0,
    )

    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None


def test_no_debt_facts_with_small_liabilities_scores_the_cap() -> None:
    """Total liabilities upper-bound debt: a tiny balance sheet is evidence."""

    repo = _repo(Liabilities=60.0)

    result = InterestCoverageMetric().compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == INTEREST_COVERAGE_CAP


def test_no_debt_facts_with_large_liabilities_is_na() -> None:
    """Debt fields null and liabilities material: debt is unknown, not zero."""

    repo = _repo(Liabilities=1200.0)

    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None


def test_no_balance_sheet_facts_at_all_is_na() -> None:
    """No fresh balance sheet is a data gap — the deliberate inversion of the
    2026-07-05 rule, which capped this exact shape."""

    repo = _repo()

    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None


def test_debt_exactly_at_threshold_scores_the_cap() -> None:
    """The materiality bound is inclusive: debt == 1.0x TTM EBIT still caps."""

    repo = _repo(LongTermDebt=100.0)

    result = InterestCoverageMetric().compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == INTEREST_COVERAGE_CAP
