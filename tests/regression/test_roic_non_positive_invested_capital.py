"""Regression: non-positive average invested capital must fail the ROIC year.

``roic_fy_series`` guarded the average-IC denominator only against *exactly*
zero. IC = debt + equity - cash passes through zero for cash-rich firms, and
``NOPAT / avg_ic`` then explodes near zero and sign-flips below it: a
profitable year read as catastrophic (BESIY: 7y median 53%, 10y min -9,255%
in persisted state), and a loss year with negative IC read as a *good* year
that could spuriously count toward ``roic_years_above_12pct``. See
``docs/research/screener-na-investigation.md`` (2026-07-05 findings).

Non-positive averages now fail the year — the same ``<= 0`` convention the
sibling return-on-capital metrics (``roic_ttm``, ``croic``, ``roce``,
``roc_greenblatt``) already apply. Both tests fail on the ``== 0``-only code
(the metrics return sign-flipped values) and pass once the year is failed.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.roic_fy_series import (
    FAILURE_NON_POSITIVE_AVERAGE_INVESTED_CAPITAL,
    ROIC10YMinMetric,
    ROICFYSeriesCalculator,
    ROICYearsAbove12PctMetric,
)

LISTING_ID = 1
LATEST_YEAR = date.today().year - 1
# The year whose *average* IC goes negative: its own IC is -1200 against 500
# for every other year, so both its average and the following year's average
# are (500 - 1200) / 2 = -350 < 0.
NEGATIVE_IC_YEAR = LATEST_YEAR - 4


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
        symbol="BESIY.US",
        concept=concept,
        fiscal_period="FY",
        end_date=f"{year}-12-31",
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _build_records(*, ebit_in_negative_ic_year: float) -> dict[str, list[FactRecord]]:
    """Eleven IC years and ten EBIT years around one negative-IC year.

    Ten EBIT years anchor a full 10-year ROIC window; eleven invested-capital
    years (one extra at the tail) satisfy the prior-year IC each ROIC year
    needs. Every year carries IC = 100 (debt) + 500 (equity) - 100 (cash) =
    500, except ``NEGATIVE_IC_YEAR`` where cash of 1400 drives IC to -1200.
    No tax facts are supplied, so NOPAT uses the documented 21% default.
    """

    records: dict[str, list[FactRecord]] = {
        "OperatingIncomeLoss": [],
        "LongTermDebt": [],
        "StockholdersEquity": [],
        "CashAndCashEquivalents": [],
    }
    for year in range(LATEST_YEAR - 10, LATEST_YEAR + 1):
        cash = 1400.0 if year == NEGATIVE_IC_YEAR else 100.0
        records["LongTermDebt"].append(_fy_fact("LongTermDebt", year, 100.0))
        records["StockholdersEquity"].append(
            _fy_fact("StockholdersEquity", year, 500.0)
        )
        records["CashAndCashEquivalents"].append(
            _fy_fact("CashAndCashEquivalents", year, cash)
        )
    for year in range(LATEST_YEAR - 9, LATEST_YEAR + 1):
        ebit = ebit_in_negative_ic_year if year == NEGATIVE_IC_YEAR else 100.0
        records["OperatingIncomeLoss"].append(
            _fy_fact("OperatingIncomeLoss", year, ebit)
        )
    return records


def test_profitable_year_with_negative_avg_ic_fails_the_series() -> None:
    """A positive-NOPAT year over negative average IC must not tank the min."""

    repo = _FakeFactsRepo(_build_records(ebit_in_negative_ic_year=100.0))

    # On the ``== 0``-only code the year computed as NOPAT / -350 — a fake
    # negative minimum for a profitable decade — and the metric returned it.
    assert ROIC10YMinMetric().compute(LISTING_ID, repo) is None

    diagnostic = ROICFYSeriesCalculator().diagnose_series(LISTING_ID, repo)
    assert diagnostic.snapshot is None
    assert diagnostic.failure_reason == FAILURE_NON_POSITIVE_AVERAGE_INVESTED_CAPITAL
    by_year = {entry.year: entry for entry in diagnostic.year_diagnostics}
    assert not by_year[NEGATIVE_IC_YEAR].roic_available
    assert (
        by_year[NEGATIVE_IC_YEAR].roic_failure_reason
        == FAILURE_NON_POSITIVE_AVERAGE_INVESTED_CAPITAL
    )


def test_loss_year_with_negative_avg_ic_cannot_count_as_above_12pct() -> None:
    """NOPAT < 0 over IC < 0 sign-flips positive; the year must fail instead."""

    repo = _FakeFactsRepo(_build_records(ebit_in_negative_ic_year=-100.0))

    # On the ``== 0``-only code the loss year scored +79 / -350 = +22.6% and
    # counted toward the persistence gate; now the whole series is honest NA.
    assert ROICYearsAbove12PctMetric().compute(LISTING_ID, repo) is None
