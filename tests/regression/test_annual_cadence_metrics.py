"""Regression: TTM-flow metrics measure annual-only filers via the annual cadence.

Annual-only issuers (companies that file only an annual/FY income statement,
no quarterly) used to NA on every trailing-twelve-month flow metric, because
``resolve_ttm_window`` builds from quarterly rows. Each metric here opts into
the resolver's annual cadence (a single fresh FY row), so an annual-only filer
becomes computable. The cases fail on the pre-opt-in code (NA) and pass with
the annual cadence.

One case per metric also pins the *cadence-matched balance-sheet freshness*:
an annual filer's balance sheet is filed on the same once-a-year cadence, so
when the income flow resolves annual the point-in-time legs widen to the
480-day FY window (a fresh annual EBITDA over a 430-day-old balance sheet must
not be a false NA).

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.net_debt_to_ebitda import NetDebtToEBITDAMetric

LISTING_ID = 1
_TODAY = date.today()

# A fresh FY end_date (well inside 400d) and one in the 400-480d band: stale
# under the standard window, fresh under the FY window.
FRESH_FY = (_TODAY - timedelta(days=180)).isoformat()
BAND_FY = (_TODAY - timedelta(days=430)).isoformat()


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


def _net_debt_repo(*, end_date: str) -> _FakeFactsRepo:
    # EBITDA = FY EBIT 80 + FY D&A 20 = 100; net debt = (30 + 70) - 50 = 50;
    # ratio = 0.5.
    return _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [
                _fy("OperatingIncomeLoss", 80.0, end_date=end_date)
            ],
            "DepreciationDepletionAndAmortization": [
                _fy("DepreciationDepletionAndAmortization", 20.0, end_date=end_date)
            ],
            "ShortTermDebt": [_fy("ShortTermDebt", 30.0, end_date=end_date)],
            "LongTermDebt": [_fy("LongTermDebt", 70.0, end_date=end_date)],
            "CashAndShortTermInvestments": [
                _fy("CashAndShortTermInvestments", 50.0, end_date=end_date)
            ],
        }
    )


def test_net_debt_to_ebitda_measures_an_annual_only_filer() -> None:
    result = NetDebtToEBITDAMetric().compute(
        LISTING_ID, _net_debt_repo(end_date=FRESH_FY)
    )
    assert result is not None
    assert result.value == 0.5


def test_net_debt_to_ebitda_cadence_matched_freshness_in_post_fye_band() -> None:
    # FY data 430 days old: the income leg resolves on the 480-day annual
    # window, and the net-debt legs must follow it (not the 400-day default).
    result = NetDebtToEBITDAMetric().compute(
        LISTING_ID, _net_debt_repo(end_date=BAND_FY)
    )
    assert result is not None
    assert result.value == 0.5
