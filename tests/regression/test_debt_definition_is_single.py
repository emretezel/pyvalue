"""Regression: every metric must resolve the same debt for the same listing.

Until 2026-08-10 pyvalue carried two debt resolvers with different rules:

- ``resolve_total_debt`` -- components-preferred, used by ``enterprise_value``
  and ``net_debt_to_ebitda``
- ``resolve_debt_evidence`` -- ``max(components, rollup)``, used by
  ``interest_coverage``'s debt-free cap check

So one listing could carry two debt figures on the same day. Microsoft
(2026-06-30) was the case that surfaced it: ``interest_coverage`` saw the
$128.8B rollup while ``enterprise_value`` saw the $50.0B component sum. Any
screen mixing a price gate with a debt-service gate was therefore evaluating
two different balance sheets.

The audit found the evidence rule was the right one -- the wider scope is the
reading that captured the finance leases -- so the two merged into
``resolve_total_debt`` and the second function was deleted.

``test_ev_and_coverage_cap_agree_on_debt`` fails on the pre-fix code: the cap
bounded debt at the rollup's 50 while EV used the components' 10.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from pyvalue.currency import MetricUnitKind
from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.marketdata.base import PriceData
from pyvalue.metrics.enterprise_value_ratios import EVToEBITMetric
from pyvalue.metrics.interest_coverage import (
    INTEREST_COVERAGE_CAP,
    InterestCoverageMetric,
)
from pyvalue.persistence.storage import MarketDataRepository

LISTING_ID = 1
_TODAY = date.today()
FRESH_FY = (_TODAY - timedelta(days=180)).isoformat()
# Four fresh quarter-ends so the coverage metric's TTM EBIT window resolves.
FRESH_QUARTERS = tuple(
    (_TODAY - timedelta(days=days)).isoformat() for days in (30, 120, 210, 300)
)
QUARTER_PERIODS = ("Q4", "Q3", "Q2", "Q1")


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


class _FakeMarketRepo(MarketDataRepository):
    # Nominal MarketDataRepository subtype whose __init__ skips super() so no
    # SQLite DB is opened; serves one fixed fresh in-memory snapshot.
    def __init__(self, price: float) -> None:
        self._price = price

    def latest_snapshot_by_id(self, listing_id: int) -> PriceData | None:
        return PriceData(
            symbol="TEST.US",
            price=self._price,
            as_of=(_TODAY - timedelta(days=1)).isoformat(),
            currency="USD",
        )

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


def _quarterly_ebit(per_quarter: float) -> list[FactRecord]:
    """Four equal fresh quarters, so TTM EBIT = 4 x ``per_quarter``."""

    return [
        FactRecord(
            symbol="TEST.US",
            concept="OperatingIncomeLoss",
            fiscal_period=period,
            end_date=end_date,
            unit_kind="monetary",
            value=per_quarter,
            filed=None,
            currency="USD",
        )
        for period, end_date in zip(QUARTER_PERIODS, FRESH_QUARTERS)
    ]


def _repo(*, components: float, rollup: float) -> _FakeFactsRepo:
    # TTM EBIT = 100. Market cap = 100 shares x $1.00. No cash, so EV is
    # market cap plus whatever debt figure the resolver picks -- which makes
    # EV/EBIT read the debt directly.
    return _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _quarterly_ebit(25.0),
            "ShortTermDebt": _fy("ShortTermDebt", components),
            "TotalDebtFromBalanceSheet": _fy("TotalDebtFromBalanceSheet", rollup),
            "CashAndShortTermInvestments": _fy("CashAndShortTermInvestments", 0.0),
            "CommonStockSharesOutstanding": _fy(
                "CommonStockSharesOutstanding", 100.0, unit_kind="count"
            ),
        }
    )


def test_ev_and_coverage_cap_agree_on_debt() -> None:
    """Rollup 50 vs components 10: both metrics must read 50."""

    repo = _repo(components=10.0, rollup=50.0)

    # The cap fires: 50 is at or under 1.0x TTM EBIT (100), so debt is
    # immaterial and the debt-free cap is safe.
    coverage = InterestCoverageMetric().compute(LISTING_ID, repo)
    assert coverage is not None
    assert coverage.value == INTEREST_COVERAGE_CAP

    # EV must be built on the same 50, not the components' 10.
    # EV = market cap 100 + debt 50 - cash 0 = 150; / TTM EBIT 100 = 1.5.
    # Pre-fix this was (100 + 10) / 100 = 1.1.
    ev = EVToEBITMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=1.0))
    assert ev is not None
    assert abs(ev.value - 1.5) < 1e-12


def test_ev_and_coverage_cap_agree_when_debt_is_material() -> None:
    """Rollup 900 vetoes the cap, and the same 900 must land in EV."""

    repo = _repo(components=10.0, rollup=900.0)

    # 900 is 9x TTM EBIT: material debt with no measurable interest line, so
    # the cap is unsafe and the metric concedes NA.
    assert InterestCoverageMetric().compute(LISTING_ID, repo) is None

    # EV = market cap 100 + debt 900 - cash 0 = 1000; / TTM EBIT 100 = 10.0.
    ev = EVToEBITMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=1.0))
    assert ev is not None
    assert abs(ev.value - 10.0) < 1e-12
