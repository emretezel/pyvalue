"""Regression: EV must resolve debt/cash like net_debt_to_ebitda does.

The enterprise-value denominator used to hard-require all three of
``ShortTermDebt``, ``LongTermDebt`` and ``CashAndShortTermInvestments`` as
bare latest facts. ``net_debt_to_ebitda`` resolved the very same balance
sheet through fallback chains (cash rollup else equivalents plus short-term
investments; either debt side), so ~6.5k listings carried a net-debt position
but no EV — every EV ratio and owner-earnings yield went NA on them (see the
2026-07-05 screener audit, "missing EV debt/cash facts").

EV now resolves debt and cash through the shared
:mod:`pyvalue.metrics.balance_sheet` resolvers. Both value tests below fail
on the all-three-required code (metric is ``None``) and pass with the shared
chains.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.marketdata.base import PriceData
from pyvalue.metrics.enterprise_value_ratios import EVToSalesMetric
from pyvalue.persistence.storage import MarketDataRepository

LISTING_ID = 1

_TODAY = date.today()
FRESH = (_TODAY - timedelta(days=30)).isoformat()
QUARTER_DATES = tuple(
    (_TODAY - timedelta(days=days)).isoformat() for days in (30, 120, 210, 300)
)
QUARTER_PERIODS = ("Q4", "Q3", "Q2", "Q1")


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


class _FakeMarketRepo(MarketDataRepository):
    # Nominal MarketDataRepository subtype whose __init__ skips super() so no
    # SQLite DB is opened; serves one fixed in-memory snapshot.
    def __init__(self, price: float) -> None:
        self._price = price

    def latest_snapshot_by_id(self, listing_id: int) -> PriceData | None:
        return PriceData(
            symbol="EVFB.AU", price=self._price, as_of=FRESH, currency="USD"
        )

    def ticker_currency_by_id(self, listing_id: int) -> str | None:
        return "USD"


def _monetary(concept: str, value: float) -> list[FactRecord]:
    return [
        FactRecord(
            symbol="EVFB.AU",
            concept=concept,
            fiscal_period="Q4",
            end_date=FRESH,
            unit_kind="monetary",
            value=value,
            filed=None,
            currency="USD",
        )
    ]


def _shares(value: float) -> list[FactRecord]:
    return [
        FactRecord(
            symbol="EVFB.AU",
            concept="CommonStockSharesOutstanding",
            fiscal_period="Q4",
            end_date=FRESH,
            unit_kind="count",
            value=value,
            filed=None,
            currency=None,
        )
    ]


def _revenue_quarters(values: tuple[float, float, float, float]) -> list[FactRecord]:
    return [
        FactRecord(
            symbol="EVFB.AU",
            concept="Revenues",
            fiscal_period=period,
            end_date=end_date,
            unit_kind="monetary",
            value=value,
            filed=None,
            currency="USD",
        )
        for period, end_date, value in zip(
            QUARTER_PERIODS, QUARTER_DATES, values, strict=True
        )
    ]


def test_ev_resolves_cash_from_equivalents_plus_short_term_investments() -> None:
    """No CashAndShortTermInvestments rollup — the parts must reconstruct it."""

    repo = _FakeFactsRepo(
        {
            "Revenues": _revenue_quarters((150.0, 150.0, 150.0, 150.0)),
            "ShortTermDebt": _monetary("ShortTermDebt", 50.0),
            "LongTermDebt": _monetary("LongTermDebt", 150.0),
            "CashAndCashEquivalents": _monetary("CashAndCashEquivalents", 80.0),
            "ShortTermInvestments": _monetary("ShortTermInvestments", 20.0),
            "CommonStockSharesOutstanding": _shares(100.0),
        }
    )

    result = EVToSalesMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=5.0))

    # EV = 100 x 5 + (50 + 150) - (80 + 20) = 600 over TTM revenue 600.
    # The all-three-required code returned None ("missing EV debt/cash facts").
    assert result is not None
    assert abs(result.value - 1.0) < 1e-12


def test_ev_accepts_a_single_debt_side() -> None:
    """Only long-term debt reported — a real shape, not a data gap."""

    repo = _FakeFactsRepo(
        {
            "Revenues": _revenue_quarters((150.0, 150.0, 150.0, 150.0)),
            "LongTermDebt": _monetary("LongTermDebt", 200.0),
            "CashAndShortTermInvestments": _monetary(
                "CashAndShortTermInvestments", 100.0
            ),
            "CommonStockSharesOutstanding": _shares(100.0),
        }
    )

    result = EVToSalesMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=5.0))

    # EV = 500 + 200 - 100 = 600 over TTM revenue 600.
    assert result is not None
    assert abs(result.value - 1.0) < 1e-12
