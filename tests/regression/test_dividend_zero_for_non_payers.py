"""Regression: dividend_yield_ttm is 0, not NA, for evidenced non-payers.

A cash-flow statement must report dividends paid whenever any were, so a
listing whose fresh statements carry no usable dividends line is a non-payer
with a trailing payout of exactly 0 -- not a data gap. The old behaviour
returned NA, which silently voided ``shareholder_yield_ttm`` (a 0.20-weight
QARP ranking metric) for every non-payer: ~31.5k listings universe-wide,
including ADBE/AMD/TSLA/PLTR on the anchor watchlist.

The zero is inferred only under two guards (both pinned here):

- no fresh *nonzero* dividends-paid row exists (a payer whose trailing window
  merely failed to form keeps an honest NA);
- the operating-cash-flow TTM window resolves (fresh statements back the
  inference; annual-only filers resolve through the FY cadence).

These cases fail on the pre-fix code (NA) and pass with the zero inference.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.marketdata.base import PriceData
from pyvalue.metrics.profitability_returns_growth import (
    DividendYieldTTMMetric,
    ShareholderYieldTTMMetric,
)
from pyvalue.persistence.storage import MarketDataRepository

LISTING_ID = 1
_TODAY = date.today()

# Four consecutive quarter-ends at the resolver's expected ~91-day cadence,
# newest one comfortably fresh.
_QUARTER_ENDS = tuple(
    (_TODAY - timedelta(days=30 + 91 * offset)).isoformat() for offset in range(4)
)
FRESH_FY = (_TODAY - timedelta(days=180)).isoformat()
# Long-stale dividend history (~8 years): the ADBE archetype, whose last
# dividends-paid row dates from 2016 while its statements stay current.
STALE = (_TODAY - timedelta(days=8 * 365)).isoformat()


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


def _monetary(
    concept: str, value: float, *, end_date: str, fiscal_period: str = "Q4"
) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period=fiscal_period,
        end_date=end_date,
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _quarters(concept: str, value: float) -> list[FactRecord]:
    periods = ("Q4", "Q3", "Q2", "Q1")
    return [
        _monetary(concept, value, end_date=end_date, fiscal_period=period)
        for end_date, period in zip(_QUARTER_ENDS, periods)
    ]


def _fresh_cfo() -> dict[str, list[FactRecord]]:
    return {
        "NetCashProvidedByUsedInOperatingActivities": _quarters(
            "NetCashProvidedByUsedInOperatingActivities", 25.0
        )
    }


def test_never_payer_with_fresh_cfo_yields_zero() -> None:
    # No dividends-paid or DPS rows at all, statements current: yield is 0.
    repo = _FakeFactsRepo(_fresh_cfo())
    result = DividendYieldTTMMetric().compute(
        LISTING_ID, repo, _FakeMarketRepo(price=5.0)
    )
    assert result is not None
    assert result.value == 0.0


def test_stopped_payer_with_stale_dividends_yields_zero() -> None:
    # The ADBE archetype: a nonzero dividends row far in the past, fresh CFO
    # -- trailing-twelve-month dividends are 0 regardless of ancient history.
    repo = _FakeFactsRepo(
        {
            "CommonStockDividendsPaid": [
                _monetary("CommonStockDividendsPaid", -12.0, end_date=STALE)
            ],
            **_fresh_cfo(),
        }
    )
    result = DividendYieldTTMMetric().compute(
        LISTING_ID, repo, _FakeMarketRepo(price=5.0)
    )
    assert result is not None
    assert result.value == 0.0


def test_fresh_nonzero_dividends_with_broken_window_stay_na() -> None:
    # One fresh nonzero dividends row whose window cannot form (irregular
    # payer): the issuer does pay, so a zero would be false -- NA stands.
    repo = _FakeFactsRepo(
        {
            "CommonStockDividendsPaid": [
                _monetary("CommonStockDividendsPaid", -12.0, end_date=_QUARTER_ENDS[0])
            ],
            **_fresh_cfo(),
        }
    )
    assert (
        DividendYieldTTMMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=5.0))
        is None
    )


def test_non_payer_without_fresh_cash_flow_statement_stays_na() -> None:
    # No dividend facts and no fresh CF statement: nothing evidences the zero,
    # so the honest answer remains NA.
    repo = _FakeFactsRepo({})
    assert (
        DividendYieldTTMMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=5.0))
        is None
    )


def test_annual_only_non_payer_yields_zero() -> None:
    # Annual-only filer: the CFO evidence resolves through the FY cadence,
    # matching the dividend leg's own annual opt-in.
    repo = _FakeFactsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": [
                _monetary(
                    "NetCashProvidedByUsedInOperatingActivities",
                    90.0,
                    end_date=FRESH_FY,
                    fiscal_period="FY",
                )
            ]
        }
    )
    result = DividendYieldTTMMetric().compute(
        LISTING_ID, repo, _FakeMarketRepo(price=5.0)
    )
    assert result is not None
    assert result.value == 0.0


def test_shareholder_yield_composes_zero_dividend_leg() -> None:
    # The point of the fix: a non-payer's shareholder yield degrades to its
    # buyback leg instead of going NA. Buybacks 4 x 10 = 40 over market cap
    # (100 shares x 5) = 500 -> 0.08; dividend leg contributes exactly 0.
    repo = _FakeFactsRepo(
        {
            "SalePurchaseOfStock": _quarters("SalePurchaseOfStock", -10.0),
            "CommonStockSharesOutstanding": [
                FactRecord(
                    symbol="TEST.US",
                    concept="CommonStockSharesOutstanding",
                    fiscal_period="FY",
                    end_date=FRESH_FY,
                    unit_kind="count",
                    value=100.0,
                    filed=None,
                    currency=None,
                )
            ],
            **_fresh_cfo(),
        }
    )
    result = ShareholderYieldTTMMetric().compute(
        LISTING_ID, repo, _FakeMarketRepo(price=5.0)
    )
    assert result is not None
    assert abs(result.value - 0.08) < 1e-12
