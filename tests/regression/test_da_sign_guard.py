"""Regression: negative D&A is treated as unavailable, not added back raw.

EODHD emits negative Depreciation & Amortization -- sign errors on operating
companies (e.g. Argan/AGX FY2026 raw ``depreciationAndAmortization = -4,743,000``
alongside a positive ``+1,912,000`` cash-flow depreciation) and scale blow-ups on
financials (e.g. SuRo/SSSS raw cash-flow depreciation ``-87,445,149,000,000``).
The EBITDA/owner-earnings add-backs used to add the raw signed value, which
understates them; ``abs()`` would instead explode them.

The sign guard drops a negative D&A fact so the primary->cash-flow fallback
engages, and a name with no usable D&A degrades to NA rather than a corrupted
number. The first test fails on the pre-guard code (which computed EBIT + the
negative primary D&A) and passes with the guard.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.marketdata.base import PriceData
from pyvalue.metrics.enterprise_value_ratios import EVToEBITDAMetric
from pyvalue.metrics.net_debt_to_ebitda import NetDebtToEBITDAMetric
from pyvalue.persistence.storage import MarketDataRepository

LISTING_ID = 1
_TODAY = date.today()
FRESH_FY = (_TODAY - timedelta(days=180)).isoformat()
# SuRo/SSSS raw cash-flow depreciation -- a real EODHD row, absurd by orders of
# magnitude. Used to prove the guard drops it rather than abs()-ing it into a
# gigantic positive add-back.
SURO_SCALE_ERROR = -87_445_149_000_000.0


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
    """One fixed fresh in-memory snapshot; skips super().__init__ (no SQLite)."""

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


def _fy(concept: str, value: float) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period="FY",
        end_date=FRESH_FY,
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _shares(value: float) -> list[FactRecord]:
    return [
        FactRecord(
            symbol="TEST.US",
            concept="CommonStockSharesOutstanding",
            fiscal_period="FY",
            end_date=FRESH_FY,
            unit_kind="count",
            value=value,
            filed=None,
            currency=None,
        )
    ]


def _repo(
    *, primary_da: Optional[float], fallback_da: Optional[float]
) -> _FakeFactsRepo:
    # FY EBIT 80; net debt (30 + 70) - 50 = 50; shares 100 x price 5 = 500 market
    # cap, so EV = 500 + 50 = 550. The D&A legs vary per test.
    records: dict[str, list[FactRecord]] = {
        "OperatingIncomeLoss": [_fy("OperatingIncomeLoss", 80.0)],
        "ShortTermDebt": [_fy("ShortTermDebt", 30.0)],
        "LongTermDebt": [_fy("LongTermDebt", 70.0)],
        "CashAndShortTermInvestments": [_fy("CashAndShortTermInvestments", 50.0)],
        "CommonStockSharesOutstanding": _shares(100.0),
    }
    if primary_da is not None:
        records["DepreciationDepletionAndAmortization"] = [
            _fy("DepreciationDepletionAndAmortization", primary_da)
        ]
    if fallback_da is not None:
        records["DepreciationFromCashFlow"] = [
            _fy("DepreciationFromCashFlow", fallback_da)
        ]
    return _FakeFactsRepo(records)


def test_net_debt_ebitda_uses_positive_cashflow_fallback_when_primary_da_negative() -> (
    None
):
    # Primary D&A -20 is dropped; the +20 cash-flow fallback fills its place, so
    # EBITDA = 80 + 20 = 100 and net debt / EBITDA = 50 / 100 = 0.5. Pre-guard the
    # metric added the raw -20 (EBITDA 60 -> ratio 0.833...).
    result = NetDebtToEBITDAMetric().compute(
        LISTING_ID, _repo(primary_da=-20.0, fallback_da=20.0)
    )
    assert result is not None
    assert result.value == 0.5


def test_ev_to_ebitda_uses_positive_cashflow_fallback_when_primary_da_negative() -> (
    None
):
    result = EVToEBITDAMetric().compute(
        LISTING_ID,
        _repo(primary_da=-20.0, fallback_da=20.0),
        _FakeMarketRepo(price=5.0),
    )
    assert result is not None
    # EV 550 / EBITDA 100.
    assert abs(result.value - 5.5) < 1e-12


def test_ebitda_is_na_when_all_da_negative_no_explosion() -> None:
    # Both D&A legs negative (incl. the -$87T scale error): both are dropped, so
    # D&A is missing for the window and EBITDA degrades to NA -- it must not
    # become EBIT + (-87e12) nor, under an abs() fix, EBIT + 87e12.
    repo = _repo(primary_da=-20.0, fallback_da=SURO_SCALE_ERROR)
    assert NetDebtToEBITDAMetric().compute(LISTING_ID, repo) is None
    assert (
        EVToEBITDAMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=5.0)) is None
    )
