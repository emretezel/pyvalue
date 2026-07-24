"""Regression: vendor-EBITDA hole-filler for the component EBITDA build.

Component EBITDA (EBIT + per-period D&A) is the primary derivation for
``net_debt_to_ebitda``, ``ev_to_ebitda``, and ``fcf_to_ebitda``. When the
EBIT window resolves but some window period lacks a D&A companion, the whole
metric used to void ("missing D&A for a TTM window quarter" -- ~11k listings
per metric universe-wide, ~8k of which carry a fresh vendor ``EBITDA`` fact).
The shared helper now falls back to the vendor-supplied EBITDA line for
exactly that hole, guarded by plausibility: vendor EBITDA below the resolved
TTM EBIT implies negative D&A -- an established provider-artifact signature --
and is rejected.

Pinned here: the fallback fills the hole, the guard rejects contaminated
rows, the component path always wins when it resolves (vendor rows are then
ignored entirely), and a listing with neither D&A nor vendor rows stays NA.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from hypothesis import given
from hypothesis import strategies as st

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.marketdata.base import PriceData
from pyvalue.metrics.enterprise_value_ratios import EVToEBITDAMetric
from pyvalue.metrics.fcf_to_ebitda import FCFToEBITDAMetric
from pyvalue.metrics.net_debt_to_ebitda import NetDebtToEBITDAMetric
from pyvalue.persistence.storage import MarketDataRepository

LISTING_ID = 1
_TODAY = date.today()
FRESH_FY = (_TODAY - timedelta(days=180)).isoformat()
# Four consecutive quarter-ends at the resolver's expected ~91-day cadence.
_QUARTER_ENDS = tuple(
    (_TODAY - timedelta(days=30 + 91 * offset)).isoformat() for offset in range(4)
)
_QUARTER_PERIODS = ("Q4", "Q3", "Q2", "Q1")


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


def _quarters(concept: str, values: tuple[float, ...]) -> list[FactRecord]:
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
        for end_date, period, value in zip(_QUARTER_ENDS, _QUARTER_PERIODS, values)
    ]


def _net_debt_legs() -> dict[str, list[FactRecord]]:
    # Net debt = (30 + 70) - 50 = 50.
    return {
        "ShortTermDebt": [_fy("ShortTermDebt", 30.0)],
        "LongTermDebt": [_fy("LongTermDebt", 70.0)],
        "CashAndShortTermInvestments": [_fy("CashAndShortTermInvestments", 50.0)],
    }


def test_net_debt_to_ebitda_vendor_fallback_fills_da_hole() -> None:
    # Annual filer with EBIT but no D&A line at all: vendor FY EBITDA 100
    # (>= EBIT 80, plausible) fills the hole. Ratio = net debt 50 / 100.
    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [_fy("OperatingIncomeLoss", 80.0)],
            "EBITDA": [_fy("EBITDA", 100.0)],
            **_net_debt_legs(),
        }
    )
    result = NetDebtToEBITDAMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 0.5) < 1e-12


def test_quarterly_da_hole_rescued_by_quarterly_vendor_rows() -> None:
    # Quarterly filer whose D&A line misses one window quarter: the component
    # build fails but four plausible vendor rows (sum 100 >= EBIT 80) rescue.
    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _quarters(
                "OperatingIncomeLoss", (20.0, 20.0, 20.0, 20.0)
            ),
            # Only three of the four window quarters carry D&A.
            "DepreciationDepletionAndAmortization": _quarters(
                "DepreciationDepletionAndAmortization", (5.0, 5.0, 5.0)
            ),
            "EBITDA": _quarters("EBITDA", (25.0, 25.0, 25.0, 25.0)),
            **_net_debt_legs(),
        }
    )
    result = NetDebtToEBITDAMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 0.5) < 1e-12


def test_vendor_below_ebit_rejected_as_implied_negative_da() -> None:
    # Vendor EBITDA 60 under EBIT 80 implies negative D&A -- the provider
    # artifact the sign guard exists for -- so the fallback must refuse it.
    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [_fy("OperatingIncomeLoss", 80.0)],
            "EBITDA": [_fy("EBITDA", 60.0)],
            **_net_debt_legs(),
        }
    )
    assert NetDebtToEBITDAMetric().compute(LISTING_ID, repo) is None


def test_component_path_wins_over_contradictory_vendor_rows() -> None:
    # Full D&A coverage: the component build resolves (80 + 20 = 100) and the
    # vendor line -- however wild -- must never be consulted.
    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [_fy("OperatingIncomeLoss", 80.0)],
            "DepreciationDepletionAndAmortization": [
                _fy("DepreciationDepletionAndAmortization", 20.0)
            ],
            "EBITDA": [_fy("EBITDA", 999.0)],
            **_net_debt_legs(),
        }
    )
    result = NetDebtToEBITDAMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 0.5) < 1e-12


def test_da_hole_without_vendor_rows_stays_na() -> None:
    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [_fy("OperatingIncomeLoss", 80.0)],
            **_net_debt_legs(),
        }
    )
    assert NetDebtToEBITDAMetric().compute(LISTING_ID, repo) is None


def test_fcf_to_ebitda_vendor_fallback() -> None:
    # FCF = FY OCF 60 (capex absent, assumed zero); vendor EBITDA 100.
    repo = _FakeFactsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": [
                _fy("NetCashProvidedByUsedInOperatingActivities", 60.0)
            ],
            "OperatingIncomeLoss": [_fy("OperatingIncomeLoss", 80.0)],
            "EBITDA": [_fy("EBITDA", 100.0)],
        }
    )
    result = FCFToEBITDAMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 0.6) < 1e-12


def test_ev_to_ebitda_vendor_fallback() -> None:
    # EV = market cap (100 shares x 5) + net debt 50 = 550; vendor EBITDA 100.
    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [_fy("OperatingIncomeLoss", 80.0)],
            "EBITDA": [_fy("EBITDA", 100.0)],
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
            **_net_debt_legs(),
        }
    )
    result = EVToEBITDAMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=5.0))
    assert result is not None
    assert abs(result.value - 5.5) < 1e-12


@given(
    ebit=st.floats(min_value=1.0, max_value=1e6),
    da=st.floats(min_value=0.0, max_value=1e6),
    vendor=st.floats(min_value=-1e6, max_value=1e6),
)
def test_fallback_never_fires_when_component_resolves(
    ebit: float, da: float, vendor: float
) -> None:
    # Property: with full D&A coverage the metric equals net debt over the
    # component EBITDA for every vendor value -- the vendor line is dead code
    # on that path.
    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": [_fy("OperatingIncomeLoss", ebit)],
            "DepreciationDepletionAndAmortization": [
                _fy("DepreciationDepletionAndAmortization", da)
            ],
            "EBITDA": [_fy("EBITDA", vendor)],
            **_net_debt_legs(),
        }
    )
    result = NetDebtToEBITDAMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 50.0 / (ebit + da)) < 1e-9
