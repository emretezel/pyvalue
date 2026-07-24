"""Regression: total-debt resolution falls back to the provider rollup.

``resolve_total_debt`` (shared by ``net_debt_to_ebitda`` and the
enterprise-value denominator) required a fresh ``ShortTermDebt`` or
``LongTermDebt`` component and returned ``None`` when a feed populated only
the ``TotalDebtFromBalanceSheet`` rollup -- the last asymmetry against
``invested_capital`` / ``debt_paydown_years``, whose chains already accept
the rollup (research-doc queue item 2; "missing EV debt/cash facts",
~1k listings). The resolver now uses the same components-preferred chain:
component sides win, the rollup only fills a component-less balance sheet.

The rescue tests fail on the pre-fix code (metric is ``None``); the
precedence test pins that a measured component sum is never displaced.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.marketdata.base import PriceData
from pyvalue.metrics.enterprise_value_ratios import EVToEBITMetric
from pyvalue.metrics.net_debt_to_ebitda import NetDebtToEBITDAMetric
from pyvalue.persistence.storage import MarketDataRepository

LISTING_ID = 1
_TODAY = date.today()
FRESH_FY = (_TODAY - timedelta(days=180)).isoformat()


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


def _fy(concept: str, value: float, *, unit_kind: str = "monetary") -> list[FactRecord]:
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


def _income_statement() -> dict[str, list[FactRecord]]:
    # Component EBITDA = FY EBIT 80 + FY D&A 20 = 100.
    return {
        "OperatingIncomeLoss": _fy("OperatingIncomeLoss", 80.0),
        "DepreciationDepletionAndAmortization": _fy(
            "DepreciationDepletionAndAmortization", 20.0
        ),
    }


def test_net_debt_to_ebitda_resolves_via_debt_rollup() -> None:
    # No component debt sides -- only the provider rollup 100. Net debt =
    # 100 - cash 50 = 50; ratio = 50 / EBITDA 100 = 0.5.
    repo = _FakeFactsRepo(
        {
            **_income_statement(),
            "TotalDebtFromBalanceSheet": _fy("TotalDebtFromBalanceSheet", 100.0),
            "CashAndShortTermInvestments": _fy("CashAndShortTermInvestments", 50.0),
        }
    )
    result = NetDebtToEBITDAMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 0.5) < 1e-12


def test_ev_to_ebit_resolves_via_debt_rollup() -> None:
    # EV = market cap (100 shares x 5) + rollup debt 100 - cash 50 = 550;
    # EV/EBIT = 550 / 80.
    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _fy("OperatingIncomeLoss", 80.0),
            "TotalDebtFromBalanceSheet": _fy("TotalDebtFromBalanceSheet", 100.0),
            "CashAndShortTermInvestments": _fy("CashAndShortTermInvestments", 50.0),
            "CommonStockSharesOutstanding": _fy(
                "CommonStockSharesOutstanding", 100.0, unit_kind="count"
            ),
        }
    )
    result = EVToEBITMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=5.0))
    assert result is not None
    assert abs(result.value - 550.0 / 80.0) < 1e-12


def test_component_sides_win_over_rollup() -> None:
    # A measured component sum (30 + 70 = 100) must never be displaced by a
    # disagreeing rollup (999, e.g. lease-contaminated): ratio stays 0.5.
    repo = _FakeFactsRepo(
        {
            **_income_statement(),
            "ShortTermDebt": _fy("ShortTermDebt", 30.0),
            "LongTermDebt": _fy("LongTermDebt", 70.0),
            "TotalDebtFromBalanceSheet": _fy("TotalDebtFromBalanceSheet", 999.0),
            "CashAndShortTermInvestments": _fy("CashAndShortTermInvestments", 50.0),
        }
    )
    result = NetDebtToEBITDAMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 0.5) < 1e-12


def test_no_debt_concept_at_all_stays_na() -> None:
    # Neither components nor rollup: debt is unknown, not zero.
    repo = _FakeFactsRepo(
        {
            **_income_statement(),
            "CashAndShortTermInvestments": _fy("CashAndShortTermInvestments", 50.0),
        }
    )
    assert NetDebtToEBITDAMetric().compute(LISTING_ID, repo) is None
