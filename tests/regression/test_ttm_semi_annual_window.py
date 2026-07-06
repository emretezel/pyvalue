"""Regression: semi-annual reporters must not sum two years into a "TTM".

EODHD stores half-yearly reporters (Australia, the UK, France, ...) as Q2/Q4
rows in the quarterly table. Every TTM builder used to take the latest 4
quarterly rows unconditionally, so for those listings (~5.3k with fresh
anchors in the 2026-07 universe) every additive TTM covered *two years* of
flows: EBITDA doubled (halving ``net_debt_to_ebitda``), free cash flow and
revenue doubled, and ratio metrics whose numerator and denominator windows
diverged were silently distorted.

With the cadence-aware window resolver (``pyvalue.metrics.ttm``) a
semi-annual history resolves to the latest TWO half-year rows -- a true
twelve months -- and a history whose spacing forms neither a quarterly nor a
semi-annual window (a hole right below the anchor, a cadence transition)
resolves to NA instead of a wrong number. The value assertions below fail on
the latest-4 code (which sums the older halves too) and pass with the
resolver.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.cash_conversion import CFOToNITTMMetric
from pyvalue.metrics.fcf_to_ebitda import FCFToEBITDAMetric
from pyvalue.metrics.net_debt_to_ebitda import NetDebtToEBITDAMetric

LISTING_ID = 1

_TODAY = date.today()
# Four fresh half-year ends, newest ~1 month old, ~182 days apart -- the
# EODHD shape for AU/LSE/PA semi-annual reporters (Q2/Q4 rows only).
SEMI_ANNUAL_DATES = tuple(
    (_TODAY - timedelta(days=days)).isoformat() for days in (30, 212, 395, 577)
)
SEMI_ANNUAL_PERIODS = ("Q4", "Q2", "Q4", "Q2")
# A hole directly below the anchor: half-year gap first, then quarters.
MIXED_DATES = tuple(
    (_TODAY - timedelta(days=days)).isoformat() for days in (30, 212, 303, 395)
)
MIXED_PERIODS = ("Q4", "Q2", "Q1", "Q4")


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


def _rows(
    concept: str,
    values: tuple[float, ...],
    *,
    dates: tuple[str, ...] = SEMI_ANNUAL_DATES,
    periods: tuple[str, ...] = SEMI_ANNUAL_PERIODS,
) -> list[FactRecord]:
    return [
        FactRecord(
            symbol="SEMI.AU",
            concept=concept,
            fiscal_period=period,
            end_date=end_date,
            unit_kind="monetary",
            value=value,
            filed=None,
            currency="USD",
        )
        for period, end_date, value in zip(periods, dates, values, strict=True)
    ]


def _instant(concept: str, value: float) -> list[FactRecord]:
    return _rows(
        concept, (value,), dates=SEMI_ANNUAL_DATES[:1], periods=SEMI_ANNUAL_PERIODS[:1]
    )


def test_net_debt_to_ebitda_sums_two_halves_not_four() -> None:
    """EBITDA = the latest two half-years; the latest-4 code doubled it."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _rows(
                "OperatingIncomeLoss", (50.0, 50.0, 40.0, 40.0)
            ),
            "DepreciationDepletionAndAmortization": _rows(
                "DepreciationDepletionAndAmortization", (10.0, 10.0, 10.0, 10.0)
            ),
            "ShortTermDebt": _instant("ShortTermDebt", 60.0),
            "LongTermDebt": _instant("LongTermDebt", 180.0),
            "CashAndShortTermInvestments": _instant(
                "CashAndShortTermInvestments", 40.0
            ),
        }
    )

    result = NetDebtToEBITDAMetric().compute(LISTING_ID, repo)

    # True TTM EBITDA = (50+10) + (50+10) = 120; net debt = 240 - 40 = 200.
    # The latest-4 code summed 220 of EBITDA and reported ~0.91x leverage.
    assert result is not None
    assert abs(result.value - 200.0 / 120.0) < 1e-12
    assert result.as_of == SEMI_ANNUAL_DATES[0]


def test_cfo_to_ni_ttm_uses_matching_one_year_windows() -> None:
    """Growth makes the two-year ratio diverge from the true TTM ratio."""

    repo = _FakeFactsRepo(
        {
            "NetCashProvidedByUsedInOperatingActivities": _rows(
                "NetCashProvidedByUsedInOperatingActivities", (60.0, 40.0, 10.0, 10.0)
            ),
            "NetIncomeLoss": _rows("NetIncomeLoss", (50.0, 50.0, 50.0, 50.0)),
        }
    )

    result = CFOToNITTMMetric().compute(LISTING_ID, repo)

    # True TTM: (60+40) / (50+50) = 1.0; the latest-4 code said 120/200 = 0.6
    # and would have failed QARP's >= 0.90 cash-conversion gate.
    assert result is not None
    assert abs(result.value - 1.0) < 1e-12


def test_fcf_to_ebitda_uses_one_year_of_flows() -> None:
    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _rows(
                "OperatingIncomeLoss", (50.0, 50.0, 40.0, 40.0)
            ),
            "DepreciationDepletionAndAmortization": _rows(
                "DepreciationDepletionAndAmortization", (10.0, 10.0, 10.0, 10.0)
            ),
            "NetCashProvidedByUsedInOperatingActivities": _rows(
                "NetCashProvidedByUsedInOperatingActivities", (80.0, 60.0, 30.0, 30.0)
            ),
            "CapitalExpenditures": _rows(
                "CapitalExpenditures", (20.0, 20.0, 10.0, 10.0)
            ),
        }
    )

    result = FCFToEBITDAMetric().compute(LISTING_ID, repo)

    # True TTM: FCF = (80+60) - (20+20) = 100 over EBITDA 120.
    assert result is not None
    assert abs(result.value - 100.0 / 120.0) < 1e-12


def test_mixed_cadence_history_is_na_not_a_wrong_sum() -> None:
    """A hole right below the anchor forms no TTM window at all."""

    repo = _FakeFactsRepo(
        {
            "OperatingIncomeLoss": _rows(
                "OperatingIncomeLoss",
                (50.0, 50.0, 40.0, 40.0),
                dates=MIXED_DATES,
                periods=MIXED_PERIODS,
            ),
            "DepreciationDepletionAndAmortization": _rows(
                "DepreciationDepletionAndAmortization",
                (10.0, 10.0, 10.0, 10.0),
                dates=MIXED_DATES,
                periods=MIXED_PERIODS,
            ),
            "ShortTermDebt": _instant("ShortTermDebt", 60.0),
            "LongTermDebt": _instant("LongTermDebt", 180.0),
            "CashAndShortTermInvestments": _instant(
                "CashAndShortTermInvestments", 40.0
            ),
        }
    )

    assert NetDebtToEBITDAMetric().compute(LISTING_ID, repo) is None
