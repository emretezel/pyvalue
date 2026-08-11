"""Regression: a debt-less fiscal period no longer voids invested capital.

Removing the fabricated ``LongTermDebt`` on 2026-08-10 (commit ``d5d8b34``)
left ``invested_capital`` with no debt leg for any period where the provider
nulls all three of its debt fields. Because ``_build_points`` drops such a
period outright, a single gap anywhere in the decade voided the strict 10-year
ROIC chain -- ``roic_years_above_12pct`` and ``roic_10y_min`` went NA on ~6,000
listings, taking PayPal (no borrowings at all in FY2015-FY2016), Kweichow
Moutai, Evolution AB and Cirrus Logic out of the QARP universe.

The gap is filled with ``LiabilitiesNoncurrent`` as an explicit **bound**, not
as a measurement. Reading the missing row as zero was rejected on evidence:
EODHD reports explicit zeroes when a company genuinely has no debt (141k
``LongTermDebt`` and 88k ``ShortTermDebt`` rows are exactly 0), so an absent
row means unreported -- and on a 1,457-period sample the nearest reported debt
exceeded 20% of that period's total liabilities in 48.8% of cases.

The bound is safe wherever the error direction is one-way. Inside invested
capital it can only inflate the denominator, so it can only depress a return.
``enterprise_value`` and ``net_debt_to_ebitda`` were added on 2026-08-11 for the
same reason: an overstated debt figure raises EV and raises leverage, so it can
block a gate but never clear one. ``short_term_debt_share`` is the counter-case
and stays NA -- there total debt is the *denominator of a share*, so a bound
understates it, which is the flattering direction.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta

from pyvalue.currency import MetricUnitKind
from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.marketdata.base import PriceData
from pyvalue.metrics.enterprise_value_ratios import EVToEBITMetric
from pyvalue.metrics.invested_capital import ICFYMetric
from pyvalue.metrics.net_debt_to_ebitda import NetDebtToEBITDAMetric
from pyvalue.metrics.short_term_debt_share import ShortTermDebtShareMetric
from pyvalue.persistence.storage import MarketDataRepository

LISTING_ID = 1
_TODAY = date.today()
FRESH_FY = (_TODAY - timedelta(days=120)).isoformat()


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


def _balance_sheet_without_debt() -> dict[str, list[FactRecord]]:
    """A complete FY balance sheet whose three debt fields are all nulled."""

    return {
        "StockholdersEquity": _fy("StockholdersEquity", 1_000.0),
        "CashAndCashEquivalents": _fy("CashAndCashEquivalents", 200.0),
    }


def test_noncurrent_liabilities_stand_in_for_a_missing_debt_leg() -> None:
    # Fails on the pre-fix code: the period carries no ShortTermDebt,
    # LongTermDebt or TotalDebtFromBalanceSheet, so it was skipped and ic_fy
    # came back None. Invested capital = bound 150 + equity 1000 - cash 200.
    repo = _FakeFactsRepo(
        {
            **_balance_sheet_without_debt(),
            "LiabilitiesNoncurrent": _fy("LiabilitiesNoncurrent", 150.0),
        }
    )
    result = ICFYMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 950.0) < 1e-9


def test_measured_debt_wins_over_the_bound() -> None:
    # The bound is last-resort only. With a measured rollup of 40 present, the
    # much larger non-current-liabilities figure must not be substituted:
    # invested capital = 40 + 1000 - 200, not 150 + 1000 - 200.
    repo = _FakeFactsRepo(
        {
            **_balance_sheet_without_debt(),
            "TotalDebtFromBalanceSheet": _fy("TotalDebtFromBalanceSheet", 40.0),
            "LiabilitiesNoncurrent": _fy("LiabilitiesNoncurrent", 150.0),
        }
    )
    result = ICFYMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 840.0) < 1e-9


def test_a_single_reported_component_still_wins_over_the_bound() -> None:
    # One component side reported alone is a real balance-sheet shape, so it is
    # a measurement and outranks the bound even when it is the smaller number.
    repo = _FakeFactsRepo(
        {
            **_balance_sheet_without_debt(),
            "ShortTermDebt": _fy("ShortTermDebt", 10.0),
            "LiabilitiesNoncurrent": _fy("LiabilitiesNoncurrent", 150.0),
        }
    )
    result = ICFYMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - 810.0) < 1e-9


def test_no_debt_and_no_bound_stays_na() -> None:
    # Without either, the period is still dropped and the metric is still NA.
    repo = _FakeFactsRepo(dict(_balance_sheet_without_debt()))
    assert ICFYMetric().compute(LISTING_ID, repo) is None


def test_net_debt_to_ebitda_also_takes_the_bound() -> None:
    # REVERSED 2026-08-11 by operator decision. This case originally asserted
    # containment -- that only invested capital may substitute the bound,
    # because net_debt_to_ebitda reports a debt figure a reader acts on. The
    # containment cost 5,255 listings all three price arms and removed six
    # former QARP passers that had not become expensive, only unmeasurable.
    #
    # The error direction settles it: overstating debt can only *raise* this
    # ratio and *raise* EV, so the bound can block a leverage or price gate but
    # never clear one -- the same one-way-safe property that justifies it inside
    # invested capital. Net debt = bound 150 - cash 200 = -50 (net cash);
    # EBITDA = EBIT 80 + D&A 20 = 100; ratio = -0.5.
    repo = _FakeFactsRepo(
        {
            **_balance_sheet_without_debt(),
            "LiabilitiesNoncurrent": _fy("LiabilitiesNoncurrent", 150.0),
            "OperatingIncomeLoss": _fy("OperatingIncomeLoss", 80.0),
            "DepreciationDepletionAndAmortization": _fy(
                "DepreciationDepletionAndAmortization", 20.0
            ),
        }
    )
    result = NetDebtToEBITDAMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - (-0.5)) < 1e-9


def test_enterprise_value_also_takes_the_bound() -> None:
    # The other half of the 2026-08-11 decision, and the one that matters most:
    # without it the price gate is NA and the listing leaves the screen. EV =
    # market cap (100 shares x 5) + bound 150 - cash 200 = 450; / EBIT 80.
    repo = _FakeFactsRepo(
        {
            **_balance_sheet_without_debt(),
            "LiabilitiesNoncurrent": _fy("LiabilitiesNoncurrent", 150.0),
            "OperatingIncomeLoss": _fy("OperatingIncomeLoss", 80.0),
            "CommonStockSharesOutstanding": _fy(
                "CommonStockSharesOutstanding", 100.0, unit_kind="count"
            ),
        }
    )
    result = EVToEBITMetric().compute(LISTING_ID, repo, _FakeMarketRepo(price=5.0))
    assert result is not None
    assert abs(result.value - 450.0 / 80.0) < 1e-9


def test_short_term_debt_share_still_refuses_the_bound() -> None:
    # The containment that survives, and the reason it is not arbitrary: total
    # debt is this metric's *denominator*, so a bound understates the share --
    # the flattering direction, not the safe one. Same balance sheet, still NA.
    repo = _FakeFactsRepo(
        {
            **_balance_sheet_without_debt(),
            "LiabilitiesNoncurrent": _fy("LiabilitiesNoncurrent", 150.0),
            "ShortTermDebt": [],
        }
    )
    assert ShortTermDebtShareMetric().compute(LISTING_ID, repo) is None
