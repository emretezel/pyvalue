"""Regression: fcf_per_share_cagr_10y divides by outstanding, not diluted, shares.

The metric originally required ``WeightedAverageNumberOfDilutedSharesOutstanding``
-- a concept EODHD never supplies (zero rows in the entire database) -- so it
failed for 100% of the universe (61,426 listings) on every run. The per-share
basis is now period-end ``CommonStockSharesOutstanding``: split-adjusted across
history, so both CAGR endpoints share one consistent basis.

The first case fails on the pre-fix code (no diluted concept seeded -> NA);
the second pins the basis choice by seeding both concepts with different
counts and asserting the outstanding series wins.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.profitability_returns_growth import FCFPerShareCAGR10YMetric

LISTING_ID = 1
LATEST_YEAR = date.today().year - 1


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


def _fy_money(concept: str, value: float, *, year: int) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period="FY",
        end_date=f"{year}-09-30",
        unit_kind="monetary",
        value=value,
        filed=None,
        currency="USD",
    )


def _fy_count(concept: str, value: float, *, year: int) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period="FY",
        end_date=f"{year}-09-30",
        unit_kind="count",
        value=value,
        filed=None,
        currency=None,
    )


def _fcf_endpoints() -> dict[str, list[FactRecord]]:
    # FCF endpoints: latest 120 - 20 = 100; prior (10y back) 60 - 10 = 50.
    return {
        "NetCashProvidedByUsedInOperatingActivities": [
            _fy_money(
                "NetCashProvidedByUsedInOperatingActivities", 120.0, year=LATEST_YEAR
            ),
            _fy_money(
                "NetCashProvidedByUsedInOperatingActivities",
                60.0,
                year=LATEST_YEAR - 10,
            ),
        ],
        "CapitalExpenditures": [
            _fy_money("CapitalExpenditures", 20.0, year=LATEST_YEAR),
            _fy_money("CapitalExpenditures", 10.0, year=LATEST_YEAR - 10),
        ],
    }


def test_computes_from_outstanding_shares_alone() -> None:
    # No diluted-share concept anywhere -- exactly the real EODHD universe --
    # and the metric must still compute: FCF/share doubles over ten years.
    repo = _FakeFactsRepo(
        {
            **_fcf_endpoints(),
            "CommonStockSharesOutstanding": [
                _fy_count("CommonStockSharesOutstanding", 10.0, year=LATEST_YEAR),
                _fy_count("CommonStockSharesOutstanding", 10.0, year=LATEST_YEAR - 10),
            ],
        }
    )
    result = FCFPerShareCAGR10YMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - (2.0**0.1 - 1.0)) < 1e-9


def test_outstanding_basis_wins_over_stray_diluted_rows() -> None:
    # Even if diluted rows ever appeared, the declared basis is the
    # outstanding series: counts of 10 (not the stray 20s) set the per-share
    # values, so the CAGR still reflects the outstanding basis.
    repo = _FakeFactsRepo(
        {
            **_fcf_endpoints(),
            "CommonStockSharesOutstanding": [
                _fy_count("CommonStockSharesOutstanding", 10.0, year=LATEST_YEAR),
                _fy_count("CommonStockSharesOutstanding", 10.0, year=LATEST_YEAR - 10),
            ],
            "WeightedAverageNumberOfDilutedSharesOutstanding": [
                _fy_count(
                    "WeightedAverageNumberOfDilutedSharesOutstanding",
                    20.0,
                    year=LATEST_YEAR,
                ),
                _fy_count(
                    "WeightedAverageNumberOfDilutedSharesOutstanding",
                    20.0,
                    year=LATEST_YEAR - 10,
                ),
            ],
        }
    )
    result = FCFPerShareCAGR10YMetric().compute(LISTING_ID, repo)
    assert result is not None
    assert abs(result.value - (2.0**0.1 - 1.0)) < 1e-9
