"""Regression: invested capital must subtract true cash, not a sub-line.

The EODHD mapping preferred ``cashAndEquivalents`` over ``cash`` for the
``CashAndCashEquivalents`` concept. ``cash`` is the provider's canonical
headline figure (``cash + shortTermInvestments == cashAndShortTermInvestments``
holds exactly wherever all three are present); ``cashAndEquivalents`` is a
sparsely populated supplement that, when it diverges, is a narrower sub-line.
For TSLA FY2025 the old priority stored 1.89B instead of the true 16.513B,
so FY invested capital computed as 8,376M + 82,137M - 1,890M = 88,623M
(persisted in pyvalue.db) instead of 74,000M, understating ``roic_ttm`` and
``croic`` by ~20%. See ``docs/research/qarp-dvg-metric-verification-2026-07.md``
(A2/B3, backlog P3).

The test drives the real pipeline seam end-to-end: a TSLA-shaped raw payload
through ``EODHDFactsNormalizer`` into an in-memory repo, then FY invested
capital. On the old field priority it fails with 88,623M.

Author: Emre Tezel
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date

from pyvalue.facts import FactRecord, RegionFactsRepository
from pyvalue.metrics.invested_capital import InvestedCapitalCalculator
from pyvalue.normalization.eodhd import EODHDFactsNormalizer

LISTING_ID = 1
# Dynamic FY end keeps the fixture inside compute_fy's 480-day freshness gate
# regardless of when the suite runs.
LATEST_YEAR = date.today().year - 1


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


def _tsla_fy_payload() -> dict[str, object]:
    """TSLA FY2025 balance-sheet shape with the real (diverging) cash fields."""

    return {
        "Financials": {
            "Balance_Sheet": {
                "yearly": [
                    {
                        "date": f"{LATEST_YEAR}-12-31",
                        # The divergence under test: ``cash`` is the filing's
                        # cash & equivalents; ``cashAndEquivalents`` a sub-line.
                        "cash": 16_513_000_000.0,
                        "cashAndEquivalents": 1_890_000_000.0,
                        "cashAndShortTermInvestments": 44_059_000_000.0,
                        "shortTermInvestments": 27_546_000_000.0,
                        "shortTermDebt": 1_640_000_000.0,
                        "longTermDebtTotal": 6_736_000_000.0,
                        "shortLongTermDebtTotal": 8_376_000_000.0,
                        "totalStockholderEquity": 82_137_000_000.0,
                        "currency_symbol": "USD",
                    }
                ]
            }
        },
        "General": {"CurrencyCode": "USD"},
    }


def test_fy_invested_capital_subtracts_true_cash_not_sub_line() -> None:
    """FY IC = debt 8,376M + equity 82,137M - cash 16,513M = 74,000M."""

    records = EODHDFactsNormalizer().normalize(_tsla_fy_payload(), symbol="TSLA.US")
    by_concept: dict[str, list[FactRecord]] = defaultdict(list)
    for record in records:
        by_concept[record.concept].append(record)
    repo = _FakeFactsRepo(dict(by_concept))

    snapshot = InvestedCapitalCalculator().compute_fy(LISTING_ID, repo)
    assert snapshot is not None
    # The old priority subtracted the 1,890M sub-line instead, computing the
    # 88,623M that was persisted in pyvalue.db for TSLA.
    assert snapshot.money.amount == 74_000_000_000.0
    assert snapshot.money.currency == "USD"
    assert snapshot.as_of == f"{LATEST_YEAR}-12-31"
