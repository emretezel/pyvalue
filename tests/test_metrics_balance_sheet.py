"""Tests for the shared balance-sheet position resolvers.

Author: Emre Tezel
"""

from datetime import date, timedelta

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.balance_sheet import (
    resolve_cash_position,
    resolve_total_debt,
)
from pyvalue.persistence.storage import FactRecord
from test_metrics import fact

LISTING_ID = 1

FRESH = (date.today() - timedelta(days=30)).isoformat()
FRESH_OLDER = (date.today() - timedelta(days=60)).isoformat()
STALE = (date.today() - timedelta(days=500)).isoformat()


class _FakeFactsRepo(RegionFactsRepository):
    """Serves one latest fact per concept, as the balance-sheet reads do."""

    def __init__(self, latest_by_concept: dict[str, FactRecord]) -> None:
        super().__init__(self)
        self._latest_by_concept = latest_by_concept

    def facts_for_concept(
        self,
        listing_id: int,
        concept: str,
        fiscal_period: str | None = None,
        limit: int | None = None,
    ) -> list[FactRecord]:
        record = self._latest_by_concept.get(concept)
        return [record] if record is not None else []

    def latest_fact(self, listing_id: int, concept: str) -> FactRecord | None:
        return self._latest_by_concept.get(concept)

    def ticker_currency_by_id(self, listing_id: int) -> str | None:
        return "USD"


def _instant(concept: str, value: float, *, end_date: str = FRESH) -> FactRecord:
    return fact(concept=concept, fiscal_period="Q4", end_date=end_date, value=value)


def _cash(repo: _FakeFactsRepo) -> float | None:
    position = resolve_cash_position(
        LISTING_ID, repo, target_currency="USD", metric_id="test"
    )
    return None if position is None else position.money.amount


def _debt(repo: _FakeFactsRepo) -> float | None:
    position = resolve_total_debt(
        LISTING_ID, repo, target_currency="USD", metric_id="test"
    )
    return None if position is None else position.money.amount


def test_cash_prefers_the_broad_rollup() -> None:
    repo = _FakeFactsRepo(
        {
            "CashAndShortTermInvestments": _instant(
                "CashAndShortTermInvestments", 120.0
            ),
            "CashAndCashEquivalents": _instant("CashAndCashEquivalents", 80.0),
            "ShortTermInvestments": _instant("ShortTermInvestments", 20.0),
        }
    )
    assert _cash(repo) == 120.0


def test_cash_reconstructs_rollup_from_equivalents_plus_investments() -> None:
    repo = _FakeFactsRepo(
        {
            "CashAndCashEquivalents": _instant("CashAndCashEquivalents", 80.0),
            "ShortTermInvestments": _instant(
                "ShortTermInvestments", 20.0, end_date=FRESH_OLDER
            ),
        }
    )
    position = resolve_cash_position(
        LISTING_ID, repo, target_currency="USD", metric_id="test"
    )
    assert position is not None
    assert position.money.amount == 100.0
    # The observation date covers the newest contributing component.
    assert position.as_of == FRESH


def test_cash_equivalents_alone_suffice() -> None:
    repo = _FakeFactsRepo(
        {"CashAndCashEquivalents": _instant("CashAndCashEquivalents", 80.0)}
    )
    assert _cash(repo) == 80.0


def test_cash_stale_rollup_falls_back_to_fresh_equivalents() -> None:
    repo = _FakeFactsRepo(
        {
            "CashAndShortTermInvestments": _instant(
                "CashAndShortTermInvestments", 120.0, end_date=STALE
            ),
            "CashAndCashEquivalents": _instant("CashAndCashEquivalents", 80.0),
        }
    )
    assert _cash(repo) == 80.0


def test_cash_short_term_investments_alone_are_not_cash() -> None:
    repo = _FakeFactsRepo(
        {"ShortTermInvestments": _instant("ShortTermInvestments", 20.0)}
    )
    assert _cash(repo) is None


def test_debt_sums_both_sides() -> None:
    repo = _FakeFactsRepo(
        {
            "ShortTermDebt": _instant("ShortTermDebt", 50.0),
            "LongTermDebt": _instant("LongTermDebt", 150.0),
        }
    )
    assert _debt(repo) == 200.0


def test_debt_accepts_a_single_side() -> None:
    repo = _FakeFactsRepo({"LongTermDebt": _instant("LongTermDebt", 150.0)})
    assert _debt(repo) == 150.0


def test_debt_without_any_side_is_unknown_not_zero() -> None:
    repo = _FakeFactsRepo({})
    assert _debt(repo) is None


def test_debt_ignores_a_stale_side() -> None:
    repo = _FakeFactsRepo(
        {
            "ShortTermDebt": _instant("ShortTermDebt", 50.0, end_date=STALE),
            "LongTermDebt": _instant("LongTermDebt", 150.0),
        }
    )
    assert _debt(repo) == 150.0
