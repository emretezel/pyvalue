"""Shared balance-sheet position resolvers (cash, total debt).

Author: Emre Tezel

``net_debt_to_ebitda`` and the enterprise-value denominator both need "the
listing's cash position" and "the listing's total debt" from the latest
balance sheet. Before this module each had its own resolution (and EV's was
strictly narrower: single cash concept, both debt sides required, no
freshness), so economically identical listings could carry a net-debt figure
but no EV. One implementation here keeps the two metrics' balance-sheet
semantics identical by construction:

- cash: ``CashAndShortTermInvestments`` when fresh, else
  ``CashAndCashEquivalents`` (required) plus ``ShortTermInvestments``
  (optional add-on) -- providers normalize one or the other rollup, and the
  two-step chain reconstructs the broad definition when only the parts exist;
- debt: ``ShortTermDebt`` and/or ``LongTermDebt`` -- one side reported alone
  is a real balance-sheet shape (no current maturities, or no long-term
  debt), so a single side suffices; both absent means debt is unknown, not
  zero, and resolves to ``None``;
- freshness: every fact must be within the standard 400-day window --
  a stale balance sheet is a data gap, not a usable position.

Resolvers return ``None`` silently; callers own the metric-scoped logging so
persisted failure reasons keep their per-metric wording.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.utils import is_recent_fact, require_metric_money
from pyvalue.money import Money

# Resolution order matters: the first cash concept is the broad rollup, the
# latter two reconstruct it. Kept as tuples so metric ``required_concepts``
# declarations can splice them in for fact preloading.
CASH_CONCEPTS: tuple[str, ...] = (
    "CashAndShortTermInvestments",
    "CashAndCashEquivalents",
    "ShortTermInvestments",
)
DEBT_CONCEPTS: tuple[str, ...] = ("ShortTermDebt", "LongTermDebt")


@dataclass(frozen=True)
class BalanceSheetPosition:
    """A resolved balance-sheet amount with its observation date."""

    money: Money
    as_of: str


def resolve_cash_position(
    listing_id: int,
    repo: RegionFactsRepository,
    *,
    target_currency: str,
    metric_id: str,
) -> Optional[BalanceSheetPosition]:
    """Resolve the latest fresh cash position through the fallback chain."""

    primary = _latest_recent_fact(repo, listing_id, CASH_CONCEPTS[0])
    if primary is not None:
        return BalanceSheetPosition(
            money=_money(primary, target_currency, listing_id, metric_id),
            as_of=primary.end_date,
        )

    cash_eq = _latest_recent_fact(repo, listing_id, CASH_CONCEPTS[1])
    if cash_eq is None:
        return None
    cash_money = _money(cash_eq, target_currency, listing_id, metric_id)
    as_of_candidates = [cash_eq.end_date]

    # Short-term investments are an optional add-on: their absence usually
    # means "none held", so only the equivalents leg is mandatory.
    short_term_investments = _latest_recent_fact(repo, listing_id, CASH_CONCEPTS[2])
    if short_term_investments is not None:
        cash_money = cash_money + _money(
            short_term_investments, target_currency, listing_id, metric_id
        )
        as_of_candidates.append(short_term_investments.end_date)
    return BalanceSheetPosition(money=cash_money, as_of=max(as_of_candidates))


def resolve_total_debt(
    listing_id: int,
    repo: RegionFactsRepository,
    *,
    target_currency: str,
    metric_id: str,
) -> Optional[BalanceSheetPosition]:
    """Resolve total debt from whichever fresh debt sides are reported."""

    short_debt = _latest_recent_fact(repo, listing_id, DEBT_CONCEPTS[0])
    long_debt = _latest_recent_fact(repo, listing_id, DEBT_CONCEPTS[1])
    if short_debt is None and long_debt is None:
        return None

    debt_money: Optional[Money] = None
    as_of_candidates: list[str] = []
    for record in (short_debt, long_debt):
        if record is None:
            continue
        component = _money(record, target_currency, listing_id, metric_id)
        debt_money = component if debt_money is None else debt_money + component
        as_of_candidates.append(record.end_date)
    # At least one side is present (guarded above).
    assert debt_money is not None
    return BalanceSheetPosition(money=debt_money, as_of=max(as_of_candidates))


def _latest_recent_fact(
    repo: RegionFactsRepository,
    listing_id: int,
    concept: str,
) -> Optional[MonetaryFact]:
    record = repo.latest_monetary_fact(listing_id, concept)
    if record is None or not is_recent_fact(record):
        return None
    return record


def _money(
    fact: MonetaryFact,
    target_currency: str,
    listing_id: int,
    metric_id: str,
) -> Money:
    return require_metric_money(
        fact.money,
        target_currency=target_currency,
        metric_id=metric_id,
        listing_id=listing_id,
        input_name=fact.concept,
        as_of=fact.end_date,
    )


__all__ = [
    "BalanceSheetPosition",
    "CASH_CONCEPTS",
    "DEBT_CONCEPTS",
    "resolve_cash_position",
    "resolve_total_debt",
]
