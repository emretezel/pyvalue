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
- freshness: every fact must be within a caller-supplied window (the standard
  400-day one by default) -- a stale balance sheet is a data gap, not a usable
  position. Callers whose income flow resolved on the annual cadence pass the
  480-day FY window instead, so an annual-only filer's balance-sheet legs stay
  as fresh as its once-a-year income statement rather than going stale in the
  post-fiscal-year-end gap.

Two further resolvers serve evidence checks rather than net-debt arithmetic:

- ``resolve_debt_evidence``: an *upper bound* on the debt burden -- the
  larger of the component sum and the provider's total-debt rollup. Used by
  ``interest_coverage`` to decide whether its debt-free cap is safe to emit;
  deliberate overstatement (e.g. the normalizer's derived ``LongTermDebt`` =
  total minus current liabilities, or lease-contaminated rollups) can only
  *block* a cap, never create one, so it is fail-safe by construction.
- ``resolve_total_liabilities``: the latest fresh total-liabilities figure,
  the coarsest upper bound of all (debt is a subset of liabilities), for
  listings whose feed carries no debt concept at all.

Resolvers return ``None`` silently; callers own the metric-scoped logging so
persisted failure reasons keep their per-metric wording.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    is_recent_fact,
    require_metric_money,
)
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
# Evidence checks also consult the provider's total-debt rollup
# (``shortLongTermDebtTotal``), which providers populate independently of the
# component fields -- and total liabilities as the bound of last resort.
DEBT_EVIDENCE_CONCEPTS: tuple[str, ...] = (*DEBT_CONCEPTS, "TotalDebtFromBalanceSheet")
TOTAL_LIABILITIES_CONCEPT: str = "Liabilities"


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
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> Optional[BalanceSheetPosition]:
    """Resolve the latest fresh cash position through the fallback chain."""

    primary = _latest_recent_fact(
        repo, listing_id, CASH_CONCEPTS[0], max_age_days=max_age_days
    )
    if primary is not None:
        return BalanceSheetPosition(
            money=_money(primary, target_currency, listing_id, metric_id),
            as_of=primary.end_date,
        )

    cash_eq = _latest_recent_fact(
        repo, listing_id, CASH_CONCEPTS[1], max_age_days=max_age_days
    )
    if cash_eq is None:
        return None
    cash_money = _money(cash_eq, target_currency, listing_id, metric_id)
    as_of_candidates = [cash_eq.end_date]

    # Short-term investments are an optional add-on: their absence usually
    # means "none held", so only the equivalents leg is mandatory.
    short_term_investments = _latest_recent_fact(
        repo, listing_id, CASH_CONCEPTS[2], max_age_days=max_age_days
    )
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
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> Optional[BalanceSheetPosition]:
    """Resolve total debt from whichever fresh debt sides are reported."""

    short_debt = _latest_recent_fact(
        repo, listing_id, DEBT_CONCEPTS[0], max_age_days=max_age_days
    )
    long_debt = _latest_recent_fact(
        repo, listing_id, DEBT_CONCEPTS[1], max_age_days=max_age_days
    )
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


def resolve_debt_evidence(
    listing_id: int,
    repo: RegionFactsRepository,
    *,
    target_currency: str,
    metric_id: str,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> Optional[BalanceSheetPosition]:
    """Resolve an upper bound on the listing's fresh debt burden.

    Returns the *larger* of the component sum (``resolve_total_debt``) and
    the provider's ``TotalDebtFromBalanceSheet`` rollup, or ``None`` when no
    debt concept has a fresh row. ``max()`` -- not the components-preferred
    chain ``debt_paydown_years`` uses -- because the two representations can
    disagree (a provider unit error in one field, leases in the other) and an
    evidence check must trust the worst reading: overstatement blocks a
    debt-free cap (an honest NA), understatement would manufacture a false
    gate pass. Do not reuse this for net-debt arithmetic, where a measured
    single representation is wanted instead.
    """

    component_sum = resolve_total_debt(
        listing_id,
        repo,
        target_currency=target_currency,
        metric_id=metric_id,
        max_age_days=max_age_days,
    )
    rollup_fact = _latest_recent_fact(
        repo, listing_id, DEBT_EVIDENCE_CONCEPTS[2], max_age_days=max_age_days
    )
    rollup: Optional[BalanceSheetPosition] = None
    if rollup_fact is not None:
        rollup = BalanceSheetPosition(
            money=_money(rollup_fact, target_currency, listing_id, metric_id),
            as_of=rollup_fact.end_date,
        )

    if component_sum is None:
        return rollup
    if rollup is None:
        return component_sum
    # Both minted to target_currency above, so Money ordering is safe.
    return rollup if rollup.money > component_sum.money else component_sum


def resolve_total_liabilities(
    listing_id: int,
    repo: RegionFactsRepository,
    *,
    target_currency: str,
    metric_id: str,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> Optional[BalanceSheetPosition]:
    """Resolve the latest fresh total-liabilities figure.

    Debt is a subset of total liabilities, so this is the coarsest upper
    bound available -- the evidence of last resort when a feed carries no
    debt concept at all (some providers null the debt fields instead of
    reporting zeroes). ``None`` when absent or stale.
    """

    fact = _latest_recent_fact(
        repo, listing_id, TOTAL_LIABILITIES_CONCEPT, max_age_days=max_age_days
    )
    if fact is None:
        return None
    return BalanceSheetPosition(
        money=_money(fact, target_currency, listing_id, metric_id),
        as_of=fact.end_date,
    )


def _latest_recent_fact(
    repo: RegionFactsRepository,
    listing_id: int,
    concept: str,
    *,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> Optional[MonetaryFact]:
    record = repo.latest_monetary_fact(listing_id, concept)
    if record is None or not is_recent_fact(record, max_age_days=max_age_days):
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
    "DEBT_EVIDENCE_CONCEPTS",
    "TOTAL_LIABILITIES_CONCEPT",
    "resolve_cash_position",
    "resolve_debt_evidence",
    "resolve_total_debt",
    "resolve_total_liabilities",
]
