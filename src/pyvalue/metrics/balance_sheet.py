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
- debt: the *broadest measured* of two representations of the same quantity --
  the ``ShortTermDebt`` + ``LongTermDebt`` component sum, and the provider's
  ``TotalDebtFromBalanceSheet`` rollup. One component side reported alone is a
  real balance-sheet shape (no current maturities, or no long-term debt), so a
  single side suffices; all three absent means debt is unknown, **not zero**,
  and ``resolve_total_debt`` resolves to ``None``. See ``resolve_total_debt``
  for why the larger wins, and ``resolve_total_debt_bounded`` for the two
  callers allowed to substitute an upper bound rather than give up;
- freshness: every fact must be within a caller-supplied window (the standard
  400-day one by default) -- a stale balance sheet is a data gap, not a usable
  position. Callers whose income flow resolved on the annual cadence pass the
  480-day FY window instead, so an annual-only filer's balance-sheet legs stay
  as fresh as its once-a-year income statement rather than going stale in the
  post-fiscal-year-end gap.

Two further resolvers bound the debt burden for listings whose debt concepts
are absent entirely -- ``interest_coverage`` walks them in order to decide
whether its debt-free cap is safe to emit:

- ``resolve_noncurrent_liabilities``: everything owed beyond twelve months.
  Debt is a subset of it, so it is an upper bound -- and a tighter one than
  total liabilities.
- ``resolve_total_liabilities``: the coarsest bound of all, for feeds that
  carry neither a debt concept nor a non-current split.

Both overstate deliberately, which is fail-safe for a cap check: an inflated
bound can only *block* a debt-free cap, never manufacture one.

Resolvers return ``None`` silently; callers own the metric-scoped logging so
persisted failure reasons keep their per-metric wording.
"""

from __future__ import annotations

import logging

from dataclasses import dataclass
from typing import Optional

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    is_recent_fact,
    require_metric_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

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
NONCURRENT_LIABILITIES_CONCEPT: str = "LiabilitiesNoncurrent"
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
    """Resolve total debt as the broadest of the measured representations.

    Providers describe the same debt twice: as components (``ShortTermDebt`` +
    ``LongTermDebt``) and as a single ``TotalDebtFromBalanceSheet`` rollup.
    Across the EODHD universe the two agree on only 24% of the 33,004 listings
    carrying both, so the choice between them is load-bearing rather than
    cosmetic. **The larger wins.**

    Two representations of one quantity differ because their *scope* differs,
    and the wider scope is the one that captured more of the balance sheet --
    most often finance leases, which EODHD folds into the rollup but not into
    the components. Microsoft (2026-06-30) reported components of $49.97B
    against a $128.81B rollup; reading the components made a company with
    $52.0B of net debt look like it held $26.9B of net cash and understated
    enterprise value by 2.1%.

    Taking the larger is also the safe direction for a value screen: an
    understated debt figure shrinks EV and *manufactures* a cheap-looking
    yield, while an overstated one only costs an opportunity.

    This resolver was components-preferred until 2026-08-10, guarding against
    "lease contamination" of the rollup. Leases are debt -- non-cancellable
    obligations against productive assets -- so that guard was protecting the
    wrong number. The genuine hazard behind it was the normalizer's fabricated
    ``LongTermDebt`` (non-current liabilities filed as borrowings), which would
    have won this ``max()`` on 46% of the universe. That input was removed at
    its source in the same audit, which is what makes taking the larger safe;
    do not reintroduce a derived value into either debt concept.
    """

    component_sum = _component_debt_sum(
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
    return broader_debt_reading(component_sum, rollup)


def resolve_total_debt_bounded(
    listing_id: int,
    repo: RegionFactsRepository,
    *,
    target_currency: str,
    metric_id: str,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> Optional[BalanceSheetPosition]:
    """Measured total debt, else non-current liabilities as an upper bound.

    For ``enterprise_value`` and ``net_debt_to_ebitda`` only. Both consume debt
    as one term of a composite, and both are wrong in a *knowable direction*
    when that term is too large: an inflated debt figure raises EV, which lowers
    every yield built on it, and raises the leverage ratio. It can only make a
    listing look worse than it is -- never cheaper, never safer. So substituting
    an upper bound cannot manufacture a passing gate or a better rank, and the
    alternative is worse: ``resolve_total_debt`` alone returns ``None`` for
    every listing whose latest balance sheet carries no debt row, which costs
    5,255 listings all three price arms and removes them from the screen
    entirely. Those listings did not become expensive; they became unmeasurable.

    Two callers must **not** use this, and the asymmetry is the whole point:

    - ``short_term_debt_share`` puts total debt in the *denominator* of a share,
      so a bound there understates the share -- the flattering direction.
    - ``interest_coverage`` keeps its own ordered ladder because it needs the
      coarser ``Liabilities`` rung as well, and because it must know which rung
      answered before deciding whether its debt-free cap is safe to emit.

    Metrics that report a debt figure as their own value (``long_term_debt``,
    ``debt_paydown_years``, ``fcf_to_debt``) also stay measured-only: there the
    bound would not be an input to a conclusion, it would *be* the conclusion.

    The substitution is logged at INFO so it is visible in
    ``data/logs/pyvalue.log`` -- the stored metric carries no provenance column.
    """

    measured = resolve_total_debt(
        listing_id,
        repo,
        target_currency=target_currency,
        metric_id=metric_id,
        max_age_days=max_age_days,
    )
    if measured is not None:
        return measured

    bound = resolve_noncurrent_liabilities(
        listing_id,
        repo,
        target_currency=target_currency,
        metric_id=metric_id,
        max_age_days=max_age_days,
    )
    if bound is None:
        return None

    LOGGER.info(
        "%s: no fresh debt row for listing_id=%s -- bounding total debt by "
        "non-current liabilities (%s)",
        metric_id,
        listing_id,
        bound.as_of,
    )
    return bound


def broader_debt_reading(
    component_sum: Optional[BalanceSheetPosition],
    rollup: Optional[BalanceSheetPosition],
) -> Optional[BalanceSheetPosition]:
    """Pick the wider of two measured debt representations.

    The debt *rule* for the whole codebase, kept separate from any particular
    way of selecting facts. ``resolve_total_debt`` feeds it the latest fresh
    rows; ``invested_capital`` feeds it one fiscal period's rows. Both must
    reach the same answer from the same inputs, which is only guaranteed if the
    comparison itself lives in exactly one place -- before the 2026-08 debt
    audit five private copies of this decision disagreed with each other.

    Callers must mint both positions to the same currency first, which makes
    the ``Money`` ordering below well defined. ``None`` when neither side has a
    usable row: debt is unknown, not zero.
    """

    if component_sum is None:
        return rollup
    if rollup is None:
        return component_sum
    return rollup if rollup.money > component_sum.money else component_sum


def _component_debt_sum(
    listing_id: int,
    repo: RegionFactsRepository,
    *,
    target_currency: str,
    metric_id: str,
    max_age_days: int,
) -> Optional[BalanceSheetPosition]:
    """Sum whichever fresh component debt sides are reported.

    One side alone is a real balance-sheet shape (no current maturities, or no
    long-term debt), so a single side suffices; neither side means ``None``.
    """

    short_debt = _latest_recent_fact(
        repo, listing_id, DEBT_CONCEPTS[0], max_age_days=max_age_days
    )
    long_debt = _latest_recent_fact(
        repo, listing_id, DEBT_CONCEPTS[1], max_age_days=max_age_days
    )
    debt_money: Optional[Money] = None
    as_of_candidates: list[str] = []
    for record in (short_debt, long_debt):
        if record is None:
            continue
        component = _money(record, target_currency, listing_id, metric_id)
        debt_money = component if debt_money is None else debt_money + component
        as_of_candidates.append(record.end_date)
    if debt_money is None:
        return None
    return BalanceSheetPosition(money=debt_money, as_of=max(as_of_candidates))


def resolve_noncurrent_liabilities(
    listing_id: int,
    repo: RegionFactsRepository,
    *,
    target_currency: str,
    metric_id: str,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> Optional[BalanceSheetPosition]:
    """Resolve the latest fresh non-current-liabilities figure.

    Everything owed beyond twelve months: long-term borrowings plus deferred
    revenue, deferred tax, pensions and provisions. Debt is a subset, so this
    bounds it from above -- more tightly than total liabilities, because the
    current-liability block is excluded. Used only where an upper bound is
    wanted (``interest_coverage``'s cap check), never as debt itself.
    """

    fact = _latest_recent_fact(
        repo, listing_id, NONCURRENT_LIABILITIES_CONCEPT, max_age_days=max_age_days
    )
    if fact is None:
        return None
    return BalanceSheetPosition(
        money=_money(fact, target_currency, listing_id, metric_id),
        as_of=fact.end_date,
    )


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
    "NONCURRENT_LIABILITIES_CONCEPT",
    "TOTAL_LIABILITIES_CONCEPT",
    "broader_debt_reading",
    "resolve_cash_position",
    "resolve_noncurrent_liabilities",
    "resolve_total_debt",
    "resolve_total_debt_bounded",
    "resolve_total_liabilities",
]
