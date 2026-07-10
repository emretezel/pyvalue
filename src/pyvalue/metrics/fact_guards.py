"""Metric-layer data-quality guards over monetary fact reads.

Author: Emre Tezel

This module owns the single seam through which metrics read facts whose sign
is economically constrained. EODHD emits *negative* values for flow lines that
can never really be negative, and the artifact taxonomy is the same across
concepts:

* Pure sign flips on ordinary operating companies -- e.g. Argan (AGX) FY2026
  raw ``depreciationAndAmortization = -4,743,000`` against a real positive
  figure; Adobe (ADBE) Q1 FY2026 raw ``interestExpense = -63,000,000`` against
  +62/+68/+67/+66M for the four surrounding quarters.
* Mislabels and scale blow-ups -- for BDCs/insurers/REITs the "D&A" field is
  really net accretion of discount, and some rows are absurd by orders of
  magnitude (SuRo/SSSS raw cash-flow depreciation ``-87,445,149,000,000``; the
  worst interest row in the 2026-07 universe is ``-262.5B``).

Consumers assume these values are non-negative: D&A is an *add-back*
(``EBITDA = EBIT + D&A``), interest expense is a *denominator*
(``coverage = EBIT / interest``). Taking ``abs()`` would be worse than doing
nothing -- it would turn scale-error rows into gigantic add-backs or crushing
denominators. Instead a negative fact is treated as *absent*: dropped at read
time so concept fallbacks and the TTM-window resolver behave as if the
provider never supplied it, and the metric degrades to the last clean window,
a fallback source, or an honest NA rather than a corrupted number. Zero is
kept -- a genuine no-D&A or no-interest period is a valid value.

The guard lives here, in the metrics layer, rather than in persistence or
normalization: the stored fact really is negative in EODHD, so mutating it at
write time would violate the single-source-of-truth rule -- and a read guard
takes effect on the next ``compute-metrics`` run with no re-normalization.
See ``docs/reference/metrics.md``.
"""

from __future__ import annotations

from typing import Optional

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.depreciation import DA_CONCEPTS

# Concepts whose facts are economically never negative: membership -- not any
# per-call flag -- is what drives the filtering, so a new artifact-prone
# concept only needs to join this set to be guarded everywhere. The D&A names
# are imported from their owner module; the interest names are spelled as
# literals because importing them from ``metrics.interest_coverage`` (their
# owner) would create an import cycle -- a membership test in
# ``tests/unit/test_fact_guards.py`` pins the two spellings together. The
# derived ``InterestExpenseFromNetInterestIncome`` is already non-negative at
# derivation (normalization guards ``candidate > 0``); its membership states
# the economic invariant rather than patching an observed artifact.
NON_NEGATIVE_CONCEPTS: frozenset[str] = frozenset(
    {
        *DA_CONCEPTS,
        "InterestExpense",
        "InterestExpenseFromNetInterestIncome",
    }
)


def guarded_monetary_facts(
    repo: RegionFactsRepository,
    listing_id: int,
    concept: str,
    *,
    fiscal_period: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[MonetaryFact]:
    """Read monetary facts for a metric, applying metric-layer data-quality guards.

    For a sign-constrained concept (:data:`NON_NEGATIVE_CONCEPTS`) negative rows
    are dropped -- a negative depreciation add-back or interest-expense
    denominator is never economically valid, so the row is treated as *absent*
    (letting concept fallbacks and the TTM-window resolver behave as if EODHD
    never supplied it). Zero is kept. Every other concept passes through
    untouched: capex is a real negative cash outflow and net income is
    legitimately negative for a loss-making firm, so neither may be filtered.

    ``MonetaryFact`` deliberately exposes no bare magnitude; the sign is read via
    ``fact.money.amount``. Sign is currency-independent, so no FX alignment is
    needed for the comparison.
    """

    facts = repo.monetary_facts_for_concept(
        listing_id, concept, fiscal_period=fiscal_period, limit=limit
    )
    if concept in NON_NEGATIVE_CONCEPTS:
        return [fact for fact in facts if fact.money.amount >= 0]
    return facts


__all__ = [
    "NON_NEGATIVE_CONCEPTS",
    "guarded_monetary_facts",
]
