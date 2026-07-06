"""Depreciation & amortization concepts and a data-quality read guard.

Author: Emre Tezel

This module owns the two D&A concept names and the single seam through which
metrics read D&A facts. EODHD's fundamentals payload emits *negative* D&A for
reasons that are never real depreciation:

* Sign errors on ordinary operating companies -- e.g. Argan (AGX) FY2026 raw
  ``Income_Statement.depreciationAndAmortization = -4,743,000`` while the real
  figure is positive and its own ``Cash_Flow.depreciation`` is ``+1,912,000``.
* Financial-sector mislabels and scale blow-ups -- for BDCs/insurers/REITs the
  "D&A" field is really net accretion of discount (genuinely negative but not
  depreciation), and some rows are absurd (e.g. SuRo/SSSS raw cash-flow
  depreciation ``-87,445,149,000,000``).

Every metric that uses D&A as an *add-back* (``EBITDA = EBIT + D&A``;
``OwnerEarnings = NI/NOPAT + D&A - ...``) assumes a non-negative value. Taking
``abs()`` would be worse than doing nothing -- it would turn the scale-error
rows into gigantic positive add-backs. Instead we treat a negative D&A fact as
*absent*: dropped at read time so the caller's primary->cash-flow fallback
engages, and a name with no usable D&A degrades to no add-back (owner earnings)
or NA (EBITDA-family) rather than a corrupted number. Zero is kept -- a genuine
no-D&A period is a valid add-back of nothing.

The guard lives here, in the metrics layer, rather than in persistence: the
stored fact really is negative in EODHD, so mutating it would violate the
single-source-of-truth rule. See ``docs/reference/metrics.md``.
"""

from __future__ import annotations

from typing import Optional

from pyvalue.facts import MonetaryFact, RegionFactsRepository

# The income-statement D&A line (EODHD ``depreciationAndAmortization``, then
# ``reconciledDepreciation``) is the primary source; the cash-flow ``depreciation``
# line is the fallback. Both are normalized as monetary facts and are meant to be
# positive.
DA_PRIMARY_CONCEPT = "DepreciationDepletionAndAmortization"
DA_FALLBACK_CONCEPT = "DepreciationFromCashFlow"
DA_PRIMARY_CONCEPTS = (DA_PRIMARY_CONCEPT,)
DA_FALLBACK_CONCEPTS = (DA_FALLBACK_CONCEPT,)

# The set of concepts the sign guard applies to. Membership -- not any per-call
# flag -- is what drives the filtering, so a new D&A-like concept only needs to
# join this set to be guarded everywhere.
DA_CONCEPTS = frozenset({DA_PRIMARY_CONCEPT, DA_FALLBACK_CONCEPT})

# Maintenance-capex proxy caps capex at 1.1x D&A so a one-off growth-capex spike
# does not understate owner earnings. Shared by mcapex and both owner-earnings
# calculators.
DA_MULTIPLIER = 1.1


def guarded_monetary_facts(
    repo: RegionFactsRepository,
    listing_id: int,
    concept: str,
    *,
    fiscal_period: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[MonetaryFact]:
    """Read monetary facts for a metric, applying metric-layer data-quality guards.

    For a D&A concept (:data:`DA_CONCEPTS`) negative rows are dropped -- a negative
    depreciation add-back is never economically valid, so the row is treated as
    *absent* (letting the primary->cash-flow fallback and the TTM-window resolver
    behave as if EODHD never supplied it). Zero is kept. Every other concept
    passes through untouched: capex is a real negative cash outflow and net income
    is legitimately negative for a loss-making firm, so neither may be filtered.

    ``MonetaryFact`` deliberately exposes no bare magnitude; the sign is read via
    ``fact.money.amount``. Sign is currency-independent, so no FX alignment is
    needed for the comparison.
    """

    facts = repo.monetary_facts_for_concept(
        listing_id, concept, fiscal_period=fiscal_period, limit=limit
    )
    if concept in DA_CONCEPTS:
        return [fact for fact in facts if fact.money.amount >= 0]
    return facts


__all__ = [
    "DA_PRIMARY_CONCEPT",
    "DA_FALLBACK_CONCEPT",
    "DA_PRIMARY_CONCEPTS",
    "DA_FALLBACK_CONCEPTS",
    "DA_CONCEPTS",
    "DA_MULTIPLIER",
    "guarded_monetary_facts",
]
