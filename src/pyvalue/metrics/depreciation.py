"""Depreciation & amortization concept names and shared constants.

Author: Emre Tezel

This module owns the two D&A concept names and the constants shared by the
metrics that consume them. EODHD's fundamentals payload emits *negative* D&A
rows (sign errors, financial-sector accretion mislabels, scale blow-ups) that
would corrupt every add-back consumer; those rows are dropped at read time by
the sign guard in ``pyvalue.metrics.fact_guards`` -- D&A metrics must read
their facts through :func:`pyvalue.metrics.fact_guards.guarded_monetary_facts`,
never through the repository directly. See that module for the artifact
taxonomy and the treat-as-absent policy.
"""

from __future__ import annotations

# The income-statement D&A line (EODHD ``depreciationAndAmortization``, then
# ``reconciledDepreciation``) is the primary source; the cash-flow ``depreciation``
# line is the fallback. Both are normalized as monetary facts and are meant to be
# positive.
DA_PRIMARY_CONCEPT = "DepreciationDepletionAndAmortization"
DA_FALLBACK_CONCEPT = "DepreciationFromCashFlow"
DA_PRIMARY_CONCEPTS = (DA_PRIMARY_CONCEPT,)
DA_FALLBACK_CONCEPTS = (DA_FALLBACK_CONCEPT,)

# The D&A members of the fact_guards non-negative set. Kept here so concept
# ownership stays with this module; fact_guards composes them into
# NON_NEGATIVE_CONCEPTS.
DA_CONCEPTS = frozenset({DA_PRIMARY_CONCEPT, DA_FALLBACK_CONCEPT})

# Maintenance-capex proxy caps capex at 1.1x D&A so a one-off growth-capex spike
# does not understate owner earnings. Shared by mcapex and both owner-earnings
# calculators.
DA_MULTIPLIER = 1.1

__all__ = [
    "DA_PRIMARY_CONCEPT",
    "DA_FALLBACK_CONCEPT",
    "DA_PRIMARY_CONCEPTS",
    "DA_FALLBACK_CONCEPTS",
    "DA_CONCEPTS",
    "DA_MULTIPLIER",
]
