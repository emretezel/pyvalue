"""Shared trailing component-EBITDA construction.

EBITDA is deliberately built as EBIT + D&A from the same statements (the
"component" derivation) rather than read from the vendor-supplied ``EBITDA``
fact, so every EBITDA-based metric (``net_debt_to_ebitda``, ``ev_to_ebitda``,
``fcf_to_ebitda``) sits on one derivation policy. This module is that policy's
single home; the per-metric copies it replaced had drifted only in plumbing,
never in formula.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.depreciation import DA_FALLBACK_CONCEPT, DA_PRIMARY_CONCEPT
from pyvalue.metrics.fact_guards import guarded_monetary_facts
from pyvalue.metrics.ttm import Cadence, paired_records, resolve_ttm_window
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    require_metric_money,
    sum_money,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

EBIT_CONCEPT = "OperatingIncomeLoss"
# EODHD's own per-period EBITDA line. Never the primary source -- vendor
# derivations are junk-prone (see the negative-D&A audit in
# docs/research/screener-na-investigation.md) -- but a plausibility-guarded
# hole-filler when the component build lacks a D&A companion.
VENDOR_EBITDA_CONCEPT = "EBITDA"


@dataclass(frozen=True)
class EBITDAResult:
    """A trailing-twelve-month EBITDA aligned to the listing currency."""

    money: Money
    as_of: str
    # The reporting cadence the EBITDA window resolved on. An annual-only
    # filer resolves "annual", and its balance sheet is filed on the same
    # once-a-year cadence -- callers that divide by a point-in-time balance
    # sheet leg widen their freshness window to match.
    cadence: Cadence


def compute_component_ttm_ebitda(
    listing_id: int,
    repo: RegionFactsRepository,
    *,
    target_currency: str,
    context: str,
) -> Optional[EBITDAResult]:
    """Build TTM EBITDA as EBIT + D&A over one resolved income window.

    The window resolves on the shared cadence rules (quarterly, semi-annual,
    or -- opted in -- a single fresh FY row), and every window period must
    find a same-period D&A companion: the primary income-statement D&A wins a
    period, the cash-flow D&A only fills its holes, and negative D&A rows were
    already dropped at read time by the sign guard (they are provider
    artifacts, not real credits).
    """
    resolution = resolve_ttm_window(
        repo.monetary_facts_for_concept(listing_id, EBIT_CONCEPT),
        annual_max_age_days=MAX_FY_FACT_AGE_DAYS,
    )
    window = resolution.window
    if window is None:
        LOGGER.warning(
            "%s: %s (concept=%s, listing_id=%s)",
            context,
            resolution.failure,
            EBIT_CONCEPT,
            listing_id,
        )
        return None

    # Primary D&A rows are listed before the fallback rows: paired_records
    # keeps the first candidate per end_date, so the primary concept wins a
    # period and the fallback only fills its holes.
    pairs = paired_records(
        window,
        [
            *guarded_monetary_facts(repo, listing_id, DA_PRIMARY_CONCEPT),
            *guarded_monetary_facts(repo, listing_id, DA_FALLBACK_CONCEPT),
        ],
    )
    if pairs is None:
        # The income window itself resolved -- only the D&A companion is
        # missing for some window period. That exact hole is what the vendor
        # EBITDA line can fill under a plausibility guard; a failed fallback
        # keeps the established missing-D&A failure reason.
        fallback = _vendor_ttm_ebitda(
            listing_id,
            repo,
            ebit_window_records=window.records,
            target_currency=target_currency,
            context=context,
        )
        if fallback is not None:
            return fallback
        LOGGER.warning(
            "%s: missing D&A for a TTM window quarter (listing_id=%s)",
            context,
            listing_id,
        )
        return None

    quarter_totals = [
        _money(ebit_record, target_currency, listing_id, context)
        + _money(da_record, target_currency, listing_id, context)
        for ebit_record, da_record in pairs
    ]
    return EBITDAResult(
        money=sum_money(quarter_totals),
        as_of=window.as_of,
        cadence=window.cadence,
    )


def _vendor_ttm_ebitda(
    listing_id: int,
    repo: RegionFactsRepository,
    *,
    ebit_window_records: Sequence[MonetaryFact],
    target_currency: str,
    context: str,
) -> Optional[EBITDAResult]:
    """Vendor-supplied EBITDA as a guarded hole-filler.

    Fires only when the component build resolved an EBIT window but found no
    D&A companion for some window period. The vendor figure is accepted only
    when it is at least the resolved TTM EBIT: EBITDA below EBIT implies
    negative D&A, which the sign guard already establishes as a provider
    artifact -- such a row is rejected as contaminated rather than trusted.
    The vendor window resolves on its own cadence (annual opted in), so the
    result's freshness and balance-sheet widening follow the vendor rows.
    """
    resolution = resolve_ttm_window(
        repo.monetary_facts_for_concept(listing_id, VENDOR_EBITDA_CONCEPT),
        annual_max_age_days=MAX_FY_FACT_AGE_DAYS,
    )
    window = resolution.window
    if window is None:
        return None

    vendor_total = sum_money(
        [
            _money(record, target_currency, listing_id, context)
            for record in window.records
        ]
    )
    ebit_total = sum_money(
        [
            _money(record, target_currency, listing_id, context)
            for record in ebit_window_records
        ]
    )
    # No amounts in the message: failure reasons group on the scrubbed
    # template, and embedded Money values would fragment it per currency.
    if vendor_total.amount < ebit_total.amount:
        LOGGER.warning(
            "%s: vendor EBITDA below TTM EBIT for listing_id=%s"
            " -- implied negative D&A, fallback rejected",
            context,
            listing_id,
        )
        return None

    # INFO, not WARNING: the fallback is a measured outcome (documented-cap
    # precedent), so it must not pollute failure reasons on success.
    LOGGER.info(
        "%s: component D&A missing; vendor EBITDA fallback engaged for listing_id=%s",
        context,
        listing_id,
    )
    return EBITDAResult(
        money=vendor_total,
        as_of=window.as_of,
        cadence=window.cadence,
    )


def _money(
    fact: MonetaryFact, target_currency: str, listing_id: int, context: str
) -> Money:
    return require_metric_money(
        fact.money,
        target_currency=target_currency,
        metric_id=context,
        listing_id=listing_id,
        input_name=fact.concept,
        as_of=fact.end_date,
    )


__all__ = [
    "EBITDAResult",
    "EBIT_CONCEPT",
    "VENDOR_EBITDA_CONCEPT",
    "compute_component_ttm_ebitda",
]
