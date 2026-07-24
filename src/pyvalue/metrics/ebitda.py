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
from typing import Optional

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


__all__ = ["EBITDAResult", "EBIT_CONCEPT", "compute_component_ttm_ebitda"]
