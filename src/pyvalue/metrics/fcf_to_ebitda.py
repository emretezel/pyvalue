"""FCF to EBITDA cash-conversion metric.

Operationalizes Buffett's critique of EBITDA: depreciation is a real cost paid
in advance, so EBITDA overstates distributable cash for capital-hungry
businesses. Dividing trailing free cash flow by trailing EBITDA measures how
much of the accounting cash-earnings proxy actually converts into free cash.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.depreciation import DA_FALLBACK_CONCEPT, DA_PRIMARY_CONCEPT
from pyvalue.metrics.enterprise_value_ratios import (
    CAPEX_CONCEPT,
    EBIT_CONCEPT,
    OPERATING_CASH_FLOW_CONCEPT,
    EnterpriseValueRatioCalculator,
)

LOGGER = logging.getLogger(__name__)

# Both numerator (OCF - capex) and denominator (EBIT + D&A) reuse the shared
# EV-ratio calculator policies, so the required concepts are exactly its
# inputs -- without the EV balance-sheet concepts, because this ratio needs no
# market data or enterprise-value denominator.
REQUIRED_CONCEPTS = (
    OPERATING_CASH_FLOW_CONCEPT,
    CAPEX_CONCEPT,
    EBIT_CONCEPT,
    DA_PRIMARY_CONCEPT,
    DA_FALLBACK_CONCEPT,
)


@dataclass
class FCFToEBITDAMetric:
    """Compute trailing FCF divided by trailing component EBITDA."""

    id: str = "fcf_to_ebitda"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        calculator = EnterpriseValueRatioCalculator()

        fcf = calculator.compute_ttm_fcf(listing_id, repo, context=self.id)
        if fcf is None:
            LOGGER.warning("%s: missing TTM FCF for listing_id=%s", self.id, listing_id)
            return None

        # Component EBITDA (EBIT + D&A), consistent with ev_to_ebitda, rather
        # than the vendor-supplied EBITDA fact -- keeps every EBITDA-based
        # metric on one derivation policy.
        ebitda = calculator.compute_ttm_ebitda(listing_id, repo, context=self.id)
        if ebitda is None:
            LOGGER.warning(
                "%s: missing TTM EBITDA for listing_id=%s", self.id, listing_id
            )
            return None
        if ebitda.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive EBITDA for listing_id=%s", self.id, listing_id
            )
            return None

        # Negative FCF is deliberately allowed: a negative conversion ratio is
        # the signal this metric exists to surface (EBITDA masking cash burn).
        # Only a non-positive denominator makes the ratio uninterpretable.
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=fcf.money / ebitda.money,
            as_of=max(fcf.as_of, ebitda.as_of),
        )


__all__ = ["FCFToEBITDAMetric"]
