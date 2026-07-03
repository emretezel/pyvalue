"""Cash return on invested capital (CROIC) metric.

The cash lens on capital efficiency favored by Terry Smith and Chuck Akre:
trailing free cash flow -- rather than accounting NOPAT -- over invested
capital. Pairs with roic_ttm: a persistent gap between the two flags
accrual-heavy earnings or chronic capex hunger that NOPAT-based returns
cannot see.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.enterprise_value_ratios import (
    CAPEX_CONCEPT,
    OPERATING_CASH_FLOW_CONCEPT,
    EnterpriseValueRatioCalculator,
)
from pyvalue.metrics.invested_capital import (
    REQUIRED_CONCEPTS as INVESTED_CAPITAL_REQUIRED_CONCEPTS,
    InvestedCapitalCalculator,
)

LOGGER = logging.getLogger(__name__)

REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        (OPERATING_CASH_FLOW_CONCEPT, CAPEX_CONCEPT)
        + INVESTED_CAPITAL_REQUIRED_CONCEPTS
    )
)


@dataclass
class CROICMetric:
    """Compute cash return on invested capital: TTM FCF / avg invested capital."""

    id: str = "croic"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        # Same FCF policy as every other FCF metric (OCF - capex, missing capex
        # treated as zero) and the same averaged invested-capital denominator
        # as roic_ttm, so CROIC vs ROIC comparisons differ only in numerator.
        fcf = EnterpriseValueRatioCalculator().compute_ttm_fcf(
            listing_id, repo, context=self.id
        )
        if fcf is None:
            LOGGER.warning("%s: missing TTM FCF for listing_id=%s", self.id, listing_id)
            return None

        avg_ic = InvestedCapitalCalculator().compute_avg(listing_id, repo)
        if avg_ic is None:
            LOGGER.warning("%s: missing avg_ic for listing_id=%s", self.id, listing_id)
            return None
        if avg_ic.money.amount <= 0:
            LOGGER.warning(
                "%s: non-positive avg_ic for listing_id=%s", self.id, listing_id
            )
            return None

        # Negative FCF flows through as a negative cash return (mirroring
        # fcf_yield_ev): cash burn on deployed capital is exactly the signal a
        # quality screen wants to see. roic_ttm's non-positive-NOPAT guard
        # exists only because its tax-rate model breaks there.
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=fcf.money / avg_ic.money,
            as_of=max(fcf.as_of, avg_ic.as_of),
            unit_kind="percent",
        )


__all__ = ["CROICMetric"]
