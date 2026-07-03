"""PEG ratio metrics (plain and dividend-adjusted).

Peter Lynch's growth-at-a-reasonable-price yardstick: P/E divided by the
EPS growth rate in percentage points, with PEG < 1 flagging a multiple more
than covered by growth. The growth input is the Graham smoothed CAGR
(3-year-averaged endpoints over a 10-FY window, the same computation as
``graham_eps_10y_cagr_3y_avg``), trading responsiveness for endpoint
robustness. The dividend-adjusted variant credits the dividend yield
alongside growth -- Lynch's refinement for slower growers with fat payouts,
where total return (growth + yield) is what covers the multiple.

Both metrics compose existing metrics rather than re-deriving inputs:
P/E is the inverse of ``earnings_yield`` and the yield credit reuses
``dividend_yield_ttm`` (the same pattern ``shareholder_yield_ttm`` uses).

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.earnings_yield import EarningsYieldMetric
from pyvalue.metrics.graham_eps_cagr import compute_graham_eps_cagr
from pyvalue.metrics.profitability_returns_growth import DividendYieldTTMMetric
from pyvalue.persistence.storage import MarketDataRepository

LOGGER = logging.getLogger(__name__)

EPS_CONCEPT = "EarningsPerShare"

PEG_REQUIRED_CONCEPTS = (EPS_CONCEPT,)
# The dividend leg divides cash dividends by market cap (or DPS by price), so
# the div-adjusted variant must preload everything dividend_yield_ttm reads.
PEG_DIV_ADJ_REQUIRED_CONCEPTS = tuple(
    dict.fromkeys((EPS_CONCEPT,) + tuple(DividendYieldTTMMetric.required_concepts))
)


def _resolve_pe(
    listing_id: int,
    repo: RegionFactsRepository,
    market_repo: MarketDataRepository,
    *,
    context: str,
) -> Optional[tuple[float, str]]:
    """Resolve the trailing P/E as the inverse of the earnings-yield metric.

    Composing ``earnings_yield`` keeps a single TTM-EPS/price policy across
    both metrics; a non-positive yield means non-positive TTM EPS (the yield
    only emits for positive prices), for which no P/E -- and hence no PEG --
    is meaningful.
    """

    earnings_yield = EarningsYieldMetric().compute(listing_id, repo, market_repo)
    if earnings_yield is None:
        LOGGER.warning(
            "%s: missing earnings yield for listing_id=%s", context, listing_id
        )
        return None
    if earnings_yield.value <= 0:
        LOGGER.warning(
            "%s: non-positive TTM EPS for listing_id=%s", context, listing_id
        )
        return None
    return 1.0 / earnings_yield.value, earnings_yield.as_of


@dataclass
class PEGRatioMetric:
    """Compute the PEG ratio: trailing P/E over EPS growth percentage."""

    id: str = "peg_ratio"
    required_concepts = PEG_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        pe = _resolve_pe(listing_id, repo, market_repo, context=self.id)
        if pe is None:
            return None
        pe_value, pe_as_of = pe

        growth = compute_graham_eps_cagr(listing_id, repo, context=self.id)
        if growth is None:
            return None
        growth_rate, growth_as_of = growth
        if growth_rate <= 0:
            # PEG is meaningless for shrinking earnings: a negative denominator
            # would make collapsing businesses look "cheapest".
            LOGGER.warning(
                "%s: non-positive EPS growth for listing_id=%s", self.id, listing_id
            )
            return None

        # Growth is a decimal (0.15 = 15%); Lynch's convention divides P/E by
        # growth in percentage points, hence the factor of 100.
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=pe_value / (100.0 * growth_rate),
            as_of=max(pe_as_of, growth_as_of),
        )


@dataclass
class PEGRatioDividendAdjustedMetric:
    """Compute Lynch's dividend-adjusted PEG: P/E over growth% + yield%."""

    id: str = "peg_ratio_div_adj"
    required_concepts = PEG_DIV_ADJ_REQUIRED_CONCEPTS
    uses_market_data = True

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        market_repo: MarketDataRepository,
    ) -> Optional[MetricResult]:
        pe = _resolve_pe(listing_id, repo, market_repo, context=self.id)
        if pe is None:
            return None
        pe_value, pe_as_of = pe

        growth = compute_graham_eps_cagr(listing_id, repo, context=self.id)
        if growth is None:
            return None
        growth_rate, growth_as_of = growth

        # A missing dividend yield means a non-payer (the metric returns None
        # when neither cash dividends nor a DPS fallback exist): credit zero
        # yield so the variant degrades to the plain PEG.
        dividend_yield = DividendYieldTTMMetric().compute(listing_id, repo, market_repo)
        yield_rate = dividend_yield.value if dividend_yield is not None else 0.0

        # Unlike the plain PEG, mildly negative growth is acceptable when the
        # payout covers it: the denominator is the *total* return in percentage
        # points, and only its sign gates the metric.
        denominator = 100.0 * (growth_rate + yield_rate)
        if denominator <= 0:
            LOGGER.warning(
                "%s: non-positive growth+yield for listing_id=%s",
                self.id,
                listing_id,
            )
            return None

        as_of = max(pe_as_of, growth_as_of)
        if dividend_yield is not None:
            as_of = max(as_of, dividend_yield.as_of)
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=pe_value / denominator,
            as_of=as_of,
        )


__all__ = ["PEGRatioMetric", "PEGRatioDividendAdjustedMetric"]
