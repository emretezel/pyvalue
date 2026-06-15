"""Graham EPS 10y period CAGR% (3y average) metric.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    filter_unique_fy,
    has_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
)

EPS_CONCEPTS = ["EarningsPerShare"]
WINDOW_YEARS = 10
AVG_WINDOW = 3
CAGR_YEARS = 7
MIN_REQUIRED = WINDOW_YEARS

LOGGER = logging.getLogger(__name__)


@dataclass
class GrahamEPSCAGRMetric:
    id: str = "graham_eps_10y_cagr_3y_avg"
    required_concepts = tuple(EPS_CONCEPTS)

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        records: List[MonetaryFact] = []
        for concept in EPS_CONCEPTS:
            records = repo.monetary_facts_for_concept(
                listing_id, concept, fiscal_period="FY"
            )
            if records:
                break
        if len(records) < MIN_REQUIRED:
            LOGGER.warning(
                "graham_eps_cagr: need >=%s FY EPS records for listing_id=%s, found %s",
                MIN_REQUIRED,
                listing_id,
                len(records),
            )
            return None
        if not has_recent_fact(
            repo, listing_id, EPS_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "graham_eps_cagr: no recent FY EPS fact for listing_id=%s", listing_id
            )
            return None
        latest_date = records[0].end_date
        filtered = filter_unique_fy(records)
        ordered = sorted(filtered.values(), key=lambda r: r.end_date)
        if len(ordered) < MIN_REQUIRED:
            LOGGER.warning(
                "graham_eps_cagr: need >=%s unique FY EPS records for listing_id=%s after filtering, found %s",
                MIN_REQUIRED,
                listing_id,
                len(ordered),
            )
            return None

        # EPS is per-share money; align every year to the listing currency before
        # the CAGR (currency cancels in the ratio, but the invariant still holds).
        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            metric_id=self.id,
            input_name="EarningsPerShare",
            as_of=latest_date,
        )
        eps_amounts = [
            require_metric_money(
                record.money,
                target_currency=target_currency,
                metric_id=self.id,
                listing_id=listing_id,
                input_name="EarningsPerShare",
                as_of=record.end_date,
            ).amount
            for record in ordered
        ]

        cagr_value = self._compute_cagr(eps_amounts)
        if cagr_value is None:
            LOGGER.warning(
                "graham_eps_cagr: could not derive CAGR value for listing_id=%s",
                listing_id,
            )
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=cagr_value,
            as_of=latest_date,
        )

    def _compute_cagr(self, eps_amounts: List[float]) -> Optional[float]:
        eps_history = eps_amounts[-WINDOW_YEARS:]
        start_values = eps_history[:AVG_WINDOW]
        end_values = eps_history[-AVG_WINDOW:]
        if any(value <= 0 for value in start_values + end_values):
            return None
        start_avg = sum(start_values) / AVG_WINDOW
        end_avg = sum(end_values) / AVG_WINDOW
        if start_avg <= 0 or end_avg <= 0:
            return None
        return (end_avg / start_avg) ** (1 / CAGR_YEARS) - 1
