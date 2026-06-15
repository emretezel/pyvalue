"""Earnings per share streak metric.

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
)

EPS_CONCEPTS = ["EarningsPerShare"]

LOGGER = logging.getLogger(__name__)


@dataclass
class EPSStreakMetric:
    id: str = "eps_streak"
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
        if not records:
            LOGGER.warning(
                "eps_streak: no FY EPS records for listing_id=%s", listing_id
            )
            return None
        if not has_recent_fact(
            repo, listing_id, EPS_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "eps_streak: no recent FY EPS fact for listing_id=%s", listing_id
            )
            return None

        unique = filter_unique_fy(records)

        streak = 0
        latest_as_of = records[0].end_date
        for end_date in sorted(unique.keys(), reverse=True):
            record = unique[end_date]
            # EPS is per-share money; a non-positive amount (in any currency)
            # ends the positive-EPS streak.
            if record.money.amount <= 0:
                break
            streak += 1
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=float(streak),
            as_of=latest_as_of,
        )
