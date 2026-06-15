"""Six-year average EPS metric using fiscal year data.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    filter_unique_fy,
    has_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)

EPS_CONCEPTS = ["EarningsPerShare"]

LOGGER = logging.getLogger(__name__)


@dataclass
class EPSAverageSixYearMetric:
    id: str = "eps_6y_avg"
    required_concepts = tuple(EPS_CONCEPTS)
    uses_market_data = False

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
    ) -> Optional[MetricResult]:
        history = self._fetch_history(listing_id, repo)
        if len(history) < 6:
            LOGGER.warning(
                "eps_6y_avg: need >=6 FY EPS records for listing_id=%s, found %s",
                listing_id,
                len(history),
            )
            return None
        if not has_recent_fact(
            repo, listing_id, EPS_CONCEPTS, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "eps_6y_avg: no recent FY EPS fact for listing_id=%s", listing_id
            )
            return None
        latest_records = history[:6]
        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            metric_id=self.id,
            input_name="EarningsPerShare",
            as_of=latest_records[0].end_date,
        )
        # EPS is per-share money; align all six years to the listing currency,
        # then average (sum / 6) within that single currency.
        total = sum_money(
            [
                require_metric_money(
                    record.money,
                    target_currency=target_currency,
                    metric_id=self.id,
                    listing_id=listing_id,
                    input_name="EarningsPerShare",
                    as_of=record.end_date,
                )
                for record in latest_records
            ]
        )
        avg = total.amount / 6
        as_of = latest_records[0].end_date
        return MetricResult.per_share(
            listing_id=listing_id,
            metric_id=self.id,
            value=avg,
            as_of=as_of,
            currency=target_currency,
        )

    def _fetch_history(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> list[MonetaryFact]:
        for concept in EPS_CONCEPTS:
            records = repo.monetary_facts_for_concept(
                listing_id, concept, fiscal_period="FY"
            )
            unique = filter_unique_fy(records)
            if unique:
                ordered = sorted(
                    unique.values(), key=lambda rec: rec.end_date, reverse=True
                )
                if len(ordered) >= 6:
                    return ordered
        return []


__all__ = ["EPSAverageSixYearMetric"]
