"""Earnings per share TTM metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.ttm import (
    FAILURE_TOO_FEW_QUARTERLY_RECORDS,
    TTMWindowResolution,
    resolve_ttm_window,
)
from pyvalue.metrics.utils import (
    is_recent_fact,
    require_metric_money,
    require_metric_ticker_currency,
    sum_money,
)

EPS_CONCEPTS = ["EarningsPerShare"]

LOGGER = logging.getLogger(__name__)


@dataclass
class EarningsPerShareTTM:
    id: str = "eps_ttm"
    required_concepts = tuple(EPS_CONCEPTS)
    uses_market_data = False

    def compute(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
    ) -> Optional[MetricResult]:
        resolution = self._resolve_window(listing_id, repo)
        window = resolution.window
        if window is not None:
            # Per-share amounts sum like flows: four quarters -- or two
            # half-years for a semi-annual reporter -- cover twelve months.
            ttm_value, currency = self._align_records(listing_id, repo, window.records)
            return MetricResult.per_share(
                listing_id=listing_id,
                metric_id=self.id,
                value=ttm_value,
                as_of=window.as_of,
                currency=currency,
            )

        fy_record = self._latest_fy_eps(listing_id, repo)
        if fy_record is None:
            LOGGER.warning(
                "eps_ttm: %s for listing_id=%s", resolution.failure, listing_id
            )
            return None
        if not is_recent_fact(fy_record):
            LOGGER.warning(
                "eps_ttm: latest FY EPS too old for listing_id=%s (%s)",
                listing_id,
                fy_record.end_date,
            )
            return None
        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            metric_id=self.id,
            input_name="EarningsPerShare",
            as_of=fy_record.end_date,
        )
        money = require_metric_money(
            fy_record.money,
            target_currency=target_currency,
            metric_id=self.id,
            listing_id=listing_id,
            input_name="EarningsPerShare",
            as_of=fy_record.end_date,
        )
        return MetricResult.per_share(
            listing_id=listing_id,
            metric_id=self.id,
            value=money.amount,
            as_of=fy_record.end_date,
            currency=target_currency,
        )

    def _resolve_window(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> TTMWindowResolution[MonetaryFact]:
        """Resolve the EPS TTM window from the first concept that forms one.

        Preserves the legacy concept-fallback iteration: the first concept
        whose quarterly history resolves to a trailing-twelve-month window
        wins. When none does, the last failure is returned so ``compute`` can
        log why the quarterly path was skipped once the FY fallback also comes
        up empty. The initial value only covers an empty concept list.
        """

        resolution: TTMWindowResolution[MonetaryFact] = TTMWindowResolution(
            window=None, failure=FAILURE_TOO_FEW_QUARTERLY_RECORDS
        )
        for concept in EPS_CONCEPTS:
            resolution = resolve_ttm_window(
                repo.monetary_facts_for_concept(listing_id, concept)
            )
            if resolution.window is not None:
                break
        return resolution

    def _latest_fy_eps(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MonetaryFact]:
        for concept in EPS_CONCEPTS:
            records = repo.monetary_facts_for_concept(
                listing_id, concept, fiscal_period="FY", limit=1
            )
            if records:
                return records[0]
        return None

    def _align_records(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        records: Sequence[MonetaryFact],
    ) -> tuple[float, str]:
        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            metric_id=self.id,
            input_name="EarningsPerShare",
            as_of=records[0].end_date if records else None,
        )
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
                for record in records
            ]
        )
        return total.amount, target_currency


__all__ = ["EarningsPerShareTTM"]
