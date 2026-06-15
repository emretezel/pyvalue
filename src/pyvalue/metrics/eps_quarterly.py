"""Earnings per share TTM metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import logging

from pyvalue.facts import MonetaryFact, RegionFactsRepository
from pyvalue.metrics.base import MetricResult
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
        latest_records = self._fetch_quarters(listing_id, repo)
        if len(latest_records) >= 4 and is_recent_fact(latest_records[0]):
            ttm_value, currency = self._align_records(
                listing_id, repo, latest_records[:4]
            )
            as_of = latest_records[0].end_date
            return MetricResult.per_share(
                listing_id=listing_id,
                metric_id=self.id,
                value=ttm_value,
                as_of=as_of,
                currency=currency,
            )

        fy_record = self._latest_fy_eps(listing_id, repo)
        if fy_record is None:
            if len(latest_records) < 4:
                LOGGER.warning(
                    "eps_ttm: missing EPS quarters for listing_id=%s", listing_id
                )
            else:
                LOGGER.warning(
                    "eps_ttm: latest EPS quarter too old for listing_id=%s (%s)",
                    listing_id,
                    latest_records[0].end_date,
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

    def _fetch_quarters(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> list[MonetaryFact]:
        for concept in EPS_CONCEPTS:
            records = repo.monetary_facts_for_concept(listing_id, concept)
            quarterly = self._filter_quarterly(records)
            if len(quarterly) >= 4:
                return quarterly[:4]
        return []

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

    def _filter_quarterly(self, records: Iterable[MonetaryFact]) -> list[MonetaryFact]:
        filtered: list[MonetaryFact] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in {"Q1", "Q2", "Q3", "Q4"}:
                continue
            if record.end_date in seen_end_dates:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _align_records(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        records: list[MonetaryFact],
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
