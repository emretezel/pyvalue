"""Earnings per share TTM metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import logging

from pyvalue.metrics.base import Metric, MetricResult
from pyvalue.metrics.utils import is_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository

EPS_CONCEPTS = ["EarningsPerShareDiluted", "EarningsPerShareBasic"]

LOGGER = logging.getLogger(__name__)


@dataclass
class EarningsPerShareTTM:
    id: str = "eps_ttm"
    required_concepts = tuple(EPS_CONCEPTS)
    uses_market_data = False

    def compute(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
    ) -> Optional[MetricResult]:
        latest_records = self._fetch_quarters(symbol, repo)
        if len(latest_records) < 4:
            LOGGER.warning("eps_ttm: missing EPS quarters for %s", symbol)
            return None
        if not is_recent_fact(latest_records[0]):
            LOGGER.warning("eps_ttm: latest EPS quarter too old for %s (%s)", symbol, latest_records[0].end_date)
            return None
        ttm_value = sum(record.value for record in latest_records[:4])
        as_of = latest_records[0].end_date
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=ttm_value,
            as_of=as_of,
        )

    def _fetch_quarters(self, symbol: str, repo: FinancialFactsRepository) -> list[FactRecord]:
        for concept in EPS_CONCEPTS:
            records = repo.facts_for_concept(symbol, concept)
            quarterly = self._filter_quarterly(records)
            if len(quarterly) >= 4:
                return quarterly[:4]
        return []

    def _filter_quarterly(self, records: Iterable[FactRecord]) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in {"Q1", "Q2", "Q3", "Q4"}:
                continue
            if record.end_date in seen_end_dates:
                continue
            if record.value is None:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered


__all__ = ["EarningsPerShareTTM"]
