"""Earnings per share TTM metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import is_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository

EPS_CONCEPTS = ["EarningsPerShare"]

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
        if len(latest_records) >= 4 and is_recent_fact(latest_records[0]):
            ttm_value = sum(record.value for record in latest_records[:4])
            as_of = latest_records[0].end_date
            return MetricResult(
                symbol=symbol,
                metric_id=self.id,
                value=ttm_value,
                as_of=as_of,
            )

        fy_record = self._latest_fy_eps(symbol, repo)
        if fy_record is None:
            if len(latest_records) < 4:
                LOGGER.warning("eps_ttm: missing EPS quarters for %s", symbol)
            else:
                LOGGER.warning(
                    "eps_ttm: latest EPS quarter too old for %s (%s)",
                    symbol,
                    latest_records[0].end_date,
                )
            return None
        if not is_recent_fact(fy_record):
            LOGGER.warning(
                "eps_ttm: latest FY EPS too old for %s (%s)", symbol, fy_record.end_date
            )
            return None
        if fy_record.value is None:
            LOGGER.warning("eps_ttm: missing FY EPS value for %s", symbol)
            return None

        ttm_value = fy_record.value
        as_of = fy_record.end_date
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=ttm_value,
            as_of=as_of,
        )

    def _fetch_quarters(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> list[FactRecord]:
        for concept in EPS_CONCEPTS:
            records = repo.facts_for_concept(symbol, concept)
            quarterly = self._filter_quarterly(records)
            if len(quarterly) >= 4:
                return quarterly[:4]
        return []

    def _latest_fy_eps(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[FactRecord]:
        for concept in EPS_CONCEPTS:
            records = repo.facts_for_concept(
                symbol, concept, fiscal_period="FY", limit=1
            )
            if records:
                return records[0]
        return None

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
