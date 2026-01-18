"""Interest coverage metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import is_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
EBIT_CONCEPTS = ("OperatingIncomeLoss",)
INTEREST_CONCEPTS = ("InterestExpense",)


@dataclass
class InterestCoverageMetric:
    """Compute TTM interest coverage (EBIT / interest expense)."""

    id: str = "interest_coverage"
    required_concepts = tuple(EBIT_CONCEPTS + INTEREST_CONCEPTS)

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        ebit_quarters = self._quarterly_map(
            repo.facts_for_concept(symbol, EBIT_CONCEPTS[0])
        )
        interest_quarters = self._quarterly_map(
            repo.facts_for_concept(symbol, INTEREST_CONCEPTS[0])
        )
        common_dates = [
            end_date for end_date in ebit_quarters if end_date in interest_quarters
        ]
        if len(common_dates) < 4:
            LOGGER.warning(
                "interest_coverage: need 4 aligned quarterly records for %s", symbol
            )
            return None
        common_dates = common_dates[:4]

        ebit_records = [ebit_quarters[end_date] for end_date in common_dates]
        interest_records = [interest_quarters[end_date] for end_date in common_dates]
        if not is_recent_fact(ebit_records[0]) or not is_recent_fact(
            interest_records[0]
        ):
            LOGGER.warning("interest_coverage: latest quarter too old for %s", symbol)
            return None

        currency = self._resolve_currency(ebit_records + interest_records)
        if currency is None and any(
            record.currency for record in (ebit_records + interest_records)
        ):
            LOGGER.warning("interest_coverage: currency mismatch for %s", symbol)
            return None

        ebit_ttm = sum(record.value for record in ebit_records)
        interest_ttm = sum(record.value for record in interest_records)
        if ebit_ttm <= 0:
            LOGGER.warning("interest_coverage: non-positive EBIT for %s", symbol)
            return None
        if interest_ttm <= 0:
            LOGGER.warning(
                "interest_coverage: non-positive interest expense for %s", symbol
            )
            return None

        ratio = ebit_ttm / interest_ttm
        as_of = max(ebit_records[0].end_date, interest_records[0].end_date)
        return MetricResult(symbol=symbol, metric_id=self.id, value=ratio, as_of=as_of)

    def _quarterly_map(self, records: list[FactRecord]) -> dict[str, FactRecord]:
        # Keep the latest record per end_date to align quarters across concepts.
        ordered = self._filter_quarterly(records)
        return {record.end_date: record for record in ordered}

    def _filter_quarterly(self, records: list[FactRecord]) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in QUARTERLY_PERIODS:
                continue
            if record.end_date in seen_end_dates:
                continue
            if record.value is None:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _resolve_currency(self, records: list[FactRecord]) -> Optional[str]:
        currency = None
        for record in records:
            code = record.currency
            if not code:
                continue
            if currency is None:
                currency = code
            elif code != currency:
                return None
        return currency


__all__ = ["InterestCoverageMetric"]
