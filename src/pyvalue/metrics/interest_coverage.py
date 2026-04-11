"""Interest coverage metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    is_recent_fact,
    normalize_metric_record,
    require_metric_ticker_currency,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
EBIT_CONCEPTS = ("OperatingIncomeLoss",)
INTEREST_CONCEPTS = ("InterestExpense",)
INTEREST_FALLBACK_CONCEPTS = ("InterestExpenseFromNetInterestIncome",)


@dataclass
class InterestCoverageMetric:
    """Compute TTM interest coverage (EBIT / interest expense)."""

    id: str = "interest_coverage"
    required_concepts = tuple(
        EBIT_CONCEPTS + INTEREST_CONCEPTS + INTEREST_FALLBACK_CONCEPTS
    )

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        ebit_quarters = self._quarterly_map(
            repo.facts_for_concept(symbol, EBIT_CONCEPTS[0])
        )
        direct_interest_quarters = self._quarterly_map(
            repo.facts_for_concept(symbol, INTEREST_CONCEPTS[0])
        )
        result = self._compute_from_maps(
            symbol=symbol,
            ebit_quarters=ebit_quarters,
            interest_quarters=direct_interest_quarters,
            log_failures=False,
            repo=repo,
        )
        if result is not None:
            return result

        fallback_interest_quarters = self._quarterly_map(
            repo.facts_for_concept(symbol, INTEREST_FALLBACK_CONCEPTS[0])
        )
        if not fallback_interest_quarters:
            LOGGER.warning(
                "interest_coverage: missing direct and fallback interest expense for %s",
                symbol,
            )
            return None

        merged_interest_quarters = dict(direct_interest_quarters)
        for end_date, record in fallback_interest_quarters.items():
            merged_interest_quarters.setdefault(end_date, record)
        return self._compute_from_maps(
            symbol=symbol,
            ebit_quarters=ebit_quarters,
            interest_quarters=merged_interest_quarters,
            log_failures=True,
            repo=repo,
        )

    def _compute_from_maps(
        self,
        *,
        symbol: str,
        ebit_quarters: dict[str, FactRecord],
        interest_quarters: dict[str, FactRecord],
        log_failures: bool,
        repo: FinancialFactsRepository,
    ) -> Optional[MetricResult]:
        common_dates = sorted(
            set(ebit_quarters).intersection(interest_quarters), reverse=True
        )
        if len(common_dates) < 4:
            if log_failures:
                LOGGER.warning(
                    "interest_coverage: need 4 aligned quarterly records for %s",
                    symbol,
                )
            return None
        common_dates = common_dates[:4]

        ebit_records = [ebit_quarters[end_date] for end_date in common_dates]
        interest_records = [interest_quarters[end_date] for end_date in common_dates]
        if not is_recent_fact(ebit_records[0]) or not is_recent_fact(
            interest_records[0]
        ):
            if log_failures:
                LOGGER.warning(
                    "interest_coverage: latest quarter too old for %s", symbol
                )
            return None

        normalized_ebit, ebit_currency = self._normalize_records(
            ebit_records,
            symbol=symbol,
            concept=EBIT_CONCEPTS[0],
            repo=repo,
        )
        normalized_interest, interest_currency = self._normalize_records(
            interest_records,
            symbol=symbol,
            concept=INTEREST_CONCEPTS[0],
            repo=repo,
        )

        ebit_ttm = sum(normalized_ebit)
        interest_ttm = sum(normalized_interest)
        if ebit_ttm <= 0:
            if log_failures:
                LOGGER.warning("interest_coverage: non-positive EBIT for %s", symbol)
            return None
        if interest_ttm <= 0:
            if log_failures:
                LOGGER.warning(
                    "interest_coverage: non-positive interest expense for %s", symbol
                )
            return None

        ratio = ebit_ttm / interest_ttm
        as_of = max(ebit_records[0].end_date, interest_records[0].end_date)
        return MetricResult.ratio(
            symbol=symbol,
            metric_id=self.id,
            value=ratio,
            as_of=as_of,
        )

    def _quarterly_map(self, records: Sequence[FactRecord]) -> dict[str, FactRecord]:
        # Keep the latest record per end_date to align quarters across concepts.
        ordered = self._filter_quarterly(records)
        return {record.end_date: record for record in ordered}

    def _filter_quarterly(self, records: Sequence[FactRecord]) -> list[FactRecord]:
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
        filtered.sort(key=lambda record: record.end_date, reverse=True)
        return filtered

    def _normalize_records(
        self,
        records: Sequence[FactRecord],
        *,
        symbol: str,
        concept: str,
        repo: FinancialFactsRepository,
    ) -> tuple[list[float], str]:
        target_currency = require_metric_ticker_currency(
            symbol,
            repo,
            metric_id=self.id,
            input_name=concept,
            as_of=records[0].end_date if records else None,
            candidate_currencies=[record.currency for record in records],
        )
        normalized: list[float] = []
        for record in records:
            value, _ = normalize_metric_record(
                record,
                metric_id=self.id,
                symbol=symbol,
                input_name=concept,
                expected_currency=target_currency,
                contexts=(repo,),
            )
            normalized.append(value)
        return normalized, target_currency


__all__ = ["InterestCoverageMetric"]
