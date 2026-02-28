"""Maintenance capex proxy metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    is_recent_fact,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

CAPEX_CONCEPT = "CapitalExpenditures"
DA_PRIMARY_CONCEPT = "DepreciationDepletionAndAmortization"
DA_FALLBACK_CONCEPT = "DepreciationFromCashFlow"
CAPEX_CONCEPTS = (CAPEX_CONCEPT,)
DA_PRIMARY_CONCEPTS = (DA_PRIMARY_CONCEPT,)
DA_FALLBACK_CONCEPTS = (DA_FALLBACK_CONCEPT,)
ALL_CONCEPTS = CAPEX_CONCEPTS + DA_PRIMARY_CONCEPTS + DA_FALLBACK_CONCEPTS
QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}
DA_MULTIPLIER = 1.1


@dataclass
class _AmountResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class _FYPoint:
    value: float
    as_of: str


class _MCapexBase:
    def _compute_mcapex_value(
        self,
        capex: Optional[_AmountResult],
        da: Optional[_AmountResult],
        symbol: str,
        *,
        context: str,
    ) -> Optional[_AmountResult]:
        if capex is None and da is None:
            return None
        if capex is not None and da is not None:
            if not self._currencies_match(capex.currency, da.currency):
                LOGGER.warning("%s: currency mismatch for %s", context, symbol)
                return None
            return _AmountResult(
                total=min(capex.total, DA_MULTIPLIER * da.total),
                as_of=max(capex.as_of, da.as_of),
                currency=capex.currency or da.currency,
            )
        if capex is not None:
            return _AmountResult(
                total=capex.total,
                as_of=capex.as_of,
                currency=capex.currency,
            )
        assert da is not None
        return _AmountResult(
            total=DA_MULTIPLIER * da.total,
            as_of=da.as_of,
            currency=da.currency,
        )

    def _compute_ttm_amount(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
    ) -> Optional[_AmountResult]:
        for concept in concepts:
            records = repo.facts_for_concept(symbol, concept)
            quarterly = self._filter_periods(records, QUARTERLY_PERIODS)
            if len(quarterly) < 4:
                LOGGER.warning(
                    "%s: need 4 quarterly %s records for %s, found %s",
                    context,
                    concept,
                    symbol,
                    len(quarterly),
                )
                continue
            if not is_recent_fact(quarterly[0], max_age_days=MAX_FACT_AGE_DAYS):
                LOGGER.warning(
                    "%s: latest %s (%s) too old for %s",
                    context,
                    concept,
                    quarterly[0].end_date,
                    symbol,
                )
                continue
            normalized, currency = self._normalize_records(quarterly[:4])
            if normalized is None:
                LOGGER.warning(
                    "%s: currency conflict in %s quarterly values for %s",
                    context,
                    concept,
                    symbol,
                )
                continue
            return _AmountResult(
                total=sum(normalized),
                as_of=quarterly[0].end_date,
                currency=currency,
            )
        return None

    def _build_fy_points(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> list[_FYPoint]:
        capex_map = self._fy_map(symbol, repo, CAPEX_CONCEPT)
        da_primary_map = self._fy_map(symbol, repo, DA_PRIMARY_CONCEPT)
        da_fallback_map = self._fy_map(symbol, repo, DA_FALLBACK_CONCEPT)

        candidate_dates = sorted(
            set(capex_map.keys())
            .union(da_primary_map.keys())
            .union(da_fallback_map.keys()),
            reverse=True,
        )
        points: list[_FYPoint] = []
        for end_date in candidate_dates:
            capex = self._amount_from_record(capex_map.get(end_date))
            da_record = da_primary_map.get(end_date) or da_fallback_map.get(end_date)
            da = self._amount_from_record(da_record)
            value = self._compute_mcapex_value(capex, da, symbol, context="mcapex_fy")
            if value is None:
                continue
            points.append(_FYPoint(value=value.total, as_of=end_date))
        return points

    def _amount_from_record(
        self, record: Optional[FactRecord]
    ) -> Optional[_AmountResult]:
        if record is None:
            return None
        value, currency = self._normalize_currency(record)
        return _AmountResult(total=value, as_of=record.end_date, currency=currency)

    def _fy_map(
        self, symbol: str, repo: FinancialFactsRepository, concept: str
    ) -> dict[str, FactRecord]:
        records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
        ordered = self._filter_periods(records, FY_PERIODS)
        return {record.end_date: record for record in ordered}

    def _filter_periods(
        self, records: Sequence[FactRecord], periods: set[str]
    ) -> list[FactRecord]:
        filtered: list[FactRecord] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in periods:
                continue
            if record.end_date in seen_end_dates:
                continue
            if record.value is None:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _normalize_records(
        self, records: Sequence[FactRecord]
    ) -> tuple[Optional[list[float]], Optional[str]]:
        currency = None
        normalized: list[float] = []
        for record in records:
            value, code = self._normalize_currency(record)
            if currency is None and code:
                currency = code
            elif code and currency and code != currency:
                return None, None
            normalized.append(value)
        return normalized, currency

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        value = record.value
        code = record.currency
        if code in {"GBX", "GBP0.01"}:
            value = value / 100.0
            code = "GBP"
        return abs(value), code

    def _is_recent_as_of(self, as_of: str, *, max_age_days: int) -> bool:
        try:
            end_date = date.fromisoformat(as_of)
        except ValueError:
            return False
        return end_date >= (date.today() - timedelta(days=max_age_days))

    def _currencies_match(self, left: Optional[str], right: Optional[str]) -> bool:
        if left and right:
            return left == right
        return True


@dataclass
class MCapexFYMetric(_MCapexBase):
    """Compute latest fiscal-year maintenance capex proxy (EODHD-only)."""

    id: str = "mcapex_fy"
    required_concepts = ALL_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_fy_points(symbol, repo)
        if not points:
            LOGGER.warning("mcapex_fy: missing FY capex and D&A inputs for %s", symbol)
            return None
        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "mcapex_fy: latest FY (%s) too old for %s", latest.as_of, symbol
            )
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=latest.value, as_of=latest.as_of
        )


@dataclass
class MCapexFiveYearMetric(_MCapexBase):
    """Compute 5-year average of FY maintenance capex proxy (EODHD-only)."""

    id: str = "mcapex_5y"
    required_concepts = ALL_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_fy_points(symbol, repo)
        if len(points) < 5:
            LOGGER.warning(
                "mcapex_5y: need 5 FY maintenance capex values for %s, found %s",
                symbol,
                len(points),
            )
            return None
        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "mcapex_5y: latest FY (%s) too old for %s", latest.as_of, symbol
            )
            return None
        latest_five = points[:5]
        average = sum(point.value for point in latest_five) / 5.0
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=average,
            as_of=latest_five[0].as_of,
        )


@dataclass
class MCapexTTMMetric(_MCapexBase):
    """Compute trailing-12-month maintenance capex proxy (EODHD-only)."""

    id: str = "mcapex_ttm"
    required_concepts = ALL_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        capex = self._compute_ttm_amount(
            symbol, repo, CAPEX_CONCEPTS, context="mcapex_ttm"
        )
        da = self._compute_ttm_amount(
            symbol, repo, DA_PRIMARY_CONCEPTS, context="mcapex_ttm"
        )
        if da is None:
            da = self._compute_ttm_amount(
                symbol, repo, DA_FALLBACK_CONCEPTS, context="mcapex_ttm"
            )
        if capex is None and da is None:
            LOGGER.warning(
                "mcapex_ttm: missing TTM capex and D&A inputs for %s", symbol
            )
            return None
        value = self._compute_mcapex_value(capex, da, symbol, context="mcapex_ttm")
        if value is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=value.total, as_of=value.as_of
        )


__all__ = ["MCapexFYMetric", "MCapexFiveYearMetric", "MCapexTTMMetric"]
