"""Cash conversion metric implementations.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.fx import FXService
from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    is_recent_fact,
)
from pyvalue.money import align_money_values, fx_service_for_context
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

OPERATING_CASH_FLOW_CONCEPT = "NetCashProvidedByUsedInOperatingActivities"
NET_INCOME_PRIMARY_CONCEPT = "NetIncomeLoss"
NET_INCOME_FALLBACK_CONCEPT = "NetIncomeLossAvailableToCommonStockholdersBasic"

OPERATING_CASH_FLOW_CONCEPTS = (OPERATING_CASH_FLOW_CONCEPT,)
NET_INCOME_CONCEPTS = (
    NET_INCOME_PRIMARY_CONCEPT,
    NET_INCOME_FALLBACK_CONCEPT,
)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}
SERIES_YEARS = 10

REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(OPERATING_CASH_FLOW_CONCEPTS + NET_INCOME_CONCEPTS)
)


@dataclass(frozen=True)
class CashConversionSnapshot:
    value: float
    as_of: str
    currency: Optional[str]


@dataclass
class _AmountResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class _CashConversionFYPoint:
    year: int
    value: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class CashConversionTenYearSnapshot:
    points: tuple[_CashConversionFYPoint, ...]
    as_of: str
    currency: Optional[str]


class CashConversionCalculator:
    """Shared calculator for TTM and FY-series cash conversion metrics."""

    def compute_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[CashConversionSnapshot]:
        fx_service = fx_service_for_context(repo)
        cfo = self._compute_ttm_amount(
            symbol,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            context="cfo_to_ni_ttm",
            fx_service=fx_service,
        )
        if cfo is None:
            LOGGER.warning("cfo_to_ni_ttm: missing TTM CFO for %s", symbol)
            return None

        net_income = self._compute_ttm_amount(
            symbol,
            repo,
            NET_INCOME_CONCEPTS,
            context="cfo_to_ni_ttm",
            fx_service=fx_service,
        )
        if net_income is None:
            LOGGER.warning("cfo_to_ni_ttm: missing TTM net income for %s", symbol)
            return None
        if net_income.total <= 0:
            LOGGER.warning("cfo_to_ni_ttm: non-positive TTM net income for %s", symbol)
            return None

        aligned, _ = align_money_values(
            values=[
                (cfo.total, cfo.currency, cfo.as_of, OPERATING_CASH_FLOW_CONCEPT),
                (
                    net_income.total,
                    net_income.currency,
                    net_income.as_of,
                    NET_INCOME_PRIMARY_CONCEPT,
                ),
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation="metric:cfo_to_ni_ttm",
            symbol=symbol,
            target_currency=cfo.currency or net_income.currency,
        )
        if aligned is None:
            LOGGER.warning("cfo_to_ni_ttm: currency mismatch for %s", symbol)
            return None

        return CashConversionSnapshot(
            value=aligned[0] / aligned[1],
            as_of=max(cfo.as_of, net_income.as_of),
            currency=None,
        )

    def compute_10y_series(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[CashConversionTenYearSnapshot]:
        fx_service = fx_service_for_context(repo)
        cfo_map = self._build_fy_amount_map(
            symbol,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            fx_service=fx_service,
        )
        if not cfo_map:
            LOGGER.warning("cfo_to_ni_10y: missing FY CFO history for %s", symbol)
            return None

        net_income_map = self._build_fy_amount_map(
            symbol,
            repo,
            NET_INCOME_CONCEPTS,
            fx_service=fx_service,
        )
        if not net_income_map:
            LOGGER.warning(
                "cfo_to_ni_10y: missing FY net income history for %s", symbol
            )
            return None

        candidate_years = set(cfo_map.keys()).intersection(net_income_map.keys())
        if not candidate_years:
            LOGGER.warning(
                "cfo_to_ni_10y: missing overlapping FY history for %s", symbol
            )
            return None

        latest_year = max(candidate_years)
        selected: list[_CashConversionFYPoint] = []
        # Use the latest exact 10-year chain so every point is from the same cycle window.
        for year in range(latest_year, latest_year - SERIES_YEARS, -1):
            cfo = cfo_map.get(year)
            net_income = net_income_map.get(year)
            if cfo is None or net_income is None:
                LOGGER.warning(
                    "cfo_to_ni_10y: missing strict consecutive FY chain for %s", symbol
                )
                return None
            if net_income.total <= 0:
                LOGGER.warning(
                    "cfo_to_ni_10y: non-positive FY net income in %s for %s",
                    year,
                    symbol,
                )
                return None
            aligned, _ = align_money_values(
                values=[
                    (cfo.total, cfo.currency, cfo.as_of, OPERATING_CASH_FLOW_CONCEPT),
                    (
                        net_income.total,
                        net_income.currency,
                        net_income.as_of,
                        NET_INCOME_PRIMARY_CONCEPT,
                    ),
                ],
                fx_service=fx_service,
                logger=LOGGER,
                operation="metric:cfo_to_ni_10y_median",
                symbol=symbol,
                target_currency=cfo.currency or net_income.currency,
            )
            if aligned is None:
                LOGGER.warning(
                    "cfo_to_ni_10y: currency mismatch in %s for %s", year, symbol
                )
                return None

            selected.append(
                _CashConversionFYPoint(
                    year=year,
                    value=aligned[0] / aligned[1],
                    as_of=max(cfo.as_of, net_income.as_of),
                    currency=None,
                )
            )

        if not self._is_recent_as_of(
            selected[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("cfo_to_ni_10y: latest FY point too old for %s", symbol)
            return None

        return CashConversionTenYearSnapshot(
            points=tuple(selected),
            as_of=selected[0].as_of,
            currency=None,
        )

    def _compute_ttm_amount(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        fx_service: FXService,
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

            normalized, currency = self._normalize_records(
                quarterly[:4],
                symbol=symbol,
                concept=concept,
                context=context,
                fx_service=fx_service,
            )
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

    def _build_fy_amount_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        fx_service: FXService,
    ) -> dict[int, _AmountResult]:
        concept_maps = [
            self._fy_map(symbol, repo, concept, fx_service=fx_service)
            for concept in concepts
        ]
        merged: dict[int, _AmountResult] = {}
        candidate_years: set[int] = set()
        for mapped in concept_maps:
            candidate_years.update(mapped.keys())
        for year in sorted(candidate_years, reverse=True):
            for mapped in concept_maps:
                if year in mapped:
                    merged[year] = mapped[year]
                    break
        return merged

    def _fy_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
        *,
        fx_service: FXService,
    ) -> dict[int, _AmountResult]:
        records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
        ordered = self._filter_periods(records, FY_PERIODS)
        mapped: dict[int, _AmountResult] = {}
        for record in ordered:
            year = self._extract_year(record.end_date)
            if year is None or year in mapped:
                continue
            value, currency = self._normalize_currency(record)
            mapped[year] = _AmountResult(
                total=value,
                as_of=record.end_date,
                currency=currency,
            )
        return mapped

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
        filtered.sort(key=lambda record: record.end_date, reverse=True)
        return filtered

    def _normalize_records(
        self,
        records: Sequence[FactRecord],
        *,
        symbol: str,
        concept: str,
        context: str,
        fx_service: FXService,
    ) -> tuple[Optional[list[float]], Optional[str]]:
        return align_money_values(
            values=[
                (record.value, record.currency, record.end_date, concept)
                for record in records
                if record.value is not None
            ],
            fx_service=fx_service,
            logger=LOGGER,
            operation=f"metric:{context}:{concept}",
            symbol=symbol,
            target_currency=records[0].currency if records else None,
        )

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        value = record.value
        code = record.currency
        if code in {"GBX", "GBP0.01"}:
            return value / 100.0, "GBP"
        return value, code

    def _extract_year(self, value: str) -> Optional[int]:
        if len(value) < 4:
            return None
        prefix = value[:4]
        if not prefix.isdigit():
            return None
        return int(prefix)

    def _is_recent_as_of(self, as_of: str, *, max_age_days: int) -> bool:
        try:
            end_date = date.fromisoformat(as_of)
        except ValueError:
            return False
        return end_date >= (date.today() - timedelta(days=max_age_days))


@dataclass
class CFOToNITTMMetric:
    """Compute trailing 12-month operating cash flow to net income."""

    id: str = "cfo_to_ni_ttm"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = CashConversionCalculator().compute_ttm(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
            unit_kind="ratio",
        )


@dataclass
class CFOToNITenYearMedianMetric:
    """Compute 10-year median FY cash conversion using a strict consecutive window."""

    id: str = "cfo_to_ni_10y_median"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = CashConversionCalculator().compute_10y_series(symbol, repo)
        if snapshot is None:
            return None
        values = sorted(point.value for point in snapshot.points)
        median = (values[4] + values[5]) / 2.0
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=median,
            as_of=snapshot.as_of,
            unit_kind="ratio",
        )


__all__ = [
    "CashConversionSnapshot",
    "CashConversionTenYearSnapshot",
    "CashConversionCalculator",
    "CFOToNITTMMetric",
    "CFOToNITenYearMedianMetric",
]
