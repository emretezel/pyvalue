"""Owner earnings equity metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.nwc import DeltaNWCMaintMetric
from pyvalue.metrics.utils import (
    MAX_FACT_AGE_DAYS,
    MAX_FY_FACT_AGE_DAYS,
    is_recent_fact,
)
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

NI_PRIMARY_CONCEPT = "NetIncomeLoss"
NI_FALLBACK_CONCEPT = "NetIncomeLossAvailableToCommonStockholdersBasic"
DA_PRIMARY_CONCEPT = "DepreciationDepletionAndAmortization"
DA_FALLBACK_CONCEPT = "DepreciationFromCashFlow"
CAPEX_CONCEPT = "CapitalExpenditures"

NI_CONCEPTS = (NI_PRIMARY_CONCEPT, NI_FALLBACK_CONCEPT)
DA_PRIMARY_CONCEPTS = (DA_PRIMARY_CONCEPT,)
DA_FALLBACK_CONCEPTS = (DA_FALLBACK_CONCEPT,)
CAPEX_CONCEPTS = (CAPEX_CONCEPT,)
QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}
DA_MULTIPLIER = 1.1

NWC_MAINT_REQUIRED_CONCEPTS = (
    "AssetsCurrent",
    "LiabilitiesCurrent",
    "CashAndShortTermInvestments",
    "CashAndCashEquivalents",
    "ShortTermInvestments",
    "ShortTermDebt",
)

REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        NI_CONCEPTS
        + DA_PRIMARY_CONCEPTS
        + DA_FALLBACK_CONCEPTS
        + CAPEX_CONCEPTS
        + NWC_MAINT_REQUIRED_CONCEPTS
    )
)


@dataclass(frozen=True)
class OwnerEarningsEquitySnapshot:
    value: float
    as_of: str
    currency: Optional[str]


@dataclass
class _AmountResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class _FYPoint:
    value: float
    as_of: str
    currency: Optional[str]


class OwnerEarningsEquityCalculator:
    """Shared calculator for owner earnings equity numerators."""

    def compute_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[OwnerEarningsEquitySnapshot]:
        delta_nwc_maint = self._compute_delta_nwc_maint(symbol, repo)
        if delta_nwc_maint is None:
            LOGGER.warning("oe_equity_ttm: missing delta_nwc_maint for %s", symbol)
            return None

        ni = self._compute_ttm_amount(
            symbol,
            repo,
            NI_CONCEPTS,
            context="oe_equity_ttm",
            absolute=False,
        )
        if ni is None:
            LOGGER.warning("oe_equity_ttm: missing TTM net income for %s", symbol)
            return None

        da = self._compute_ttm_amount(
            symbol,
            repo,
            DA_PRIMARY_CONCEPTS,
            context="oe_equity_ttm",
            absolute=False,
        )
        if da is None:
            da = self._compute_ttm_amount(
                symbol,
                repo,
                DA_FALLBACK_CONCEPTS,
                context="oe_equity_ttm",
                absolute=False,
            )

        mcapex = self._compute_mcapex_ttm(symbol, repo)
        if mcapex is None:
            LOGGER.warning("oe_equity_ttm: missing TTM mcapex inputs for %s", symbol)
            return None

        currency = self._combine_currency(
            [ni.currency, da.currency if da else None, mcapex.currency]
        )
        if currency is None and any(
            code is not None
            for code in (ni.currency, da.currency if da else None, mcapex.currency)
        ):
            LOGGER.warning("oe_equity_ttm: currency mismatch for %s", symbol)
            return None

        da_total = da.total if da is not None else 0.0
        as_of_dates = [ni.as_of, mcapex.as_of, delta_nwc_maint.as_of]
        if da is not None:
            as_of_dates.append(da.as_of)
        as_of = max(as_of_dates)
        value = ni.total + da_total - mcapex.total - delta_nwc_maint.value
        return OwnerEarningsEquitySnapshot(value=value, as_of=as_of, currency=currency)

    def compute_5y_average(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[OwnerEarningsEquitySnapshot]:
        delta_nwc_maint = self._compute_delta_nwc_maint(symbol, repo)
        if delta_nwc_maint is None:
            LOGGER.warning("oe_equity_5y_avg: missing delta_nwc_maint for %s", symbol)
            return None

        ni_map = self._build_fy_amount_map(symbol, repo, NI_CONCEPTS, absolute=False)
        da_map = self._build_fy_amount_map(
            symbol, repo, DA_PRIMARY_CONCEPTS + DA_FALLBACK_CONCEPTS, absolute=False
        )
        mcapex_map = self._build_mcapex_fy_map(symbol, repo)

        candidate_dates = sorted(
            set(ni_map.keys()).intersection(mcapex_map.keys()),
            reverse=True,
        )
        points: list[_FYPoint] = []
        for end_date in candidate_dates:
            ni = ni_map[end_date]
            mcapex = mcapex_map[end_date]
            da = da_map.get(end_date)

            point_currency = self._combine_currency(
                [ni.currency, da.currency if da else None, mcapex.currency]
            )
            if point_currency is None and any(
                code is not None
                for code in (ni.currency, da.currency if da else None, mcapex.currency)
            ):
                LOGGER.warning(
                    "oe_equity_5y_avg: currency mismatch on %s for %s",
                    end_date,
                    symbol,
                )
                continue

            point_value = ni.total + (da.total if da is not None else 0.0)
            point_value = point_value - mcapex.total - delta_nwc_maint.value
            points.append(
                _FYPoint(value=point_value, as_of=end_date, currency=point_currency)
            )

        if len(points) < 5:
            LOGGER.warning(
                "oe_equity_5y_avg: need 5 FY owner earnings values for %s, found %s",
                symbol,
                len(points),
            )
            return None

        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=MAX_FY_FACT_AGE_DAYS):
            LOGGER.warning(
                "oe_equity_5y_avg: latest FY (%s) too old for %s",
                latest.as_of,
                symbol,
            )
            return None

        latest_five = points[:5]
        series_currency = self._combine_currency(
            [point.currency for point in latest_five]
        )
        if series_currency is None and any(
            point.currency is not None for point in latest_five
        ):
            LOGGER.warning(
                "oe_equity_5y_avg: currency mismatch across selected FY series for %s",
                symbol,
            )
            return None

        average = sum(point.value for point in latest_five) / 5.0
        return OwnerEarningsEquitySnapshot(
            value=average,
            as_of=latest_five[0].as_of,
            currency=series_currency,
        )

    def _compute_delta_nwc_maint(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        return DeltaNWCMaintMetric().compute(symbol, repo)

    def _compute_ttm_amount(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        context: str,
        absolute: bool = False,
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
                quarterly[:4], absolute=absolute
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

    def _compute_mcapex_ttm(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountResult]:
        capex = self._compute_ttm_amount(
            symbol,
            repo,
            CAPEX_CONCEPTS,
            context="oe_equity_ttm",
            absolute=True,
        )
        da = self._compute_ttm_amount(
            symbol,
            repo,
            DA_PRIMARY_CONCEPTS,
            context="oe_equity_ttm",
            absolute=True,
        )
        if da is None:
            da = self._compute_ttm_amount(
                symbol,
                repo,
                DA_FALLBACK_CONCEPTS,
                context="oe_equity_ttm",
                absolute=True,
            )
        return self._compute_mcapex_value(
            capex, da, symbol=symbol, context="oe_equity_ttm"
        )

    def _build_fy_amount_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concepts: Sequence[str],
        *,
        absolute: bool = False,
    ) -> dict[str, _AmountResult]:
        maps = [
            self._fy_map(symbol, repo, concept, absolute=absolute)
            for concept in concepts
        ]
        candidate_dates: set[str] = set()
        for mapped in maps:
            candidate_dates.update(mapped.keys())

        merged: dict[str, _AmountResult] = {}
        for end_date in sorted(candidate_dates, reverse=True):
            for mapped in maps:
                if end_date in mapped:
                    merged[end_date] = mapped[end_date]
                    break
        return merged

    def _build_mcapex_fy_map(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> dict[str, _AmountResult]:
        capex_map = self._fy_map(symbol, repo, CAPEX_CONCEPT, absolute=True)
        da_primary_map = self._fy_map(symbol, repo, DA_PRIMARY_CONCEPT, absolute=True)
        da_fallback_map = self._fy_map(symbol, repo, DA_FALLBACK_CONCEPT, absolute=True)

        candidate_dates = sorted(
            set(capex_map.keys())
            .union(da_primary_map.keys())
            .union(da_fallback_map.keys()),
            reverse=True,
        )
        mcapex_map: dict[str, _AmountResult] = {}
        for end_date in candidate_dates:
            capex = capex_map.get(end_date)
            da = da_primary_map.get(end_date) or da_fallback_map.get(end_date)
            value = self._compute_mcapex_value(
                capex,
                da,
                symbol=symbol,
                context="oe_equity_5y_avg",
            )
            if value is None:
                continue
            mcapex_map[end_date] = value
        return mcapex_map

    def _compute_mcapex_value(
        self,
        capex: Optional[_AmountResult],
        da: Optional[_AmountResult],
        *,
        symbol: str,
        context: str,
    ) -> Optional[_AmountResult]:
        if capex is None and da is None:
            return None
        if capex is not None and da is not None:
            if not self._currencies_match(capex.currency, da.currency):
                LOGGER.warning("%s: mcapex currency mismatch for %s", context, symbol)
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

    def _fy_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
        *,
        absolute: bool = False,
    ) -> dict[str, _AmountResult]:
        records = repo.facts_for_concept(symbol, concept, fiscal_period="FY")
        ordered = self._filter_periods(records, FY_PERIODS)
        mapped: dict[str, _AmountResult] = {}
        for record in ordered:
            value, currency = self._normalize_currency(record, absolute=absolute)
            mapped[record.end_date] = _AmountResult(
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
        return filtered

    def _normalize_records(
        self, records: Sequence[FactRecord], *, absolute: bool = False
    ) -> tuple[Optional[list[float]], Optional[str]]:
        currency = None
        normalized: list[float] = []
        for record in records:
            value, code = self._normalize_currency(record, absolute=absolute)
            if currency is None and code:
                currency = code
            elif code and currency and code != currency:
                return None, None
            normalized.append(value)
        return normalized, currency

    def _normalize_currency(
        self, record: FactRecord, *, absolute: bool = False
    ) -> tuple[float, Optional[str]]:
        value = record.value
        code = record.currency
        if code in {"GBX", "GBP0.01"}:
            value = value / 100.0
            code = "GBP"
        if absolute:
            value = abs(value)
        return value, code

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

    def _combine_currency(self, values: Sequence[Optional[str]]) -> Optional[str]:
        merged = None
        for value in values:
            if not value:
                continue
            if merged is None:
                merged = value
            elif merged != value:
                return None
        return merged


@dataclass
class OwnerEarningsEquityTTMMetric:
    """Compute TTM owner earnings equity for EODHD-oriented data."""

    id: str = "oe_equity_ttm"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEquityCalculator().compute_ttm(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


@dataclass
class OwnerEarningsEquityFiveYearAverageMetric:
    """Compute 5-year average FY owner earnings equity for EODHD-oriented data."""

    id: str = "oe_equity_5y_avg"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = OwnerEarningsEquityCalculator().compute_5y_average(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


__all__ = [
    "OwnerEarningsEquitySnapshot",
    "OwnerEarningsEquityCalculator",
    "OwnerEarningsEquityTTMMetric",
    "OwnerEarningsEquityFiveYearAverageMetric",
]
