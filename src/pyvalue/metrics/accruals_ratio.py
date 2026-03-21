"""Accruals ratio metric implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, is_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

ASSETS_CONCEPT = "Assets"
OPERATING_CASH_FLOW_CONCEPT = "NetCashProvidedByUsedInOperatingActivities"
NET_INCOME_PRIMARY_CONCEPT = "NetIncomeLoss"
NET_INCOME_FALLBACK_CONCEPT = "NetIncomeLossAvailableToCommonStockholdersBasic"

OPERATING_CASH_FLOW_CONCEPTS = (OPERATING_CASH_FLOW_CONCEPT,)
NET_INCOME_CONCEPTS = (
    NET_INCOME_PRIMARY_CONCEPT,
    NET_INCOME_FALLBACK_CONCEPT,
)
REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        (ASSETS_CONCEPT,) + OPERATING_CASH_FLOW_CONCEPTS + NET_INCOME_CONCEPTS
    )
)

QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}


@dataclass(frozen=True)
class AccrualsRatioSnapshot:
    value: float
    as_of: str
    currency: Optional[str]


@dataclass
class _AmountResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass
class _AssetPoint:
    value: float
    as_of: str
    fiscal_period: str
    currency: Optional[str]


class AccrualsRatioCalculator:
    """Shared calculator for accruals ratio inputs."""

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[AccrualsRatioSnapshot]:
        net_income = self._compute_ttm_amount(
            symbol,
            repo,
            NET_INCOME_CONCEPTS,
            context="accruals_ratio",
        )
        if net_income is None:
            LOGGER.warning("accruals_ratio: missing TTM net income for %s", symbol)
            return None

        cfo = self._compute_ttm_amount(
            symbol,
            repo,
            OPERATING_CASH_FLOW_CONCEPTS,
            context="accruals_ratio",
        )
        if cfo is None:
            LOGGER.warning("accruals_ratio: missing TTM CFO for %s", symbol)
            return None

        numerator_currency = self._combine_currency([net_income.currency, cfo.currency])
        if numerator_currency is None and any(
            code is not None for code in (net_income.currency, cfo.currency)
        ):
            LOGGER.warning(
                "accruals_ratio: currency mismatch in numerator for %s", symbol
            )
            return None

        average_assets = self._compute_avg_total_assets(symbol, repo)
        if average_assets is None:
            return None
        if average_assets.total <= 0:
            LOGGER.warning("accruals_ratio: non-positive average assets for %s", symbol)
            return None

        if not self._currencies_match(numerator_currency, average_assets.currency):
            LOGGER.warning(
                "accruals_ratio: numerator/denominator currency mismatch for %s",
                symbol,
            )
            return None

        value = (net_income.total - cfo.total) / average_assets.total
        return AccrualsRatioSnapshot(
            value=value,
            as_of=max(net_income.as_of, cfo.as_of, average_assets.as_of),
            currency=numerator_currency or average_assets.currency,
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

    def _compute_avg_total_assets(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[_AmountResult]:
        records = repo.facts_for_concept(symbol, ASSETS_CONCEPT)
        quarterly = self._filter_periods(records, QUARTERLY_PERIODS)
        if not quarterly:
            LOGGER.warning("accruals_ratio: missing quarterly assets for %s", symbol)
            return None

        latest = quarterly[0]
        if not is_recent_fact(latest, max_age_days=MAX_FACT_AGE_DAYS):
            LOGGER.warning(
                "accruals_ratio: latest assets quarter (%s) too old for %s",
                latest.end_date,
                symbol,
            )
            return None

        latest_value, latest_currency = self._normalize_currency(latest)
        latest_point = _AssetPoint(
            value=latest_value,
            as_of=latest.end_date,
            fiscal_period=(latest.fiscal_period or "").upper(),
            currency=latest_currency,
        )

        latest_year = self._extract_year(latest.end_date)
        if latest_year is None:
            LOGGER.warning("accruals_ratio: invalid latest assets date for %s", symbol)
            return None

        prior_point: Optional[_AssetPoint] = None
        for record in quarterly[1:]:
            point_year = self._extract_year(record.end_date)
            if (
                point_year is not None
                and (record.fiscal_period or "").upper() == latest_point.fiscal_period
                and point_year == latest_year - 1
            ):
                value, currency = self._normalize_currency(record)
                prior_point = _AssetPoint(
                    value=value,
                    as_of=record.end_date,
                    fiscal_period=(record.fiscal_period or "").upper(),
                    currency=currency,
                )
                break

        if prior_point is None:
            LOGGER.warning(
                "accruals_ratio: missing same-quarter prior-year assets for %s", symbol
            )
            return None

        if not self._currencies_match(latest_point.currency, prior_point.currency):
            LOGGER.warning("accruals_ratio: assets currency mismatch for %s", symbol)
            return None

        return _AmountResult(
            total=(latest_point.value + prior_point.value) / 2.0,
            as_of=latest_point.as_of,
            currency=latest_point.currency or prior_point.currency,
        )

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
            return value / 100.0, "GBP"
        return value, code

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

    def _currencies_match(self, left: Optional[str], right: Optional[str]) -> bool:
        if left and right:
            return left == right
        return True

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
class AccrualsRatioMetric:
    """Compute accruals ratio using TTM net income/CFO over average total assets."""

    id: str = "accruals_ratio"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = AccrualsRatioCalculator().compute(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


__all__ = [
    "AccrualsRatioSnapshot",
    "AccrualsRatioCalculator",
    "AccrualsRatioMetric",
]
