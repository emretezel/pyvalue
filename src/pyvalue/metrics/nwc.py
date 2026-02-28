"""Net working capital and delta metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, MAX_FY_FACT_AGE_DAYS
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

ASSETS_CURRENT_CONCEPT = "AssetsCurrent"
LIABILITIES_CURRENT_CONCEPT = "LiabilitiesCurrent"
CASH_PRIMARY_CONCEPT = "CashAndShortTermInvestments"
CASH_EQUIVALENTS_CONCEPT = "CashAndCashEquivalents"
SHORT_TERM_INVESTMENTS_CONCEPT = "ShortTermInvestments"
SHORT_TERM_DEBT_CONCEPT = "ShortTermDebt"
REQUIRED_CONCEPTS = (
    ASSETS_CURRENT_CONCEPT,
    LIABILITIES_CURRENT_CONCEPT,
    CASH_PRIMARY_CONCEPT,
    CASH_EQUIVALENTS_CONCEPT,
    SHORT_TERM_INVESTMENTS_CONCEPT,
    SHORT_TERM_DEBT_CONCEPT,
)
QUARTERLY_PERIODS = {"Q1", "Q2", "Q3", "Q4"}
FY_PERIODS = {"FY"}


@dataclass
class _NWCPoint:
    value: float
    as_of: str
    fiscal_period: str
    currency: Optional[str]


class _NWCBase:
    def _build_points(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        periods: set[str],
    ) -> list[_NWCPoint]:
        assets_map = self._period_map(
            repo.facts_for_concept(symbol, ASSETS_CURRENT_CONCEPT), periods
        )
        liabilities_map = self._period_map(
            repo.facts_for_concept(symbol, LIABILITIES_CURRENT_CONCEPT), periods
        )
        cash_primary_map = self._period_map(
            repo.facts_for_concept(symbol, CASH_PRIMARY_CONCEPT), periods
        )
        cash_eq_map = self._period_map(
            repo.facts_for_concept(symbol, CASH_EQUIVALENTS_CONCEPT), periods
        )
        short_term_investments_map = self._period_map(
            repo.facts_for_concept(symbol, SHORT_TERM_INVESTMENTS_CONCEPT), periods
        )
        short_term_debt_map = self._period_map(
            repo.facts_for_concept(symbol, SHORT_TERM_DEBT_CONCEPT), periods
        )

        candidate_keys = sorted(
            set(assets_map.keys()).intersection(liabilities_map.keys()),
            key=lambda item: (item[0], item[1]),
            reverse=True,
        )
        points: list[_NWCPoint] = []
        for key in candidate_keys:
            point = self._compute_point_for_key(
                symbol=symbol,
                key=key,
                assets=assets_map.get(key),
                liabilities=liabilities_map.get(key),
                cash_primary=cash_primary_map.get(key),
                cash_equivalents=cash_eq_map.get(key),
                short_term_investments=short_term_investments_map.get(key),
                short_term_debt=short_term_debt_map.get(key),
            )
            if point is not None:
                points.append(point)
        return points

    def _compute_point_for_key(
        self,
        *,
        symbol: str,
        key: tuple[str, str],
        assets: Optional[FactRecord],
        liabilities: Optional[FactRecord],
        cash_primary: Optional[FactRecord],
        cash_equivalents: Optional[FactRecord],
        short_term_investments: Optional[FactRecord],
        short_term_debt: Optional[FactRecord],
    ) -> Optional[_NWCPoint]:
        if assets is None or liabilities is None:
            return None

        assets_value, assets_currency = self._normalize_currency(assets)
        liabilities_value, liabilities_currency = self._normalize_currency(liabilities)

        cash_amount = self._cash_amount(
            symbol=symbol,
            key=key,
            cash_primary=cash_primary,
            cash_equivalents=cash_equivalents,
            short_term_investments=short_term_investments,
        )
        if cash_amount is None:
            return None
        cash_value, cash_currency = cash_amount

        short_term_debt_value = 0.0
        short_term_debt_currency = None
        if short_term_debt is not None:
            short_term_debt_value, short_term_debt_currency = self._normalize_currency(
                short_term_debt
            )

        currency = self._merge_currency(
            [
                assets_currency,
                liabilities_currency,
                cash_currency,
                short_term_debt_currency,
            ]
        )
        if currency is None and any(
            code is not None
            for code in (
                assets_currency,
                liabilities_currency,
                cash_currency,
                short_term_debt_currency,
            )
        ):
            LOGGER.warning(
                "nwc: currency mismatch for %s on %s/%s",
                symbol,
                key[0],
                key[1],
            )
            return None

        adjusted_liabilities = max(liabilities_value - short_term_debt_value, 0.0)
        nwc_value = (assets_value - cash_value) - adjusted_liabilities
        return _NWCPoint(
            value=nwc_value,
            as_of=key[0],
            fiscal_period=key[1],
            currency=currency,
        )

    def _cash_amount(
        self,
        *,
        symbol: str,
        key: tuple[str, str],
        cash_primary: Optional[FactRecord],
        cash_equivalents: Optional[FactRecord],
        short_term_investments: Optional[FactRecord],
    ) -> Optional[tuple[float, Optional[str]]]:
        if cash_primary is not None:
            return self._normalize_currency(cash_primary)

        if cash_equivalents is None and short_term_investments is None:
            LOGGER.warning(
                "nwc: missing cash inputs for %s on %s/%s", symbol, key[0], key[1]
            )
            return None

        cash_eq_value = 0.0
        cash_eq_currency = None
        if cash_equivalents is not None:
            cash_eq_value, cash_eq_currency = self._normalize_currency(cash_equivalents)

        short_term_investments_value = 0.0
        short_term_investments_currency = None
        if short_term_investments is not None:
            (
                short_term_investments_value,
                short_term_investments_currency,
            ) = self._normalize_currency(short_term_investments)

        currency = self._merge_currency(
            [cash_eq_currency, short_term_investments_currency]
        )
        if currency is None and any(
            code is not None
            for code in (cash_eq_currency, short_term_investments_currency)
        ):
            LOGGER.warning(
                "nwc: cash fallback currency mismatch for %s on %s/%s",
                symbol,
                key[0],
                key[1],
            )
            return None

        return cash_eq_value + short_term_investments_value, currency

    def _period_map(
        self, records: Sequence[FactRecord], periods: set[str]
    ) -> dict[tuple[str, str], FactRecord]:
        mapped: dict[tuple[str, str], FactRecord] = {}
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in periods:
                continue
            if record.value is None:
                continue
            key = (record.end_date, period)
            if key not in mapped:
                mapped[key] = record
        return mapped

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        value = record.value
        code = record.currency
        if code in {"GBX", "GBP0.01"}:
            return value / 100.0, "GBP"
        return value, code

    def _merge_currency(self, codes: Sequence[Optional[str]]) -> Optional[str]:
        merged = None
        for code in codes:
            if not code:
                continue
            if merged is None:
                merged = code
            elif merged != code:
                return None
        return merged

    def _extract_year(self, value: str) -> Optional[int]:
        if len(value) < 4:
            return None
        year = value[:4]
        if not year.isdigit():
            return None
        return int(year)

    def _is_recent_as_of(self, as_of: str, *, max_age_days: int) -> bool:
        try:
            end_date = date.fromisoformat(as_of)
        except ValueError:
            return False
        return end_date >= (date.today() - timedelta(days=max_age_days))

    def _select_latest_point(
        self,
        points: Sequence[_NWCPoint],
        *,
        max_age_days: int,
        context: str,
        symbol: str,
    ) -> Optional[_NWCPoint]:
        if not points:
            LOGGER.warning("%s: missing NWC points for %s", context, symbol)
            return None
        latest = points[0]
        if not self._is_recent_as_of(latest.as_of, max_age_days=max_age_days):
            LOGGER.warning(
                "%s: latest point (%s) too old for %s", context, latest.as_of, symbol
            )
            return None
        return latest


@dataclass
class NWCMostRecentQuarterMetric(_NWCBase):
    """Compute NWC for the most recent quarter (EODHD-oriented)."""

    id: str = "nwc_mqr"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_points(symbol, repo, QUARTERLY_PERIODS)
        latest = self._select_latest_point(
            points, max_age_days=MAX_FACT_AGE_DAYS, context="nwc_mqr", symbol=symbol
        )
        if latest is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=latest.value, as_of=latest.as_of
        )


@dataclass
class NWCFYMetric(_NWCBase):
    """Compute NWC for latest fiscal year end (EODHD-oriented)."""

    id: str = "nwc_fy"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_points(symbol, repo, FY_PERIODS)
        latest = self._select_latest_point(
            points, max_age_days=MAX_FY_FACT_AGE_DAYS, context="nwc_fy", symbol=symbol
        )
        if latest is None:
            return None
        return MetricResult(
            symbol=symbol, metric_id=self.id, value=latest.value, as_of=latest.as_of
        )


@dataclass
class DeltaNWCTTMMetric(_NWCBase):
    """Compute quarter-over-quarter-year delta in NWC (EODHD-oriented)."""

    id: str = "delta_nwc_ttm"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_points(symbol, repo, QUARTERLY_PERIODS)
        latest = self._select_latest_point(
            points,
            max_age_days=MAX_FACT_AGE_DAYS,
            context="delta_nwc_ttm",
            symbol=symbol,
        )
        if latest is None:
            return None
        latest_year = self._extract_year(latest.as_of)
        if latest_year is None:
            LOGGER.warning("delta_nwc_ttm: invalid latest quarter date for %s", symbol)
            return None

        prior: Optional[_NWCPoint] = None
        for point in points[1:]:
            point_year = self._extract_year(point.as_of)
            if (
                point_year is not None
                and point.fiscal_period == latest.fiscal_period
                and point_year == latest_year - 1
            ):
                prior = point
                break
        if prior is None:
            LOGGER.warning(
                "delta_nwc_ttm: missing prior-year %s for %s",
                latest.fiscal_period,
                symbol,
            )
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=latest.value - prior.value,
            as_of=latest.as_of,
        )


@dataclass
class DeltaNWCFYMetric(_NWCBase):
    """Compute year-over-year fiscal-year NWC delta (EODHD-oriented)."""

    id: str = "delta_nwc_fy"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_points(symbol, repo, FY_PERIODS)
        latest = self._select_latest_point(
            points,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            context="delta_nwc_fy",
            symbol=symbol,
        )
        if latest is None:
            return None
        latest_year = self._extract_year(latest.as_of)
        if latest_year is None:
            LOGGER.warning("delta_nwc_fy: invalid latest FY date for %s", symbol)
            return None

        prior: Optional[_NWCPoint] = None
        for point in points[1:]:
            point_year = self._extract_year(point.as_of)
            if point_year is not None and point_year == latest_year - 1:
                prior = point
                break
        if prior is None:
            LOGGER.warning("delta_nwc_fy: missing strict prior FY for %s", symbol)
            return None

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=latest.value - prior.value,
            as_of=latest.as_of,
        )


@dataclass
class DeltaNWCMaintMetric(_NWCBase):
    """Compute maintenance delta NWC as max(avg(last 3 FY deltas), 0)."""

    id: str = "delta_nwc_maint"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        points = self._build_points(symbol, repo, FY_PERIODS)
        latest = self._select_latest_point(
            points,
            max_age_days=MAX_FY_FACT_AGE_DAYS,
            context="delta_nwc_maint",
            symbol=symbol,
        )
        if latest is None:
            return None
        latest_year = self._extract_year(latest.as_of)
        if latest_year is None:
            LOGGER.warning("delta_nwc_maint: invalid latest FY date for %s", symbol)
            return None

        by_year: dict[int, _NWCPoint] = {}
        for point in points:
            year = self._extract_year(point.as_of)
            if year is None:
                continue
            if year not in by_year:
                by_year[year] = point

        required_years = [
            latest_year,
            latest_year - 1,
            latest_year - 2,
            latest_year - 3,
        ]
        if not all(year in by_year for year in required_years):
            LOGGER.warning(
                "delta_nwc_maint: need 4 consecutive FY NWC points for %s",
                symbol,
            )
            return None

        delta_latest = by_year[latest_year].value - by_year[latest_year - 1].value
        delta_prev_1 = by_year[latest_year - 1].value - by_year[latest_year - 2].value
        delta_prev_2 = by_year[latest_year - 2].value - by_year[latest_year - 3].value
        average_delta = (delta_latest + delta_prev_1 + delta_prev_2) / 3.0

        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=max(average_delta, 0.0),
            as_of=latest.as_of,
        )


__all__ = [
    "NWCMostRecentQuarterMetric",
    "NWCFYMetric",
    "DeltaNWCTTMMetric",
    "DeltaNWCFYMetric",
    "DeltaNWCMaintMetric",
]
