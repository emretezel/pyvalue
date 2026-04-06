"""ROIC FY-series metrics implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging

from pyvalue.metrics.base import MetricResult
from pyvalue.metrics.invested_capital import (
    CASH_FALLBACK_CONCEPT,
    CASH_PRIMARY_CONCEPT,
    EQUITY_FALLBACK_CONCEPT,
    EQUITY_PRIMARY_CONCEPT,
    LONG_TERM_DEBT_CONCEPT,
    REQUIRED_CONCEPTS as INVESTED_CAPITAL_REQUIRED_CONCEPTS,
    SHORT_TERM_DEBT_CONCEPT,
    TOTAL_DEBT_CONCEPT,
)
from pyvalue.metrics.utils import MAX_FY_FACT_AGE_DAYS
from pyvalue.money import normalize_money_value
from pyvalue.storage import FactRecord, FinancialFactsRepository

LOGGER = logging.getLogger(__name__)

EBIT_CONCEPT = "OperatingIncomeLoss"
TAX_EXPENSE_CONCEPT = "IncomeTaxExpense"
PRETAX_INCOME_CONCEPT = "IncomeBeforeIncomeTaxes"

EBIT_CONCEPTS = (EBIT_CONCEPT,)
TAX_EXPENSE_CONCEPTS = (TAX_EXPENSE_CONCEPT,)
PRETAX_INCOME_CONCEPTS = (PRETAX_INCOME_CONCEPT,)
FY_PERIODS = {"FY"}

DEFAULT_TAX_RATE = 0.21
PRETAX_MIN_ABS = 1.0
ABOVE_THRESHOLD = 0.12
DEFAULT_SERIES_YEARS = 10
STRICT_7Y_YEARS = 7
IROIC_LOOKBACK_YEARS = 5
IROIC_MIN_RELATIVE_DELTA_IC = 0.01

FAILURE_MISSING_FY_EBIT_HISTORY = "missing FY EBIT history"
FAILURE_FEWER_THAN_REQUIRED_FY_EBIT_YEARS = "fewer than required FY EBIT years"
FAILURE_MISSING_CURRENT_FY_INVESTED_CAPITAL = "missing current FY invested capital"
FAILURE_MISSING_PRIOR_FY_INVESTED_CAPITAL = "missing prior FY invested capital"
FAILURE_MISSING_INVESTED_CAPITAL_DEBT_INPUT = "missing invested capital debt input"
FAILURE_MISSING_INVESTED_CAPITAL_EQUITY_INPUT = "missing invested capital equity input"
FAILURE_MISSING_INVESTED_CAPITAL_CASH_INPUT = "missing invested capital cash input"
FAILURE_CURRENCY_CONFLICT = "currency conflict"
FAILURE_ZERO_AVERAGE_INVESTED_CAPITAL = "zero average invested capital"
FAILURE_LATEST_FY_POINT_TOO_OLD = "latest FY point too old"

TAX_RATE_SOURCE_PERIOD = "period"
TAX_RATE_SOURCE_LATEST_VALID_FY = "latest_valid_fy"
TAX_RATE_SOURCE_DEFAULT_21PCT = "default_21pct"

REQUIRED_CONCEPTS = tuple(
    dict.fromkeys(
        EBIT_CONCEPTS
        + TAX_EXPENSE_CONCEPTS
        + PRETAX_INCOME_CONCEPTS
        + INVESTED_CAPITAL_REQUIRED_CONCEPTS
    )
)


@dataclass(frozen=True)
class _AmountResult:
    total: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class _TaxRateResult:
    rate: float
    as_of: Optional[str]
    source: str


@dataclass(frozen=True)
class _InvestedCapitalYearDiagnostic:
    year: int
    available: bool
    as_of: Optional[str]
    currency: Optional[str]
    failure_reason: Optional[str]


@dataclass(frozen=True)
class _ROICFYPoint:
    year: int
    value: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class ROICFYSeriesSnapshot:
    points: tuple[_ROICFYPoint, ...]
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class IncrementalROICSnapshot:
    value: float
    as_of: str
    currency: Optional[str]


@dataclass(frozen=True)
class ROICFYYearDiagnostic:
    year: int
    ebit_available: bool
    ebit_as_of: Optional[str]
    ebit_currency: Optional[str]
    tax_available: bool
    pretax_available: bool
    tax_rate: Optional[float]
    tax_rate_as_of: Optional[str]
    tax_rate_source: Optional[str]
    invested_capital_available: bool
    invested_capital_as_of: Optional[str]
    invested_capital_currency: Optional[str]
    invested_capital_failure_reason: Optional[str]
    roic_available: bool
    roic_value: Optional[float]
    roic_as_of: Optional[str]
    roic_currency: Optional[str]
    roic_failure_reason: Optional[str]


@dataclass(frozen=True)
class ROICFYSeriesDiagnostic:
    symbol: str
    window_years: int
    ebit_years: tuple[int, ...]
    invested_capital_years: tuple[int, ...]
    roic_years: tuple[int, ...]
    latest_ebit_year: Optional[int]
    latest_valid_roic_year: Optional[int]
    required_window_years: tuple[int, ...]
    missing_window_years: tuple[int, ...]
    selected_window_years: tuple[int, ...]
    selected_missing_years: tuple[int, ...]
    latest_point_is_recent: bool
    failure_reason: Optional[str]
    snapshot: Optional[ROICFYSeriesSnapshot]
    year_diagnostics: tuple[ROICFYYearDiagnostic, ...]


class ROICFYSeriesCalculator:
    """Build strict-window FY ROIC series diagnostics and snapshots."""

    def diagnose_series(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        window_years: int = DEFAULT_SERIES_YEARS,
    ) -> ROICFYSeriesDiagnostic:
        ebit_map = self._fy_map(symbol, repo, EBIT_CONCEPT)
        tax_map = self._fy_map(symbol, repo, TAX_EXPENSE_CONCEPT)
        pretax_map = self._fy_map(symbol, repo, PRETAX_INCOME_CONCEPT)
        latest_valid_tax_rate = self._latest_valid_fy_tax_rate(tax_map, pretax_map)
        ic_map, ic_diagnostics = self._fy_invested_capital_diagnostics(symbol, repo)

        roic_by_year: dict[int, _ROICFYPoint] = {}
        roic_failure_by_year: dict[int, str] = {}
        tax_rate_by_year: dict[int, _TaxRateResult] = {}
        for year, ebit in ebit_map.items():
            tax_rate = self._tax_rate_for_year(
                year=year,
                tax_map=tax_map,
                pretax_map=pretax_map,
                latest_valid_tax_rate=latest_valid_tax_rate,
            )
            tax_rate_by_year[year] = tax_rate

            ic_current = ic_map.get(year)
            if ic_current is None:
                roic_failure_by_year[year] = FAILURE_MISSING_CURRENT_FY_INVESTED_CAPITAL
                continue

            ic_previous = ic_map.get(year - 1)
            if ic_previous is None:
                roic_failure_by_year[year] = FAILURE_MISSING_PRIOR_FY_INVESTED_CAPITAL
                continue

            ic_currency = self._combine_currency(
                [ic_current.currency, ic_previous.currency]
            )
            if ic_currency is None and any(
                code is not None for code in (ic_current.currency, ic_previous.currency)
            ):
                roic_failure_by_year[year] = FAILURE_CURRENCY_CONFLICT
                continue

            avg_ic = (ic_current.total + ic_previous.total) / 2.0
            if avg_ic == 0:
                roic_failure_by_year[year] = FAILURE_ZERO_AVERAGE_INVESTED_CAPITAL
                continue

            if not self._currencies_match(ebit.currency, ic_currency):
                roic_failure_by_year[year] = FAILURE_CURRENCY_CONFLICT
                continue

            nopat = ebit.total * (1.0 - tax_rate.rate)
            as_of_values = [ebit.as_of, ic_current.as_of, ic_previous.as_of]
            if tax_rate.as_of is not None:
                as_of_values.append(tax_rate.as_of)
            roic_by_year[year] = _ROICFYPoint(
                year=year,
                value=nopat / avg_ic,
                as_of=max(as_of_values),
                currency=ebit.currency or ic_currency,
            )

        latest_ebit_year = max(ebit_map.keys()) if ebit_map else None
        latest_valid_roic_year = max(roic_by_year.keys()) if roic_by_year else None

        required_window_years = self._window_years(latest_ebit_year, window_years)
        missing_window_years = tuple(
            year for year in required_window_years if year not in roic_by_year
        )
        selected_window_years = self._window_years(latest_valid_roic_year, window_years)
        selected_missing_years = tuple(
            year for year in selected_window_years if year not in roic_by_year
        )

        snapshot: Optional[ROICFYSeriesSnapshot] = None
        latest_point_is_recent = False
        if latest_valid_roic_year is not None and not selected_missing_years:
            selected_points = tuple(
                roic_by_year[year] for year in selected_window_years
            )
            latest_point_is_recent = self._is_recent_as_of(
                selected_points[0].as_of,
                max_age_days=MAX_FY_FACT_AGE_DAYS,
            )
            if latest_point_is_recent:
                series_currency = self._combine_currency(
                    [point.currency for point in selected_points]
                )
                if series_currency is not None or not any(
                    point.currency is not None for point in selected_points
                ):
                    snapshot = ROICFYSeriesSnapshot(
                        points=selected_points,
                        as_of=selected_points[0].as_of,
                        currency=series_currency,
                    )

        all_years = (
            set(ebit_map.keys())
            | set(tax_map.keys())
            | set(pretax_map.keys())
            | set(ic_diagnostics.keys())
            | set(roic_by_year.keys())
            | set(roic_failure_by_year.keys())
            | set(required_window_years)
            | {year - 1 for year in ebit_map}
        )
        year_diagnostics = tuple(
            self._build_year_diagnostic(
                year=year,
                ebit_map=ebit_map,
                tax_map=tax_map,
                pretax_map=pretax_map,
                tax_rate_by_year=tax_rate_by_year,
                ic_diagnostics=ic_diagnostics,
                roic_by_year=roic_by_year,
                roic_failure_by_year=roic_failure_by_year,
            )
            for year in sorted(all_years, reverse=True)
        )
        failure_reason = self._determine_series_failure_reason(
            latest_ebit_year=latest_ebit_year,
            latest_valid_roic_year=latest_valid_roic_year,
            required_window_years=required_window_years,
            missing_window_years=missing_window_years,
            selected_window_years=selected_window_years,
            selected_missing_years=selected_missing_years,
            ebit_map=ebit_map,
            ic_diagnostics=ic_diagnostics,
            roic_failure_by_year=roic_failure_by_year,
            latest_point_is_recent=latest_point_is_recent,
            snapshot=snapshot,
        )
        return ROICFYSeriesDiagnostic(
            symbol=symbol,
            window_years=window_years,
            ebit_years=tuple(sorted(ebit_map.keys(), reverse=True)),
            invested_capital_years=tuple(
                sorted(
                    (
                        year
                        for year, diagnostic in ic_diagnostics.items()
                        if diagnostic.available
                    ),
                    reverse=True,
                )
            ),
            roic_years=tuple(sorted(roic_by_year.keys(), reverse=True)),
            latest_ebit_year=latest_ebit_year,
            latest_valid_roic_year=latest_valid_roic_year,
            required_window_years=required_window_years,
            missing_window_years=missing_window_years,
            selected_window_years=selected_window_years,
            selected_missing_years=selected_missing_years,
            latest_point_is_recent=latest_point_is_recent,
            failure_reason=failure_reason,
            snapshot=snapshot,
            year_diagnostics=year_diagnostics,
        )

    def compute_series(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        window_years: int = DEFAULT_SERIES_YEARS,
    ) -> Optional[ROICFYSeriesSnapshot]:
        diagnostic = self.diagnose_series(symbol, repo, window_years=window_years)
        if diagnostic.snapshot is not None:
            return diagnostic.snapshot
        failure_reason = (
            diagnostic.failure_reason or FAILURE_MISSING_CURRENT_FY_INVESTED_CAPITAL
        )
        LOGGER.warning(
            "%s: %s for %s",
            self._series_context(window_years),
            failure_reason,
            symbol,
        )
        return None

    def compute_incremental_5y(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[IncrementalROICSnapshot]:
        ebit_map = self._fy_map(symbol, repo, EBIT_CONCEPT)
        if not ebit_map:
            LOGGER.warning("iroic_5y: missing FY EBIT history for %s", symbol)
            return None

        ic_map, _ = self._fy_invested_capital_diagnostics(symbol, repo)
        if not ic_map:
            LOGGER.warning(
                "iroic_5y: missing FY invested capital history for %s", symbol
            )
            return None

        latest_year = self._latest_incremental_year(ebit_map, ic_map)
        if latest_year is None:
            LOGGER.warning("iroic_5y: missing strict t and t-5 FY pair for %s", symbol)
            return None

        prior_year = latest_year - IROIC_LOOKBACK_YEARS
        latest_ebit = ebit_map[latest_year]
        prior_ebit = ebit_map[prior_year]
        latest_ic = ic_map[latest_year]
        prior_ic = ic_map[prior_year]

        latest_pair_as_of = max(latest_ebit.as_of, latest_ic.as_of)
        if not self._is_recent_as_of(
            latest_pair_as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning("iroic_5y: latest FY point too old for %s", symbol)
            return None

        pair_currency = self._combine_currency(
            [
                latest_ebit.currency,
                prior_ebit.currency,
                latest_ic.currency,
                prior_ic.currency,
            ]
        )
        if pair_currency is None and any(
            code is not None
            for code in (
                latest_ebit.currency,
                prior_ebit.currency,
                latest_ic.currency,
                prior_ic.currency,
            )
        ):
            LOGGER.warning("iroic_5y: currency mismatch across FY pair for %s", symbol)
            return None

        tax_map = self._fy_map(symbol, repo, TAX_EXPENSE_CONCEPT)
        pretax_map = self._fy_map(symbol, repo, PRETAX_INCOME_CONCEPT)
        latest_valid_tax_rate = self._latest_valid_fy_tax_rate(tax_map, pretax_map)
        latest_tax_rate = self._tax_rate_for_year(
            year=latest_year,
            tax_map=tax_map,
            pretax_map=pretax_map,
            latest_valid_tax_rate=latest_valid_tax_rate,
        )
        prior_tax_rate = self._tax_rate_for_year(
            year=prior_year,
            tax_map=tax_map,
            pretax_map=pretax_map,
            latest_valid_tax_rate=latest_valid_tax_rate,
        )

        latest_nopat = latest_ebit.total * (1.0 - latest_tax_rate.rate)
        prior_nopat = prior_ebit.total * (1.0 - prior_tax_rate.rate)
        delta_nopat = latest_nopat - prior_nopat

        delta_ic = latest_ic.total - prior_ic.total
        if delta_ic <= 0:
            LOGGER.warning(
                "iroic_5y: non-positive delta invested capital for %s", symbol
            )
            return None

        ic_scale = max(abs(latest_ic.total), abs(prior_ic.total), 1.0)
        relative_delta_ic = abs(delta_ic) / ic_scale
        if relative_delta_ic < IROIC_MIN_RELATIVE_DELTA_IC:
            LOGGER.warning("iroic_5y: tiny delta invested capital for %s", symbol)
            return None

        as_of_values = [latest_ebit.as_of, latest_ic.as_of]
        if latest_tax_rate.as_of is not None:
            as_of_values.append(latest_tax_rate.as_of)

        return IncrementalROICSnapshot(
            value=delta_nopat / delta_ic,
            as_of=max(as_of_values),
            currency=pair_currency,
        )

    def _build_year_diagnostic(
        self,
        *,
        year: int,
        ebit_map: dict[int, _AmountResult],
        tax_map: dict[int, _AmountResult],
        pretax_map: dict[int, _AmountResult],
        tax_rate_by_year: dict[int, _TaxRateResult],
        ic_diagnostics: dict[int, _InvestedCapitalYearDiagnostic],
        roic_by_year: dict[int, _ROICFYPoint],
        roic_failure_by_year: dict[int, str],
    ) -> ROICFYYearDiagnostic:
        ebit = ebit_map.get(year)
        tax = tax_map.get(year)
        pretax = pretax_map.get(year)
        tax_rate = tax_rate_by_year.get(year)
        ic_diagnostic = ic_diagnostics.get(
            year,
            _InvestedCapitalYearDiagnostic(
                year=year,
                available=False,
                as_of=None,
                currency=None,
                failure_reason=None,
            ),
        )
        roic_point = roic_by_year.get(year)
        return ROICFYYearDiagnostic(
            year=year,
            ebit_available=ebit is not None,
            ebit_as_of=ebit.as_of if ebit else None,
            ebit_currency=ebit.currency if ebit else None,
            tax_available=tax is not None,
            pretax_available=pretax is not None,
            tax_rate=tax_rate.rate if tax_rate else None,
            tax_rate_as_of=tax_rate.as_of if tax_rate else None,
            tax_rate_source=tax_rate.source if tax_rate else None,
            invested_capital_available=ic_diagnostic.available,
            invested_capital_as_of=ic_diagnostic.as_of,
            invested_capital_currency=ic_diagnostic.currency,
            invested_capital_failure_reason=ic_diagnostic.failure_reason,
            roic_available=roic_point is not None,
            roic_value=roic_point.value if roic_point else None,
            roic_as_of=roic_point.as_of if roic_point else None,
            roic_currency=roic_point.currency if roic_point else None,
            roic_failure_reason=roic_failure_by_year.get(year),
        )

    def _determine_series_failure_reason(
        self,
        *,
        latest_ebit_year: Optional[int],
        latest_valid_roic_year: Optional[int],
        required_window_years: tuple[int, ...],
        missing_window_years: tuple[int, ...],
        selected_window_years: tuple[int, ...],
        selected_missing_years: tuple[int, ...],
        ebit_map: dict[int, _AmountResult],
        ic_diagnostics: dict[int, _InvestedCapitalYearDiagnostic],
        roic_failure_by_year: dict[int, str],
        latest_point_is_recent: bool,
        snapshot: Optional[ROICFYSeriesSnapshot],
    ) -> Optional[str]:
        if snapshot is not None:
            return None
        if latest_ebit_year is None:
            return FAILURE_MISSING_FY_EBIT_HISTORY

        missing_ebit_years = tuple(
            year for year in required_window_years if year not in ebit_map
        )
        if missing_ebit_years:
            return FAILURE_FEWER_THAN_REQUIRED_FY_EBIT_YEARS

        if missing_window_years:
            for year in missing_window_years:
                reason = self._specific_roic_failure_reason(
                    year=year,
                    ic_diagnostics=ic_diagnostics,
                    roic_failure_by_year=roic_failure_by_year,
                )
                if reason is not None:
                    return reason

        if latest_valid_roic_year is None:
            return (
                self._specific_roic_failure_reason(
                    year=latest_ebit_year,
                    ic_diagnostics=ic_diagnostics,
                    roic_failure_by_year=roic_failure_by_year,
                )
                or FAILURE_MISSING_CURRENT_FY_INVESTED_CAPITAL
            )

        if selected_window_years and selected_missing_years:
            for year in selected_missing_years:
                reason = self._specific_roic_failure_reason(
                    year=year,
                    ic_diagnostics=ic_diagnostics,
                    roic_failure_by_year=roic_failure_by_year,
                )
                if reason is not None:
                    return reason

        if selected_window_years and not latest_point_is_recent:
            return FAILURE_LATEST_FY_POINT_TOO_OLD

        return FAILURE_CURRENCY_CONFLICT

    def _specific_roic_failure_reason(
        self,
        *,
        year: int,
        ic_diagnostics: dict[int, _InvestedCapitalYearDiagnostic],
        roic_failure_by_year: dict[int, str],
    ) -> Optional[str]:
        roic_failure = roic_failure_by_year.get(year)
        if roic_failure == FAILURE_MISSING_CURRENT_FY_INVESTED_CAPITAL:
            ic_reason = ic_diagnostics.get(year)
            return self._specific_invested_capital_reason(ic_reason) or roic_failure
        if roic_failure == FAILURE_MISSING_PRIOR_FY_INVESTED_CAPITAL:
            ic_reason = ic_diagnostics.get(year - 1)
            return self._specific_invested_capital_reason(ic_reason) or roic_failure
        return roic_failure

    def _specific_invested_capital_reason(
        self, diagnostic: Optional[_InvestedCapitalYearDiagnostic]
    ) -> Optional[str]:
        if diagnostic is None:
            return None
        if diagnostic.failure_reason in {
            FAILURE_MISSING_INVESTED_CAPITAL_DEBT_INPUT,
            FAILURE_MISSING_INVESTED_CAPITAL_EQUITY_INPUT,
            FAILURE_MISSING_INVESTED_CAPITAL_CASH_INPUT,
            FAILURE_CURRENCY_CONFLICT,
        }:
            return diagnostic.failure_reason
        return None

    def _fy_invested_capital_diagnostics(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> tuple[dict[int, _AmountResult], dict[int, _InvestedCapitalYearDiagnostic]]:
        short_map = self._fy_period_map(
            repo.facts_for_concept(symbol, SHORT_TERM_DEBT_CONCEPT)
        )
        long_map = self._fy_period_map(
            repo.facts_for_concept(symbol, LONG_TERM_DEBT_CONCEPT)
        )
        total_map = self._fy_period_map(
            repo.facts_for_concept(symbol, TOTAL_DEBT_CONCEPT)
        )
        equity_map = self._fy_period_map(
            repo.facts_for_concept(symbol, EQUITY_PRIMARY_CONCEPT)
        )
        common_equity_map = self._fy_period_map(
            repo.facts_for_concept(symbol, EQUITY_FALLBACK_CONCEPT)
        )
        cash_primary_map = self._fy_period_map(
            repo.facts_for_concept(symbol, CASH_PRIMARY_CONCEPT)
        )
        cash_fallback_map = self._fy_period_map(
            repo.facts_for_concept(symbol, CASH_FALLBACK_CONCEPT)
        )

        candidate_keys = sorted(
            set(short_map.keys())
            | set(long_map.keys())
            | set(total_map.keys())
            | set(equity_map.keys())
            | set(common_equity_map.keys())
            | set(cash_primary_map.keys())
            | set(cash_fallback_map.keys()),
            key=lambda item: (item[0], item[1]),
            reverse=True,
        )

        ic_map: dict[int, _AmountResult] = {}
        failure_by_year: dict[int, _InvestedCapitalYearDiagnostic] = {}
        for key in candidate_keys:
            year = self._extract_year(key[0])
            if year is None or year in ic_map:
                continue

            debt, debt_failure = self._resolve_invested_capital_debt(
                short_debt=short_map.get(key),
                long_debt=long_map.get(key),
                total_debt=total_map.get(key),
            )
            if debt is None:
                failure_by_year.setdefault(
                    year,
                    _InvestedCapitalYearDiagnostic(
                        year=year,
                        available=False,
                        as_of=key[0],
                        currency=None,
                        failure_reason=debt_failure,
                    ),
                )
                continue

            equity, equity_failure = self._resolve_invested_capital_single_amount(
                primary=equity_map.get(key),
                fallback=common_equity_map.get(key),
                missing_failure=FAILURE_MISSING_INVESTED_CAPITAL_EQUITY_INPUT,
            )
            if equity is None:
                failure_by_year.setdefault(
                    year,
                    _InvestedCapitalYearDiagnostic(
                        year=year,
                        available=False,
                        as_of=key[0],
                        currency=None,
                        failure_reason=equity_failure,
                    ),
                )
                continue

            cash, cash_failure = self._resolve_invested_capital_single_amount(
                primary=cash_primary_map.get(key),
                fallback=cash_fallback_map.get(key),
                missing_failure=FAILURE_MISSING_INVESTED_CAPITAL_CASH_INPUT,
            )
            if cash is None:
                failure_by_year.setdefault(
                    year,
                    _InvestedCapitalYearDiagnostic(
                        year=year,
                        available=False,
                        as_of=key[0],
                        currency=None,
                        failure_reason=cash_failure,
                    ),
                )
                continue

            currency = self._combine_currency(
                [debt.currency, equity.currency, cash.currency]
            )
            if currency is None and any(
                code is not None
                for code in (debt.currency, equity.currency, cash.currency)
            ):
                failure_by_year.setdefault(
                    year,
                    _InvestedCapitalYearDiagnostic(
                        year=year,
                        available=False,
                        as_of=max(debt.as_of, equity.as_of, cash.as_of),
                        currency=None,
                        failure_reason=FAILURE_CURRENCY_CONFLICT,
                    ),
                )
                continue

            point = _AmountResult(
                total=debt.total + equity.total - cash.total,
                as_of=max(debt.as_of, equity.as_of, cash.as_of),
                currency=currency,
            )
            ic_map[year] = point
            failure_by_year[year] = _InvestedCapitalYearDiagnostic(
                year=year,
                available=True,
                as_of=point.as_of,
                currency=point.currency,
                failure_reason=None,
            )

        return ic_map, failure_by_year

    def _resolve_invested_capital_debt(
        self,
        *,
        short_debt: Optional[FactRecord],
        long_debt: Optional[FactRecord],
        total_debt: Optional[FactRecord],
    ) -> tuple[Optional[_AmountResult], Optional[str]]:
        if short_debt is not None and long_debt is not None:
            short_value, short_currency = self._normalize_currency(short_debt)
            long_value, long_currency = self._normalize_currency(long_debt)
            currency = self._combine_currency([short_currency, long_currency])
            if currency is None and any(
                code is not None for code in (short_currency, long_currency)
            ):
                return None, FAILURE_CURRENCY_CONFLICT
            return (
                _AmountResult(
                    total=short_value + long_value,
                    as_of=max(short_debt.end_date, long_debt.end_date),
                    currency=currency,
                ),
                None,
            )

        if total_debt is not None:
            total_value, total_currency = self._normalize_currency(total_debt)
            return (
                _AmountResult(
                    total=total_value,
                    as_of=total_debt.end_date,
                    currency=total_currency,
                ),
                None,
            )

        one_side = short_debt or long_debt
        if one_side is None:
            return None, FAILURE_MISSING_INVESTED_CAPITAL_DEBT_INPUT

        value, currency = self._normalize_currency(one_side)
        return (
            _AmountResult(total=value, as_of=one_side.end_date, currency=currency),
            None,
        )

    def _resolve_invested_capital_single_amount(
        self,
        *,
        primary: Optional[FactRecord],
        fallback: Optional[FactRecord],
        missing_failure: str,
    ) -> tuple[Optional[_AmountResult], Optional[str]]:
        record = primary or fallback
        if record is None:
            return None, missing_failure
        value, currency = self._normalize_currency(record)
        return (
            _AmountResult(total=value, as_of=record.end_date, currency=currency),
            None,
        )

    def _fy_period_map(
        self, records: Sequence[FactRecord]
    ) -> dict[tuple[str, str], FactRecord]:
        mapped: dict[tuple[str, str], FactRecord] = {}
        for record in sorted(records, key=lambda item: item.end_date, reverse=True):
            period = (record.fiscal_period or "").upper()
            if period not in FY_PERIODS:
                continue
            if record.value is None:
                continue
            key = (record.end_date, period)
            if key not in mapped:
                mapped[key] = record
        return mapped

    def _fy_map(
        self,
        symbol: str,
        repo: FinancialFactsRepository,
        concept: str,
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

    def _latest_valid_fy_tax_rate(
        self,
        tax_map: dict[int, _AmountResult],
        pretax_map: dict[int, _AmountResult],
    ) -> Optional[_TaxRateResult]:
        for year in sorted(set(tax_map).intersection(pretax_map), reverse=True):
            rate = self._rate_from_amounts(tax_map[year], pretax_map[year])
            if rate is not None:
                return _TaxRateResult(
                    rate=rate.rate,
                    as_of=rate.as_of,
                    source=TAX_RATE_SOURCE_LATEST_VALID_FY,
                )
        return None

    def _tax_rate_for_year(
        self,
        *,
        year: int,
        tax_map: dict[int, _AmountResult],
        pretax_map: dict[int, _AmountResult],
        latest_valid_tax_rate: Optional[_TaxRateResult],
    ) -> _TaxRateResult:
        tax = tax_map.get(year)
        pretax = pretax_map.get(year)
        period_rate = self._rate_from_amounts(tax, pretax)
        if period_rate is not None:
            return period_rate
        if latest_valid_tax_rate is not None:
            return latest_valid_tax_rate
        return _TaxRateResult(
            rate=DEFAULT_TAX_RATE,
            as_of=None,
            source=TAX_RATE_SOURCE_DEFAULT_21PCT,
        )

    def _latest_incremental_year(
        self,
        ebit_map: dict[int, _AmountResult],
        ic_map: dict[int, _AmountResult],
    ) -> Optional[int]:
        for year in sorted(set(ebit_map).intersection(ic_map), reverse=True):
            prior_year = year - IROIC_LOOKBACK_YEARS
            if prior_year in ebit_map and prior_year in ic_map:
                return year
        return None

    def _rate_from_amounts(
        self,
        tax: Optional[_AmountResult],
        pretax: Optional[_AmountResult],
    ) -> Optional[_TaxRateResult]:
        if tax is None or pretax is None:
            return None
        if not self._currencies_match(tax.currency, pretax.currency):
            return None
        if pretax.total <= PRETAX_MIN_ABS:
            return None
        rate = tax.total / pretax.total
        if rate < 0 or rate > 1:
            return None
        return _TaxRateResult(
            rate=rate,
            as_of=max(tax.as_of, pretax.as_of),
            source=TAX_RATE_SOURCE_PERIOD,
        )

    def _window_years(
        self, latest_year: Optional[int], window_years: int
    ) -> tuple[int, ...]:
        if latest_year is None:
            return ()
        return tuple(range(latest_year, latest_year - window_years, -1))

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

    def _normalize_currency(self, record: FactRecord) -> tuple[float, Optional[str]]:
        normalized_value, normalized_currency = normalize_money_value(
            record.value,
            record.currency,
        )
        return (
            record.value if normalized_value is None else normalized_value,
            normalized_currency,
        )

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

    def _series_context(self, window_years: int) -> str:
        return f"roic_{window_years}y"


@dataclass
class ROIC10YMedianMetric:
    """Compute median FY ROIC over the latest strict 10-year series."""

    id: str = "roic_10y_median"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(
            symbol, repo, window_years=DEFAULT_SERIES_YEARS
        )
        if snapshot is None:
            return None
        values = sorted(point.value for point in snapshot.points)
        midpoint = len(values) // 2
        median = (values[midpoint - 1] + values[midpoint]) / 2.0
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=median,
            as_of=snapshot.as_of,
        )


@dataclass
class ROIC7YMedianMetric:
    """Compute median FY ROIC over the latest strict 7-year series."""

    id: str = "roic_7y_median"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(
            symbol, repo, window_years=STRICT_7Y_YEARS
        )
        if snapshot is None:
            return None
        values = sorted(point.value for point in snapshot.points)
        median = values[len(values) // 2]
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=median,
            as_of=snapshot.as_of,
        )


@dataclass
class ROICYearsAbove12PctMetric:
    """Count FY ROIC years above 12% over latest strict 10 consecutive years."""

    id: str = "roic_years_above_12pct"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(
            symbol, repo, window_years=DEFAULT_SERIES_YEARS
        )
        if snapshot is None:
            return None
        count = sum(1 for point in snapshot.points if point.value > ABOVE_THRESHOLD)
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=float(count),
            as_of=snapshot.as_of,
        )


@dataclass
class ROIC10YMinMetric:
    """Compute minimum FY ROIC over the latest strict 10-year series."""

    id: str = "roic_10y_min"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(
            symbol, repo, window_years=DEFAULT_SERIES_YEARS
        )
        if snapshot is None:
            return None
        minimum = min(point.value for point in snapshot.points)
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=minimum,
            as_of=snapshot.as_of,
        )


@dataclass
class ROIC7YMinMetric:
    """Compute minimum FY ROIC over the latest strict 7-year series."""

    id: str = "roic_7y_min"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(
            symbol, repo, window_years=STRICT_7Y_YEARS
        )
        if snapshot is None:
            return None
        minimum = min(point.value for point in snapshot.points)
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=minimum,
            as_of=snapshot.as_of,
        )


@dataclass
class IncrementalROICFiveYearMetric:
    """Compute incremental ROIC using FY t versus strict FY t-5."""

    id: str = "iroic_5y"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, symbol: str, repo: FinancialFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_incremental_5y(symbol, repo)
        if snapshot is None:
            return None
        return MetricResult(
            symbol=symbol,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


__all__ = [
    "ROICFYYearDiagnostic",
    "ROICFYSeriesDiagnostic",
    "ROICFYSeriesSnapshot",
    "IncrementalROICSnapshot",
    "ROICFYSeriesCalculator",
    "ROIC10YMedianMetric",
    "ROIC7YMedianMetric",
    "ROICYearsAbove12PctMetric",
    "ROIC10YMinMetric",
    "ROIC7YMinMetric",
    "IncrementalROICFiveYearMetric",
]
