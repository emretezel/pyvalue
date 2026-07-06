"""ROIC FY-series metrics implementation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Sequence

import logging
import statistics

from pyvalue.facts import MonetaryFact, RegionFactsRepository
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
from pyvalue.metrics.utils import (
    MAX_FY_FACT_AGE_DAYS,
    latest_consecutive_year_chain,
    require_metric_money,
    require_metric_ticker_currency,
)
from pyvalue.money import Money

LOGGER = logging.getLogger(__name__)

_METRIC_ID = "roic_fy_series"

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
# Floor for the adaptive median chain: six valid ROIC points, which under the
# history-boundary IC convention correspond to exactly six FY years of
# fundamentals — deliberately equal to the six-year chain floors of the DVG
# screen's other adaptive gates (``cash_conversion.MIN_CHAIN_YEARS`` /
# ``fundamental_consistency.MIN_CHAIN_YEARS``).
ADAPTIVE_MIN_POINTS = 6
IROIC_LOOKBACK_YEARS = 5
IROIC_MIN_RELATIVE_DELTA_IC = 0.01
# Documented cap emitted when incremental ROIC is economically unbounded:
# NOPAT grew over the 5-year lookback while invested capital shrank or moved
# less than the materiality floor above -- the capital-light growth shape
# (buyback-heavy compounders, asset-lightening businesses). DeltaNOPAT /
# DeltaIC has no meaningful magnitude there (near-zero or negative
# denominator), yet the economics are exactly what a reinvestment gate wants
# to reward, so NA would exclude the best capital allocators (~6k listings
# hit the non-positive-DeltaIC reason in the 2026-07 audit). 1.0 (100%) sits
# above every screen threshold in use and is a *convention*, not a
# measurement -- see docs/reference/metrics.md. Shrinking NOPAT on flat or
# shrinking capital stays NA: there is nothing rewardable to measure.
IROIC_CAP: float = 1.0

FAILURE_MISSING_FY_EBIT_HISTORY = "missing FY EBIT history"
FAILURE_FEWER_THAN_REQUIRED_FY_EBIT_YEARS = "fewer than required FY EBIT years"
FAILURE_MISSING_CURRENT_FY_INVESTED_CAPITAL = "missing current FY invested capital"
FAILURE_MISSING_PRIOR_FY_INVESTED_CAPITAL = "missing prior FY invested capital"
FAILURE_MISSING_INVESTED_CAPITAL_DEBT_INPUT = "missing invested capital debt input"
FAILURE_MISSING_INVESTED_CAPITAL_EQUITY_INPUT = "missing invested capital equity input"
FAILURE_MISSING_INVESTED_CAPITAL_CASH_INPUT = "missing invested capital cash input"
FAILURE_CURRENCY_CONFLICT = "currency conflict"
FAILURE_NON_POSITIVE_AVERAGE_INVESTED_CAPITAL = "non-positive average invested capital"
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
    money: Money
    as_of: str


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
    listing_id: int
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


@dataclass(frozen=True)
class _ROICYearComputation:
    """Output of the shared per-year ROIC pass.

    One construction feeds two selectors: the strict-window diagnostics
    (``diagnose_series``) and the adaptive-chain median
    (``compute_adaptive_series``), so a valid ROIC year means exactly the
    same thing in both.
    """

    ebit_map: dict[int, _AmountResult]
    tax_map: dict[int, _AmountResult]
    pretax_map: dict[int, _AmountResult]
    tax_rate_by_year: dict[int, _TaxRateResult]
    ic_diagnostics: dict[int, _InvestedCapitalYearDiagnostic]
    roic_by_year: dict[int, _ROICFYPoint]
    roic_failure_by_year: dict[int, str]


class ROICFYSeriesCalculator:
    """Build strict-window FY ROIC series diagnostics and snapshots.

    Every monetary input is aligned to the listing currency through the shared
    Money seam (:func:`require_metric_money`), which *raises* a structured
    invariant error on a currency it cannot reconcile. So by the time amounts
    reach the per-year math here they are all one currency by construction --
    there is no per-year/per-component currency reconciliation to do, and a
    genuine conflict surfaces as an unavailable metric (the wrapped ``compute``
    turns the raise into ``None``) rather than a silently dropped year.
    """

    def _compute_roic_years(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> _ROICYearComputation:
        """Run the shared per-year ROIC pass (no window selection)."""

        ebit_map = self._fy_map(listing_id, repo, EBIT_CONCEPT)
        tax_map = self._fy_map(listing_id, repo, TAX_EXPENSE_CONCEPT)
        pretax_map = self._fy_map(listing_id, repo, PRETAX_INCOME_CONCEPT)
        latest_valid_tax_rate = self._latest_valid_fy_tax_rate(tax_map, pretax_map)
        ic_map, ic_diagnostics = self._fy_invested_capital_diagnostics(listing_id, repo)

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
            if ic_previous is None and year != min(ic_map):
                # A missing prior year *inside* the observable IC history is a
                # data hole and still fails the year; only the history
                # boundary (the oldest IC year, which cannot have a prior
                # balance sheet) falls through to the end-of-FY convention
                # below.
                roic_failure_by_year[year] = FAILURE_MISSING_PRIOR_FY_INVESTED_CAPITAL
                continue

            if ic_previous is None:
                # History boundary: average IC is unobservable for the oldest
                # balance-sheet year, so use that year's own end-of-FY level.
                # Conservative for a growing business (end >= average, so ROIC
                # is biased low), and it lets a 10-FY history support a full
                # 10-year window instead of silently demanding an 11th year —
                # the same 10-FY maturity bar the other strict-10y gates use.
                avg_ic = ic_current.money
            else:
                avg_ic = (ic_current.money + ic_previous.money) / 2.0
            # ROIC has no economic meaning at or below a zero capital base:
            # IC = debt + equity - cash passes through zero for cash-rich
            # firms, and NOPAT / avg_ic explodes near zero and sign-flips
            # below it (a profitable year reads as catastrophic; a loss year
            # with negative IC reads as a *good* year). Fail the year instead
            # of emitting a garbage point — the same `<= 0` convention the
            # sibling return-on-capital metrics (roic_ttm, croic, roce,
            # roc_greenblatt) already apply.
            if avg_ic.amount <= 0:
                roic_failure_by_year[year] = (
                    FAILURE_NON_POSITIVE_AVERAGE_INVESTED_CAPITAL
                )
                continue

            nopat = ebit.money * (1.0 - tax_rate.rate)
            as_of_values = [ebit.as_of, ic_current.as_of]
            if ic_previous is not None:
                as_of_values.append(ic_previous.as_of)
            if tax_rate.as_of is not None:
                as_of_values.append(tax_rate.as_of)
            roic_by_year[year] = _ROICFYPoint(
                year=year,
                value=nopat / avg_ic,
                as_of=max(as_of_values),
                currency=ebit.money.currency,
            )

        return _ROICYearComputation(
            ebit_map=ebit_map,
            tax_map=tax_map,
            pretax_map=pretax_map,
            tax_rate_by_year=tax_rate_by_year,
            ic_diagnostics=ic_diagnostics,
            roic_by_year=roic_by_year,
            roic_failure_by_year=roic_failure_by_year,
        )

    def diagnose_series(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        window_years: int = DEFAULT_SERIES_YEARS,
    ) -> ROICFYSeriesDiagnostic:
        computation = self._compute_roic_years(listing_id, repo)
        ebit_map = computation.ebit_map
        tax_map = computation.tax_map
        pretax_map = computation.pretax_map
        tax_rate_by_year = computation.tax_rate_by_year
        ic_diagnostics = computation.ic_diagnostics
        roic_by_year = computation.roic_by_year
        roic_failure_by_year = computation.roic_failure_by_year

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
                snapshot = ROICFYSeriesSnapshot(
                    points=selected_points,
                    as_of=selected_points[0].as_of,
                    currency=selected_points[0].currency,
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
            listing_id=listing_id,
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
        listing_id: int,
        repo: RegionFactsRepository,
        window_years: int = DEFAULT_SERIES_YEARS,
    ) -> Optional[ROICFYSeriesSnapshot]:
        diagnostic = self.diagnose_series(listing_id, repo, window_years=window_years)
        if diagnostic.snapshot is not None:
            return diagnostic.snapshot
        failure_reason = (
            diagnostic.failure_reason or FAILURE_MISSING_CURRENT_FY_INVESTED_CAPITAL
        )
        LOGGER.warning(
            "%s: %s for listing_id=%s",
            self._series_context(window_years),
            failure_reason,
            listing_id,
        )
        return None

    def compute_adaptive_series(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        *,
        max_years: int = DEFAULT_SERIES_YEARS,
        min_points: int = ADAPTIVE_MIN_POINTS,
    ) -> Optional[ROICFYSeriesSnapshot]:
        """Return the latest consecutive valid-ROIC chain, capped at ``max_years``.

        The adaptive sibling of the strict windows, mirroring
        ``cfo_to_ni_10y_median`` / ``ni_loss_year_share``: anchored at the
        newest valid ROIC year and walking back until the first failed or
        missing year, so short histories (young listings, thin exchange
        coverage) shrink the window instead of voiding the metric. Under the
        history-boundary IC convention, ``min_points`` chain points need
        exactly ``min_points`` FY years of fundamentals. Freshness is gated
        on the chain anchor.
        """

        computation = self._compute_roic_years(listing_id, repo)
        roic_by_year = computation.roic_by_year
        if not roic_by_year:
            LOGGER.warning(
                "roic_10y_median_adaptive: no valid FY ROIC years for listing_id=%s",
                listing_id,
            )
            return None

        chain = latest_consecutive_year_chain(roic_by_year, max_years=max_years)
        if len(chain) < min_points:
            LOGGER.warning(
                "roic_10y_median_adaptive: valid FY ROIC chain too short for "
                "listing_id=%s: %s of %s years",
                listing_id,
                len(chain),
                min_points,
            )
            return None

        points = tuple(point for _, point in chain)
        if not self._is_recent_as_of(
            points[0].as_of, max_age_days=MAX_FY_FACT_AGE_DAYS
        ):
            LOGGER.warning(
                "roic_10y_median_adaptive: latest FY point (%s) too old for listing_id=%s",
                points[0].as_of,
                listing_id,
            )
            return None

        return ROICFYSeriesSnapshot(
            points=points,
            as_of=points[0].as_of,
            currency=points[0].currency,
        )

    def compute_incremental_5y(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[IncrementalROICSnapshot]:
        ebit_map = self._fy_map(listing_id, repo, EBIT_CONCEPT)
        if not ebit_map:
            LOGGER.warning(
                "iroic_5y: missing FY EBIT history for listing_id=%s", listing_id
            )
            return None

        ic_map, _ = self._fy_invested_capital_diagnostics(listing_id, repo)
        if not ic_map:
            LOGGER.warning(
                "iroic_5y: missing FY invested capital history for listing_id=%s",
                listing_id,
            )
            return None

        latest_year = self._latest_incremental_year(ebit_map, ic_map)
        if latest_year is None:
            LOGGER.warning(
                "iroic_5y: missing strict t and t-5 FY pair for listing_id=%s",
                listing_id,
            )
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
            LOGGER.warning(
                "iroic_5y: latest FY point too old for listing_id=%s", listing_id
            )
            return None

        tax_map = self._fy_map(listing_id, repo, TAX_EXPENSE_CONCEPT)
        pretax_map = self._fy_map(listing_id, repo, PRETAX_INCOME_CONCEPT)
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

        latest_nopat = latest_ebit.money * (1.0 - latest_tax_rate.rate)
        prior_nopat = prior_ebit.money * (1.0 - prior_tax_rate.rate)
        delta_nopat = latest_nopat - prior_nopat

        as_of_values = [latest_ebit.as_of, latest_ic.as_of]
        if latest_tax_rate.as_of is not None:
            as_of_values.append(latest_tax_rate.as_of)

        # The ratio only measures anything when capital actually grew by a
        # material amount; below that floor (including outright shrinkage) the
        # denominator is noise. Growing NOPAT on flat-or-released capital is
        # the capital-light growth shape and scores the documented cap;
        # non-growing NOPAT stays NA.
        delta_ic = latest_ic.money - prior_ic.money
        ic_scale = max(abs(latest_ic.money.amount), abs(prior_ic.money.amount), 1.0)
        relative_delta_ic = abs(delta_ic.amount) / ic_scale
        if delta_ic.amount <= 0 or relative_delta_ic < IROIC_MIN_RELATIVE_DELTA_IC:
            if delta_nopat.amount > 0:
                LOGGER.info(
                    "iroic_5y: NOPAT grew on flat/shrinking invested capital for "
                    "listing_id=%s -- emitting documented cap %.0f%%",
                    listing_id,
                    IROIC_CAP * 100.0,
                )
                return IncrementalROICSnapshot(
                    value=IROIC_CAP,
                    as_of=max(as_of_values),
                    currency=latest_ebit.money.currency,
                )
            LOGGER.warning(
                "iroic_5y: flat or shrinking invested capital without NOPAT "
                "growth for listing_id=%s",
                listing_id,
            )
            return None

        return IncrementalROICSnapshot(
            value=delta_nopat / delta_ic,
            as_of=max(as_of_values),
            currency=latest_ebit.money.currency,
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
            ebit_currency=ebit.money.currency if ebit else None,
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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> tuple[dict[int, _AmountResult], dict[int, _InvestedCapitalYearDiagnostic]]:
        short_map = self._fy_period_map(
            repo.monetary_facts_for_concept(listing_id, SHORT_TERM_DEBT_CONCEPT)
        )
        long_map = self._fy_period_map(
            repo.monetary_facts_for_concept(listing_id, LONG_TERM_DEBT_CONCEPT)
        )
        total_map = self._fy_period_map(
            repo.monetary_facts_for_concept(listing_id, TOTAL_DEBT_CONCEPT)
        )
        equity_map = self._fy_period_map(
            repo.monetary_facts_for_concept(listing_id, EQUITY_PRIMARY_CONCEPT)
        )
        common_equity_map = self._fy_period_map(
            repo.monetary_facts_for_concept(listing_id, EQUITY_FALLBACK_CONCEPT)
        )
        cash_primary_map = self._fy_period_map(
            repo.monetary_facts_for_concept(listing_id, CASH_PRIMARY_CONCEPT)
        )
        cash_fallback_map = self._fy_period_map(
            repo.monetary_facts_for_concept(listing_id, CASH_FALLBACK_CONCEPT)
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
                listing_id=listing_id,
                repo=repo,
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
                listing_id=listing_id,
                repo=repo,
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
                listing_id=listing_id,
                repo=repo,
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

            point = _AmountResult(
                money=debt.money + equity.money - cash.money,
                as_of=max(debt.as_of, equity.as_of, cash.as_of),
            )
            ic_map[year] = point
            failure_by_year[year] = _InvestedCapitalYearDiagnostic(
                year=year,
                available=True,
                as_of=point.as_of,
                currency=point.money.currency,
                failure_reason=None,
            )

        return ic_map, failure_by_year

    def _resolve_invested_capital_debt(
        self,
        *,
        listing_id: int,
        repo: RegionFactsRepository,
        short_debt: Optional[MonetaryFact],
        long_debt: Optional[MonetaryFact],
        total_debt: Optional[MonetaryFact],
    ) -> tuple[Optional[_AmountResult], Optional[str]]:
        if short_debt is not None and long_debt is not None:
            return (
                _AmountResult(
                    money=self._money(short_debt, listing_id, repo)
                    + self._money(long_debt, listing_id, repo),
                    as_of=max(short_debt.end_date, long_debt.end_date),
                ),
                None,
            )

        if total_debt is not None:
            return (
                _AmountResult(
                    money=self._money(total_debt, listing_id, repo),
                    as_of=total_debt.end_date,
                ),
                None,
            )

        one_side = short_debt or long_debt
        if one_side is None:
            return None, FAILURE_MISSING_INVESTED_CAPITAL_DEBT_INPUT

        return (
            _AmountResult(
                money=self._money(one_side, listing_id, repo),
                as_of=one_side.end_date,
            ),
            None,
        )

    def _resolve_invested_capital_single_amount(
        self,
        *,
        listing_id: int,
        repo: RegionFactsRepository,
        primary: Optional[MonetaryFact],
        fallback: Optional[MonetaryFact],
        missing_failure: str,
    ) -> tuple[Optional[_AmountResult], Optional[str]]:
        record = primary or fallback
        if record is None:
            return None, missing_failure
        return (
            _AmountResult(
                money=self._money(record, listing_id, repo),
                as_of=record.end_date,
            ),
            None,
        )

    def _fy_period_map(
        self, records: Sequence[MonetaryFact]
    ) -> dict[tuple[str, str], MonetaryFact]:
        mapped: dict[tuple[str, str], MonetaryFact] = {}
        for record in sorted(records, key=lambda item: item.end_date, reverse=True):
            period = (record.fiscal_period or "").upper()
            if period not in FY_PERIODS:
                continue
            key = (record.end_date, period)
            if key not in mapped:
                mapped[key] = record
        return mapped

    def _fy_map(
        self,
        listing_id: int,
        repo: RegionFactsRepository,
        concept: str,
    ) -> dict[int, _AmountResult]:
        records = repo.monetary_facts_for_concept(
            listing_id, concept, fiscal_period="FY"
        )
        ordered = self._filter_periods(records, FY_PERIODS)
        mapped: dict[int, _AmountResult] = {}
        for record in ordered:
            year = self._extract_year(record.end_date)
            if year is None or year in mapped:
                continue
            mapped[year] = _AmountResult(
                money=self._money(record, listing_id, repo),
                as_of=record.end_date,
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
        if pretax.money.amount <= PRETAX_MIN_ABS:
            return None
        rate = tax.money / pretax.money
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
        self, records: Sequence[MonetaryFact], periods: set[str]
    ) -> list[MonetaryFact]:
        filtered: list[MonetaryFact] = []
        seen_end_dates: set[str] = set()
        for record in records:
            period = (record.fiscal_period or "").upper()
            if period not in periods:
                continue
            if record.end_date in seen_end_dates:
                continue
            filtered.append(record)
            seen_end_dates.add(record.end_date)
        return filtered

    def _money(
        self, fact: MonetaryFact, listing_id: int, repo: RegionFactsRepository
    ) -> Money:
        target_currency = require_metric_ticker_currency(
            listing_id,
            repo,
            metric_id=_METRIC_ID,
            input_name=fact.concept,
            as_of=fact.end_date,
        )
        return require_metric_money(
            fact.money,
            target_currency=target_currency,
            metric_id=_METRIC_ID,
            listing_id=listing_id,
            input_name=fact.concept,
            as_of=fact.end_date,
        )

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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(
            listing_id, repo, window_years=DEFAULT_SERIES_YEARS
        )
        if snapshot is None:
            return None
        values = sorted(point.value for point in snapshot.points)
        midpoint = len(values) // 2
        median = (values[midpoint - 1] + values[midpoint]) / 2.0
        return MetricResult(
            listing_id=listing_id,
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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(
            listing_id, repo, window_years=STRICT_7Y_YEARS
        )
        if snapshot is None:
            return None
        values = sorted(point.value for point in snapshot.points)
        median = values[len(values) // 2]
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=median,
            as_of=snapshot.as_of,
        )


@dataclass
class ROICMedianAdaptiveMetric:
    """Compute median FY ROIC over the latest adaptive (<= 10y, >= 6 point) chain.

    DVG's coverage-friendly sibling of the strict medians: the same per-year
    ROIC construction, selected over the latest consecutive run of valid
    years instead of a strict window, so 6-9-year histories stay screenable.
    QARP keeps gating on the strict ``roic_7y_median``.
    """

    id: str = "roic_10y_median_adaptive"
    required_concepts = REQUIRED_CONCEPTS

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_adaptive_series(listing_id, repo)
        if snapshot is None:
            return None
        median = statistics.median(point.value for point in snapshot.points)
        return MetricResult(
            listing_id=listing_id,
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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(
            listing_id, repo, window_years=DEFAULT_SERIES_YEARS
        )
        if snapshot is None:
            return None
        count = sum(1 for point in snapshot.points if point.value > ABOVE_THRESHOLD)
        return MetricResult(
            listing_id=listing_id,
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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(
            listing_id, repo, window_years=DEFAULT_SERIES_YEARS
        )
        if snapshot is None:
            return None
        minimum = min(point.value for point in snapshot.points)
        return MetricResult(
            listing_id=listing_id,
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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_series(
            listing_id, repo, window_years=STRICT_7Y_YEARS
        )
        if snapshot is None:
            return None
        minimum = min(point.value for point in snapshot.points)
        return MetricResult(
            listing_id=listing_id,
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
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]:
        snapshot = ROICFYSeriesCalculator().compute_incremental_5y(listing_id, repo)
        if snapshot is None:
            return None
        return MetricResult(
            listing_id=listing_id,
            metric_id=self.id,
            value=snapshot.value,
            as_of=snapshot.as_of,
        )


__all__ = [
    "IROIC_CAP",
    "ROICFYYearDiagnostic",
    "ROICFYSeriesDiagnostic",
    "ROICFYSeriesSnapshot",
    "IncrementalROICSnapshot",
    "ROICFYSeriesCalculator",
    "ROIC10YMedianMetric",
    "ROIC7YMedianMetric",
    "ROICMedianAdaptiveMetric",
    "ROICYearsAbove12PctMetric",
    "ROIC10YMinMetric",
    "ROIC7YMinMetric",
    "IncrementalROICFiveYearMetric",
]
