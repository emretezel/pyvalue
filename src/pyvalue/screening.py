"""Screening configuration and evaluation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import List, Optional
import logging

import yaml  # type: ignore[import-untyped]

from pyvalue.currency import (
    MetricUnitKind,
    is_monetary_unit_kind,
    normalize_currency_code,
)
from pyvalue.facts import RegionFactsRepository
from pyvalue.fx import FXService
from pyvalue.storage import (
    FinancialFactsRepository,
    MarketDataRepository,
    MetricRecord,
    MetricsRepository,
)


LOGGER = logging.getLogger(__name__)
_RATIO_LIKE_UNIT_KINDS = frozenset({"ratio", "percent"})


@dataclass
class Term:
    metric: Optional[str] = None
    multiplier: float = 1.0
    value: Optional[float] = None
    currency: Optional[str] = None


@dataclass
class Criterion:
    name: str
    left: Term
    operator: str
    right: Term


@dataclass
class ScreenDefinition:
    criteria: List[Criterion]
    ranking: Optional["RankingDefinition"] = None


@dataclass(frozen=True)
class RankingMetric:
    """One metric that contributes to a screen ranking score."""

    metric_id: str
    weight: float
    direction: str
    cap: Optional[float] = None
    currency: Optional[str] = None


@dataclass(frozen=True)
class RankingTieBreaker:
    """Secondary ordering rule after the final ranking score."""

    metric_id: str
    direction: str
    currency: Optional[str] = None


@dataclass(frozen=True)
class RankingDefinition:
    """Optional ranking rules applied after pass/fail screening."""

    peer_group: str
    min_sector_peers: int
    winsor_lower_percentile: float
    winsor_upper_percentile: float
    metrics: tuple[RankingMetric, ...]
    tie_breakers: tuple[RankingTieBreaker, ...] = ()


@dataclass(frozen=True)
class ResolvedTerm:
    """Resolved screening term value for one symbol."""

    metric_id: Optional[str]
    value: Optional[float]
    unit_kind: Optional[MetricUnitKind] = None
    currency: Optional[str] = None
    unit_label: Optional[str] = None
    as_of: Optional[str] = None


@dataclass(frozen=True)
class CriterionEvaluation:
    """Detailed evaluation result for one screening criterion."""

    passed: bool
    left_value: Optional[float]
    right_value: Optional[float]
    lhs: Optional[float]
    rhs: Optional[float]
    failure_kind: Optional[str]
    missing_metric_ids: tuple[str, ...]
    left_metric_id: Optional[str] = None
    right_metric_id: Optional[str] = None


def load_screen(path: str | Path) -> ScreenDefinition:
    data = yaml.safe_load(Path(path).read_text())
    criteria = []
    for entry in data.get("criteria", []):
        criteria.append(
            Criterion(
                name=entry.get("name", "criterion"),
                left=Term(**entry["left"]),
                operator=entry.get("operator", "<="),
                right=Term(**entry["right"]),
            )
        )
    ranking = _load_ranking_definition(data.get("ranking"))
    return ScreenDefinition(criteria=criteria, ranking=ranking)


def screen_metric_ids(definition: ScreenDefinition) -> List[str]:
    """Return unique metric ids referenced by the screen in first-seen order."""

    metric_ids: List[str] = []
    seen: set[str] = set()
    for criterion in definition.criteria:
        for term in (criterion.left, criterion.right):
            if not term.metric or term.metric in seen:
                continue
            seen.add(term.metric)
            metric_ids.append(term.metric)
    return metric_ids


def ranking_metric_ids(definition: ScreenDefinition) -> List[str]:
    """Return unique ranking metric ids in first-seen order."""

    if definition.ranking is None:
        return []
    metric_ids: List[str] = []
    seen: set[str] = set()
    for metric in definition.ranking.metrics:
        if metric.metric_id in seen:
            continue
        seen.add(metric.metric_id)
        metric_ids.append(metric.metric_id)
    return metric_ids


def evaluate_criterion(
    criterion: Criterion,
    symbol: str,
    metrics_repo: MetricsRepository,
    fact_repo: FinancialFactsRepository | RegionFactsRepository,
    market_repo: Optional[MarketDataRepository] = None,
) -> bool:
    passed, _ = evaluate_criterion_verbose(
        criterion, symbol, metrics_repo, fact_repo, market_repo
    )
    return passed


def evaluate_criterion_verbose(
    criterion: Criterion,
    symbol: str,
    metrics_repo: MetricsRepository,
    fact_repo: FinancialFactsRepository | RegionFactsRepository,
    market_repo: Optional[MarketDataRepository] = None,
) -> tuple[bool, Optional[float]]:
    evaluation = evaluate_criterion_detail(
        criterion, symbol, metrics_repo, fact_repo, market_repo
    )
    return evaluation.passed, evaluation.left_value


def evaluate_criterion_detail(
    criterion: Criterion,
    symbol: str,
    metrics_repo: MetricsRepository,
    fact_repo: FinancialFactsRepository | RegionFactsRepository,
    market_repo: Optional[MarketDataRepository] = None,
    *,
    log_missing_metrics: bool = True,
) -> CriterionEvaluation:
    """Return a detailed evaluation for one criterion."""

    left = _resolve_term(
        criterion.left,
        symbol,
        metrics_repo,
        fact_repo,
        market_repo,
        log_missing_metrics=log_missing_metrics,
    )
    right = _resolve_term(
        criterion.right,
        symbol,
        metrics_repo,
        fact_repo,
        market_repo,
        log_missing_metrics=log_missing_metrics,
    )
    missing_metric_ids = _dedupe_missing_metric_ids(
        left.metric_id if left.value is None else None,
        right.metric_id if right.value is None else None,
    )
    if left.value is None or right.value is None:
        missing_failure_kind: str
        if left.value is None and right.value is None:
            missing_failure_kind = "both_missing"
        elif left.value is None:
            missing_failure_kind = "left_missing"
        else:
            missing_failure_kind = "right_missing"
        return CriterionEvaluation(
            passed=False,
            left_value=left.value,
            right_value=right.value,
            lhs=None,
            rhs=None,
            failure_kind=missing_failure_kind,
            missing_metric_ids=missing_metric_ids,
            left_metric_id=left.metric_id,
            right_metric_id=right.metric_id,
        )

    lhs: Optional[float]
    rhs: Optional[float]
    failure_kind: Optional[str]
    lhs, rhs, failure_kind = _align_comparison_values(
        symbol,
        criterion,
        left,
        right,
        metrics_repo,
    )
    if failure_kind is not None or lhs is None or rhs is None:
        return CriterionEvaluation(
            passed=False,
            left_value=left.value,
            right_value=right.value,
            lhs=lhs,
            rhs=rhs,
            failure_kind=failure_kind or "comparison_unavailable",
            missing_metric_ids=missing_metric_ids,
            left_metric_id=left.metric_id,
            right_metric_id=right.metric_id,
        )

    if criterion.operator == "<=":
        passed = lhs <= rhs
    elif criterion.operator == ">=":
        passed = lhs >= rhs
    elif criterion.operator == "<":
        passed = lhs < rhs
    elif criterion.operator == ">":
        passed = lhs > rhs
    elif criterion.operator == "==":
        passed = lhs == rhs
    else:
        raise ValueError(f"Unsupported operator: {criterion.operator}")

    return CriterionEvaluation(
        passed=passed,
        left_value=left.value,
        right_value=right.value,
        lhs=lhs,
        rhs=rhs,
        failure_kind=None if passed else "comparison_failed",
        missing_metric_ids=missing_metric_ids,
        left_metric_id=left.metric_id,
        right_metric_id=right.metric_id,
    )


def _align_comparison_values(
    symbol: str,
    criterion: Criterion,
    left: ResolvedTerm,
    right: ResolvedTerm,
    metrics_repo: MetricsRepository,
) -> tuple[Optional[float], Optional[float], Optional[str]]:
    left_value = left.value
    right_value = right.value
    if left_value is None or right_value is None:
        return None, None, "comparison_unavailable"

    anchor = _comparison_anchor(left, right)
    if anchor is None:
        if not _non_monetary_terms_compatible(left, right):
            LOGGER.warning(
                "Screen unit mismatch | symbol=%s criterion=%s left_kind=%s right_kind=%s",
                symbol,
                criterion.name,
                left.unit_kind,
                right.unit_kind,
            )
            return None, None, "unit_mismatch"
        lhs = left_value
        rhs = right_value * criterion.right.multiplier
        return lhs, rhs, None

    if is_monetary_unit_kind(anchor.unit_kind):
        if anchor.currency is None:
            LOGGER.warning(
                "Missing metric currency during screen evaluation | symbol=%s criterion=%s anchor_metric=%s",
                symbol,
                criterion.name,
                anchor.metric_id,
            )
            return None, None, "currency_missing"
        fx_service = _fx_service_for_db(metrics_repo.db_path)
        left_aligned, left_error = _convert_term_to_anchor(
            left,
            anchor,
            symbol=symbol,
            criterion_name=criterion.name,
            side="left",
            fx_service=fx_service,
        )
        if left_error is not None:
            return None, None, left_error
        if left_aligned is None:
            return None, None, "comparison_unavailable"
        right_aligned, right_error = _convert_term_to_anchor(
            right,
            anchor,
            symbol=symbol,
            criterion_name=criterion.name,
            side="right",
            fx_service=fx_service,
        )
        if right_error is not None:
            return None, None, right_error
        if right_aligned is None:
            return None, None, "comparison_unavailable"
        return left_aligned, right_aligned * criterion.right.multiplier, None

    if not _non_monetary_terms_compatible(left, right):
        LOGGER.warning(
            "Screen unit mismatch | symbol=%s criterion=%s left_kind=%s right_kind=%s",
            symbol,
            criterion.name,
            left.unit_kind,
            right.unit_kind,
        )
        return None, None, "unit_mismatch"
    if left.currency is not None or right.currency is not None:
        LOGGER.warning(
            "Unexpected currency on non-monetary screen term | symbol=%s criterion=%s left_currency=%s right_currency=%s",
            symbol,
            criterion.name,
            left.currency,
            right.currency,
        )
        return None, None, "unit_mismatch"
    return left_value, right_value * criterion.right.multiplier, None


def _comparison_anchor(
    left: ResolvedTerm, right: ResolvedTerm
) -> Optional[ResolvedTerm]:
    for term in (left, right):
        if term.metric_id is None:
            continue
        if term.unit_kind is None:
            continue
        return term
    return None


def _non_monetary_terms_compatible(left: ResolvedTerm, right: ResolvedTerm) -> bool:
    left_kind = left.unit_kind
    right_kind = right.unit_kind
    if left_kind is None or right_kind is None:
        return True
    if left_kind == right_kind:
        return True
    if left_kind in _RATIO_LIKE_UNIT_KINDS and right_kind in _RATIO_LIKE_UNIT_KINDS:
        return True
    return False


def _convert_term_to_anchor(
    term: ResolvedTerm,
    anchor: ResolvedTerm,
    *,
    symbol: str,
    criterion_name: str,
    side: str,
    fx_service: FXService,
) -> tuple[Optional[float], Optional[str]]:
    if term.value is None:
        return None, "missing"
    if anchor.currency is None:
        return None, "currency_missing"

    if term.metric_id is None:
        if term.currency is None:
            return term.value, None
        source_currency = normalize_currency_code(term.currency)
        if source_currency is None:
            return None, "currency_missing"
        if source_currency == anchor.currency:
            return term.value, None
        converted = fx_service.convert_amount(
            term.value,
            source_currency,
            anchor.currency,
            anchor.as_of or date.today().isoformat(),
        )
        if converted is None:
            LOGGER.warning(
                "Missing FX during screen evaluation | symbol=%s criterion=%s side=%s from=%s to=%s as_of=%s",
                symbol,
                criterion_name,
                side,
                source_currency,
                anchor.currency,
                anchor.as_of,
            )
            return None, "fx_missing"
        return float(converted), None

    if not is_monetary_unit_kind(term.unit_kind):
        return None, "unit_mismatch"
    source_currency = normalize_currency_code(term.currency)
    if source_currency is None:
        LOGGER.warning(
            "Missing metric currency during screen evaluation | symbol=%s criterion=%s side=%s metric=%s",
            symbol,
            criterion_name,
            side,
            term.metric_id,
        )
        return None, "currency_missing"
    if source_currency == anchor.currency:
        return term.value, None

    converted = fx_service.convert_amount(
        term.value,
        source_currency,
        anchor.currency,
        term.as_of or anchor.as_of or date.today().isoformat(),
    )
    if converted is None:
        LOGGER.warning(
            "Missing FX during screen evaluation | symbol=%s criterion=%s side=%s metric=%s from=%s to=%s as_of=%s",
            symbol,
            criterion_name,
            side,
            term.metric_id,
            source_currency,
            anchor.currency,
            term.as_of,
        )
        return None, "fx_missing"
    return float(converted), None


def _resolve_term(
    term: Term,
    symbol: str,
    metrics_repo: MetricsRepository,
    fact_repo: FinancialFactsRepository | RegionFactsRepository,
    market_repo: Optional[MarketDataRepository],
    *,
    log_missing_metrics: bool,
) -> ResolvedTerm:
    if term.value is not None:
        return ResolvedTerm(
            metric_id=None,
            value=term.value,
            unit_kind="monetary" if term.currency else None,
            currency=normalize_currency_code(term.currency),
            unit_label=None,
            as_of=None,
        )
    if not term.metric:
        return ResolvedTerm(metric_id=None, value=None)
    record = _ensure_metric_record(
        symbol,
        term.metric,
        metrics_repo,
        fact_repo,
        market_repo,
        log_missing_metric=log_missing_metrics,
    )
    if record is None:
        return ResolvedTerm(metric_id=term.metric, value=None)
    return ResolvedTerm(
        metric_id=term.metric,
        value=record.value,
        unit_kind=record.unit_kind,
        currency=record.currency,
        unit_label=record.unit_label,
        as_of=record.as_of,
    )


def _ensure_metric_record(
    symbol: str,
    metric_id: str,
    metrics_repo: MetricsRepository,
    fact_repo: FinancialFactsRepository | RegionFactsRepository,
    market_repo: Optional[MarketDataRepository],
    *,
    log_missing_metric: bool = True,
) -> Optional[MetricRecord]:
    del fact_repo, market_repo
    record = metrics_repo.fetch(symbol, metric_id)
    if record is not None:
        return record
    if log_missing_metric:
        LOGGER.warning(
            "Metric %s missing for %s; run compute-metrics first", metric_id, symbol
        )
    return None


def _dedupe_missing_metric_ids(*metric_ids: Optional[str]) -> tuple[str, ...]:
    ordered: List[str] = []
    seen: set[str] = set()
    for metric_id in metric_ids:
        if metric_id is None or metric_id in seen:
            continue
        seen.add(metric_id)
        ordered.append(metric_id)
    return tuple(ordered)


@lru_cache(maxsize=8)
def _fx_service_for_db(db_path: str) -> FXService:
    return FXService(db_path)


def _load_ranking_definition(data: object) -> Optional[RankingDefinition]:
    if not isinstance(data, dict):
        return None
    winsorize = data.get("winsorize")
    winsorize_data = winsorize if isinstance(winsorize, dict) else {}
    metrics = tuple(
        RankingMetric(
            metric_id=str(entry.get("metric") or entry.get("metric_id") or "").strip(),
            weight=float(entry.get("weight") or 0.0),
            direction=str(entry.get("direction") or "higher").strip().lower(),
            cap=(
                float(entry["cap"])
                if entry.get("cap") is not None and str(entry.get("cap")).strip() != ""
                else None
            ),
            currency=normalize_currency_code(entry.get("currency")),
        )
        for entry in data.get("metrics", [])
        if isinstance(entry, dict)
        and str(entry.get("metric") or entry.get("metric_id") or "").strip()
    )
    tie_breakers = tuple(
        RankingTieBreaker(
            metric_id=str(entry.get("metric") or entry.get("metric_id") or "").strip(),
            direction=str(entry.get("direction") or "ascending").strip().lower(),
            currency=normalize_currency_code(entry.get("currency")),
        )
        for entry in data.get("tie_breakers", [])
        if isinstance(entry, dict)
        and str(entry.get("metric") or entry.get("metric_id") or "").strip()
    )
    return RankingDefinition(
        peer_group=str(data.get("peer_group") or "sector").strip().lower(),
        min_sector_peers=int(data.get("min_sector_peers") or 10),
        winsor_lower_percentile=_normalize_percentile_threshold(
            winsorize_data.get("lower_percentile", 0.05)
        ),
        winsor_upper_percentile=_normalize_percentile_threshold(
            winsorize_data.get("upper_percentile", 0.95)
        ),
        metrics=metrics,
        tie_breakers=tie_breakers,
    )


def _normalize_percentile_threshold(value: object) -> float:
    if not isinstance(value, (int, float, str)):
        raise TypeError("Percentile threshold must be numeric")
    numeric = float(value)
    return numeric / 100.0 if numeric > 1.0 else numeric


__all__ = [
    "Criterion",
    "CriterionEvaluation",
    "RankingDefinition",
    "RankingMetric",
    "RankingTieBreaker",
    "ResolvedTerm",
    "ScreenDefinition",
    "Term",
    "evaluate_criterion",
    "evaluate_criterion_detail",
    "evaluate_criterion_verbose",
    "load_screen",
    "ranking_metric_ids",
    "screen_metric_ids",
]
