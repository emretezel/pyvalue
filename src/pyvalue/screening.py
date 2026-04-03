"""Screening configuration and evaluation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import logging

import yaml  # type: ignore[import-untyped]

from pyvalue.facts import RegionFactsRepository
from pyvalue.storage import (
    FinancialFactsRepository,
    MarketDataRepository,
    MetricsRepository,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class Term:
    metric: Optional[str] = None
    multiplier: float = 1.0
    value: Optional[float] = None


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


@dataclass(frozen=True)
class RankingTieBreaker:
    """Secondary ordering rule after the final ranking score."""

    metric_id: str
    direction: str


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
        if left.value is None and right.value is None:
            failure_kind = "both_missing"
        elif left.value is None:
            failure_kind = "left_missing"
        else:
            failure_kind = "right_missing"
        return CriterionEvaluation(
            passed=False,
            left_value=left.value,
            right_value=right.value,
            lhs=None,
            rhs=None,
            failure_kind=failure_kind,
            missing_metric_ids=missing_metric_ids,
            left_metric_id=left.metric_id,
            right_metric_id=right.metric_id,
        )

    lhs = left.value
    rhs = right.value * criterion.right.multiplier
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
        return ResolvedTerm(metric_id=None, value=term.value)
    if not term.metric:
        return ResolvedTerm(metric_id=None, value=None)
    return ResolvedTerm(
        metric_id=term.metric,
        value=_ensure_metric_value(
            symbol,
            term.metric,
            metrics_repo,
            fact_repo,
            market_repo,
            log_missing_metric=log_missing_metrics,
        ),
    )


def _ensure_metric_value(
    symbol: str,
    metric_id: str,
    metrics_repo: MetricsRepository,
    fact_repo: FinancialFactsRepository | RegionFactsRepository,
    market_repo: Optional[MarketDataRepository],
    *,
    log_missing_metric: bool = True,
) -> Optional[float]:
    record = metrics_repo.fetch(symbol, metric_id)
    if record is not None:
        return record[0]
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
        )
        for entry in data.get("metrics", [])
        if isinstance(entry, dict)
        and str(entry.get("metric") or entry.get("metric_id") or "").strip()
    )
    tie_breakers = tuple(
        RankingTieBreaker(
            metric_id=str(entry.get("metric") or entry.get("metric_id") or "").strip(),
            direction=str(entry.get("direction") or "ascending").strip().lower(),
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
