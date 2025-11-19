"""Screening configuration and evaluation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
import logging

import yaml

from pyvalue.metrics import REGISTRY
from pyvalue.storage import FinancialFactsRepository, MarketDataRepository, MetricsRepository

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
    return ScreenDefinition(criteria=criteria)


def evaluate_criterion(
    criterion: Criterion,
    symbol: str,
    metrics_repo: MetricsRepository,
    fact_repo: FinancialFactsRepository,
    market_repo: Optional[MarketDataRepository] = None,
) -> bool:
    passed, _ = evaluate_criterion_verbose(criterion, symbol, metrics_repo, fact_repo, market_repo)
    return passed


def evaluate_criterion_verbose(
    criterion: Criterion,
    symbol: str,
    metrics_repo: MetricsRepository,
    fact_repo: FinancialFactsRepository,
    market_repo: Optional[MarketDataRepository] = None,
) -> tuple[bool, Optional[float]]:
    left_value = _resolve_term_value(criterion.left, symbol, metrics_repo, fact_repo, market_repo)
    right_value = _resolve_term_value(criterion.right, symbol, metrics_repo, fact_repo, market_repo)
    if left_value is None or right_value is None:
        return False, left_value
    lhs = left_value
    rhs = right_value * criterion.right.multiplier
    if criterion.operator == "<=":
        return lhs <= rhs, lhs
    if criterion.operator == ">=":
        return lhs >= rhs, lhs
    if criterion.operator == "<":
        return lhs < rhs, lhs
    if criterion.operator == ">":
        return lhs > rhs, lhs
    if criterion.operator == "==":
        return lhs == rhs, lhs
    raise ValueError(f"Unsupported operator: {criterion.operator}")


def _resolve_term_value(
    term: Term,
    symbol: str,
    metrics_repo: MetricsRepository,
    fact_repo: FinancialFactsRepository,
    market_repo: Optional[MarketDataRepository],
) -> Optional[float]:
    if term.value is not None:
        return term.value
    if not term.metric:
        return None
    return _ensure_metric_value(symbol, term.metric, metrics_repo, fact_repo, market_repo)


def _ensure_metric_value(
    symbol: str,
    metric_id: str,
    metrics_repo: MetricsRepository,
    fact_repo: FinancialFactsRepository,
    market_repo: Optional[MarketDataRepository],
) -> Optional[float]:
    record = metrics_repo.fetch(symbol, metric_id)
    if record is not None:
        return record[0]
    LOGGER.warning("Metric %s missing for %s; run compute-metrics first", metric_id, symbol)
    return None
