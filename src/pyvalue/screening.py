"""Screening configuration and evaluation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from pyvalue.metrics import REGISTRY
from pyvalue.storage import FinancialFactsRepository, MetricsRepository


@dataclass
class Term:
    metric: str
    multiplier: float = 1.0


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
) -> bool:
    left_value = _ensure_metric_value(symbol, criterion.left.metric, metrics_repo, fact_repo)
    right_value = _ensure_metric_value(symbol, criterion.right.metric, metrics_repo, fact_repo)
    if left_value is None or right_value is None:
        return False
    lhs = left_value
    rhs = right_value * criterion.right.multiplier
    if criterion.operator == "<=":
        return lhs <= rhs
    if criterion.operator == ">=":
        return lhs >= rhs
    if criterion.operator == "<":
        return lhs < rhs
    if criterion.operator == ">":
        return lhs > rhs
    if criterion.operator == "==":
        return lhs == rhs
    raise ValueError(f"Unsupported operator: {criterion.operator}")


def _ensure_metric_value(
    symbol: str,
    metric_id: str,
    metrics_repo: MetricsRepository,
    fact_repo: FinancialFactsRepository,
) -> Optional[float]:
    record = metrics_repo.fetch(symbol, metric_id)
    if record is not None:
        return record[0]
    metric_cls = REGISTRY.get(metric_id)
    if metric_cls is None:
        return None
    result = metric_cls().compute(symbol, fact_repo)
    if result is None:
        return None
    metrics_repo.upsert(result.symbol, result.metric_id, result.value, result.as_of)
    return result.value
