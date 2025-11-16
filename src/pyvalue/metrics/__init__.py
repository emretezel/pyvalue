# Author: Emre Tezel
"""Metric computation interfaces and implementations."""

from .base import Metric, MetricResult
from .working_capital import WorkingCapitalMetric
from .long_term_debt import LongTermDebtMetric

REGISTRY = {
    WorkingCapitalMetric.id: WorkingCapitalMetric,
    LongTermDebtMetric.id: LongTermDebtMetric,
}

__all__ = [
    "Metric",
    "MetricResult",
    "WorkingCapitalMetric",
    "LongTermDebtMetric",
    "REGISTRY",
]
