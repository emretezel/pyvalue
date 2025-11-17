# Author: Emre Tezel
"""Metric computation interfaces and implementations."""

from .base import Metric, MetricResult
from .working_capital import WorkingCapitalMetric
from .long_term_debt import LongTermDebtMetric
from .eps_streak import EPSStreakMetric

REGISTRY = {
    WorkingCapitalMetric.id: WorkingCapitalMetric,
    LongTermDebtMetric.id: LongTermDebtMetric,
    EPSStreakMetric.id: EPSStreakMetric,
}

__all__ = [
    "Metric",
    "MetricResult",
    "WorkingCapitalMetric",
    "LongTermDebtMetric",
    "EPSStreakMetric",
    "REGISTRY",
]
