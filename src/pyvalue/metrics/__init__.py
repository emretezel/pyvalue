# Author: Emre Tezel
"""Metric computation interfaces and implementations."""

from .base import Metric, MetricResult
from .working_capital import WorkingCapitalMetric
from .long_term_debt import LongTermDebtMetric
from .eps_streak import EPSStreakMetric
from .graham_eps_cagr import GrahamEPSCAGRMetric

REGISTRY = {
    WorkingCapitalMetric.id: WorkingCapitalMetric,
    LongTermDebtMetric.id: LongTermDebtMetric,
    EPSStreakMetric.id: EPSStreakMetric,
    GrahamEPSCAGRMetric.id: GrahamEPSCAGRMetric,
}

__all__ = [
    "Metric",
    "MetricResult",
    "WorkingCapitalMetric",
    "LongTermDebtMetric",
    "EPSStreakMetric",
    "GrahamEPSCAGRMetric",
    "REGISTRY",
]
