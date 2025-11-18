"""Metric computation interfaces and implementations.

Author: Emre Tezel
"""

from .base import Metric, MetricResult
from .working_capital import WorkingCapitalMetric
from .current_ratio import CurrentRatioMetric
from .long_term_debt import LongTermDebtMetric
from .eps_streak import EPSStreakMetric
from .graham_eps_cagr import GrahamEPSCAGRMetric
from .graham_multiplier import GrahamMultiplierMetric
from .earnings_yield import EarningsYieldMetric
from .price_to_fcf import PriceToFCFMetric
from .roc_greenblatt import ROCGreenblattMetric
from .roe_greenblatt import ROEGreenblattMetric

REGISTRY = {
    WorkingCapitalMetric.id: WorkingCapitalMetric,
    CurrentRatioMetric.id: CurrentRatioMetric,
    LongTermDebtMetric.id: LongTermDebtMetric,
    EPSStreakMetric.id: EPSStreakMetric,
    GrahamEPSCAGRMetric.id: GrahamEPSCAGRMetric,
    GrahamMultiplierMetric.id: GrahamMultiplierMetric,
    EarningsYieldMetric.id: EarningsYieldMetric,
    PriceToFCFMetric.id: PriceToFCFMetric,
    ROCGreenblattMetric.id: ROCGreenblattMetric,
    ROEGreenblattMetric.id: ROEGreenblattMetric,
}

__all__ = [
    "Metric",
    "MetricResult",
    "WorkingCapitalMetric",
    "CurrentRatioMetric",
    "LongTermDebtMetric",
    "EPSStreakMetric",
    "GrahamEPSCAGRMetric",
    "GrahamMultiplierMetric",
    "EarningsYieldMetric",
    "PriceToFCFMetric",
    "ROCGreenblattMetric",
    "ROEGreenblattMetric",
    "REGISTRY",
]
