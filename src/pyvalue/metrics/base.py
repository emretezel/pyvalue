"""
Base classes and primitives for financial metrics.
Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union


@dataclass(frozen=True)
class MetricResult:
    """Represents the outcome of a metric calculation for a stock."""

    stock_id: int
    metric_name: str
    value: float
    data_from_date: date
    computed_at: datetime
    metadata: Optional[Dict[str, Any]] = None


class MetricPrerequisiteMissing(RuntimeError):
    """Raised when a metric cannot be evaluated because inputs are missing."""


class Metric:
    """Base class for metrics that operate on financial statement data."""

    name: str = "metric"
    requires: Sequence[str] = ()

    def fetch_inputs(self, data_access: Any, stock_id: int) -> Dict[str, Any]:
        """Fetch datasets declared in `requires` using the provided data access."""
        inputs = {}
        for requirement in self.requires:
            inputs[requirement] = data_access.fetch(requirement, stock_id)
        return inputs

    def evaluate(
        self,
        data_access: Any,
        stock_id: int,
        computed_at: Optional[datetime] = None,
    ) -> List[MetricResult]:
        """Evaluate the metric and return MetricResult objects."""
        computed_at = computed_at or datetime.now(timezone.utc)
        inputs = self.fetch_inputs(data_access, stock_id)
        results = self.compute(stock_id, inputs, computed_at)
        if isinstance(results, MetricResult):
            return [results]
        return list(results)

    def compute(
        self,
        stock_id: int,
        inputs: Dict[str, Any],
        computed_at: datetime,
    ) -> Union[MetricResult, Iterable[MetricResult]]:
        """Subclasses must implement and return MetricResult(s)."""
        raise NotImplementedError
