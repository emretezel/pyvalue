# Author: Emre Tezel
"""Abstract metric base classes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, Sequence


@dataclass
class MetricResult:
    """Represents the computed value of a metric for a symbol."""

    symbol: str
    metric_id: str
    value: float
    as_of: str


class Metric(Protocol):
    """Protocol that all metric implementations must follow."""

    id: str
    required_concepts: Sequence[str]

    def compute(self, symbol: str, repo) -> Optional[MetricResult]:
        ...
