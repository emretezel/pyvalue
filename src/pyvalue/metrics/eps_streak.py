"""
EPS streak metric implementation.
Author: Emre Tezel
"""

from __future__ import annotations

from typing import Dict, List

from pyvalue.metrics.base import Metric, MetricResult, MetricPrerequisiteMissing


class EpsStreak(Metric):
    name = "eps_streak"
    requires = ("earnings_history",)

    def compute(self, stock_id: int, inputs: Dict[str, object], computed_at):
        history: List = inputs.get("earnings_history") or []
        if not history:
            raise MetricPrerequisiteMissing(
                "EPS streak requires earnings history for the stock."
            )

        streak = 0
        for report in history:
            actual = report.actual_eps
            if actual is None or actual <= 0:
                break
            streak += 1

        latest_date = history[0].date

        return MetricResult(
            stock_id=stock_id,
            metric_name=self.name,
            value=float(streak),
            data_from_date=latest_date,
            computed_at=computed_at,
        )
