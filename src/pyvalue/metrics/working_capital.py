"""
Working capital metric implementation.
Author: Emre Tezel
"""

from __future__ import annotations

from typing import Dict

from pyvalue.metrics.base import Metric, MetricResult, MetricPrerequisiteMissing


class WorkingCapital(Metric):
    name = "working_capital"
    requires = ("balance_sheet_latest",)

    def compute(
        self,
        stock_id: int,
        inputs: Dict[str, object],
        computed_at,
    ):
        sheet = inputs.get("balance_sheet_latest")
        if sheet is None:
            raise MetricPrerequisiteMissing(
                "Latest balance sheet required to compute working capital."
            )

        current_assets = sheet.total_current_assets
        current_liabilities = sheet.total_current_liabilities
        if current_assets is None or current_liabilities is None:
            raise MetricPrerequisiteMissing(
                "Working capital requires current assets and liabilities."
            )

        value = current_assets - current_liabilities

        return MetricResult(
            stock_id=stock_id,
            metric_name=self.name,
            value=value,
            data_from_date=sheet.date,
            computed_at=computed_at,
            metadata={"balance_sheet_id": sheet.id},
        )
