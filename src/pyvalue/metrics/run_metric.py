"""
Script to evaluate metrics for a stock and store results.
Author: Emre Tezel
"""

import json
import sys
from datetime import datetime, timezone
from typing import Type

from pyvalue.data.stock import Stock
from pyvalue.data.balance_sheet import BalanceSheet
from pyvalue.data.metric_value import MetricValue
from pyvalue.metrics import (
    DataAccess,
    WorkingCapital,
    EpsStreak,
    Metric,
    MetricResult,
)
from pyvalue.ingestion import Session

METRIC_REGISTRY = {
    "workingcapital": WorkingCapital,
    "working_capital": WorkingCapital,
    "WorkingCapital": WorkingCapital,
    "epsstreak": EpsStreak,
    "eps_streak": EpsStreak,
    "EPSStreak": EpsStreak,
}


def get_metric_class(metric_name: str) -> Type[Metric]:
    if metric_name not in METRIC_REGISTRY:
        raise ValueError(f"Unknown metric '{metric_name}'. Known metrics: {list(METRIC_REGISTRY)}")
    return METRIC_REGISTRY[metric_name]


def upsert_metric_value(session, result: MetricResult) -> None:
    metadata_json = json.dumps(result.metadata) if result.metadata else None
    existing = (
        session.query(MetricValue)
        .filter_by(
            stock_id=result.stock_id,
            metric_name=result.metric_name,
            data_from_date=result.data_from_date,
        )
        .first()
    )
    if existing:
        existing.value = result.value
        existing.computed_at = result.computed_at
        existing.metadata_json = metadata_json
    else:
        session.add(
            MetricValue(
                stock_id=result.stock_id,
                metric_name=result.metric_name,
                value=result.value,
                data_from_date=result.data_from_date,
                computed_at=result.computed_at,
                metadata_json=metadata_json,
            )
        )


def calculate_metric_for_symbol(session, symbol: str, metric: Metric):
    stock = session.query(Stock).filter_by(symbol=symbol).first()
    if stock is None:
        raise ValueError(f"Stock with symbol '{symbol}' not found.")

    sheets = (
        session.query(BalanceSheet)
        .filter_by(stock_id=stock.id)
        .order_by(BalanceSheet.date.asc())
        .all()
    )
    if not sheets:
        raise ValueError(f"No balance sheets found for stock '{symbol}'.")

    data_access = DataAccess(session)
    results = []
    for sheet in sheets:
        data_access.register_fetcher(
            "balance_sheet_latest", lambda _stock_id, sheet=sheet: sheet
        )
        computed_at = datetime.now(timezone.utc)
        metric_results = metric.evaluate(
            data_access, stock.id, computed_at=computed_at
        )
        for result in metric_results:
            results.append(result)
            upsert_metric_value(session, result)

    session.commit()
    return results


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: python -m pyvalue.metrics.run_metric <SYMBOL> [SYMBOL ...] <MetricName>"
        )
        sys.exit(1)

    metric_name = sys.argv[-1]
    symbols = sys.argv[1:-1]

    metric_cls = get_metric_class(metric_name)
    metric = metric_cls()

    with Session() as session:
        for symbol in symbols:
            calculate_metric_for_symbol(session, symbol, metric)


if __name__ == "__main__":
    main()
