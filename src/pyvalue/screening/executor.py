"""
Database-backed screening executor.
Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from sqlalchemy import and_, func
from sqlalchemy.orm import Session, aliased

from pyvalue.data.metric_value import MetricValue
from pyvalue.data.stock import Stock
from pyvalue.screening.config import FilterSpec, ScreenSpec


_OPERATORS = {
    ">": lambda column, value: column > value,
    ">=": lambda column, value: column >= value,
    "<": lambda column, value: column < value,
    "<=": lambda column, value: column <= value,
    "==": lambda column, value: column == value,
    "!=": lambda column, value: column != value,
}


@dataclass
class FilterResult:
    spec: FilterSpec
    rows: Dict[int, MetricValue]  # keyed by stock_id


def _latest_metric_subquery(metric_name: str, session: Session):
    return (
        session.query(
            MetricValue.stock_id.label("stock_id"),
            func.max(MetricValue.data_from_date).label("max_date"),
        )
        .filter(MetricValue.metric_name == metric_name)
        .group_by(MetricValue.stock_id)
        .subquery()
    )


def _fetch_filter_rows(session: Session, spec: FilterSpec) -> Dict[int, MetricValue]:
    mv = aliased(MetricValue)
    query = session.query(mv).filter(mv.metric_name == spec.metric)
    comparator = _OPERATORS[spec.operator]
    query = query.filter(comparator(mv.value, spec.value))

    if spec.scope == "latest":
        latest = _latest_metric_subquery(spec.metric, session)
        query = query.join(
            latest,
            and_(
                mv.stock_id == latest.c.stock_id,
                mv.data_from_date == latest.c.max_date,
            ),
        )

    query = query.order_by(mv.data_from_date.desc())

    rows = {}
    for record in query.all():
        if record.stock_id not in rows:
            rows[record.stock_id] = record

    return rows


def apply_screen(session: Session, screen: ScreenSpec) -> List[dict]:
    """Return stocks meeting all filters along with their evaluation details."""
    filter_results: List[FilterResult] = []
    matching_ids = None

    for spec in screen.filters:
        rows = _fetch_filter_rows(session, spec)
        stock_ids = set(rows.keys())
        if matching_ids is None:
            matching_ids = stock_ids
        else:
            matching_ids &= stock_ids
        filter_results.append(FilterResult(spec=spec, rows=rows))

    matching_ids = matching_ids or set()
    if not matching_ids:
        return []

    stocks = (
        session.query(Stock)
        .filter(Stock.id.in_(matching_ids))
        .order_by(Stock.symbol.asc())
        .all()
    )
    stock_map = {stock.id: stock for stock in stocks}

    results = []
    for stock_id in matching_ids:
        stock = stock_map.get(stock_id)
        if stock is None:
            continue

        filter_details = []
        for filter_result in filter_results:
            row = filter_result.rows.get(stock_id)
            if row is None:
                continue
            filter_details.append(
                {
                    "metric": filter_result.spec.metric,
                    "operator": filter_result.spec.operator,
                    "threshold": filter_result.spec.value,
                    "value": row.value,
                    "data_from_date": row.data_from_date,
                }
            )

        results.append({"stock": stock, "filters": filter_details})

    results.sort(key=lambda entry: entry["stock"].symbol)
    return results
