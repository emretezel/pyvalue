"""
Ingests earnings data for stocks.
Author: Emre Tezel
"""

import sys
from datetime import datetime
from typing import Iterable, List

from sqlalchemy import inspect

from pyvalue.data.stock import Stock
from pyvalue.data.earnings import EarningsReport
from pyvalue.ingestion import Session, get_json_parsed_data


EARNINGS_URL = (
    "https://financialmodelingprep.com/stable/earnings"
    "?symbol={symbol}&apikey={api_key}"
)


def ensure_earnings_table(session):
    """Ensure the earnings_reports table exists."""
    engine = session.get_bind()
    inspector = inspect(engine)
    if EarningsReport.__tablename__ not in inspector.get_table_names():
        EarningsReport.__table__.create(bind=engine, checkfirst=True)


def _normalize_entries(payload) -> List[dict]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("historical", "earnings", "results", "data", "items"):
            data = payload.get(key)
            if isinstance(data, list):
                return data
        return []
    return []


def _parse_date(entry: dict):
    for key in ("date", "fiscalDateEnding", "reportedDate"):
        value = entry.get(key)
        if value:
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                continue
    return None


def _parse_float(entry: dict, *keys):
    for key in keys:
        if key in entry:
            value = entry.get(key)
            if value in (None, ""):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None


def upsert_earnings_report(session, stock: Stock, entry: dict):
    report_date = _parse_date(entry)
    if report_date is None:
        return

    actual_eps = _parse_float(
        entry,
        "epsActual",
        "actualEPS",
        "actualEps",
        "eps",
    )
    if actual_eps is None:
        return

    existing = (
        session.query(EarningsReport)
        .filter_by(stock_id=stock.id, date=report_date)
        .first()
    )

    if existing is None:
        report = EarningsReport(
            stock_id=stock.id,
            date=report_date,
            actual_eps=actual_eps,
        )
        stock.earnings_reports.append(report)
    else:
        existing.actual_eps = actual_eps


def update_earnings(symbol: str, api_key: str) -> None:
    with Session() as session:
        ensure_earnings_table(session)

        stock = session.query(Stock).filter_by(symbol=symbol).first()
        if stock is None:
            raise ValueError(f"Stock with symbol {symbol} not found.")

        url = EARNINGS_URL.format(symbol=symbol, api_key=api_key)
        payload = get_json_parsed_data(url)
        entries = _normalize_entries(payload)

        for entry in entries:
            upsert_earnings_report(session, stock, entry)

        session.commit()


def main():
    if len(sys.argv) < 3:
        raise ValueError(
            "Usage: python -m pyvalue.ingestion.update_earnings <API_KEY> <SYMBOL> [SYMBOL ...]"
        )

    api_key = sys.argv[1]
    symbols = sys.argv[2:]
    for symbol in symbols:
        update_earnings(symbol, api_key)


if __name__ == "__main__":
    main()
