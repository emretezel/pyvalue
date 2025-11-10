"""Tests for the metric framework."""

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pyvalue.data import Base
from pyvalue.data.stock import Stock
from pyvalue.data.balance_sheet import BalanceSheet
from pyvalue.metrics import (
    DataAccess,
    MetricPrerequisiteMissing,
    WorkingCapital,
)
from pyvalue.metrics.run_metric import calculate_metric_for_symbol, get_metric_class
from pyvalue.data.metric_value import MetricValue


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    db_session = SessionLocal()
    try:
        yield db_session
    finally:
        db_session.close()
        Base.metadata.drop_all(engine)


def test_working_capital_uses_latest_balance_sheet(session):
    stock = Stock(symbol="WKC", name="Working Capital Inc", exchange="NYSE")
    session.add(stock)
    session.flush()

    older = BalanceSheet(
        stock_id=stock.id,
        date=date(2020, 12, 31),
        total_assets=100,
        total_liabilities=40,
        total_current_assets=60,
        total_current_liabilities=20,
    )
    newer = BalanceSheet(
        stock_id=stock.id,
        date=date(2021, 12, 31),
        total_assets=150,
        total_liabilities=70,
        total_current_assets=80,
        total_current_liabilities=30,
    )
    session.add_all([older, newer])
    session.commit()

    metric = WorkingCapital()
    data_access = DataAccess(session)

    result = metric.evaluate(data_access, stock.id)[0]

    assert result.value == 50  # 80 - 30 from the latest balance sheet
    assert result.data_from_date == date(2021, 12, 31)
    assert result.metric_name == "working_capital"


def test_working_capital_requires_current_fields(session):
    stock = Stock(symbol="MISS", name="Missing Fields Corp", exchange="NASDAQ")
    session.add(stock)
    session.flush()

    sheet = BalanceSheet(
        stock_id=stock.id,
        date=date(2021, 6, 30),
        total_assets=90,
        total_liabilities=40,
    )
    session.add(sheet)
    session.commit()

    metric = WorkingCapital()
    data_access = DataAccess(session)

    with pytest.raises(MetricPrerequisiteMissing):
        metric.evaluate(data_access, stock.id)


def test_calculate_metric_for_symbol_persists_values(session):
    stock = Stock(symbol="SYM", name="Symbol Corp", exchange="NYSE")
    session.add(stock)
    session.flush()

    b1 = BalanceSheet(
        stock_id=stock.id,
        date=date(2020, 12, 31),
        total_assets=90,
        total_liabilities=30,
        total_current_assets=55,
        total_current_liabilities=20,
    )
    b2 = BalanceSheet(
        stock_id=stock.id,
        date=date(2021, 12, 31),
        total_assets=120,
        total_liabilities=40,
        total_current_assets=70,
        total_current_liabilities=25,
    )
    session.add_all([b1, b2])
    session.commit()

    metric = WorkingCapital()
    calculate_metric_for_symbol(session, "SYM", metric)

    values = (
        session.query(MetricValue)
        .filter_by(stock_id=stock.id, metric_name="working_capital")
        .order_by(MetricValue.data_from_date.asc())
        .all()
    )
    assert len(values) == 2
    assert values[0].value == 35  # 55 - 20
    assert values[1].value == 45  # 70 - 25


def test_get_metric_class_unknown(monkeypatch):
    with pytest.raises(ValueError):
        get_metric_class("UnknownMetric")
