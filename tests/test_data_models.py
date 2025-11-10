"""Unit tests for the SQLAlchemy models under pyvalue.data."""

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pyvalue.data import Base
from pyvalue.data.balance_sheet import BalanceSheet
from pyvalue.data.stock import Stock


@pytest.fixture
def session():
    """Provide an isolated in-memory database session per test."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    db_session = SessionLocal()
    try:
        yield db_session
    finally:
        db_session.close()
        Base.metadata.drop_all(engine)


def test_stock_persists_and_repr(session):
    stock = Stock(symbol="TEST", name="Test Corp", exchange="NYSE")
    session.add(stock)
    session.commit()

    retrieved = session.query(Stock).filter_by(symbol="TEST").one()

    assert retrieved.name == "Test Corp"
    assert retrieved.exchange == "NYSE"
    assert repr(retrieved).startswith("Stock(")


def test_balance_sheet_relationship(session):
    stock = Stock(symbol="VAL", name="Value Inc", exchange="NASDAQ")
    sheet = BalanceSheet(
        date=date(2020, 12, 31),
        total_assets=100.0,
        total_liabilities=25.0,
    )
    stock.balance_sheets.append(sheet)

    session.add(stock)
    session.commit()

    retrieved_stock = session.query(Stock).filter_by(symbol="VAL").one()
    assert len(retrieved_stock.balance_sheets) == 1

    retrieved_sheet = retrieved_stock.balance_sheets[0]
    assert retrieved_sheet.stock_id == retrieved_stock.id
    assert retrieved_sheet.stock is retrieved_stock
    assert retrieved_sheet.total_assets == 100.0
    assert retrieved_sheet.total_liabilities == 25.0
