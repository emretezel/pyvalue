"""Unit tests for the SQLAlchemy models under pyvalue.data."""

from datetime import date

import pytest
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

from pyvalue.data import Base
from pyvalue.data.balance_sheet import BalanceSheet
from pyvalue.data.stock import Stock
from pyvalue.ingestion.update_balance_sheet import (
    ensure_balance_sheet_schema,
    upsert_balance_sheet_entry,
)


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
        long_term_debt=10.0,
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
    assert retrieved_sheet.long_term_debt == 10.0


def test_schema_sync_adds_missing_columns():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE stocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol VARCHAR NOT NULL,
                    name VARCHAR,
                    exchange VARCHAR
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE balance_sheets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_id INTEGER NOT NULL,
                    date DATE NOT NULL,
                    total_assets FLOAT NOT NULL,
                    total_liabilities FLOAT NOT NULL
                )
                """
            )
        )

    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        ensure_balance_sheet_schema(session)
        inspector = inspect(engine)
        column_names = {
            column["name"]
            for column in inspector.get_columns("balance_sheets")
        }
        assert "long_term_debt" in column_names
    finally:
        session.close()


def test_upsert_balance_sheet_inserts_and_updates(session):
    stock = Stock(symbol="UPS", name="Upsert Corp", exchange="NYSE")
    session.add(stock)
    session.commit()

    initial_entry = {
        "date": "2021-03-31",
        "totalAssets": 200.0,
        "totalLiabilities": 75.0,
        "longTermDebt": 30.0,
    }

    upsert_balance_sheet_entry(session, stock, initial_entry)
    session.commit()

    sheet = session.query(BalanceSheet).filter_by(stock_id=stock.id).one()
    assert sheet.total_assets == 200.0
    assert sheet.total_liabilities == 75.0
    assert sheet.long_term_debt == 30.0

    updated_entry = {
        "date": "2021-03-31",
        "totalAssets": 250.0,
        "totalLiabilities": 80.0,
        "longTermDebt": 35.0,
    }

    upsert_balance_sheet_entry(session, stock, updated_entry)
    session.commit()

    updated_sheet = session.query(BalanceSheet).filter_by(stock_id=stock.id).one()
    assert updated_sheet.total_assets == 250.0
    assert updated_sheet.total_liabilities == 80.0
    assert updated_sheet.long_term_debt == 35.0
