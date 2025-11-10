"""Unit tests for the SQLAlchemy models under pyvalue.data."""

from datetime import date

import pytest
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

from pyvalue.data import Base
from pyvalue.data.balance_sheet import BalanceSheet
from pyvalue.data.earnings import EarningsReport
from pyvalue.data.stock import Stock
from pyvalue.ingestion.update_balance_sheet import (
    ensure_balance_sheet_schema,
    upsert_balance_sheet_entry,
)
from pyvalue.ingestion.update_earnings import upsert_earnings_report, _normalize_entries


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
        total_current_assets=60.0,
        total_current_liabilities=15.0,
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
    assert retrieved_sheet.total_current_assets == 60.0
    assert retrieved_sheet.total_current_liabilities == 15.0
    assert retrieved_sheet.long_term_debt == 10.0


def test_balance_sheet_long_term_debt_optional(session):
    stock = Stock(symbol="NTD", name="No Debt Corp", exchange="NYSE")
    sheet = BalanceSheet(
        date=date(2021, 6, 30),
        total_assets=150.0,
        total_liabilities=50.0,
    )
    stock.balance_sheets.append(sheet)

    session.add(stock)
    session.commit()

    retrieved_sheet = (
        session.query(BalanceSheet).filter_by(stock_id=stock.id).one()
    )
    assert retrieved_sheet.total_current_assets is None
    assert retrieved_sheet.total_current_liabilities is None
    assert retrieved_sheet.long_term_debt is None


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
        assert "total_current_assets" in column_names
        assert "total_current_liabilities" in column_names
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
        "totalCurrentAssets": 120.0,
        "totalCurrentLiabilities": 40.0,
        "longTermDebt": 30.0,
    }

    upsert_balance_sheet_entry(session, stock, initial_entry)
    session.commit()

    sheet = session.query(BalanceSheet).filter_by(stock_id=stock.id).one()
    assert sheet.total_assets == 200.0
    assert sheet.total_liabilities == 75.0
    assert sheet.total_current_assets == 120.0
    assert sheet.total_current_liabilities == 40.0
    assert sheet.long_term_debt == 30.0

    updated_entry = {
        "date": "2021-03-31",
        "totalAssets": 250.0,
        "totalLiabilities": 80.0,
        "totalCurrentAssets": 130.0,
        "totalCurrentLiabilities": 42.0,
        "longTermDebt": 35.0,
    }

    upsert_balance_sheet_entry(session, stock, updated_entry)
    session.commit()

    updated_sheet = session.query(BalanceSheet).filter_by(stock_id=stock.id).one()
    assert updated_sheet.total_assets == 250.0
    assert updated_sheet.total_liabilities == 80.0
    assert updated_sheet.total_current_assets == 130.0
    assert updated_sheet.total_current_liabilities == 42.0
    assert updated_sheet.long_term_debt == 35.0


def test_earnings_report_relationship(session):
    stock = Stock(symbol="ERN", name="Earnings Corp", exchange="NYSE")
    report = EarningsReport(
        date=date(2022, 12, 31),
        actual_eps=2.5,
    )
    stock.earnings_reports.append(report)

    session.add(stock)
    session.commit()

    retrieved = session.query(Stock).filter_by(symbol="ERN").one()
    assert len(retrieved.earnings_reports) == 1
    stored = retrieved.earnings_reports[0]
    assert stored.actual_eps == 2.5
    assert stored.stock is retrieved


def test_upsert_earnings_report(session):
    stock = Stock(symbol="EPS", name="EPS Inc", exchange="NASDAQ")
    session.add(stock)
    session.commit()

    entry = {
        "date": "2023-03-31",
        "epsActual": 1.2,
    }

    upsert_earnings_report(session, stock, entry)
    session.commit()

    report = session.query(EarningsReport).filter_by(stock_id=stock.id).one()
    assert report.actual_eps == 1.2
    assert report.stock is stock

    updated_entry = {
        "date": "2023-03-31",
        "eps": 1.25,
    }

    upsert_earnings_report(session, stock, updated_entry)
    session.commit()

    updated = session.query(EarningsReport).filter_by(stock_id=stock.id).one()
    assert updated.actual_eps == 1.25
    assert updated.actual_eps == 1.25


def test_normalize_entries_handles_dict():
    payload = {
        "symbol": "ABC",
        "historical": [
            {"date": "2023-03-31", "eps": 1.0},
        ],
    }
    entries = _normalize_entries(payload)
    assert isinstance(entries, list)
    assert entries[0]["date"] == "2023-03-31"


def test_upsert_earnings_skips_when_actual_missing(session):
    stock = Stock(symbol="MISS", name="Missing EPS", exchange="NYSE")
    session.add(stock)
    session.commit()

    entry = {
        "date": "2024-03-31",
        "epsActual": None,
        "actualEPS": None,
    }

    upsert_earnings_report(session, stock, entry)
    session.commit()

    count = session.query(EarningsReport).filter_by(stock_id=stock.id).count()
    assert count == 0
