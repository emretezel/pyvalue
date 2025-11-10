"""
A module which updates balance sheets of stocks in the database.
Author: Emre Tezel
"""

import sys
from datetime import datetime

from sqlalchemy import inspect, text
from sqlalchemy.schema import CreateColumn

from pyvalue.data.stock import Stock
from pyvalue.data.balance_sheet import BalanceSheet
from pyvalue.ingestion import Session
from pyvalue.ingestion import get_json_parsed_data


def ensure_balance_sheet_schema(session) -> None:
    """
    Ensure the balance_sheets table has all columns defined in the ORM model.
    Adds any missing columns so ingestion keeps working as the model evolves.
    """
    engine = session.get_bind()
    inspector = inspect(engine)
    table_name = BalanceSheet.__tablename__

    if table_name not in inspector.get_table_names():
        BalanceSheet.__table__.create(bind=engine, checkfirst=True)
        return

    existing_columns = {
        column["name"] for column in inspector.get_columns(table_name)
    }

    for column in BalanceSheet.__table__.columns:
        if column.name in existing_columns:
            continue

        column_ddl = CreateColumn(column).compile(dialect=engine.dialect)
        session.execute(
            text(f"ALTER TABLE {table_name} ADD COLUMN {column_ddl}")
        )


def upsert_balance_sheet_entry(session, stock, balance_sheet_entry) -> None:
    """Insert or update a balance sheet row for a given stock/date."""
    date = datetime.strptime(balance_sheet_entry["date"], "%Y-%m-%d").date()
    total_assets = balance_sheet_entry.get("totalAssets")
    total_liabilities = balance_sheet_entry.get("totalLiabilities")
    total_current_assets = balance_sheet_entry.get("totalCurrentAssets")
    total_current_liabilities = balance_sheet_entry.get("totalCurrentLiabilities")
    long_term_debt = balance_sheet_entry.get("longTermDebt")

    existing_balance_sheet = (
        session.query(BalanceSheet)
        .filter_by(stock_id=stock.id, date=date)
        .first()
    )

    if existing_balance_sheet is None:
        new_balance_sheet = BalanceSheet(
            stock_id=stock.id,
            date=date,
            total_assets=total_assets,
            total_liabilities=total_liabilities,
            total_current_assets=total_current_assets,
            total_current_liabilities=total_current_liabilities,
            long_term_debt=long_term_debt,
        )
        stock.balance_sheets.append(new_balance_sheet)
    else:
        existing_balance_sheet.total_assets = total_assets
        existing_balance_sheet.total_liabilities = total_liabilities
        existing_balance_sheet.total_current_assets = total_current_assets
        existing_balance_sheet.total_current_liabilities = total_current_liabilities
        existing_balance_sheet.long_term_debt = long_term_debt


def update_balance_sheet(symbol: str, api_key: str) -> None:
    """
    Updates the balance sheet of a stock in the database.
    :param symbol:
    :param api_key:
    :return:
    """
    with Session() as session:
        ensure_balance_sheet_schema(session)
        stock = session.query(Stock).filter_by(symbol=symbol).first()
        if stock is None:
            raise ValueError(f"Stock with symbol {symbol} not found in the database.")

        # Query FMP for the balance sheet data. Replace YOUR_API_KEY with your actual API key and symbol
        # with the stock symbol.
        url = (
            "https://financialmodelingprep.com/stable/balance-sheet-statement"
            f"?symbol={symbol}&period=quarter&apikey={api_key}"
        )

        response = get_json_parsed_data(url)

        # Iterate over the balance sheet data and update the database.
        for balance_sheet in response:
            upsert_balance_sheet_entry(session, stock, balance_sheet)

        session.commit()


if __name__ == "__main__":
    # Read the API key from parameters
    if len(sys.argv) > 2:
        api = sys.argv[1]
        ticker = sys.argv[2]
    else:
        # Raise an error if the API key is not provided
        raise ValueError("API key and stock symbol are required")

    update_balance_sheet(ticker, api)
