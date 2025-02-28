"""
A module which updates balance sheets of stocks in the database.
Author: Emre Tezel
"""

from pyvalue.data.stock import Stock
from pyvalue.data.balance_sheet import BalanceSheet
from pyvalue.ingestion import Session
from pyvalue.ingestion import get_json_parsed_data
import sys
from datetime import datetime


def update_balance_sheet(symbol: str, api_key: str) -> None:
    """
    Updates the balance sheet of a stock in the database.
    :param symbol:
    :param api_key:
    :return:
    """
    with Session() as session:
        stock = session.query(Stock).filter_by(symbol=symbol).first()
        if stock is None:
            raise ValueError(f"Stock with symbol {symbol} not found in the database.")

        # Query FMP for the balance sheet data. Replace YOUR_API_KEY with your actual API key and symbol
        # with the stock symbol.
        url = (
            f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}"
            f"?period=annual&apikey={api_key}"
        )

        response = get_json_parsed_data(url)

        # Iterate over the balance sheet data and update the database.
        for balance_sheet in response:
            date = balance_sheet["date"]
            date = datetime.strptime(date, "%Y-%m-%d").date()
            total_assets = balance_sheet["totalAssets"]
            total_liabilities = balance_sheet["totalLiabilities"]

            # Check if the balance sheet already exists in the database
            existing_balance_sheet = (
                session.query(BalanceSheet)
                .filter_by(stock_id=stock.id, date=date)
                .first()
            )

            if existing_balance_sheet is None:
                # Create a new BalanceSheet object
                new_balance_sheet = BalanceSheet(
                    stock_id=stock.id,
                    date=date,
                    total_assets=total_assets,
                    total_liabilities=total_liabilities,
                )

                stock.balance_sheets.append(new_balance_sheet)

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
