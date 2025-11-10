"""
A module to update tickers in the database.
Author: Emre Tezel
"""

import sys
from pyvalue.data.stock import Stock
from pyvalue.ingestion import Session, get_json_parsed_data

if __name__ == "__main__":
    # Read the API key from parameters
    if len(sys.argv) > 1:
        api_key = sys.argv[1]
    else:
        # Raise an error if the API key is not provided
        raise ValueError("API key is required")

    url_call = (
        "https://financialmodelingprep.com/api/v3/available-traded/list?apikey="
        + api_key
    )

    response_data = get_json_parsed_data(url_call)

    with Session() as session:
        # Iterate over the stocks and update the sqlite database
        for stock in response_data:
            # Create a new Stock object
            new_stock = Stock(
                symbol=stock["symbol"],
                name=stock["name"],
                exchange=stock["exchangeShortName"],
            )

            existing_stock = (
                session.query(Stock).filter_by(symbol=new_stock.symbol).first()
            )

            if existing_stock is None:
                session.add(new_stock)

        session.commit()
