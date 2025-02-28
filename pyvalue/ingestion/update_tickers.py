"""
A module to update tickers in the database.
Author: Emre Tezel
"""

from urllib.request import urlopen
import certifi
import json
import sys
from pyvalue.data.stock import Stock
from pyvalue.data.common import Base
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_json_parsed_data(url: str) -> dict:
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)


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

    # Initialise the sqlite database located under data directory
    # and create the stocks table if it doesn't exist.
    db_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, "stocks.db")

    # Create an SQLite database
    engine = create_engine(f"sqlite:///{db_path}", echo=True)
    Base.metadata.create_all(engine)

    # Create a session factory
    Session = sessionmaker(bind=engine)
    session = Session()

    # Iterate over the stocks and update the sqlite database
    for stock in response_data:
        # Create a new Stock object
        new_stock = Stock(
            symbol=stock["symbol"],
            name=stock["name"],
            exchange=stock["exchangeShortName"],
        )

        existing_stock = session.query(Stock).filter_by(symbol=new_stock.symbol).first()

        if existing_stock is None:
            session.add(new_stock)

    session.commit()
