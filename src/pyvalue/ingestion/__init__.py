"""
This module contains common attributes and methods for ingestion scripts.
Author: Emre Tezel
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pyvalue.data import Base
from urllib.request import urlopen
import certifi
import json

# Define the database file location
db_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
os.makedirs(db_dir, exist_ok=True)
db_path = os.path.join(db_dir, "stocks.db")

# Create an SQLite database engine
engine = create_engine(f"sqlite:///{db_path}", echo=True)

# Create tables if they don't exist
Base.metadata.create_all(engine)

# Define a session factory
Session = sessionmaker(bind=engine)


def get_json_parsed_data(url: str) -> dict:
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)
