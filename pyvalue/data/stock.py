"""
Includes Stock class which is mappable to a sqlite table.
Author: Emre Tezel
"""

from sqlalchemy import Column, Integer, String
from pyvalue.data.common import Base
from sqlalchemy.orm import relationship


class Stock(Base):
    """
    A class representing a stock, mappable to a SQLite table.
    """

    __tablename__ = "stocks"
    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True)
    name = Column(String)
    exchange = Column(String)
    balance_sheets = relationship("BalanceSheet", back_populates="stock")

    def __repr__(self):
        return f"Stock(id={self.id}, symbol='{self.symbol}', name='{self.name}', exchange='{self.exchange}')"
