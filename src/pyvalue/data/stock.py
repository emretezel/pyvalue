"""
Includes Stock class which is mappable to a sqlite table.
Author: Emre Tezel
"""

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from pyvalue.data import Base
from pyvalue.data.balance_sheet import BalanceSheet
from pyvalue.data.metric_value import MetricValue
from pyvalue.data.earnings import EarningsReport


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
    metric_values = relationship("MetricValue", back_populates="stock")
    earnings_reports = relationship("EarningsReport", back_populates="stock")

    def __repr__(self):
        return (
            f"Stock(id={self.id}, symbol='{self.symbol}', "
            f"name='{self.name}', exchange='{self.exchange}')"
        )
