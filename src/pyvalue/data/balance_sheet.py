"""
A module that represents a balance sheet of a stock as of a specific date.
Author: Emre Tezel
"""

from sqlalchemy import Column, Integer, ForeignKey, Date, Float
from sqlalchemy.orm import relationship
from pyvalue.data import Base


class BalanceSheet(Base):
    __tablename__ = "balance_sheets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False)
    date = Column(Date, nullable=False)
    total_assets = Column(Float, nullable=False)
    total_liabilities = Column(Float, nullable=False)
    long_term_debt = Column(Float, nullable=True)

    stock = relationship("Stock", back_populates="balance_sheets")
