"""
Model for per-period earnings data.
Author: Emre Tezel
"""

from sqlalchemy import Column, Date, Float, ForeignKey, Integer
from sqlalchemy.orm import relationship

from pyvalue.data import Base


class EarningsReport(Base):
    __tablename__ = "earnings_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False)
    date = Column(Date, nullable=False)
    actual_eps = Column(Float, nullable=True)

    stock = relationship("Stock", back_populates="earnings_reports")
