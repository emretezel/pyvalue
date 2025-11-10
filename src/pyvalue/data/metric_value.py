"""
MetricValue model stores calculated metric outputs for stocks.
Author: Emre Tezel
"""

from datetime import datetime, date, timezone
import json

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from pyvalue.data import Base


class MetricValue(Base):
    __tablename__ = "metric_values"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False)
    metric_name = Column(String, nullable=False)
    value = Column(Float, nullable=False)
    data_from_date = Column(Date, nullable=False)
    computed_at = Column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    metadata_json = Column(Text, nullable=True)

    stock = relationship("Stock", back_populates="metric_values")

    @property
    def metadata_dict(self):
        return json.loads(self.metadata_json) if self.metadata_json else None

    @metadata_dict.setter
    def metadata_dict(self, value):
        self.metadata_json = json.dumps(value) if value is not None else None
