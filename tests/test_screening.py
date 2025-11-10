"""Tests for screening configuration and execution."""

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pyvalue.data import Base
from pyvalue.data.metric_value import MetricValue
from pyvalue.data.stock import Stock
from pyvalue.screening.config import load_screening_config, ScreenSpec, FilterSpec
from pyvalue.screening.executor import apply_screen


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    db_session = SessionLocal()
    try:
        yield db_session
    finally:
        db_session.close()
        Base.metadata.drop_all(engine)


def test_load_screening_config(tmp_path):
    config_path = tmp_path / "screens.yaml"
    config_path.write_text(
        """
screens:
  - name: positive_wc
    filters:
      - metric: working_capital
        operator: ">"
        value: 0
"""
    )

    config = load_screening_config(config_path)
    screen = config.get("positive_wc")
    assert screen.name == "positive_wc"
    assert len(screen.filters) == 1
    assert screen.filters[0].metric == "working_capital"
    assert screen.filters[0].scope == "latest"


def test_apply_screen_returns_matching_stocks(session):
    now = datetime.now(timezone.utc)
    # create stocks
    stock_a = Stock(symbol="AAA", name="Alpha", exchange="NYSE")
    stock_b = Stock(symbol="BBB", name="Beta", exchange="NYSE")
    session.add_all([stock_a, stock_b])
    session.flush()

    session.add_all(
        [
            MetricValue(
                stock_id=stock_a.id,
                metric_name="working_capital",
                value=10,
                data_from_date=date(2022, 12, 31),
                computed_at=now,
            ),
            MetricValue(
                stock_id=stock_b.id,
                metric_name="working_capital",
                value=-5,
                data_from_date=date(2022, 12, 31),
                computed_at=now,
            ),
        ]
    )
    session.commit()

    screen = ScreenSpec(
        name="positive_wc",
        filters=[FilterSpec(metric="working_capital", operator=">", value=0)],
    )

    results = apply_screen(session, screen)

    assert len(results) == 1
    assert results[0]["stock"].symbol == "AAA"
    assert results[0]["filters"][0]["value"] == 10
