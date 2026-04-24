"""Metric freshness guards.

Author: Emre Tezel
"""

from datetime import date, timedelta

from pyvalue.metrics.eps_quarterly import EarningsPerShareTTM
from pyvalue.metrics.eps_average import EPSAverageSixYearMetric
from pyvalue.metrics.long_term_debt import LongTermDebtMetric
from pyvalue.metrics.roc_greenblatt import ROCGreenblattMetric
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS
from pyvalue.storage import (
    FactRecord,
    FinancialFactsRepository,
    MarketDataRepository,
    SupportedTickerRepository,
)
from pyvalue.universe import Listing


def _store_market_currency(db_path, symbol: str, as_of: str, currency: str = "USD"):
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    ticker_repo.replace_from_listings(
        "SEC",
        "US",
        [
            Listing(
                symbol=symbol,
                security_name=symbol,
                exchange="NYSE",
                currency=currency,
            )
        ],
    )
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    market_repo.upsert_price(symbol, as_of, 10.0, currency=currency)


def test_metric_skips_when_latest_fact_is_stale(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "facts.db")
    repo.initialize_schema()
    stale_date = (date.today() - timedelta(days=MAX_FACT_AGE_DAYS + 1)).isoformat()
    repo.replace_facts(
        "AAPL.US",
        [
            FactRecord(
                symbol="AAPL.US",
                concept="LongTermDebt",
                fiscal_period="FY",
                end_date=stale_date,
                unit="USD",
                value=150.0,
            )
        ],
    )
    _store_market_currency(tmp_path / "facts.db", "AAPL.US", stale_date)

    metric = LongTermDebtMetric()

    assert metric.compute("AAPL.US", repo) is None


def test_ttm_metric_requires_recent_quarters(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "quarters.db")
    repo.initialize_schema()
    today = date.today()
    records = []
    for idx, months_ago in enumerate((1, 3, 4, 5), start=1):
        records.append(
            FactRecord(
                symbol="AAPL.US",
                concept="EarningsPerShare",
                fiscal_period=f"Q{idx}",
                end_date=(today - timedelta(days=months_ago * 30)).isoformat(),
                unit="USD",
                value=float(idx),
            )
        )
    repo.replace_facts("AAPL.US", records)
    _store_market_currency(
        tmp_path / "quarters.db",
        "AAPL.US",
        (today - timedelta(days=30)).isoformat(),
    )

    metric = EarningsPerShareTTM()
    result = metric.compute("AAPL.US", repo)

    assert result is not None
    assert result.value == sum(float(idx) for idx in range(1, 5))


def test_ttm_metric_skips_when_latest_quarter_is_stale(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "stale_quarters.db")
    repo.initialize_schema()
    today = date.today()
    records = []
    for idx, days_ago in enumerate(
        (
            MAX_FACT_AGE_DAYS + 10,
            MAX_FACT_AGE_DAYS + 70,
            MAX_FACT_AGE_DAYS + 160,
            MAX_FACT_AGE_DAYS + 250,
        ),
        start=1,
    ):
        records.append(
            FactRecord(
                symbol="AAPL.US",
                concept="EarningsPerShare",
                fiscal_period=f"Q{idx}",
                end_date=(today - timedelta(days=days_ago)).isoformat(),
                unit="USD",
                value=float(idx),
            )
        )
    repo.replace_facts("AAPL.US", records)
    _store_market_currency(
        tmp_path / "stale_quarters.db",
        "AAPL.US",
        (today - timedelta(days=MAX_FACT_AGE_DAYS + 10)).isoformat(),
    )

    metric = EarningsPerShareTTM()

    assert metric.compute("AAPL.US", repo) is None


def test_fy_metric_accepts_when_recent_quarter_exists(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "epsavg.db")
    repo.initialize_schema()
    # Six FY records older than a year.
    fy_records = []
    for year in range(2018, 2024):
        fy_records.append(
            FactRecord(
                symbol="AAPL.US",
                concept="EarningsPerShare",
                fiscal_period="FY",
                end_date=f"{year}-12-31",
                unit="USD",
                value=float(year),
                frame=f"CY{year}",
            )
        )
    # Recent quarterly record to satisfy freshness requirement.
    fy_records.append(
        FactRecord(
            symbol="AAPL.US",
            concept="EarningsPerShare",
            fiscal_period="Q3",
            end_date=(date.today() - timedelta(days=60)).isoformat(),
            unit="USD",
            value=1.0,
        )
    )
    repo.replace_facts("AAPL.US", fy_records)
    _store_market_currency(
        tmp_path / "epsavg.db",
        "AAPL.US",
        (date.today() - timedelta(days=60)).isoformat(),
    )

    metric = EPSAverageSixYearMetric()
    result = metric.compute("AAPL.US", repo)

    assert result is not None


def test_roc_metric_uses_recent_concept_even_if_fy_old(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "roc.db")
    repo.initialize_schema()
    # FY data older than a year.
    fy_old = (date.today() - timedelta(days=500)).isoformat()
    # Recent quarterly facts to satisfy freshness.
    repo.replace_facts(
        "TEST.US",
        [
            FactRecord(
                symbol="TEST.US",
                concept="OperatingIncomeLoss",
                fiscal_period="FY",
                end_date=fy_old,
                unit="USD",
                value=200.0,
            ),
            FactRecord(
                symbol="TEST.US",
                concept="PropertyPlantAndEquipmentNet",
                fiscal_period="FY",
                end_date=fy_old,
                unit="USD",
                value=100.0,
            ),
            FactRecord(
                symbol="TEST.US",
                concept="AssetsCurrent",
                fiscal_period="FY",
                end_date=fy_old,
                unit="USD",
                value=50.0,
            ),
            FactRecord(
                symbol="TEST.US",
                concept="LiabilitiesCurrent",
                fiscal_period="FY",
                end_date=fy_old,
                unit="USD",
                value=25.0,
            ),
            FactRecord(
                symbol="TEST.US",
                concept="OperatingIncomeLoss",
                fiscal_period="Q3",
                end_date=(date.today() - timedelta(days=45)).isoformat(),
                unit="USD",
                value=180.0,
            ),
            FactRecord(
                symbol="TEST.US",
                concept="PropertyPlantAndEquipmentNet",
                fiscal_period="Q3",
                end_date=(date.today() - timedelta(days=45)).isoformat(),
                unit="USD",
                value=90.0,
            ),
            FactRecord(
                symbol="TEST.US",
                concept="AssetsCurrent",
                fiscal_period="Q3",
                end_date=(date.today() - timedelta(days=45)).isoformat(),
                unit="USD",
                value=40.0,
            ),
            FactRecord(
                symbol="TEST.US",
                concept="LiabilitiesCurrent",
                fiscal_period="Q3",
                end_date=(date.today() - timedelta(days=45)).isoformat(),
                unit="USD",
                value=20.0,
            ),
        ],
    )
    _store_market_currency(
        tmp_path / "roc.db",
        "TEST.US",
        (date.today() - timedelta(days=45)).isoformat(),
    )

    metric = ROCGreenblattMetric()

    assert metric.compute("TEST.US", repo) is not None
