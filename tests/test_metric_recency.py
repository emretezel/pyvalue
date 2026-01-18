"""Metric freshness guards.

Author: Emre Tezel
"""

from datetime import date, timedelta

from pyvalue.metrics.eps_quarterly import EarningsPerShareTTM
from pyvalue.metrics.eps_average import EPSAverageSixYearMetric
from pyvalue.metrics.long_term_debt import LongTermDebtMetric
from pyvalue.metrics.roc_greenblatt import ROCGreenblattMetric
from pyvalue.storage import FactRecord, FinancialFactsRepository


def test_metric_skips_when_latest_fact_is_stale(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "facts.db")
    repo.initialize_schema()
    stale_date = (date.today() - timedelta(days=400)).isoformat()
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

    metric = EarningsPerShareTTM()
    result = metric.compute("AAPL.US", repo)

    assert result is not None
    assert result.value == sum(float(idx) for idx in range(1, 5))


def test_ttm_metric_skips_when_latest_quarter_is_stale(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "stale_quarters.db")
    repo.initialize_schema()
    today = date.today()
    records = []
    for idx, months_ago in enumerate((13, 15, 16, 17), start=1):
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

    metric = ROCGreenblattMetric()

    assert metric.compute("TEST.US", repo) is not None
