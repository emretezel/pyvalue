"""LiabilitiesCurrent fallback derivation."""

from datetime import date, timedelta

from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.metrics.working_capital import WorkingCapitalMetric
from pyvalue.storage import FactRecord, FinancialFactsRepository


def _fact(symbol: str, concept: str, days_ago: int, value: float, fiscal_period: str = "Q1") -> FactRecord:
    return FactRecord(
        symbol=symbol,
        concept=concept,
        fiscal_period=fiscal_period,
        end_date=(date.today() - timedelta(days=days_ago)).isoformat(),
        unit="USD",
        value=value,
    )


def test_working_capital_derives_liabilities_current_when_missing(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "facts.db")
    repo.initialize_schema()
    symbol = "TEST.US"
    records = [
        _fact(symbol, "AssetsCurrent", 10, 75.0),
        _fact(symbol, "AccountsPayableCurrent", 10, 15.0),
        _fact(symbol, "AccruedLiabilitiesCurrent", 10, 5.0),
    ]
    repo.replace_facts(symbol, records)

    metric = WorkingCapitalMetric()
    result = metric.compute(symbol, repo)

    assert result is not None
    assert result.value == 75.0 - (15.0 + 5.0)


def test_current_ratio_ignores_stale_liabilities_current_when_components_fresh(tmp_path):
    repo = FinancialFactsRepository(tmp_path / "facts_stale.db")
    repo.initialize_schema()
    symbol = "TEST2.US"
    stale_days = 400
    fresh_days = 5
    records = [
        _fact(symbol, "AssetsCurrent", fresh_days, 60.0),
        _fact(symbol, "LiabilitiesCurrent", stale_days, 50.0),
        _fact(symbol, "DeferredRevenueCurrent", fresh_days, 20.0),
        _fact(symbol, "ShortTermBorrowings", fresh_days, 10.0),
    ]
    repo.replace_facts(symbol, records)

    metric = CurrentRatioMetric()
    result = metric.compute(symbol, repo)

    assert result is not None
    assert result.value == 60.0 / (20.0 + 10.0)
