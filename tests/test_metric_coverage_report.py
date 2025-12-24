"""Metric coverage report."""

from pyvalue.cli import cmd_report_metric_coverage
from pyvalue.metrics.working_capital import WorkingCapitalMetric
from pyvalue.metrics.current_ratio import CurrentRatioMetric
from pyvalue.storage import FactRecord, FinancialFactsRepository, UniverseRepository
from pyvalue.universe import Listing


def _seed_universe(db_path):
    universe = UniverseRepository(db_path)
    universe.initialize_schema()
    universe.replace_universe(
        [
            Listing(symbol="AAA.US", security_name="AAA", exchange="NYSE"),
            Listing(symbol="BBB.US", security_name="BBB", exchange="NYSE"),
        ]
    )


def test_metric_coverage_counts_symbols(tmp_path, capsys):
    db_path = tmp_path / "coverage.db"
    _seed_universe(db_path)
    repo = FinancialFactsRepository(db_path)
    repo.initialize_schema()
    # AAA has full facts; BBB missing liabilities.
    repo.replace_facts(
        "AAA.US",
        [
            FactRecord(symbol="AAA.US", concept="AssetsCurrent", fiscal_period="Q1", end_date="2025-01-01", unit="USD", value=100.0),
            FactRecord(symbol="AAA.US", concept="LiabilitiesCurrent", fiscal_period="Q1", end_date="2025-01-01", unit="USD", value=50.0),
        ],
    )
    repo.replace_facts(
        "BBB.US",
        [
            FactRecord(symbol="BBB.US", concept="AssetsCurrent", fiscal_period="Q1", end_date="2025-01-01", unit="USD", value=200.0),
        ],
    )

    exit_code = cmd_report_metric_coverage(
        database=str(db_path),
        exchange_code="NYSE",
        metric_ids=[WorkingCapitalMetric.id, CurrentRatioMetric.id],
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "all metrics computed: 1/2" in output.lower()
    assert "- working_capital: 1/2" in output
