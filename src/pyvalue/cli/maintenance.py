"""CLI handlers for clearing tables.

Author: Emre Tezel
"""

from __future__ import annotations


from pyvalue.persistence.storage import (
    FundamentalsNormalizationStateRepository,
    FundamentalsRepository,
    FinancialFactsRepository,
    FinancialFactsRefreshStateRepository,
    MarketDataRepository,
    MetricComputeStatusRepository,
    MetricsRepository,
)


def cmd_clear_financial_facts(database: str) -> int:
    """Delete all normalized financial facts."""

    repo = FinancialFactsRepository(database)
    state_repo = FundamentalsNormalizationStateRepository(database)
    refresh_state_repo = FinancialFactsRefreshStateRepository(database)
    metric_status_repo = MetricComputeStatusRepository(database)
    # DELETE FROM (not DROP TABLE) so migration-added FK / CHECK constraints
    # survive this command — DROP would force initialize_schema() to recreate
    # the table from the legacy CREATE TABLE IF NOT EXISTS DDL, silently
    # stripping every constraint a later migration added.
    repo.initialize_schema()
    refresh_state_repo.initialize_schema()
    metric_status_repo.initialize_schema()
    state_repo.initialize_schema()
    with repo._connect() as conn:
        conn.execute("DELETE FROM financial_facts")
        conn.execute("DELETE FROM financial_facts_refresh_state")
        conn.execute("DELETE FROM metric_compute_status")
        conn.execute("DELETE FROM fundamentals_normalization_state")
    print(f"Cleared financial_facts table in {database}")
    return 0


def cmd_clear_fundamentals_raw(database: str) -> int:
    """Delete all stored raw fundamentals."""

    repo = FundamentalsRepository(database)
    state_repo = FundamentalsNormalizationStateRepository(database)
    # DELETE FROM (not DROP TABLE) preserves migration-added constraints —
    # see cmd_clear_financial_facts for the full rationale.
    repo.initialize_schema()
    state_repo.initialize_schema()
    with repo._connect() as conn:
        conn.execute("DELETE FROM fundamentals_raw")
        conn.execute("DELETE FROM fundamentals_normalization_state")
    print(f"Cleared fundamentals_raw table in {database}")
    return 0


def cmd_clear_metrics(database: str) -> int:
    """Delete all computed metrics."""

    repo = MetricsRepository(database)
    status_repo = MetricComputeStatusRepository(database)
    # DELETE FROM (not DROP TABLE) preserves migration-added constraints —
    # see cmd_clear_financial_facts for the full rationale.
    repo.initialize_schema()
    status_repo.initialize_schema()
    with repo._connect() as conn:
        conn.execute("DELETE FROM metrics")
        conn.execute("DELETE FROM metric_compute_status")
    print(f"Cleared metrics table in {database}")
    return 0


def cmd_clear_market_data(database: str) -> int:
    """Delete all stored market data."""

    repo = MarketDataRepository(database)
    # DELETE FROM (not DROP TABLE) preserves migration-added constraints —
    # see cmd_clear_financial_facts for the full rationale.
    repo.initialize_schema()
    with repo._connect() as conn:
        conn.execute("DELETE FROM market_data")
    print(f"Cleared market_data table in {database}")
    return 0
