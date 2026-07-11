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

    # Each repo truncates the table it owns (DELETE FROM, preserving migration-added
    # constraints -- never DROP). All SQL and connection handling stays inside the
    # persistence package; the CLI only orchestrates the repo calls.
    FinancialFactsRepository(database).clear()
    FinancialFactsRefreshStateRepository(database).clear()
    MetricComputeStatusRepository(database).clear()
    FundamentalsNormalizationStateRepository(database).clear()
    print(f"Cleared financial_facts table in {database}")
    return 0


def cmd_clear_fundamentals_raw(database: str) -> int:
    """Delete all stored raw fundamentals."""

    FundamentalsRepository(database).clear()
    FundamentalsNormalizationStateRepository(database).clear()
    print(f"Cleared fundamentals_raw table in {database}")
    return 0


def cmd_clear_metrics(database: str) -> int:
    """Delete all computed metrics."""

    MetricsRepository(database).clear()
    MetricComputeStatusRepository(database).clear()
    print(f"Cleared metrics table in {database}")
    return 0


def cmd_clear_market_data(database: str) -> int:
    """Delete all stored market data (canonical and provider layers)."""

    MarketDataRepository(database).clear()
    print(f"Cleared market_data and provider_market_data tables in {database}")
    return 0
