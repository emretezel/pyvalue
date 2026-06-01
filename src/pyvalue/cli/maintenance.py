"""CLI handlers for clearing tables and purging non-filer US listings.

Author: Emre Tezel
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import (
    List,
)

from pyvalue.persistence.storage import (
    FundamentalsNormalizationStateRepository,
    FundamentalsRepository,
    FinancialFactsRepository,
    FinancialFactsRefreshStateRepository,
    MarketDataRepository,
    MetricComputeStatusRepository,
    MetricsRepository,
    SupportedTickerRepository,
)

from ._common import (
    _resolve_database_path,
)


def _eligible_sec_filers(db_path: Path) -> List[str]:
    """Return symbols that have at least one 10-K/10-Q filing in SEC raw facts."""

    repo = FundamentalsRepository(db_path)
    repo.initialize_schema()
    allowed = {"10-K", "10-K/A", "10-Q", "10-Q/A"}
    eligible: set[str] = set()
    with repo._connect() as conn:
        rows = conn.execute(
            """
            SELECT catalog.provider_symbol, fr.data
            FROM fundamentals_raw fr
            JOIN provider_listing_catalog catalog
              ON catalog.provider_listing_id = fr.provider_listing_id
            WHERE catalog.provider = 'SEC'
            """
        ).fetchall()
    for symbol, payload_json in rows:
        try:
            data = json.loads(payload_json)
        except Exception:
            continue
        facts = data.get("facts", {}).get("us-gaap", {}) or {}
        for detail in facts.values():
            units = detail.get("units", {}) if isinstance(detail, dict) else {}
            for entries in units.values():
                if not isinstance(entries, list):
                    continue
                for item in entries:
                    form = item.get("form")
                    if form in allowed:
                        eligible.add(symbol.upper())
                        break
                if symbol.upper() in eligible:
                    break
            if symbol.upper() in eligible:
                break
    return sorted(eligible)


def cmd_purge_us_nonfilers(database: str, apply: bool) -> int:
    """Remove SEC US supported tickers with no 10-K/10-Q filings stored in SEC facts."""

    db_path = _resolve_database_path(database)
    ticker_repo = SupportedTickerRepository(db_path)
    ticker_repo.initialize_schema()
    us_symbols = ticker_repo.list_symbols_by_exchange("SEC", "US")

    eligible = set(_eligible_sec_filers(db_path))
    to_remove = sorted([sym for sym in us_symbols if sym.upper() not in eligible])
    if not to_remove:
        print("No US non-filers found to purge.")
        return 0

    print(f"Found {len(to_remove)} SEC US supported tickers without 10-K/10-Q filings.")
    for sym in to_remove:
        print(f"- {sym}")
    if not apply:
        print("Dry run only. Re-run with --apply to delete from provider_listing.")
        return 0

    ticker_repo.delete_symbols("SEC", to_remove)
    print(f"Deleted {len(to_remove)} SEC US supported tickers from provider_listing.")
    return 0


def cmd_clear_listings(database: str) -> int:
    """Delete the canonical provider_listing catalog (legacy command alias)."""

    repo = SupportedTickerRepository(database)
    deleted = repo.clear()
    print(
        f"Deprecated command: cleared {deleted} provider_listing rows in {database}. "
        "Use provider_listing as the canonical catalog."
    )
    return 0


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
