"""
Data access helpers for metric evaluation.
Author: Emre Tezel
"""

from __future__ import annotations

from typing import Callable, Dict

from sqlalchemy.orm import Session

from pyvalue.data.balance_sheet import BalanceSheet


class DataAccess:
    """Provides shared data-fetching utilities for metrics."""

    def __init__(self, session: Session):
        self.session = session
        self._fetchers: Dict[str, Callable[[int], object]] = {
            "balance_sheet_latest": self._fetch_balance_sheet_latest,
        }

    def register_fetcher(
        self, requirement: str, fetcher: Callable[[int], object]
    ) -> None:
        """Register a custom fetcher for additional metric requirements."""
        self._fetchers[requirement] = fetcher

    def fetch(self, requirement: str, stock_id: int):
        """Fetch a dataset by requirement name."""
        if requirement not in self._fetchers:
            raise KeyError(f"No fetcher registered for requirement '{requirement}'")
        return self._fetchers[requirement](stock_id)

    def _fetch_balance_sheet_latest(self, stock_id: int):
        """Return the most recent balance sheet for a stock."""
        return (
            self.session.query(BalanceSheet)
            .filter_by(stock_id=stock_id)
            .order_by(BalanceSheet.date.desc())
            .first()
        )
