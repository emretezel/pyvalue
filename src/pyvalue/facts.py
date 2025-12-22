"""Access to normalized financial facts.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Optional

from pyvalue.storage import FactRecord, FinancialFactsRepository


class RegionFactsRepository:
    """Wrap FinancialFactsRepository with a stable interface."""

    def __init__(self, repo: FinancialFactsRepository) -> None:
        self._repo = repo

    def latest_fact(
        self,
        symbol: str,
        concept: str,
    ) -> Optional[FactRecord]:
        return self._repo.latest_fact(symbol, concept)

    def facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[FactRecord]:
        return self._repo.facts_for_concept(
            symbol,
            concept,
            fiscal_period=fiscal_period,
            limit=limit,
        )

    def __getattr__(self, name: str):
        return getattr(self._repo, name)


__all__ = ["RegionFactsRepository"]
