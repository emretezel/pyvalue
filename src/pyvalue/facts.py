"""Provider-aware access to normalized financial facts.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Optional, Sequence

from pyvalue.storage import FactRecord, FinancialFactsRepository


def providers_for_symbol(symbol: str) -> tuple[str, ...]:
    """Return provider priority for a symbol based on region conventions."""

    if symbol.upper().endswith(".US"):
        return ("SEC",)
    return ("EODHD",)


class RegionFactsRepository:
    """Wrap FinancialFactsRepository with region-specific provider selection."""

    def __init__(self, repo: FinancialFactsRepository) -> None:
        self._repo = repo

    def latest_fact(
        self,
        symbol: str,
        concept: str,
        providers: Optional[Sequence[str]] = None,
    ) -> Optional[FactRecord]:
        provider_list = providers or providers_for_symbol(symbol)
        return self._repo.latest_fact(symbol, concept, providers=provider_list)

    def facts_for_concept(
        self,
        symbol: str,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
        providers: Optional[Sequence[str]] = None,
    ) -> list[FactRecord]:
        provider_list = providers or providers_for_symbol(symbol)
        return self._repo.facts_for_concept(
            symbol,
            concept,
            fiscal_period=fiscal_period,
            limit=limit,
            providers=provider_list,
        )

    def __getattr__(self, name: str):
        return getattr(self._repo, name)


__all__ = ["RegionFactsRepository", "providers_for_symbol"]
