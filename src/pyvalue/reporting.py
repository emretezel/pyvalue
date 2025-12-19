"""Reporting helpers for financial fact coverage.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple, Type

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, is_recent_fact
from pyvalue.storage import FactRecord, FinancialFactsRepository


@dataclass
class ConceptCoverage:
    """Coverage summary for a single fact concept."""

    concept: str
    missing: int = 0
    stale: int = 0


@dataclass
class MetricCoverage:
    """Coverage summary for the concepts required by a metric."""

    metric_id: str
    total_symbols: int
    fully_covered: int
    concepts: List[ConceptCoverage]


def compute_fact_coverage(
    fact_repo: FinancialFactsRepository,
    symbols: Sequence[str],
    metric_classes: Sequence[Type],
    *,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> List[MetricCoverage]:
    """Return coverage summaries for the financial facts used by metrics.

    Args:
        fact_repo: Repository that stores normalized financial facts.
        symbols: Sequence of ticker symbols to evaluate.
        metric_classes: Metric classes with ``id`` and ``required_concepts`` attributes.
        max_age_days: Maximum allowed age (days) for a fact to be considered fresh.
    """

    fact_repo.initialize_schema()
    if not isinstance(fact_repo, RegionFactsRepository):
        fact_repo = RegionFactsRepository(fact_repo)
    symbols_upper = [symbol.upper() for symbol in symbols]
    coverage: List[MetricCoverage] = []
    fact_cache: Dict[Tuple[str, str], FactRecord | None] = {}

    for metric_cls in metric_classes:
        required = getattr(metric_cls, "required_concepts", ()) or ()
        ordered_concepts: List[str] = []
        seen: set[str] = set()
        for concept in required:
            if concept in seen:
                continue
            ordered_concepts.append(concept)
            seen.add(concept)

        concept_counts: Dict[str, Dict[str, int]] = {concept: {"missing": 0, "stale": 0} for concept in ordered_concepts}
        fully_covered = len(symbols_upper) if not ordered_concepts else 0

        for symbol in symbols_upper:
            if not ordered_concepts:
                break
            symbol_has_all = True
            for concept in ordered_concepts:
                key = (symbol, concept)
                record = fact_cache.get(key)
                if key not in fact_cache:
                    record = fact_repo.latest_fact(symbol, concept)
                    fact_cache[key] = record

                if record is None:
                    concept_counts[concept]["missing"] += 1
                    symbol_has_all = False
                    continue
                if not is_recent_fact(record, max_age_days=max_age_days):
                    concept_counts[concept]["stale"] += 1
                    symbol_has_all = False
            if symbol_has_all:
                fully_covered += 1

        metric_id = getattr(metric_cls, "id", metric_cls.__name__)
        coverage.append(
            MetricCoverage(
                metric_id=metric_id,
                total_symbols=len(symbols_upper),
                fully_covered=fully_covered,
                concepts=[
                    ConceptCoverage(concept=concept, missing=concept_counts[concept]["missing"], stale=concept_counts[concept]["stale"])
                    for concept in ordered_concepts
                ],
            )
        )

    return coverage


__all__ = [
    "ConceptCoverage",
    "MetricCoverage",
    "compute_fact_coverage",
]
