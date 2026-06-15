"""Reporting helpers for financial fact coverage.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple, Type

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, is_recent_fact
from pyvalue.persistence.storage import FactRecord, FinancialFactsRepository


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
    fact_repo: FinancialFactsRepository | RegionFactsRepository,
    listing_ids: Sequence[int],
    metric_classes: Sequence[Type],
    *,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> List[MetricCoverage]:
    """Return coverage summaries for the financial facts used by metrics.

    Args:
        fact_repo: Repository that stores normalized financial facts.
        listing_ids: Scope-resolved ``listing_id`` values to evaluate. The
            ``report-fact-freshness`` caller resolves these once via
            ``_resolve_canonical_scope_listings`` and passes them straight in, so
            the bulk fact load seeks by id with no symbol resolution. The output
            is metric/concept-keyed counts only -- no per-listing identity is
            emitted -- so the display symbol is not needed here.
        metric_classes: Metric classes with ``id`` and ``required_concepts`` attributes.
        max_age_days: Maximum allowed age (days) for a fact to be considered fresh.
    """

    if isinstance(fact_repo, RegionFactsRepository):
        fact_repo_wrapped = fact_repo
    else:
        fact_repo.initialize_schema()
        fact_repo_wrapped = RegionFactsRepository(fact_repo)
    normalized_ids = [int(listing_id) for listing_id in listing_ids]
    coverage: List[MetricCoverage] = []

    # Bulk-load every required concept for the whole scope in one indexed pass
    # keyed by the carried listing ids. The bulk query orders (listing_id,
    # concept, end_date DESC, filed DESC), so the first row per (listing_id,
    # concept) is the latest, exactly what latest_fact returned.
    all_concepts: List[str] = []
    seen_all_concepts: set[str] = set()
    for metric_cls in metric_classes:
        for concept in getattr(metric_cls, "required_concepts", ()) or ():
            if concept not in seen_all_concepts:
                seen_all_concepts.add(concept)
                all_concepts.append(concept)

    latest_fact_by_id_concept: Dict[Tuple[int, str], FactRecord] = {}
    if all_concepts and normalized_ids:
        facts_by_id = fact_repo_wrapped.facts_for_ids_many(
            normalized_ids,
            concepts=all_concepts,
        )
        for listing_id, records in facts_by_id.items():
            for record in records:
                key = (listing_id, record.concept)
                # Rows arrive newest-first within each concept; keep the first.
                if key not in latest_fact_by_id_concept:
                    latest_fact_by_id_concept[key] = record

    for metric_cls in metric_classes:
        required = getattr(metric_cls, "required_concepts", ()) or ()
        ordered_concepts: List[str] = []
        seen: set[str] = set()
        for concept in required:
            if concept in seen:
                continue
            ordered_concepts.append(concept)
            seen.add(concept)

        concept_counts: Dict[str, Dict[str, int]] = {
            concept: {"missing": 0, "stale": 0} for concept in ordered_concepts
        }
        fully_covered = len(normalized_ids) if not ordered_concepts else 0

        for listing_id in normalized_ids:
            if not ordered_concepts:
                break
            listing_has_all = True
            for concept in ordered_concepts:
                record = latest_fact_by_id_concept.get((listing_id, concept))
                if record is None:
                    concept_counts[concept]["missing"] += 1
                    listing_has_all = False
                    continue
                if not is_recent_fact(record, max_age_days=max_age_days):
                    concept_counts[concept]["stale"] += 1
                    listing_has_all = False
            if listing_has_all:
                fully_covered += 1

        metric_id = getattr(metric_cls, "id", metric_cls.__name__)
        coverage.append(
            MetricCoverage(
                metric_id=metric_id,
                total_symbols=len(normalized_ids),
                fully_covered=fully_covered,
                concepts=[
                    ConceptCoverage(
                        concept=concept,
                        missing=concept_counts[concept]["missing"],
                        stale=concept_counts[concept]["stale"],
                    )
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
