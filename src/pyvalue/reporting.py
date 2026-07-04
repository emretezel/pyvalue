"""Reporting helpers for financial fact coverage.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple, Type

from pyvalue.facts import RegionFactsRepository
from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS, is_recent_fact
from pyvalue.persistence.storage import FactRecord, FinancialFactsRepository

# fiscal_period values that count as quarterly history when sizing a concept's
# depth (TTM metrics need four consecutive quarters of these).
QUARTERLY_FISCAL_PERIODS = ("Q1", "Q2", "Q3", "Q4")


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


@dataclass(frozen=True)
class ConceptDetail:
    """What one listing actually holds for one required fact concept.

    The per-listing sibling of :class:`ConceptCoverage`: instead of aggregate
    missing/stale counts over a scope, it describes the latest stored point
    (end date, fiscal period, filing date, value, currency), a freshness
    verdict against the metric window, and how deep the FY/quarterly history
    is -- the inputs a per-symbol "why is this metric NA" explanation needs.
    """

    concept: str
    present: bool
    fresh: bool
    latest_end_date: Optional[str]
    latest_fiscal_period: Optional[str]
    latest_filed: Optional[str]
    latest_value: Optional[float]
    latest_currency: Optional[str]
    fy_rows: int
    quarterly_rows: int
    total_rows: int


def compute_fact_detail(
    fact_repo: FinancialFactsRepository | RegionFactsRepository,
    listing_id: int,
    metric_cls: Type,
    *,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> List[ConceptDetail]:
    """Per-concept presence/freshness/history detail for one listing.

    Returns one :class:`ConceptDetail` per ``required_concepts`` entry of
    ``metric_cls`` (first-seen order, duplicates dropped). A concept with no
    stored rows comes back ``present=False`` with ``None`` latest fields, so
    callers can render "MISSING" without special-casing.
    """

    if isinstance(fact_repo, RegionFactsRepository):
        fact_repo_wrapped = fact_repo
    else:
        fact_repo.initialize_schema()
        fact_repo_wrapped = RegionFactsRepository(fact_repo)

    ordered_concepts: List[str] = []
    seen: set[str] = set()
    for concept in getattr(metric_cls, "required_concepts", ()) or ():
        if concept not in seen:
            seen.add(concept)
            ordered_concepts.append(concept)
    if not ordered_concepts:
        return []

    facts_by_id = fact_repo_wrapped.facts_for_ids_many(
        [int(listing_id)],
        concepts=ordered_concepts,
    )
    records_by_concept: Dict[str, List[FactRecord]] = {}
    for record in facts_by_id.get(int(listing_id), []):
        records_by_concept.setdefault(record.concept, []).append(record)

    details: List[ConceptDetail] = []
    for concept in ordered_concepts:
        records = records_by_concept.get(concept, [])
        if not records:
            details.append(
                ConceptDetail(
                    concept=concept,
                    present=False,
                    fresh=False,
                    latest_end_date=None,
                    latest_fiscal_period=None,
                    latest_filed=None,
                    latest_value=None,
                    latest_currency=None,
                    fy_rows=0,
                    quarterly_rows=0,
                    total_rows=0,
                )
            )
            continue
        # Rows arrive (end_date DESC, filed DESC) per concept, so the first
        # record is exactly the "latest fact" the metric seam would read.
        latest = records[0]
        details.append(
            ConceptDetail(
                concept=concept,
                present=True,
                fresh=is_recent_fact(latest, max_age_days=max_age_days),
                latest_end_date=latest.end_date,
                latest_fiscal_period=latest.fiscal_period,
                latest_filed=latest.filed,
                latest_value=latest.value,
                latest_currency=latest.currency,
                fy_rows=sum(1 for r in records if r.fiscal_period == "FY"),
                quarterly_rows=sum(
                    1 for r in records if r.fiscal_period in QUARTERLY_FISCAL_PERIODS
                ),
                total_rows=len(records),
            )
        )
    return details


__all__ = [
    "ConceptCoverage",
    "ConceptDetail",
    "MetricCoverage",
    "compute_fact_coverage",
    "compute_fact_detail",
]
