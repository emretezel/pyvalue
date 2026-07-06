"""Screening configuration and evaluation.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import List, Optional
import logging

import yaml

from pyvalue.currency import (
    MetricUnitKind,
    is_monetary_unit_kind,
    normalize_currency_code,
)
from pyvalue.money.fx import FXService
from pyvalue.persistence.storage import (
    MetricRecord,
    MetricsRepository,
)


LOGGER = logging.getLogger(__name__)
_RATIO_LIKE_UNIT_KINDS = frozenset({"ratio", "percent"})


@dataclass
class Term:
    metric: Optional[str] = None
    multiplier: float = 1.0
    value: Optional[float] = None
    currency: Optional[str] = None


@dataclass
class Criterion:
    name: str
    left: Term
    operator: str
    right: Term


@dataclass
class CriterionGroup:
    """A named group of criteria combined by a K-of-N pass rule.

    A screen is an AND of groups (conjunctive normal form). A group passes when
    at least ``min_pass`` of its ``members`` pass: ``min_pass == 1`` is OR -- the
    common case, "any of a subset passes" -- ``min_pass == len(members)`` is a
    plain AND of a named subset, and ``1 < min_pass < len(members)`` is a
    scorecard ("pass at least K of N"). An ordinary single criterion is a
    one-member group whose ``name`` is the criterion's name, which is why the
    flat, pre-group screener YAML keeps parsing unchanged.

    ``name`` is the group's reportable unit: the screen-output CSV column header,
    the fallout-funnel label, and the per-group value key. It must be unique
    across the screen (``load_screen`` enforces this).
    """

    name: str
    members: tuple[Criterion, ...]
    min_pass: int = 1


@dataclass
class ScreenDefinition:
    criteria: List[CriterionGroup]
    ranking: Optional["RankingDefinition"] = None


@dataclass(frozen=True)
class RankingMetric:
    """One metric that contributes to a screen ranking score."""

    metric_id: str
    weight: float
    direction: str
    cap: Optional[float] = None
    currency: Optional[str] = None


@dataclass(frozen=True)
class RankingTieBreaker:
    """Secondary ordering rule after the final ranking score."""

    metric_id: str
    direction: str
    currency: Optional[str] = None


@dataclass(frozen=True)
class RankingDefinition:
    """Optional ranking rules applied after pass/fail screening."""

    peer_group: str
    min_sector_peers: int
    winsor_lower_percentile: float
    winsor_upper_percentile: float
    metrics: tuple[RankingMetric, ...]
    tie_breakers: tuple[RankingTieBreaker, ...] = ()


@dataclass(frozen=True)
class ResolvedTerm:
    """Resolved screening term value for one symbol."""

    metric_id: Optional[str]
    value: Optional[float]
    unit_kind: Optional[MetricUnitKind] = None
    currency: Optional[str] = None
    unit_label: Optional[str] = None
    as_of: Optional[str] = None


@dataclass(frozen=True)
class CriterionEvaluation:
    """Detailed evaluation result for one screening criterion."""

    passed: bool
    left_value: Optional[float]
    right_value: Optional[float]
    lhs: Optional[float]
    rhs: Optional[float]
    failure_kind: Optional[str]
    missing_metric_ids: tuple[str, ...]
    left_metric_id: Optional[str] = None
    right_metric_id: Optional[str] = None


@dataclass(frozen=True)
class GroupEvaluation:
    """Detailed evaluation result for one criterion group.

    ``failure_kind`` distinguishes the two ways a group can fail so the fallout
    funnel can bucket them. ``comparison_failed`` means at least one member had
    the data it needed and its comparison came back false (a genuine threshold
    miss). ``na_blocked`` means no member reached a real comparison at all -- every
    failing arm was missing data. This is the OR/K-of-N coverage payoff: a metric
    missing on one arm is not blamed for the exclusion when another arm produced a
    real (data-backed) answer, so ``na_blocked`` is the only case where the missing
    metrics are attributed to NA fallout.
    """

    passed: bool
    member_evaluations: tuple[CriterionEvaluation, ...]
    reported_value: Optional[float]
    pass_count: int
    failure_kind: Optional[str]
    missing_metric_ids: tuple[str, ...]


def load_screen(path: str | Path) -> ScreenDefinition:
    """Parse a screener YAML file into a :class:`ScreenDefinition`.

    Each ``criteria`` entry is either a bare criterion (``left``/``operator``/
    ``right``) or an OR/K-of-N group (``any_of`` plus an optional ``at_least``).
    A bare criterion becomes a one-member group, so pre-group screeners parse
    unchanged. Group names must be unique because they are the output columns and
    fallout labels.
    """

    data = yaml.safe_load(Path(path).read_text())
    groups: List[CriterionGroup] = []
    seen_names: set[str] = set()
    for entry in data.get("criteria", []):
        group = _parse_group(entry)
        if group.name in seen_names:
            raise ValueError(
                f"Duplicate screen criterion/group name: {group.name!r}. Names are the "
                "output CSV columns and fallout labels, so they must be unique."
            )
        seen_names.add(group.name)
        groups.append(group)
    ranking = _load_ranking_definition(data.get("ranking"))
    return ScreenDefinition(criteria=groups, ranking=ranking)


def _parse_group(entry: object) -> CriterionGroup:
    """Parse one ``criteria`` entry into a :class:`CriterionGroup`.

    ``any_of`` marks an explicit group (its members are OR-ed, or K-of-N via
    ``at_least``); anything else is a bare criterion wrapped as a one-member group.
    """

    if not isinstance(entry, dict):
        raise ValueError(
            f"Screen criteria entry must be a mapping, got {type(entry).__name__}"
        )
    any_of = entry.get("any_of")
    if any_of is None:
        # Bare criterion: a one-member group named by the criterion itself, so the
        # group name lands in exactly the CSV-column / label slot the flat design used.
        criterion = _parse_criterion(entry)
        return CriterionGroup(name=criterion.name, members=(criterion,), min_pass=1)
    if not isinstance(any_of, list) or not any_of:
        raise ValueError("Screen group 'any_of' must be a non-empty list of criteria")
    name = entry.get("name")
    if not name:
        raise ValueError(
            "Screen group with 'any_of' must have a 'name' (its output CSV column)"
        )
    members = tuple(_parse_criterion(member) for member in any_of)
    # at_least is the K in "pass at least K of N"; it defaults to 1 (OR) and cannot
    # exceed the member count (that would be an unsatisfiable group).
    min_pass = int(entry.get("at_least", 1))
    if not 1 <= min_pass <= len(members):
        raise ValueError(
            f"Screen group {name!r}: 'at_least' must be between 1 and "
            f"{len(members)}, got {min_pass}"
        )
    return CriterionGroup(name=str(name), members=members, min_pass=min_pass)


def _parse_criterion(entry: object) -> Criterion:
    """Parse a single criterion mapping (``name``/``left``/``operator``/``right``)."""

    if not isinstance(entry, dict):
        raise ValueError(
            f"Screen criterion must be a mapping, got {type(entry).__name__}"
        )
    if "left" not in entry or "right" not in entry:
        raise ValueError("Screen criterion must have both 'left' and 'right' terms")
    return Criterion(
        name=str(entry.get("name", "criterion")),
        left=_parse_term(entry["left"]),
        operator=str(entry.get("operator", "<=")),
        right=_parse_term(entry["right"]),
    )


def _parse_term(data: object) -> Term:
    """Parse a term mapping; unknown keys raise via ``Term(**...)`` as before."""

    if not isinstance(data, dict):
        raise ValueError(f"Screen term must be a mapping, got {type(data).__name__}")
    return Term(**data)


def screen_metric_ids(definition: ScreenDefinition) -> List[str]:
    """Return unique metric ids referenced by the screen in first-seen order."""

    metric_ids: List[str] = []
    seen: set[str] = set()
    for group in definition.criteria:
        for criterion in group.members:
            for term in (criterion.left, criterion.right):
                if not term.metric or term.metric in seen:
                    continue
                seen.add(term.metric)
                metric_ids.append(term.metric)
    return metric_ids


def ranking_metric_ids(definition: ScreenDefinition) -> List[str]:
    """Return unique ranking metric ids in first-seen order."""

    if definition.ranking is None:
        return []
    metric_ids: List[str] = []
    seen: set[str] = set()
    for metric in definition.ranking.metrics:
        if metric.metric_id in seen:
            continue
        seen.add(metric.metric_id)
        metric_ids.append(metric.metric_id)
    return metric_ids


def evaluate_criterion(
    criterion: Criterion,
    listing_id: int,
    metrics_repo: MetricsRepository,
    *,
    display_symbol: str,
) -> bool:
    passed, _ = evaluate_criterion_verbose(
        criterion, listing_id, metrics_repo, display_symbol=display_symbol
    )
    return passed


def evaluate_criterion_verbose(
    criterion: Criterion,
    listing_id: int,
    metrics_repo: MetricsRepository,
    *,
    display_symbol: str,
) -> tuple[bool, Optional[float]]:
    evaluation = evaluate_criterion_detail(
        criterion, listing_id, metrics_repo, display_symbol=display_symbol
    )
    return evaluation.passed, evaluation.left_value


def evaluate_criterion_detail(
    criterion: Criterion,
    listing_id: int,
    metrics_repo: MetricsRepository,
    *,
    display_symbol: str,
    log_missing_metrics: bool = True,
) -> CriterionEvaluation:
    """Return a detailed evaluation for one criterion.

    Identity is the ``listing_id`` (metric values are read by id). ``display_symbol``
    is a label used only in diagnostic log lines (unit mismatch, missing FX,
    missing metric) -- it never selects which entity's data is read.
    """

    left = _resolve_term(
        criterion.left,
        listing_id,
        metrics_repo,
        display_symbol=display_symbol,
        log_missing_metrics=log_missing_metrics,
    )
    right = _resolve_term(
        criterion.right,
        listing_id,
        metrics_repo,
        display_symbol=display_symbol,
        log_missing_metrics=log_missing_metrics,
    )
    missing_metric_ids = _dedupe_missing_metric_ids(
        left.metric_id if left.value is None else None,
        right.metric_id if right.value is None else None,
    )
    if left.value is None or right.value is None:
        missing_failure_kind: str
        if left.value is None and right.value is None:
            missing_failure_kind = "both_missing"
        elif left.value is None:
            missing_failure_kind = "left_missing"
        else:
            missing_failure_kind = "right_missing"
        return CriterionEvaluation(
            passed=False,
            left_value=left.value,
            right_value=right.value,
            lhs=None,
            rhs=None,
            failure_kind=missing_failure_kind,
            missing_metric_ids=missing_metric_ids,
            left_metric_id=left.metric_id,
            right_metric_id=right.metric_id,
        )

    lhs: Optional[float]
    rhs: Optional[float]
    failure_kind: Optional[str]
    lhs, rhs, failure_kind = _align_comparison_values(
        display_symbol,
        criterion,
        left,
        right,
        metrics_repo,
    )
    if failure_kind is not None or lhs is None or rhs is None:
        return CriterionEvaluation(
            passed=False,
            left_value=left.value,
            right_value=right.value,
            lhs=lhs,
            rhs=rhs,
            failure_kind=failure_kind or "comparison_unavailable",
            missing_metric_ids=missing_metric_ids,
            left_metric_id=left.metric_id,
            right_metric_id=right.metric_id,
        )

    if criterion.operator == "<=":
        passed = lhs <= rhs
    elif criterion.operator == ">=":
        passed = lhs >= rhs
    elif criterion.operator == "<":
        passed = lhs < rhs
    elif criterion.operator == ">":
        passed = lhs > rhs
    elif criterion.operator == "==":
        passed = lhs == rhs
    else:
        raise ValueError(f"Unsupported operator: {criterion.operator}")

    return CriterionEvaluation(
        passed=passed,
        left_value=left.value,
        right_value=right.value,
        lhs=lhs,
        rhs=rhs,
        failure_kind=None if passed else "comparison_failed",
        missing_metric_ids=missing_metric_ids,
        left_metric_id=left.metric_id,
        right_metric_id=right.metric_id,
    )


def evaluate_group(
    group: CriterionGroup,
    listing_id: int,
    metrics_repo: MetricsRepository,
    *,
    display_symbol: str,
) -> tuple[bool, Optional[float]]:
    """Return ``(passed, reported_value)`` for a group, short-circuiting.

    Members are evaluated in order and the scan stops as soon as ``min_pass`` of
    them pass; ``reported_value`` is the left-hand value of the member that
    reached the threshold (for OR that is simply the first passing member). A
    passing criterion always resolves its left term, so a passing group's reported
    value is never ``None``. Returns ``(False, None)`` when fewer than ``min_pass``
    members pass. This mirrors the fast :func:`evaluate_criterion_verbose` path and
    is what the per-listing screen loop uses.
    """

    pass_count = 0
    for member in group.members:
        passed, left_value = evaluate_criterion_verbose(
            member, listing_id, metrics_repo, display_symbol=display_symbol
        )
        if not passed:
            continue
        pass_count += 1
        if pass_count >= group.min_pass:
            return True, left_value
    return False, None


def evaluate_group_detail(
    group: CriterionGroup,
    listing_id: int,
    metrics_repo: MetricsRepository,
    *,
    display_symbol: str,
    log_missing_metrics: bool = True,
) -> GroupEvaluation:
    """Return a full evaluation of every member of ``group``.

    Unlike :func:`evaluate_group` this evaluates all members (no short-circuit) so
    the single-symbol detail view and the fallout funnel can show each arm and
    classify *why* a failing group failed. ``failure_kind`` is ``na_blocked`` only
    when no member reached a real comparison (every failing arm was missing data);
    otherwise a failing group is ``comparison_failed`` (see :class:`GroupEvaluation`).
    """

    member_evaluations = tuple(
        evaluate_criterion_detail(
            member,
            listing_id,
            metrics_repo,
            display_symbol=display_symbol,
            log_missing_metrics=log_missing_metrics,
        )
        for member in group.members
    )
    pass_count = sum(1 for evaluation in member_evaluations if evaluation.passed)
    passed = pass_count >= group.min_pass

    reported_value: Optional[float] = None
    for evaluation in member_evaluations:
        if evaluation.passed:
            reported_value = evaluation.left_value
            break

    failure_kind: Optional[str]
    if passed:
        failure_kind = None
    elif any(
        evaluation.failure_kind == "comparison_failed"
        for evaluation in member_evaluations
    ):
        # At least one arm had its data and genuinely missed the bar -- a real
        # threshold fail, not a data gap.
        failure_kind = "comparison_failed"
    else:
        failure_kind = "na_blocked"

    missing_metric_ids = _dedupe_missing_metric_ids(
        *(
            metric_id
            for evaluation in member_evaluations
            for metric_id in evaluation.missing_metric_ids
        )
    )
    return GroupEvaluation(
        passed=passed,
        member_evaluations=member_evaluations,
        reported_value=reported_value,
        pass_count=pass_count,
        failure_kind=failure_kind,
        missing_metric_ids=missing_metric_ids,
    )


def _align_comparison_values(
    symbol: str,
    criterion: Criterion,
    left: ResolvedTerm,
    right: ResolvedTerm,
    metrics_repo: MetricsRepository,
) -> tuple[Optional[float], Optional[float], Optional[str]]:
    left_value = left.value
    right_value = right.value
    if left_value is None or right_value is None:
        return None, None, "comparison_unavailable"

    anchor = _comparison_anchor(left, right)
    if anchor is None:
        if not _non_monetary_terms_compatible(left, right):
            LOGGER.warning(
                "Screen unit mismatch | symbol=%s criterion=%s left_kind=%s right_kind=%s",
                symbol,
                criterion.name,
                left.unit_kind,
                right.unit_kind,
            )
            return None, None, "unit_mismatch"
        lhs = left_value
        rhs = right_value * criterion.right.multiplier
        return lhs, rhs, None

    if is_monetary_unit_kind(anchor.unit_kind):
        if anchor.currency is None:
            LOGGER.warning(
                "Missing metric currency during screen evaluation | symbol=%s criterion=%s anchor_metric=%s",
                symbol,
                criterion.name,
                anchor.metric_id,
            )
            return None, None, "currency_missing"
        fx_service = _fx_service_for_db(metrics_repo.db_path)
        left_aligned, left_error = _convert_term_to_anchor(
            left,
            anchor,
            symbol=symbol,
            criterion_name=criterion.name,
            side="left",
            fx_service=fx_service,
        )
        if left_error is not None:
            return None, None, left_error
        if left_aligned is None:
            return None, None, "comparison_unavailable"
        right_aligned, right_error = _convert_term_to_anchor(
            right,
            anchor,
            symbol=symbol,
            criterion_name=criterion.name,
            side="right",
            fx_service=fx_service,
        )
        if right_error is not None:
            return None, None, right_error
        if right_aligned is None:
            return None, None, "comparison_unavailable"
        return left_aligned, right_aligned * criterion.right.multiplier, None

    if not _non_monetary_terms_compatible(left, right):
        LOGGER.warning(
            "Screen unit mismatch | symbol=%s criterion=%s left_kind=%s right_kind=%s",
            symbol,
            criterion.name,
            left.unit_kind,
            right.unit_kind,
        )
        return None, None, "unit_mismatch"
    if left.currency is not None or right.currency is not None:
        LOGGER.warning(
            "Unexpected currency on non-monetary screen term | symbol=%s criterion=%s left_currency=%s right_currency=%s",
            symbol,
            criterion.name,
            left.currency,
            right.currency,
        )
        return None, None, "unit_mismatch"
    return left_value, right_value * criterion.right.multiplier, None


def _comparison_anchor(
    left: ResolvedTerm, right: ResolvedTerm
) -> Optional[ResolvedTerm]:
    for term in (left, right):
        if term.metric_id is None:
            continue
        if term.unit_kind is None:
            continue
        return term
    return None


def _non_monetary_terms_compatible(left: ResolvedTerm, right: ResolvedTerm) -> bool:
    left_kind = left.unit_kind
    right_kind = right.unit_kind
    if left_kind is None or right_kind is None:
        return True
    if left_kind == right_kind:
        return True
    if left_kind in _RATIO_LIKE_UNIT_KINDS and right_kind in _RATIO_LIKE_UNIT_KINDS:
        return True
    return False


def _convert_term_to_anchor(
    term: ResolvedTerm,
    anchor: ResolvedTerm,
    *,
    symbol: str,
    criterion_name: str,
    side: str,
    fx_service: FXService,
) -> tuple[Optional[float], Optional[str]]:
    if term.value is None:
        return None, "missing"
    if anchor.currency is None:
        return None, "currency_missing"

    if term.metric_id is None:
        if term.currency is None:
            return term.value, None
        source_currency = normalize_currency_code(term.currency)
        if source_currency is None:
            return None, "currency_missing"
        if source_currency == anchor.currency:
            return term.value, None
        converted = fx_service.convert_amount(
            term.value,
            source_currency,
            anchor.currency,
            anchor.as_of or date.today().isoformat(),
        )
        if converted is None:
            LOGGER.warning(
                "Missing FX during screen evaluation | symbol=%s criterion=%s side=%s from=%s to=%s as_of=%s",
                symbol,
                criterion_name,
                side,
                source_currency,
                anchor.currency,
                anchor.as_of,
            )
            return None, "fx_missing"
        return float(converted), None

    if not is_monetary_unit_kind(term.unit_kind):
        return None, "unit_mismatch"
    source_currency = normalize_currency_code(term.currency)
    if source_currency is None:
        LOGGER.warning(
            "Missing metric currency during screen evaluation | symbol=%s criterion=%s side=%s metric=%s",
            symbol,
            criterion_name,
            side,
            term.metric_id,
        )
        return None, "currency_missing"
    if source_currency == anchor.currency:
        return term.value, None

    converted = fx_service.convert_amount(
        term.value,
        source_currency,
        anchor.currency,
        term.as_of or anchor.as_of or date.today().isoformat(),
    )
    if converted is None:
        LOGGER.warning(
            "Missing FX during screen evaluation | symbol=%s criterion=%s side=%s metric=%s from=%s to=%s as_of=%s",
            symbol,
            criterion_name,
            side,
            term.metric_id,
            source_currency,
            anchor.currency,
            term.as_of,
        )
        return None, "fx_missing"
    return float(converted), None


def _resolve_term(
    term: Term,
    listing_id: int,
    metrics_repo: MetricsRepository,
    *,
    display_symbol: str,
    log_missing_metrics: bool,
) -> ResolvedTerm:
    if term.value is not None:
        return ResolvedTerm(
            metric_id=None,
            value=term.value,
            unit_kind="monetary" if term.currency else None,
            currency=normalize_currency_code(term.currency),
            unit_label=None,
            as_of=None,
        )
    if not term.metric:
        return ResolvedTerm(metric_id=None, value=None)
    record = _ensure_metric_record(
        listing_id,
        term.metric,
        metrics_repo,
        display_symbol=display_symbol,
        log_missing_metric=log_missing_metrics,
    )
    if record is None:
        return ResolvedTerm(metric_id=term.metric, value=None)
    return ResolvedTerm(
        metric_id=term.metric,
        value=record.value,
        unit_kind=record.unit_kind,
        currency=record.currency,
        unit_label=record.unit_label,
        as_of=record.as_of,
    )


def _ensure_metric_record(
    listing_id: int,
    metric_id: str,
    metrics_repo: MetricsRepository,
    *,
    display_symbol: str,
    log_missing_metric: bool = True,
) -> Optional[MetricRecord]:
    record = metrics_repo.fetch_by_id(listing_id, metric_id)
    if record is not None:
        return record
    if log_missing_metric:
        LOGGER.warning(
            "Metric %s missing for %s; run compute-metrics first",
            metric_id,
            display_symbol,
        )
    return None


def _dedupe_missing_metric_ids(*metric_ids: Optional[str]) -> tuple[str, ...]:
    ordered: List[str] = []
    seen: set[str] = set()
    for metric_id in metric_ids:
        if metric_id is None or metric_id in seen:
            continue
        seen.add(metric_id)
        ordered.append(metric_id)
    return tuple(ordered)


@lru_cache(maxsize=8)
def _fx_service_for_db(db_path: str) -> FXService:
    return FXService(db_path)


def _load_ranking_definition(data: object) -> Optional[RankingDefinition]:
    if not isinstance(data, dict):
        return None
    winsorize = data.get("winsorize")
    winsorize_data = winsorize if isinstance(winsorize, dict) else {}
    metrics = tuple(
        RankingMetric(
            metric_id=str(entry.get("metric") or entry.get("metric_id") or "").strip(),
            weight=float(entry.get("weight") or 0.0),
            direction=str(entry.get("direction") or "higher").strip().lower(),
            cap=(
                float(entry["cap"])
                if entry.get("cap") is not None and str(entry.get("cap")).strip() != ""
                else None
            ),
            currency=normalize_currency_code(entry.get("currency")),
        )
        for entry in data.get("metrics", [])
        if isinstance(entry, dict)
        and str(entry.get("metric") or entry.get("metric_id") or "").strip()
    )
    tie_breakers = tuple(
        RankingTieBreaker(
            metric_id=str(entry.get("metric") or entry.get("metric_id") or "").strip(),
            direction=str(entry.get("direction") or "ascending").strip().lower(),
            currency=normalize_currency_code(entry.get("currency")),
        )
        for entry in data.get("tie_breakers", [])
        if isinstance(entry, dict)
        and str(entry.get("metric") or entry.get("metric_id") or "").strip()
    )
    return RankingDefinition(
        peer_group=str(data.get("peer_group") or "sector").strip().lower(),
        min_sector_peers=int(data.get("min_sector_peers") or 10),
        winsor_lower_percentile=_normalize_percentile_threshold(
            winsorize_data.get("lower_percentile", 0.05)
        ),
        winsor_upper_percentile=_normalize_percentile_threshold(
            winsorize_data.get("upper_percentile", 0.95)
        ),
        metrics=metrics,
        tie_breakers=tie_breakers,
    )


def _normalize_percentile_threshold(value: object) -> float:
    if not isinstance(value, (int, float, str)):
        raise TypeError("Percentile threshold must be numeric")
    numeric = float(value)
    return numeric / 100.0 if numeric > 1.0 else numeric


__all__ = [
    "Criterion",
    "CriterionEvaluation",
    "CriterionGroup",
    "GroupEvaluation",
    "RankingDefinition",
    "RankingMetric",
    "RankingTieBreaker",
    "ResolvedTerm",
    "ScreenDefinition",
    "Term",
    "evaluate_criterion",
    "evaluate_criterion_detail",
    "evaluate_criterion_verbose",
    "evaluate_group",
    "evaluate_group_detail",
    "load_screen",
    "ranking_metric_ids",
    "screen_metric_ids",
]
