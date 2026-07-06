"""Screening: screen-definition parsing/evaluation and post-screen ranking.

This package groups screen evaluation (``screen``) with the ranking that runs
over a screen's results (``ranking``). Both public APIs are re-exported here,
so call sites keep using ``from pyvalue.screening import ScreenDefinition`` and
``from pyvalue.screening import compute_screen_ranking``.

Author: Emre Tezel
"""

from .ranking import ScreenRankingResult, compute_screen_ranking
from .screen import (
    Criterion,
    CriterionEvaluation,
    CriterionGroup,
    GroupEvaluation,
    RankingDefinition,
    RankingMetric,
    RankingTieBreaker,
    ResolvedTerm,
    ScreenDefinition,
    Term,
    evaluate_criterion,
    evaluate_criterion_detail,
    evaluate_criterion_verbose,
    evaluate_group,
    evaluate_group_detail,
    load_screen,
    ranking_metric_ids,
    screen_metric_ids,
)

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
    "ScreenRankingResult",
    "Term",
    "compute_screen_ranking",
    "evaluate_criterion",
    "evaluate_criterion_detail",
    "evaluate_criterion_verbose",
    "evaluate_group",
    "evaluate_group_detail",
    "load_screen",
    "ranking_metric_ids",
    "screen_metric_ids",
]
