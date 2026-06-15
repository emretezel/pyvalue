"""Post-screen ranking helpers.

Author: Emre Tezel
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Sequence

from .screen import RankingDefinition


_ALL_PEERS = "__all__"


@dataclass(frozen=True)
class ScreenRankingResult:
    """Computed ranking score and order for one screen run, keyed by ``listing_id``."""

    ordered_listing_ids: tuple[int, ...]
    scores: Dict[int, float]
    ranks: Dict[int, int]


def compute_screen_ranking(
    listing_ids: Sequence[int],
    ranking: RankingDefinition,
    metric_values: Mapping[str, Mapping[int, float]],
    sectors: Mapping[int, Optional[str]],
    *,
    display_symbols: Optional[Mapping[int, str]] = None,
) -> ScreenRankingResult:
    """Rank passing listings using percentile-based subscores.

    Identity is the ``listing_id`` throughout. ``display_symbols`` supplies the
    canonical symbol used *only* as the final deterministic tie-break key, so the
    output order stays stable and matches the historical symbol-ordered result;
    it never affects a score.
    """

    ordered_listing_ids = tuple(int(listing_id) for listing_id in listing_ids)
    if not ordered_listing_ids:
        return ScreenRankingResult(ordered_listing_ids=(), scores={}, ranks={})

    labels = dict(display_symbols or {})

    raw_values_by_metric: Dict[str, Dict[int, float]] = {
        metric_id: {
            int(listing_id): float(value) for listing_id, value in values.items()
        }
        for metric_id, values in metric_values.items()
    }
    capped_values_by_metric: Dict[str, Dict[int, float]] = {
        metric.metric_id: {
            listing_id: _apply_cap(value, metric.cap)
            for listing_id, value in raw_values_by_metric.get(
                metric.metric_id, {}
            ).items()
        }
        for metric in ranking.metrics
    }
    sector_members = _sector_members(ordered_listing_ids, sectors)
    eligible_sectors = {
        sector
        for sector, members in sector_members.items()
        if len(members) >= ranking.min_sector_peers
    }
    group_key_by_listing_id = {
        listing_id: _group_key_for_listing(
            listing_id,
            ranking=ranking,
            sectors=sectors,
            eligible_sectors=eligible_sectors,
        )
        for listing_id in ordered_listing_ids
    }
    group_listings = {
        _ALL_PEERS: ordered_listing_ids,
        **{
            sector: tuple(members)
            for sector, members in sector_members.items()
            if sector in eligible_sectors
        },
    }

    scores: Dict[int, float] = {}
    for listing_id in ordered_listing_ids:
        weighted_score = 0.0
        available_weight = 0.0
        group_key = group_key_by_listing_id[listing_id]
        peer_listings = group_listings[group_key]
        for metric in ranking.metrics:
            metric_values_for_listings = capped_values_by_metric.get(
                metric.metric_id, {}
            )
            value = metric_values_for_listings.get(listing_id)
            if value is None:
                continue
            peer_values = [
                metric_values_for_listings[peer_listing]
                for peer_listing in peer_listings
                if peer_listing in metric_values_for_listings
            ]
            if not peer_values:
                continue
            lower_bound = _quantile(
                peer_values,
                ranking.winsor_lower_percentile,
            )
            upper_bound = _quantile(
                peer_values,
                ranking.winsor_upper_percentile,
            )
            winsorized_peer_values = sorted(
                _winsorize(peer_value, lower_bound, upper_bound)
                for peer_value in peer_values
            )
            winsorized_value = _winsorize(value, lower_bound, upper_bound)
            percentile = _midrank_percentile(winsorized_peer_values, winsorized_value)
            subscore = (
                100.0 * percentile
                if metric.direction == "higher"
                else 100.0 * (1.0 - percentile)
            )
            weighted_score += metric.weight * subscore
            available_weight += metric.weight
        scores[listing_id] = (
            weighted_score / available_weight if available_weight else 0.0
        )

    sorted_listing_ids = tuple(
        sorted(
            ordered_listing_ids,
            key=lambda listing_id: _sort_key(
                listing_id,
                scores[listing_id],
                ranking=ranking,
                raw_values_by_metric=raw_values_by_metric,
                label=labels.get(listing_id, ""),
            ),
        )
    )
    ranks = {
        listing_id: idx for idx, listing_id in enumerate(sorted_listing_ids, start=1)
    }
    return ScreenRankingResult(
        ordered_listing_ids=sorted_listing_ids,
        scores=scores,
        ranks=ranks,
    )


def _sector_members(
    listing_ids: Sequence[int],
    sectors: Mapping[int, Optional[str]],
) -> Dict[str, list[int]]:
    members: Dict[str, list[int]] = {}
    for listing_id in listing_ids:
        sector = _normalized_sector(sectors.get(listing_id))
        if sector is None:
            continue
        members.setdefault(sector, []).append(listing_id)
    return members


def _group_key_for_listing(
    listing_id: int,
    *,
    ranking: RankingDefinition,
    sectors: Mapping[int, Optional[str]],
    eligible_sectors: set[str],
) -> str:
    sector = _normalized_sector(sectors.get(listing_id))
    if ranking.peer_group == "sector" and sector in eligible_sectors:
        return sector
    return _ALL_PEERS


def _normalized_sector(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _apply_cap(value: float, cap: Optional[float]) -> float:
    if cap is None:
        return value
    return min(value, cap)


def _winsorize(value: float, lower_bound: float, upper_bound: float) -> float:
    return max(lower_bound, min(value, upper_bound))


def _quantile(values: Sequence[float], percentile: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise ValueError("Cannot compute quantile for empty values")
    if len(ordered) == 1:
        return ordered[0]
    bounded = max(0.0, min(1.0, percentile))
    position = bounded * (len(ordered) - 1)
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    if lower_index == upper_index:
        return ordered[lower_index]
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    fraction = position - lower_index
    return lower_value + (upper_value - lower_value) * fraction


def _midrank_percentile(values: Sequence[float], value: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("Cannot compute percentile for empty values")
    lower = bisect_left(ordered, value)
    upper = bisect_right(ordered, value)
    equal_count = upper - lower
    return (lower + 0.5 * equal_count) / len(ordered)


def _sort_key(
    listing_id: int,
    score: float,
    *,
    ranking: RankingDefinition,
    raw_values_by_metric: Mapping[str, Mapping[int, float]],
    label: str,
) -> tuple[object, ...]:
    key_parts: list[object] = [-score]
    for tie_breaker in ranking.tie_breakers:
        metric_id = tie_breaker.metric_id
        if metric_id in {"canonical_symbol", "symbol", "ticker", "id"}:
            continue
        value = raw_values_by_metric.get(metric_id, {}).get(listing_id)
        key_parts.extend(_numeric_sort_key(value, tie_breaker.direction))
    # Final, fully-deterministic tie-break on the display symbol (label only) so
    # the output order is stable and matches the historical symbol ordering --
    # identity is the listing_id, but the alphabetical symbol order is preserved.
    key_parts.append(label)
    return tuple(key_parts)


def _numeric_sort_key(
    value: Optional[float],
    direction: str,
) -> tuple[int, float]:
    if value is None:
        return (1, 0.0)
    if direction == "descending":
        return (0, -value)
    return (0, value)


__all__ = ["ScreenRankingResult", "compute_screen_ranking"]
