"""Post-screen ranking helpers.

Author: Emre Tezel
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Sequence

from pyvalue.screening import RankingDefinition


_ALL_PEERS = "__all__"


@dataclass(frozen=True)
class ScreenRankingResult:
    """Computed ranking score and order for one screen run."""

    ordered_symbols: tuple[str, ...]
    scores: Dict[str, float]
    ranks: Dict[str, int]


def compute_screen_ranking(
    symbols: Sequence[str],
    ranking: RankingDefinition,
    metric_values: Mapping[str, Mapping[str, float]],
    sectors: Mapping[str, Optional[str]],
) -> ScreenRankingResult:
    """Rank passing symbols using percentile-based subscores."""

    ordered_symbols = tuple(symbol.upper() for symbol in symbols)
    if not ordered_symbols:
        return ScreenRankingResult(ordered_symbols=(), scores={}, ranks={})

    raw_values_by_metric: Dict[str, Dict[str, float]] = {
        metric_id: {symbol.upper(): float(value) for symbol, value in values.items()}
        for metric_id, values in metric_values.items()
    }
    capped_values_by_metric: Dict[str, Dict[str, float]] = {
        metric.metric_id: {
            symbol: _apply_cap(value, metric.cap)
            for symbol, value in raw_values_by_metric.get(metric.metric_id, {}).items()
        }
        for metric in ranking.metrics
    }
    sector_members = _sector_members(ordered_symbols, sectors)
    eligible_sectors = {
        sector
        for sector, members in sector_members.items()
        if len(members) >= ranking.min_sector_peers
    }
    group_key_by_symbol = {
        symbol: _group_key_for_symbol(
            symbol,
            ranking=ranking,
            sectors=sectors,
            eligible_sectors=eligible_sectors,
        )
        for symbol in ordered_symbols
    }
    group_symbols = {
        _ALL_PEERS: ordered_symbols,
        **{
            sector: tuple(members)
            for sector, members in sector_members.items()
            if sector in eligible_sectors
        },
    }

    scores: Dict[str, float] = {}
    for symbol in ordered_symbols:
        weighted_score = 0.0
        available_weight = 0.0
        group_key = group_key_by_symbol[symbol]
        peer_symbols = group_symbols[group_key]
        for metric in ranking.metrics:
            metric_values_for_symbols = capped_values_by_metric.get(
                metric.metric_id, {}
            )
            value = metric_values_for_symbols.get(symbol)
            if value is None:
                continue
            peer_values = [
                metric_values_for_symbols[peer_symbol]
                for peer_symbol in peer_symbols
                if peer_symbol in metric_values_for_symbols
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
        scores[symbol] = weighted_score / available_weight if available_weight else 0.0

    sorted_symbols = tuple(
        sorted(
            ordered_symbols,
            key=lambda symbol: _sort_key(
                symbol,
                scores[symbol],
                ranking=ranking,
                raw_values_by_metric=raw_values_by_metric,
            ),
        )
    )
    ranks = {symbol: idx for idx, symbol in enumerate(sorted_symbols, start=1)}
    return ScreenRankingResult(
        ordered_symbols=sorted_symbols,
        scores=scores,
        ranks=ranks,
    )


def _sector_members(
    symbols: Sequence[str],
    sectors: Mapping[str, Optional[str]],
) -> Dict[str, list[str]]:
    members: Dict[str, list[str]] = {}
    for symbol in symbols:
        sector = _normalized_sector(sectors.get(symbol))
        if sector is None:
            continue
        members.setdefault(sector, []).append(symbol)
    return members


def _group_key_for_symbol(
    symbol: str,
    *,
    ranking: RankingDefinition,
    sectors: Mapping[str, Optional[str]],
    eligible_sectors: set[str],
) -> str:
    sector = _normalized_sector(sectors.get(symbol))
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
    symbol: str,
    score: float,
    *,
    ranking: RankingDefinition,
    raw_values_by_metric: Mapping[str, Mapping[str, float]],
) -> tuple[object, ...]:
    key_parts: list[object] = [-score]
    for tie_breaker in ranking.tie_breakers:
        metric_id = tie_breaker.metric_id
        if metric_id in {"canonical_symbol", "symbol", "ticker", "id"}:
            continue
        value = raw_values_by_metric.get(metric_id, {}).get(symbol)
        key_parts.extend(_numeric_sort_key(value, tie_breaker.direction))
    key_parts.append(symbol)
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
