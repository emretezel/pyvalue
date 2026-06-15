"""Tests for post-screen ranking.

Author: Emre Tezel
"""

from collections.abc import Mapping, Sequence
from types import SimpleNamespace
from typing import Optional

from pyvalue.screening import compute_screen_ranking
from pyvalue.screening import RankingDefinition, RankingMetric, RankingTieBreaker


def _ranking_definition(
    metrics: Sequence[RankingMetric],
    tie_breakers: Sequence[RankingTieBreaker] = (),
    min_sector_peers: int = 10,
) -> RankingDefinition:
    return RankingDefinition(
        peer_group="sector",
        min_sector_peers=min_sector_peers,
        winsor_lower_percentile=0.05,
        winsor_upper_percentile=0.95,
        metrics=tuple(metrics),
        tie_breakers=tuple(tie_breakers),
    )


def _rank_by_symbol(
    symbols: Sequence[str],
    ranking: RankingDefinition,
    metric_values: Mapping[str, Mapping[str, float]],
    sectors: Mapping[str, Optional[str]],
) -> SimpleNamespace:
    """Adapt the ``listing_id``-keyed ranker to symbol-keyed test scenarios.

    Identity is the ``listing_id``; tests express scenarios by symbol, so this
    assigns each symbol a stable id, passes the symbols as the display labels (the
    final deterministic tie-break key), and maps the result back to symbols. It
    keeps the assertions readable and verifies that the id-keyed ranking preserves
    the historical symbol-ordered output.
    """

    ids = {symbol: index for index, symbol in enumerate(symbols, start=1)}
    reverse = {index: symbol for symbol, index in ids.items()}
    listing_ids = [ids[symbol] for symbol in symbols]
    metric_values_by_id = {
        metric_id: {ids[symbol]: value for symbol, value in values.items()}
        for metric_id, values in metric_values.items()
    }
    sectors_by_id = {ids[symbol]: sector for symbol, sector in sectors.items()}
    result = compute_screen_ranking(
        listing_ids,
        ranking,
        metric_values_by_id,
        sectors_by_id,
        display_symbols=reverse,
    )
    return SimpleNamespace(
        ordered_symbols=tuple(reverse[lid] for lid in result.ordered_listing_ids),
        scores={reverse[lid]: score for lid, score in result.scores.items()},
        ranks={reverse[lid]: rank for lid, rank in result.ranks.items()},
    )


def test_compute_screen_ranking_single_passer_is_neutral() -> None:
    ranking = _ranking_definition(
        [RankingMetric(metric_id="oey_ev_norm", weight=1.0, direction="higher")]
    )

    result = _rank_by_symbol(
        ["AAA.US"],
        ranking,
        {"oey_ev_norm": {"AAA.US": 0.08}},
        {"AAA.US": "Technology"},
    )

    assert result.ordered_symbols == ("AAA.US",)
    assert result.ranks == {"AAA.US": 1}
    assert result.scores["AAA.US"] == 50.0


def test_compute_screen_ranking_caps_metric_before_winsorization() -> None:
    ranking = _ranking_definition(
        [
            RankingMetric(
                metric_id="cfo_to_ni_ttm", weight=1.0, direction="higher", cap=1.5
            )
        ]
    )
    symbols = ["AAA.US", "BBB.US", "CCC.US", "DDD.US"]

    result = _rank_by_symbol(
        symbols,
        ranking,
        {
            "cfo_to_ni_ttm": {
                "AAA.US": 10.0,
                "BBB.US": 2.0,
                "CCC.US": 1.5,
                "DDD.US": 1.4,
            }
        },
        {symbol: None for symbol in symbols},
    )

    assert result.ordered_symbols == ("AAA.US", "BBB.US", "CCC.US", "DDD.US")
    assert result.scores["AAA.US"] == 62.5
    assert result.scores["BBB.US"] == 62.5
    assert result.scores["CCC.US"] == 62.5
    assert result.scores["DDD.US"] == 12.5


def test_compute_screen_ranking_uses_sector_peers_when_group_is_large_enough() -> None:
    ranking = _ranking_definition(
        [RankingMetric(metric_id="roic_10y_median", weight=1.0, direction="higher")]
    )
    tech_symbols = [f"T{i:02d}.US" for i in range(10)]
    other_symbols = ["O1.US", "O2.US"]
    symbols = tech_symbols + other_symbols
    metric_values = {
        "roic_10y_median": {
            **{symbol: float(index + 1) for index, symbol in enumerate(tech_symbols)},
            "O1.US": 100.0,
            "O2.US": 200.0,
        }
    }
    sectors = {symbol: "Technology" for symbol in tech_symbols}
    sectors["O1.US"] = "Utilities"
    sectors["O2.US"] = "Utilities"

    result = _rank_by_symbol(symbols, ranking, metric_values, sectors)

    assert result.ordered_symbols[0] == "O2.US"
    assert result.scores["T09.US"] == 95.0


def test_compute_screen_ranking_falls_back_to_full_universe_when_sector_too_small() -> (
    None
):
    ranking = _ranking_definition(
        [RankingMetric(metric_id="roic_10y_median", weight=1.0, direction="higher")],
        min_sector_peers=10,
    )
    tech_symbols = [f"T{i:02d}.US" for i in range(9)]
    other_symbols = ["O1.US", "O2.US"]
    symbols = tech_symbols + other_symbols
    metric_values = {
        "roic_10y_median": {
            **{symbol: float(index + 1) for index, symbol in enumerate(tech_symbols)},
            "O1.US": 100.0,
            "O2.US": 200.0,
        }
    }
    sectors = {symbol: "Technology" for symbol in tech_symbols}
    sectors["O1.US"] = "Utilities"
    sectors["O2.US"] = "Utilities"

    result = _rank_by_symbol(symbols, ranking, metric_values, sectors)

    assert result.scores["T08.US"] == 77.27272727272727


def test_compute_screen_ranking_normalizes_by_available_weight() -> None:
    ranking = _ranking_definition(
        [
            RankingMetric(metric_id="metric_a", weight=0.7, direction="higher"),
            RankingMetric(metric_id="metric_b", weight=0.3, direction="higher"),
        ]
    )

    result = _rank_by_symbol(
        ["AAA.US", "BBB.US"],
        ranking,
        {
            "metric_a": {"AAA.US": 10.0, "BBB.US": 5.0},
            "metric_b": {"BBB.US": 3.0},
        },
        {"AAA.US": None, "BBB.US": None},
    )

    assert result.scores["AAA.US"] == 75.0
    assert result.scores["BBB.US"] == 32.5


def test_compute_screen_ranking_uses_configured_tie_breakers() -> None:
    ranking = _ranking_definition(
        [RankingMetric(metric_id="primary", weight=1.0, direction="higher")],
        tie_breakers=(
            RankingTieBreaker(metric_id="oey_ev_norm", direction="descending"),
            RankingTieBreaker(metric_id="net_debt_to_ebitda", direction="ascending"),
            RankingTieBreaker(metric_id="canonical_symbol", direction="ascending"),
        ),
    )

    result = _rank_by_symbol(
        ["AAA.US", "BBB.US", "CCC.US"],
        ranking,
        {
            "primary": {"AAA.US": 1.0, "BBB.US": 1.0, "CCC.US": 1.0},
            "oey_ev_norm": {"AAA.US": 0.05, "BBB.US": 0.07, "CCC.US": 0.07},
            "net_debt_to_ebitda": {"AAA.US": 2.0, "BBB.US": 1.5, "CCC.US": 1.5},
        },
        {"AAA.US": None, "BBB.US": None, "CCC.US": None},
    )

    assert result.ordered_symbols == ("BBB.US", "CCC.US", "AAA.US")
