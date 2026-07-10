"""Unit tests for the company-total share-count resolver.

Covers the pure arbitration rule (matrix + Hypothesis properties) and the
fact-reading wrapper (candidate gathering, lazy anchor, currency policy).
The anchored fixtures are the four real-world shapes from the 2026-07 P/B
investigation: GOOGL/PLTR (dual-class snapshot, periodic total) and TSLA/C
(true snapshot, weighted-average / issued-incl-treasury periodic).

Author: Emre Tezel
"""

from __future__ import annotations

import math
from typing import Optional

from hypothesis import given, strategies as st

from pyvalue.currency import MetricUnitKind
from pyvalue.marketdata.base import PriceData
from pyvalue.metrics.share_resolver import (
    ANCHOR_MAX_DISTANCE_RATIO,
    ANCHOR_PRICE_MAX_DISTANCE_DAYS,
    PROVIDER_MARKET_CAP_CONCEPT,
    SNAPSHOT_AGREEMENT_MAX_RATIO,
    STRUCTURAL_DIVERGENCE_MIN_RATIO,
    arbitrate_share_count,
    resolve_current_share_count,
)
from pyvalue.persistence.storage import FactRecord, MarketDataRepository

LISTING_ID = 1


# ---------------------------------------------------------------------------
# Pure rule: matrix
# ---------------------------------------------------------------------------


def test_neither_candidate_returns_none() -> None:
    assert arbitrate_share_count(None, None, 100.0) is None


def test_single_candidate_identity() -> None:
    assert arbitrate_share_count(100.0, None, None) == "snapshot"
    assert arbitrate_share_count(None, 100.0, None) == "periodic"


def test_non_positive_candidates_treated_as_absent() -> None:
    assert arbitrate_share_count(0.0, 100.0, None) == "periodic"
    assert arbitrate_share_count(-5.0, 100.0, None) == "periodic"
    assert arbitrate_share_count(100.0, 0.0, None) == "snapshot"
    assert arbitrate_share_count(0.0, -1.0, None) is None


def test_agreement_band_prefers_snapshot_and_ignores_anchor() -> None:
    # 1.9% apart: same total; the anchor (wildly off both) must not be consulted.
    assert arbitrate_share_count(100.0, 101.9, 500.0) == "snapshot"


def test_anchored_pick_both_directions() -> None:
    # The four verified real-world shapes (values in millions of shares).
    googl = arbitrate_share_count(5_822.0, 12_228.0, 12_134.0)
    pltr = arbitrate_share_count(2_291.5, 2_573.5, 2_487.5)
    tsla = arbitrate_share_count(3_752.4, 3_539.0, 3_821.6)
    citi = arbitrate_share_count(1_749.3, 1_862.6, 1_751.1)
    assert googl == "periodic"
    assert pltr == "periodic"
    assert tsla == "snapshot"
    assert citi == "snapshot"


def test_anchor_beyond_two_x_of_both_candidates_is_ignored() -> None:
    # Garbled anchor (e.g. computed off a sentinel price): fall through to the
    # anchorless policy in both directions of the structural threshold.
    assert arbitrate_share_count(100.0, 130.0, 1_000.0) == "periodic"  # >=1.25x
    assert arbitrate_share_count(100.0, 110.0, 1_000.0) == "snapshot"  # <1.25x


def test_anchored_exact_tie_goes_to_periodic() -> None:
    # 80 and 125 are geometrically equidistant from 100 (log distance 0.2231),
    # both within the 2x gate: the filing-based total wins the coin toss.
    assert arbitrate_share_count(80.0, 125.0, 100.0) == "periodic"


def test_anchorless_structural_gap_goes_to_periodic() -> None:
    assert arbitrate_share_count(100.0, 125.0, None) == "periodic"  # ratio 1.25
    assert arbitrate_share_count(5_822.0, 12_228.0, None) == "periodic"  # GOOGL


def test_anchorless_small_gap_keeps_snapshot() -> None:
    assert arbitrate_share_count(100.0, 120.0, None) == "snapshot"
    assert arbitrate_share_count(3_752.4, 3_539.0, None) == "snapshot"  # TSLA


# ---------------------------------------------------------------------------
# Pure rule: Hypothesis properties
# ---------------------------------------------------------------------------

# Share counts span pennies-stock floats to mega-caps; keep well inside float
# range so ratios/logs stay finite.
_COUNTS = st.floats(min_value=1e3, max_value=1e13, allow_nan=False)
_OPTIONAL_COUNTS = st.one_of(st.none(), _COUNTS)


@given(snapshot=_OPTIONAL_COUNTS, periodic=_OPTIONAL_COUNTS, implied=_OPTIONAL_COUNTS)
def test_result_is_always_a_candidate_or_none(
    snapshot: Optional[float],
    periodic: Optional[float],
    implied: Optional[float],
) -> None:
    result = arbitrate_share_count(snapshot, periodic, implied)
    assert result in (None, "snapshot", "periodic")
    if result == "snapshot":
        assert snapshot is not None
    if result == "periodic":
        assert periodic is not None
    if snapshot is None and periodic is None:
        assert result is None


@given(value=_COUNTS, implied=_OPTIONAL_COUNTS)
def test_single_candidate_is_returned_regardless_of_anchor(
    value: float, implied: Optional[float]
) -> None:
    assert arbitrate_share_count(value, None, implied) == "snapshot"
    assert arbitrate_share_count(None, value, implied) == "periodic"


@given(
    snapshot=_COUNTS,
    ratio=st.floats(min_value=1.0, max_value=SNAPSHOT_AGREEMENT_MAX_RATIO),
    implied=_OPTIONAL_COUNTS,
)
def test_agreement_band_dominates_any_anchor(
    snapshot: float, ratio: float, implied: Optional[float]
) -> None:
    periodic = snapshot * ratio
    assert arbitrate_share_count(snapshot, periodic, implied) == "snapshot"


@given(
    base=st.floats(min_value=1e4, max_value=1e12),
    gap=st.floats(min_value=0.05, max_value=0.6),
    offset_fraction=st.floats(min_value=0.0, max_value=0.9),
    toward_snapshot=st.booleans(),
)
def test_anchor_perturbation_stability(
    base: float, gap: float, offset_fraction: float, toward_snapshot: bool
) -> None:
    """Any implied strictly inside one candidate's log-neighbourhood picks it.

    The neighbourhood is half the log-gap between the candidates (strictly
    inside, so never on the tie boundary) capped by the 2x anchor gate; the
    choice must be invariant under anchor perturbation within it.
    """

    snapshot = base
    periodic = base * math.exp(gap)  # log-gap between candidates is `gap`
    # Log-offset of the implied value from the chosen candidate: strictly less
    # than half the gap (tie boundary) and than the anchor gate.
    offset = (
        offset_fraction * min(gap / 2.0, math.log(ANCHOR_MAX_DISTANCE_RATIO)) * 0.999
    )
    if toward_snapshot:
        implied = snapshot * math.exp(offset)
        expected = "snapshot"
    else:
        implied = periodic * math.exp(-offset)
        expected = "periodic"
    assert arbitrate_share_count(snapshot, periodic, implied) == expected


# ---------------------------------------------------------------------------
# Wrapper: fact-reading, laziness, currency policy
# ---------------------------------------------------------------------------


class _FakeFactsRepo:
    """Minimal RawFactSource: records per concept, newest-first like storage."""

    def __init__(self, records_by_concept: dict[str, list[FactRecord]]) -> None:
        self._records_by_concept = records_by_concept

    def facts_for_concept(
        self,
        listing_id: int,
        concept: str,
        fiscal_period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[FactRecord]:
        records = list(self._records_by_concept.get(concept, []))
        if fiscal_period:
            records = [r for r in records if r.fiscal_period == fiscal_period]
        if limit is not None:
            records = records[:limit]
        return records

    def latest_fact(self, listing_id: int, concept: str) -> Optional[FactRecord]:
        records = self.facts_for_concept(listing_id, concept)
        return records[0] if records else None


class _FakeMarketRepo(MarketDataRepository):
    """Nominal MarketDataRepository whose __init__ skips SQLite entirely."""

    def __init__(
        self,
        near_price: Optional[float] = None,
        near_currency: Optional[str] = "USD",
    ) -> None:
        self._near_price = near_price
        self._near_currency = near_currency
        self.near_date_calls = 0

    def snapshot_near_date_by_id(
        self,
        listing_id: int,
        as_of: str,
        *,
        max_distance_days: int,
    ) -> Optional[PriceData]:
        self.near_date_calls += 1
        assert max_distance_days == ANCHOR_PRICE_MAX_DISTANCE_DAYS
        if self._near_price is None:
            return None
        return PriceData(
            symbol="TEST.US",
            price=self._near_price,
            as_of=as_of,
            currency=self._near_currency,
        )


def _count(
    concept: str,
    value: float,
    *,
    fiscal_period: str,
    end_date: str,
    unit_kind: MetricUnitKind = "count",
) -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=concept,
        fiscal_period=fiscal_period,
        end_date=end_date,
        unit_kind=unit_kind,
        value=value,
        filed=None,
        currency=None,
    )


def _anchor(value: float, *, end_date: str, currency: str = "USD") -> FactRecord:
    return FactRecord(
        symbol="TEST.US",
        concept=PROVIDER_MARKET_CAP_CONCEPT,
        fiscal_period="INSTANT",
        end_date=end_date,
        unit_kind="monetary",
        value=value,
        filed=None,
        currency=currency,
    )


def _snapshot(value: float, *, end_date: str = "2026-03-29") -> FactRecord:
    return _count(
        "CommonStockSharesOutstanding",
        value,
        fiscal_period="INSTANT",
        end_date=end_date,
    )


def _periodic(
    value: float,
    *,
    concept: str = "EntityCommonStockSharesOutstanding",
    fiscal_period: str = "Q4",
    end_date: str = "2025-12-31",
) -> FactRecord:
    return _count(concept, value, fiscal_period=fiscal_period, end_date=end_date)


def test_snapshot_only_listing_resolves_to_snapshot() -> None:
    repo = _FakeFactsRepo({"CommonStockSharesOutstanding": [_snapshot(1_000.0)]})
    record = resolve_current_share_count(LISTING_ID, repo, _FakeMarketRepo())
    assert record is not None
    assert record.fiscal_period == "INSTANT"
    assert record.value == 1_000.0


def test_periodic_only_listing_resolves_to_periodic() -> None:
    repo = _FakeFactsRepo({"EntityCommonStockSharesOutstanding": [_periodic(2_000.0)]})
    record = resolve_current_share_count(LISTING_ID, repo, _FakeMarketRepo())
    assert record is not None
    assert record.fiscal_period == "Q4"
    assert record.value == 2_000.0


def test_agreement_band_never_touches_market_data() -> None:
    # Candidates 1% apart: the resolver must not read the anchor price even
    # though an anchor fact exists -- the common case stays one-query cheap.
    repo = _FakeFactsRepo(
        {
            "CommonStockSharesOutstanding": [_snapshot(1_010.0)],
            "EntityCommonStockSharesOutstanding": [_periodic(1_000.0)],
            PROVIDER_MARKET_CAP_CONCEPT: [_anchor(1e9, end_date="2026-03-29")],
        }
    )
    market = _FakeMarketRepo(near_price=100.0)
    record = resolve_current_share_count(LISTING_ID, repo, market)
    assert record is not None
    assert record.value == 1_010.0  # fresher snapshot
    assert market.near_date_calls == 0


def test_anchored_arbitration_rejects_dual_class_snapshot() -> None:
    # GOOGL-shaped: anchor / close endorses the periodic total.
    repo = _FakeFactsRepo(
        {
            "CommonStockSharesOutstanding": [
                _snapshot(5.822e9),
                _periodic(12.228e9, concept="CommonStockSharesOutstanding"),
            ],
            "EntityCommonStockSharesOutstanding": [_periodic(12.228e9)],
            PROVIDER_MARKET_CAP_CONCEPT: [_anchor(3_318_691e6, end_date="2026-03-29")],
        }
    )
    market = _FakeMarketRepo(near_price=273.50)
    record = resolve_current_share_count(LISTING_ID, repo, market)
    assert record is not None
    assert record.value == 12.228e9
    assert market.near_date_calls == 1


def test_anchored_arbitration_keeps_true_snapshot() -> None:
    # TSLA-shaped: anchor / close endorses the snapshot over the periodic row.
    repo = _FakeFactsRepo(
        {
            "CommonStockSharesOutstanding": [_snapshot(3.7524e9)],
            "EntityCommonStockSharesOutstanding": [_periodic(3.539e9)],
            PROVIDER_MARKET_CAP_CONCEPT: [_anchor(1_357_742e6, end_date="2026-03-29")],
        }
    )
    record = resolve_current_share_count(
        LISTING_ID, repo, _FakeMarketRepo(near_price=361.83)
    )
    assert record is not None
    assert record.value == 3.7524e9


def test_missing_anchor_fact_degrades_to_anchorless_rule() -> None:
    repo = _FakeFactsRepo(
        {
            "CommonStockSharesOutstanding": [_snapshot(5.822e9)],
            "EntityCommonStockSharesOutstanding": [_periodic(12.228e9)],
        }
    )
    record = resolve_current_share_count(LISTING_ID, repo, _FakeMarketRepo())
    assert record is not None
    assert record.value == 12.228e9  # structural gap -> periodic without anchor


def test_no_price_near_anchor_date_degrades_to_anchorless_rule() -> None:
    repo = _FakeFactsRepo(
        {
            "CommonStockSharesOutstanding": [_snapshot(3.7524e9)],
            "EntityCommonStockSharesOutstanding": [_periodic(3.539e9)],
            PROVIDER_MARKET_CAP_CONCEPT: [_anchor(1_357_742e6, end_date="2026-03-29")],
        }
    )
    market = _FakeMarketRepo(near_price=None)  # window miss
    record = resolve_current_share_count(LISTING_ID, repo, market)
    assert record is not None
    assert record.value == 3.7524e9  # small gap -> snapshot
    assert market.near_date_calls == 1


def test_anchor_price_currency_mismatch_is_ignored() -> None:
    # Anchor USD vs price GBP: reject the anchor (never FX-convert); the
    # structural gap then resolves periodic.
    repo = _FakeFactsRepo(
        {
            "CommonStockSharesOutstanding": [_snapshot(5.822e9)],
            "EntityCommonStockSharesOutstanding": [_periodic(12.228e9)],
            PROVIDER_MARKET_CAP_CONCEPT: [_anchor(3_318_691e6, end_date="2026-03-29")],
        }
    )
    market = _FakeMarketRepo(near_price=273.50, near_currency="GBP")
    record = resolve_current_share_count(LISTING_ID, repo, market)
    assert record is not None
    assert record.value == 12.228e9


def test_non_count_rows_are_skipped() -> None:
    repo = _FakeFactsRepo(
        {
            "CommonStockSharesOutstanding": [
                _count(
                    "CommonStockSharesOutstanding",
                    9.9e9,
                    fiscal_period="INSTANT",
                    end_date="2026-03-29",
                    unit_kind="other",
                ),
            ],
            "EntityCommonStockSharesOutstanding": [_periodic(2_000.0)],
        }
    )
    record = resolve_current_share_count(LISTING_ID, repo, _FakeMarketRepo())
    assert record is not None
    assert record.value == 2_000.0  # mis-kinded snapshot ignored


def test_fresher_common_periodic_beats_older_entity() -> None:
    repo = _FakeFactsRepo(
        {
            "EntityCommonStockSharesOutstanding": [
                _periodic(1_000.0, end_date="2025-09-30", fiscal_period="Q3")
            ],
            "CommonStockSharesOutstanding": [
                _periodic(
                    1_010.0,
                    concept="CommonStockSharesOutstanding",
                    end_date="2025-12-31",
                )
            ],
        }
    )
    record = resolve_current_share_count(LISTING_ID, repo, _FakeMarketRepo())
    assert record is not None
    assert record.end_date == "2025-12-31"
    assert record.value == 1_010.0


def test_equal_date_periodic_tie_goes_to_entity() -> None:
    repo = _FakeFactsRepo(
        {
            "EntityCommonStockSharesOutstanding": [_periodic(1_000.0)],
            "CommonStockSharesOutstanding": [
                _periodic(1_005.0, concept="CommonStockSharesOutstanding")
            ],
        }
    )
    record = resolve_current_share_count(LISTING_ID, repo, _FakeMarketRepo())
    assert record is not None
    assert record.concept == "EntityCommonStockSharesOutstanding"
    assert record.value == 1_000.0


def test_thresholds_are_wired_to_the_documented_values() -> None:
    # The constants are load-bearing for every case above; pin them so a
    # silent retune shows up in review.
    assert SNAPSHOT_AGREEMENT_MAX_RATIO == 1.02
    assert STRUCTURAL_DIVERGENCE_MIN_RATIO == 1.25
    assert ANCHOR_MAX_DISTANCE_RATIO == 2.0
    assert ANCHOR_PRICE_MAX_DISTANCE_DAYS == 10
