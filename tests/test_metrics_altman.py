"""Tests for the Altman Z-Score metric.

Author: Emre Tezel
"""

from hypothesis import given
from hypothesis import strategies as st
import pytest

from pyvalue.metrics.altman_z import AltmanZMetric
from pyvalue.persistence.storage import FactRecord
from test_metrics import (
    LISTING_ID,
    _build_market_repo,
    _net_debt_quarter_dates,
    _OwnerEarningsRepo,
    _quarterly_records,
    fact,
)


def _altman_records(
    *,
    q4: str,
    q3: str,
    q2: str,
    q1: str,
    assets: float = 1000.0,
    liabilities: float = 600.0,
    current_assets: float = 400.0,
    current_liabilities: float = 200.0,
    retained_earnings: float | None = 300.0,
    ebit_quarters: tuple[float, float, float, float] | None = (25.0, 25.0, 25.0, 25.0),
    revenue_quarters: tuple[float, float, float, float] | None = (
        300.0,
        300.0,
        300.0,
        300.0,
    ),
    ebit_fy: float | None = None,
    revenue_fy: float | None = None,
) -> dict[str, list[FactRecord]]:
    # Defaults give the hand-computed vector: X1=0.2, X2=0.3, X3=0.1, X5=1.2,
    # and with the market repo pinning cap at 900, X4=1.5 -> Z = 3.09.
    def _instant(concept: str, value: float) -> list[FactRecord]:
        return [fact(concept=concept, fiscal_period="Q4", end_date=q4, value=value)]

    records: dict[str, list[FactRecord]] = {
        "Assets": _instant("Assets", assets),
        "Liabilities": _instant("Liabilities", liabilities),
        "AssetsCurrent": _instant("AssetsCurrent", current_assets),
        "LiabilitiesCurrent": _instant("LiabilitiesCurrent", current_liabilities),
    }
    if retained_earnings is not None:
        records["RetainedEarnings"] = _instant("RetainedEarnings", retained_earnings)
    if ebit_quarters is not None:
        records["OperatingIncomeLoss"] = _quarterly_records(
            "OperatingIncomeLoss", (q4, q3, q2, q1), ebit_quarters
        )
    elif ebit_fy is not None:
        records["OperatingIncomeLoss"] = [
            fact(
                concept="OperatingIncomeLoss",
                fiscal_period="FY",
                end_date=q4,
                value=ebit_fy,
            )
        ]
    if revenue_quarters is not None:
        records["Revenues"] = _quarterly_records(
            "Revenues", (q4, q3, q2, q1), revenue_quarters
        )
    elif revenue_fy is not None:
        records["Revenues"] = [
            fact(concept="Revenues", fiscal_period="FY", end_date=q4, value=revenue_fy)
        ]
    return records


def test_altman_z_computes_hand_vector() -> None:
    metric = AltmanZMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(_altman_records(q4=q4, q3=q3, q2=q2, q1=q1))

    result = metric.compute(
        LISTING_ID, repo, _build_market_repo(market_cap=900.0, as_of=q4)
    )

    # Z = 1.2*0.2 + 1.4*0.3 + 3.3*0.1 + 0.6*1.5 + 1.0*1.2 = 3.09 (safe zone).
    assert result is not None
    expected = 1.2 * 0.2 + 1.4 * 0.3 + 3.3 * 0.1 + 0.6 * 1.5 + 1.0 * 1.2
    assert result.value == pytest.approx(expected, rel=1e-12)
    assert result.as_of == q4


def test_altman_z_returns_none_when_retained_earnings_missing() -> None:
    metric = AltmanZMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _altman_records(q4=q4, q3=q3, q2=q2, q1=q1, retained_earnings=None)
    )

    # Strict five-factor policy: a missing component suppresses the score
    # rather than skewing it.
    assert (
        metric.compute(LISTING_ID, repo, _build_market_repo(market_cap=900.0, as_of=q4))
        is None
    )


def test_altman_z_allows_negative_retained_earnings() -> None:
    metric = AltmanZMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _altman_records(q4=q4, q3=q3, q2=q2, q1=q1, retained_earnings=-300.0)
    )

    result = metric.compute(
        LISTING_ID, repo, _build_market_repo(market_cap=900.0, as_of=q4)
    )

    # An accumulated deficit drags the score down but must not suppress it.
    assert result is not None
    expected = 1.2 * 0.2 + 1.4 * -0.3 + 3.3 * 0.1 + 0.6 * 1.5 + 1.0 * 1.2
    assert result.value == pytest.approx(expected, rel=1e-12)


def test_altman_z_uses_fy_fallback_for_flows() -> None:
    metric = AltmanZMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _altman_records(
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
            ebit_quarters=None,
            revenue_quarters=None,
            ebit_fy=100.0,
            revenue_fy=1200.0,
        )
    )

    result = metric.compute(
        LISTING_ID, repo, _build_market_repo(market_cap=900.0, as_of=q4)
    )

    # Annual EBIT/sales stand in when the quarterly history is too thin.
    assert result is not None
    expected = 1.2 * 0.2 + 1.4 * 0.3 + 3.3 * 0.1 + 0.6 * 1.5 + 1.0 * 1.2
    assert result.value == pytest.approx(expected, rel=1e-12)


def test_altman_z_returns_none_when_assets_non_positive() -> None:
    metric = AltmanZMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(_altman_records(q4=q4, q3=q3, q2=q2, q1=q1, assets=0.0))

    assert (
        metric.compute(LISTING_ID, repo, _build_market_repo(market_cap=900.0, as_of=q4))
        is None
    )


def test_altman_z_returns_none_when_market_cap_missing() -> None:
    metric = AltmanZMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(_altman_records(q4=q4, q3=q3, q2=q2, q1=q1))

    assert (
        metric.compute(LISTING_ID, repo, _build_market_repo(market_cap=None, as_of=q4))
        is None
    )


@given(
    current_assets=st.floats(min_value=0.0, max_value=1e6, allow_nan=False),
    current_liabilities=st.floats(min_value=0.0, max_value=1e6, allow_nan=False),
    retained=st.floats(min_value=-1e6, max_value=1e6, allow_nan=False),
    ebit_quarter=st.floats(min_value=-1e5, max_value=1e5, allow_nan=False),
    revenue_quarter=st.floats(min_value=0.0, max_value=1e6, allow_nan=False),
    price=st.floats(min_value=0.01, max_value=1e6, allow_nan=False),
)
def test_altman_z_matches_weighted_sum_property(
    current_assets: float,
    current_liabilities: float,
    retained: float,
    ebit_quarter: float,
    revenue_quarter: float,
    price: float,
) -> None:
    # Property: with assets/liabilities fixed (1000/600), Z is exactly the
    # stated weighted sum of the five factors. Expected mirrors the metric's
    # operation order; only the TTM sum (vs multiply) rounding differs.
    metric = AltmanZMetric()
    q4, q3, q2, q1 = _net_debt_quarter_dates()
    repo = _OwnerEarningsRepo(
        _altman_records(
            q4=q4,
            q3=q3,
            q2=q2,
            q1=q1,
            current_assets=current_assets,
            current_liabilities=current_liabilities,
            retained_earnings=retained,
            ebit_quarters=(ebit_quarter,) * 4,
            revenue_quarters=(revenue_quarter,) * 4,
        )
    )

    result = metric.compute(
        LISTING_ID, repo, _build_market_repo(market_cap=price, as_of=q4)
    )

    assert result is not None
    expected = (
        1.2 * ((current_assets - current_liabilities) / 1000.0)
        + 1.4 * (retained / 1000.0)
        + 3.3 * ((4 * ebit_quarter) / 1000.0)
        + 0.6 * (price / 600.0)
        + 1.0 * ((4 * revenue_quarter) / 1000.0)
    )
    assert result.value == pytest.approx(expected, rel=1e-9, abs=1e-9)
