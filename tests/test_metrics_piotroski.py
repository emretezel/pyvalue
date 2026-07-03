"""Tests for the Piotroski F-Score metric.

Author: Emre Tezel
"""

from collections.abc import Sequence

from hypothesis import given
from hypothesis import strategies as st
import pytest

from pyvalue.metrics.base import metadata_for_metric
from pyvalue.metrics.piotroski import PiotroskiFScoreMetric
from pyvalue.persistence.storage import FactRecord
from test_metrics import (
    LISTING_ID,
    _net_debt_quarter_dates,
    _OwnerEarningsRepo,
    fact,
)


def _piotroski_records(
    *,
    latest_fy: str,
    ni: Sequence[float] = (100.0, 80.0),
    cfo: Sequence[float] = (120.0,),
    assets: Sequence[float] = (1000.0, 900.0, 800.0),
    ltd: Sequence[float] | None = (100.0, 150.0),
    current_assets: Sequence[float] = (400.0, 350.0),
    current_liabilities: Sequence[float] = (200.0, 200.0),
    gross_profit: Sequence[float] | None = (500.0, 400.0),
    cost_of_revenue: Sequence[float] | None = None,
    revenues: Sequence[float] = (1000.0, 850.0),
    shares: Sequence[float] = (100.0, 100.0),
) -> dict[str, list[FactRecord]]:
    # Each sequence is newest-first FY values: index 0 = year t, 1 = t-1, ...
    # The baseline hand-score is 9/9:
    #   F1 ROA_t = 100/900 > 0; F2 CFO 120 > 0; F3 dROA 0.111 > 0.100;
    #   F4 CFO 120 > NI 100; F5 lever 100/950 < 150/850; F6 CR 2.0 > 1.75;
    #   F7 shares flat; F8 GM 0.5 > 0.471; F9 turn 1.111 > 1.063.
    year = int(latest_fy[:4])
    suffix = latest_fy[4:]

    def _fy_series(concept: str, values: Sequence[float]) -> list[FactRecord]:
        return [
            fact(
                concept=concept,
                fiscal_period="FY",
                end_date=f"{year - offset}{suffix}",
                value=value,
            )
            for offset, value in enumerate(values)
        ]

    records: dict[str, list[FactRecord]] = {
        "NetIncomeLoss": _fy_series("NetIncomeLoss", ni),
        "NetCashProvidedByUsedInOperatingActivities": _fy_series(
            "NetCashProvidedByUsedInOperatingActivities", cfo
        ),
        "Assets": _fy_series("Assets", assets),
        "AssetsCurrent": _fy_series("AssetsCurrent", current_assets),
        "LiabilitiesCurrent": _fy_series("LiabilitiesCurrent", current_liabilities),
        "Revenues": _fy_series("Revenues", revenues),
        "CommonStockSharesOutstanding": _fy_series(
            "CommonStockSharesOutstanding", shares
        ),
    }
    if ltd is not None:
        records["LongTermDebt"] = _fy_series("LongTermDebt", ltd)
    if gross_profit is not None:
        records["GrossProfit"] = _fy_series("GrossProfit", gross_profit)
    if cost_of_revenue is not None:
        records["CostOfRevenue"] = _fy_series("CostOfRevenue", cost_of_revenue)
    return records


def test_piotroski_scores_nine_when_all_signals_pass() -> None:
    metric = PiotroskiFScoreMetric()
    latest_fy = _net_debt_quarter_dates()[0]
    repo = _OwnerEarningsRepo(_piotroski_records(latest_fy=latest_fy))

    result = metric.compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == 9.0
    assert result.as_of == latest_fy
    assert metadata_for_metric("piotroski_f_score").unit_kind == "count"


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        # F1 off (loss year); dROA and accruals still pass.
        pytest.param({"ni": (-10.0, -20.0)}, 8.0, id="loss-year"),
        # F2 and F4 both fail on negative CFO.
        pytest.param({"cfo": (-5.0,)}, 7.0, id="cash-burn"),
        # F3 off: ROA declined (80/900 < 100/800).
        pytest.param({"ni": (80.0, 100.0)}, 8.0, id="roa-decline"),
        # F4 off: earnings not cash-backed (CFO 90 < NI 100).
        pytest.param({"cfo": (90.0,)}, 8.0, id="accrual-earnings"),
        # F5 off: leverage rose (200/950 > 100/850).
        pytest.param({"ltd": (200.0, 100.0)}, 8.0, id="levering-up"),
        # F6 off: current ratio fell (1.5 < 1.75).
        pytest.param({"current_assets": (300.0, 350.0)}, 8.0, id="liquidity-drop"),
        # F7 off: share count grew.
        pytest.param({"shares": (105.0, 100.0)}, 8.0, id="dilution"),
        # F8 off: gross margin fell (0.4 < 450/850).
        pytest.param({"gross_profit": (400.0, 450.0)}, 8.0, id="margin-drop"),
        # F9 off: turnover fell (900/900 < 850/800); margin still improves.
        pytest.param({"revenues": (900.0, 850.0)}, 8.0, id="turnover-drop"),
    ],
)
def test_piotroski_flips_individual_signals(
    overrides: dict[str, tuple[float, ...]], expected: float
) -> None:
    metric = PiotroskiFScoreMetric()
    latest_fy = _net_debt_quarter_dates()[0]
    repo = _OwnerEarningsRepo(_piotroski_records(latest_fy=latest_fy, **overrides))

    result = metric.compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == expected


def test_piotroski_debt_free_pair_earns_leverage_point() -> None:
    metric = PiotroskiFScoreMetric()
    latest_fy = _net_debt_quarter_dates()[0]
    # No LongTermDebt facts at all: absence reads as zero debt in both years,
    # which earns the point (0 -> 0 is "did not lever up").
    repo = _OwnerEarningsRepo(_piotroski_records(latest_fy=latest_fy, ltd=None))

    result = metric.compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == 9.0


def test_piotroski_gross_profit_falls_back_to_cost_of_revenue() -> None:
    metric = PiotroskiFScoreMetric()
    latest_fy = _net_debt_quarter_dates()[0]
    repo = _OwnerEarningsRepo(
        _piotroski_records(
            latest_fy=latest_fy,
            gross_profit=None,
            cost_of_revenue=(500.0, 450.0),
        )
    )

    result = metric.compute(LISTING_ID, repo)

    # GM_t = (1000-500)/1000 = 0.5 > GM_(t-1) = (850-450)/850 -> point kept.
    assert result is not None
    assert result.value == 9.0


def test_piotroski_returns_none_without_three_consecutive_fy_assets() -> None:
    metric = PiotroskiFScoreMetric()
    latest_fy = _net_debt_quarter_dates()[0]
    repo = _OwnerEarningsRepo(
        _piotroski_records(latest_fy=latest_fy, assets=(1000.0, 900.0))
    )

    assert metric.compute(LISTING_ID, repo) is None


def test_piotroski_returns_none_when_cfo_missing() -> None:
    metric = PiotroskiFScoreMetric()
    latest_fy = _net_debt_quarter_dates()[0]
    records = _piotroski_records(latest_fy=latest_fy)
    records.pop("NetCashProvidedByUsedInOperatingActivities")
    repo = _OwnerEarningsRepo(records)

    assert metric.compute(LISTING_ID, repo) is None


def test_piotroski_returns_none_without_gross_profit_basis() -> None:
    metric = PiotroskiFScoreMetric()
    latest_fy = _net_debt_quarter_dates()[0]
    repo = _OwnerEarningsRepo(
        _piotroski_records(latest_fy=latest_fy, gross_profit=None, cost_of_revenue=None)
    )

    assert metric.compute(LISTING_ID, repo) is None


def test_piotroski_returns_none_when_prior_shares_missing() -> None:
    metric = PiotroskiFScoreMetric()
    latest_fy = _net_debt_quarter_dates()[0]
    repo = _OwnerEarningsRepo(_piotroski_records(latest_fy=latest_fy, shares=(100.0,)))

    assert metric.compute(LISTING_ID, repo) is None


@given(
    ni_latest=st.floats(min_value=-1e4, max_value=1e4, allow_nan=False),
    ni_prior=st.floats(min_value=-1e4, max_value=1e4, allow_nan=False),
    cfo_latest=st.floats(min_value=-1e4, max_value=1e4, allow_nan=False),
    assets_t=st.floats(min_value=1.0, max_value=1e6, allow_nan=False),
    assets_t1=st.floats(min_value=1.0, max_value=1e6, allow_nan=False),
    assets_t2=st.floats(min_value=1.0, max_value=1e6, allow_nan=False),
    ltd_latest=st.floats(min_value=0.0, max_value=1e6, allow_nan=False),
    ltd_prior=st.floats(min_value=0.0, max_value=1e6, allow_nan=False),
    ca_latest=st.floats(min_value=0.0, max_value=1e6, allow_nan=False),
    ca_prior=st.floats(min_value=0.0, max_value=1e6, allow_nan=False),
    cl_latest=st.floats(min_value=1.0, max_value=1e6, allow_nan=False),
    cl_prior=st.floats(min_value=1.0, max_value=1e6, allow_nan=False),
    gp_latest=st.floats(min_value=-1e5, max_value=1e6, allow_nan=False),
    gp_prior=st.floats(min_value=-1e5, max_value=1e6, allow_nan=False),
    rev_latest=st.floats(min_value=1.0, max_value=1e7, allow_nan=False),
    rev_prior=st.floats(min_value=1.0, max_value=1e7, allow_nan=False),
    shares_latest=st.floats(min_value=1.0, max_value=1e9, allow_nan=False),
    shares_prior=st.floats(min_value=1.0, max_value=1e9, allow_nan=False),
)
def test_piotroski_score_is_always_an_integer_in_range(
    ni_latest: float,
    ni_prior: float,
    cfo_latest: float,
    assets_t: float,
    assets_t1: float,
    assets_t2: float,
    ltd_latest: float,
    ltd_prior: float,
    ca_latest: float,
    ca_prior: float,
    cl_latest: float,
    cl_prior: float,
    gp_latest: float,
    gp_prior: float,
    rev_latest: float,
    rev_prior: float,
    shares_latest: float,
    shares_prior: float,
) -> None:
    # Invariant: whenever a score is emitted it is an integer count in [0, 9].
    metric = PiotroskiFScoreMetric()
    latest_fy = _net_debt_quarter_dates()[0]
    repo = _OwnerEarningsRepo(
        _piotroski_records(
            latest_fy=latest_fy,
            ni=(ni_latest, ni_prior),
            cfo=(cfo_latest,),
            assets=(assets_t, assets_t1, assets_t2),
            ltd=(ltd_latest, ltd_prior),
            current_assets=(ca_latest, ca_prior),
            current_liabilities=(cl_latest, cl_prior),
            gross_profit=(gp_latest, gp_prior),
            revenues=(rev_latest, rev_prior),
            shares=(shares_latest, shares_prior),
        )
    )

    result = metric.compute(LISTING_ID, repo)

    assert result is not None
    assert result.value == float(int(result.value))
    assert 0.0 <= result.value <= 9.0
