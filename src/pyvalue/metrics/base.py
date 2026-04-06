"""Abstract metric base classes.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, Sequence

from pyvalue.currency import MetricUnitKind, metric_currency_or_none


@dataclass(frozen=True)
class MetricMetadata:
    """Declared output-unit metadata for one metric id."""

    unit_kind: MetricUnitKind
    unit_label: Optional[str] = None


_METRIC_METADATA: dict[str, MetricMetadata] = {
    "working_capital": MetricMetadata("monetary"),
    "current_ratio": MetricMetadata("ratio"),
    "debt_paydown_years": MetricMetadata("multiple", "years"),
    "fcf_to_debt": MetricMetadata("ratio"),
    "cfo_to_ni_ttm": MetricMetadata("ratio"),
    "cfo_to_ni_10y_median": MetricMetadata("ratio"),
    "fcf_fy_median_5y": MetricMetadata("monetary"),
    "fcf_neg_years_10y": MetricMetadata("count"),
    "ni_loss_years_10y": MetricMetadata("count"),
    "accruals_ratio": MetricMetadata("ratio"),
    "long_term_debt": MetricMetadata("monetary"),
    "eps_streak": MetricMetadata("count"),
    "eps_ttm": MetricMetadata("per_share", "per_share"),
    "eps_6y_avg": MetricMetadata("per_share", "per_share"),
    "graham_eps_10y_cagr_3y_avg": MetricMetadata("percent"),
    "graham_multiplier": MetricMetadata("multiple", "x"),
    "earnings_yield": MetricMetadata("percent"),
    "interest_coverage": MetricMetadata("ratio"),
    "market_cap": MetricMetadata("monetary"),
    "price_to_fcf": MetricMetadata("multiple", "x"),
    "roc_greenblatt_5y_avg": MetricMetadata("percent"),
    "roe_greenblatt_5y_avg": MetricMetadata("percent"),
    "net_debt_to_ebitda": MetricMetadata("multiple", "x"),
    "short_term_debt_share": MetricMetadata("percent"),
    "ic_mqr": MetricMetadata("monetary"),
    "ic_fy": MetricMetadata("monetary"),
    "avg_ic": MetricMetadata("monetary"),
    "roic_ttm": MetricMetadata("percent"),
    "roic_10y_median": MetricMetadata("percent"),
    "roic_7y_median": MetricMetadata("percent"),
    "roic_years_above_12pct": MetricMetadata("count"),
    "roic_10y_min": MetricMetadata("percent"),
    "roic_7y_min": MetricMetadata("percent"),
    "iroic_5y": MetricMetadata("percent"),
    "gm_10y_std": MetricMetadata("percent"),
    "opm_10y_std": MetricMetadata("percent"),
    "opm_10y_min": MetricMetadata("percent"),
    "share_count_cagr_10y": MetricMetadata("percent"),
    "shares_10y_pct_change": MetricMetadata("percent"),
    "net_buyback_yield": MetricMetadata("percent"),
    "ebit_yield_ev": MetricMetadata("percent"),
    "fcf_yield_ev": MetricMetadata("percent"),
    "ev_to_ebit": MetricMetadata("multiple", "x"),
    "ev_to_ebitda": MetricMetadata("multiple", "x"),
    "sbc_to_revenue": MetricMetadata("percent"),
    "sbc_to_fcf": MetricMetadata("percent"),
    "gross_margin_ttm": MetricMetadata("percent"),
    "operating_margin_ttm": MetricMetadata("percent"),
    "fcf_margin_ttm": MetricMetadata("percent"),
    "roe_ttm": MetricMetadata("percent"),
    "roa_ttm": MetricMetadata("percent"),
    "roetce_ttm": MetricMetadata("percent"),
    "dividend_yield_ttm": MetricMetadata("percent"),
    "shareholder_yield_ttm": MetricMetadata("percent"),
    "dividend_payout_ratio_ttm": MetricMetadata("percent"),
    "revenue_cagr_10y": MetricMetadata("percent"),
    "fcf_per_share_cagr_10y": MetricMetadata("percent"),
    "owner_earnings_cagr_10y": MetricMetadata("percent"),
    "gross_profit_to_assets_ttm": MetricMetadata("percent"),
    "return_on_invested_capital": MetricMetadata("percent"),
    "mcapex_fy": MetricMetadata("monetary"),
    "mcapex_5y": MetricMetadata("monetary"),
    "mcapex_ttm": MetricMetadata("monetary"),
    "nwc_mqr": MetricMetadata("monetary"),
    "nwc_fy": MetricMetadata("monetary"),
    "delta_nwc_ttm": MetricMetadata("monetary"),
    "delta_nwc_fy": MetricMetadata("monetary"),
    "delta_nwc_maint": MetricMetadata("monetary"),
    "oe_equity_ttm": MetricMetadata("monetary"),
    "oe_equity_5y_avg": MetricMetadata("monetary"),
    "oey_equity": MetricMetadata("percent"),
    "oey_equity_5y": MetricMetadata("percent"),
    "oey_ev": MetricMetadata("percent"),
    "oey_ev_norm": MetricMetadata("percent"),
    "oe_ev_ttm": MetricMetadata("monetary"),
    "oe_ev_5y_avg": MetricMetadata("monetary"),
    "oe_ev_fy_median_5y": MetricMetadata("monetary"),
    "worst_oe_ev_fy_10y": MetricMetadata("monetary"),
}


@dataclass
class MetricResult:
    """Represents the computed value of a metric for a symbol."""

    symbol: str
    metric_id: str
    value: float
    as_of: str
    unit_kind: MetricUnitKind = "other"
    currency: Optional[str] = None
    unit_label: Optional[str] = None

    def __post_init__(self) -> None:
        self.currency = metric_currency_or_none(self.unit_kind, self.currency)

    @classmethod
    def monetary(
        cls,
        *,
        symbol: str,
        metric_id: str,
        value: float,
        as_of: str,
        currency: Optional[str],
        unit_label: Optional[str] = None,
    ) -> "MetricResult":
        return cls(
            symbol=symbol,
            metric_id=metric_id,
            value=value,
            as_of=as_of,
            unit_kind="monetary",
            currency=currency,
            unit_label=unit_label,
        )

    @classmethod
    def per_share(
        cls,
        *,
        symbol: str,
        metric_id: str,
        value: float,
        as_of: str,
        currency: Optional[str],
        unit_label: Optional[str] = None,
    ) -> "MetricResult":
        return cls(
            symbol=symbol,
            metric_id=metric_id,
            value=value,
            as_of=as_of,
            unit_kind="per_share",
            currency=currency,
            unit_label=unit_label or "per_share",
        )

    @classmethod
    def ratio(
        cls,
        *,
        symbol: str,
        metric_id: str,
        value: float,
        as_of: str,
        unit_kind: MetricUnitKind = "ratio",
        unit_label: Optional[str] = None,
    ) -> "MetricResult":
        return cls(
            symbol=symbol,
            metric_id=metric_id,
            value=value,
            as_of=as_of,
            unit_kind=unit_kind,
            unit_label=unit_label,
        )


class Metric(Protocol):
    """Protocol that all metric implementations must follow."""

    id: str
    required_concepts: Sequence[str]
    unit_kind: MetricUnitKind
    unit_label: Optional[str]

    def compute(self, symbol: str, repo) -> Optional[MetricResult]: ...


def metadata_for_metric(
    metric_id: str, metric: Optional[Metric] = None
) -> MetricMetadata:
    """Return explicit output-unit metadata for ``metric_id``."""

    if metric is not None:
        unit_kind = getattr(metric, "unit_kind", None)
        if unit_kind is not None:
            return MetricMetadata(
                unit_kind=unit_kind,
                unit_label=getattr(metric, "unit_label", None),
            )
    return _METRIC_METADATA.get(metric_id, MetricMetadata("other"))
