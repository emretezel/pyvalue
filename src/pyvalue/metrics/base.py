"""Abstract metric base classes.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Optional, Protocol, Sequence, TypeVar

from pyvalue.currency import MetricUnitKind, metric_currency_or_none

if TYPE_CHECKING:
    # Imported only for annotations to avoid a runtime import cycle: the metrics
    # package already depends on pyvalue.facts at runtime, so importing it back
    # here eagerly would be circular.
    from pyvalue.facts import RegionFactsRepository

# wrap_metric_currency_invariants() takes a metric *class* and returns the same
# class with its ``compute`` wrapped, so this TypeVar preserves the caller's
# concrete class type instead of widening it to ``type``.
_MetricClassT = TypeVar("_MetricClassT", bound=type)


@dataclass(frozen=True)
class MetricMetadata:
    """Declared output-unit metadata for one metric id."""

    unit_kind: MetricUnitKind
    unit_label: Optional[str] = None


_METRIC_METADATA: dict[str, MetricMetadata] = {
    "working_capital": MetricMetadata("monetary"),
    "ncav": MetricMetadata("monetary"),
    "price_to_ncav": MetricMetadata("multiple", "x"),
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
    "price_to_book": MetricMetadata("multiple", "x"),
    "price_to_tangible_book": MetricMetadata("multiple", "x"),
    "price_to_fcf": MetricMetadata("multiple", "x"),
    "roc_greenblatt_5y_avg": MetricMetadata("percent"),
    "roe_greenblatt_5y_avg": MetricMetadata("percent"),
    "net_debt_to_ebitda": MetricMetadata("multiple", "x"),
    "short_term_debt_share": MetricMetadata("percent"),
    "ic_mqr": MetricMetadata("monetary"),
    "ic_fy": MetricMetadata("monetary"),
    "avg_ic": MetricMetadata("monetary"),
    "roic_ttm": MetricMetadata("percent"),
    "roce": MetricMetadata("percent"),
    "croic": MetricMetadata("percent"),
    "roic_10y_median": MetricMetadata("percent"),
    "roic_7y_median": MetricMetadata("percent"),
    "roic_years_above_12pct": MetricMetadata("count"),
    "roic_10y_min": MetricMetadata("percent"),
    "roic_7y_min": MetricMetadata("percent"),
    "iroic_5y": MetricMetadata("percent"),
    "gm_10y_std": MetricMetadata("percent"),
    "opm_10y_std": MetricMetadata("percent"),
    "opm_10y_min": MetricMetadata("percent"),
    "opm_7y_min": MetricMetadata("percent"),
    "share_count_cagr_5y": MetricMetadata("percent"),
    "share_count_cagr_10y": MetricMetadata("percent"),
    "shares_10y_pct_change": MetricMetadata("percent"),
    "net_buyback_yield": MetricMetadata("percent"),
    "ebit_yield_ev": MetricMetadata("percent"),
    "fcf_yield_ev": MetricMetadata("percent"),
    "ev_to_ebit": MetricMetadata("multiple", "x"),
    "ev_to_ebitda": MetricMetadata("multiple", "x"),
    "ev_to_sales": MetricMetadata("multiple", "x"),
    "fcf_to_ebitda": MetricMetadata("ratio"),
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
    """Represents the computed value of a metric for a listing."""

    listing_id: int
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
        listing_id: int,
        metric_id: str,
        value: float,
        as_of: str,
        currency: Optional[str],
        unit_label: Optional[str] = None,
    ) -> "MetricResult":
        return cls(
            listing_id=listing_id,
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
        listing_id: int,
        metric_id: str,
        value: float,
        as_of: str,
        currency: Optional[str],
        unit_label: Optional[str] = None,
    ) -> "MetricResult":
        return cls(
            listing_id=listing_id,
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
        listing_id: int,
        metric_id: str,
        value: float,
        as_of: str,
        unit_kind: MetricUnitKind = "ratio",
        unit_label: Optional[str] = None,
    ) -> "MetricResult":
        return cls(
            listing_id=listing_id,
            metric_id=metric_id,
            value=value,
            as_of=as_of,
            unit_kind=unit_kind,
            unit_label=unit_label,
        )


@dataclass
class MetricCurrencyInvariantError(RuntimeError):
    """Structured metric failure for currency invariant violations."""

    metric_id: str
    listing_id: int
    input_name: str
    reason_code: str
    expected_currency: Optional[str] = None
    actual_currency: Optional[str] = None
    as_of: Optional[str] = None

    def __str__(self) -> str:
        return (
            "Metric currency invariant violated "
            f"(metric={self.metric_id} listing_id={self.listing_id} "
            f"input={self.input_name} reason={self.reason_code} "
            f"expected={self.expected_currency} actual={self.actual_currency} "
            f"as_of={self.as_of})"
        )

    @property
    def summary_reason(self) -> str:
        if self.reason_code == "missing_trading_currency":
            return "currency invariant: missing listing currency"
        if self.reason_code == "missing_input_currency":
            return f"currency invariant: missing currency on {self.input_name}"
        if self.reason_code == "currency_mismatch":
            expected = self.expected_currency or "<expected>"
            actual = self.actual_currency or "<missing>"
            return f"currency invariant: {self.input_name} expected {expected} got {actual}"
        if self.reason_code == "missing_fx_rate":
            target = self.expected_currency or "<target>"
            source = self.actual_currency or "<source>"
            return (
                f"currency invariant: no FX rate to convert {self.input_name} "
                f"{source}->{target}"
            )
        return f"currency invariant: {self.reason_code}"


class Metric(Protocol):
    """Protocol that all metric implementations must follow."""

    id: str
    required_concepts: Sequence[str]
    unit_kind: MetricUnitKind
    unit_label: Optional[str]

    def compute(
        self, listing_id: int, repo: RegionFactsRepository
    ) -> Optional[MetricResult]: ...


def wrap_metric_currency_invariants(metric_cls: _MetricClassT) -> _MetricClassT:
    """Return ``metric_cls`` with ``compute`` guarded against invariant exceptions.

    Direct metric callers historically treat currency conflicts as an unavailable
    metric rather than as a process-level exception. We preserve that contract by
    storing the structured invariant error on the instance and returning ``None``.
    Higher-level CLI flows can then consume that stored error and report grouped
    invariant summaries without aborting the batch.
    """

    original_compute = getattr(metric_cls, "compute", None)
    if not callable(original_compute):
        return metric_cls
    if getattr(original_compute, "_pyvalue_currency_guarded", False):
        return metric_cls

    @wraps(original_compute)
    def guarded_compute(
        self: Metric, *args: object, **kwargs: object
    ) -> Optional[MetricResult]:
        setattr(self, "_last_currency_invariant_error", None)
        try:
            return original_compute(self, *args, **kwargs)
        except MetricCurrencyInvariantError as exc:
            setattr(self, "_last_currency_invariant_error", exc)
            return None

    setattr(guarded_compute, "_pyvalue_currency_guarded", True)
    setattr(metric_cls, "compute", guarded_compute)
    return metric_cls


def consume_metric_currency_invariant_error(
    metric: object,
) -> Optional[MetricCurrencyInvariantError]:
    """Return and clear the last guarded metric invariant error, if any."""

    error = getattr(metric, "_last_currency_invariant_error", None)
    setattr(metric, "_last_currency_invariant_error", None)
    if isinstance(error, MetricCurrencyInvariantError):
        return error
    return None


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


__all__ = [
    "Metric",
    "MetricCurrencyInvariantError",
    "MetricMetadata",
    "MetricResult",
    "consume_metric_currency_invariant_error",
    "metadata_for_metric",
    "wrap_metric_currency_invariants",
]
