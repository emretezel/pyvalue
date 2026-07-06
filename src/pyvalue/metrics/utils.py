"""Shared helpers for metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date, timedelta
import logging
from typing import (
    Dict,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
)

from pyvalue.currency import normalize_currency_code
from pyvalue.facts import FactView, RawFactSource
from pyvalue.money.fx import FXService
from pyvalue.metrics.base import MetricCurrencyInvariantError
from pyvalue.money import CurrencyMismatchError, Money, fx_service_for_context
from pyvalue.persistence.storage import FactRecord, MarketDataRepository

# Default freshness windows (days), measured on the period end_date (not the
# filing date). Quarterly/TTM data stays at 400 days. FY-series metrics get
# 480: an annual-only filer with a December fiscal year end would otherwise go
# stale in early February of the second year -- months before its next annual
# report is published and ingested -- and "latest FY too old" was the top
# failure for several decade-window metrics (2026-07 screener audit). 480
# days keeps such filers screenable through late April while a genuinely dead
# listing still fails the quarterly gates and price-dependent metrics.
MAX_FACT_AGE_DAYS = 400
MAX_FY_FACT_AGE_DAYS = 480

# Metric *metadata* helpers (recency, FY filtering, quarterly selection)
# read only the provenance surface (:class:`~pyvalue.facts.FactView`), so they
# are generic over the concrete fact type -- a raw ``FactRecord`` or a
# kind-tagged ``MonetaryFact`` / ``ScalarFact`` -- and preserve it on return.
FactT = TypeVar("FactT", bound=FactView)

# The consecutive-year chain builder is agnostic about what a year's payload is
# (a single Money point, a (CFO, NI) pair, ...), so it stays generic.
_ChainValueT = TypeVar("_ChainValueT")

# Shares-outstanding concepts, in resolution priority. Mirrors the default order
# in ``FinancialFactsRepository.latest_share_counts_many_by_ids`` so the
# on-demand market-cap share count matches the bulk reader.
SHARE_COUNT_CONCEPTS: tuple[str, ...] = (
    "EntityCommonStockSharesOutstanding",
    "CommonStockSharesOutstanding",
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketCap:
    """Market capitalization as ``Money`` plus the price date it was computed for.

    ``as_of`` is the latest ``market_data`` price date (market cap = latest share
    count x latest price), so callers that report a market-cap-derived value --
    e.g. the ``market_cap`` metric itself -- can stamp the right observation date.
    """

    money: Money
    as_of: str


def is_recent_date(
    value: str | None,
    *,
    max_age_days: int = MAX_FACT_AGE_DAYS,
    reference_date: date | None = None,
) -> bool:
    """Return True if the ISO date string is within ``max_age_days`` of today.

    The bare-string sibling of :func:`is_recent_fact`, for callers that hold a
    derived ``as_of`` (an averaged point, a composite component) rather than a
    fact object.
    """

    if not value:
        return False
    try:
        end_date = date.fromisoformat(value)
    except ValueError:
        return False
    today = reference_date or date.today()
    cutoff = today - timedelta(days=max_age_days)
    return end_date >= cutoff


def is_recent_fact(
    record: FactView | None,
    *,
    max_age_days: int = MAX_FACT_AGE_DAYS,
    reference_date: date | None = None,
) -> bool:
    """Return True if the fact's end_date is within ``max_age_days`` of today."""

    if record is None:
        return False
    return is_recent_date(
        record.end_date, max_age_days=max_age_days, reference_date=reference_date
    )


def extract_year(value: str) -> Optional[int]:
    """Return the 4-digit year prefix of an ISO date string, or None.

    Year-over-year metrics pair facts by calendar year of their ``end_date``;
    this is the single shared parser for that convention.
    """

    if len(value) < 4 or not value[:4].isdigit():
        return None
    return int(value[:4])


def has_recent_fact(
    repo: RawFactSource,
    listing_id: int,
    concepts: Sequence[str],
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> bool:
    """Return True if any concept has a recent fact regardless of fiscal period."""

    for concept in concepts:
        record = None
        if hasattr(repo, "latest_fact"):
            record = repo.latest_fact(listing_id, concept)
            if is_recent_fact(record, max_age_days=max_age_days):
                return True
        if hasattr(repo, "facts_for_concept"):
            records = repo.facts_for_concept(listing_id, concept)
            for rec in records:
                if is_recent_fact(rec, max_age_days=max_age_days):
                    return True
    return False


def filter_unique_fy(records: Iterable[FactT]) -> Dict[str, FactT]:
    """Return a dict of end_date -> fact for full-year (``FY``) entries.

    Annual facts are identified by ``fiscal_period == "FY"``. The EODHD normalizer
    tags every full-year statement ``FY`` and reserves ``Q1``..``Q4`` for quarters
    and ``INSTANT``/``TTM`` for snapshot facts, so this single test selects exactly
    the annual rows the FY metrics (``eps_average``, ``eps_streak``,
    ``graham_eps_cagr``) consume. (This replaced an equivalent check on the derived
    ``CY####`` ``frame`` tag, which was dropped as redundant with ``fiscal_period``.)
    """

    unique: Dict[str, FactT] = {}
    for record in records:
        if record.fiscal_period != "FY":
            continue
        if record.end_date not in unique:
            unique[record.end_date] = record
    return unique


def latest_consecutive_year_chain(
    values_by_year: Mapping[int, _ChainValueT],
    *,
    max_years: int,
) -> list[tuple[int, _ChainValueT]]:
    """Return the longest consecutive-year suffix ending at the latest year.

    Anchored at ``max(values_by_year)`` and walking backwards one calendar year
    at a time, the chain stops at the first missing year or after ``max_years``
    entries, whichever comes first. Entries are returned newest-first as
    ``(year, value)`` pairs; an empty mapping (or non-positive ``max_years``)
    yields an empty list.

    This is the *adaptive* sibling of the strict ``range(latest, latest - N)``
    loops the FY-series metrics hand-roll: instead of failing outright when a
    year is missing, callers get whatever consecutive history exists and apply
    their own minimum-length policy. A fiscal-year-end change can leave a
    calendar-year hole in an otherwise continuous history; the chain then
    truncates at the hole (strictly better than the old hard failure, but the
    reason callers must still enforce a minimum length).
    """

    if not values_by_year or max_years <= 0:
        return []
    anchor = max(values_by_year)
    chain: list[tuple[int, _ChainValueT]] = []
    for year in range(anchor, anchor - max_years, -1):
        if year not in values_by_year:
            break
        chain.append((year, values_by_year[year]))
    return chain


def resolve_metric_ticker_currency(
    listing_id: int,
    *objects: object,
    candidate_currencies: Iterable[Optional[str]] = (),
) -> Optional[str]:
    """Return the stored listing currency for metric-side currency assertions.

    Listing currency is stored as ``listing.currency`` and may be a quote-unit
    subunit. Metric currency assertions use its normalized base currency.
    ``candidate_currencies`` is accepted for backwards-compatible call sites but is
    intentionally ignored here so metrics cannot silently infer a currency from facts.
    """

    for obj in objects:
        if obj is None:
            continue
        resolver = getattr(obj, "ticker_currency_by_id", None)
        if callable(resolver):
            resolved = normalize_currency_code(resolver(listing_id))
            if resolved is not None:
                return resolved
    del candidate_currencies
    return None


def require_metric_ticker_currency(
    listing_id: int,
    *objects: object,
    metric_id: str,
    input_name: str = "listing_currency",
    as_of: Optional[str] = None,
    candidate_currencies: Iterable[Optional[str]] = (),
) -> str:
    """Return the stored listing currency or raise a structured invariant error."""

    resolved = resolve_metric_ticker_currency(
        listing_id,
        *objects,
        candidate_currencies=candidate_currencies,
    )
    if resolved is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            listing_id=listing_id,
            input_name=input_name,
            reason_code="missing_trading_currency",
            as_of=as_of,
        )
    assert resolved is not None
    return resolved


def _raise_currency_invariant(
    *,
    metric_id: str,
    listing_id: int,
    input_name: str,
    reason_code: str,
    expected_currency: Optional[str] = None,
    actual_currency: Optional[str] = None,
    as_of: Optional[str] = None,
) -> None:
    error = MetricCurrencyInvariantError(
        metric_id=metric_id,
        listing_id=listing_id,
        input_name=input_name,
        reason_code=reason_code,
        expected_currency=expected_currency,
        actual_currency=actual_currency,
        as_of=as_of,
    )
    LOGGER.warning(
        "Metric currency invariant violated | metric=%s listing_id=%s input=%s reason=%s expected=%s actual=%s as_of=%s",
        metric_id,
        listing_id,
        input_name,
        reason_code,
        expected_currency,
        actual_currency,
        as_of,
    )
    raise error


# -- listing-currency FX context (Phase 5b) ----------------------------------
#
# The seam below converts every cross-currency metric input to the listing
# currency. It needs an :class:`~pyvalue.money.fx.FXService`, but the metric ``compute``
# signatures (and the ~36 ``_money`` call sites) deliberately do not carry one --
# the 5a seam promised "call sites do not change, only this body". So the compute
# driver binds one FX service for the whole batch via
# :func:`metric_fx_service_context`, and the seam reads it from this context var.
# When unbound (e.g. a unit test that never opened an FX-backed DB) the seam falls
# back to the no-fetch ephemeral service, so a cross-currency input with no
# available rate degrades to an unavailable metric -- the same observable outcome
# the 5a rejection produced.
_ACTIVE_FX_SERVICE: ContextVar[Optional[FXService]] = ContextVar(
    "_pyvalue_active_metric_fx_service", default=None
)


@contextmanager
def metric_fx_service_context(*contexts: object) -> Iterator[FXService]:
    """Bind one FX service (resolved from ``contexts``) for the duration of a batch.

    ``contexts`` are objects that may expose a ``db_path`` (the fact / market
    repos); the first match backs the FX service, so metric inputs convert against
    the same ``fx_rates`` the rest of the pipeline uses, and the service's rate
    cache is shared across every symbol and metric in the batch. The previous
    binding is restored on exit so sequential batches do not leak state.
    """

    service = fx_service_for_context(*contexts)
    token = _ACTIVE_FX_SERVICE.set(service)
    try:
        yield service
    finally:
        _ACTIVE_FX_SERVICE.reset(token)


@contextmanager
def reuse_or_bind_metric_fx_service(*contexts: object) -> Iterator[FXService]:
    """Reuse the already-bound batch FX service, or bind one if none is active.

    The compute driver binds a single service for the whole symbol loop and each
    symbol's compute reuses it through this helper -- so a batch builds one
    ``FXService`` (one rate cache, one schema check) instead of one per symbol.
    A standalone caller that computes a single symbol with no surrounding batch
    binding still gets a freshly bound service, preserving the same observable
    behaviour as :func:`metric_fx_service_context`.
    """

    existing = _ACTIVE_FX_SERVICE.get()
    if existing is not None:
        yield existing
        return
    with metric_fx_service_context(*contexts) as service:
        yield service


def _active_fx_service() -> FXService:
    """Return the batch FX service, or a no-fetch ephemeral one when unbound."""

    service = _ACTIVE_FX_SERVICE.get()
    return service if service is not None else fx_service_for_context()


def require_metric_money(
    money: Money,
    *,
    target_currency: str,
    metric_id: str,
    listing_id: int,
    input_name: str,
    as_of: Optional[str],
) -> Money:
    """Return ``money`` converted into ``target_currency`` (the listing currency).

    This is the single seam where a metric input's currency meets the listing
    (target) currency. A same-currency input is returned unchanged; a
    cross-currency input is converted via the active FX service
    (:func:`metric_fx_service_context`), **logging each conversion**. If no rate is
    available the conversion fails and a structured
    :class:`MetricCurrencyInvariantError` (``missing_fx_rate``) is raised, which
    :func:`wrap_metric_currency_invariants` turns into an unavailable metric rather
    than aborting the batch. Converting here -- not at each call site -- keeps the
    listing-currency invariant impossible to bypass: a metric cannot combine two
    currencies without first passing each through this seam.
    """

    if money.currency == target_currency:
        return money

    converted: Optional[Money] = None
    if as_of:
        converted = money.convert(
            target_currency, fx_service=_active_fx_service(), as_of=as_of
        )
    if converted is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            listing_id=listing_id,
            input_name=input_name,
            reason_code="missing_fx_rate",
            expected_currency=target_currency,
            actual_currency=money.currency,
            as_of=as_of,
        )
    assert converted is not None
    LOGGER.info(
        "metric FX conversion | metric=%s listing_id=%s input=%s %s->%s as_of=%s",
        metric_id,
        listing_id,
        input_name,
        money.currency,
        target_currency,
        as_of,
    )
    return converted


def require_metric_amount_money(
    amount: Optional[float],
    currency: Optional[str],
    *,
    target_currency: str,
    metric_id: str,
    listing_id: int,
    input_name: str,
    as_of: Optional[str],
) -> Money:
    """Mint ``Money`` from a raw ``(amount, currency)`` and enforce the target.

    For monetary inputs that do not arrive as a kind-tagged fact -- chiefly the
    market price read from ``market_data`` -- this mints a
    :class:`~pyvalue.money.Money` (collapsing any subunit at the boundary) and
    then applies :func:`require_metric_money`. A missing amount or unusable
    currency raises ``missing_input_currency``, matching the old assert-based
    flow.
    """

    money = Money.from_value(amount, currency)
    if money is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            listing_id=listing_id,
            input_name=input_name,
            reason_code="missing_input_currency",
            expected_currency=target_currency,
            as_of=as_of,
        )
    assert money is not None
    return require_metric_money(
        money,
        target_currency=target_currency,
        metric_id=metric_id,
        listing_id=listing_id,
        input_name=input_name,
        as_of=as_of,
    )


def sum_money(values: Sequence[Money]) -> Money:
    """Return the sum of a non-empty sequence of same-currency ``Money`` values.

    The float amounts are summed once and a single ``Money`` is minted, rather
    than folding ``Money + Money`` pairwise: folding would re-normalize every
    intermediate (a float->Decimal->float round-trip per step) and so accumulate
    float error differently from a plain sum -- this keeps the result bit-equal
    to summing the bare amounts, and does one normalization instead of N.

    Callers must first align every value to one currency (e.g. via
    :func:`require_metric_money`); a residual mismatch is a programming error and
    raises ``CurrencyMismatchError``. Empty input raises ``ValueError``.
    """

    if not values:
        raise ValueError("sum_money requires at least one value")
    currency = values[0].currency
    for value in values:
        if value.currency != currency:
            raise CurrencyMismatchError(currency, value.currency, "sum")
    return Money.of(sum(value.amount for value in values), currency)


def _latest_share_count_fact(
    listing_id: int, repo: RawFactSource
) -> Optional[FactRecord]:
    """Return the most recent positive shares-outstanding fact (Entity first).

    The share count is a currency-less ``count`` fact, so it is read raw (no
    ``Money`` minting) and ``repo`` is typed against the minimal
    :class:`~pyvalue.facts.RawFactSource` -- satisfied by both the SQLite DAO and
    the metric-facing :class:`~pyvalue.facts.RegionFactsRepository`.
    """

    for concept in SHARE_COUNT_CONCEPTS:
        fact = repo.latest_fact(listing_id, concept)
        if fact is not None and fact.value is not None and fact.value > 0:
            return fact
    return None


def market_cap_money(
    listing_id: int,
    *,
    repo: RawFactSource,
    market_repo: MarketDataRepository,
    metric_id: str,
    target_currency: Optional[str] = None,
    contexts: Sequence[object] = (),
) -> Optional[MarketCap]:
    """Compute market cap on demand as the latest share count x the latest price.

    Market cap is shares-outstanding x price, so persisting it (the removed
    ``market_data.market_cap`` column, migration 072) duplicated derivable
    state. The amount is the latest positive shares-outstanding
    ``financial_facts`` row times the latest ``market_data`` price
    (:meth:`MarketDataRepository.latest_snapshot_by_id`). Pairing the latest share
    count with the latest price -- rather than the price as of the share-count
    date -- means every price refresh re-prices market cap (and every metric
    built on it), which is what a value screen that refreshes prices between
    quarterly fundamentals needs. Shares outstanding move slowly, so a share
    count that is at most a quarter stale adds negligible error; price is the
    fast, decision-relevant input and is kept current. Both inputs are on the
    current split basis (EODHD adjusts historical share counts to it, and the
    latest price is as-traded today), so the product is split-consistent.

    The stored price is already in the listing's major currency, so the market
    cap is too. Returns ``None`` when there is no usable share count or no stored
    price at all; ``MarketCap.as_of`` is the latest price's date.

    The listing-currency invariant is preserved via the shared Money seam
    (:func:`require_metric_amount_money`): if the price currency differs from the
    resolved target this raises a structured
    :class:`MetricCurrencyInvariantError` rather than mixing currencies. Phase 5b
    will turn that seam into an FX conversion to the target instead.
    """

    share_fact = _latest_share_count_fact(listing_id, repo)
    if share_fact is None:
        LOGGER.warning(
            "%s: no shares-outstanding fact for listing_id=%s", metric_id, listing_id
        )
        return None

    snapshot = market_repo.latest_snapshot_by_id(listing_id)
    if snapshot is None or snapshot.price is None or snapshot.price <= 0:
        LOGGER.warning(
            "%s: no latest market price for listing_id=%s",
            metric_id,
            listing_id,
        )
        return None

    # Resolve the target (listing) currency first, then mint the price and check
    # it against the target through the shared Money seam, so market cap obeys
    # the same 5a-reject / 5b-convert policy as every other monetary input.
    # Market cap = price (per share) x share count, so it carries the price's
    # (target) currency; the share count is a dimensionless multiplier.
    target = normalize_currency_code(target_currency) or resolve_metric_ticker_currency(
        listing_id, *contexts
    )
    if target is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            listing_id=listing_id,
            input_name="market_cap",
            reason_code="missing_trading_currency",
            as_of=snapshot.as_of,
        )
    assert target is not None

    price_money = require_metric_amount_money(
        snapshot.price,
        snapshot.currency,
        target_currency=target,
        metric_id=metric_id,
        listing_id=listing_id,
        input_name="market_cap_price",
        as_of=snapshot.as_of,
    )
    return MarketCap(money=price_money * share_fact.value, as_of=snapshot.as_of)


__all__ = [
    "filter_unique_fy",
    "latest_consecutive_year_chain",
    "is_recent_fact",
    "MAX_FY_FACT_AGE_DAYS",
    "MAX_FACT_AGE_DAYS",
    "has_recent_fact",
    "MarketCap",
    "market_cap_money",
    "metric_fx_service_context",
    "reuse_or_bind_metric_fx_service",
    "require_metric_amount_money",
    "require_metric_money",
    "require_metric_ticker_currency",
    "resolve_metric_ticker_currency",
    "sum_money",
]
