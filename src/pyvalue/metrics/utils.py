"""Shared helpers for metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import logging
from typing import Callable, Dict, Iterable, List, Optional, Sequence, TypeVar

from pyvalue.currency import normalize_currency_code
from pyvalue.facts import FactView, RawFactSource
from pyvalue.metrics.base import MetricCurrencyInvariantError
from pyvalue.money import CurrencyMismatchError, Money
from pyvalue.storage import FactRecord, MarketDataRepository

# Default freshness windows (days)
MAX_FACT_AGE_DAYS = 400
MAX_FY_FACT_AGE_DAYS = 400

# Metric *metadata* helpers (recency, FY-frame filtering, quarterly selection)
# read only the provenance surface (:class:`~pyvalue.facts.FactView`), so they
# are generic over the concrete fact type -- a raw ``FactRecord`` or a
# kind-tagged ``MonetaryFact`` / ``ScalarFact`` -- and preserve it on return.
FactT = TypeVar("FactT", bound=FactView)

# Shares-outstanding concepts, in resolution priority. Mirrors the default order
# in ``FinancialFactsRepository.latest_share_counts_many`` so the on-demand
# market-cap share count matches the bulk reader.
SHARE_COUNT_CONCEPTS: tuple[str, ...] = (
    "EntityCommonStockSharesOutstanding",
    "CommonStockSharesOutstanding",
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketCap:
    """Market capitalization as ``Money`` plus the price date it was computed for.

    ``as_of`` is the ``market_data`` price date paired with the share-count fact
    (so callers that report a market-cap-derived value -- e.g. the
    ``market_cap`` metric itself -- can stamp the right observation date).
    """

    money: Money
    as_of: str


def is_recent_fact(
    record: FactView | None,
    *,
    max_age_days: int = MAX_FACT_AGE_DAYS,
    reference_date: date | None = None,
) -> bool:
    """Return True if the fact's end_date is within ``max_age_days`` of today."""

    if record is None or not record.end_date:
        return False
    try:
        end_date = date.fromisoformat(record.end_date)
    except ValueError:
        return False
    today = reference_date or date.today()
    cutoff = today - timedelta(days=max_age_days)
    return end_date >= cutoff


def has_recent_fact(
    repo, symbol: str, concepts: Sequence[str], max_age_days: int = MAX_FACT_AGE_DAYS
) -> bool:
    """Return True if any concept has a recent fact regardless of fiscal period."""

    for concept in concepts:
        record = None
        if hasattr(repo, "latest_fact"):
            record = repo.latest_fact(symbol, concept)
            if is_recent_fact(record, max_age_days=max_age_days):
                return True
        if hasattr(repo, "facts_for_concept"):
            records = repo.facts_for_concept(symbol, concept)  # type: ignore[arg-type]
            for rec in records:
                if is_recent_fact(rec, max_age_days=max_age_days):
                    return True
    return False


def filter_unique_fy(records: Iterable[FactT]) -> Dict[str, FactT]:
    """Return a dict of end_date -> fact for valid full-year entries."""

    unique: Dict[str, FactT] = {}
    for record in records:
        if not _is_valid_fy_frame(record.frame):
            continue
        if record.end_date not in unique:
            unique[record.end_date] = record
    return unique


def _is_valid_fy_frame(frame: str | None) -> bool:
    if not frame:
        return False
    if not frame.startswith("CY"):
        return False
    if frame.endswith(("Q1", "Q2", "Q3", "Q4")):
        return False
    year_part = frame[2:]
    return len(year_part) == 4 and year_part.isdigit()


def ttm_sum(records: Sequence[FactRecord], periods: int = 4) -> float | None:
    """Return the sum of the latest ``periods`` records if enough quarterly data exists."""

    quarterly = _filter_quarterly(records)
    if len(quarterly) < periods:
        return None
    return sum(item.value for item in quarterly[:periods])


def latest_quarterly_records(
    repo_fetcher: Callable[[str, str], Sequence[FactT]],
    symbol: str,
    concepts: Sequence[str],
    periods: int = 4,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> List[FactT]:
    """Fetch recent quarterly records for the first concept with enough data."""

    for concept in concepts:
        records = repo_fetcher(symbol, concept)
        quarterly = _filter_quarterly(records)
        if not quarterly:
            continue
        if not is_recent_fact(quarterly[0], max_age_days=max_age_days):
            continue
        if len(quarterly) >= periods:
            return quarterly[:periods]
    return []


def resolve_metric_ticker_currency(
    symbol: str,
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
        resolver = getattr(obj, "ticker_currency", None)
        if callable(resolver):
            resolved = normalize_currency_code(resolver(symbol))
            if resolved is not None:
                return resolved
    del candidate_currencies
    return None


def require_metric_ticker_currency(
    symbol: str,
    *objects: object,
    metric_id: str,
    input_name: str = "listing_currency",
    as_of: Optional[str] = None,
    candidate_currencies: Iterable[Optional[str]] = (),
) -> str:
    """Return the stored listing currency or raise a structured invariant error."""

    resolved = resolve_metric_ticker_currency(
        symbol,
        *objects,
        candidate_currencies=candidate_currencies,
    )
    if resolved is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            symbol=symbol,
            input_name=input_name,
            reason_code="missing_trading_currency",
            as_of=as_of,
        )
    assert resolved is not None
    return resolved


def _raise_currency_invariant(
    *,
    metric_id: str,
    symbol: str,
    input_name: str,
    reason_code: str,
    expected_currency: Optional[str] = None,
    actual_currency: Optional[str] = None,
    as_of: Optional[str] = None,
) -> None:
    error = MetricCurrencyInvariantError(
        metric_id=metric_id,
        symbol=symbol,
        input_name=input_name,
        reason_code=reason_code,
        expected_currency=expected_currency,
        actual_currency=actual_currency,
        as_of=as_of,
    )
    LOGGER.warning(
        "Metric currency invariant violated | metric=%s symbol=%s input=%s reason=%s expected=%s actual=%s as_of=%s",
        metric_id,
        symbol,
        input_name,
        reason_code,
        expected_currency,
        actual_currency,
        as_of,
    )
    raise error


def require_metric_money(
    money: Money,
    *,
    target_currency: str,
    metric_id: str,
    symbol: str,
    input_name: str,
    as_of: Optional[str],
) -> Money:
    """Return ``money`` guaranteed to be denominated in ``target_currency``.

    This is the single seam where a metric input's currency meets the listing
    (target) currency. In Phase 5a the listing-currency invariant is enforced by
    *rejection*: a mismatched input raises a structured
    :class:`MetricCurrencyInvariantError` (which
    :func:`wrap_metric_currency_invariants` turns into an unavailable metric)
    rather than letting downstream ``Money`` arithmetic raise an uncaught
    ``CurrencyMismatchError`` and abort the whole compute batch. Phase 5b will
    replace the raise with an FX conversion to ``target_currency`` -- call sites
    do not change, only this body.
    """

    if money.currency != target_currency:
        _raise_currency_invariant(
            metric_id=metric_id,
            symbol=symbol,
            input_name=input_name,
            reason_code="currency_mismatch",
            expected_currency=target_currency,
            actual_currency=money.currency,
            as_of=as_of,
        )
    return money


def require_metric_amount_money(
    amount: Optional[float],
    currency: Optional[str],
    *,
    target_currency: str,
    metric_id: str,
    symbol: str,
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
            symbol=symbol,
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
        symbol=symbol,
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


def _latest_share_count_fact(symbol: str, repo: RawFactSource) -> Optional[FactRecord]:
    """Return the most recent positive shares-outstanding fact (Entity first).

    The share count is a currency-less ``count`` fact, so it is read raw (no
    ``Money`` minting) and ``repo`` is typed against the minimal
    :class:`~pyvalue.facts.RawFactSource` -- satisfied by both the SQLite DAO and
    the metric-facing :class:`~pyvalue.facts.RegionFactsRepository`.
    """

    for concept in SHARE_COUNT_CONCEPTS:
        fact = repo.latest_fact(symbol, concept)
        if fact is not None and fact.value is not None and fact.value > 0:
            return fact
    return None


def market_cap_money(
    symbol: str,
    *,
    repo: RawFactSource,
    market_repo: MarketDataRepository,
    metric_id: str,
    target_currency: Optional[str] = None,
    contexts: Sequence[object] = (),
) -> Optional[MarketCap]:
    """Compute market cap on demand as a share-count fact x a co-dated price.

    Market cap is shares-outstanding x price, so persisting it (the removed
    ``market_data.market_cap`` column, migration 072) duplicated derivable
    state. The amount is the latest shares-outstanding ``financial_facts`` row
    times the ``market_data`` price *as of that fact's date*
    (:meth:`MarketDataRepository.price_as_of`). Co-dating the share count with
    its contemporaneous price means we never multiply today's price by a stale
    share count -- and it removes the need for the old cross-snapshot
    suspicious-jump guard.

    The stored price is already in the listing's major currency, so the market
    cap is too. Returns ``None`` when there is no usable share count or no price
    as of that date (the latter applies until ``update-market-data`` is extended
    to backfill prices at share-count dates -- see the refactor doc).

    The listing-currency invariant is preserved via the shared Money seam
    (:func:`require_metric_amount_money`): if the price currency differs from the
    resolved target this raises a structured
    :class:`MetricCurrencyInvariantError` rather than mixing currencies. Phase 5b
    will turn that seam into an FX conversion to the target instead.
    """

    share_fact = _latest_share_count_fact(symbol, repo)
    if share_fact is None:
        LOGGER.warning("%s: no shares-outstanding fact for %s", metric_id, symbol)
        return None

    snapshot = market_repo.price_as_of(symbol, share_fact.end_date)
    if snapshot is None or snapshot.price is None or snapshot.price <= 0:
        LOGGER.warning(
            "%s: no market price as of %s for %s",
            metric_id,
            share_fact.end_date,
            symbol,
        )
        return None

    # Resolve the target (listing) currency first, then mint the price and check
    # it against the target through the shared Money seam, so market cap obeys
    # the same 5a-reject / 5b-convert policy as every other monetary input.
    # Market cap = price (per share) x share count, so it carries the price's
    # (target) currency; the share count is a dimensionless multiplier.
    target = normalize_currency_code(target_currency) or resolve_metric_ticker_currency(
        symbol, *contexts
    )
    if target is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            symbol=symbol,
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
        symbol=symbol,
        input_name="market_cap_price",
        as_of=snapshot.as_of,
    )
    return MarketCap(money=price_money * share_fact.value, as_of=snapshot.as_of)


def _filter_quarterly(records: Iterable[FactT]) -> List[FactT]:
    filtered: List[FactT] = []
    seen_end_dates: set[str] = set()
    for record in records:
        period = (record.fiscal_period or "").upper()
        if period not in {"Q1", "Q2", "Q3", "Q4"}:
            continue
        if record.end_date in seen_end_dates:
            continue
        filtered.append(record)
        seen_end_dates.add(record.end_date)
    return filtered


__all__ = [
    "filter_unique_fy",
    "ttm_sum",
    "latest_quarterly_records",
    "is_recent_fact",
    "MAX_FY_FACT_AGE_DAYS",
    "MAX_FACT_AGE_DAYS",
    "has_recent_fact",
    "MarketCap",
    "market_cap_money",
    "require_metric_amount_money",
    "require_metric_money",
    "require_metric_ticker_currency",
    "resolve_metric_ticker_currency",
    "sum_money",
]
