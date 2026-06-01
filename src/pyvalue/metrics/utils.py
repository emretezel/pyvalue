"""Shared helpers for metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import logging
from typing import Dict, Iterable, List, Optional, Sequence

from pyvalue.currency import normalize_currency_code
from pyvalue.metrics.base import MetricCurrencyInvariantError
from pyvalue.money import Money, normalize_money_value
from pyvalue.storage import FactRecord, FinancialFactsRepository, MarketDataRepository

# Default freshness windows (days)
MAX_FACT_AGE_DAYS = 400
MAX_FY_FACT_AGE_DAYS = 400

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
    record: FactRecord | None,
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


def filter_unique_fy(records: Iterable[FactRecord]) -> Dict[str, FactRecord]:
    """Return a dict of end_date -> FactRecord for valid full-year entries."""

    unique: Dict[str, FactRecord] = {}
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
    repo_fetcher,
    symbol: str,
    concepts: Sequence[str],
    periods: int = 4,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> List[FactRecord]:
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


def ensure_metric_currency(
    *,
    metric_id: str,
    symbol: str,
    input_name: str,
    actual_currency: Optional[str],
    expected_currency: Optional[str] = None,
    as_of: Optional[str] = None,
    contexts: Sequence[object] = (),
    fallback_currencies: Iterable[Optional[str]] = (),
) -> str:
    """Assert that one currency-bearing metric input matches the ticker currency."""

    resolved_expected = normalize_currency_code(
        expected_currency
    ) or resolve_metric_ticker_currency(
        symbol,
        *contexts,
    )
    if resolved_expected is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            symbol=symbol,
            input_name=input_name,
            reason_code="missing_trading_currency",
            as_of=as_of,
        )
    assert resolved_expected is not None

    resolved_actual = normalize_currency_code(actual_currency)
    if resolved_actual is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            symbol=symbol,
            input_name=input_name,
            reason_code="missing_input_currency",
            expected_currency=resolved_expected,
            as_of=as_of,
        )
    if resolved_actual != resolved_expected:
        _raise_currency_invariant(
            metric_id=metric_id,
            symbol=symbol,
            input_name=input_name,
            reason_code="currency_mismatch",
            expected_currency=resolved_expected,
            actual_currency=resolved_actual,
            as_of=as_of,
        )
    return resolved_expected


def normalize_metric_amount(
    amount: float,
    currency: Optional[str],
    *,
    metric_id: str,
    symbol: str,
    input_name: str,
    as_of: Optional[str],
    expected_currency: Optional[str] = None,
    contexts: Sequence[object] = (),
    fallback_currencies: Iterable[Optional[str]] = (),
) -> tuple[float, str]:
    """Normalize one monetary/per-share input and assert listing-currency equality."""

    normalized_amount, normalized_currency = normalize_money_value(amount, currency)
    if normalized_amount is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            symbol=symbol,
            input_name=input_name,
            reason_code="missing_input_currency",
            expected_currency=normalize_currency_code(expected_currency),
            as_of=as_of,
        )
    assert normalized_amount is not None
    resolved_currency = ensure_metric_currency(
        metric_id=metric_id,
        symbol=symbol,
        input_name=input_name,
        actual_currency=normalized_currency,
        expected_currency=expected_currency,
        as_of=as_of,
        contexts=contexts,
        fallback_currencies=fallback_currencies,
    )
    return float(normalized_amount), resolved_currency


def _latest_share_count_fact(
    symbol: str, repo: FinancialFactsRepository
) -> Optional[FactRecord]:
    """Return the most recent positive shares-outstanding fact (Entity first)."""

    for concept in SHARE_COUNT_CONCEPTS:
        fact = repo.latest_fact(symbol, concept)
        if fact is not None and fact.value is not None and fact.value > 0:
            return fact
    return None


def market_cap_money(
    symbol: str,
    *,
    repo: FinancialFactsRepository,
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

    The listing-currency invariant is preserved: if ``target_currency`` (or the
    resolved listing currency) differs from the price currency this raises a
    structured :class:`MetricCurrencyInvariantError` rather than mixing
    currencies. Phase 5 will replace that hard failure with an FX conversion.
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

    price_currency = normalize_currency_code(snapshot.currency)
    if price_currency is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            symbol=symbol,
            input_name="market_cap_price",
            reason_code="missing_input_currency",
            as_of=snapshot.as_of,
        )
    assert price_currency is not None

    cap = Money.of(snapshot.price * share_fact.value, price_currency)

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
    if target != cap.currency:
        _raise_currency_invariant(
            metric_id=metric_id,
            symbol=symbol,
            input_name="market_cap",
            reason_code="currency_mismatch",
            expected_currency=target,
            actual_currency=cap.currency,
            as_of=snapshot.as_of,
        )
    return MarketCap(money=cap, as_of=snapshot.as_of)


def normalize_metric_record(
    record: FactRecord,
    *,
    metric_id: str,
    symbol: str,
    input_name: Optional[str] = None,
    expected_currency: Optional[str] = None,
    contexts: Sequence[object] = (),
    fallback_currencies: Iterable[Optional[str]] = (),
) -> tuple[float, str]:
    """Normalize one fact value and assert the fact currency matches the ticker."""

    return normalize_metric_amount(
        record.value,
        record.currency,
        metric_id=metric_id,
        symbol=symbol,
        input_name=input_name or record.concept,
        as_of=record.end_date,
        expected_currency=expected_currency,
        contexts=contexts,
        fallback_currencies=fallback_currencies,
    )


def align_metric_money_values(
    *,
    values: Iterable[tuple[float, Optional[str], str, str]],
    metric_id: str,
    symbol: str,
    expected_currency: Optional[str] = None,
    contexts: Sequence[object] = (),
) -> tuple[list[float], str]:
    """Return metric inputs after enforcing a shared listing-currency invariant."""

    collected = list(values)
    resolved_currency = normalize_currency_code(
        expected_currency
    ) or resolve_metric_ticker_currency(
        symbol,
        *contexts,
    )
    if resolved_currency is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            symbol=symbol,
            input_name="listing_currency",
            reason_code="missing_trading_currency",
            as_of=collected[0][2] if collected else None,
        )
    assert resolved_currency is not None

    aligned: list[float] = []
    for amount, currency, as_of, field_name in collected:
        normalized_amount, _ = normalize_metric_amount(
            amount,
            currency,
            metric_id=metric_id,
            symbol=symbol,
            input_name=field_name,
            as_of=as_of,
            expected_currency=resolved_currency,
            contexts=contexts,
        )
        aligned.append(normalized_amount)
    return aligned, resolved_currency


def _filter_quarterly(records: Iterable[FactRecord]) -> List[FactRecord]:
    filtered: List[FactRecord] = []
    seen_end_dates: set[str] = set()
    for record in records:
        period = (record.fiscal_period or "").upper()
        if period not in {"Q1", "Q2", "Q3", "Q4"}:
            continue
        if record.end_date in seen_end_dates:
            continue
        if record.value is None:
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
    "align_metric_money_values",
    "ensure_metric_currency",
    "MarketCap",
    "market_cap_money",
    "normalize_metric_amount",
    "normalize_metric_record",
    "require_metric_ticker_currency",
    "resolve_metric_ticker_currency",
]
