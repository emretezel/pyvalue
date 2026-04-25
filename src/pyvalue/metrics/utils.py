"""Shared helpers for metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta
import logging
from typing import Dict, Iterable, List, Optional, Sequence

from pyvalue.currency import normalize_currency_code
from pyvalue.metrics.base import MetricCurrencyInvariantError
from pyvalue.money import normalize_money_value
from pyvalue.storage import FactRecord

# Default freshness windows (days)
MAX_FACT_AGE_DAYS = 400
MAX_FY_FACT_AGE_DAYS = 400

LOGGER = logging.getLogger(__name__)


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


def normalize_market_cap_amount(
    amount: float,
    *,
    metric_id: str,
    symbol: str,
    input_name: str = "market_cap",
    as_of: Optional[str],
    expected_currency: Optional[str] = None,
    contexts: Sequence[object] = (),
) -> tuple[float, str]:
    """Assert a stored market cap against the listing's base currency."""

    base_currency = normalize_currency_code(
        expected_currency
    ) or resolve_metric_ticker_currency(symbol, *contexts)
    if base_currency is None:
        _raise_currency_invariant(
            metric_id=metric_id,
            symbol=symbol,
            input_name=input_name,
            reason_code="missing_trading_currency",
            as_of=as_of,
        )
    assert base_currency is not None
    return normalize_metric_amount(
        amount,
        base_currency,
        metric_id=metric_id,
        symbol=symbol,
        input_name=input_name,
        as_of=as_of,
        expected_currency=base_currency,
        contexts=contexts,
    )


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
    "normalize_market_cap_amount",
    "normalize_metric_amount",
    "normalize_metric_record",
    "require_metric_ticker_currency",
    "resolve_metric_ticker_currency",
]
