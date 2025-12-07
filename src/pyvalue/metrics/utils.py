"""Shared helpers for metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pyvalue.storage import FactRecord

# Default freshness windows (days)
MAX_FACT_AGE_DAYS = 365
EODHD_FACT_AGE_DAYS = 365
MAX_FY_FACT_AGE_DAYS = 366

# Components that can be summed when AssetsCurrent is unavailable or stale.
ASSETS_CURRENT_COMPONENTS: Tuple[str, ...] = (
    "CashAndCashEquivalentsAtCarryingValue",
    "CashAndCashEquivalents",
    "ShortTermInvestments",
    "MarketableSecuritiesCurrent",
    "AvailableForSaleSecuritiesDebtSecuritiesCurrent",
    "HeldToMaturitySecuritiesCurrent",
    "AccountsReceivableNetCurrent",
    "LoansAndLeasesReceivableNetCurrent",
    "InventoryNet",
    "Inventories",
    "PrepaidExpenseAndOtherAssetsCurrent",
    "PrepaidExpenseCurrent",
    "DeferredTaxAssetsNetCurrent",
    "OtherAssetsCurrent",
    "OtherShortTermFinancialAssets",
    "CurrentFinancialAssetsOtherThanCashAndCashEquivalents",
    "TradeAndOtherCurrentReceivables",
    "CurrentTradeReceivables",
    "OtherCurrentReceivables",
    "CurrentTaxAssets",
    "OtherCurrentNonfinancialAssets",
)

# Components summed when LiabilitiesCurrent is unavailable or stale.
LIABILITIES_CURRENT_COMPONENTS: Tuple[str, ...] = (
    "AccountsPayableCurrent",
    "AccruedLiabilitiesCurrent",
    "EmployeeRelatedLiabilitiesCurrent",
    "TaxesPayableCurrent",
    "InterestPayableCurrent",
    "DeferredRevenueCurrent",
    "ShortTermBorrowings",
    "CommercialPaper",
    "LongTermDebtCurrent",
    "FinanceLeaseLiabilityCurrent",
    "OperatingLeaseLiabilityCurrent",
    "OtherLiabilitiesCurrent",
    "TradeAndOtherCurrentPayables",
    "CurrentTradePayables",
    "OtherCurrentPayables",
    "CurrentTaxLiabilities",
    "CurrentProvisions",
    "CurrentFinancialLiabilities",
    "CurrentBorrowings",
    "CurrentPortionOfNoncurrentBorrowings",
    "OtherCurrentFinancialLiabilities",
    "OtherCurrentNonfinancialLiabilities",
)


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
    effective_age = max_age_days
    if getattr(record, "provider", None) == "EODHD":
        effective_age = max(max_age_days, EODHD_FACT_AGE_DAYS)
    cutoff = today - timedelta(days=effective_age)
    return end_date >= cutoff


def has_recent_fact(repo, symbol: str, concepts: Sequence[str], max_age_days: int = MAX_FACT_AGE_DAYS) -> bool:
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


def resolve_assets_current(
    repo,
    symbol: str,
    *,
    end_date: Optional[str] = None,
    fiscal_period: Optional[str] = None,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> Optional[FactRecord]:
    """Return a fresh AssetsCurrent fact or a derived sum of component facts."""

    direct = repo.latest_fact(symbol, "AssetsCurrent") if hasattr(repo, "latest_fact") else None
    if direct and (end_date is None or direct.end_date == end_date) and is_recent_fact(direct, max_age_days=max_age_days):
        return direct

    derived = _derive_assets_current(repo, symbol, end_date=end_date, fiscal_period=fiscal_period, max_age_days=max_age_days)
    if derived:
        return derived

    if direct and end_date and direct.end_date == end_date and is_recent_fact(direct, max_age_days=max_age_days):
        return direct
    return None


def resolve_liabilities_current(
    repo,
    symbol: str,
    *,
    end_date: Optional[str] = None,
    fiscal_period: Optional[str] = None,
    max_age_days: int = MAX_FACT_AGE_DAYS,
) -> Optional[FactRecord]:
    """Return a fresh LiabilitiesCurrent fact or a derived sum of components."""

    direct = repo.latest_fact(symbol, "LiabilitiesCurrent") if hasattr(repo, "latest_fact") else None
    if direct and (end_date is None or direct.end_date == end_date) and is_recent_fact(direct, max_age_days=max_age_days):
        return direct

    derived = _derive_current_sum(
        repo,
        symbol,
        components=LIABILITIES_CURRENT_COMPONENTS,
        concept="LiabilitiesCurrent",
        end_date=end_date,
        fiscal_period=fiscal_period,
        max_age_days=max_age_days,
    )
    if derived:
        return derived

    if direct and end_date and direct.end_date == end_date and is_recent_fact(direct, max_age_days=max_age_days):
        return direct
    return None


def _derive_assets_current(
    repo,
    symbol: str,
    *,
    end_date: Optional[str],
    fiscal_period: Optional[str],
    max_age_days: int,
) -> Optional[FactRecord]:
    return _derive_current_sum(
        repo,
        symbol,
        components=ASSETS_CURRENT_COMPONENTS,
        concept="AssetsCurrent",
        end_date=end_date,
        fiscal_period=fiscal_period,
        max_age_days=max_age_days,
    )


def _derive_current_sum(
    repo,
    symbol: str,
    *,
    components: Sequence[str],
    concept: str,
    end_date: Optional[str],
    fiscal_period: Optional[str],
    max_age_days: int,
) -> Optional[FactRecord]:
    fetcher = repo.facts_for_concept if hasattr(repo, "facts_for_concept") else None
    if fetcher is None:
        return None

    grouped: Dict[str, List[FactRecord]] = {}
    for component in components:
        records = fetcher(symbol, component)
        selected = _select_fresh_record(records, end_date=end_date, max_age_days=max_age_days)
        if selected is None:
            continue
        grouped.setdefault(selected.end_date, []).append(selected)

    if not grouped:
        return None

    target_date = end_date
    if target_date and target_date not in grouped:
        return None
    if target_date is None:
        target_date = sorted(grouped.keys(), key=lambda d: (len(grouped[d]), d), reverse=True)[0]

    components = grouped.get(target_date, [])
    if not components:
        return None

    total = sum(record.value for record in components if record.value is not None)
    first = components[0]
    return FactRecord(
        symbol=symbol.upper(),
        provider="DERIVED",
        concept=concept,
        fiscal_period=fiscal_period or first.fiscal_period,
        end_date=target_date,
        unit=first.unit,
        value=total,
        accn=None,
        filed=None,
        frame=first.frame,
        start_date=None,
        accounting_standard=first.accounting_standard,
        currency=first.currency,
    )


def _select_fresh_record(records: Iterable[FactRecord], *, end_date: Optional[str], max_age_days: int) -> Optional[FactRecord]:
    for record in records:
        if record.value is None:
            continue
        if end_date is not None and record.end_date != end_date:
            continue
        if not is_recent_fact(record, max_age_days=max_age_days):
            continue
        return record
    return None


__all__ = [
    "filter_unique_fy",
    "ttm_sum",
    "latest_quarterly_records",
    "is_recent_fact",
    "MAX_FY_FACT_AGE_DAYS",
    "MAX_FACT_AGE_DAYS",
    "has_recent_fact",
    "ASSETS_CURRENT_COMPONENTS",
    "LIABILITIES_CURRENT_COMPONENTS",
    "resolve_assets_current",
    "resolve_liabilities_current",
]
