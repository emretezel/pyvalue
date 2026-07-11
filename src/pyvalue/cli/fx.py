"""CLI handlers for refreshing FX rates from the EODHD provider.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import (
    Optional,
    Sequence,
)

from pyvalue.currency import (
    normalize_currency_code,
)
from pyvalue.money.fx import (
    EODHDFXProvider,
)
from pyvalue.persistence.storage import (
    FXRateRecord,
    FXRefreshStateRepository,
    FXRatesRepository,
    FXSupportedPairRecord,
    FXSupportedPairsRepository,
)

from ._common import (
    FX_FULL_BACKFILL_START,
    LOGGER,
    _require_eodhd_key,
    _resolve_database_path,
)


def _print_fx_progress_bar(
    completed_batches: int,
    total_batches: int,
    *,
    item_label: Optional[str] = None,
) -> None:
    """Print a compact ASCII bar for FX refresh batching."""

    if total_batches <= 0:
        percent = 100.0
    else:
        percent = (completed_batches / total_batches) * 100.0
    bar_width = 20
    filled_width = min(bar_width, max(0, round((percent / 100.0) * bar_width)))
    bar = "#" * filled_width + "-" * (bar_width - filled_width)
    item_suffix = f" pair={item_label}" if item_label else ""
    print(
        f"Progress: [{bar}] {completed_batches}/{total_batches} FX batches complete ({percent:.1f}%){item_suffix}",
        flush=True,
    )


def cmd_refresh_fx_rates(
    database: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> int:
    """Refresh and store direct FX rates from the EODHD provider."""

    resolved_start, resolved_end, explicit_start_date = _resolve_fx_refresh_dates(
        start_date,
        end_date,
    )
    return _cmd_refresh_fx_rates_eodhd(
        database=database,
        start_date=resolved_start,
        end_date=resolved_end,
        explicit_start_date=explicit_start_date,
    )


def _resolve_fx_refresh_dates(
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple[date, date, bool]:
    try:
        resolved_end = date.fromisoformat(end_date) if end_date else date.today()
    except ValueError as exc:
        raise SystemExit(f"Invalid --end-date value: {end_date}") from exc
    try:
        resolved_start = date.fromisoformat(start_date) if start_date else resolved_end
    except ValueError as exc:
        raise SystemExit(f"Invalid --start-date value: {start_date}") from exc
    if resolved_start > resolved_end:
        raise SystemExit("--start-date must be on or before --end-date")
    return resolved_start, resolved_end, start_date is not None


def _parse_optional_rate_date(value: Optional[str]) -> Optional[date]:
    """Return a parsed ISO date or None for empty/invalid stored coverage."""

    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _describe_eodhd_fx_refresh_scope(
    *,
    start_date: date,
    end_date: date,
    explicit_start_date: bool,
) -> str:
    """Return a user-facing description of the requested EODHD refresh scope."""

    if explicit_start_date:
        return f"requested_range={start_date.isoformat()}..{end_date.isoformat()}"
    return (
        "mode=auto-full-history "
        f"requested_end={end_date.isoformat()} "
        f"first_backfill_start={FX_FULL_BACKFILL_START.isoformat()}"
    )


def _plan_eodhd_fx_refresh_ranges(
    *,
    start_date: date,
    end_date: date,
    min_rate_date: Optional[str],
    max_rate_date: Optional[str],
    full_history_backfilled: bool,
    explicit_start_date: bool,
) -> tuple[list[tuple[date, date]], bool]:
    """Return the older/newer EODHD FX history ranges that need refresh."""

    min_covered = _parse_optional_rate_date(min_rate_date)
    max_covered = _parse_optional_rate_date(max_rate_date)
    if min_covered is None or max_covered is None:
        if explicit_start_date:
            return [(start_date, end_date)], False
        return [(FX_FULL_BACKFILL_START, end_date)], True

    ranges: list[tuple[date, date]] = []
    next_full = full_history_backfilled
    if explicit_start_date:
        if start_date < min_covered:
            older_end = min_covered - timedelta(days=1)
            if start_date <= older_end:
                ranges.append((start_date, older_end))
        if end_date > max_covered:
            newer_start = max_covered + timedelta(days=1)
            if newer_start <= end_date:
                ranges.append((newer_start, end_date))
        return ranges, next_full

    older_needed = False
    if not full_history_backfilled and FX_FULL_BACKFILL_START < min_covered:
        older_end = min_covered - timedelta(days=1)
        if FX_FULL_BACKFILL_START <= older_end:
            ranges.append((FX_FULL_BACKFILL_START, older_end))
            older_needed = True
    if end_date > max_covered:
        newer_start = max_covered + timedelta(days=1)
        if newer_start <= end_date:
            ranges.append((newer_start, end_date))
    next_full = full_history_backfilled or not older_needed
    return ranges, next_full


def _extend_coverage(
    current_min: Optional[str],
    current_max: Optional[str],
    rows: Sequence[FXRateRecord],
) -> tuple[Optional[str], Optional[str]]:
    """Widen stored min/max coverage by the dates in a freshly upserted batch.

    Upserts only insert or refresh rows inside the fetched window -- they never
    delete -- so a pair's new coverage is its prior coverage widened by the
    batch's own dates. Deriving it here avoids re-querying ``provider_fx_rates``
    with a second per-range ``pair_coverage`` MIN/MAX scan. ``rate_date`` values
    are zero-padded ISO-8601 strings, so lexical min/max match chronological
    order and reproduce exactly what ``pair_coverage`` would return after the
    upsert.
    """

    batch_dates = [record.rate_date for record in rows if record.rate_date]
    if not batch_dates:
        return current_min, current_max
    batch_min = min(batch_dates)
    batch_max = max(batch_dates)
    new_min = batch_min if current_min is None else min(current_min, batch_min)
    new_max = batch_max if current_max is None else max(current_max, batch_max)
    return new_min, new_max


def _cmd_refresh_fx_rates_eodhd(
    *,
    database: str,
    start_date: date,
    end_date: date,
    explicit_start_date: bool,
) -> int:
    """Refresh direct FX rates from the EODHD FOREX catalog."""

    db_path = _resolve_database_path(database)
    print(
        "Preparing FX refresh schema and indexes (the first run after an upgrade may take a while on large databases)...",
        flush=True,
    )
    fx_repo = FXRatesRepository(db_path)
    catalog_repo = FXSupportedPairsRepository(db_path)
    state_repo = FXRefreshStateRepository(db_path)
    provider = EODHDFXProvider(api_key=_require_eodhd_key())

    print("Syncing EODHD FOREX catalog...", flush=True)
    catalog_entries = provider.list_catalog()
    catalog_repo.replace_provider_catalog(
        provider.provider_name,
        [
            FXSupportedPairRecord(
                provider=provider.provider_name,
                symbol=entry.symbol,
                canonical_symbol=entry.canonical_symbol,
                base_currency=entry.base_currency,
                quote_currency=entry.quote_currency,
                name=entry.name,
                is_alias=entry.is_alias,
                is_refreshable=entry.is_refreshable,
            )
            for entry in catalog_entries
        ],
    )
    refreshable_pairs = catalog_repo.list_refreshable(provider.provider_name)
    scope_description = _describe_eodhd_fx_refresh_scope(
        start_date=start_date,
        end_date=end_date,
        explicit_start_date=explicit_start_date,
    )
    print(
        "Refreshing FX rates: "
        f"provider={provider.provider_name} "
        f"canonical_pairs={len(refreshable_pairs)} "
        f"{scope_description}",
        flush=True,
    )
    _print_fx_progress_bar(0, len(refreshable_pairs))

    stored = 0
    skipped_pairs = 0
    failed_pairs = 0
    completed_pairs = 0
    for entry in refreshable_pairs:
        base_currency = normalize_currency_code(entry.base_currency)
        quote_currency = normalize_currency_code(entry.quote_currency)
        if base_currency is None or quote_currency is None:
            failed_pairs += 1
            completed_pairs += 1
            _print_fx_progress_bar(
                completed_pairs,
                len(refreshable_pairs),
                item_label=entry.canonical_symbol,
            )
            continue
        state = state_repo.fetch(provider.provider_name, entry.canonical_symbol)
        min_rate_date, max_rate_date = fx_repo.pair_coverage(
            provider.provider_name,
            base_currency,
            quote_currency,
        )
        refresh_ranges, next_full_history = _plan_eodhd_fx_refresh_ranges(
            start_date=start_date,
            end_date=end_date,
            min_rate_date=min_rate_date,
            max_rate_date=max_rate_date,
            full_history_backfilled=state.full_history_backfilled if state else False,
            explicit_start_date=explicit_start_date,
        )
        attempted_full_history_backfill = any(
            range_start == FX_FULL_BACKFILL_START for range_start, _ in refresh_ranges
        )
        if not refresh_ranges:
            skipped_pairs += 1
            if state is not None:
                state_repo.mark_success(
                    provider.provider_name,
                    entry.canonical_symbol,
                    min_rate_date=min_rate_date,
                    max_rate_date=max_rate_date,
                    full_history_backfilled=state.full_history_backfilled,
                )
            completed_pairs += 1
            _print_fx_progress_bar(
                completed_pairs,
                len(refreshable_pairs),
                item_label=entry.canonical_symbol,
            )
            continue

        pair_failed = False
        current_min = min_rate_date
        current_max = max_rate_date
        current_full = next_full_history
        for range_start, range_end in refresh_ranges:
            if range_start > range_end:
                continue
            try:
                rows = provider.fetch_history(
                    canonical_symbol=entry.canonical_symbol,
                    start_date=range_start,
                    end_date=range_end,
                )
            except Exception as exc:
                LOGGER.warning(
                    "EODHD FX refresh failed | provider=%s symbol=%s range=%s..%s exception=%s",
                    provider.provider_name,
                    entry.canonical_symbol,
                    range_start.isoformat(),
                    range_end.isoformat(),
                    exc,
                )
                state_repo.mark_failure(
                    provider.provider_name, entry.canonical_symbol, str(exc)
                )
                pair_failed = True
                break
            if not rows and current_min is None and current_max is None:
                error = (
                    "No FX history returned "
                    f"for {entry.canonical_symbol} in range {range_start.isoformat()}..{range_end.isoformat()}"
                )
                LOGGER.warning(error)
                state_repo.mark_failure(
                    provider.provider_name, entry.canonical_symbol, error
                )
                pair_failed = True
                break
            stored += fx_repo.upsert_many(rows)
            # Widen coverage from the rows we just stored rather than issuing a
            # second full-group MIN/MAX scan of provider_fx_rates per range.
            current_min, current_max = _extend_coverage(current_min, current_max, rows)
        if pair_failed:
            failed_pairs += 1
            completed_pairs += 1
            _print_fx_progress_bar(
                completed_pairs,
                len(refreshable_pairs),
                item_label=entry.canonical_symbol,
            )
            continue
        if attempted_full_history_backfill:
            current_full = True

        state_repo.mark_success(
            provider.provider_name,
            entry.canonical_symbol,
            min_rate_date=current_min,
            max_rate_date=current_max,
            full_history_backfilled=current_full,
        )
        completed_pairs += 1
        _print_fx_progress_bar(
            completed_pairs,
            len(refreshable_pairs),
            item_label=entry.canonical_symbol,
        )

    print(
        "Stored FX rates: "
        f"provider={provider.provider_name} "
        f"pairs={len(refreshable_pairs)} "
        f"rows={stored} "
        f"skipped_pairs={skipped_pairs} "
        f"failed_pairs={failed_pairs} "
        f"{scope_description}"
    )
    return 0
