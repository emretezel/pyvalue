"""CLI handlers for running screens plus screen table/CSV/preview formatting.

Author: Emre Tezel
"""

from __future__ import annotations

import csv
import shutil
import textwrap
import time
from typing import (
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
)

from pyvalue.currency import (
    is_monetary_unit_kind,
    normalize_currency_code,
)
from pyvalue.money.fx import (
    FXService,
)
from pyvalue.metrics import REGISTRY
from pyvalue.screening import (
    Criterion,
    ScreenDefinition,
    compute_screen_ranking,
    evaluate_criterion_verbose,
    load_screen,
    ranking_metric_ids,
    screen_metric_ids,
)
from pyvalue.logging_utils import (
    suppress_console_metric_warnings,
)
from pyvalue.persistence.storage import (
    MarketDataRepository,
    MetricRecord,
    MetricsRepository,
    SecurityRepository,
    SupportedTickerRepository,
)

from ._common import (
    LOGGER,
    SCREEN_CONSOLE_MAX_DESCRIPTION_WIDTH,
    SCREEN_CONSOLE_MAX_ENTITY_WIDTH,
    SCREEN_CONSOLE_MIN_DESCRIPTION_WIDTH,
    SCREEN_CONSOLE_MIN_ENTITY_WIDTH,
    SCREEN_CONSOLE_PREVIEW_MAX_ROWS,
    SCREEN_PROGRESS_INTERVAL_SECONDS,
    _format_value,
    _prepare_output_csv_path,
    _print_symbol_progress,
    _resolve_canonical_scope_listings,
    _resolve_database_path,
)
from ._repos import (
    _PreloadedMetricsRepository,
    _SchemaReadyMarketDataRepository,
    _StatusAwareMetricsRepository,
)
from .metrics import (
    _initialize_metric_read_schema,
)


_SCREEN_SYMBOL_TIE_BREAKER_IDS = frozenset(
    {"canonical_symbol", "symbol", "ticker", "id"}
)


def _ordered_unique_metric_ids(*metric_id_lists: Sequence[str]) -> List[str]:
    ordered: List[str] = []
    seen: set[str] = set()
    for metric_ids in metric_id_lists:
        for metric_id in metric_ids:
            candidate = str(metric_id).strip()
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            ordered.append(candidate)
    return ordered


def _screen_filter_metric_ids(definition: ScreenDefinition) -> List[str]:
    return screen_metric_ids(definition)


def _screen_ranking_extra_metric_ids(definition: ScreenDefinition) -> List[str]:
    ranking = getattr(definition, "ranking", None)
    if ranking is None:
        return []

    metric_ids = list(ranking_metric_ids(definition))
    for tie_breaker in ranking.tie_breakers:
        metric_id = str(tie_breaker.metric_id).strip()
        if metric_id in _SCREEN_SYMBOL_TIE_BREAKER_IDS:
            continue
        if metric_id not in metric_ids:
            metric_ids.append(metric_id)

    filter_metric_ids = set(_screen_filter_metric_ids(definition))
    return [metric_id for metric_id in metric_ids if metric_id not in filter_metric_ids]


def _screen_requested_metric_ids(definition: ScreenDefinition) -> List[str]:
    return _ordered_unique_metric_ids(
        _screen_filter_metric_ids(definition),
        _screen_ranking_extra_metric_ids(definition),
    )


def _merge_metric_rows_by_id(
    existing_rows: Dict[int, Dict[str, MetricRecord]],
    additional_rows: Mapping[int, Mapping[str, MetricRecord]],
) -> None:
    for listing_id, metric_rows in additional_rows.items():
        existing_rows.setdefault(listing_id, {}).update(metric_rows)


def _evaluate_screen_scope(
    definition: ScreenDefinition,
    listings: Sequence[tuple[int, str]],
    metrics_repo: MetricsRepository,
    universe_names: Mapping[str, Optional[str]],
    *,
    report_progress: bool,
) -> tuple[List[int], Dict[str, Dict[int, float]], Dict[int, str]]:
    """Evaluate every criterion per listing, keyed by ``listing_id``.

    ``listings`` are the scope-resolved ``(listing_id, canonical_symbol)`` pairs.
    The display label comes from ``universe_names`` (one batch query loaded by the
    caller) rather than a per-listing identity lookup -- the canonical symbol is
    used only as a metric log label and as the dict's display value.
    """

    entity_labels: Dict[int, str] = {}
    passed_listing_ids: List[int] = []
    criterion_values: Dict[str, Dict[int, float]] = {
        criterion.name: {} for criterion in definition.criteria
    }
    completed = 0
    total = len(listings)
    last_progress_at = time.monotonic()
    last_reported_completed = -1

    def maybe_report_progress(force: bool = False) -> None:
        nonlocal last_progress_at, last_reported_completed
        if not report_progress:
            return
        if completed == last_reported_completed:
            return
        elapsed = time.monotonic() - last_progress_at
        if not force and elapsed < SCREEN_PROGRESS_INTERVAL_SECONDS:
            return
        _print_symbol_progress(completed, total)
        last_reported_completed = completed
        last_progress_at = time.monotonic()

    for listing_id, display_symbol in listings:
        listing_passed = True
        per_listing_values: Dict[str, float] = {}
        entity_labels[listing_id] = universe_names.get(display_symbol) or display_symbol
        for criterion in definition.criteria:
            passed, left_value = evaluate_criterion_verbose(
                criterion,
                listing_id,
                metrics_repo,
                display_symbol=display_symbol,
            )
            if not passed or left_value is None:
                listing_passed = False
                break
            per_listing_values[criterion.name] = left_value
        if listing_passed:
            passed_listing_ids.append(listing_id)
            for criterion in definition.criteria:
                criterion_values[criterion.name][listing_id] = per_listing_values[
                    criterion.name
                ]
        completed += 1
        maybe_report_progress()

    maybe_report_progress(force=True)
    return passed_listing_ids, criterion_values, entity_labels


def _rank_screen_passers(
    definition: ScreenDefinition,
    passed_listings: Sequence[tuple[int, str]],
    metrics_repo: MetricsRepository,
    security_repo: SecurityRepository,
) -> tuple[List[int], List[tuple[str, Dict[int, object]]]]:
    passed_listing_ids = [listing_id for listing_id, _ in passed_listings]
    if definition.ranking is None or not passed_listings:
        return passed_listing_ids, []

    display_symbols = {listing_id: symbol for listing_id, symbol in passed_listings}

    metric_ids = ranking_metric_ids(definition)
    for tie_breaker in definition.ranking.tie_breakers:
        if tie_breaker.metric_id in _SCREEN_SYMBOL_TIE_BREAKER_IDS:
            continue
        if tie_breaker.metric_id not in metric_ids:
            metric_ids.append(tie_breaker.metric_id)
    ranking_metric_config = {
        metric.metric_id: metric for metric in definition.ranking.metrics
    }
    tie_breaker_config = {
        tie_breaker.metric_id: tie_breaker
        for tie_breaker in definition.ranking.tie_breakers
        if tie_breaker.metric_id not in _SCREEN_SYMBOL_TIE_BREAKER_IDS
    }
    fx_service = FXService(metrics_repo.db_path)
    metric_values: Dict[str, Dict[int, float]] = {}
    for metric_id in metric_ids:
        records_by_id: Dict[int, MetricRecord] = {}
        unit_kinds = set()
        currencies = set()
        for listing_id in passed_listing_ids:
            record = metrics_repo.fetch_by_id(listing_id, metric_id)
            if record is None:
                continue
            records_by_id[listing_id] = record
            unit_kinds.add(record.unit_kind)
            if record.currency:
                currencies.add(record.currency)

        if not records_by_id:
            metric_values[metric_id] = {}
            continue

        if len(unit_kinds) > 1:
            LOGGER.warning(
                "Ranking metric skipped due to mixed unit kinds | metric=%s unit_kinds=%s",
                metric_id,
                ",".join(sorted(unit_kinds)),
            )
            metric_values[metric_id] = {}
            continue

        sample = next(iter(records_by_id.values()))
        config_entry = ranking_metric_config.get(metric_id) or tie_breaker_config.get(
            metric_id
        )
        comparison_currency = normalize_currency_code(
            getattr(config_entry, "currency", None)
        )

        if is_monetary_unit_kind(sample.unit_kind):
            if comparison_currency is None and len(currencies) > 1:
                LOGGER.warning(
                    "Ranking metric skipped due to mixed currencies without comparison currency | metric=%s currencies=%s",
                    metric_id,
                    ",".join(sorted(currencies)),
                )
                metric_values[metric_id] = {}
                continue
            target_currency = comparison_currency or next(iter(currencies), None)
            converted_values: Dict[int, float] = {}
            for listing_id, record in records_by_id.items():
                if target_currency is None:
                    continue
                if record.currency is None:
                    LOGGER.warning(
                        "Ranking metric missing currency | metric=%s symbol=%s",
                        metric_id,
                        display_symbols.get(listing_id, listing_id),
                    )
                    continue
                if record.currency == target_currency:
                    converted_values[listing_id] = record.value
                    continue
                converted = fx_service.convert_amount(
                    record.value,
                    record.currency,
                    target_currency,
                    record.as_of,
                )
                if converted is None:
                    LOGGER.warning(
                        "Ranking FX conversion failed | metric=%s symbol=%s from=%s to=%s as_of=%s",
                        metric_id,
                        display_symbols.get(listing_id, listing_id),
                        record.currency,
                        target_currency,
                        record.as_of,
                    )
                    continue
                converted_values[listing_id] = float(converted)
            metric_values[metric_id] = converted_values
            continue

        metric_values[metric_id] = {
            listing_id: record.value for listing_id, record in records_by_id.items()
        }

    metadata = security_repo.fetch_many_by_id(passed_listing_ids)
    sectors = {
        listing_id: (metadata[listing_id].sector if listing_id in metadata else None)
        for listing_id in passed_listing_ids
    }
    ranking_result = compute_screen_ranking(
        passed_listing_ids,
        definition.ranking,
        metric_values,
        sectors,
        display_symbols=display_symbols,
    )
    return list(ranking_result.ordered_listing_ids), [
        (
            "qarp_rank",
            {
                listing_id: ranking_result.ranks[listing_id]
                for listing_id in ranking_result.ordered_listing_ids
            },
        ),
        (
            "qarp_score",
            {
                listing_id: ranking_result.scores[listing_id]
                for listing_id in ranking_result.ordered_listing_ids
            },
        ),
    ]


def _emit_screen_results(
    criteria: Sequence[Criterion],
    ordered_listings: Sequence[tuple[int, str]],
    values: Dict[str, Dict[int, float]],
    entity_labels: Mapping[int, str],
    security_repo: SecurityRepository,
    market_repo: MarketDataRepository,
    output_csv: Optional[str],
    extra_rows: Optional[Sequence[tuple[str, Dict[int, object]]]] = None,
) -> None:
    """Render screen output: the single ``listing_id -> canonical_symbol`` boundary.

    Identity stays ``listing_id`` up to here; the table / CSV layer below is keyed
    by the canonical display symbol (the human-facing output identity). Description
    and price are read in one batch each (by listing id), not per passing row.
    """

    listing_ids = [listing_id for listing_id, _ in ordered_listings]
    symbols = [symbol for _, symbol in ordered_listings]
    securities = security_repo.fetch_many_by_id(listing_ids)
    snapshots = market_repo.latest_snapshots_many_by_ids(listing_ids)

    selected_names: Dict[str, str] = {}
    selected_descriptions: Dict[str, str] = {}
    selected_prices: Dict[str, str] = {}
    selected_price_currencies: Dict[str, str] = {}
    for listing_id, symbol in ordered_listings:
        selected_names[symbol] = entity_labels.get(listing_id, symbol)
        security = securities.get(listing_id)
        description = security.description if security is not None else None
        selected_descriptions[symbol] = description if description else "N/A"
        snapshot = snapshots.get(listing_id)
        if snapshot is not None:
            selected_prices[symbol] = _format_value(snapshot.price)
            selected_price_currencies[symbol] = snapshot.currency or "N/A"
        else:
            selected_prices[symbol] = "N/A"
            selected_price_currencies[symbol] = "N/A"

    # Remap the id-keyed criterion values and extra rows to the symbol-keyed
    # output layer (the display boundary).
    display_by_id = {listing_id: symbol for listing_id, symbol in ordered_listings}
    symbol_values: Dict[str, Dict[str, float]] = {
        criterion_name: {
            display_by_id[listing_id]: value
            for listing_id, value in by_id.items()
            if listing_id in display_by_id
        }
        for criterion_name, by_id in values.items()
    }
    symbol_extra_rows: List[tuple[str, Dict[str, object]]] = [
        (
            row_name,
            {
                display_by_id[listing_id]: value
                for listing_id, value in by_id.items()
                if listing_id in display_by_id
            },
        )
        for row_name, by_id in (extra_rows or ())
    ]

    _print_screen_table(
        criteria,
        symbols,
        symbol_values,
        selected_names,
        selected_descriptions,
        selected_prices,
        selected_price_currencies,
        output_csv=output_csv,
        extra_rows=symbol_extra_rows,
    )
    if output_csv:
        _write_screen_csv(
            criteria,
            symbols,
            symbol_values,
            selected_names,
            selected_descriptions,
            selected_prices,
            selected_price_currencies,
            output_csv,
            extra_rows=symbol_extra_rows,
        )


def cmd_run_screen_stage(
    config_path: str,
    database: str,
    symbols: Optional[Sequence[str]],
    exchange_codes: Optional[Sequence[str]],
    all_supported: bool,
    output_csv: Optional[str],
    show_metric_warnings: bool = False,
) -> int:
    """Unified screen evaluation over symbol, exchange, or full supported scope."""

    db_path = _resolve_database_path(database)
    scope_listings, _explicit_symbols, resolved_exchange_codes = (
        _resolve_canonical_scope_listings(
            str(db_path),
            symbols,
            exchange_codes,
            all_supported,
        )
    )
    # The scope query already returns the (listing_id, canonical_symbol) pairs, so
    # the natural listing_id is the identity end-to-end and the canonical symbol is
    # carried alongside purely for display/log output -- the screen never
    # re-resolves symbol->listing_id.
    display_by_id = {listing_id: symbol for listing_id, symbol in scope_listings}
    definition = load_screen(config_path)
    filter_metric_ids = _screen_filter_metric_ids(definition)
    ranking_extra_metric_ids = _screen_ranking_extra_metric_ids(definition)
    requested_metric_ids = _ordered_unique_metric_ids(
        filter_metric_ids,
        ranking_extra_metric_ids,
    )
    include_market_data = any(
        getattr(REGISTRY.get(metric_id), "uses_market_data", False)
        for metric_id in requested_metric_ids
        if REGISTRY.get(metric_id) is not None
    )
    MetricsRepository(db_path).initialize_schema()
    _initialize_metric_read_schema(db_path, include_market_data)
    market_repo = MarketDataRepository(db_path)
    market_repo.initialize_schema()
    metrics_repo = _StatusAwareMetricsRepository(
        db_path,
        market_repo=_SchemaReadyMarketDataRepository(db_path),
    )
    security_repo = SecurityRepository(db_path)
    security_repo.initialize_schema()

    with suppress_console_metric_warnings(not show_metric_warnings):
        if len(scope_listings) == 1:
            listing_id, symbol = scope_listings[0]
            security = security_repo.fetch(listing_id)
            entity_name = (
                security.entity_name if security and security.entity_name else symbol
            )
            description = (
                security.description if security and security.description else "N/A"
            )
            snapshot = market_repo.latest_snapshot_record_by_id(listing_id)
            price_label = _format_value(snapshot.price) if snapshot else "N/A"
            print(f"Entity: {entity_name}")
            print(f"Description: {description}")
            print(f"Price: {price_label}")
            results = []
            for criterion in definition.criteria:
                passed, left_value = evaluate_criterion_verbose(
                    criterion, listing_id, metrics_repo, display_symbol=symbol
                )
                results.append((criterion.name, passed, left_value))
            passed_all = all(flag for _, flag, _ in results)
            for name, passed, value in results:
                value_display = _format_value(value) if value is not None else "N/A"
                print(f"{name}: {'PASS' if passed else 'FAIL'} (value={value_display})")
            return 0 if passed_all else 1

        ticker_repo = SupportedTickerRepository(db_path)
        universe_names = dict(
            ticker_repo.list_canonical_symbol_name_pairs(
                resolved_exchange_codes,
                primary_only=True,
            )
        )
        listing_ids = [listing_id for listing_id, _ in scope_listings]
        metric_rows_by_id = metrics_repo.fetch_many_by_ids(
            listing_ids,
            filter_metric_ids,
        )
        evaluation_metrics_repo = _PreloadedMetricsRepository(
            db_path,
            metric_rows_by_id,
        )
        passed_listing_ids, criterion_values, entity_labels = _evaluate_screen_scope(
            definition,
            scope_listings,
            evaluation_metrics_repo,
            universe_names,
            report_progress=True,
        )

        if not passed_listing_ids:
            print("No symbols satisfied all criteria.")
            if output_csv:
                _write_screen_csv(
                    definition.criteria,
                    [],
                    {},
                    {},
                    {},
                    {},
                    {},
                    output_csv,
                )
            return 1

        if ranking_extra_metric_ids:
            ranking_metric_rows = metrics_repo.fetch_many_by_ids(
                passed_listing_ids,
                ranking_extra_metric_ids,
            )
            if ranking_metric_rows:
                _merge_metric_rows_by_id(
                    metric_rows_by_id,
                    ranking_metric_rows,
                )
                evaluation_metrics_repo = _PreloadedMetricsRepository(
                    db_path,
                    metric_rows_by_id,
                )

        passed_listings = [
            (listing_id, display_by_id[listing_id]) for listing_id in passed_listing_ids
        ]
        ordered_listing_ids, extra_rows = _rank_screen_passers(
            definition,
            passed_listings,
            evaluation_metrics_repo,
            security_repo,
        )
        ordered_listings = [
            (listing_id, display_by_id[listing_id])
            for listing_id in ordered_listing_ids
        ]
        _emit_screen_results(
            definition.criteria,
            ordered_listings,
            criterion_values,
            entity_labels,
            security_repo,
            market_repo,
            output_csv,
            extra_rows=extra_rows,
        )
        return 0


def _print_screen_table(
    criteria: Sequence[Criterion],
    symbols: Sequence[str],
    values: Dict[str, Dict[str, float]],
    entity_names: Dict[str, str],
    descriptions: Dict[str, str],
    prices: Dict[str, str],
    price_currencies: Dict[str, str],
    output_csv: Optional[str] = None,
    extra_rows: Optional[Sequence[tuple[str, Dict[str, object]]]] = None,
) -> None:
    output_rows = _build_screen_output_rows(
        criteria,
        symbols,
        values,
        entity_names,
        descriptions,
        prices,
        price_currencies,
        extra_rows=extra_rows,
    )
    if not output_rows:
        return

    preview_rows = output_rows[:SCREEN_CONSOLE_PREVIEW_MAX_ROWS]
    print(f"Passing symbols: {len(output_rows)}")
    if len(output_rows) > len(preview_rows):
        print(f"Showing top {len(preview_rows)} of {len(output_rows)} passing symbols.")
    if output_csv:
        print(f"CSV output: {output_csv}")
    elif len(output_rows) > len(preview_rows):
        print("Use --output-csv to save the full result set.")
    print()

    preview_fields = [row_name for row_name, _ in extra_rows or ()]
    preview_fields.extend(["symbol", "entity", "price_display", "description"])
    header = [_screen_preview_label(field_name) for field_name in preview_fields]
    rows: List[List[str]] = [
        [_truncate_display(row[field_name], 1_000) for field_name in preview_fields]
        for row in preview_rows
    ]
    widths = _screen_preview_widths(header, rows)
    print(" | ".join(title.ljust(widths[idx]) for idx, title in enumerate(header)))
    print("-+-".join("-" * widths[idx] for idx in range(len(header))))
    for row in rows:
        print(
            " | ".join(
                _truncate_display(cell, widths[idx]).ljust(widths[idx])
                for idx, cell in enumerate(row)
            )
        )


def _build_screen_output_rows(
    criteria: Sequence[Criterion],
    symbols: Sequence[str],
    values: Dict[str, Dict[str, float]],
    entity_names: Mapping[str, str],
    descriptions: Mapping[str, str],
    prices: Mapping[str, str],
    price_currencies: Mapping[str, str],
    extra_rows: Optional[Sequence[tuple[str, Dict[str, object]]]] = None,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for symbol in symbols:
        price = prices.get(symbol, "N/A")
        price_currency = price_currencies.get(symbol, "N/A")
        row = {
            "symbol": symbol,
            "entity": entity_names.get(symbol, symbol),
            "description": descriptions.get(symbol, "N/A"),
            "price": price,
            "price_currency": price_currency,
            "price_display": _screen_price_display(price, price_currency),
        }
        for row_name, row_values in extra_rows or ():
            value = row_values.get(symbol)
            row[row_name] = "" if value is None else _format_output_cell(value)
        for criterion in criteria:
            value = values.get(criterion.name, {}).get(symbol)
            row[criterion.name] = "" if value is None else _format_value(value)
        rows.append(row)
    return rows


def _screen_output_columns(
    criteria: Sequence[Criterion],
    extra_rows: Optional[Sequence[tuple[str, Dict[str, object]]]] = None,
) -> List[str]:
    return [
        "symbol",
        "entity",
        "description",
        "price",
        "price_currency",
        *[row_name for row_name, _ in extra_rows or ()],
        *[criterion.name for criterion in criteria],
    ]


def _screen_price_display(price: str, price_currency: str) -> str:
    if price == "N/A":
        return price
    if not price_currency or price_currency == "N/A":
        return price
    return f"{price} {price_currency}"


def _screen_preview_label(field_name: str) -> str:
    if field_name == "symbol":
        return "Symbol"
    if field_name == "entity":
        return "Entity"
    if field_name == "description":
        return "Description"
    if field_name == "price_display":
        return "Price"
    if field_name == "qarp_rank":
        return "Rank"
    if field_name == "qarp_score":
        return "Score"
    return field_name


def _screen_preview_widths(
    header: Sequence[str], rows: Sequence[Sequence[str]]
) -> List[int]:
    widths = []
    terminal_width = shutil.get_terminal_size(fallback=(140, 20)).columns
    for idx, title in enumerate(header):
        column_width = max(len(title), *(len(row[idx]) for row in rows))
        if title == "Entity":
            column_width = max(
                SCREEN_CONSOLE_MIN_ENTITY_WIDTH,
                min(column_width, SCREEN_CONSOLE_MAX_ENTITY_WIDTH),
            )
        elif title == "Description":
            column_width = max(
                SCREEN_CONSOLE_MIN_DESCRIPTION_WIDTH,
                min(column_width, SCREEN_CONSOLE_MAX_DESCRIPTION_WIDTH),
            )
        elif title == "Symbol":
            column_width = min(column_width, 16)
        elif title == "Price":
            column_width = min(column_width, 18)
        elif title == "Rank":
            column_width = min(column_width, 6)
        elif title == "Score":
            column_width = min(column_width, 9)
        widths.append(column_width)

    total_width = sum(widths) + (3 * (len(widths) - 1))
    if total_width <= terminal_width:
        return widths

    def shrink(title: str, minimum: int, overflow: int) -> int:
        nonlocal total_width
        if overflow <= 0:
            return overflow
        try:
            index = header.index(title)
        except ValueError:
            return overflow
        reducible = max(0, widths[index] - minimum)
        reduction = min(reducible, overflow)
        widths[index] -= reduction
        total_width -= reduction
        return overflow - reduction

    overflow = total_width - terminal_width
    overflow = shrink("Description", SCREEN_CONSOLE_MIN_DESCRIPTION_WIDTH, overflow)
    overflow = shrink("Entity", SCREEN_CONSOLE_MIN_ENTITY_WIDTH, overflow)
    overflow = shrink("Price", len("Price"), overflow)
    overflow = shrink("Score", len("Score"), overflow)
    overflow = shrink("Symbol", len("Symbol"), overflow)

    if overflow > 0:
        for idx, title in enumerate(header):
            minimum = len(title)
            reducible = max(0, widths[idx] - minimum)
            if reducible <= 0:
                continue
            reduction = min(reducible, overflow)
            widths[idx] -= reduction
            overflow -= reduction
            if overflow <= 0:
                break

    return widths


def _truncate_display(value: str, width: int) -> str:
    if width <= 0:
        return ""
    collapsed = " ".join(str(value).split())
    if len(collapsed) <= width:
        return collapsed
    if width <= 3:
        return collapsed[:width]
    return textwrap.shorten(collapsed, width=width, placeholder="...")


def _format_output_cell(value: object) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _format_value(float(value))
    return str(value)


def _write_screen_csv(
    criteria: Sequence[Criterion],
    symbols: Sequence[str],
    values: Dict[str, Dict[str, float]],
    entity_names: Dict[str, str],
    descriptions: Dict[str, str],
    prices: Dict[str, str],
    price_currencies: Dict[str, str],
    path: str,
    extra_rows: Optional[Sequence[tuple[str, Dict[str, object]]]] = None,
) -> None:
    output_path = _prepare_output_csv_path(path)
    columns = _screen_output_columns(criteria, extra_rows=extra_rows)
    rows = _build_screen_output_rows(
        criteria,
        symbols,
        values,
        entity_names,
        descriptions,
        prices,
        price_currencies,
        extra_rows=extra_rows,
    )
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row.get(column, "") for column in columns])
