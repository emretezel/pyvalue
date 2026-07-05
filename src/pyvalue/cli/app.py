"""CLI argument parser construction and the main dispatch entry point.

Author: Emre Tezel
"""

from __future__ import annotations

import argparse
from typing import (
    Optional,
    Sequence,
)

from pyvalue.metrics.utils import MAX_FACT_AGE_DAYS
from pyvalue.logging_utils import (
    setup_logging,
)

from ._batch import (
    _cancel_cli_command,
)
from .universe import (
    cmd_refresh_supported_exchanges,
    cmd_refresh_supported_tickers,
)
from .ingest import (
    cmd_ingest_fundamentals_stage,
    cmd_reconcile_listing_status,
    cmd_report_fundamentals_progress,
)
from .market_data import (
    cmd_report_market_data_progress,
    cmd_update_market_data_stage,
)
from .normalize import (
    cmd_normalize_fundamentals_stage,
)
from .metrics import (
    cmd_compute_metrics_stage,
)
from .screen import (
    cmd_run_screen_stage,
)
from .explain import (
    cmd_explain_metric,
)
from .reports import (
    cmd_report_fact_freshness,
    cmd_report_metric_status,
    cmd_report_screen_failures,
)
from .fx import (
    cmd_refresh_fx_rates,
)
from .maintenance import (
    cmd_clear_financial_facts,
    cmd_clear_fundamentals_raw,
    cmd_clear_market_data,
    cmd_clear_metrics,
)
from .security import (
    cmd_refresh_security_metadata,
)


def build_parser() -> argparse.ArgumentParser:
    """Configure the root parser with subcommands."""

    parser = argparse.ArgumentParser(description="pyvalue data utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_scope_args(command_parser: argparse.ArgumentParser) -> None:
        scope = command_parser.add_mutually_exclusive_group(required=False)
        scope.add_argument(
            "--symbols",
            nargs="+",
            default=None,
            help=(
                "Space or comma separated list of fully qualified symbols. "
                "Defaults to the full supported universe when omitted."
            ),
        )
        scope.add_argument(
            "--exchange-codes",
            nargs="+",
            default=None,
            help=(
                "Space or comma separated list of exchange codes. "
                "Defaults to the full supported universe when omitted."
            ),
        )
        scope.add_argument(
            "--all-supported",
            action="store_true",
            help="Select the full supported universe in the current catalog.",
        )

    refresh_supported_exchanges = subparsers.add_parser(
        "refresh-supported-exchanges",
        help="Refresh and persist the provider-supported exchange catalog.",
    )
    refresh_supported_exchanges.add_argument(
        "--provider",
        default="EODHD",
        choices=["EODHD"],
        help="Supported exchange provider to refresh (default: %(default)s).",
    )
    refresh_supported_exchanges.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    refresh_supported_tickers = subparsers.add_parser(
        "refresh-supported-tickers",
        help="Refresh and persist the provider-supported ticker catalog.",
    )
    refresh_supported_tickers.add_argument(
        "--provider",
        default="EODHD",
        choices=["EODHD"],
        help="Supported ticker provider to refresh (default: %(default)s).",
    )
    refresh_supported_tickers.add_argument(
        "--exchange-codes",
        nargs="+",
        default=None,
        help="Optional exchange-code subset (space or comma separated).",
    )
    refresh_supported_tickers.add_argument(
        "--all-supported",
        action="store_true",
        help="Refresh every supported exchange for the provider.",
    )
    refresh_supported_tickers.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    clear_facts = subparsers.add_parser(
        "clear-financial-facts",
        help="Delete all normalized financial facts.",
    )
    clear_facts.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    clear_fundamentals_raw = subparsers.add_parser(
        "clear-fundamentals-raw",
        help="Delete all stored raw fundamentals.",
    )
    clear_fundamentals_raw.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    clear_metrics = subparsers.add_parser(
        "clear-metrics",
        help="Delete all computed metrics.",
    )
    clear_metrics.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    clear_market_data = subparsers.add_parser(
        "clear-market-data",
        help="Delete all stored market data snapshots.",
    )
    clear_market_data.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    ingest_fundamentals = subparsers.add_parser(
        "ingest-fundamentals",
        help="Download fundamentals for supported tickers from the chosen provider.",
    )
    ingest_fundamentals.add_argument(
        "--provider",
        default="EODHD",
        choices=["EODHD"],
        help="Fundamentals provider to use (default: %(default)s).",
    )
    add_scope_args(ingest_fundamentals)
    ingest_fundamentals.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    ingest_fundamentals.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Throttle rate in EODHD symbols/min (default: eodhd.fundamentals_requests_per_minute).",
    )
    ingest_fundamentals.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Maximum number of symbols to ingest in this run.",
    )
    ingest_fundamentals.add_argument(
        "--max-age-days",
        type=int,
        default=30,
        help=(
            "Only ingest symbols with older fundamentals (days) or missing "
            "data (default: %(default)s)."
        ),
    )
    ingest_fundamentals.add_argument(
        "--retry-failed-now",
        action="store_true",
        help="Ignore retry backoff and retry previously failed symbols immediately.",
    )

    reconcile_listing_status = subparsers.add_parser(
        "reconcile-listing-status",
        help=(
            "Backfill cached EODHD primary/secondary listing classification from "
            "stored raw fundamentals without downloading data."
        ),
    )
    reconcile_listing_status.add_argument(
        "--provider",
        default="EODHD",
        choices=["EODHD"],
        help="Listing-classification provider to reconcile (default: %(default)s).",
    )
    add_scope_args(reconcile_listing_status)
    reconcile_listing_status.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )

    fundamentals_progress = subparsers.add_parser(
        "report-fundamentals-progress",
        help="Report EODHD fundamentals ingest progress across supported tickers.",
    )
    fundamentals_progress.add_argument(
        "--provider",
        default="EODHD",
        choices=["EODHD"],
        help="Fundamentals provider to report on (default: %(default)s).",
    )
    fundamentals_progress.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    fundamentals_progress.add_argument(
        "--exchange-codes",
        nargs="+",
        default=None,
        help="Optional exchange-code filter (space or comma separated). Defaults to all stored supported tickers.",
    )
    fundamentals_progress_mode = fundamentals_progress.add_mutually_exclusive_group()
    fundamentals_progress_mode.add_argument(
        "--max-age-days",
        type=int,
        default=30,
        help="Freshness window in days (default: %(default)s).",
    )
    fundamentals_progress_mode.add_argument(
        "--missing-only",
        action="store_true",
        help="Only require that a raw fundamentals payload exists, regardless of age.",
    )

    market_data_progress = subparsers.add_parser(
        "report-market-data-progress",
        help="Report EODHD market data refresh progress across supported tickers.",
    )
    market_data_progress.add_argument(
        "--provider",
        default="EODHD",
        choices=["EODHD"],
        help="Market data provider to report on (default: %(default)s).",
    )
    market_data_progress.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    market_data_progress.add_argument(
        "--exchange-codes",
        nargs="+",
        default=None,
        help="Optional exchange-code filter (space or comma separated). Defaults to all stored supported tickers.",
    )
    market_data_progress.add_argument(
        "--max-age-days",
        type=int,
        default=30,
        help="Freshness window in days (default: %(default)s).",
    )

    market_data = subparsers.add_parser(
        "update-market-data",
        help="Fetch latest market data for supported tickers and persist it.",
    )
    market_data.add_argument(
        "--provider",
        default="EODHD",
        choices=["EODHD"],
        help="Market data provider to use (default: %(default)s).",
    )
    market_data.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(market_data)
    market_data.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Throttle rate in requests per minute (defaults to eodhd.market_data_requests_per_minute).",
    )
    market_data.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Maximum number of symbols to attempt in this run, before quota capping.",
    )
    market_data.add_argument(
        "--max-age-days",
        type=int,
        default=30,
        help=(
            "Refresh only stale or missing market data older than this many "
            "days (default: %(default)s)."
        ),
    )
    market_data.add_argument(
        "--retry-failed-now",
        action="store_true",
        help="Ignore retry backoff and retry previously failed symbols immediately.",
    )

    normalize_fundamentals = subparsers.add_parser(
        "normalize-fundamentals",
        help=(
            "Normalize stored fundamentals across the requested supported-ticker "
            "scope. Bulk runs parallelize automatically."
        ),
    )
    normalize_fundamentals.add_argument(
        "--provider",
        default="EODHD",
        choices=["EODHD"],
        help="Fundamentals provider to normalize (default: %(default)s).",
    )
    add_scope_args(normalize_fundamentals)
    normalize_fundamentals.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    normalize_fundamentals.add_argument(
        "--force",
        action="store_true",
        help="Re-normalize even when stored raw fundamentals are already up to date.",
    )

    compute_metrics = subparsers.add_parser(
        "compute-metrics",
        help="Compute one or more metrics for the requested canonical ticker scope.",
    )
    compute_metrics.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(compute_metrics)
    compute_metrics.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to compute (default: all registered metrics).",
    )
    compute_metrics.add_argument(
        "--show-metric-warnings",
        action="store_true",
        help="Show metric/data-quality warnings on the console (default: suppressed).",
    )
    compute_metrics.add_argument(
        "--profile",
        action="store_true",
        help=(
            "Print read/compute/write/total wall-clock timings at end of run "
            "(useful for tuning compute-metrics performance)."
        ),
    )

    refresh_fx_rates = subparsers.add_parser(
        "refresh-fx-rates",
        help="Fetch and store FX rates for currencies already present in the project database.",
    )
    refresh_fx_rates.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    refresh_fx_rates.add_argument(
        "--start-date",
        default=None,
        help="Optional historical FX backfill start date (YYYY-MM-DD). Defaults to the end date.",
    )
    refresh_fx_rates.add_argument(
        "--end-date",
        default=None,
        help="Optional FX refresh end date (YYYY-MM-DD). Defaults to today.",
    )

    fact_report = subparsers.add_parser(
        "report-fact-freshness",
        help="List missing or stale financial facts required by metrics for the requested canonical scope.",
    )
    fact_report.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(fact_report)
    fact_report.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to include (default: all registered metrics)",
    )
    fact_report.add_argument(
        "--max-age-days",
        type=int,
        default=MAX_FACT_AGE_DAYS,
        help="Fact freshness window in days (default: %(default)s)",
    )
    fact_report.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV path for detailed concept coverage.",
    )
    fact_report.add_argument(
        "--show-all",
        action="store_true",
        help="Show concepts even when all symbols are fresh.",
    )

    metric_status_report = subparsers.add_parser(
        "report-metric-status",
        help="Rank metrics by persisted NA share (failed or never-attempted) for the requested canonical scope without recomputing.",
    )
    metric_status_report.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(metric_status_report)
    metric_status_report.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to include (default: all registered metrics; mutually exclusive with --config)",
    )
    metric_status_report.add_argument(
        "--config",
        default=None,
        help="Optional screening config (YAML); restricts the report to the screen's criteria metrics.",
    )
    metric_status_report.add_argument(
        "--reasons",
        action="store_true",
        help="Break each metric's failures down by persisted reason code with an example symbol.",
    )
    metric_status_report.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV path for the metric status summary.",
    )

    explain_metric = subparsers.add_parser(
        "explain-metric",
        help="Explain per symbol why a metric computes or comes out NA: persisted state, fact inputs, market seam, and a write-free live recompute with untemplated warnings.",
    )
    explain_metric.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    explain_metric.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="Fully qualified symbols to explain (this command is deliberately symbol-scoped; use the report-* commands for scope-wide surveys).",
    )
    explain_metric.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Metric identifiers to explain (mutually exclusive with --config).",
    )
    explain_metric.add_argument(
        "--config",
        default=None,
        help="Screening config (YAML); explains the screen's criteria metrics (mutually exclusive with --metrics).",
    )
    explain_metric.add_argument(
        "--max-age-days",
        type=int,
        default=MAX_FACT_AGE_DAYS,
        help="Fact freshness window in days (default: %(default)s)",
    )

    screen_failure_report = subparsers.add_parser(
        "report-screen-failures",
        help="Rank which screen criteria and missing metrics exclude the most symbols for the requested canonical scope.",
    )
    screen_failure_report.add_argument(
        "--config",
        required=True,
        help="Path to screening config (YAML)",
    )
    screen_failure_report.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    add_scope_args(screen_failure_report)
    screen_failure_report.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV path for metric-level screen failure reasons.",
    )

    run_screen = subparsers.add_parser(
        "run-screen",
        help="Evaluate screening criteria for the requested canonical scope.",
    )
    run_screen.add_argument(
        "--config",
        required=True,
        help="Path to screening config (YAML)",
    )
    run_screen.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file (default: %(default)s)",
    )
    add_scope_args(run_screen)
    run_screen.add_argument(
        "--show-metric-warnings",
        action="store_true",
        help="Show metric/data-quality warnings on the console (default: suppressed).",
    )
    run_screen.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV path for passing results.",
    )

    refresh_security_metadata = subparsers.add_parser(
        "refresh-security-metadata",
        help="Refresh canonical security metadata from stored raw fundamentals without rewriting normalized facts.",
    )
    refresh_security_metadata.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file (default: %(default)s)",
    )
    add_scope_args(refresh_security_metadata)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entrypoint used by console_scripts."""

    setup_logging()
    parser = build_parser()
    try:
        args = parser.parse_args(argv)

        if args.command == "refresh-supported-exchanges":
            return cmd_refresh_supported_exchanges(
                provider=args.provider,
                database=args.database,
            )
        if args.command == "refresh-supported-tickers":
            return cmd_refresh_supported_tickers(
                provider=args.provider,
                database=args.database,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
            )
        if args.command == "refresh-fx-rates":
            return cmd_refresh_fx_rates(
                database=args.database,
                start_date=args.start_date,
                end_date=args.end_date,
            )
        if args.command == "reconcile-listing-status":
            return cmd_reconcile_listing_status(
                provider=args.provider,
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
            )
        if args.command == "ingest-fundamentals":
            return cmd_ingest_fundamentals_stage(
                provider=args.provider,
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                rate=args.rate,
                max_symbols=args.max_symbols,
                max_age_days=args.max_age_days,
                respect_backoff=not args.retry_failed_now,
            )
        if args.command == "report-fundamentals-progress":
            return cmd_report_fundamentals_progress(
                provider=args.provider,
                database=args.database,
                exchange_codes=args.exchange_codes,
                max_age_days=args.max_age_days,
                missing_only=args.missing_only,
            )
        if args.command == "report-market-data-progress":
            return cmd_report_market_data_progress(
                provider=args.provider,
                database=args.database,
                exchange_codes=args.exchange_codes,
                max_age_days=args.max_age_days,
            )
        if args.command == "update-market-data":
            return cmd_update_market_data_stage(
                provider=args.provider,
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                rate=args.rate,
                max_symbols=args.max_symbols,
                max_age_days=args.max_age_days,
                respect_backoff=not args.retry_failed_now,
            )
        if args.command == "normalize-fundamentals":
            return cmd_normalize_fundamentals_stage(
                provider=args.provider,
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                force=args.force,
            )
        if args.command == "clear-financial-facts":
            return cmd_clear_financial_facts(database=args.database)
        if args.command == "clear-fundamentals-raw":
            return cmd_clear_fundamentals_raw(database=args.database)
        if args.command == "clear-metrics":
            return cmd_clear_metrics(database=args.database)
        if args.command == "clear-market-data":
            return cmd_clear_market_data(database=args.database)
        if args.command == "compute-metrics":
            return cmd_compute_metrics_stage(
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                metric_ids=args.metrics,
                show_metric_warnings=args.show_metric_warnings,
                profile=args.profile,
            )
        if args.command == "report-fact-freshness":
            return cmd_report_fact_freshness(
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                metric_ids=args.metrics,
                max_age_days=args.max_age_days,
                output_csv=args.output_csv,
                show_all=args.show_all,
            )
        if args.command == "explain-metric":
            return cmd_explain_metric(
                database=args.database,
                symbols=args.symbols,
                metric_ids=args.metrics,
                config_path=args.config,
                max_age_days=args.max_age_days,
            )
        if args.command == "report-metric-status":
            return cmd_report_metric_status(
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                metric_ids=args.metrics,
                config_path=args.config,
                show_reasons=args.reasons,
                output_csv=args.output_csv,
            )
        if args.command == "report-screen-failures":
            return cmd_report_screen_failures(
                config_path=args.config,
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                output_csv=args.output_csv,
            )
        if args.command == "run-screen":
            return cmd_run_screen_stage(
                config_path=args.config,
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
                output_csv=args.output_csv,
                show_metric_warnings=args.show_metric_warnings,
            )
        if args.command == "refresh-security-metadata":
            return cmd_refresh_security_metadata(
                database=args.database,
                symbols=args.symbols,
                exchange_codes=args.exchange_codes,
                all_supported=args.all_supported,
            )
        parser.error(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        return _cancel_cli_command("Cancelled by user.")


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    raise SystemExit(main())
