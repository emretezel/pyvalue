"""Public facade for the pyvalue CLI package.

Re-exports the console entry point (:func:`main`) and the symbols the test suite
reaches through ``import pyvalue.cli as cli`` — both the command handlers and the
internals that tests fake. These are plain re-exports: attribute *reads* resolve
here. Tests that *fake* an internal use ``cli_test_helpers.patch_cli``, which
patches the owning sub-module(s) directly — necessary because a symbol such as
``EODHDFundamentalsClient`` is imported independently by several command
sub-modules, each holding its own binding. Names re-exported only so tests can
read/patch them (absent from ``__all__``) carry a per-line ``# noqa: F401``
marking them intentional.

Author: Emre Tezel
"""

from __future__ import annotations

import time  # noqa: F401  (re-exported as cli.time for tests that patch cli.time.*)

from pyvalue.config import Config  # noqa: F401
from pyvalue.logging_utils import (
    current_logging_config,
    setup_logging,
    suppress_console_logging,  # noqa: F401
    suppress_console_metric_warnings,  # noqa: F401
)
from pyvalue.metrics import REGISTRY
from pyvalue.money.fx import FXService  # noqa: F401

from ._common import (
    _ComputedMetricsResult,
    _MetricAttemptResult,
    _ProfiledComputedMetricsBatchResult,
    _reconcile_eodhd_listing_scope,  # noqa: F401
    _resolve_canonical_scope_listings,
    _resolve_database_path,
    _resolve_provider_scope,
    _validate_scope_selector,
)
from ._batch import (
    _RateLimiter,
    _create_interruptible_thread_executor,
    _initialize_worker_logging,
    _shutdown_executor_now,  # noqa: F401
)
from ._repos import (
    _StatusAwareMetricsRepository,
)
from .universe import (
    _report_skipped_no_currency,
    cmd_refresh_supported_exchanges,
    cmd_refresh_supported_tickers,
)
from .ingest import (
    EODHDFundamentalsClient,  # noqa: F401
    as_completed,  # noqa: F401
    cmd_ingest_fundamentals_stage,  # noqa: F401
    cmd_reconcile_listing_status,
    cmd_report_fundamentals_progress,  # noqa: F401
    cmd_report_ingest_progress,
)
from .market_data import (
    EODHDProvider,  # noqa: F401
    MarketDataService,  # noqa: F401
    _build_market_data_update,  # noqa: F401
    _fetch_symbol_market_data,  # noqa: F401
    _plan_market_data_stage_run,
    cmd_report_market_data_progress,
    cmd_update_market_data_stage,
)
from .normalize import (
    EODHDFactsNormalizer,  # noqa: F401
    _normalization_worker_count,  # noqa: F401
    _plan_normalization_selection,  # noqa: F401
    _process_local_fx_service,  # noqa: F401
    _process_local_fx_service_db,  # noqa: F401
    cmd_normalize_eodhd_fundamentals_bulk,
    cmd_normalize_fundamentals_stage,
)
from .metrics import (
    METRICS_COMPUTE_BATCH_SIZE,  # noqa: F401
    METRICS_PROGRESS_INTERVAL_SECONDS,  # noqa: F401
    METRICS_WRITE_BATCH_INTERVAL_SECONDS,  # noqa: F401
    METRICS_WRITE_BATCH_SIZE,  # noqa: F401
    _compute_metric_batch_results,
    _compute_metrics_for_symbol,
    _compute_metrics_for_symbol_batch_worker,
    _compute_metrics_for_symbol_batch_worker_profiled,  # noqa: F401
    _compute_metrics_for_symbol_worker,  # noqa: F401
    _create_process_pool_executor,  # noqa: F401
    _flush_metric_write_batch,
    _initialize_metric_read_schema,
    _metric_worker_count,  # noqa: F401
    _run_metric_computation,
    cmd_compute_metrics_stage,
)
from .screen import (
    SCREEN_CONSOLE_MAX_DESCRIPTION_WIDTH,  # noqa: F401
    SCREEN_CONSOLE_PREVIEW_MAX_ROWS,  # noqa: F401
    _rank_screen_passers,
    cmd_run_screen_stage,
)
from .explain import (
    cmd_explain_metric,
)
from .reports import (
    SCREEN_PROGRESS_INTERVAL_SECONDS,  # noqa: F401
    cmd_report_fact_freshness,
    cmd_report_metric_status,
    cmd_report_screen_failures,
)
from .fx import (
    EODHDFXProvider,  # noqa: F401
    _require_eodhd_key,  # noqa: F401
    cmd_refresh_fx_rates,
)
from .maintenance import (
    cmd_clear_financial_facts,
    cmd_clear_fundamentals_raw,
    cmd_clear_metrics,
)
from .security import (
    SECURITY_METADATA_CHUNK_SIZE,  # noqa: F401
    SECURITY_METADATA_PROGRESS_INTERVAL_SECONDS,  # noqa: F401
    cmd_refresh_security_metadata,
)
from .app import (
    build_parser,
    main,
)

__all__ = [
    "main",
    "_ComputedMetricsResult",
    "_MetricAttemptResult",
    "_ProfiledComputedMetricsBatchResult",
    "_resolve_canonical_scope_listings",
    "_resolve_database_path",
    "_resolve_provider_scope",
    "_validate_scope_selector",
    "_RateLimiter",
    "_create_interruptible_thread_executor",
    "_initialize_worker_logging",
    "_StatusAwareMetricsRepository",
    "_report_skipped_no_currency",
    "cmd_refresh_supported_exchanges",
    "cmd_refresh_supported_tickers",
    "cmd_reconcile_listing_status",
    "cmd_report_ingest_progress",
    "_plan_market_data_stage_run",
    "cmd_report_market_data_progress",
    "cmd_update_market_data_stage",
    "cmd_normalize_eodhd_fundamentals_bulk",
    "cmd_normalize_fundamentals_stage",
    "_compute_metric_batch_results",
    "_compute_metrics_for_symbol",
    "_compute_metrics_for_symbol_batch_worker",
    "_flush_metric_write_batch",
    "_initialize_metric_read_schema",
    "_run_metric_computation",
    "cmd_compute_metrics_stage",
    "_rank_screen_passers",
    "cmd_run_screen_stage",
    "cmd_explain_metric",
    "cmd_report_fact_freshness",
    "cmd_report_metric_status",
    "cmd_report_screen_failures",
    "cmd_refresh_fx_rates",
    "cmd_clear_financial_facts",
    "cmd_clear_fundamentals_raw",
    "cmd_clear_metrics",
    "cmd_refresh_security_metadata",
    "build_parser",
    "REGISTRY",
    "current_logging_config",
    "setup_logging",
    "suppress_console_logging",
    "suppress_console_metric_warnings",
    "time",
]
