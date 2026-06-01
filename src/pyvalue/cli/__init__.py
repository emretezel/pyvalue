"""Public facade for the pyvalue CLI package.

Re-exports the console entry point (:func:`main`) and every symbol the test
suite reaches through ``import pyvalue.cli as cli``. The original ``cli.py`` was a
single module, so tests both *read* internals as ``cli.<name>`` and *patch* them
with ``monkeypatch.setattr(cli, "<name>", fake)`` expecting the handler code to
observe the patch. After the split the handlers live in submodules and read their
own module-level globals, so a plain re-export would let reads work but silently
break patches.

To preserve that contract exactly, this module installs a custom module subclass
whose ``__setattr__`` forwards each assignment to every ``pyvalue.cli`` submodule
that already binds that name as a global. ``monkeypatch.setattr(cli, name, value)``
then updates the binding the handler actually reads, and monkeypatch teardown
restores it through the same path. Reads continue to resolve against the facade.
The forwarding is computed dynamically from the live submodules (no name->module
table to keep in sync). Names re-exported only for patching/reading (absent from
``__all__``) carry a per-line ``# noqa: F401`` marking them intentional.

Author: Emre Tezel
"""

from __future__ import annotations

import sys
import time  # noqa: F401  (re-exported as cli.time for tests that patch cli.time.*)
from types import ModuleType

from pyvalue.logging_utils import (
    current_logging_config,
    setup_logging,
    suppress_console_metric_warnings,  # noqa: F401
    suppress_console_missing_fx_warnings,  # noqa: F401
)
from pyvalue.metrics import REGISTRY

from ._common import (
    _ComputedMetricsResult,
    _MetricAttemptResult,
    _ProfiledComputedMetricsBatchResult,
    _resolve_canonical_scope_symbols,
    _resolve_database_path,
    _resolve_provider_scope_rows,
    _resolve_ticker_target_currency,
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
    USUniverseLoader,  # noqa: F401
    _report_skipped_no_currency,
    cmd_load_universe,
    cmd_refresh_supported_exchanges,
    cmd_refresh_supported_tickers,
)
from .ingest import (
    EODHDFundamentalsClient,  # noqa: F401
    SECCompanyFactsClient,  # noqa: F401
    as_completed,  # noqa: F401
    cmd_ingest_fundamentals,
    cmd_ingest_fundamentals_bulk,
    cmd_ingest_fundamentals_global,
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
    cmd_update_market_data_bulk,
    cmd_update_market_data_global,
    cmd_update_market_data_stage,
)
from .normalize import (
    EODHDFactsNormalizer,  # noqa: F401
    SECFactsNormalizer,  # noqa: F401
    _normalization_worker_count,  # noqa: F401
    _plan_normalization_selection,  # noqa: F401
    _process_local_fx_service,  # noqa: F401
    _process_local_fx_service_db,  # noqa: F401
    _process_local_ticker_repo,  # noqa: F401
    _process_local_ticker_repo_db,  # noqa: F401
    cmd_normalize_eodhd_fundamentals_bulk,
    cmd_normalize_fundamentals,
    cmd_normalize_fundamentals_bulk,
    cmd_normalize_fundamentals_stage,
    cmd_normalize_us_facts_bulk,
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
    _ensure_metrics_wal_mode,  # noqa: F401
    _flush_metric_write_batch,
    _initialize_metric_read_schema,
    _metric_worker_count,  # noqa: F401
    _run_metric_computation,
    cmd_compute_metrics,
    cmd_compute_metrics_bulk,
    cmd_compute_metrics_stage,
)
from .screen import (
    SCREEN_CONSOLE_MAX_DESCRIPTION_WIDTH,  # noqa: F401
    SCREEN_CONSOLE_PREVIEW_MAX_ROWS,  # noqa: F401
    _rank_screen_passers,
    cmd_run_screen_bulk,
    cmd_run_screen_stage,
)
from .reports import (
    SCREEN_PROGRESS_INTERVAL_SECONDS,  # noqa: F401
    cmd_report_fact_freshness,
    cmd_report_metric_coverage,
    cmd_report_metric_failures,
    cmd_report_screen_failures,
)
from .fx import (
    Config,  # noqa: F401
    EODHDFXProvider,  # noqa: F401
    FXService,  # noqa: F401
    _reconcile_eodhd_listing_scope,  # noqa: F401
    _require_eodhd_key,  # noqa: F401
    cmd_refresh_fx_rates,
)
from .maintenance import (
    cmd_clear_financial_facts,
    cmd_clear_fundamentals_raw,
    cmd_clear_metrics,
    cmd_purge_us_nonfilers,
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
    "_resolve_canonical_scope_symbols",
    "_resolve_database_path",
    "_resolve_provider_scope_rows",
    "_resolve_ticker_target_currency",
    "_validate_scope_selector",
    "_RateLimiter",
    "_create_interruptible_thread_executor",
    "_initialize_worker_logging",
    "_StatusAwareMetricsRepository",
    "_report_skipped_no_currency",
    "cmd_load_universe",
    "cmd_refresh_supported_exchanges",
    "cmd_refresh_supported_tickers",
    "cmd_ingest_fundamentals",
    "cmd_ingest_fundamentals_bulk",
    "cmd_ingest_fundamentals_global",
    "cmd_reconcile_listing_status",
    "cmd_report_ingest_progress",
    "_plan_market_data_stage_run",
    "cmd_report_market_data_progress",
    "cmd_update_market_data_bulk",
    "cmd_update_market_data_global",
    "cmd_update_market_data_stage",
    "cmd_normalize_eodhd_fundamentals_bulk",
    "cmd_normalize_fundamentals",
    "cmd_normalize_fundamentals_bulk",
    "cmd_normalize_fundamentals_stage",
    "cmd_normalize_us_facts_bulk",
    "_compute_metric_batch_results",
    "_compute_metrics_for_symbol",
    "_compute_metrics_for_symbol_batch_worker",
    "_flush_metric_write_batch",
    "_initialize_metric_read_schema",
    "_run_metric_computation",
    "cmd_compute_metrics",
    "cmd_compute_metrics_bulk",
    "cmd_compute_metrics_stage",
    "_rank_screen_passers",
    "cmd_run_screen_bulk",
    "cmd_run_screen_stage",
    "cmd_report_fact_freshness",
    "cmd_report_metric_coverage",
    "cmd_report_metric_failures",
    "cmd_report_screen_failures",
    "cmd_refresh_fx_rates",
    "cmd_clear_financial_facts",
    "cmd_clear_fundamentals_raw",
    "cmd_clear_metrics",
    "cmd_purge_us_nonfilers",
    "cmd_refresh_security_metadata",
    "build_parser",
    "REGISTRY",
    "current_logging_config",
    "setup_logging",
    "suppress_console_metric_warnings",
    "suppress_console_missing_fx_warnings",
    "time",
]

_PACKAGE = "pyvalue.cli"


class _CLIFacadeModule(ModuleType):
    """Module type that forwards attribute writes to the owning submodules.

    Preserves the pre-split contract where ``monkeypatch.setattr(cli, name,
    value)`` updated the single binding every handler read. The value is also set
    on the facade itself so subsequent reads stay consistent.
    """

    def __setattr__(self, name: str, value: object) -> None:
        super().__setattr__(name, value)
        # Propagate to every CLI submodule that already binds ``name`` as a
        # global, reproducing the pre-split single-binding behaviour. Computed
        # from the live module table so there is nothing to keep in sync.
        prefix = _PACKAGE + "."
        for mod_name, submodule in list(sys.modules.items()):
            if (
                submodule is not None
                and mod_name.startswith(prefix)
                and name in getattr(submodule, "__dict__", {})
            ):
                setattr(submodule, name, value)


# Swap to the forwarding subclass once all imports above are bound, so facade
# construction itself is not intercepted.
sys.modules[__name__].__class__ = _CLIFacadeModule
