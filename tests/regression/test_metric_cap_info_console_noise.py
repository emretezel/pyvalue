"""Regression: documented-cap INFO notices stay off the console.

``compute-metrics`` echoed one line per capped listing to the console --
``interest_coverage: no measurable interest expense with positive TTM EBIT
for listing_id=... -- emitting documented cap 100x`` -- because the
console-only metric-noise filter behind ``suppress_console_metric_warnings``
matched WARNING records exclusively, letting INFO diagnostics from
``pyvalue.metrics`` loggers through. Per-listing INFO notices are batch noise
exactly like the metric warnings: they belong in ``data/logs/pyvalue.log``
only. Fails on the WARNING-only filter (the cap notice reaches the console)
and passes once metric records at WARNING and below are console-suppressed.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from pyvalue.logging_utils import setup_logging, suppress_console_metric_warnings
from pyvalue.metrics.interest_coverage import INTEREST_COVERAGE_CAP, LOGGER


def _clear_root_handlers() -> None:
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()


def test_cap_notice_is_file_only_on_console_suppressed_runs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The production cap emission must not reach the console handler."""

    log_dir = tmp_path / "logs"
    _clear_root_handlers()
    setup_logging(log_dir=log_dir)
    try:
        with suppress_console_metric_warnings(True):
            # The exact production emission from InterestCoverageMetric's
            # debt-free cap path, through the module's real logger -- the
            # filter keys on the record's logger name and level.
            LOGGER.info(
                "interest_coverage: no measurable interest expense with positive TTM "
                "EBIT for listing_id=%s -- emitting documented cap %.0fx",
                22625,
                INTEREST_COVERAGE_CAP,
            )

        captured = capsys.readouterr()
        assert "emitting documented cap" not in captured.err
        assert "emitting documented cap" not in captured.out

        log_text = (log_dir / "pyvalue.log").read_text(encoding="utf-8")
        assert (
            "interest_coverage: no measurable interest expense with positive TTM "
            "EBIT for listing_id=22625 -- emitting documented cap 100x"
        ) in log_text
    finally:
        _clear_root_handlers()
