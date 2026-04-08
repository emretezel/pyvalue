"""Logging configuration helpers.

Author: Emre Tezel
"""

from __future__ import annotations

from contextlib import contextmanager
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Iterator, Optional, Union


def setup_logging(
    log_dir: Union[str, Path] = "data/logs",
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
) -> None:
    """Configure root logging with console and rotating file handlers."""

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(min(console_level, file_level))

    console = logging.StreamHandler()
    console.setLevel(console_level)
    console.setFormatter(logging.Formatter("%(levelname)s %(message)s"))

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        filename=log_path / "pyvalue.log",
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )

    root.addHandler(console)
    root.addHandler(file_handler)

    # Quiet noisy libraries.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def current_logging_config() -> tuple[Optional[Path], int, int]:
    """Return the active log directory plus console/file levels."""

    root = logging.getLogger()
    console_levels = [
        handler.level
        for handler in root.handlers
        if isinstance(handler, logging.StreamHandler)
        and not isinstance(handler, logging.FileHandler)
    ]
    file_handlers = [
        handler
        for handler in root.handlers
        if isinstance(handler, TimedRotatingFileHandler)
    ]
    log_dir = (
        Path(file_handlers[0].baseFilename).parent.resolve() if file_handlers else None
    )
    file_levels = [handler.level for handler in file_handlers]
    return (
        log_dir,
        min(console_levels) if console_levels else logging.INFO,
        min(file_levels) if file_levels else logging.DEBUG,
    )


class _ConsoleMetricWarningFilter(logging.Filter):
    """Suppress noisy metric and screen warnings on console only."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno != logging.WARNING:
            return True
        if record.name.startswith("pyvalue.metrics"):
            return False
        if (
            record.name == "pyvalue.screening"
            and record.msg == "Metric %s missing for %s; run compute-metrics first"
        ):
            return False
        return not (
            record.name == "pyvalue.cli"
            and record.msg == "Metric %s could not be computed for %s"
        )


class _ConsoleMissingFXWarningFilter(logging.Filter):
    """Suppress missing-FX warning noise on console only."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno != logging.WARNING:
            return True
        if (
            record.name == "pyvalue.fx"
            and record.msg
            == "Missing FX rate | provider=%s base=%s quote=%s as_of=%s operation=get_fx_rate"
        ):
            return False
        return not (
            record.msg
            == "Missing FX rate for monetary conversion | operation=%s symbol=%s field=%s from=%s to=%s as_of=%s"
        )


def _console_handlers() -> list[logging.Handler]:
    root = logging.getLogger()
    return [
        handler
        for handler in root.handlers
        if isinstance(handler, logging.StreamHandler)
        and not isinstance(handler, logging.FileHandler)
    ]


@contextmanager
def _suppress_console_filter(
    warning_filter: logging.Filter, enabled: bool = True
) -> Iterator[None]:
    """Apply one console-only logging filter for the current process."""

    if not enabled:
        yield
        return

    console_handlers = _console_handlers()
    for handler in console_handlers:
        handler.addFilter(warning_filter)
    try:
        yield
    finally:
        for handler in console_handlers:
            handler.removeFilter(warning_filter)


@contextmanager
def suppress_console_metric_warnings(enabled: bool = True) -> Iterator[None]:
    """Hide metric warning noise from console handlers only."""

    with _suppress_console_filter(_ConsoleMetricWarningFilter(), enabled):
        yield


@contextmanager
def suppress_console_missing_fx_warnings(enabled: bool = True) -> Iterator[None]:
    """Hide missing-FX warning noise from console handlers only."""

    with _suppress_console_filter(_ConsoleMissingFXWarningFilter(), enabled):
        yield


__all__ = [
    "current_logging_config",
    "setup_logging",
    "suppress_console_metric_warnings",
    "suppress_console_missing_fx_warnings",
]
