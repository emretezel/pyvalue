"""Logging configuration helpers.

Author: Emre Tezel
"""

from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional, Union


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


__all__ = ["setup_logging"]
