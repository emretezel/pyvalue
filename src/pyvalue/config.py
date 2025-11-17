"""Configuration loader for pyvalue.

Author: Emre Tezel
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import configparser


class Config:
    """Access project configuration stored in private/config.toml."""

    def __init__(self, path: Optional[Path] = None) -> None:
        base = path or Path("private/config.toml")
        self.path = base
        self._parser = configparser.ConfigParser()
        if base.exists():
            self._parser.read(base)

    @property
    def alpha_vantage_api_key(self) -> Optional[str]:
        return self._parser.get("alpha_vantage", "api_key", fallback=None)


__all__ = ["Config"]
