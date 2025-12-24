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
        return self._get_value("alpha_vantage", "api_key")

    @property
    def eodhd_api_key(self) -> Optional[str]:
        return self._get_value("eodhd", "api_key")

    @property
    def sec_user_agent(self) -> Optional[str]:
        return self._get_value("sec", "user_agent")

    def _get_value(self, section: str, option: str) -> Optional[str]:
        value = self._parser.get(section, option, fallback=None)
        if value is None:
            return None
        cleaned = value.strip()
        if cleaned.startswith('"') and cleaned.endswith('"'):
            cleaned = cleaned[1:-1]
        if cleaned.startswith("'") and cleaned.endswith("'"):
            cleaned = cleaned[1:-1]
        return cleaned


__all__ = ["Config"]
