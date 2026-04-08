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
    def eodhd_api_key(self) -> Optional[str]:
        return self._get_value("eodhd", "api_key")

    @property
    def eodhd_fundamentals_requests_per_minute(self) -> int:
        return self._get_int_value(
            "eodhd",
            "fundamentals_requests_per_minute",
            default=950,
        )

    @property
    def eodhd_fundamentals_daily_buffer_calls(self) -> int:
        return self._get_int_value(
            "eodhd",
            "fundamentals_daily_buffer_calls",
            default=5000,
        )

    @property
    def eodhd_market_data_requests_per_minute(self) -> int:
        return self._get_int_value(
            "eodhd",
            "market_data_requests_per_minute",
            default=950,
        )

    @property
    def eodhd_market_data_daily_buffer_calls(self) -> int:
        return self._get_int_value(
            "eodhd",
            "market_data_daily_buffer_calls",
            default=5000,
        )

    @property
    def sec_user_agent(self) -> Optional[str]:
        return self._get_value("sec", "user_agent")

    @property
    def fx_provider(self) -> str:
        return (self._get_value("fx", "provider") or "EODHD").strip().upper()

    @property
    def fx_pivot_currency(self) -> str:
        return (self._get_value("fx", "pivot_currency") or "USD").strip().upper()

    @property
    def fx_secondary_pivot_currency(self) -> Optional[str]:
        value = self._get_value("fx", "secondary_pivot_currency")
        if value is None:
            return "EUR"
        cleaned = value.strip().upper()
        return cleaned or None

    @property
    def fx_stale_warning_days(self) -> int:
        return self._get_int_value("fx", "stale_warning_days", default=7)

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

    def _get_int_value(self, section: str, option: str, default: int) -> int:
        value = self._get_value(section, option)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    def _get_bool_value(self, section: str, option: str, default: bool) -> bool:
        value = self._get_value(section, option)
        if value is None:
            return default
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        return default


__all__ = ["Config"]
