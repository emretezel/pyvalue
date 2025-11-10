"""
Utilities for loading and validating screening configuration files.
Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

import yaml


@dataclass
class FilterSpec:
    metric: str
    operator: str
    value: float
    scope: str = "latest"


@dataclass
class ScreenSpec:
    name: str
    filters: List[FilterSpec] = field(default_factory=list)


@dataclass
class ScreeningConfig:
    screens: Dict[str, ScreenSpec]

    def get(self, screen_name: str) -> ScreenSpec:
        if screen_name not in self.screens:
            raise KeyError(
                f"Screen '{screen_name}' not found. Available screens: {list(self.screens)}"
            )
        return self.screens[screen_name]


VALID_OPERATORS = {">", ">=", "<", "<=", "==", "!="}
VALID_SCOPES = {"latest", "historical"}


def _parse_filter(entry: dict) -> FilterSpec:
    metric = entry.get("metric")
    operator = entry.get("operator")
    value = entry.get("value")
    scope = entry.get("scope", "latest")

    if metric is None or operator is None or value is None:
        raise ValueError(f"Filter entries must include metric, operator, and value: {entry}")
    if operator not in VALID_OPERATORS:
        raise ValueError(f"Unsupported operator '{operator}' in {entry}")
    if scope not in VALID_SCOPES:
        raise ValueError(f"Unsupported scope '{scope}' in {entry}")

    return FilterSpec(
        metric=str(metric),
        operator=operator,
        value=float(value),
        scope=scope,
    )


def _parse_screen(entry: dict) -> ScreenSpec:
    name = entry.get("name")
    filters = entry.get("filters") or entry.get("all")
    if not name:
        raise ValueError(f"Screen entry missing name: {entry}")
    if not filters:
        raise ValueError(f"Screen '{name}' must include at least one filter.")
    filter_specs = [_parse_filter(f) for f in filters]
    return ScreenSpec(name=name, filters=filter_specs)


def load_screening_config(path: str | Path) -> ScreeningConfig:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Screening config file not found: {path}")
    raw = yaml.safe_load(path.read_text()) or {}
    screen_entries = raw.get("screens")
    if not screen_entries:
        raise ValueError("Screening config must define at least one screen under 'screens'.")

    screens = {}
    for entry in screen_entries:
        screen = _parse_screen(entry)
        screens[screen.name] = screen

    return ScreeningConfig(screens=screens)
