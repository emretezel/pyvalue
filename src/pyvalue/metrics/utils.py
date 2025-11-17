"""Shared helpers for metrics.

Author: Emre Tezel
"""

from __future__ import annotations

from typing import Dict, Iterable

from pyvalue.storage import FactRecord


def filter_unique_fy(records: Iterable[FactRecord]) -> Dict[str, FactRecord]:
    """Return a dict of end_date -> FactRecord for valid full-year entries."""

    unique: Dict[str, FactRecord] = {}
    for record in records:
        if not _is_valid_fy_frame(record.frame):
            continue
        if record.end_date not in unique:
            unique[record.end_date] = record
    return unique


def _is_valid_fy_frame(frame: str | None) -> bool:
    if not frame:
        return False
    if not frame.startswith("CY"):
        return False
    if frame.endswith(("Q1", "Q2", "Q3", "Q4")):
        return False
    year_part = frame[2:]
    return len(year_part) == 4 and year_part.isdigit()


__all__ = ["filter_unique_fy"]
