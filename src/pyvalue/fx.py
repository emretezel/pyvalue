"""FX rate loader and conversion helpers using data/fx CSVs.

Author: Emre Tezel
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def _to_date(value: object) -> Optional[date]:
    if value is None:
        return None
    text = str(value).strip()[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _parse_rate(row: Dict[str, str]) -> Optional[float]:
    for key in ("rate", "value", "price"):
        if key in row:
            try:
                return float(row[key])
            except (TypeError, ValueError):
                return None
    for key, raw in row.items():
        if key.lower() in {"date", "as_of"}:
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


@dataclass
class FXRate:
    as_of: date
    rate: float


class FXRateStore:
    """Load FX rates from ``data/fx`` CSV files and provide conversion helpers."""

    def __init__(self, root: Path | str = "data/fx") -> None:
        self.root = Path(root)
        self.cache: Dict[str, List[FXRate]] = {}

    def convert(self, amount: float, from_currency: str, to_currency: str, as_of: str | date) -> Optional[float]:
        """Convert ``amount`` from ``from_currency`` into ``to_currency`` at the closest available rate."""

        from_code = (from_currency or "").upper()
        to_code = (to_currency or "").upper()
        if not from_code or not to_code:
            return None
        if from_code == to_code:
            return amount
        as_of_date = _to_date(as_of)
        if as_of_date is None:
            return None

        rate = self._rate(from_code, to_code, as_of_date)
        if rate is not None:
            return amount * rate
        inverse = self._rate(to_code, from_code, as_of_date)
        if inverse is not None and inverse != 0:
            return amount / inverse
        return None

    def _rate(self, base: str, quote: str, as_of: date) -> Optional[float]:
        pair = f"{base}{quote}".upper()
        series = self._load_pair(pair)
        if not series:
            return None
        return self._closest(series, as_of)

    def _load_pair(self, pair: str) -> List[FXRate]:
        if pair in self.cache:
            return self.cache[pair]
        path = self.root / f"{pair}.csv"
        if not path.exists():
            self.cache[pair] = []
            return []

        rates: List[FXRate] = []
        with path.open("r", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                as_of = _to_date(
                    row.get("date")
                    or row.get("Date")
                    or row.get("as_of")
                    or row.get("AsOf")
                    or row.get("datetime")
                    or row.get("DATETIME")
                )
                if as_of is None:
                    # Try any column with "date" in its name as a last resort.
                    for key, raw in row.items():
                        if "date" in key.lower():
                            as_of = _to_date(raw)
                            if as_of:
                                break
                rate = _parse_rate(row)
                if as_of is None or rate is None:
                    continue
                rates.append(FXRate(as_of=as_of, rate=rate))
        rates.sort(key=lambda entry: entry.as_of)
        self.cache[pair] = rates
        return rates

    def _closest(self, series: Iterable[FXRate], target: date) -> Optional[float]:
        closest: Tuple[int, float] | None = None
        for entry in series:
            delta = abs((entry.as_of - target).days)
            if closest is None or delta < closest[0]:
                closest = (delta, entry.rate)
        if closest is None:
            return None
        return closest[1]


__all__ = ["FXRateStore"]
