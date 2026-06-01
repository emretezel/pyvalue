"""Domain models shared by the universe and persistence layers.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Listing:
    """Represents a security listing on an exchange."""

    symbol: str
    security_name: str
    exchange: str
    market_category: Optional[str] = None
    is_etf: bool = False
    is_test_issue: bool = False
    status: Optional[str] = None
    round_lot_size: Optional[int] = None
    source: Optional[str] = None
    isin: Optional[str] = None
    currency: Optional[str] = None
