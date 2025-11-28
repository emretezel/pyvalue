"""Universe loader for US-listed equities across major exchanges.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Mapping, Optional, Sequence
import csv
import io
import logging
from ftplib import FTP

LOGGER = logging.getLogger(__name__)

# Nasdaq Trader publishes daily symbol directories on ftp://ftp.nasdaqtrader.com.
NASDAQ_LISTED_PATH = "symboldirectory/nasdaqlisted.txt"
OTHER_LISTED_PATH = "symboldirectory/otherlisted.txt"


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


class USUniverseLoader:
    """Download and normalize the US equity universe."""

    #: Exchanges we include by default. Values must match normalized names.
    DEFAULT_MAJOR_EXCHANGES = {
        "NASDAQ",
        "NYSE",
        "NYSE Arca",
        "NYSE MKT",
        "Cboe BZX",
    }

    def __init__(
        self,
        allowed_exchanges: Optional[Sequence[str]] = None,
        ftp_host: str = "ftp.nasdaqtrader.com",
        ftp_timeout: int = 60,
        fetcher: Optional[Callable[[str], str]] = None,
    ) -> None:
        self.allowed_exchanges = set(allowed_exchanges or self.DEFAULT_MAJOR_EXCHANGES)
        self.ftp_host = ftp_host
        self.ftp_timeout = ftp_timeout
        self._custom_fetcher = fetcher

    def load(self) -> List[Listing]:
        """Return the consolidated list of US listings."""

        # Pull both Nasdaq-listed and other-listed universes so we cover every exchange.
        nasdaq_rows = self._download_and_parse(NASDAQ_LISTED_PATH, source="nasdaq")
        other_rows = self._download_and_parse(OTHER_LISTED_PATH, source="other")

        listings: Dict[str, Listing] = {}
        for row in [*nasdaq_rows, *other_rows]:
            listing = self._row_to_listing(row)
            if listing is None:
                continue
            # Deduplicate by ticker so the latest row for a symbol wins.
            listings[listing.symbol] = listing
        LOGGER.info("Loaded %s symbols from SEC/Nasdaq feeds", len(listings))
        return sorted(listings.values(), key=lambda l: l.symbol)

    def _download_and_parse(self, path: str, source: str) -> List[Mapping[str, str]]:
        # Simple helper that fetches a remote table and parses it.
        LOGGER.debug("Fetching %s from ftp://%s", path, self.ftp_host)
        body = self._fetch_from_ftp(path)
        return self._parse_pipe_table(body, source)

    def _fetch_from_ftp(self, path: str) -> str:
        if self._custom_fetcher is not None:
            return self._custom_fetcher(path)

        buffer = io.BytesIO()
        with FTP(self.ftp_host, timeout=self.ftp_timeout) as ftp:
            ftp.login()
            ftp.retrbinary(f"RETR {path}", buffer.write)
        return buffer.getvalue().decode("utf-8", errors="ignore")

    def _parse_pipe_table(self, body: str, source: str) -> List[Mapping[str, str]]:
        # Nasdaq symbol files end with a footer row. We drop incomplete rows.
        cleaned_lines = [line for line in body.splitlines() if line and "File Creation" not in line]
        reader = csv.DictReader(io.StringIO("\n".join(cleaned_lines)), delimiter="|")
        rows: List[Mapping[str, str]] = []
        for row in reader:
            if not row.get("Symbol") and not row.get("ACT Symbol"):
                continue
            row["__source"] = source
            rows.append(row)
        return rows

    def _row_to_listing(self, row: Mapping[str, str]) -> Optional[Listing]:
        # Convert a Nasdaq Trader row into our Listing model.
        source = row.get("__source") or "unknown"
        symbol = (row.get("Symbol") or row.get("ACT Symbol") or "").strip()
        if not symbol or symbol.upper() == "TEST":
            return None

        is_test_issue = (row.get("Test Issue") or "N").strip().upper() == "Y"
        if is_test_issue:
            return None

        exchange = self._normalize_exchange(row)
        if exchange not in self.allowed_exchanges:
            return None

        status = (row.get("Financial Status") or row.get("Status")) or None
        lot_size = row.get("Round Lot Size")
        try:
            # Round lot is often represented as float text; coerce to int if possible.
            round_lot = int(float(lot_size)) if lot_size else None
        except ValueError:
            round_lot = None

        is_etf = (row.get("ETF") or row.get("ETF?" ) or "N").strip().upper() == "Y"
        market_category = row.get("Market Category") or row.get("Tier") or None

        qualified = symbol.upper()
        if not qualified.endswith(".US"):
            qualified = f"{qualified}.US"

        return Listing(
            symbol=qualified,
            security_name=(row.get("Security Name") or row.get("Company Name") or "").strip(),
            exchange=exchange,
            market_category=market_category,
            is_etf=is_etf,
            is_test_issue=is_test_issue,
            status=status,
            round_lot_size=round_lot,
            source=source,
            isin=None,
            currency=None,
        )

    def _normalize_exchange(self, row: Mapping[str, str]) -> str:
        source = row.get("__source")
        if source == "nasdaq":
            return "NASDAQ"

        code = (row.get("Exchange") or row.get("Listing Exchange") or "").upper()
        # Map Nasdaq Trader exchange codes to user-friendly names.
        mapping = {
            "A": "NYSE MKT",
            "N": "NYSE",
            "P": "NYSE Arca",
            "Z": "Cboe BZX",
            "B": "NYSE",
            "T": "NASDAQ",
        }
        return mapping.get(code, code or "UNKNOWN")


__all__ = ["Listing", "USUniverseLoader"]
