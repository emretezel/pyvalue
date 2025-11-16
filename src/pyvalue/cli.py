# Author: Emre Tezel
"""Command line utilities for pyvalue."""

from __future__ import annotations

import argparse
import logging
from typing import Optional, Sequence

from pyvalue.storage import UniverseRepository
from pyvalue.universe import USUniverseLoader

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Configure the root parser with subcommands."""

    parser = argparse.ArgumentParser(description="pyvalue data utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    load_us = subparsers.add_parser(
        "load-us-universe",
        help="Download Nasdaq Trader files and persist the US equity universe.",
    )
    load_us.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database file used for storage (default: %(default)s)",
    )
    load_us.add_argument(
        "--include-etfs",
        action="store_true",
        help="Persist ETFs alongside operating companies.",
    )

    return parser


def _should_keep_listing(include_etfs: bool, listing_is_etf: bool) -> bool:
    """Return True if the listing should be kept after ETF filtering."""

    return include_etfs or not listing_is_etf


def cmd_load_us_universe(database: str, include_etfs: bool) -> int:
    """Execute the US universe load command."""

    loader = USUniverseLoader()
    listings = loader.load()
    LOGGER.info("Fetched %s US listings", len(listings))

    # Drop ETFs unless explicitly requested in the CLI arguments.
    filtered = [item for item in listings if _should_keep_listing(include_etfs, item.is_etf)]
    LOGGER.info("Remaining listings after ETF filter: %s", len(filtered))

    # Persist the normalized listings to SQLite storage.
    repo = UniverseRepository(database)
    repo.initialize_schema()
    inserted = repo.replace_universe(filtered, region="US")

    print(f"Stored {inserted} US listings in {database}")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entrypoint used by console_scripts."""

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "load-us-universe":
        return cmd_load_us_universe(database=args.database, include_etfs=args.include_etfs)

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":  # pragma: no cover - manual execution helper
    raise SystemExit(main())
